use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;

use super::common::counting_metrics::CountingMetrics;
use super::common::manager_env::ManagerEnv;
use super::common::persist_completed_iterations;
use super::common::provider_harness::{ProviderHarness, ProviderStorageRequest};
use super::common::storage_wrapper::CountingStorage;
use crate::{
    FinishedIterationSink, JobCleaner, JobCleanerConfig, JobCode, JobDefinition, JobDefinitionId,
    JobDefinitionRegistry, JobRegistry, JobStateCodecKind, JobsManagerConfig, MetricsSink, NoopMetrics, Storage,
    StorageError, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, Worker, task_fn,
};

/// Sink for the tests here, which drive workers without a manager and do not observe iteration ends.
struct DiscardedIterations;

impl FinishedIterationSink for DiscardedIterations {
    fn record_finished_iteration(&self, _job_code: &JobCode, _iter_num: u64) {}
}

/// Bound on every wait: iteration cleanup is asynchronous, so tests poll for its effect.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(20);

/// Prefix every case here keeps its state objects under.
const STATE_PREFIX: &str = "jobs";

/// Inverted iteration numbers used in state object names, stated literally so assertions do not
/// depend on the key builder under test. The layout is `state-{u64::MAX - iter_num:020}{ext}`, and
/// the entry at index `n` is the inverted number of iteration `n`.
const INVERTED_ITER_NUMS: [&str; 11] = [
    "18446744073709551615",
    "18446744073709551614",
    "18446744073709551613",
    "18446744073709551612",
    "18446744073709551611",
    "18446744073709551610",
    "18446744073709551609",
    "18446744073709551608",
    "18446744073709551607",
    "18446744073709551606",
    "18446744073709551605",
];

fn build_state_key(job_code: &JobCode, iter_num: usize, extension: &str) -> String {
    format!(
        "{STATE_PREFIX}/{job_code}/state-{}{extension}",
        INVERTED_ITER_NUMS[iter_num]
    )
}

fn build_job_prefix(job_code: &JobCode) -> String {
    format!("{STATE_PREFIX}/{job_code}/")
}

fn build_job_definition(
    job_code: &JobCode,
    iteration_retention: u64,
) -> Result<JobDefinition, Box<dyn std::error::Error>> {
    let executor = task_fn(|_ctx| async { Ok(TaskOutcome::Completed(b"done".to_vec())) });
    let task_def = TaskDefinition::new(TaskCode::new("cleanup_task"), Duration::from_secs(5));

    Ok(JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_iteration_retention(iteration_retention)?)
}

async fn build_storage(
    harness: &dyn ProviderHarness,
    job_registry: &Arc<JobRegistry>,
    codec: JobStateCodecKind,
) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
    harness
        .build_storage(&ProviderStorageRequest::new(
            STATE_PREFIX,
            codec,
            Arc::clone(job_registry) as Arc<dyn JobDefinitionRegistry>,
            Arc::new(NoopMetrics),
        ))
        .await
}

/// The backend under test with `metrics` behind it, so a case can count the requests its calls were
/// billed for and the pair each was recorded under.
async fn build_measured_storage(
    harness: &dyn ProviderHarness,
    job_registry: &Arc<JobRegistry>,
    metrics: &Arc<CountingMetrics>,
) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
    harness
        .build_storage(&ProviderStorageRequest::new(
            STATE_PREFIX,
            JobStateCodecKind::Json,
            Arc::clone(job_registry) as Arc<dyn JobDefinitionRegistry>,
            Arc::clone(metrics) as Arc<dyn MetricsSink>,
        ))
        .await
}

/// The file extension state objects of `codec` carry, stated rather than read off the codec under
/// test.
const fn state_extension_of(codec: JobStateCodecKind) -> &'static str {
    match codec {
        JobStateCodecKind::Json => ".json",
        JobStateCodecKind::Cbor => ".cbor",
    }
}

async fn wait_for_state_keys(
    harness: &dyn ProviderHarness,
    job_code: &JobCode,
    expected_keys: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let keys = harness.read_state_keys(&build_job_prefix(job_code)).await?;
        if keys == expected_keys {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            return Err(format!(
                "timeout waiting for state keys {expected_keys:?} on {}, found {keys:?}",
                harness.provider_name()
            )
            .into());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Polls until `job_code` has finished iteration `iter_num`, so a test does not depend on how long
/// the workers need to get there.
async fn wait_for_processed_iteration(
    storage: &dyn Storage,
    job_code: &JobCode,
    iter_num: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
    loop {
        match storage.get_job(job_code, &CancellationToken::new()).await {
            Ok(job) if job.is_processed() && job.iter_num() >= iter_num => return Ok(()),
            Ok(_) | Err(StorageError::NotFound(_)) => {}
            Err(e) => return Err(Box::new(e)),
        }
        if tokio::time::Instant::now() > deadline {
            return Err(format!("timeout waiting for job {job_code} to finish iteration {iter_num}").into());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// The keys of iterations `kept` in descending iteration order, which is the ascending key order a
/// listing answers with.
fn build_kept_keys(job_code: &JobCode, kept: std::ops::RangeInclusive<usize>, extension: &str) -> Vec<String> {
    kept.rev()
        .map(|iter_num| build_state_key(job_code, iter_num, extension))
        .collect()
}

/// Only the worker that persisted the start of an iteration reports it, so however many workers
/// raced for that iteration, the cleaner hears about it once.
///
/// The workers are started by hand rather than through `JobsManager`, and the report channel is
/// read directly instead of being handed to a `JobCleaner`: a duplicate report would otherwise
/// only show up as an extra asynchronous delete, which
/// [`ManagerEnv::stop`](super::common::manager_env::ManagerEnv::stop) can discard before the
/// assertion runs.
async fn run_concurrent_workers_report_each_started_iteration_once(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_single_report_job");
    let max_iterations = 9;

    let job_def = build_job_definition(&job_code, 5)?.with_max_iterations(max_iterations)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = Arc::new(CountingStorage::new(
        build_storage(harness, &job_registry, JobStateCodecKind::Json).await?,
    ));

    // Wider than any legal number of reports, so a duplicate cannot be swallowed by a full channel.
    let (sender, mut receiver) = mpsc::channel(64);
    let finished_iterations = Arc::new(DiscardedIterations) as Arc<dyn FinishedIterationSink>;
    let cancel_token = CancellationToken::new();
    let mut worker_tasks = JoinSet::new();
    for _ in 0..3 {
        let worker = Worker::new(
            Arc::clone(&job_registry),
            Arc::clone(&storage) as Arc<dyn Storage>,
            super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
            Arc::new(NoopMetrics),
            Some(sender.clone()),
            Arc::clone(&finished_iterations),
        );
        let worker_token = cancel_token.clone();
        worker_tasks.spawn(async move { worker.start(worker_token).await });
    }

    // The diagnostics are turned into a `String` here rather than carried as an error: the value
    // lives across the joins below, and a boxed error is not `Send`.
    let wait_result = wait_for_processed_iteration(storage.as_ref(), &job_code, max_iterations)
        .await
        .map_err(|e| e.to_string());
    cancel_token.cancel();
    while let Some(worker_result) = worker_tasks.join_next().await {
        worker_result??;
    }
    wait_result?;

    drop(sender);
    let mut reported_iter_nums = Vec::new();
    while let Some(report) = receiver.recv().await {
        reported_iter_nums.push(report.iter_num);
    }
    reported_iter_nums.sort_unstable();

    // Iteration 1 arrives through job creation, which reports nothing: no iteration can have left
    // the retention window yet.
    assert_eq!(
        reported_iter_nums,
        (2..=max_iterations).collect::<Vec<u64>>(),
        "every started iteration must be reported exactly once"
    );
    // Without a lost conditional write the workers never actually raced, and the assertion above
    // would hold for a single worker just as well.
    assert!(
        storage.put_attempts() > storage.put_successes(),
        "the workers must have contended for the same iteration: {} save attempts, {} stored",
        storage.put_attempts(),
        storage.put_successes()
    );

    Ok(())
}

/// The whole request budget of reconciling one job, counted in calls of the trait: one
/// `find_job_meta` for the current iteration, one listing of the tail, and one delete carrying
/// every outdated iteration. How many requests a provider turns that delete into is its own
/// business - S3 batches, Azure sends one per iteration.
async fn run_startup_reconciliation_trims_tail_left_by_earlier_run(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_reconcile_job");

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = build_storage(harness, &job_registry, JobStateCodecKind::Json).await?;
    persist_completed_iterations(storage.as_ref(), &job_code, 1..=7).await?;

    let counting_storage = Arc::new(CountingStorage::new(Arc::clone(&storage)));
    let cleaner = JobCleaner::new(
        Arc::clone(&job_registry),
        Arc::clone(&counting_storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
    );
    let cancel_token = CancellationToken::new();
    let (_sender, receiver) = tokio::sync::mpsc::channel(1);
    let cleaner_token = cancel_token.clone();
    let cleaner_task = tokio::spawn(async move { cleaner.start(receiver, cleaner_token).await });

    // See the note above: the value lives across the join below, so it carries a `String`.
    let wait_result = wait_for_state_keys(harness, &job_code, &build_kept_keys(&job_code, 3..=7, ".json"))
        .await
        .map_err(|e| e.to_string());

    cancel_token.cancel();
    tokio::time::timeout(CLEANUP_TIMEOUT, cleaner_task)
        .await??
        .map_err(|e| format!("job cleaner stopped with error: {e}"))?;
    wait_result?;

    assert_eq!(counting_storage.find_meta_calls(), 1);
    assert_eq!(counting_storage.list_outdated_calls(), 1);
    assert_eq!(counting_storage.delete_iterations_calls(), 1);
    assert_eq!(counting_storage.deleted_iterations_total(), 2);

    Ok(())
}

/// A tail longer than one listing page has to be paged through whole: an iteration left off the
/// first page is one cleanup would never delete.
async fn run_list_outdated_iterations_pages_through_whole_tail(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_paging_job");

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = harness
        .build_storage(
            &ProviderStorageRequest::new(
                STATE_PREFIX,
                JobStateCodecKind::Json,
                Arc::clone(&job_registry) as Arc<dyn JobDefinitionRegistry>,
                Arc::new(NoopMetrics),
            )
            .with_list_page_size(2),
        )
        .await?;
    persist_completed_iterations(storage.as_ref(), &job_code, 1..=7).await?;

    let mut outdated_iter_nums = storage
        .list_job_outdated_iterations(&job_code, 5, &CancellationToken::new())
        .await?;
    outdated_iter_nums.sort_unstable();

    assert_eq!(outdated_iter_nums, vec![1, 2, 3, 4, 5]);
    Ok(())
}

/// State objects of another codec are not this backend's to delete: it cannot read them back, and
/// a pool that swapped codecs would otherwise erase the state the previous one wrote.
async fn run_list_outdated_iterations_ignores_states_of_another_codec(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_foreign_codec_job");

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let json_storage = build_storage(harness, &job_registry, JobStateCodecKind::Json).await?;
    persist_completed_iterations(json_storage.as_ref(), &job_code, 1..=3).await?;

    let cbor_storage = build_storage(harness, &job_registry, JobStateCodecKind::Cbor).await?;
    let outdated_iter_nums = cbor_storage
        .list_job_outdated_iterations(&job_code, 2, &CancellationToken::new())
        .await?;

    assert!(
        outdated_iter_nums.is_empty(),
        "states of another codec must not be reported as outdated: {outdated_iter_nums:?}"
    );
    assert_eq!(
        harness.read_state_keys(&build_job_prefix(&job_code)).await?,
        build_kept_keys(&job_code, 1..=3, ".json"),
        "states of another codec must stay untouched"
    );

    Ok(())
}

/// Deleting an iteration twice is the ordinary case - two instances reconcile the same tail - and
/// the second delete must not be an error.
async fn run_repeated_delete_iterations_succeeds(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_idempotent_job");

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = build_storage(harness, &job_registry, JobStateCodecKind::Json).await?;
    persist_completed_iterations(storage.as_ref(), &job_code, 1..=2).await?;

    let cancel_token = CancellationToken::new();
    storage.delete_job_iterations(&job_code, &[1], &cancel_token).await?;
    storage.delete_job_iterations(&job_code, &[1], &cancel_token).await?;

    assert_eq!(
        harness.read_state_keys(&build_job_prefix(&job_code)).await?,
        vec![build_state_key(&job_code, 2, ".json")]
    );

    Ok(())
}

/// The same idempotence over several iterations at once, which is the shape start-up
/// reconciliation asks for. A provider that batches the delete reports per-key outcomes inside a
/// success, and a key that was never there must not be reported as a failure.
async fn run_repeated_delete_of_absent_iterations_succeeds(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_idempotent_many_job");

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = build_storage(harness, &job_registry, JobStateCodecKind::Json).await?;
    persist_completed_iterations(storage.as_ref(), &job_code, 1..=3).await?;

    let cancel_token = CancellationToken::new();
    storage.delete_job_iterations(&job_code, &[1, 2], &cancel_token).await?;
    storage.delete_job_iterations(&job_code, &[1, 2], &cancel_token).await?;

    assert_eq!(
        harness.read_state_keys(&build_job_prefix(&job_code)).await?,
        vec![build_state_key(&job_code, 3, ".json")]
    );

    Ok(())
}

/// The delete of an iteration that is already gone is what makes cleanup idempotent, and it is
/// recorded as the success the caller asked for rather than as the refusal one provider answers it
/// with: S3 answers `204` where Azure answers `404`, and a status read off the answer alone would
/// report two instances reconciling one tail - the ordinary case - as a stream of failed deletes on
/// one provider and as clean work on the other.
///
/// The tail is written through a store of the test's own, so what the counters hold is the two
/// deletes alone, and the total is asserted beside the pair so a request under any other pair
/// cannot hide behind the named one.
///
/// Checked by breaking it: running the delete of `AzureBackend::delete_job_iterations` through
/// `send_request`, which records the answer the service gave, puts the second delete under
/// `404`.
async fn run_a_repeated_delete_is_recorded_as_a_delete_that_succeeded(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_recorded_delete_job");

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let tail_writer = build_storage(harness, &job_registry, JobStateCodecKind::Json).await?;
    persist_completed_iterations(tail_writer.as_ref(), &job_code, 1..=2).await?;

    let metrics = Arc::new(CountingMetrics::default());
    let storage = build_measured_storage(harness, &job_registry, &metrics).await?;
    let cancel_token = CancellationToken::new();
    storage.delete_job_iterations(&job_code, &[1], &cancel_token).await?;
    storage.delete_job_iterations(&job_code, &[1], &cancel_token).await?;

    assert_eq!(
        harness.read_state_keys(&build_job_prefix(&job_code)).await?,
        vec![build_state_key(&job_code, 2, ".json")],
        "the count is only about a pass whose first delete really removed the iteration"
    );
    assert_eq!(
        metrics.storage_operations("DELETE", "OK"),
        2,
        "a delete of an iteration that is already gone is the success cleanup asked for"
    );
    assert_eq!(
        metrics.storage_operations_total(),
        2,
        "and nothing was recorded under any other pair"
    );

    Ok(())
}

/// A deleted iteration must not make the job look absent: the restart has to continue from the
/// iteration the first run reached, never recreate the job from iteration 1.
///
/// Run under both codecs, because what a restart reads back is a state object written by the run
/// before it: a format whose round trip only holds within one process would pass every case that
/// never restarts.
async fn run_restarted_manager_continues_iterations_after_cleanup(
    harness: &dyn ProviderHarness,
    codec: JobStateCodecKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_restart_job");
    let extension = state_extension_of(codec);

    let first_job_def = build_job_definition(&job_code, 5)?.with_max_iterations(9)?;
    let first_registry = Arc::new(JobRegistry::new(vec![first_job_def.clone()])?);
    let first_storage = build_storage(harness, &first_registry, codec).await?;
    let manager_config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(50), Duration::from_millis(10)),
        ..Default::default()
    };

    let mut first_manager_env = ManagerEnv::new(
        first_storage,
        manager_config.clone(),
        Arc::clone(&first_registry),
        vec![first_job_def],
    )?;
    first_manager_env.wait_for_all_jobs_completion(CLEANUP_TIMEOUT).await?;

    wait_for_state_keys(harness, &job_code, &build_kept_keys(&job_code, 5..=9, extension)).await?;
    first_manager_env.stop().await;

    let second_job_def = build_job_definition(&job_code, 5)?.with_max_iterations(10)?;
    let second_registry = Arc::new(JobRegistry::new(vec![second_job_def.clone()])?);
    let second_storage = build_storage(harness, &second_registry, codec).await?;
    let mut second_manager_env = ManagerEnv::new(
        Arc::clone(&second_storage),
        manager_config,
        Arc::clone(&second_registry),
        vec![second_job_def],
    )?;
    second_manager_env.wait_for_all_jobs_completion(CLEANUP_TIMEOUT).await?;
    second_manager_env.stop().await;

    let job = second_storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(job.iter_num(), 10, "the restarted manager must continue the same job");

    let keys = harness.read_state_keys(&build_job_prefix(&job_code)).await?;
    assert!(
        !keys.contains(&build_state_key(&job_code, 1, extension)),
        "iteration 1 must not be recreated: {keys:?}"
    );

    Ok(())
}

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::{
        run_a_repeated_delete_is_recorded_as_a_delete_that_succeeded,
        run_concurrent_workers_report_each_started_iteration_once,
        run_list_outdated_iterations_ignores_states_of_another_codec,
        run_list_outdated_iterations_pages_through_whole_tail, run_repeated_delete_iterations_succeeds,
        run_repeated_delete_of_absent_iterations_succeeds, run_restarted_manager_continues_iterations_after_cleanup,
        run_startup_reconciliation_trims_tail_left_by_earlier_run,
    };
    use crate::JobStateCodecKind;
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_workers_report_each_started_iteration_once_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_concurrent_workers_report_each_started_iteration_once(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn startup_reconciliation_trims_tail_left_by_earlier_run_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_startup_reconciliation_trims_tail_left_by_earlier_run(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn list_outdated_iterations_pages_through_whole_tail_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_list_outdated_iterations_pages_through_whole_tail(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn list_outdated_iterations_ignores_states_of_another_codec_on_s3() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_list_outdated_iterations_ignores_states_of_another_codec(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn repeated_delete_iterations_succeeds_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_repeated_delete_iterations_succeeds(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn repeated_delete_of_absent_iterations_succeeds_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_repeated_delete_of_absent_iterations_succeeds(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn a_repeated_delete_is_recorded_as_a_delete_that_succeeded_on_s3() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_a_repeated_delete_is_recorded_as_a_delete_that_succeeded(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn restarted_manager_continues_json_iterations_after_cleanup_on_s3() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_restarted_manager_continues_iterations_after_cleanup(
            &S3ProviderHarness::start().await?,
            JobStateCodecKind::Json,
        )
        .await
    }

    #[tokio::test]
    async fn restarted_manager_continues_cbor_iterations_after_cleanup_on_s3() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_restarted_manager_continues_iterations_after_cleanup(
            &S3ProviderHarness::start().await?,
            JobStateCodecKind::Cbor,
        )
        .await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::{
        run_a_repeated_delete_is_recorded_as_a_delete_that_succeeded,
        run_concurrent_workers_report_each_started_iteration_once,
        run_list_outdated_iterations_ignores_states_of_another_codec,
        run_list_outdated_iterations_pages_through_whole_tail, run_repeated_delete_iterations_succeeds,
        run_repeated_delete_of_absent_iterations_succeeds, run_restarted_manager_continues_iterations_after_cleanup,
        run_startup_reconciliation_trims_tail_left_by_earlier_run,
    };
    use crate::JobStateCodecKind;
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_workers_report_each_started_iteration_once_on_azure() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_concurrent_workers_report_each_started_iteration_once(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn startup_reconciliation_trims_tail_left_by_earlier_run_on_azure() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_startup_reconciliation_trims_tail_left_by_earlier_run(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn list_outdated_iterations_pages_through_whole_tail_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_list_outdated_iterations_pages_through_whole_tail(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn list_outdated_iterations_ignores_states_of_another_codec_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_list_outdated_iterations_ignores_states_of_another_codec(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn repeated_delete_iterations_succeeds_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_repeated_delete_iterations_succeeds(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn repeated_delete_of_absent_iterations_succeeds_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_repeated_delete_of_absent_iterations_succeeds(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn a_repeated_delete_is_recorded_as_a_delete_that_succeeded_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_repeated_delete_is_recorded_as_a_delete_that_succeeded(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn restarted_manager_continues_json_iterations_after_cleanup_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_restarted_manager_continues_iterations_after_cleanup(
            &AzureProviderHarness::start().await?,
            JobStateCodecKind::Json,
        )
        .await
    }

    #[tokio::test]
    async fn restarted_manager_continues_cbor_iterations_after_cleanup_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_restarted_manager_continues_iterations_after_cleanup(
            &AzureProviderHarness::start().await?,
            JobStateCodecKind::Cbor,
        )
        .await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::{
        run_a_repeated_delete_is_recorded_as_a_delete_that_succeeded,
        run_concurrent_workers_report_each_started_iteration_once,
        run_list_outdated_iterations_ignores_states_of_another_codec,
        run_list_outdated_iterations_pages_through_whole_tail, run_repeated_delete_iterations_succeeds,
        run_repeated_delete_of_absent_iterations_succeeds, run_restarted_manager_continues_iterations_after_cleanup,
        run_startup_reconciliation_trims_tail_left_by_earlier_run,
    };
    use crate::JobStateCodecKind;
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_workers_report_each_started_iteration_once_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_concurrent_workers_report_each_started_iteration_once(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn startup_reconciliation_trims_tail_left_by_earlier_run_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_startup_reconciliation_trims_tail_left_by_earlier_run(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn list_outdated_iterations_pages_through_whole_tail_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_list_outdated_iterations_pages_through_whole_tail(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn list_outdated_iterations_ignores_states_of_another_codec_on_gcs() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_list_outdated_iterations_ignores_states_of_another_codec(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn repeated_delete_iterations_succeeds_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_repeated_delete_iterations_succeeds(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn repeated_delete_of_absent_iterations_succeeds_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_repeated_delete_of_absent_iterations_succeeds(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn a_repeated_delete_is_recorded_as_a_delete_that_succeeded_on_gcs() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_a_repeated_delete_is_recorded_as_a_delete_that_succeeded(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test]
    async fn restarted_manager_continues_json_iterations_after_cleanup_on_gcs() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_restarted_manager_continues_iterations_after_cleanup(
            &GcsProviderHarness::start().await?,
            JobStateCodecKind::Json,
        )
        .await
    }

    #[tokio::test]
    async fn restarted_manager_continues_cbor_iterations_after_cleanup_on_gcs() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_restarted_manager_continues_iterations_after_cleanup(
            &GcsProviderHarness::start().await?,
            JobStateCodecKind::Cbor,
        )
        .await
    }
}
