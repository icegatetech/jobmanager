use std::{collections::HashMap, sync::Arc, time::Duration};

use aws_sdk_s3::Client;
use chrono::{Duration as ChronoDuration, Utc};
use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::manager_env::ManagerEnv;
use super::common::s3_container::S3TestContainer;
use super::common::storage_wrapper::CountingStorage;
use crate::{
    Job, JobCleaner, JobCleanerConfig, JobCode, JobDefinition, JobRegistry, JobStateCodecKind, JobStatus,
    JobsManagerConfig, Metrics, S3Storage, S3StorageConfig, Storage, StorageError, TaskCode, TaskDefinition,
    TaskExecutorFn, TaskLimits, Worker, WorkerConfig,
};

/// Bound on every wait: iteration cleanup is asynchronous, so tests poll for its effect.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(20);

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

fn build_state_key(bucket_prefix: &str, job_code: &JobCode, iter_num: usize, extension: &str) -> String {
    format!(
        "{bucket_prefix}/{job_code}/state-{}{extension}",
        INVERTED_ITER_NUMS[iter_num]
    )
}

fn build_job_definition(
    job_code: &JobCode,
    iteration_retention: u64,
) -> Result<JobDefinition, Box<dyn std::error::Error>> {
    let executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
        let task_id = *task.id();
        Box::pin(async move { manager.complete_task(&task_id, b"done".to_vec()) })
    });
    let task_def = TaskDefinition::new(TaskCode::new("cleanup_task"), Vec::new(), ChronoDuration::seconds(5))?;
    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("cleanup_task"), executor);

    Ok(JobDefinition::new(job_code.clone(), vec![task_def], executors)?
        .with_iteration_retention(iteration_retention)?)
}

fn build_s3_config(
    store: &S3TestContainer,
    bucket_name: &str,
    bucket_prefix: &str,
    codec: JobStateCodecKind,
) -> S3StorageConfig {
    S3StorageConfig::new(
        store.endpoint(),
        store.username(),
        store.password(),
        bucket_name,
        "us-east-1",
    )
    .with_bucket_prefix(bucket_prefix)
    .with_job_state_codec(codec)
}

async fn build_s3_storage(
    store: &S3TestContainer,
    bucket_name: &str,
    bucket_prefix: &str,
    codec: JobStateCodecKind,
    job_registry: Arc<JobRegistry>,
) -> Result<S3Storage, Box<dyn std::error::Error>> {
    Ok(S3Storage::new(
        build_s3_config(store, bucket_name, bucket_prefix, codec),
        job_registry,
        Metrics::new_disabled(),
    )
    .await?)
}

/// S3 client of the test itself, used to inspect the bucket without going through the code under
/// test.
async fn build_bucket_reader(store: &S3TestContainer) -> Client {
    let credentials =
        aws_sdk_s3::config::Credentials::new(store.username(), store.password(), None, None, "test-reader");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(credentials)
        .load()
        .await;
    let s3_config = aws_sdk_s3::config::Builder::from(&sdk_config)
        .endpoint_url(store.endpoint().to_string())
        .force_path_style(true)
        .build();

    Client::from_conf(s3_config)
}

async fn read_bucket_keys(
    reader: &Client,
    bucket_name: &str,
    prefix: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut keys = Vec::new();
    let mut continuation_token = None;

    loop {
        let output = reader
            .list_objects_v2()
            .bucket(bucket_name)
            .prefix(prefix)
            .set_continuation_token(continuation_token)
            .send()
            .await?;

        keys.extend(output.contents().iter().filter_map(|object| object.key().map(String::from)));

        continuation_token = output.next_continuation_token().map(String::from);
        if continuation_token.is_none() {
            break;
        }
    }

    keys.sort();
    Ok(keys)
}

/// Persists `iter_nums` as completed iterations of `job_code` without running a manager, so a test
/// can start from a job that already has a tail.
async fn persist_completed_iterations(
    storage: &dyn Storage,
    job_code: &JobCode,
    iter_nums: impl IntoIterator<Item = u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cancel_token = CancellationToken::new();
    let job_id = Uuid::from_u128(7_001);
    let worker_id = Uuid::from_u128(7_002);

    for iter_num in iter_nums {
        let mut job = Job::restore(
            job_id,
            job_code.clone(),
            String::new(),
            iter_num,
            JobStatus::Completed,
            Vec::new(),
            worker_id,
            Utc::now(),
            None,
            Some(Utc::now()),
            None,
            HashMap::new(),
            None,
            None,
            TaskLimits::default(),
        );
        storage.save_job(&mut job, &cancel_token).await?;
    }

    Ok(())
}

async fn wait_for_bucket_keys(
    reader: &Client,
    bucket_name: &str,
    prefix: &str,
    expected_keys: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + CLEANUP_TIMEOUT;
    loop {
        let keys = read_bucket_keys(reader, bucket_name, prefix).await?;
        if keys == expected_keys {
            return Ok(());
        }
        if tokio::time::Instant::now() > deadline {
            return Err(format!("timeout waiting for bucket keys {expected_keys:?}, found {keys:?}").into());
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

/// Only the worker that persisted the start of an iteration reports it, so however many workers
/// raced for that iteration, the cleaner hears about it once.
///
/// The workers are started by hand rather than through `JobsManager`, and the report channel is
/// read directly instead of being handed to a `JobCleaner`: a duplicate report would otherwise
/// only show up as an extra asynchronous delete, which
/// [`ManagerEnv::stop`](super::common::manager_env::ManagerEnv::stop) can discard before the
/// assertion runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_workers_report_each_started_iteration_once() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let bucket_name = "cleanup-single-report";
    let bucket_prefix = "jobs";
    let job_code = JobCode::new("cleanup_single_report_job");
    let max_iterations = 9;
    let store = S3TestContainer::start().await?;

    let job_def = build_job_definition(&job_code, 5)?.with_max_iterations(max_iterations)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = Arc::new(CountingStorage::new(Arc::new(
        build_s3_storage(
            &store,
            bucket_name,
            bucket_prefix,
            JobStateCodecKind::Json,
            Arc::clone(&job_registry),
        )
        .await?,
    ) as Arc<dyn Storage>));

    // Wider than any legal number of reports, so a duplicate cannot be swallowed by a full channel.
    let (sender, mut receiver) = mpsc::channel(64);
    let cancel_token = CancellationToken::new();
    let mut worker_tasks = JoinSet::new();
    for _ in 0..3 {
        let worker = Worker::new(
            Arc::clone(&job_registry),
            Arc::clone(&storage) as Arc<dyn Storage>,
            WorkerConfig {
                poll_interval: Duration::from_millis(20),
                poll_interval_randomization: Duration::from_millis(0),
                ..Default::default()
            },
            Metrics::new_disabled(),
            Some(sender.clone()),
        );
        let worker_token = cancel_token.clone();
        worker_tasks.spawn(async move { worker.start(worker_token).await });
    }

    let wait_result = wait_for_processed_iteration(storage.as_ref(), &job_code, max_iterations).await;
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

#[tokio::test]
async fn test_startup_reconciliation_trims_tail_left_by_earlier_run() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let bucket_name = "cleanup-reconcile";
    let bucket_prefix = "jobs";
    let job_code = JobCode::new("cleanup_reconcile_job");
    let store = S3TestContainer::start().await?;

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = Arc::new(
        build_s3_storage(
            &store,
            bucket_name,
            bucket_prefix,
            JobStateCodecKind::Json,
            Arc::clone(&job_registry),
        )
        .await?,
    );
    persist_completed_iterations(storage.as_ref(), &job_code, 1..=7).await?;

    let counting_storage = Arc::new(CountingStorage::new(Arc::clone(&storage) as Arc<dyn Storage>));
    let cleaner = JobCleaner::new(
        Arc::clone(&job_registry),
        Arc::clone(&counting_storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
    );
    let cancel_token = CancellationToken::new();
    let (_sender, receiver) = tokio::sync::mpsc::channel(1);
    let cleaner_token = cancel_token.clone();
    let cleaner_task = tokio::spawn(async move { cleaner.start(receiver, cleaner_token).await });

    let reader = build_bucket_reader(&store).await;
    let expected_keys: Vec<String> = (3..=7)
        .rev()
        .map(|iter_num| build_state_key(bucket_prefix, &job_code, iter_num, ".json"))
        .collect();
    let wait_result = wait_for_bucket_keys(
        &reader,
        bucket_name,
        &format!("{bucket_prefix}/{job_code}/"),
        &expected_keys,
    )
    .await;

    cancel_token.cancel();
    tokio::time::timeout(CLEANUP_TIMEOUT, cleaner_task)
        .await??
        .map_err(|e| format!("job cleaner stopped with error: {e}"))?;
    wait_result?;

    // The whole request budget of reconciling one job: two LISTs - `find_job_meta` for the current
    // iteration, then one listing of the tail - and one batched delete for every outdated
    // iteration. Both LISTs are the price of the start-up pass only; the steady state pays none
    // (see `test_started_iterations_delete_one_iteration_each_without_listing`).
    assert_eq!(counting_storage.find_meta_calls(), 1);
    assert_eq!(counting_storage.list_outdated_calls(), 1);
    assert_eq!(counting_storage.delete_iterations_calls(), 1);
    assert_eq!(counting_storage.deleted_iterations_total(), 2);

    Ok(())
}

#[tokio::test]
async fn test_list_outdated_iterations_pages_through_whole_tail() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let bucket_name = "cleanup-paging";
    let bucket_prefix = "jobs";
    let job_code = JobCode::new("cleanup_paging_job");
    let store = S3TestContainer::start().await?;

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = Arc::new(
        S3Storage::new(
            build_s3_config(&store, bucket_name, bucket_prefix, JobStateCodecKind::Json).with_list_page_size(2)?,
            job_registry.clone(),
            Metrics::new_disabled(),
        )
        .await?,
    );
    persist_completed_iterations(storage.as_ref(), &job_code, 1..=7).await?;

    let mut outdated_iter_nums = storage
        .list_job_outdated_iterations(&job_code, 5, &CancellationToken::new())
        .await?;
    outdated_iter_nums.sort_unstable();

    assert_eq!(outdated_iter_nums, vec![1, 2, 3, 4, 5]);
    Ok(())
}

#[tokio::test]
async fn test_delete_iterations_deletes_every_chunk_and_keeps_the_rest_json() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();

    run_delete_iterations_deletes_every_chunk_and_keeps_the_rest(
        "cleanup-chunking-json",
        JobStateCodecKind::Json,
        ".json",
    )
    .await
}

/// The state objects of a `Cbor` job carry a different extension, which both the key builder and
/// the cleanup filter have to agree on.
#[tokio::test]
async fn test_delete_iterations_deletes_every_chunk_and_keeps_the_rest_cbor() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();

    run_delete_iterations_deletes_every_chunk_and_keeps_the_rest(
        "cleanup-chunking-cbor",
        JobStateCodecKind::Cbor,
        ".cbor",
    )
    .await
}

async fn run_delete_iterations_deletes_every_chunk_and_keeps_the_rest(
    bucket_name: &str,
    codec: JobStateCodecKind,
    extension: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let bucket_prefix = "jobs";
    let job_code = JobCode::new("cleanup_chunking_job");
    let store = S3TestContainer::start().await?;

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = Arc::new(
        S3Storage::new(
            build_s3_config(&store, bucket_name, bucket_prefix, codec).with_delete_batch_size(2)?,
            job_registry.clone(),
            Metrics::new_disabled(),
        )
        .await?,
    );
    persist_completed_iterations(storage.as_ref(), &job_code, 1..=7).await?;

    storage
        .delete_job_iterations(&job_code, &[1, 2, 3, 4, 5], &CancellationToken::new())
        .await?;

    let reader = build_bucket_reader(&store).await;
    let keys = read_bucket_keys(&reader, bucket_name, &format!("{bucket_prefix}/{job_code}/")).await?;
    let expected_keys: Vec<String> = (6..=7)
        .rev()
        .map(|iter_num| build_state_key(bucket_prefix, &job_code, iter_num, extension))
        .collect();

    assert_eq!(keys, expected_keys);
    Ok(())
}

#[tokio::test]
async fn test_list_outdated_iterations_ignores_states_of_another_codec() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let bucket_name = "cleanup-foreign-codec";
    let bucket_prefix = "jobs";
    let job_code = JobCode::new("cleanup_foreign_codec_job");
    let store = S3TestContainer::start().await?;

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let json_storage = build_s3_storage(
        &store,
        bucket_name,
        bucket_prefix,
        JobStateCodecKind::Json,
        Arc::clone(&job_registry),
    )
    .await?;
    persist_completed_iterations(&json_storage, &job_code, 1..=3).await?;

    let cbor_storage = build_s3_storage(
        &store,
        bucket_name,
        bucket_prefix,
        JobStateCodecKind::Cbor,
        Arc::clone(&job_registry),
    )
    .await?;
    let outdated_iter_nums = cbor_storage
        .list_job_outdated_iterations(&job_code, 2, &CancellationToken::new())
        .await?;

    assert!(
        outdated_iter_nums.is_empty(),
        "states of another codec must not be reported as outdated: {outdated_iter_nums:?}"
    );

    let reader = build_bucket_reader(&store).await;
    let keys = read_bucket_keys(&reader, bucket_name, &format!("{bucket_prefix}/{job_code}/")).await?;
    let expected_keys: Vec<String> = (1..=3)
        .rev()
        .map(|iter_num| build_state_key(bucket_prefix, &job_code, iter_num, ".json"))
        .collect();
    assert_eq!(keys, expected_keys, "states of another codec must stay untouched");

    Ok(())
}

#[tokio::test]
async fn test_repeated_delete_iterations_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let bucket_name = "cleanup-idempotent";
    let bucket_prefix = "jobs";
    let job_code = JobCode::new("cleanup_idempotent_job");
    let store = S3TestContainer::start().await?;

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = build_s3_storage(
        &store,
        bucket_name,
        bucket_prefix,
        JobStateCodecKind::Json,
        Arc::clone(&job_registry),
    )
    .await?;
    persist_completed_iterations(&storage, &job_code, 1..=2).await?;

    let cancel_token = CancellationToken::new();
    storage.delete_job_iterations(&job_code, &[1], &cancel_token).await?;
    storage.delete_job_iterations(&job_code, &[1], &cancel_token).await?;

    let reader = build_bucket_reader(&store).await;
    let keys = read_bucket_keys(&reader, bucket_name, &format!("{bucket_prefix}/{job_code}/")).await?;
    assert_eq!(keys, vec![build_state_key(bucket_prefix, &job_code, 2, ".json")]);

    Ok(())
}

/// Two instances reconciling the same job at start-up both delete the same tail, so the second
/// multi-object delete names keys that are already gone. Unlike the single-key path, this one
/// inspects the response body for per-key failures, and a key that was never there must not be
/// reported as one.
#[tokio::test]
async fn test_repeated_batch_delete_of_absent_iterations_succeeds() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let bucket_name = "cleanup-idempotent-batch";
    let bucket_prefix = "jobs";
    let job_code = JobCode::new("cleanup_idempotent_batch_job");
    let store = S3TestContainer::start().await?;

    let job_def = build_job_definition(&job_code, 5)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = build_s3_storage(
        &store,
        bucket_name,
        bucket_prefix,
        JobStateCodecKind::Json,
        Arc::clone(&job_registry),
    )
    .await?;
    persist_completed_iterations(&storage, &job_code, 1..=3).await?;

    let cancel_token = CancellationToken::new();
    storage.delete_job_iterations(&job_code, &[1, 2], &cancel_token).await?;
    storage.delete_job_iterations(&job_code, &[1, 2], &cancel_token).await?;

    let reader = build_bucket_reader(&store).await;
    let keys = read_bucket_keys(&reader, bucket_name, &format!("{bucket_prefix}/{job_code}/")).await?;
    assert_eq!(keys, vec![build_state_key(bucket_prefix, &job_code, 3, ".json")]);

    Ok(())
}

#[tokio::test]
async fn test_restarted_manager_continues_iterations_after_cleanup() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let bucket_name = "cleanup-restart";
    let bucket_prefix = "jobs";
    let job_code = JobCode::new("cleanup_restart_job");
    let store = S3TestContainer::start().await?;

    let first_job_def = build_job_definition(&job_code, 5)?.with_max_iterations(9)?;
    let first_registry = Arc::new(JobRegistry::new(vec![first_job_def.clone()])?);
    let first_storage = build_s3_storage(
        &store,
        bucket_name,
        bucket_prefix,
        JobStateCodecKind::Json,
        Arc::clone(&first_registry),
    )
    .await?;
    let manager_config = JobsManagerConfig {
        worker_count: 1,
        worker_config: WorkerConfig {
            poll_interval: Duration::from_millis(50),
            poll_interval_randomization: Duration::from_millis(10),
            ..Default::default()
        },
        ..Default::default()
    };

    let mut first_manager_env = ManagerEnv::new(
        Arc::new(first_storage),
        manager_config.clone(),
        Arc::clone(&first_registry),
        vec![first_job_def],
    )?;
    first_manager_env.wait_for_all_jobs_completion(CLEANUP_TIMEOUT).await?;

    let reader = build_bucket_reader(&store).await;
    let bucket_job_prefix = format!("{bucket_prefix}/{job_code}/");
    let trimmed_keys: Vec<String> = (5..=9)
        .rev()
        .map(|iter_num| build_state_key(bucket_prefix, &job_code, iter_num, ".json"))
        .collect();
    wait_for_bucket_keys(&reader, bucket_name, &bucket_job_prefix, &trimmed_keys).await?;
    first_manager_env.stop().await;

    // A deleted iteration must not make the job look absent: the restart has to continue from
    // iteration 9, never recreate the job from iteration 1.
    let second_job_def = build_job_definition(&job_code, 5)?.with_max_iterations(10)?;
    let second_registry = Arc::new(JobRegistry::new(vec![second_job_def.clone()])?);
    let second_storage = Arc::new(
        build_s3_storage(
            &store,
            bucket_name,
            bucket_prefix,
            JobStateCodecKind::Json,
            Arc::clone(&second_registry),
        )
        .await?,
    );
    let mut second_manager_env = ManagerEnv::new(
        Arc::clone(&second_storage) as Arc<dyn Storage>,
        manager_config,
        Arc::clone(&second_registry),
        vec![second_job_def],
    )?;
    second_manager_env.wait_for_all_jobs_completion(CLEANUP_TIMEOUT).await?;
    second_manager_env.stop().await;

    let job = second_storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(job.iter_num(), 10, "the restarted manager must continue the same job");

    let keys = read_bucket_keys(&reader, bucket_name, &bucket_job_prefix).await?;
    assert!(
        !keys.contains(&build_state_key(bucket_prefix, &job_code, 1, ".json")),
        "iteration 1 must not be recreated: {keys:?}"
    );

    Ok(())
}
