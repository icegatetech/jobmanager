use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::counting_metrics::CountingMetrics;
use super::common::manager_env::ManagerEnv;
use super::common::meta_of;
use super::common::s3_container::S3TestContainer;
use super::common::storage_wrapper::ContendingStorage;
use super::common::waiting::{
    CONDITION_TIMEOUT, OBSERVATION_WINDOW, measure_settled_requests, wait_until, wait_until_job_is_processed,
};
use crate::{
    CachedStorage, Job, JobCleanerConfig, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus,
    JobsManagerConfig, MetricsSink, NoopMetrics, S3Storage, S3StorageConfig, Storage, TaskCode, TaskDefinition,
    TaskExecutor, TaskLimits, TaskOutcome, task_fn,
};

/// Interval the pools below poll at: short enough that an observation window holds many passes, so
/// a request issued per pass would be counted many times over.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

fn build_job_definition(job_code: &JobCode, executor: Arc<dyn TaskExecutor>) -> Result<JobDefinition, crate::Error> {
    JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            TaskDefinition::new(TaskCode::from("polled"), Duration::from_secs(30)),
            executor,
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )
}

fn build_s3_config(container: &S3TestContainer, bucket_prefix: &str) -> S3StorageConfig {
    S3StorageConfig::new(
        container.endpoint(),
        container.username(),
        container.password(),
        "poll-request-quota",
        "us-east-1",
    )
    .with_bucket_prefix(bucket_prefix)
}

/// The real store the pool is billed through, with every request it makes recorded by `metrics`.
///
/// Kept apart from [`start_pool_on`] so a scenario can put a double between this store and the
/// read cache without the double's own requests landing in the count.
async fn build_measured_store(
    container: &S3TestContainer,
    bucket_prefix: &str,
    job_registry: &Arc<JobRegistry>,
    metrics: &Arc<CountingMetrics>,
) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
    let storage = S3Storage::new(
        build_s3_config(container, bucket_prefix),
        Arc::clone(job_registry) as Arc<dyn crate::JobDefinitionRegistry>,
        Arc::clone(metrics) as Arc<dyn MetricsSink>,
    )
    .await?;

    Ok(Arc::new(storage) as Arc<dyn Storage>)
}

/// The pool under test: `store` behind the read cache.
///
/// `worker_count` stays an argument rather than a constant: how many workers reach for one job is
/// what decides whether a quota is an exact number or a range, so a scenario states it.
fn start_pool_on(
    store: Arc<dyn Storage>,
    worker_count: usize,
    job_registry: Arc<JobRegistry>,
    job_def: JobDefinition,
) -> Result<ManagerEnv, Box<dyn std::error::Error>> {
    ManagerEnv::new(
        Arc::new(CachedStorage::new(store, Arc::new(NoopMetrics))) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count,
            worker_config: super::common::build_worker_config(POLL_INTERVAL, Duration::ZERO),
            cleaner_config: JobCleanerConfig {
                enabled: false,
                ..Default::default()
            },
        },
        job_registry,
        vec![job_def],
    )
}

/// A pool reaching the measured store directly, which is what every scenario but the contended one
/// wants.
async fn start_pool(
    container: &S3TestContainer,
    bucket_prefix: &str,
    worker_count: usize,
    job_def: JobDefinition,
    metrics: &Arc<CountingMetrics>,
) -> Result<ManagerEnv, Box<dyn std::error::Error>> {
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let store = build_measured_store(container, bucket_prefix, &job_registry, metrics).await?;

    start_pool_on(store, worker_count, job_registry, job_def)
}

/// Reader of the store belonging to the test itself, so proving what the pool did costs nothing the
/// pool is measured by.
async fn build_state_probe(
    container: &S3TestContainer,
    bucket_prefix: &str,
    job_def: &JobDefinition,
) -> Result<S3Storage, Box<dyn std::error::Error>> {
    Ok(S3Storage::new(
        build_s3_config(container, bucket_prefix),
        Arc::new(JobRegistry::new(vec![job_def.clone()])?),
        Arc::new(NoopMetrics),
    )
    .await?)
}

/// Run quota of [`Storage::get_changed_job`] itself, below the pool that calls it: an iteration
/// that did not move costs the one conditional `GET` the store answers `304`, and no listing at
/// all. The save that sets the scenario up is the only other request, so the total is two.
///
/// Checked by reading without the `If-None-Match` the state was taken under: the store then answers
/// with the object and the pair the number is counted under stops being reached.
#[tokio::test]
async fn a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let container = S3TestContainer::start().await?;
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("measured_read_job");
    let job_def = build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let store = build_measured_store(&container, "measured-read", &job_registry, &metrics).await?;
    let mut job = Job::new(&job_def, HashMap::new(), Uuid::from_u128(1))?;
    store.save_job(&mut job, &CancellationToken::new()).await?;

    let read = store.get_changed_job(&meta_of(&job), &CancellationToken::new()).await?;

    assert!(
        read.is_none(),
        "the scenario is about a state that did not move, got {:?}",
        read.as_ref().map(Job::version)
    );
    assert_eq!(
        metrics.storage_operations("GET", "304"),
        1,
        "an unmoved state must be recorded as a conditional GET that returned 304"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        0,
        "a conditional read must not list anything"
    );
    assert_eq!(
        metrics.storage_operations_total(),
        2,
        "and must add nothing to the save the scenario is set up with"
    );
    Ok(())
}

/// FR1 as a steady-state quota on the store that is actually billed: while an iteration is running,
/// a pass over it costs a conditional `GET` answered `304` and nothing else. The number is what a
/// window of any length may add to every other pair, which is zero - no `LIST`, no write, no
/// unconditional read - while the conditional reads themselves go on for as long as the iteration
/// does. Counted through the storage metric rather than through trait calls, because a backend
/// turns one call into as many requests as its retries and the SDK's take.
///
/// Checked by reading without the `If-None-Match` the state was taken under: the answers stop being
/// `304` and land in the very class this bounds, which then grows across the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn polling_a_running_iteration_costs_no_request_besides_a_conditional_read()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let container = S3TestContainer::start().await?;
    let metrics = Arc::new(CountingMetrics::default());
    let is_task_started = Arc::new(AtomicBool::new(false));
    let is_task_released = Arc::new(AtomicBool::new(false));

    let started_flag = Arc::clone(&is_task_started);
    let released_flag = Arc::clone(&is_task_released);
    // The task holds the iteration open for the whole window, so every pass of the other worker
    // meets a running iteration - the state the saving is about.
    let job_def = build_job_definition(
        &JobCode::new("running_iteration_job"),
        task_fn(move |_ctx| {
            let started_flag = Arc::clone(&started_flag);
            let released_flag = Arc::clone(&released_flag);
            async move {
                started_flag.store(true, Ordering::SeqCst);
                while !released_flag.load(Ordering::SeqCst) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                Ok(TaskOutcome::empty())
            }
        }),
    )?;
    let _env = start_pool(&container, "running", 2, job_def, &metrics).await?;

    wait_until(|| is_task_started.load(Ordering::SeqCst), "the iteration is running").await?;
    // Every request but the conditional read is what the scenario bounds, so that is what has to
    // settle: the conditional reads go on for as long as the iteration runs, which is the whole
    // point.
    let other_requests_before = measure_settled_requests(
        || metrics.storage_operations_besides("GET", "304"),
        "the passes that started the iteration have issued their requests",
    )
    .await?;
    let conditional_reads_before = metrics.storage_operations("GET", "304");
    tokio::time::sleep(OBSERVATION_WINDOW).await;
    let other_requests_after = metrics.storage_operations_besides("GET", "304");
    let conditional_reads_after = metrics.storage_operations("GET", "304");
    is_task_released.store(true, Ordering::SeqCst);

    assert_eq!(
        other_requests_after, other_requests_before,
        "polling a running iteration must cost nothing besides a conditional read, \
         got {other_requests_before} -> {other_requests_after} requests of every other kind"
    );
    assert!(
        conditional_reads_after > conditional_reads_before,
        "and must cost that read: {conditional_reads_before} -> {conditional_reads_after}"
    );
    Ok(())
}

/// Iteration interval of the job below: far longer than the window it is watched over, so the next
/// iteration cannot become due while the counters are read.
const LONG_ITERATION_INTERVAL: Duration = Duration::from_mins(5);

/// FR2 as a steady-state quota on the store that is actually billed: a job whose iteration ended
/// and whose next one is not due may add nothing at all across a window - no listing, no
/// conditional read, no write.
///
/// Checked by letting a pass reach the job whatever its next iteration is due at - both the wait
/// between passes and the poll gate, because either one alone still holds the pass back. The total
/// then more than doubles across the window.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let container = S3TestContainer::start().await?;
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("waiting_iteration_job");
    let job_def = build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?
        .with_iteration_interval(LONG_ITERATION_INTERVAL)?;
    let probe = build_state_probe(&container, "waiting", &job_def).await?;
    let _env = start_pool(&container, "waiting", 2, job_def, &metrics).await?;

    wait_until_job_is_processed(&probe, &job_code, &CancellationToken::new()).await?;
    let requests_before = measure_settled_requests(
        || metrics.storage_operations_total(),
        "the passes already in flight have issued their requests",
    )
    .await?;
    tokio::time::sleep(OBSERVATION_WINDOW).await;

    assert_eq!(
        metrics.storage_operations_total(),
        requests_before,
        "a job waiting for its next iteration must not be polled at all"
    );
    Ok(())
}

/// Run quota of one job from an empty store to a finished iteration: five requests, made of three
/// writes - the job created, its task started, its task finished together with the iteration - and
/// two listings, the one that finds the store empty and the one the pass after the iteration
/// answers with. Nothing is read at all: the pass that follows a write is served by the cache that
/// write filled, and the last pass recognises the listed version as the one it already holds.
///
/// One worker and one task are what make this a number rather than a range: a second worker races
/// for the same task and pays a rejected write for losing, and a second task takes a pass of its
/// own. The iteration budget of one is what makes the total settle - the pool stops polling a job
/// it has run out, so a request that should not happen has nowhere to hide.
///
/// Checked by letting a pass run before the moment it was scheduled for: the finished iteration is
/// discovered a second time and the listings go from two to three.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn running_a_single_task_job_once_costs_five_requests() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let container = S3TestContainer::start().await?;
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("single_run_job");
    let job_def =
        build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?.with_max_iterations(1)?;
    let probe = build_state_probe(&container, "single-run", &job_def).await?;
    let env = start_pool(&container, "single-run", 1, job_def, &metrics).await?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    let requests = measure_settled_requests(
        || metrics.storage_operations_total(),
        "the pool has stopped polling the job whose iteration budget is spent",
    )
    .await?;

    let stored_status = probe.get_job(&job_code, &CancellationToken::new()).await?.status().clone();
    assert_eq!(
        stored_status,
        JobStatus::Completed,
        "the budget is only about a run that reached the end of its iteration"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "OK"),
        3,
        "creating the job, starting its task and finishing it are the only writes a single run makes"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        2,
        "the empty store and the finished iteration are the only two states that cost a listing"
    );
    assert_eq!(
        requests, 5,
        "and nothing else is billed: no read, no rejected write, no failed request"
    );
    Ok(())
}

/// Run quota of the same job when one race is lost: eight requests, the five an uncontended run
/// pays plus the rejected write itself and the listing and read that re-discover the state it was
/// rejected against. The retry is not part of the price: it is the write the run owed anyway.
///
/// The race is staged rather than waited for: the rival writes over the stored state right before
/// the pool saves the task it just picked up, so the version the pool holds is stale by exactly one
/// write. The rival reaches the store through one of its own, so what the counters hold is what the
/// pool alone is billed for.
///
/// Checked by letting a pass run before the moment it was scheduled for: the finished iteration is
/// discovered a second time and the listings go from three to four.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_losing_one_race_costs_eight_requests() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let container = S3TestContainer::start().await?;
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("contended_run_job");
    let job_def =
        build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?.with_max_iterations(1)?;
    let probe = build_state_probe(&container, "contended", &job_def).await?;
    let rival_store = build_state_probe(&container, "contended", &job_def).await?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let contending = Arc::new(
        ContendingStorage::new(build_measured_store(&container, "contended", &job_registry, &metrics).await?)
            .with_rival_store(Arc::new(rival_store) as Arc<dyn Storage>),
    );
    let env = start_pool_on(Arc::clone(&contending) as Arc<dyn Storage>, 1, job_registry, job_def)?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    let requests = measure_settled_requests(
        || metrics.storage_operations_total(),
        "the pool has stopped polling the job whose iteration budget is spent",
    )
    .await?;

    assert_eq!(
        contending.interferences(),
        1,
        "the fixture must have made the pool lose exactly one race"
    );
    let stored_status = probe.get_job(&job_code, &CancellationToken::new()).await?.status().clone();
    assert_eq!(
        stored_status,
        JobStatus::Completed,
        "a lost race must not keep the run from reaching the end of its iteration"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "412"),
        1,
        "the write the rival got in ahead of is the only one refused"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        3,
        "the refusal empties the cache, so the state has to be discovered once more"
    );
    assert_eq!(
        metrics.storage_operations("GET", "OK"),
        1,
        "and read once more, which an uncontended run never does"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "OK"),
        3,
        "the retry replaces the refused write rather than adding to the writes a run makes"
    );
    assert_eq!(requests, 8, "and nothing else is billed for the race");
    Ok(())
}
