use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::manager_env::ManagerEnv;
use super::common::storage_wrapper::{
    CacheRacingStorage, ConditionalReadFailingStorage, CountingStorage, RendezvousReadStorage, UnconditionalReadStorage,
};
use super::common::waiting::{OBSERVATION_WINDOW, measure_settled_requests, wait_until_job_is_processed};
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    CachedStorage, Job, JobCleanerConfig, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobsManagerConfig,
    NoopMetrics, Storage, StorageError, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, TaskPickup, task_fn,
};

fn job_definition(job_code: &JobCode) -> Result<JobDefinition, crate::Error> {
    JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            TaskDefinition::new(TaskCode::from("polled"), Duration::from_secs(5)),
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )
}

/// FR1: once a running iteration is cached, checking it again reaches the backend through a
/// conditional read and nothing else.
///
/// Counted at the `Storage` boundary, which makes this a test of what the cache lets through rather
/// than of what the store bills - the quota for that is in [`request_quota_test`](super).
#[tokio::test]
async fn a_cached_running_iteration_is_checked_by_a_conditional_read_alone() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let counting = Arc::new(CountingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let cached = CachedStorage::new(Arc::clone(&counting) as Arc<dyn Storage>, Arc::new(NoopMetrics));
    let cancel_token = CancellationToken::new();
    let job_code = JobCode::new("running_job");
    let worker_id = Uuid::from_u128(1);

    // Written past the cache, as another worker's write would be, so the first read below really is
    // the cold one - a save through the cache would have filled it.
    let mut job = Job::new(&job_definition(&job_code)?, HashMap::new(), worker_id)?;
    job.work(&worker_id)?;
    counting.save_job(&mut job, &cancel_token).await?;

    // Warm-up: the first read discovers the iteration and fetches it.
    read_job_within_bound(&cached, &job_code, &cancel_token).await?;
    assert_eq!(counting.find_meta_calls(), 1);
    assert_eq!(counting.get_by_meta_calls(), 1);
    assert_eq!(counting.conditional_read_calls(), 0);

    for _ in 0..5 {
        read_job_within_bound(&cached, &job_code, &cancel_token).await?;
    }

    assert_eq!(
        counting.find_meta_calls(),
        1,
        "a cached running iteration must not be listed again"
    );
    assert_eq!(counting.get_by_meta_calls(), 1, "and must not be fetched again");
    assert_eq!(counting.conditional_read_calls(), 5);
    assert_eq!(counting.unchanged_reads(), 5, "an unmoved state must read as unchanged");
    Ok(())
}

/// FR2: a completed job whose next iteration is not due is not reached for at all - not a listing,
/// not a conditional read.
///
/// Counted at the `Storage` boundary, which makes this a test of what the pass decides rather than
/// of what the store bills - the quota for that is in [`request_quota_test`](super).
#[tokio::test]
async fn a_job_waiting_for_its_next_iteration_is_not_polled() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let store = Arc::new(InMemoryStorage::new());
    let counting = Arc::new(CountingStorage::new(Arc::clone(&store) as Arc<dyn Storage>));
    let job_code = JobCode::new("waiting_job");
    let job_def = job_definition(&job_code)?.with_iteration_interval(Duration::from_secs(30))?;
    let _env = ManagerEnv::new(
        Arc::new(CachedStorage::new(
            Arc::clone(&counting) as Arc<dyn Storage>,
            Arc::new(NoopMetrics),
        )) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
            cleaner_config: JobCleanerConfig {
                enabled: false,
                ..Default::default()
            },
        },
        Arc::new(JobRegistry::new(vec![job_def.clone()])?),
        vec![job_def],
    )?;

    // Read past the counting wrapper, so proving the iteration finished costs nothing the pool is
    // measured by.
    let cancel_token = CancellationToken::new();
    wait_until_job_is_processed(store.as_ref(), &job_code, &cancel_token).await?;
    let requests_after_first_iteration = measure_settled_requests(
        || counting.find_meta_calls() + counting.conditional_read_calls() + counting.get_by_meta_calls(),
        "the passes already in flight have reached the store",
    )
    .await?;
    tokio::time::sleep(OBSERVATION_WINDOW).await;

    assert_eq!(
        counting.find_meta_calls() + counting.conditional_read_calls() + counting.get_by_meta_calls(),
        requests_after_first_iteration,
        "a job waiting for its next iteration must not be polled at all"
    );
    Ok(())
}

/// FR2 with the wait between passes taken out of the picture: a neighbour that is ready again on
/// every pass wakes the worker at its base interval, so the gate is the only thing left holding the
/// waiting job off the store. With one job in the pool the two cannot be told apart - the wait is
/// then the interval until that job's own moment.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_waiting_job_is_not_polled_while_a_neighbour_wakes_the_worker() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let store = Arc::new(InMemoryStorage::new());
    let counting = Arc::new(CountingStorage::new(Arc::clone(&store) as Arc<dyn Storage>));
    let waiting_code = JobCode::new("waiting_neighbour_job");
    let busy_code = JobCode::new("busy_neighbour_job");
    // The busy job carries no iteration interval, so every pass finds its next iteration due at
    // once and the worker keeps waking at the base interval.
    let job_defs = vec![
        job_definition(&waiting_code)?.with_iteration_interval(Duration::from_secs(30))?,
        job_definition(&busy_code)?,
    ];
    let _env = ManagerEnv::new(
        Arc::new(CachedStorage::new(
            Arc::clone(&counting) as Arc<dyn Storage>,
            Arc::new(NoopMetrics),
        )) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
            cleaner_config: JobCleanerConfig {
                enabled: false,
                ..Default::default()
            },
        },
        Arc::new(JobRegistry::new(job_defs.clone())?),
        job_defs,
    )?;

    // Read past the counting wrapper, so proving the iteration finished costs nothing the pool is
    // measured by.
    let cancel_token = CancellationToken::new();
    wait_until_job_is_processed(store.as_ref(), &waiting_code, &cancel_token).await?;
    let waiting_reads = measure_settled_requests(
        || counting.read_calls_of(&waiting_code),
        "the passes already in flight have reached the store",
    )
    .await?;
    let busy_reads = counting.read_calls_of(&busy_code);
    tokio::time::sleep(OBSERVATION_WINDOW).await;

    assert!(
        counting.read_calls_of(&busy_code) > busy_reads,
        "the neighbour must have kept the worker awake across the window, got {busy_reads} reads throughout"
    );
    assert_eq!(
        counting.read_calls_of(&waiting_code),
        waiting_reads,
        "a job waiting for its next iteration must not be polled even while its neighbour is"
    );
    Ok(())
}

/// Longest a cached read below may take. Two orders of magnitude above what an in-memory read costs,
/// so a loaded machine does not fail a test while a read waiting on the cache entry does.
const CACHED_READ_TIMEOUT: Duration = Duration::from_secs(5);

/// Reads a job through the cache under a bounded wait.
///
/// Every read below goes through the hot path, where the cache takes its entry again once the store
/// has answered. A read that held the entry across the request instead - the regression R8 is about -
/// would deadlock rather than return, and an unbounded wait would hang the whole run instead of
/// failing this test.
async fn read_job_within_bound(
    cached: &dyn Storage,
    job_code: &JobCode,
    cancel_token: &CancellationToken,
) -> Result<Job, Box<dyn std::error::Error>> {
    Ok(
        tokio::time::timeout(CACHED_READ_TIMEOUT, cached.get_job(job_code, cancel_token))
            .await
            .map_err(|_| format!("the read of '{job_code}' never returned: the cache entry was held across it"))??,
    )
}

/// Risk R6: a save that lands between a conditional read and the write of its result must not be
/// rolled back by that write. The read's own write is allowed only while the entry still holds the
/// state that read was checked against.
///
/// The oracle is the cost of the *next* read: a cache still holding the last save finds the state
/// unmoved, while a rolled-back one has to be told the state again.
#[tokio::test]
async fn a_save_landing_between_a_read_and_its_write_survives_it() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let counting = Arc::new(CountingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let racing = Arc::new(CacheRacingStorage::new(Arc::clone(&counting) as Arc<dyn Storage>));
    let cached = Arc::new(CachedStorage::new(
        Arc::clone(&racing) as Arc<dyn Storage>,
        Arc::new(NoopMetrics),
    )) as Arc<dyn Storage>;
    racing.attach_cache(&cached);
    let cancel_token = CancellationToken::new();
    let job_code = JobCode::new("raced_job");
    let worker_id = Uuid::from_u128(1);

    let mut job = Job::new(&job_definition(&job_code)?, HashMap::new(), worker_id)?;
    job.work(&worker_id)?;
    cached.save_job(&mut job, &cancel_token).await?;
    // Written past the cache, as another worker's write would be, so the read below comes back
    // carrying a state - which is the read whose result the cache writes.
    counting.save_job(&mut job, &cancel_token).await?;

    read_job_within_bound(cached.as_ref(), &job_code, &cancel_token).await?;

    assert_eq!(
        racing.interferences(),
        1,
        "the fixture must have landed a save between the read and its write"
    );
    let unchanged_before = counting.unchanged_reads();
    read_job_within_bound(cached.as_ref(), &job_code, &cancel_token).await?;
    assert_eq!(
        counting.unchanged_reads(),
        unchanged_before + 1,
        "the cache must still hold the last save, so the read after the race finds the state unmoved"
    );
    Ok(())
}

/// R8: the cache releases its entry before it reaches storage, so two readers of one job are in
/// flight at the same time. Held across the request, the entry would queue every worker of the pool
/// behind one round trip, on every poll - here the second reader would never reach the store and the
/// wait would run out.
#[tokio::test]
async fn two_readers_of_one_job_reach_storage_at_the_same_time() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let rendezvous = Arc::new(RendezvousReadStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>,
        2,
    ));
    let cached = Arc::new(CachedStorage::new(
        rendezvous as Arc<dyn Storage>,
        Arc::new(NoopMetrics),
    ));
    let cancel_token = CancellationToken::new();
    let job_code = JobCode::new("read_in_parallel_job");
    let worker_id = Uuid::from_u128(1);

    let mut job = Job::new(&job_definition(&job_code)?, HashMap::new(), worker_id)?;
    job.work(&worker_id)?;
    cached.save_job(&mut job, &cancel_token).await?;

    let readers: Vec<_> = (0..2)
        .map(|_| {
            let cached = Arc::clone(&cached);
            let job_code = job_code.clone();
            let cancel_token = cancel_token.clone();
            tokio::spawn(async move { cached.get_job(&job_code, &cancel_token).await })
        })
        .collect();

    let reads = tokio::time::timeout(CACHED_READ_TIMEOUT, futures_util::future::try_join_all(readers))
        .await
        .map_err(|_| "a reader never reached the store, so the cache queued it behind the other")??;
    for read in reads {
        read?;
    }
    Ok(())
}

/// The iteration a worker holds can be deleted by the cleaner while the worker sleeps, and the
/// conditional read of it then finds nothing. Only the fall-through to a cold read keeps the worker
/// from sitting on an iteration that no longer exists.
#[tokio::test]
async fn an_iteration_that_vanished_falls_back_to_a_cold_read() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let counting = Arc::new(CountingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let cached = CachedStorage::new(Arc::clone(&counting) as Arc<dyn Storage>, Arc::new(NoopMetrics));
    let cancel_token = CancellationToken::new();
    let job_code = JobCode::new("vanished_iteration_job");
    let job_def = job_definition(&job_code)?;
    let worker_id = Uuid::from_u128(1);

    let mut job = Job::new(&job_def, HashMap::new(), worker_id)?;
    job.work(&worker_id)?;
    counting.save_job(&mut job, &cancel_token).await?;
    read_job_within_bound(&cached, &job_code, &cancel_token).await?;

    // The job moves on past the cache, and the in-memory store keeps only the current iteration -
    // so the iteration the cache holds is gone from the backend, exactly as a cleaned-up one is.
    let TaskPickup::Ready(task_id) = job.pick_task_to_execute(&worker_id)? else {
        return Err("the fixture must leave the worker a task to finish".into());
    };
    job.start_task(&task_id, worker_id)?;
    job.complete_task(&task_id, Vec::new())?;
    job.try_to_complete(&worker_id)?;
    counting.save_job(&mut job, &cancel_token).await?;
    job.next_iteration(&job_def, worker_id)?;
    counting.save_job(&mut job, &cancel_token).await?;

    let read = read_job_within_bound(&cached, &job_code, &cancel_token).await?;

    assert_eq!(read.iter_num(), 2, "the cold read must discover the current iteration");
    assert_eq!(
        counting.find_meta_calls(),
        2,
        "and it costs exactly one listing on top of the warm-up"
    );
    Ok(())
}

/// A conditional read that failed says nothing about where the job stands, so it must reach the
/// caller as the failure it is. Read as "the iteration is gone" instead, it would send every failing
/// pass into a cold read, and an unwell store would cost a listing and a fetch on every poll of
/// every worker - the very requests the conditional read exists to save.
#[tokio::test]
async fn a_failed_conditional_read_is_not_answered_by_a_cold_read() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let counting = Arc::new(CountingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let failing_reads = Arc::new(ConditionalReadFailingStorage::new(
        Arc::clone(&counting) as Arc<dyn Storage>
    ));
    let cached = CachedStorage::new(failing_reads as Arc<dyn Storage>, Arc::new(NoopMetrics));
    let cancel_token = CancellationToken::new();
    let job_code = JobCode::new("unreadable_iteration_job");
    let worker_id = Uuid::from_u128(1);

    // Saved through the cache, which fills the entry without reading anything: the read below is
    // then the check of an iteration the cache already holds - the one path a conditional read is on.
    let mut job = Job::new(&job_definition(&job_code)?, HashMap::new(), worker_id)?;
    job.work(&worker_id)?;
    cached.save_job(&mut job, &cancel_token).await?;

    let read = tokio::time::timeout(CACHED_READ_TIMEOUT, cached.get_job(&job_code, &cancel_token))
        .await
        .map_err(|_| format!("the read of '{job_code}' never returned"))?;

    let Err(error) = read else {
        return Err("a failed conditional read must not be answered with a job".into());
    };
    assert!(matches!(error, StorageError::ServiceUnavailable), "got: {error}");
    assert!(
        error.is_retryable(),
        "an unwell store must be tried again, got: {error}"
    );
    assert_eq!(
        counting.find_meta_calls(),
        0,
        "a failed conditional read must not be answered by a listing"
    );
    assert_eq!(counting.get_by_meta_calls(), 0, "nor by a fetch of the iteration");
    Ok(())
}

/// A conditional read answered with a state is the pool's only way of learning what another pool
/// wrote to the same job: the reader has to be handed that state, and the cache has to keep it -
/// otherwise the same object is fetched again on every poll.
#[tokio::test]
async fn a_changed_read_replaces_the_state_the_cache_held() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let counting = Arc::new(CountingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let cached = CachedStorage::new(Arc::clone(&counting) as Arc<dyn Storage>, Arc::new(NoopMetrics));
    let cancel_token = CancellationToken::new();
    let job_code = JobCode::new("changed_read_job");
    let worker_id = Uuid::from_u128(1);

    let mut job = Job::new(&job_definition(&job_code)?, HashMap::new(), worker_id)?;
    job.work(&worker_id)?;
    cached.save_job(&mut job, &cancel_token).await?;
    // Written past the cache, as another pool's worker would write it: the cache is now a version
    // behind the store.
    counting.save_job(&mut job, &cancel_token).await?;

    let read = read_job_within_bound(&cached, &job_code, &cancel_token).await?;

    assert_eq!(
        read.version(),
        job.version(),
        "a changed read must hand the caller the stored state, not the one the cache held"
    );
    let unchanged_before = counting.unchanged_reads();
    read_job_within_bound(&cached, &job_code, &cancel_token).await?;
    assert_eq!(
        counting.unchanged_reads(),
        unchanged_before + 1,
        "and must leave that state in the cache, so the next read finds it unmoved"
    );
    Ok(())
}

/// Longest the pool below may take to run its three iterations out: a bound that fails with a
/// diagnostic instead of hanging when the pool stops making progress.
const POOL_PROGRESS_TIMEOUT: Duration = Duration::from_secs(20);

/// FR6: a store that ignores `If-None-Match` answers every conditional read with the object itself.
/// What that costs is the saving; what it must not cost is the pool, which has to keep reaching the
/// end of a job through nothing but changed reads.
///
/// The job carries two tasks and the pool one worker, so an iteration takes more than one pass: the
/// pass that picks the second task can only see the first one finished through the conditional read
/// of an iteration still running. A pool that mishandled a changed read would stall right there.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pool_finishes_its_job_against_a_store_ignoring_the_precondition() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let counting = Arc::new(CountingStorage::new(Arc::new(UnconditionalReadStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>,
    )) as Arc<dyn Storage>));
    let job_code = JobCode::new("unconditional_read_job");
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![
            (
                TaskDefinition::new(TaskCode::from("polled_first"), Duration::from_secs(5)),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
            (
                TaskDefinition::new(TaskCode::from("polled_second"), Duration::from_secs(5)),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
        ],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(3)?;
    let env = ManagerEnv::new(
        Arc::new(CachedStorage::new(
            Arc::clone(&counting) as Arc<dyn Storage>,
            Arc::new(NoopMetrics),
        )) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
            cleaner_config: JobCleanerConfig {
                enabled: false,
                ..Default::default()
            },
        },
        Arc::new(JobRegistry::new(vec![job_def.clone()])?),
        vec![job_def],
    )?;

    env.wait_for_all_jobs_completion(POOL_PROGRESS_TIMEOUT).await?;

    assert!(
        counting.conditional_read_calls() > 0,
        "the store under test must have been on the path"
    );
    assert_eq!(
        counting.unchanged_reads(),
        0,
        "a store ignoring the precondition never answers that a state is unmoved"
    );
    Ok(())
}
