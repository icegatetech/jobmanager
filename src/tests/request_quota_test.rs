use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::counting_metrics::CountingMetrics;
use super::common::manager_env::ManagerEnv;
use super::common::provider_harness::{ProviderHarness, ProviderStorageRequest};
use super::common::storage_wrapper::{ContendedSave, ContendingStorage, IterationSettlingStorage};
use super::common::waiting::{
    CONDITION_TIMEOUT, OBSERVATION_WINDOW, measure_settled_requests, wait_until, wait_until_job_is_processed,
};
use super::common::{meta_of, persist_completed_iterations};
use crate::core::task::SkipCause;
use crate::storage::paths::JobPaths;
use crate::{
    CachedStorage, DependencyTolerance, Job, JobCleaner, JobCleanerConfig, JobCode, JobDefinition, JobDefinitionId,
    JobDefinitionRegistry, JobRegistry, JobStateCodecKind, JobStatus, JobsManagerConfig, MetricsSink, NoopMetrics,
    Storage, TaskCode, TaskDefinition, TaskExecutor, TaskLimits, TaskOutcome, TaskRef, TaskStatus, task_fn,
};

/// Interval the pools below poll at: short enough that an observation window holds many passes, so
/// a request issued per pass would be counted many times over.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Task of the scenario whose result comes back past the bound, and its deadline - which is also its
/// maximum lifetime, so the executor is released exactly when the task has outlived it.
const OUTLIVING_TASK_CODE: &str = "outliving";
const OUTLIVING_TASK_TIMEOUT: Duration = Duration::from_millis(100);

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

/// The real store the pool is billed through, with every request it makes recorded by `metrics`.
///
/// Kept apart from [`start_pool_on`] so a scenario can put a double between this store and the
/// read cache without the double's own requests landing in the count.
async fn build_measured_store(
    harness: &dyn ProviderHarness,
    state_prefix: &str,
    job_registry: &Arc<JobRegistry>,
    metrics: &Arc<CountingMetrics>,
) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
    harness
        .build_storage(&ProviderStorageRequest::new(
            state_prefix,
            JobStateCodecKind::Json,
            Arc::clone(job_registry) as Arc<dyn JobDefinitionRegistry>,
            Arc::clone(metrics) as Arc<dyn MetricsSink>,
        ))
        .await
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
    harness: &dyn ProviderHarness,
    state_prefix: &str,
    worker_count: usize,
    job_def: JobDefinition,
    metrics: &Arc<CountingMetrics>,
) -> Result<ManagerEnv, Box<dyn std::error::Error>> {
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let store = build_measured_store(harness, state_prefix, &job_registry, metrics).await?;

    start_pool_on(store, worker_count, job_registry, job_def)
}

/// Reader of the store belonging to the test itself, so proving what the pool did costs nothing the
/// pool is measured by: a second backend over the same container, whose requests nobody counts.
async fn build_state_probe(
    harness: &dyn ProviderHarness,
    state_prefix: &str,
    job_def: &JobDefinition,
) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
    harness
        .build_storage(&ProviderStorageRequest::new(
            state_prefix,
            JobStateCodecKind::Json,
            Arc::new(JobRegistry::new(vec![job_def.clone()])?) as Arc<dyn JobDefinitionRegistry>,
            Arc::new(NoopMetrics),
        ))
        .await
}

/// Run quota of [`Storage::get_changed_job`] itself, below the pool that calls it: an iteration
/// that did not move costs the one conditional `GET` the store answers `304`, and no listing at
/// all. The save that sets the scenario up is the only other request, so the total is two.
///
/// Checked by reading without the `If-None-Match` the state was taken under: the store then answers
/// with the object and the pair the number is counted under stops being reached.
async fn run_a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("measured_read_job");
    let job_def = build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let store = build_measured_store(harness, "measured-read", &job_registry, &metrics).await?;
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
async fn run_polling_a_running_iteration_costs_no_request_besides_a_conditional_read(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
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
    let _env = start_pool(harness, "running", 2, job_def, &metrics).await?;

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
async fn run_a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("waiting_iteration_job");
    let job_def = build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?
        .with_iteration_interval(LONG_ITERATION_INTERVAL)?;
    let probe = build_state_probe(harness, "waiting", &job_def).await?;
    let _env = start_pool(harness, "waiting", 2, job_def, &metrics).await?;

    wait_until_job_is_processed(probe.as_ref(), &job_code, &CancellationToken::new()).await?;
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
async fn run_running_a_single_task_job_once_costs_five_requests(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("single_run_job");
    let job_def =
        build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?.with_max_iterations(1)?;
    let probe = build_state_probe(harness, "single-run", &job_def).await?;
    let env = start_pool(harness, "single-run", 1, job_def, &metrics).await?;

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

/// Run quota of a job whose only task refuses for good: the same five requests a successful run of
/// the same shape costs, because the refusal is stored together with the verdict it settles the
/// iteration with rather than in a pass of its own.
///
/// The refusal being terminal is what makes this a number: an ordinary refusal would be retried
/// four more times, and every retry is a write of its own.
///
/// Checked by moving the verdict back into a pass of its own - dropping `try_settle_iteration` from
/// `Worker::execute_task`: the refusal is then saved under a running iteration and the verdict
/// costs a fourth write.
async fn run_a_run_ending_in_a_terminal_failure_costs_five_requests(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("terminal_failure_run_job");
    let job_def = build_job_definition(
        &job_code,
        task_fn(|_ctx| async { Ok(TaskOutcome::TerminallyFailed("decode input".to_string())) }),
    )?
    .with_max_iterations(1)?;
    let probe = build_state_probe(harness, "terminal-failure-run", &job_def).await?;
    let env = start_pool(harness, "terminal-failure-run", 1, job_def, &metrics).await?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    let requests = measure_settled_requests(
        || metrics.storage_operations_total(),
        "the pool has stopped polling the job whose iteration budget is spent",
    )
    .await?;

    let stored = probe.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        *stored.status(),
        JobStatus::Failed,
        "the number is only about a run whose iteration really ended on the refusal"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "OK"),
        3,
        "creating the job, starting its task and refusing it together with the verdict"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        2,
        "the empty store and the finished iteration are the only two states that cost a listing"
    );
    assert_eq!(
        requests, 5,
        "and a refusal costs no more than a success of the same shape"
    );
    Ok(())
}

/// Run quota of a two-task job whose first task calls its branch pointless: five requests, the same
/// a one-task run costs, because the second task is put out by the cascade instead of being run.
///
/// The number is what proves the branch was never executed: running both tasks would cost seven -
/// a write to start the second and a write to finish it.
///
/// Checked by dropping the cascade from `Job::try_settle_iteration`: the dependent then either runs -
/// two writes more - or leaves the iteration open for the deadlock the settling reports.
async fn run_a_run_whose_branch_is_skipped_costs_five_requests(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("skipped_branch_run_job");
    let definition_id = JobDefinitionId::new();
    let job_def = JobDefinition::new(
        definition_id,
        job_code.clone(),
        vec![
            (
                TaskDefinition::new(TaskCode::from("detect"), Duration::from_secs(30)),
                task_fn(|_ctx| async { Ok(TaskOutcome::SkippedBranch("nothing to do".to_string())) }),
            ),
            (
                TaskDefinition::new(TaskCode::from("prepare"), Duration::from_secs(30)),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
        ],
        Vec::new(),
        vec![(
            TaskRef::initial(definition_id, 1),
            vec![TaskRef::initial(definition_id, 0)],
        )],
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let probe = build_state_probe(harness, "skipped-branch-run", &job_def).await?;
    let env = start_pool(harness, "skipped-branch-run", 1, job_def, &metrics).await?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    let requests = measure_settled_requests(
        || metrics.storage_operations_total(),
        "the pool has stopped polling the job whose iteration budget is spent",
    )
    .await?;

    let stored = probe.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        *stored.status(),
        JobStatus::Completed,
        "a decision is not a failure, and the number is only about an iteration that ended"
    );
    assert!(
        stored
            .tasks_as_iter()
            .any(|task| task.code() == &TaskCode::from("prepare") && task.is_skipped()),
        "the dependent must have been put out by the cascade rather than left waiting"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "OK"),
        3,
        "creating the job, starting the branch and skipping it together with the cascade and the verdict"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        2,
        "the empty store and the finished iteration are the only two states that cost a listing"
    );
    assert_eq!(
        requests, 5,
        "and the branch that was skipped is paid for as if it were not there"
    );
    Ok(())
}

/// Run quota of a two-task job whose first task refuses for good and whose second declared it
/// survives that: eight requests, made of five writes - the job created, and a start and a
/// resolution for each of the two tasks - the two listings every run of this shape pays, and one
/// conditional read. The read is what a degraded path costs on top of the writes: the dependent
/// starts in a pass of its own, and that pass finds the iteration its predecessor left open.
///
/// Three more requests than the run whose branch is skipped, and that is the point of the number: a
/// tolerant dependent is work that really runs, where a dependent the cascade puts out is not.
///
/// Checked by making `Task::judge_dependency` ignore `allows_failed` for a terminally failed
/// dependency: the cascade then puts the dependent out in the pass that stores the refusal, the
/// iteration ends as failed, and the pass the dependent's own execution took - its two writes and
/// its conditional read - goes with it, leaving five requests. Dropping the tolerance from the
/// dependent's definition moves the same three requests, which is what proves the fixture reaches
/// the tolerant path at all.
async fn run_a_run_with_a_tolerant_dependent_costs_eight_requests(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("tolerant_dependent_run_job");
    let definition_id = JobDefinitionId::new();
    let job_def = JobDefinition::new(
        definition_id,
        job_code.clone(),
        vec![
            (
                TaskDefinition::new(TaskCode::from("detect"), Duration::from_secs(30)),
                task_fn(|_ctx| async { Ok(TaskOutcome::TerminallyFailed("decode input".to_string())) }),
            ),
            (
                TaskDefinition::new(TaskCode::from("prepare"), Duration::from_secs(30))
                    .with_dependencies(vec![TaskRef::initial(definition_id, 0)])
                    .with_dependency_tolerance(DependencyTolerance {
                        allows_failed: true,
                        allows_skipped: false,
                    }),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
        ],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let probe = build_state_probe(harness, "tolerant-dependent-run", &job_def).await?;
    let env = start_pool(harness, "tolerant-dependent-run", 1, job_def, &metrics).await?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    let requests = measure_settled_requests(
        || metrics.storage_operations_total(),
        "the pool has stopped polling the job whose iteration budget is spent",
    )
    .await?;

    let stored = probe.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        *stored.status(),
        JobStatus::Completed,
        "the failure was handled, and the number is only about an iteration that ended"
    );
    assert!(
        stored
            .tasks_as_iter()
            .any(|task| task.code() == &TaskCode::from("prepare") && task.is_completed()),
        "the tolerant dependent must have run on the failed dependency rather than been put out"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "OK"),
        5,
        "creating the job, and a start and a resolution for each of the two tasks"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        2,
        "the empty store and the finished iteration are the only two states that cost a listing"
    );
    assert_eq!(
        metrics.storage_operations("GET", "304"),
        1,
        "the pass that starts the dependent finds the iteration open and pays a conditional read for it"
    );
    assert_eq!(
        requests, 8,
        "and a degraded path is paid for as the work it is, no more"
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
async fn run_a_run_losing_one_race_costs_eight_requests(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("contended_run_job");
    let job_def =
        build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?.with_max_iterations(1)?;
    let probe = build_state_probe(harness, "contended", &job_def).await?;
    let rival_store = build_state_probe(harness, "contended", &job_def).await?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let contending = Arc::new(
        ContendingStorage::new(build_measured_store(harness, "contended", &job_registry, &metrics).await?)
            .with_rival_store(rival_store),
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

/// Run quota of a run whose result comes back into an iteration another worker has already settled:
/// seven requests. The task outlives its maximum lifetime while its executor runs, so the rival
/// fails it and closes the iteration, and the save carrying the result is refused.
///
/// What the number holds is that such a result is dropped rather than merged: the refused write is
/// answered by one listing and one read that re-discover the state, and nothing is written after
/// them - one request fewer than a lost race whose merge goes through, which pays for its retry.
///
/// Checked by answering `JobError::IterationAlreadySettled` with `MergeDecision::Retry` instead of
/// `SaveOutcome::JobStolen` in the conflict handler of `Worker::execute_task`: the save is then made
/// a second time and writes an eighth request.
async fn run_a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("settled_iteration_run_job");
    let task_def = TaskDefinition::new(TaskCode::from(OUTLIVING_TASK_CODE), OUTLIVING_TASK_TIMEOUT)
        .with_max_lifetime(OUTLIVING_TASK_TIMEOUT);
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            task_def,
            task_fn(|ctx| async move {
                ctx.cancel_token().cancelled().await;
                Ok(TaskOutcome::Completed(b"late".to_vec()))
            }),
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let probe = build_state_probe(harness, "settled-iteration", &job_def).await?;
    let rival_store = build_state_probe(harness, "settled-iteration", &job_def).await?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let settling = Arc::new(
        IterationSettlingStorage::new(
            build_measured_store(harness, "settled-iteration", &job_registry, &metrics).await?,
            Uuid::new_v4(),
            TaskCode::from(OUTLIVING_TASK_CODE),
        )
        .with_rival_store(rival_store),
    );
    let env = start_pool_on(Arc::clone(&settling) as Arc<dyn Storage>, 1, job_registry, job_def)?;

    env.wait_for_all_jobs_completion(CONDITION_TIMEOUT).await?;
    let requests = measure_settled_requests(
        || metrics.storage_operations_total(),
        "the pool has stopped polling the job whose iteration budget is spent",
    )
    .await?;

    assert_eq!(settling.interference_failure(), None);
    assert_eq!(
        settling.interferences(),
        1,
        "the fixture must have let the rival settle the iteration ahead of the save carrying the result"
    );
    let stored = probe.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        *stored.status(),
        JobStatus::Failed,
        "the stored iteration must be the one the rival settled"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "412"),
        1,
        "the write the rival got in ahead of is the only one refused"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "OK"),
        2,
        "the refused save is dropped rather than retried, so the writes are the creation and the pickup"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        3,
        "the refusal empties the cache, so the state has to be discovered once more"
    );
    assert_eq!(
        metrics.storage_operations("GET", "OK"),
        1,
        "and read once more, which is the read the merge is refused on"
    );
    assert_eq!(requests, 7, "and nothing else is billed for the dropped result");
    Ok(())
}

/// Run quota of the skipped-branch job when the save carrying the decision loses its race: eight
/// requests, the same shape a lost race costs anywhere - the five of the uncontended run plus the
/// rejected write and the listing and read that re-discover the state.
///
/// What the number holds is that the merge closes the iteration itself: the cascade the worker
/// applied to its own copy is not part of what a merge carries, so the merged state is settled
/// again before the retry, and the dependent is put out in the very write the retry makes.
///
/// Checked by dropping the `try_settle_iteration` call from the end of
/// `Job::merge_with_processed_task`: the retry then stores an open iteration, and the pass that
/// closes it afterwards costs a ninth request of its own.
async fn run_a_run_whose_skipped_branch_loses_one_race_costs_eight_requests(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("contended_skip_run_job");
    let definition_id = JobDefinitionId::new();
    let job_def = JobDefinition::new(
        definition_id,
        job_code.clone(),
        vec![
            (
                TaskDefinition::new(TaskCode::from("detect"), Duration::from_secs(30)),
                task_fn(|_ctx| async { Ok(TaskOutcome::SkippedBranch("nothing to do".to_string())) }),
            ),
            (
                TaskDefinition::new(TaskCode::from("prepare"), Duration::from_secs(30))
                    .with_dependencies(vec![TaskRef::initial(definition_id, 0)]),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
        ],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let probe = build_state_probe(harness, "contended-skip", &job_def).await?;
    let rival_store = build_state_probe(harness, "contended-skip", &job_def).await?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let contending = Arc::new(
        ContendingStorage::new(build_measured_store(harness, "contended-skip", &job_registry, &metrics).await?)
            .with_contended_save(ContendedSave::OfTask {
                code: TaskCode::from("detect"),
                status: TaskStatus::Skipped(SkipCause::ExecutorDecision),
            })
            .with_rival_store(rival_store),
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
        "the fixture must have made the save of the skipped branch lose exactly one race"
    );
    let stored = probe.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        *stored.status(),
        JobStatus::Completed,
        "a lost race must not keep the run from reaching the end of its iteration"
    );
    assert!(
        stored
            .tasks_as_iter()
            .any(|task| task.code() == &TaskCode::from("prepare") && task.is_skipped()),
        "the dependent must have been put out in the state the retry stored"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "412"),
        1,
        "the write the rival got in ahead of is the only one refused"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "OK"),
        3,
        "the retry replaces the refused write rather than adding to the writes a run makes"
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
        requests, 8,
        "and the cascade the merge derives again costs no write of its own"
    );
    Ok(())
}

/// Run quota of a pickup whose save loses its race after a dependency outlived its maximum
/// lifetime: ten requests.
///
/// What the number holds is that the failure and the cascade the merge derives again cost no
/// request of their own, and that the pickup is carried through rather than dropped: the dependent
/// runs in the very write the retry makes, so no further pass is paid for.
///
/// Checked by dropping the derivation from `Job::merge_with_picked_task`: the dependency then stays
/// started in the state the retry writes, so the iteration is closed by a later pass whose own write
/// and conditional read are billed on top.
async fn run_a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("contended_pickup_run_job");
    let definition_id = JobDefinitionId::new();
    let job_def = JobDefinition::new(
        definition_id,
        job_code.clone(),
        vec![
            (
                TaskDefinition::new(TaskCode::from(OUTLIVING_TASK_CODE), OUTLIVING_TASK_TIMEOUT)
                    .with_max_lifetime(OUTLIVING_TASK_TIMEOUT),
                task_fn(|ctx| async move {
                    ctx.cancel_token().cancelled().await;
                    Ok(TaskOutcome::Cancelled)
                }),
            ),
            (
                TaskDefinition::new(TaskCode::from("prepare"), Duration::from_secs(30))
                    .with_dependencies(vec![TaskRef::initial(definition_id, 0)])
                    .with_dependency_tolerance(DependencyTolerance {
                        allows_failed: true,
                        allows_skipped: false,
                    }),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
        ],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let probe = build_state_probe(harness, "contended-pickup", &job_def).await?;
    let rival_store = build_state_probe(harness, "contended-pickup", &job_def).await?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let contending = Arc::new(
        ContendingStorage::new(build_measured_store(harness, "contended-pickup", &job_registry, &metrics).await?)
            .with_contended_save(ContendedSave::OfTask {
                code: TaskCode::from("prepare"),
                status: TaskStatus::Started,
            })
            .with_rival_store(rival_store),
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
        "the fixture must have made the save of the picked dependent lose exactly one race"
    );
    let stored = probe.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        *stored.status(),
        JobStatus::Completed,
        "the failure the merge derived was handled by the dependent that ran on it"
    );
    assert!(
        stored
            .tasks_as_iter()
            .any(|task| task.code() == &TaskCode::from("prepare") && task.is_completed()),
        "the dependent must have run on the dependency the merge failed rather than been dropped"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "412"),
        1,
        "the write the rival got in ahead of is the only one refused"
    );
    assert_eq!(
        metrics.storage_operations("PUT", "OK"),
        4,
        "creating the job, starting each of the two tasks, and the one result the run stores"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        3,
        "the refusal empties the cache, so the state has to be discovered once more"
    );
    assert_eq!(
        metrics.storage_operations("GET", "OK"),
        1,
        "and read once more, which is the read the merge derives the failure on"
    );
    assert_eq!(
        metrics.storage_operations("GET", "304"),
        1,
        "the pass that finds the dependency past its lifetime pays a conditional read for the iteration"
    );
    assert_eq!(
        requests, 10,
        "and the failure and the cascade the merge derives cost nothing on top of the race"
    );
    Ok(())
}

/// Iterations the cleanup scenario starts from, and how many of them its retention window keeps.
/// Seven and five leave exactly two outdated, which is the smallest tail a batching provider can be
/// told apart from a per-key one by.
const TRIMMED_TAIL_ITERATIONS: u64 = 7;
const TRIMMED_TAIL_RETENTION: u64 = 5;

/// Prefix the cleanup scenario keeps its state under, shared by the store it writes the tail with,
/// the store it measures, and the reader that says which iterations survived.
const TRIMMED_TAIL_STATE_PREFIX: &str = "trimmed-tail";

/// Run quota of trimming a tail, which is the one scenario the two backends do not pay the same for
/// and therefore the one that states the difference as a number: the listing that finds the current
/// iteration, the listing of the tail, and then the deletes - one request carrying both iterations
/// on S3, one request per iteration on Azure.
///
/// `expected_deletes` and `expected_requests` are what the wrappers below differ in, and the totals
/// are asserted whole, so a request under any other pair - a `429`, a `500`, a retried listing -
/// cannot hide behind the named ones.
///
/// The tail is written through a store of the test's own, so what the counters hold is the cleanup
/// pass alone. The pass itself is start-up reconciliation, run to completion rather than cancelled:
/// every sender of the report channel is dropped before the cleaner starts, so the cleaner returns
/// once it has reconciled the one registered job and there is no second pass to race the counters.
///
/// Checked by breaking each number once. Three: making `S3Backend::delete_job_iterations` chunk by
/// one key turns its single multi-object delete into two, and its number becomes the Azure one.
/// Four: dropping the `next_marker` check from `AzureBackend::list_job_outdated_iterations` sends
/// the pager for a page after the last one, which it answers without a request - a third listing in
/// the count that nobody was billed for.
async fn run_trimming_a_tail_of_two_iterations(
    harness: &dyn ProviderHarness,
    expected_deletes: u64,
    expected_requests: u64,
) -> Result<(), Box<dyn std::error::Error>> {
    let metrics = Arc::new(CountingMetrics::default());
    let job_code = JobCode::new("trimmed_tail_job");
    let job_def = build_job_definition(&job_code, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }))?
        .with_iteration_retention(TRIMMED_TAIL_RETENTION)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let tail_writer = build_state_probe(harness, TRIMMED_TAIL_STATE_PREFIX, &job_def).await?;
    persist_completed_iterations(tail_writer.as_ref(), &job_code, 1..=TRIMMED_TAIL_ITERATIONS).await?;

    let store = build_measured_store(harness, TRIMMED_TAIL_STATE_PREFIX, &job_registry, &metrics).await?;
    let cleaner = JobCleaner::new(job_registry, store, &JobCleanerConfig::default());
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    drop(sender);
    cleaner.start(receiver, CancellationToken::new()).await?;

    // Which iterations survived rather than how many: a pass deleting the two newest keeps the same
    // count and the same request numbers, and what it would have thrown away is the state the job
    // runs on. The names are read back as iteration numbers so the assertion is about iterations,
    // which is what the retention boundary is stated in.
    let state_object_keys = JobPaths::new(
        TRIMMED_TAIL_STATE_PREFIX.to_string(),
        JobStateCodecKind::Json.build().as_ref(),
    );
    let kept_keys = harness.read_state_keys(TRIMMED_TAIL_STATE_PREFIX).await?;
    let mut kept_iter_nums = kept_keys
        .iter()
        .map(|key| state_object_keys.parse_iter_num(key))
        .collect::<Result<Vec<u64>, _>>()?;
    kept_iter_nums.sort_unstable();
    assert_eq!(
        kept_iter_nums,
        ((TRIMMED_TAIL_ITERATIONS - TRIMMED_TAIL_RETENTION + 1)..=TRIMMED_TAIL_ITERATIONS).collect::<Vec<u64>>(),
        "the pass must keep the newest iterations the retention window covers, found {kept_keys:?}"
    );
    assert_eq!(
        metrics.storage_operations("LIST", "OK"),
        2,
        "finding the current iteration and listing the tail are the only two listings a pass makes"
    );
    assert_eq!(
        metrics.storage_operations("DELETE", "OK"),
        expected_deletes,
        "and the deletes are what the two providers pay differently for"
    );
    assert_eq!(
        metrics.storage_operations_total(),
        expected_requests,
        "and nothing else is billed: no read, no refused request, no retry"
    );
    Ok(())
}

/// The quotas as the S3 backend pays them. Each wrapper names its number, and what the number
/// holds - and the break that was used to check it - is written once on the body it runs.
#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::{
        run_a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing,
        run_a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store,
        run_a_run_ending_in_a_terminal_failure_costs_five_requests, run_a_run_losing_one_race_costs_eight_requests,
        run_a_run_whose_branch_is_skipped_costs_five_requests,
        run_a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests,
        run_a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests,
        run_a_run_whose_skipped_branch_loses_one_race_costs_eight_requests,
        run_a_run_with_a_tolerant_dependent_costs_eight_requests,
        run_polling_a_running_iteration_costs_no_request_besides_a_conditional_read,
        run_running_a_single_task_job_once_costs_five_requests, run_trimming_a_tail_of_two_iterations,
    };
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test]
    async fn a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing_on_s3()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing(&S3ProviderHarness::start().await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn polling_a_running_iteration_costs_no_request_besides_a_conditional_read_on_s3()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_polling_a_running_iteration_costs_no_request_besides_a_conditional_read(&S3ProviderHarness::start().await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store_on_s3()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn running_a_single_task_job_once_costs_five_requests_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_running_a_single_task_job_once_costs_five_requests(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_ending_in_a_terminal_failure_costs_five_requests_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_ending_in_a_terminal_failure_costs_five_requests(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_branch_is_skipped_costs_five_requests_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_branch_is_skipped_costs_five_requests(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_with_a_tolerant_dependent_costs_eight_requests_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_with_a_tolerant_dependent_costs_eight_requests(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_losing_one_race_costs_eight_requests_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_losing_one_race_costs_eight_requests(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests_on_s3()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests(&S3ProviderHarness::start().await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_skipped_branch_loses_one_race_costs_eight_requests_on_s3()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_skipped_branch_loses_one_race_costs_eight_requests(&S3ProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests_on_s3()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests(
            &S3ProviderHarness::start().await?,
        )
        .await
    }

    #[tokio::test]
    async fn trimming_a_tail_of_two_iterations_costs_three_requests_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_trimming_a_tail_of_two_iterations(&S3ProviderHarness::start().await?, 1, 3).await
    }
}

/// The same quotas as the Azure backend pays them. Every scenario a pool repeats costs the same on
/// both backends - a read, a write and a listing page are one request each, recorded under the same
/// operation and status - and a number that differs in one of those is a defect of the backend
/// rather than a second price list.
///
/// Trimming a tail is the exception and the reason the two names carry different numbers:
/// `azure_storage_blob` exposes no client for the provider's batch delete, so a sweep costs a
/// request per iteration where S3 pays one for all of them.
#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::{
        run_a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing,
        run_a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store,
        run_a_run_ending_in_a_terminal_failure_costs_five_requests, run_a_run_losing_one_race_costs_eight_requests,
        run_a_run_whose_branch_is_skipped_costs_five_requests,
        run_a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests,
        run_a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests,
        run_a_run_whose_skipped_branch_loses_one_race_costs_eight_requests,
        run_a_run_with_a_tolerant_dependent_costs_eight_requests,
        run_polling_a_running_iteration_costs_no_request_besides_a_conditional_read,
        run_running_a_single_task_job_once_costs_five_requests, run_trimming_a_tail_of_two_iterations,
    };
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test]
    async fn a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing(
            &AzureProviderHarness::start().await?,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn polling_a_running_iteration_costs_no_request_besides_a_conditional_read_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_polling_a_running_iteration_costs_no_request_besides_a_conditional_read(
            &AzureProviderHarness::start().await?,
        )
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn running_a_single_task_job_once_costs_five_requests_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_running_a_single_task_job_once_costs_five_requests(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_ending_in_a_terminal_failure_costs_five_requests_on_azure() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_a_run_ending_in_a_terminal_failure_costs_five_requests(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_branch_is_skipped_costs_five_requests_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_branch_is_skipped_costs_five_requests(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_with_a_tolerant_dependent_costs_eight_requests_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_with_a_tolerant_dependent_costs_eight_requests(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_losing_one_race_costs_eight_requests_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_losing_one_race_costs_eight_requests(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests(&AzureProviderHarness::start().await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_skipped_branch_loses_one_race_costs_eight_requests_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_skipped_branch_loses_one_race_costs_eight_requests(&AzureProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests_on_azure()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests(
            &AzureProviderHarness::start().await?,
        )
        .await
    }

    #[tokio::test]
    async fn trimming_a_tail_of_two_iterations_costs_four_requests_on_azure() -> Result<(), Box<dyn std::error::Error>>
    {
        crate::tests::common::init_tracing();
        run_trimming_a_tail_of_two_iterations(&AzureProviderHarness::start().await?, 2, 4).await
    }
}

/// The same quotas as the Google Cloud Storage backend pays them. Every scenario a pool repeats
/// costs the same on all three backends - a read, a write and a listing page are one request each,
/// recorded under the same operation and status - and a number that differs in one of those is a
/// defect of the backend rather than a third price list.
///
/// Trimming a tail is the exception and the reason this name carries the Azure number rather than
/// the S3 one: the JSON API has a batch endpoint, and reaching it means a `multipart/mixed` body of
/// sub-requests that `gcs_client` does not build, so a sweep costs a request per iteration.
#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::{
        run_a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing,
        run_a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store,
        run_a_run_ending_in_a_terminal_failure_costs_five_requests, run_a_run_losing_one_race_costs_eight_requests,
        run_a_run_whose_branch_is_skipped_costs_five_requests,
        run_a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests,
        run_a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests,
        run_a_run_whose_skipped_branch_loses_one_race_costs_eight_requests,
        run_a_run_with_a_tolerant_dependent_costs_eight_requests,
        run_polling_a_running_iteration_costs_no_request_besides_a_conditional_read,
        run_running_a_single_task_job_once_costs_five_requests, run_trimming_a_tail_of_two_iterations,
    };
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test]
    async fn a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing_on_gcs()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_conditional_read_of_an_unmoved_iteration_costs_one_get_and_no_listing(&GcsProviderHarness::start().await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn polling_a_running_iteration_costs_no_request_besides_a_conditional_read_on_gcs()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_polling_a_running_iteration_costs_no_request_besides_a_conditional_read(&GcsProviderHarness::start().await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store_on_gcs()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_job_waiting_for_its_next_iteration_costs_nothing_on_the_store(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn running_a_single_task_job_once_costs_five_requests_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_running_a_single_task_job_once_costs_five_requests(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_ending_in_a_terminal_failure_costs_five_requests_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_ending_in_a_terminal_failure_costs_five_requests(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_branch_is_skipped_costs_five_requests_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_branch_is_skipped_costs_five_requests(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_with_a_tolerant_dependent_costs_eight_requests_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_with_a_tolerant_dependent_costs_eight_requests(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_losing_one_race_costs_eight_requests_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_losing_one_race_costs_eight_requests(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests_on_gcs()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_result_lands_in_a_settled_iteration_costs_seven_requests(&GcsProviderHarness::start().await?)
            .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_skipped_branch_loses_one_race_costs_eight_requests_on_gcs()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_skipped_branch_loses_one_race_costs_eight_requests(&GcsProviderHarness::start().await?).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests_on_gcs()
    -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_run_whose_pickup_loses_one_race_after_a_lifetime_failure_costs_ten_requests(
            &GcsProviderHarness::start().await?,
        )
        .await
    }

    #[tokio::test]
    async fn trimming_a_tail_of_two_iterations_costs_four_requests_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_trimming_a_tail_of_two_iterations(&GcsProviderHarness::start().await?, 2, 4).await
    }
}
