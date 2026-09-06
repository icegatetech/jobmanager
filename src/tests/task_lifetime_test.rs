use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::{sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::counting_metrics::CountingMetrics;
use super::common::manager_env::ManagerEnv;
use super::common::storage_wrapper::{ContendedSave, ContendingStorage, IterationSettlingStorage};
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    DependencyTolerance, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManager,
    JobsManagerConfig, MetricsSink, Storage, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, TaskRef, TaskStatus,
    task_fn,
};

const TASK_CODE: &str = "import";
/// Task that declares it survives a dependency that failed for good, which is what makes the state
/// of that dependency observable from inside an executor.
const DEGRADED_TASK_CODE: &str = "degraded";
/// How many times the worker running the task is stopped mid-execution. Equal to the attempt
/// budget below, so a run in which takeovers still spent attempts would have failed the task
/// terminally before the execution that finishes it.
const INTERRUPTIONS: u32 = 2;
const MAX_ATTEMPTS: u32 = 2;
/// Deadline of the task, and so how long the next pool waits before it may take the task over.
/// Long enough that the pool is stopped well within it, so the cancellation the executor observes
/// is the shutdown rather than the deadline.
const TASK_TIMEOUT: Duration = Duration::from_secs(1);
/// Lifetime wide enough that this test never reaches it: what is under test is the attempt
/// accounting, not the lifetime bound.
const TASK_MAX_LIFETIME: Duration = Duration::from_mins(2);
/// Bound on every wait, so a task that is never picked up fails the test instead of hanging it.
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);
/// Deadline of the task in the second pass below, which is also its maximum lifetime: the very
/// first moment the task could be taken over is the moment it has outlived its lifetime instead.
const OUTLIVING_TASK_TIMEOUT: Duration = Duration::from_millis(100);

fn manager_config() -> JobsManagerConfig {
    JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO)
            .with_max_poll_interval(Duration::from_millis(50))
            .expect("a ceiling above the poll interval is accepted"),
        ..Default::default()
    }
}

/// A worker that dies mid-task must not cost the task an attempt: the task refused nothing, and
/// the next worker takes it over on expiry.
///
/// The death is modelled by stopping the pool while the executor is inside its task and returning
/// [`TaskOutcome::Cancelled`], which persists nothing - exactly the state a killed process leaves
/// behind. Before takeovers were excluded from the budget, the task below was terminally failed
/// after `MAX_ATTEMPTS` such deaths, without its executor ever having refused it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_task_survives_as_many_lost_workers_as_it_has_attempts() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("lost_worker_job");
    let executions = Arc::new(AtomicU32::new(0));
    let (started_sender, mut started_receiver) = mpsc::unbounded_channel();

    let executions_in_executor = Arc::clone(&executions);
    let executor = task_fn(move |ctx| {
        let executions = Arc::clone(&executions_in_executor);
        let started_sender = started_sender.clone();

        async move {
            let execution = executions.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = started_sender.send(execution);

            if execution <= INTERRUPTIONS {
                ctx.cancel_token().cancelled().await;
                return Ok(TaskOutcome::Cancelled);
            }

            Ok(TaskOutcome::Completed(b"imported".to_vec()))
        }
    });

    let task_def = TaskDefinition::new(TaskCode::new(TASK_CODE), TASK_TIMEOUT)
        .with_max_attempts(MAX_ATTEMPTS)
        .with_max_lifetime(TASK_MAX_LIFETIME);
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());

    for interruption in 1..=INTERRUPTIONS {
        let mut manager_env = ManagerEnv::new(
            Arc::clone(&storage),
            manager_config(),
            Arc::clone(&job_registry),
            vec![job_def.clone()],
        )?;

        let started = timeout(WAIT_TIMEOUT, started_receiver.recv())
            .await?
            .ok_or("the executor must report the execution it started")?;
        assert_eq!(
            started, interruption,
            "the task must be picked up exactly once per pool"
        );

        manager_env.stop().await;
    }

    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage),
        manager_config(),
        Arc::clone(&job_registry),
        vec![job_def],
    )?;
    manager_env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    manager_env.stop().await;

    let job = storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        *job.status(),
        JobStatus::Completed,
        "the task must still be runnable after {INTERRUPTIONS} lost workers"
    );
    assert_eq!(
        executions.load(Ordering::SeqCst),
        INTERRUPTIONS + 1,
        "every lost worker must be followed by a takeover"
    );

    let tasks = job.get_tasks_by_code(&TaskCode::new(TASK_CODE));
    let task = tasks.first().ok_or("the finished job must still hold its task")?;
    assert_eq!(
        task.attempts(),
        1,
        "only the first start spends an attempt; the takeovers spend none"
    );

    Ok(())
}

/// The bound the takeovers above are stopped by: a task nobody ever refuses, released by its own
/// deadline every time, is failed for outliving its maximum lifetime and ends its iteration - with
/// its attempt budget untouched.
///
/// A lifetime equal to the deadline is what makes the run deterministic rather than a wait on a
/// sleep: the first execution is released exactly when both run out, so the next pass of the same
/// worker finds the task expired past its lifetime instead of taking it over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_task_outliving_its_lifetime_fails_its_iteration() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("outlived_lifetime_job");
    let executions = Arc::new(AtomicU32::new(0));

    let executions_in_executor = Arc::clone(&executions);
    let executor = task_fn(move |ctx| {
        let executions = Arc::clone(&executions_in_executor);

        async move {
            executions.fetch_add(1, Ordering::SeqCst);
            ctx.cancel_token().cancelled().await;
            Ok(TaskOutcome::Cancelled)
        }
    });

    let task_def =
        TaskDefinition::new(TaskCode::new(TASK_CODE), OUTLIVING_TASK_TIMEOUT).with_max_lifetime(OUTLIVING_TASK_TIMEOUT);
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());

    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage),
        manager_config(),
        Arc::clone(&job_registry),
        vec![job_def],
    )?;
    manager_env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    manager_env.stop().await;

    let job = storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(*job.status(), JobStatus::Failed);

    let tasks = job.get_tasks_by_code(&TaskCode::new(TASK_CODE));
    let task = tasks.first().ok_or("the failed iteration must still hold its task")?;
    assert!(task.is_failed());
    assert!(
        task.get_resolution_reason().contains("outlived its maximum lifetime"),
        "the recorded reason must name the limit that ended the task, got: {}",
        task.get_resolution_reason()
    );
    assert_eq!(
        task.attempts(),
        1,
        "the executor refused nothing: the lifetime ended the task, not its attempt budget"
    );
    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "a task past its lifetime must be failed rather than taken over again"
    );

    Ok(())
}

/// The other side of that bound: while this worker's executor was running past the lifetime,
/// another worker failed the task for outliving it and closed the iteration. The result coming back
/// has nowhere to go - the iteration is answered for, and the next one is planned from scratch - so
/// the save is dropped rather than written into a settled iteration, and the verdict is neither
/// measured nor logged a second time by the worker that did not write it.
///
/// A lifetime equal to the deadline is what makes the run deterministic: the executor is released by
/// its own deadline token, which fires at the moment the task has outlived its lifetime, so the
/// result is always returned past the bound.
///
/// What the dropped result does leave behind is a task this worker lost, measured like any other
/// takeover: the path is the one the maximum lifetime exists for, so an operator has to see it.
///
/// Three breaks prove it: dropping the `SaveOutcome::Saved` check in `Worker::execute_task` reports
/// the iteration a second time, dropping `SaveOutcome::JobStolen` from the
/// `matches!(outcome, SaveOutcome::TaskStolen | SaveOutcome::JobStolen)` check in
/// `Worker::save_processed_task` measures a task that was never stored, and dropping
/// `record_task_stolen` from the settled-iteration arm leaves the dropped result unmeasured.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_result_returned_into_a_settled_iteration_is_dropped() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("settled_iteration_job");
    let executor = task_fn(|ctx| async move {
        ctx.cancel_token().cancelled().await;
        Ok(TaskOutcome::Completed(b"late".to_vec()))
    });

    let task_def =
        TaskDefinition::new(TaskCode::new(TASK_CODE), OUTLIVING_TASK_TIMEOUT).with_max_lifetime(OUTLIVING_TASK_TIMEOUT);
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let inner: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
    let storage = Arc::new(IterationSettlingStorage::new(
        Arc::clone(&inner),
        Uuid::new_v4(),
        TaskCode::new(TASK_CODE),
    ));
    let sink = Arc::new(CountingMetrics::default());
    let manager = JobsManager::new(
        Arc::clone(&storage) as Arc<dyn Storage>,
        manager_config(),
        Arc::clone(&job_registry),
        Arc::clone(&sink) as Arc<dyn MetricsSink>,
    )?;

    let handle = manager.start()?;
    timeout(WAIT_TIMEOUT, handle.wait_for_job_completion(&job_code)).await??;
    handle.shutdown().await?;

    assert_eq!(storage.interference_failure(), None);
    assert_eq!(
        storage.interferences(),
        1,
        "the fixture must have let the rival settle the iteration ahead of the save carrying the result"
    );

    let job = inner.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(*job.status(), JobStatus::Failed);
    assert_eq!(job.iter_num(), 1, "the settled iteration must not have been replanned");
    let tasks = job.get_tasks_by_code(&TaskCode::new(TASK_CODE));
    let task = tasks.first().ok_or("the settled iteration must still hold its task")?;
    assert!(
        task.get_resolution_reason().contains("outlived its maximum lifetime"),
        "the stored task must keep the resolution of the worker that settled the iteration, got: {}",
        task.get_resolution_reason()
    );

    assert_eq!(
        sink.failed_iterations() + sink.completed_iterations(),
        0,
        "the verdict was written by the rival, so the worker whose save was dropped must not report it"
    );
    assert_eq!(
        sink.failed_tasks() + sink.completed_tasks() + sink.skipped_tasks(),
        0,
        "a result that never reached storage must not be measured as a processed task"
    );
    assert_eq!(
        sink.stolen_tasks(),
        1,
        "the task was resolved by the worker that settled the iteration, so this one lost it"
    );

    Ok(())
}

/// A pickup released by a failure the worker derived itself, saved into a state that never saw that
/// failure. The dependency outlived its maximum lifetime, which every copy of the job derives on its
/// own rather than reading, so the state the save loses its race to still shows the dependency
/// started - and the tolerant dependent merged into it would run against a dependency that neither
/// completed nor failed, with no reason on it to take a degraded path from.
///
/// The race is staged rather than waited for: the rival writes over the stored state right before
/// the pool saves the dependent it just picked up.
///
/// Checked by dropping the derivation and the check that follows it from
/// `Job::merge_with_picked_task`: the executor then reports a dependency that is not failed and
/// carries no reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dependent_merged_after_a_conflict_starts_on_a_failed_dependency() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("merged_pickup_job");
    let (reports_sender, mut reports_receiver) = mpsc::unbounded_channel();
    let degraded_executor = task_fn(move |ctx| {
        let reports = reports_sender.clone();

        async move {
            let dependencies = ctx.job().get_tasks_by_code(&TaskCode::new(TASK_CODE))?;
            let dependency = dependencies
                .first()
                .ok_or_else(|| crate::Error::Other("the job must hold the dependency".to_string()))?;
            let _ = reports.send((dependency.is_failed(), dependency.get_resolution_reason().to_string()));

            Ok(TaskOutcome::empty())
        }
    });

    let definition_id = JobDefinitionId::new();
    let job_def = JobDefinition::new(
        definition_id,
        job_code.clone(),
        vec![
            (
                TaskDefinition::new(TaskCode::new(TASK_CODE), OUTLIVING_TASK_TIMEOUT)
                    .with_max_lifetime(OUTLIVING_TASK_TIMEOUT),
                task_fn(|ctx| async move {
                    ctx.cancel_token().cancelled().await;
                    Ok(TaskOutcome::Cancelled)
                }),
            ),
            (
                TaskDefinition::new(TaskCode::new(DEGRADED_TASK_CODE), TASK_TIMEOUT)
                    .with_dependencies(vec![TaskRef::initial(definition_id, 0)])
                    .with_dependency_tolerance(DependencyTolerance {
                        allows_failed: true,
                        allows_skipped: false,
                    }),
                degraded_executor,
            ),
        ],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let inner: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
    let storage = Arc::new(
        ContendingStorage::new(Arc::clone(&inner)).with_contended_save(ContendedSave::OfTask {
            code: TaskCode::new(DEGRADED_TASK_CODE),
            status: TaskStatus::Started,
        }),
    );
    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage) as Arc<dyn Storage>,
        manager_config(),
        Arc::clone(&job_registry),
        vec![job_def],
    )?;
    manager_env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    manager_env.stop().await;

    assert_eq!(
        storage.interferences(),
        1,
        "the fixture must have made the save of the picked dependent lose exactly one race"
    );
    let (is_dependency_failed, resolution_reason) = timeout(WAIT_TIMEOUT, reports_receiver.recv())
        .await?
        .ok_or("the dependent must have run and reported what it started on")?;
    assert!(
        is_dependency_failed,
        "the dependent must start on a dependency the merge failed for outliving its lifetime"
    );
    assert!(
        resolution_reason.contains("outlived its maximum lifetime"),
        "the dependent must be able to read why its dependency will never run, got: {resolution_reason}"
    );

    let job = inner.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        *job.status(),
        JobStatus::Completed,
        "the failure was handled by the dependent that ran on it"
    );

    Ok(())
}
