use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::storage_wrapper::{ContendedSave, ContendingStorage};
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    Error, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode,
    TaskContext, TaskDefinition, TaskExecutor, TaskLimits, TaskOutcome, TaskRef, TaskStatus, task_fn,
};

const PLAN_TASK_CODE: &str = "plan";
const PLANNED_TASK_CODE: &str = "planned";
/// Tasks the planning executor creates on every execution.
const PLANNED_TASK_COUNT: usize = 2;
const TASK_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on every wait, so a job that never finishes fails the test instead of hanging it.
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);

fn manager_config() -> JobsManagerConfig {
    JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO)
            .with_max_poll_interval(Duration::from_millis(50))
            .expect("a ceiling above the poll interval is accepted"),
        ..Default::default()
    }
}

/// Plans the follow-up work of one execution: a chain in which every task waits for the one planned
/// before it, so a rollback that dropped only part of it would leave a task waiting for a
/// dependency that no longer exists.
fn plan_tasks(ctx: &TaskContext) -> Result<(), Error> {
    let mut previous: Option<TaskRef> = None;
    for _ in 0..PLANNED_TASK_COUNT {
        let definition = TaskDefinition::new(TaskCode::new(PLANNED_TASK_CODE), TASK_TIMEOUT);
        let definition = match previous {
            Some(dependency) => definition.with_dependencies(vec![dependency]),
            None => definition,
        };
        previous = Some(ctx.job().add_task(definition)?);
    }

    Ok(())
}

/// An executor that plans a chain of tasks and then refuses its own task on its first
/// `failing_executions` runs.
fn planning_executor(executions: &Arc<AtomicU32>, failing_executions: u32) -> Arc<dyn TaskExecutor> {
    let executions = Arc::clone(executions);
    task_fn(move |ctx| {
        let executions = Arc::clone(&executions);

        async move {
            let execution = executions.fetch_add(1, Ordering::SeqCst) + 1;

            plan_tasks(&ctx)?;

            if execution <= failing_executions {
                return Err(Error::Other("planning failed after creating its tasks".to_string()).into());
            }
            Ok(TaskOutcome::empty())
        }
    })
}

/// An executor that plans a chain of tasks and then fails its own task through the job handle,
/// returning [`TaskOutcome::Deferred`] - the deliberate failure, which the rollback covers like any
/// other.
fn self_failing_planning_executor(executions: &Arc<AtomicU32>) -> Arc<dyn TaskExecutor> {
    let executions = Arc::clone(executions);
    task_fn(move |ctx| {
        let executions = Arc::clone(&executions);

        async move {
            executions.fetch_add(1, Ordering::SeqCst);

            plan_tasks(&ctx)?;
            ctx.job().fail_task(ctx.id(), "planning refused the work deliberately")?;

            Ok(TaskOutcome::Deferred)
        }
    })
}

/// An executor that resolves its own task first and only then plans work - the order a rollback
/// that already ran cannot cover, so the registration itself has to be refused. The refusals are
/// counted, because a test that only checked the stored iteration would pass just as well on a
/// planning call that never happened.
fn planning_after_its_own_failure_executor(
    executions: &Arc<AtomicU32>,
    refusals: &Arc<AtomicU32>,
) -> Arc<dyn TaskExecutor> {
    let executions = Arc::clone(executions);
    let refusals = Arc::clone(refusals);
    task_fn(move |ctx| {
        let executions = Arc::clone(&executions);
        let refusals = Arc::clone(&refusals);

        async move {
            executions.fetch_add(1, Ordering::SeqCst);

            ctx.job().fail_task(ctx.id(), "planning refused the work deliberately")?;
            if ctx
                .job()
                .add_task(TaskDefinition::new(TaskCode::new(PLANNED_TASK_CODE), TASK_TIMEOUT))
                .is_err()
            {
                refusals.fetch_add(1, Ordering::SeqCst);
            }

            Ok(TaskOutcome::Deferred)
        }
    })
}

/// An executor that closes its own task successfully and only then plans the work that follows it -
/// the order a rollback never reaches, because a completed task rolls nothing back.
fn planning_after_its_own_completion_executor(executions: &Arc<AtomicU32>) -> Arc<dyn TaskExecutor> {
    let executions = Arc::clone(executions);
    task_fn(move |ctx| {
        let executions = Arc::clone(&executions);

        async move {
            executions.fetch_add(1, Ordering::SeqCst);

            ctx.job().complete_task(ctx.id(), Vec::new())?;
            plan_tasks(&ctx)?;

            Ok(TaskOutcome::Deferred)
        }
    })
}

/// The same order ending in an error instead of [`TaskOutcome::Deferred`]: the execution failed,
/// but its task was already completed, so what the worker has to persist is the result the executor
/// wrote - the completion and the chain planned on it.
fn failing_after_its_own_completion_executor(executions: &Arc<AtomicU32>) -> Arc<dyn TaskExecutor> {
    let executions = Arc::clone(executions);
    task_fn(move |ctx| {
        let executions = Arc::clone(&executions);

        async move {
            executions.fetch_add(1, Ordering::SeqCst);

            ctx.job().complete_task(ctx.id(), Vec::new())?;
            plan_tasks(&ctx)?;

            Err(Error::Other("failed after reporting its result".to_string()).into())
        }
    })
}

async fn run_job(
    job_code: &JobCode,
    plan_executor: Arc<dyn TaskExecutor>,
    max_attempts: u32,
    storage: Arc<dyn Storage>,
) -> Result<crate::Job, Box<dyn std::error::Error>> {
    let plan_def = TaskDefinition::new(TaskCode::new(PLAN_TASK_CODE), TASK_TIMEOUT).with_max_attempts(max_attempts);
    let planned_executor = task_fn(|_ctx| async { Ok(TaskOutcome::empty()) });
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(plan_def, plan_executor)],
        vec![(TaskCode::new(PLANNED_TASK_CODE), planned_executor)],
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage),
        manager_config(),
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    manager_env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    manager_env.stop().await;

    Ok(storage.get_job(job_code, &CancellationToken::new()).await?)
}

/// Work planned by an execution that then failed was planned on an assumption that did not hold,
/// so it must not reach storage: the iteration ends with the planning task alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tasks_created_by_a_failed_execution_are_not_stored() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let job = run_job(
        &JobCode::new("rollback_job"),
        planning_executor(&executions, u32::MAX),
        1,
        Arc::new(InMemoryStorage::new()),
    )
    .await?;

    assert_eq!(*job.status(), JobStatus::Failed);
    assert_eq!(executions.load(Ordering::SeqCst), 1, "one attempt, one execution");
    assert!(
        job.get_tasks_by_code(&TaskCode::new(PLANNED_TASK_CODE)).is_empty(),
        "the tasks the failed execution created must be gone"
    );
    assert_eq!(
        job.tasks_as_iter().count(),
        1,
        "the iteration must hold nothing but the planning task"
    );

    Ok(())
}

/// The retry plans again, so the rollback has to leave the failed execution's tasks out rather
/// than merely hide them: kept, they would be created a second time and run twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_retry_of_a_failed_execution_does_not_duplicate_the_tasks_it_created()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let job = run_job(
        &JobCode::new("rollback_retry_job"),
        planning_executor(&executions, 1),
        2,
        Arc::new(InMemoryStorage::new()),
    )
    .await?;

    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "the planning task must be retried"
    );
    assert_eq!(
        job.get_tasks_by_code(&TaskCode::new(PLANNED_TASK_CODE)).len(),
        PLANNED_TASK_COUNT,
        "only the tasks of the execution that succeeded may be stored"
    );

    Ok(())
}

/// Failing a task through the handle is a failure like any other: the work planned alongside it is
/// dropped too, so there is no way to leave a plan behind by refusing the task that made it.
///
/// A budget of two, so the retry the deliberate failure leaves the task open for is covered here as
/// well: it replans, and the rollback is what keeps the second run from stacking a duplicate chain
/// on the first one. Removing tasks is not something an executor can do through its handle, which is
/// why the rollback has to cover this path rather than leaving the cleanup to whoever wrote the
/// executor.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tasks_created_by_executions_that_failed_their_own_task_are_not_stored()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let job = run_job(
        &JobCode::new("rollback_deliberate_failure_job"),
        self_failing_planning_executor(&executions),
        2,
        Arc::new(InMemoryStorage::new()),
    )
    .await?;

    assert_eq!(*job.status(), JobStatus::Failed);
    assert_eq!(
        executions.load(Ordering::SeqCst),
        2,
        "a deliberate failure leaves the task pickable while its budget lasts"
    );
    let plan_task = job
        .get_tasks_by_code(&TaskCode::new(PLAN_TASK_CODE))
        .pop()
        .ok_or("the planning task must be in the stored iteration")?;
    assert!(plan_task.is_failed(), "the executor failed its own task");
    assert!(
        job.get_tasks_by_code(&TaskCode::new(PLANNED_TASK_CODE)).is_empty(),
        "each execution's chain goes with it, so neither run leaves a copy behind"
    );

    Ok(())
}

/// The rollback covers the whole execution rather than the moment it ran: an executor that fails
/// its own task and only then plans work would otherwise leave that work behind, since the rollback
/// had nothing to drop when it ran. Such a registration is refused, so the iteration ends with the
/// planning task alone whatever order the executor calls in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_task_planned_after_its_own_execution_failed_is_not_stored() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let refusals = Arc::new(AtomicU32::new(0));
    let job = run_job(
        &JobCode::new("rollback_late_planning_job"),
        planning_after_its_own_failure_executor(&executions, &refusals),
        1,
        Arc::new(InMemoryStorage::new()),
    )
    .await?;

    assert_eq!(executions.load(Ordering::SeqCst), 1, "one attempt, one execution");
    assert_eq!(
        refusals.load(Ordering::SeqCst),
        1,
        "the fixture must really have tried to plan work after resolving its own task"
    );
    assert_eq!(*job.status(), JobStatus::Failed);
    assert_eq!(
        job.tasks_as_iter().count(),
        1,
        "the iteration must hold nothing but the planning task"
    );

    Ok(())
}

/// The other order of the same pair of calls, and the boundary of the rule above: an execution that
/// completed its own task rolled nothing back, so the work it plans afterwards has nothing to
/// outlive and reaches storage like any other. Closing the task first and planning the continuation
/// after it is what a chained job does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tasks_planned_after_its_own_execution_completed_are_stored() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let job = run_job(
        &JobCode::new("rollback_late_planning_completed_job"),
        planning_after_its_own_completion_executor(&executions),
        1,
        Arc::new(InMemoryStorage::new()),
    )
    .await?;

    assert_eq!(executions.load(Ordering::SeqCst), 1, "one attempt, one execution");
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(
        job.get_tasks_by_code(&TaskCode::new(PLANNED_TASK_CODE)).len(),
        PLANNED_TASK_COUNT,
        "the work planned after the task was closed must be stored and run"
    );

    Ok(())
}

/// The conflict path of a result its executor resolved itself: the save carrying the completed
/// planning task loses its race, and the merge that follows has to carry both the completion and
/// the chain planned on it into the stored iteration. A merge that dropped either would leave the
/// task to be run again - the execution around it failed, and only the result the executor wrote
/// says the work is done.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_a_result_its_executor_completed_survives_a_conflicting_save() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let storage = Arc::new(
        ContendingStorage::new(Arc::new(InMemoryStorage::new())).with_contended_save(ContendedSave::OfTask {
            code: TaskCode::new(PLAN_TASK_CODE),
            status: TaskStatus::Completed,
        }),
    );
    let job = run_job(
        &JobCode::new("rollback_completed_conflict_job"),
        failing_after_its_own_completion_executor(&executions),
        1,
        Arc::clone(&storage) as Arc<dyn Storage>,
    )
    .await?;

    assert_eq!(
        storage.interferences(),
        1,
        "the fixture must have made the save of the completed task lose its race"
    );
    assert_eq!(executions.load(Ordering::SeqCst), 1, "one attempt, one execution");
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(
        job.get_tasks_by_code(&TaskCode::new(PLANNED_TASK_CODE)).len(),
        PLANNED_TASK_COUNT,
        "the merge after the conflict must carry the planned work over"
    );

    Ok(())
}

/// The rollback has to survive the conflict path too: the save carrying the failed task loses its
/// race, and the merge that follows must not carry the rolled-back tasks back into the stored
/// iteration.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_tasks_of_a_failed_execution_stay_dropped_when_its_save_conflicts()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let storage = Arc::new(
        ContendingStorage::new(Arc::new(InMemoryStorage::new())).with_contended_save(ContendedSave::OfTask {
            code: TaskCode::new(PLAN_TASK_CODE),
            status: TaskStatus::Failed,
        }),
    );
    let job = run_job(
        &JobCode::new("rollback_conflict_job"),
        planning_executor(&executions, u32::MAX),
        1,
        Arc::clone(&storage) as Arc<dyn Storage>,
    )
    .await?;

    assert_eq!(
        storage.interferences(),
        1,
        "the fixture must have made the save of the failed task lose its race"
    );
    assert_eq!(*job.status(), JobStatus::Failed);
    assert!(
        job.get_tasks_by_code(&TaskCode::new(PLANNED_TASK_CODE)).is_empty(),
        "the merge after the conflict must not bring the rolled-back tasks back"
    );
    assert_eq!(
        job.tasks_as_iter().count(),
        1,
        "the iteration must hold nothing but the planning task"
    );

    Ok(())
}
