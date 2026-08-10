use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    Job, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode,
    TaskDefinition, TaskExecutor, TaskLimits, TaskOutcome, task_fn,
};

/// How long a test waits for the single iteration to finish.
const ITERATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Code of the one task every job here is built around.
const TASK_CODE: &str = "task";

/// The default description of that task: long enough not to expire while its executor runs.
fn task_definition() -> TaskDefinition {
    TaskDefinition::new(TASK_CODE, Duration::from_secs(5))
}

/// Starts a one-job pool around `executor` on the in-memory backend.
///
/// The object store is not the subject here - what is, is how [`crate::Worker`] reads the outcome an
/// executor returned - so the backend only has to refuse the same conflicting writes, which the
/// in-memory one does.
fn start_one_task_job(
    job_code: &str,
    task_def: TaskDefinition,
    executor: Arc<dyn TaskExecutor>,
) -> Result<ManagerEnv, Box<dyn std::error::Error>> {
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new(job_code),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
        ..Default::default()
    };

    ManagerEnv::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>,
        config,
        job_registry,
        vec![job_def],
    )
}

/// Runs `executor` for exactly one iteration and returns the job as it was left in storage, so each
/// test asserts on persisted state rather than on what the executor believed it did.
///
/// An iteration that ends in failure is a finished iteration too, so this returns for both
/// outcomes.
async fn run_one_iteration(
    job_code: &str,
    task_def: TaskDefinition,
    executor: Arc<dyn TaskExecutor>,
) -> Result<Job, Box<dyn std::error::Error>> {
    let mut manager_env = start_one_task_job(job_code, task_def, executor)?;
    manager_env.wait_for_all_jobs_completion(ITERATION_TIMEOUT).await?;
    manager_env.stop().await;

    let job = manager_env
        .storage()
        .get_job(&JobCode::new(job_code), &CancellationToken::new())
        .await?;
    Ok(job)
}

/// The ordinary path: returning a payload closes the task, without the executor calling
/// `complete_task` at all.
#[tokio::test]
async fn completed_outcome_closes_the_task_without_an_explicit_call() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job = run_one_iteration(
        "outcome_completed",
        task_definition(),
        task_fn(|_ctx| async move { Ok(TaskOutcome::Completed(b"result".to_vec())) }),
    )
    .await?;

    assert_eq!(*job.status(), JobStatus::Completed);
    let task = job.get_tasks_by_code(&TaskCode::new(TASK_CODE)).remove(0);
    assert!(task.is_completed());
    assert_eq!(task.get_output(), b"result");
    Ok(())
}

/// `Deferred` leaves resolution to the executor: the worker must not close the task itself, and the
/// stored output is the one the executor wrote.
#[tokio::test]
async fn deferred_outcome_leaves_resolution_to_the_executor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job = run_one_iteration(
        "outcome_deferred",
        task_definition(),
        task_fn(|ctx| async move {
            let task_id = *ctx.id();
            ctx.job().complete_task(&task_id, b"by executor".to_vec())?;
            Ok(TaskOutcome::Deferred)
        }),
    )
    .await?;

    assert_eq!(*job.status(), JobStatus::Completed);
    let task = job.get_tasks_by_code(&TaskCode::new(TASK_CODE)).remove(0);
    assert!(task.is_completed());
    assert_eq!(task.get_output(), b"by executor");
    Ok(())
}

/// Failing is the other way to resolve a deferred task, and the only way an executor fails its own
/// task deliberately. The stored reason must be the executor's, not the worker's complaint about an
/// unresolved `Deferred`.
///
/// One attempt, so the failure is terminal and the iteration ends on it.
#[tokio::test]
async fn deferred_outcome_keeps_the_reason_an_executor_failed_its_task_with() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();

    let job = run_one_iteration(
        "outcome_deferred_failed",
        task_definition().with_max_attempts(1),
        task_fn(|ctx| async move {
            let task_id = *ctx.id();
            ctx.job().fail_task(&task_id, "rejected by executor")?;
            Ok(TaskOutcome::Deferred)
        }),
    )
    .await?;

    assert_eq!(*job.status(), JobStatus::Failed);
    let task = job.get_tasks_by_code(&TaskCode::new(TASK_CODE)).remove(0);
    assert!(task.is_failed());
    assert_eq!(task.get_error(), "rejected by executor");
    Ok(())
}

/// A `Deferred` the executor did not back up with a resolution fails the task right away, instead
/// of surfacing after the deadline as an iteration whose tasks ran out of attempts.
#[tokio::test]
async fn deferred_outcome_without_resolution_fails_the_task() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job = run_one_iteration(
        "outcome_deferred_unresolved",
        task_definition().with_max_attempts(1),
        task_fn(|_ctx| async { Ok(TaskOutcome::Deferred) }),
    )
    .await?;

    let task = job.get_tasks_by_code(&TaskCode::new(TASK_CODE)).remove(0);
    assert!(!task.is_completed());
    assert!(
        task.get_error().contains("Deferred"),
        "the failure must name the broken contract, got: {}",
        task.get_error()
    );
    Ok(())
}

/// `Cancelled` persists nothing: the task stays open and is picked up again later, and the job does
/// not reach a terminal state.
///
/// The iteration never finishes, so the pool is stopped on the executor's own report instead of
/// being waited out - a wait that has to time out costs the suite that timeout on every run.
#[tokio::test]
async fn cancelled_outcome_leaves_the_task_open() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let (executor_reports, mut executor_ran) = mpsc::channel::<()>(1);

    let mut manager_env = start_one_task_job(
        "outcome_cancelled",
        task_definition(),
        task_fn(move |_ctx| {
            let reports = executor_reports.clone();
            async move {
                // The report is what the test waits for; a full channel means it already arrived.
                let _ = reports.try_send(());
                Ok(TaskOutcome::Cancelled)
            }
        }),
    )?;

    tokio::time::timeout(ITERATION_TIMEOUT, executor_ran.recv())
        .await?
        .ok_or("the executor must report that it ran")?;
    manager_env.stop().await;

    let job = manager_env
        .storage()
        .get_job(&JobCode::new("outcome_cancelled"), &CancellationToken::new())
        .await?;
    assert_ne!(*job.status(), JobStatus::Completed);
    let task = job.get_tasks_by_code(&TaskCode::new(TASK_CODE)).remove(0);
    assert!(!task.is_completed());
    Ok(())
}

/// A returned error fails the task and records the executor's own message, which is what makes the
/// boxed error type worth having: the text comes from a foreign error, not from a wrapper.
#[tokio::test]
async fn returned_error_fails_the_task_with_its_own_message() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job = run_one_iteration(
        "outcome_error",
        task_definition().with_max_attempts(1),
        task_fn(|_ctx| async move {
            let parsed: u32 = "not a number".parse()?;
            Ok(TaskOutcome::Completed(parsed.to_string().into_bytes()))
        }),
    )
    .await?;

    let task = job.get_tasks_by_code(&TaskCode::new(TASK_CODE)).remove(0);
    assert!(!task.is_completed());
    assert!(
        task.get_error().contains("invalid digit found in string"),
        "got: {}",
        task.get_error()
    );
    Ok(())
}

/// The documented safety net: a panicking executor fails its own task instead of killing the worker
/// that ran it.
///
/// Two attempts, so the worker has to survive the first panic to spend the second - a worker that
/// died with it would leave the run count at one and the iteration unfinished.
#[tokio::test]
async fn a_panicking_executor_fails_its_task_and_leaves_the_worker_running() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let runs = Arc::new(AtomicUsize::new(0));
    let runs_in_executor = Arc::clone(&runs);

    let job = run_one_iteration(
        "outcome_panic",
        task_definition().with_max_attempts(2),
        task_fn(move |_ctx| {
            let runs = Arc::clone(&runs_in_executor);
            async move {
                runs.fetch_add(1, Ordering::SeqCst);
                panic!("executor exploded")
            }
        }),
    )
    .await?;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        2,
        "the worker must survive to retry the task"
    );
    let task = job.get_tasks_by_code(&TaskCode::new(TASK_CODE)).remove(0);
    assert!(!task.is_completed());
    assert!(
        task.get_error().contains("executor panicked") && task.get_error().contains("executor exploded"),
        "the failure must name both the cause and the panic's own message, got: {}",
        task.get_error()
    );
    Ok(())
}

/// A panic payload that is neither of the two string types carries nothing readable, and the task
/// has to say so rather than record an empty reason.
#[tokio::test]
async fn a_panic_with_an_unreadable_payload_is_recorded_as_unknown() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job = run_one_iteration(
        "outcome_panic_payload",
        task_definition().with_max_attempts(1),
        task_fn(|_ctx| async { std::panic::panic_any(42_u8) }),
    )
    .await?;

    let task = job.get_tasks_by_code(&TaskCode::new(TASK_CODE)).remove(0);
    assert!(!task.is_completed());
    assert!(task.get_error().contains("unknown panic"), "got: {}", task.get_error());
    Ok(())
}
