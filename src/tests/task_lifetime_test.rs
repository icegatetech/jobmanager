use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::{sync::mpsc, time::timeout};
use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode,
    TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

const TASK_CODE: &str = "import";
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
        task.get_error().contains("outlived its maximum lifetime"),
        "the recorded reason must name the limit that ended the task, got: {}",
        task.get_error()
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
