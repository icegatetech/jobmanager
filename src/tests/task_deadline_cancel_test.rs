use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio::{sync::Notify, time::timeout};
use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    Error, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode,
    TaskDefinition, TaskExecutor, TaskLimits, TaskOutcome, task_fn,
};

const TASK_CODE: &str = "long_running";
/// Deadline of the task, and so how long its executor runs before its token is cancelled.
const TASK_TIMEOUT: Duration = Duration::from_millis(300);
/// Far longer than the deadline: an executor still running when this elapses would mean the
/// cancellation never reached it.
const WORK_DURATION: Duration = Duration::from_secs(30);
/// Bound on every wait, so a job that never finishes fails the test instead of hanging it.
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);

fn manager_config(worker_count: usize) -> JobsManagerConfig {
    JobsManagerConfig {
        worker_count,
        worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO)
            .with_max_poll_interval(Duration::from_millis(50))
            .expect("a ceiling above the poll interval is accepted"),
        ..Default::default()
    }
}

async fn run_until_completed(
    job_code: &JobCode,
    executor: Arc<dyn TaskExecutor>,
    worker_count: usize,
) -> Result<crate::Job, Box<dyn std::error::Error>> {
    let task_def = TaskDefinition::new(TaskCode::new(TASK_CODE), TASK_TIMEOUT);
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
        manager_config(worker_count),
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    manager_env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    manager_env.stop().await;

    Ok(storage.get_job(job_code, &CancellationToken::new()).await?)
}

/// An executor that selects on its token is released by the task's own deadline, not only by a
/// shutdown: the pool here is never stopped while the task runs, so the cancellation can only come
/// from the deadline passing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_the_deadline_cancels_the_token_of_a_running_executor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let saw_cancellation = Arc::new(AtomicBool::new(false));

    let executions_in_executor = Arc::clone(&executions);
    let saw_cancellation_in_executor = Arc::clone(&saw_cancellation);
    let executor = task_fn(move |ctx| {
        let executions = Arc::clone(&executions_in_executor);
        let saw_cancellation = Arc::clone(&saw_cancellation_in_executor);

        async move {
            let execution = executions.fetch_add(1, Ordering::SeqCst) + 1;
            // Only the first execution waits for the deadline; the next one completes the task, so
            // the test ends on the job rather than on a timeout.
            if execution > 1 {
                return Ok(TaskOutcome::Completed(b"done".to_vec()));
            }

            tokio::select! {
                () = ctx.cancel_token().cancelled() => {
                    saw_cancellation.store(true, Ordering::SeqCst);
                    Ok(TaskOutcome::Cancelled)
                }
                () = tokio::time::sleep(WORK_DURATION) => Ok(TaskOutcome::empty()),
            }
        }
    });

    let job = run_until_completed(&JobCode::new("deadline_cancel_job"), executor, 1).await?;

    assert!(
        saw_cancellation.load(Ordering::SeqCst),
        "the deadline must cancel the token of the executor holding the task"
    );
    assert_eq!(*job.status(), JobStatus::Completed);
    let tasks = job.get_tasks_by_code(&TaskCode::new(TASK_CODE));
    let task = tasks.first().ok_or("the finished job must still hold its task")?;
    assert_eq!(
        task.attempts(),
        1,
        "the cancelled execution wrote nothing, so the takeover that completed the task spent no attempt"
    );

    Ok(())
}

/// The counterpart: cancellation is cooperative. An executor that ignores its token keeps running
/// past the deadline while another worker takes the task over, which is why two executors of one
/// task remain a legal state.
///
/// The overlap is reached by message rather than by outwaiting a sleep: the first executor runs
/// until the takeover reports itself, so a scheduler that starts the second worker late costs the
/// test nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_an_executor_ignoring_its_token_keeps_running_while_its_task_is_taken_over()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let finished_past_its_deadline = Arc::new(AtomicBool::new(false));
    let takeover_started = Arc::new(Notify::new());

    let executions_in_executor = Arc::clone(&executions);
    let finished_in_executor = Arc::clone(&finished_past_its_deadline);
    let takeover_in_executor = Arc::clone(&takeover_started);
    let executor = task_fn(move |_ctx| {
        let executions = Arc::clone(&executions_in_executor);
        let finished = Arc::clone(&finished_in_executor);
        let takeover_started = Arc::clone(&takeover_in_executor);

        async move {
            let execution = executions.fetch_add(1, Ordering::SeqCst) + 1;
            if execution > 1 {
                // A permit is kept when nobody is waiting yet, so the report cannot be missed.
                takeover_started.notify_one();
                return Ok(TaskOutcome::Completed(b"done".to_vec()));
            }

            // The token is never selected on, so this waits past the deadline and into the
            // takeover. Reaching the line below is the whole point - a cancellation that dropped
            // the future instead of signalling it would leave this executor stuck at the await
            // above.
            timeout(WAIT_TIMEOUT, takeover_started.notified())
                .await
                .map_err(|_| Error::Other("the expired task was never taken over".to_string()))?;
            finished.store(true, Ordering::SeqCst);
            Ok(TaskOutcome::Completed(b"late".to_vec()))
        }
    });

    let job = run_until_completed(&JobCode::new("ignored_cancel_job"), executor, 2).await?;

    assert!(
        finished_past_its_deadline.load(Ordering::SeqCst),
        "the first executor must run to its end rather than be torn down at its deadline"
    );
    assert!(
        executions.load(Ordering::SeqCst) >= 2,
        "the expired task must have been taken over while its first executor was still running"
    );
    assert_eq!(*job.status(), JobStatus::Completed);

    Ok(())
}
