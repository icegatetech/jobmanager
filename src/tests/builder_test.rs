use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;

use super::common::counting_metrics::CountingMetrics;
use crate::{JobCode, JobsManager, MetricsSink, TaskDefinition, TaskLimits, TaskOutcome, task_fn};

/// Bound on the wait: a broken chain must fail the test rather than hang the suite.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// A chain of three initial tasks runs in the declared order, with none of them creating the next
/// one at runtime - the ordering comes from the declaration alone.
///
/// Two workers are deliberate: with one, the order could come from the polling sequence rather than
/// from the declared dependencies, and the test would stay green if they broke.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn chained_initial_tasks_run_in_declared_order() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let order = Arc::new(Mutex::new(Vec::new()));

    let record = |order: &Arc<Mutex<Vec<&'static str>>>, name: &'static str| {
        let order = Arc::clone(order);
        task_fn(move |_ctx| {
            let order = Arc::clone(&order);
            async move {
                order.lock().push(name);
                Ok(TaskOutcome::empty())
            }
        })
    };

    let manager = JobsManager::builder()
        .in_memory()
        .workers(2)
        .poll_interval(Duration::from_millis(100))
        .job("chained_job", |j| {
            j.max_iterations(1);
            let first = j.add_task(TaskDefinition::new("a", Duration::from_secs(5)), record(&order, "a"));
            let second = j.add_task(TaskDefinition::new("b", Duration::from_secs(5)), record(&order, "b"));
            let third = j.add_task(TaskDefinition::new("c", Duration::from_secs(5)), record(&order, "c"));
            j.chain(&[first, second, third]);
        })
        .build()
        .await?;

    let handle = manager.start()?;
    let job_code = JobCode::new("chained_job");
    tokio::time::timeout(WAIT_TIMEOUT, handle.wait_for_iteration_completion(&job_code)).await??;
    handle.shutdown().await?;

    assert_eq!(*order.lock(), vec!["a", "b", "c"]);
    Ok(())
}

/// An initial task carries the input its definition declares, iteration after iteration - the
/// builder must not drop the payload on its way into the job.
#[tokio::test]
async fn an_initial_task_receives_the_declared_input() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let seen_input = Arc::new(Mutex::new(Vec::new()));
    let seen_in_executor = Arc::clone(&seen_input);

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(100))
        .job("input_job", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("with_input", Duration::from_secs(5)).with_input(b"declared".to_vec()),
                task_fn(move |ctx| {
                    let seen = Arc::clone(&seen_in_executor);
                    let input = ctx.input().to_vec();
                    async move {
                        *seen.lock() = input;
                        Ok(TaskOutcome::empty())
                    }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_iteration_completion(&JobCode::new("input_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert_eq!(*seen_input.lock(), b"declared".to_vec());
    Ok(())
}

/// Payload caps a job was described with, below the cap, exactly at it, and above it.
///
/// The setter is `const` and stores its argument unexamined, so an empty body would silently leave
/// every job on the 10 `MiB` default - which is what makes a cap lowered for protection worth a
/// test of its own.
const NARROW_PAYLOAD_LIMITS: TaskLimits = TaskLimits {
    max_input_bytes: 4,
    max_output_bytes: 4,
};

#[tokio::test]
async fn a_lowered_input_cap_rejects_an_initial_task_above_it() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let error = JobsManager::builder()
        .in_memory()
        .job("oversized_input_job", |j| {
            j.task_limits(NARROW_PAYLOAD_LIMITS);
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(5)).with_input(b"12345".to_vec()),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            );
        })
        .build()
        .await
        .err()
        .ok_or("an input above the job's cap must not build")?;

    assert!(error.to_string().contains("input size"), "got: {error}");
    Ok(())
}

/// The cap is the largest accepted input, not the smallest rejected one.
#[tokio::test]
async fn a_lowered_input_cap_accepts_an_initial_task_exactly_at_it() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(20))
        .job("input_at_cap_job", |j| {
            j.max_iterations(1);
            j.task_limits(NARROW_PAYLOAD_LIMITS);
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(5)).with_input(b"1234".to_vec()),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("input_at_cap_job")),
    )
    .await??;
    handle.shutdown().await?;
    Ok(())
}

/// The output cap is checked where an executor closes its own task, and the refusal leaves the task
/// unfinished rather than storing a truncated payload.
///
/// The executor completes with an accepted payload afterwards, so the job still finishes. Closing
/// it twice would not work: the worker completes the task itself once the executor returns
/// `Completed`, and a second close of an already-completed task is an error of its own.
#[tokio::test]
async fn a_lowered_output_cap_refuses_an_oversized_completion() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let rejection = Arc::new(Mutex::new(None));
    let rejection_in_executor = Arc::clone(&rejection);

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(20))
        .job("oversized_output_job", move |j| {
            j.max_iterations(1);
            j.task_limits(NARROW_PAYLOAD_LIMITS);
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(5)),
                task_fn(move |ctx| {
                    let rejection = Arc::clone(&rejection_in_executor);
                    async move {
                        let task_id = *ctx.id();
                        *rejection.lock() = ctx.job().complete_task(&task_id, vec![0; 5]).err().map(|e| e.to_string());
                        Ok(TaskOutcome::Completed(vec![0; 4]))
                    }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("oversized_output_job")),
    )
    .await??;
    handle.shutdown().await?;

    let rejection = rejection.lock().take();
    let error = rejection.as_ref().ok_or("an output above the job's cap must be refused")?;
    assert!(error.contains("output size"), "got: {error}");
    Ok(())
}

/// A cap raised above the default is what tells an applied limit from the default one: this payload
/// is over the 10 `MiB` a job without the setter would be held to.
#[tokio::test]
async fn a_raised_input_cap_accepts_a_payload_above_the_default() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let payload_bytes = 12 * 1024 * 1024;
    let seen_input_bytes = Arc::new(Mutex::new(0_usize));
    let seen_in_executor = Arc::clone(&seen_input_bytes);

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(20))
        .job("large_input_job", move |j| {
            j.max_iterations(1);
            j.task_limits(TaskLimits {
                max_input_bytes: 20 * 1024 * 1024,
                ..TaskLimits::default()
            });
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(10)).with_input(vec![0; payload_bytes]),
                task_fn(move |ctx| {
                    let seen = Arc::clone(&seen_in_executor);
                    let input_bytes = ctx.input().len();
                    async move {
                        *seen.lock() = input_bytes;
                        Ok(TaskOutcome::empty())
                    }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("large_input_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert_eq!(*seen_input_bytes.lock(), payload_bytes);
    Ok(())
}

/// The registration a task created at runtime is looked up by. Without it the task is created but
/// never executed, so the iteration it belongs to cannot finish.
#[tokio::test]
async fn a_runtime_task_of_a_registered_code_is_executed() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let sink = Arc::new(CountingMetrics::default());
    let child_ran = Arc::new(AtomicBool::new(false));
    let child_ran_in_executor = Arc::clone(&child_ran);

    let manager = JobsManager::builder()
        .in_memory()
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(20))
        .job("registered_runtime_task_job", move |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("plan", Duration::from_secs(5)),
                task_fn(|ctx| async move {
                    ctx.job().add_task(TaskDefinition::new("child", Duration::from_secs(5)))?;
                    Ok(TaskOutcome::empty())
                }),
            );
            j.add_task_executor(
                "child",
                task_fn(move |_ctx| {
                    let child_ran = Arc::clone(&child_ran_in_executor);
                    async move {
                        child_ran.store(true, Ordering::SeqCst);
                        Ok(TaskOutcome::empty())
                    }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("registered_runtime_task_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert!(
        child_ran.load(Ordering::SeqCst),
        "the registered executor must have run"
    );
    assert_eq!(sink.completed_iterations(), 1);
    assert_eq!(sink.failed_iterations(), 0);
    Ok(())
}

/// The mirror of the test above: an unregistered code leaves the task with nothing to run it, and
/// the iteration ends as failed once the task's single attempt has passed its deadline.
///
/// The reason itself never leaves the log - the executor is looked up before the task can be
/// resolved - so the iteration status is what the pool exposes about it.
#[tokio::test]
async fn a_runtime_task_of_an_unregistered_code_fails_its_iteration() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let sink = Arc::new(CountingMetrics::default());
    let manager = JobsManager::builder()
        .in_memory()
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(20))
        .job("unregistered_runtime_task_job", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("plan", Duration::from_secs(5)),
                task_fn(|ctx| async move {
                    ctx.job().add_task(
                        TaskDefinition::new("nobody_runs_this", Duration::from_millis(300)).with_max_attempts(1),
                    )?;
                    Ok(TaskOutcome::empty())
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("unregistered_runtime_task_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert_eq!(sink.failed_iterations(), 1);
    assert_eq!(sink.completed_iterations(), 0);
    Ok(())
}
