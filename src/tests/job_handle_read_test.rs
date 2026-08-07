//! Reads a task performs through its [`JobHandle`](crate::JobHandle) and its
//! [`TaskContext`](crate::TaskContext).
//!
//! Every executor here writes what it read into a shared cell and the assertions stand after the
//! wait, because a panic inside an executor is caught by the worker and turned into a task failure -
//! an `assert!` there would fail the iteration instead of the test.

use std::{sync::Arc, time::Duration};

use parking_lot::Mutex;
use uuid::Uuid;

use crate::{JobCode, JobsManager, TaskCode, TaskDefinition, TaskOutcome, task_fn};

/// Bound on every wait here: a job that never finishes must fail the test rather than hang the
/// suite.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// What the join task read about the dependencies it waited for.
#[derive(Default)]
struct DependencyReads {
    dependency_count: usize,
    completed_dependencies: usize,
    rows_total: u64,
}

/// How many tasks the lookup by code returned, for a code the job holds and one it does not.
#[derive(Default)]
struct TasksByCodeReads {
    tasks_of_declared_code: usize,
    tasks_of_absent_code: usize,
}

/// What the solo task read about itself and about a task identifier its job does not hold.
#[derive(Default)]
struct SoloTaskReads {
    unknown_task_error: Option<String>,
    executing_task_code: Option<TaskCode>,
    is_cancelled: bool,
}

/// The join task reads each dependency's output through the handle and sums them, which is the
/// shape `examples/fan_out_join.rs` declares as the way to pass a result between tasks of one
/// iteration.
///
/// The chunk outputs are stated literally - 2 and 3, so 5 - rather than derived from what the
/// chunk executor computed.
#[tokio::test]
async fn a_join_task_sums_the_outputs_of_its_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let reads = Arc::new(Mutex::new(DependencyReads::default()));
    let reads_in_join = Arc::clone(&reads);

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(20))
        .job("fan_out_join_job", move |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("plan", Duration::from_secs(5)),
                task_fn(|ctx| async move {
                    let mut chunk_task_refs = Vec::with_capacity(2);
                    for rows in [b"2".to_vec(), b"3".to_vec()] {
                        let chunk = TaskDefinition::new("chunk", Duration::from_secs(5)).with_input(rows);
                        chunk_task_refs.push(ctx.job().add_task(chunk)?);
                    }
                    let join = TaskDefinition::new("join", Duration::from_secs(5)).with_dependencies(chunk_task_refs);
                    ctx.job().add_task(join)?;
                    Ok(TaskOutcome::empty())
                }),
            );
            // A chunk reports back the number of rows it was told to process.
            j.add_task_executor(
                "chunk",
                task_fn(|ctx| async move { Ok(TaskOutcome::Completed(ctx.input().to_vec())) }),
            );
            j.add_task_executor(
                "join",
                task_fn(move |ctx| {
                    let reads = Arc::clone(&reads_in_join);
                    async move {
                        let mut dependency_count = 0;
                        let mut completed_dependencies = 0;
                        let mut rows_total = 0;
                        for chunk_task_id in ctx.task().depends_on() {
                            dependency_count += 1;
                            let chunk_task = ctx.job().get_task(chunk_task_id)?;
                            // An empty output is ambiguous on its own, so completion is what
                            // separates "finished with nothing to say" from "not finished".
                            if !chunk_task.is_completed() {
                                continue;
                            }
                            completed_dependencies += 1;
                            rows_total += String::from_utf8(chunk_task.get_output().to_vec())?.parse::<u64>()?;
                        }
                        *reads.lock() = DependencyReads {
                            dependency_count,
                            completed_dependencies,
                            rows_total,
                        };
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
        handle.wait_for_job_completion(&JobCode::new("fan_out_join_job")),
    )
    .await??;
    handle.shutdown().await?;

    let reads = std::mem::take(&mut *reads.lock());
    assert_eq!(
        reads.dependency_count, 2,
        "the join task must see both chunks it waits for"
    );
    assert_eq!(
        reads.completed_dependencies, 2,
        "a dependency is completed by the time the task waiting for it runs"
    );
    assert_eq!(reads.rows_total, 5);
    Ok(())
}

/// Several tasks may share one code, which is why the lookup returns a collection; a code the job
/// does not have returns an empty one rather than an error.
#[tokio::test]
async fn get_tasks_by_code_returns_every_task_of_the_code_and_none_of_an_absent_one()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let reads = Arc::new(Mutex::new(TasksByCodeReads::default()));
    let reads_in_executor = Arc::clone(&reads);

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(20))
        .job("tasks_by_code_job", move |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("plan", Duration::from_secs(5)),
                task_fn(move |ctx| {
                    let reads = Arc::clone(&reads_in_executor);
                    async move {
                        for _ in 0..2 {
                            ctx.job().add_task(TaskDefinition::new("chunk", Duration::from_secs(5)))?;
                        }

                        *reads.lock() = TasksByCodeReads {
                            tasks_of_declared_code: ctx.job().get_tasks_by_code(&TaskCode::new("chunk"))?.len(),
                            tasks_of_absent_code: ctx.job().get_tasks_by_code(&TaskCode::new("absent"))?.len(),
                        };
                        Ok(TaskOutcome::empty())
                    }
                }),
            );
            j.add_task_executor("chunk", task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }));
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("tasks_by_code_job")),
    )
    .await??;
    handle.shutdown().await?;

    let reads = std::mem::take(&mut *reads.lock());
    assert_eq!(reads.tasks_of_declared_code, 2);
    assert_eq!(reads.tasks_of_absent_code, 0);
    Ok(())
}

/// An id the job does not hold is an error rather than an empty result: the executor asked about a
/// specific task, so silence would let it read a missing dependency as a finished one.
///
/// The two delegating accessors of the context are read here as well - they have no logic of their
/// own, so they need no fixture beyond a running task.
#[tokio::test]
async fn get_task_refuses_an_id_the_job_does_not_hold() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let reads = Arc::new(Mutex::new(SoloTaskReads::default()));
    let reads_in_executor = Arc::clone(&reads);

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(20))
        .job("unknown_task_job", move |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("solo", Duration::from_secs(5)),
                task_fn(move |ctx| {
                    let reads = Arc::clone(&reads_in_executor);
                    async move {
                        *reads.lock() = SoloTaskReads {
                            unknown_task_error: ctx.job().get_task(&Uuid::new_v4()).err().map(|e| e.to_string()),
                            executing_task_code: Some(ctx.code().clone()),
                            is_cancelled: ctx.is_cancelled(),
                        };
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
        handle.wait_for_job_completion(&JobCode::new("unknown_task_job")),
    )
    .await??;
    handle.shutdown().await?;

    let reads = std::mem::take(&mut *reads.lock());
    // The handle flattens the domain error into `Error::Other`, so the text is all a caller gets.
    let error = reads.unknown_task_error.as_ref().ok_or("a foreign task id must be refused")?;
    assert!(error.contains("task not found"), "got: {error}");
    assert_eq!(reads.executing_task_code, Some(TaskCode::new("solo")));
    assert!(!reads.is_cancelled, "no shutdown was requested while the task ran");
    Ok(())
}
