use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::{Mutex, oneshot},
    time::timeout,
};

use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobsManager, JobsManagerConfig, NoopMetrics, TaskCode,
    TaskDefinition, TaskLimits, TaskOutcome, WorkerConfig, task_fn,
};

#[test]
fn start_returns_error_when_worker_count_is_zero() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let storage = Arc::new(InMemoryStorage::new());

    let executor = task_fn(|_ctx| async { Ok(TaskOutcome::empty()) });
    let task_def = TaskDefinition::new(TaskCode::new("noop"), Duration::from_secs(1));

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("zero_worker_job"),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);

    let Err(_err) = JobsManager::new(
        storage,
        JobsManagerConfig {
            worker_count: 0,
            worker_config: WorkerConfig::default(),
            ..Default::default()
        },
        job_registry,
        Arc::new(NoopMetrics),
    ) else {
        panic!("manager creation should fail with zero workers")
    };

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_shutdown_cancels_executor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let storage = Arc::new(InMemoryStorage::new());
    let (started_tx, started_rx) = oneshot::channel();
    let started_tx = Arc::new(Mutex::new(Some(started_tx)));
    let cancelled = Arc::new(AtomicBool::new(false));

    let cancelled_flag = Arc::clone(&cancelled);
    let started_tx = Arc::clone(&started_tx);
    let executor = task_fn(move |ctx| {
        let cancelled_flag = Arc::clone(&cancelled_flag);
        let started_tx = Arc::clone(&started_tx);

        async move {
            let value = started_tx.lock().await.take();
            if let Some(tx) = value {
                let _ = tx.send(());
            }

            tokio::select! {
                () = ctx.cancel_token().cancelled() => {
                    cancelled_flag.store(true, Ordering::SeqCst);
                    // The shutdown was observed mid-work, so nothing is persisted and the task
                    // stays open for a later pickup.
                    Ok(TaskOutcome::Cancelled)
                }
                () = tokio::time::sleep(Duration::from_secs(30)) => {
                    Ok(TaskOutcome::empty())
                }
            }
        }
    });

    let task_def = TaskDefinition::new(TaskCode::new("long_task"), Duration::from_secs(10));

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("shutdown_job"),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);

    let manager = JobsManager::new(
        storage,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: super::common::build_worker_config(Duration::from_millis(50), Duration::ZERO),
            ..Default::default()
        },
        job_registry,
        Arc::new(NoopMetrics),
    )?;

    let handle = manager.start()?;

    timeout(Duration::from_secs(5), started_rx).await??;
    timeout(Duration::from_secs(5), handle.shutdown()).await??;

    assert!(cancelled.load(Ordering::SeqCst), "executor should observe cancellation");

    Ok(())
}
