use std::{sync::Arc, time::Duration};

use tokio::sync::{Notify, mpsc};
use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode,
    TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// Bound on every wait here: a background task that never reports must fail the test rather than
/// hang the suite.
const PROGRESS_TIMEOUT: Duration = Duration::from_secs(10);

/// An executor is free to move its [`TaskContext`](crate::TaskContext) into a task that outlives
/// its own call, and the worker cannot then reclaim the job state it lent out. It has to carry on
/// anyway - copying the state out from under the lock - and the escaped handle has to stay refused.
///
/// The background task is parked until the job is already persisted, so it is holding the handle at
/// the moment the worker tries to reclaim it. That is what makes the reclaim fail here by
/// construction rather than by timing.
#[tokio::test]
async fn a_handle_kept_by_a_background_task_leaves_the_job_saved_and_refuses_the_late_call()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let release_background_task = Arc::new(Notify::new());
    let release_in_executor = Arc::clone(&release_background_task);
    let (late_call_reports, mut late_call_receiver) = mpsc::channel::<Option<String>>(1);

    let job_code = JobCode::new("escaped_handle_job");
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            TaskDefinition::new("escaping", Duration::from_secs(10)),
            task_fn(move |ctx| {
                let release = Arc::clone(&release_in_executor);
                let reports = late_call_reports.clone();
                async move {
                    let task_id = *ctx.id();
                    tokio::spawn(async move {
                        release.notified().await;
                        let late_call = ctx.job().complete_task(&task_id, b"late".to_vec());
                        let _ = reports.send(late_call.err().map(|e| e.to_string())).await;
                    });
                    Ok(TaskOutcome::empty())
                }
            }),
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    let mut manager_env = ManagerEnv::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
            ..Default::default()
        },
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    manager_env.wait_for_all_jobs_completion(PROGRESS_TIMEOUT).await?;

    let job = manager_env.storage().get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    let task = job.get_tasks_by_code(&TaskCode::new("escaping")).remove(0);
    assert!(
        task.is_completed(),
        "the worker must persist the task it could not reclaim the state for"
    );

    release_background_task.notify_one();
    let late_call_error = tokio::time::timeout(PROGRESS_TIMEOUT, late_call_receiver.recv())
        .await?
        .ok_or("the background task must report its late call")?
        .ok_or("a handle that outlived its call must refuse the late mutation")?;
    assert!(late_call_error.contains("no longer valid"), "got: {late_call_error}");

    manager_env.stop().await;
    Ok(())
}
