use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManagerConfig, Storage, TaskCode,
    TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// `TestTaskDependencies` verifies that task dependencies are respected.
///
/// Blocking on a dependency is a rule of the job's own state, so the in-memory backend stands in
/// for the object store here.
#[tokio::test]
async fn test_task_dependencies() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let max_iterations = 1u64;

    // 1. Track task execution
    let dep_first_done = Arc::new(AtomicBool::new(false));
    let dep_second_done = Arc::new(AtomicBool::new(false));
    let final_done = Arc::new(AtomicBool::new(false));
    // What the final task saw of its dependencies when it started. Asserted after the wait: a
    // failed assertion inside an executor is caught by the worker and would only fail the task.
    let dependencies_done_before_final = Arc::new(AtomicBool::new(false));

    let init_executor = task_fn(move |ctx| async move {
        let dep_first_def =
            TaskDefinition::new(TaskCode::new("dep_a"), Duration::from_secs(5)).with_input(b"dep-a".to_vec());
        let dep_second_def =
            TaskDefinition::new(TaskCode::new("dep_b"), Duration::from_secs(5)).with_input(b"dep-b".to_vec());

        let dep_first_task = ctx.job().add_task(dep_first_def)?;
        let dep_second_task = ctx.job().add_task(dep_second_def)?;

        let final_def = TaskDefinition::new(TaskCode::new("final_task"), Duration::from_secs(5))
            .with_input(b"final".to_vec())
            .with_dependencies(vec![dep_first_task, dep_second_task]);

        ctx.job().add_task(final_def)?;
        Ok(TaskOutcome::Completed(b"init-done".to_vec()))
    });

    let dep_first_done_clone = Arc::clone(&dep_first_done);
    let dep_first_executor = task_fn(move |_ctx| {
        let dep_first_done = Arc::clone(&dep_first_done_clone);

        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            dep_first_done.store(true, Ordering::SeqCst);
            Ok(TaskOutcome::Completed(b"dep-a-done".to_vec()))
        }
    });

    let dep_second_done_clone = Arc::clone(&dep_second_done);
    let dep_second_executor = task_fn(move |_ctx| {
        let dep_second_done = Arc::clone(&dep_second_done_clone);

        async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            dep_second_done.store(true, Ordering::SeqCst);
            Ok(TaskOutcome::Completed(b"dep-b-done".to_vec()))
        }
    });

    let final_done_clone = Arc::clone(&final_done);
    let dep_first_done_clone = Arc::clone(&dep_first_done);
    let dep_second_done_clone = Arc::clone(&dep_second_done);
    let dependencies_done_clone = Arc::clone(&dependencies_done_before_final);
    let final_executor = task_fn(move |_ctx| {
        let final_done = Arc::clone(&final_done_clone);
        let dep_first_done = Arc::clone(&dep_first_done_clone);
        let dep_second_done = Arc::clone(&dep_second_done_clone);
        let dependencies_done = Arc::clone(&dependencies_done_clone);

        async move {
            dependencies_done.store(
                dep_first_done.load(Ordering::SeqCst) && dep_second_done.load(Ordering::SeqCst),
                Ordering::SeqCst,
            );
            final_done.store(true, Ordering::SeqCst);
            Ok(TaskOutcome::Completed(b"final-done".to_vec()))
        }
    });

    let init_def = TaskDefinition::new(TaskCode::new("init_task"), Duration::from_secs(5)).with_input(b"init".to_vec());

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("test_task_dependencies_job"),
        vec![(init_def, init_executor)],
        vec![
            (TaskCode::new("dep_a"), dep_first_executor),
            (TaskCode::new("dep_b"), dep_second_executor),
            (TaskCode::new("final_task"), final_executor),
        ],
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(max_iterations)?;

    // 2. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    // 3. Start manager
    let config = JobsManagerConfig {
        worker_count: 3,
        worker_config: super::common::build_worker_config(Duration::from_millis(50), Duration::from_millis(10)),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>,
        config,
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    // 4. Wait for completion
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    // 5. Verify
    assert!(dep_first_done.load(Ordering::SeqCst), "dep_a should execute");
    assert!(dep_second_done.load(Ordering::SeqCst), "dep_b should execute");
    assert!(final_done.load(Ordering::SeqCst), "final task should execute");
    assert!(
        dependencies_done_before_final.load(Ordering::SeqCst),
        "both dependencies should be completed before the final task runs"
    );

    // Verify job state in storage
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_task_dependencies_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), max_iterations);

    Ok(())
}
