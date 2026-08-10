use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::s3_container::S3TestContainer;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStateCodecKind, JobStatus, JobsManagerConfig, NoopMetrics,
    S3Storage, S3StorageConfig, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// `TestDynamicTaskCreation` verifies that an executor can create new tasks dynamically, and that
/// the dependencies it declares between them survive a round-trip through the object store.
#[tokio::test]
async fn test_dynamic_task_creation() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    // 1. Start object storage
    let store = S3TestContainer::start().await?;

    // 2. Track task execution
    let dynamic_task_count = 5;
    let init_task_executed = Arc::new(AtomicBool::new(false));
    let dynamic_tasks_executed = Arc::new(AtomicI32::new(0));

    let init_executed_clone = Arc::clone(&init_task_executed);

    // Initial task creates multiple dynamic tasks
    let init_executor = task_fn(move |ctx| {
        let executed = Arc::clone(&init_executed_clone);

        async move {
            executed.store(true, Ordering::SeqCst);

            // Every dynamic task but the first waits for the first, so a non-empty dependency list
            // is what reaches the store.
            let mut first_dynamic_task = None;
            #[allow(clippy::cast_possible_truncation)]
            for i in 0..dynamic_task_count {
                let mut dynamic_task_def = TaskDefinition::new(TaskCode::new("dynamic_task"), Duration::from_secs(5))
                    .with_input(vec![i as u8]);
                if let Some(supplier) = first_dynamic_task {
                    dynamic_task_def = dynamic_task_def.with_dependencies(vec![supplier]);
                }
                let dynamic_task = ctx.job().add_task(dynamic_task_def)?;
                first_dynamic_task.get_or_insert(dynamic_task);
            }

            Ok(TaskOutcome::empty())
        }
    });

    let dynamic_executed_clone = Arc::clone(&dynamic_tasks_executed);

    // Dynamic task executor
    let dynamic_executor = task_fn(move |_ctx| {
        let executed = Arc::clone(&dynamic_executed_clone);

        async move {
            executed.fetch_add(1, Ordering::SeqCst);
            Ok(TaskOutcome::Completed(b"done".to_vec()))
        }
    });

    let init_task_def = TaskDefinition::new(TaskCode::new("init_task"), Duration::from_secs(5));

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("test_dynamic_job"),
        vec![(init_task_def, init_executor)],
        vec![(TaskCode::new("dynamic_task"), dynamic_executor)],
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    // 3. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    // 4. Create storage
    let storage = S3Storage::new(
        S3StorageConfig::new(
            store.endpoint(),
            store.username(),
            store.password(),
            "test-jobs",
            "us-east-1",
        )
        .with_job_state_codec(JobStateCodecKind::Json),
        job_registry.clone(),
        Arc::new(NoopMetrics),
    )
    .await?;

    // 5. Start manager
    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(100), Duration::from_millis(10)),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(Arc::new(storage), config, Arc::clone(&job_registry), vec![job_def])?;

    // 6. Wait for completion
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    // 7. Verify all tasks executed
    assert!(
        init_task_executed.load(Ordering::SeqCst),
        "init task should be executed"
    );
    assert_eq!(
        dynamic_tasks_executed.load(Ordering::SeqCst),
        i32::try_from(dynamic_task_count)?,
        "all dynamic tasks should be executed"
    );

    // Verify final job state
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_dynamic_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);

    // Verify task count: 1 init + N dynamic
    let tasks = job.get_tasks_by_code(&TaskCode::new("dynamic_task"));
    assert_eq!(
        tasks.len(),
        dynamic_task_count,
        "should have correct number of dynamic tasks"
    );

    // Every task is identified by the input it was created with, so the expectation does not depend
    // on the identifiers the job minted.
    let supplier = tasks
        .iter()
        .find(|task| task.get_input() == [0u8].as_slice())
        .ok_or("the dynamic task the others wait for should be stored")?;
    for task in &tasks {
        let expected_dependencies = if task.get_input() == [0u8].as_slice() {
            Vec::new()
        } else {
            vec![*supplier.id()]
        };
        assert_eq!(
            task.depends_on().to_vec(),
            expected_dependencies,
            "dependencies of the dynamic task with input {:?} should survive storage",
            task.get_input()
        );
    }

    Ok(())
}
