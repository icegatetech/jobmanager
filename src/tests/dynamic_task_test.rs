use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicI32, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::provider_harness::{ProviderHarness, ProviderStorageRequest};
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobDefinitionRegistry, JobRegistry, JobStateCodecKind, JobStatus,
    JobsManagerConfig, NoopMetrics, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// An executor can create new tasks dynamically, and the dependencies it declares between them
/// survive a round-trip through the object store.
async fn run_dynamic_task_creation(harness: &dyn ProviderHarness) -> Result<(), Box<dyn std::error::Error>> {
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

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    let storage = harness
        .build_storage(&ProviderStorageRequest::new(
            "dynamic-task",
            JobStateCodecKind::Json,
            Arc::clone(&job_registry) as Arc<dyn JobDefinitionRegistry>,
            Arc::new(NoopMetrics),
        ))
        .await?;

    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(100), Duration::from_millis(10)),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(storage, config, Arc::clone(&job_registry), vec![job_def])?;

    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    assert!(
        init_task_executed.load(Ordering::SeqCst),
        "init task should be executed"
    );
    assert_eq!(
        dynamic_tasks_executed.load(Ordering::SeqCst),
        i32::try_from(dynamic_task_count)?,
        "all dynamic tasks should be executed"
    );

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

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::run_dynamic_task_creation;
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test]
    async fn tasks_created_at_runtime_keep_their_dependencies_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_dynamic_task_creation(&S3ProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::run_dynamic_task_creation;
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test]
    async fn tasks_created_at_runtime_keep_their_dependencies_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_dynamic_task_creation(&AzureProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::run_dynamic_task_creation;
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test]
    async fn tasks_created_at_runtime_keep_their_dependencies_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_dynamic_task_creation(&GcsProviderHarness::start().await?).await
    }
}
