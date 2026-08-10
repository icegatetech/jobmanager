use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::s3_container::S3TestContainer;
use crate::{
    Error, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStateCodecKind, JobStatus, JobsManagerConfig,
    NoopMetrics, S3Storage, S3StorageConfig, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// `TestTaskFailureAndRetry` verifies that failed tasks are retried
#[tokio::test]
async fn test_task_failure_and_retry() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    // 1. Start object storage
    let store = S3TestContainer::start().await?;

    // 2. Track attempts
    let attempt_count = Arc::new(AtomicI32::new(0));

    let attempt_count_clone = Arc::clone(&attempt_count);

    let executor = task_fn(move |_ctx| {
        let count = Arc::clone(&attempt_count_clone);

        async move {
            let attempt = count.fetch_add(1, Ordering::SeqCst) + 1;

            // Fail on first attempt, succeed on second
            if attempt == 1 {
                tracing::warn!("Attempt {}: simulating failure", attempt);
                return Err(Error::Other("simulated failure".to_string()).into());
            }

            tracing::info!("Attempt {}: succeeding", attempt);
            Ok(TaskOutcome::Completed(b"success".to_vec()))
        }
    });

    let task_def = TaskDefinition::new(TaskCode::new("flaky_task"), Duration::from_secs(5));

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("test_retry_job"),
        vec![(task_def, executor)],
        Vec::new(),
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

    // 7. Verify task was attempted multiple times
    assert!(
        attempt_count.load(Ordering::SeqCst) >= 2,
        "task should be retried after failure"
    );

    // Verify final job state
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_retry_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);

    Ok(())
}
