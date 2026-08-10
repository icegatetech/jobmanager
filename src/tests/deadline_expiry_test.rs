use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::s3_container::S3TestContainer;
use crate::storage::Storage;
use crate::{
    CachedStorage, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStateCodecKind, JobStatus,
    JobsManagerConfig, NoopMetrics, S3Storage, S3StorageConfig, TaskCode, TaskDefinition, TaskLimits, TaskOutcome,
    task_fn,
};

/// `TestTaskDeadlineExpiry` verifies that a task started by one worker is re-picked by another worker after its deadline expires.
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_task_deadline_expiry() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let expected_attempts: u32 = 4;

    // 1. Start object storage
    let store = S3TestContainer::start().await?;

    // 2. Track task execution attempts
    let attempt_count = Arc::new(AtomicU32::new(0));

    let attempt_count_clone = Arc::clone(&attempt_count);

    let executor = task_fn(move |_ctx| {
        let count = Arc::clone(&attempt_count_clone);

        async move {
            let attempt = count.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::info!("Task attempt {} started", attempt);

            if attempt <= 3 {
                // First attempt: exceed deadline so another worker can re-pick.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            // Complete successfully (first attempt might be stolen).
            Ok(TaskOutcome::Completed(b"success".to_vec()))
        }
    });

    let task_def = TaskDefinition::new(TaskCode::new("hanging_task"), Duration::from_millis(100));

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("test_deadline_job"),
        vec![(task_def.clone(), executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    // 3. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    // 4. Create storage
    let storage = Arc::new(
        S3Storage::new(
            S3StorageConfig::new(
                store.endpoint(),
                store.username(),
                store.password(),
                "test-jobs",
                "us-east-1",
            )
            .with_job_state_codec(JobStateCodecKind::Json)
            .with_request_timeout(Duration::from_millis(100)),
            job_registry.clone(),
            Arc::new(NoopMetrics),
        )
        .await?,
    );
    let storage = Arc::new(CachedStorage::new(
        storage.clone() as Arc<dyn Storage>,
        Arc::new(NoopMetrics),
    ));

    // 5. Start manager
    let config = JobsManagerConfig {
        worker_count: 3, // need more concurrency for small resources system
        worker_config: super::common::build_worker_config(Duration::from_millis(10), Duration::ZERO),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(storage, config, Arc::clone(&job_registry), vec![job_def])?;

    // 6. Wait for job completion (should re-pick after deadline expires)
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    // 7. Verify task was attempted multiple times
    assert_eq!(
        attempt_count.load(Ordering::SeqCst),
        expected_attempts,
        "task should be attempted {expected_attempts} due to deadline expiry"
    );

    // Verify final job state
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_deadline_job"), &cancel_token)
        .await?;
    let tasks = job.get_tasks_by_code(task_def.code());
    let attempts = tasks.first().map_or(0, |t| t.attempts());
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(
        attempts, expected_attempts,
        "expected task to be restarted after deadline"
    );

    Ok(())
}
