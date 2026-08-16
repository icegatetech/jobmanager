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

    let expected_executions: u32 = 4;

    // 1. Start object storage
    let store = S3TestContainer::start().await?;

    // 2. Track task executions, which the takeovers no longer show up in the attempt counter as
    let execution_count = Arc::new(AtomicU32::new(0));

    let execution_count_clone = Arc::clone(&execution_count);

    let executor = task_fn(move |_ctx| {
        let count = Arc::clone(&execution_count_clone);

        async move {
            let execution = count.fetch_add(1, Ordering::SeqCst) + 1;
            tracing::info!("Task execution {} started", execution);

            if execution <= 3 {
                // Exceed the deadline so another worker can re-pick; the token this executor is
                // given is cancelled meanwhile, and ignoring it is what keeps the takeover legal.
                tokio::time::sleep(Duration::from_millis(500)).await;
            }

            // Complete successfully (an earlier execution might be stolen).
            Ok(TaskOutcome::Completed(b"success".to_vec()))
        }
    });

    // Four executions of half a second each need a lifetime the default - five deadlines, half a
    // second in total - does not give.
    let task_def = TaskDefinition::new(TaskCode::new("hanging_task"), Duration::from_millis(100))
        .with_max_lifetime(Duration::from_secs(30));

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

    // 7. Verify task was executed multiple times
    assert_eq!(
        execution_count.load(Ordering::SeqCst),
        expected_executions,
        "task should be executed {expected_executions} times due to deadline expiry"
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
        attempts, 1,
        "a takeover after the deadline must not spend an attempt: nothing refused the task"
    );

    Ok(())
}
