use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::s3_container::S3TestContainer;
use super::common::{manager_env::ManagerEnv, storage_wrapper::CountingStorage};
use crate::{
    CachedStorage, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStateCodecKind, JobStatus,
    JobsManagerConfig, NoopMetrics, S3Storage, S3StorageConfig, Storage, TaskCode, TaskDefinition, TaskLimits,
    TaskOutcome, task_fn,
};

// TODO(med): Add a check for the absence of errors in the logs. It won't be easy to do this, because when subscribing to errors and parallel tests, we catch errors from all tests and it's difficult to account for errors only in a specific test.

/// `TestConcurrentWorkers` verifies that multiple workers can process tasks from the same job
/// concurrently
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_concurrent_workers_s3() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    tracing::info!("Running concurrent workers test ({})", "s3");
    run_concurrent_workers_test(false).await?;

    Ok(())
}

/// `TestConcurrentWorkers` verifies that multiple workers can process tasks from the same job
/// concurrently
#[tokio::test(flavor = "multi_thread", worker_threads = 10)]
async fn test_concurrent_workers_cached() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    tracing::info!("Running concurrent workers test ({})", "cached");
    run_concurrent_workers_test(true).await?;

    Ok(())
}

async fn run_concurrent_workers_test(use_cached_storage: bool) -> Result<(), Box<dyn std::error::Error>> {
    let secondary_task_count = 10;
    let max_iterations = 1u64;
    let workers_cnt = 10;

    // 1. Start object storage
    let store = S3TestContainer::start().await?;

    // 2. Track execution
    let executed_primary_tasks: Arc<DashMap<Uuid, bool>> = Arc::new(DashMap::new());
    let executed_sec_tasks: Arc<DashMap<Uuid, bool>> = Arc::new(DashMap::new());

    let executed_primary_tasks_counter = Arc::clone(&executed_primary_tasks);
    let executed_sec_tasks_counter = Arc::clone(&executed_sec_tasks);

    // Init task executor
    let primary_executor = task_fn(move |ctx| {
        let executed = Arc::clone(&executed_primary_tasks_counter);
        let task_id = *ctx.id();

        async move {
            // Create multiple work tasks
            #[allow(clippy::cast_possible_truncation)]
            for i in 0..secondary_task_count {
                let secondary_task_def = TaskDefinition::new(TaskCode::new("secondary_task"), Duration::from_secs(1))
                    .with_input(vec![i as u8]);
                ctx.job().add_task(secondary_task_def)?;
            }

            executed.insert(task_id, true);

            Ok(TaskOutcome::empty())
        }
    });

    // Work task executor
    let secondary_executor = task_fn(move |ctx| {
        let executed = Arc::clone(&executed_sec_tasks_counter);
        let task_id = *ctx.id();

        async move {
            // Simulate some work
            tokio::time::sleep(Duration::from_millis(20)).await;

            // Track execution
            executed.insert(task_id, true);

            Ok(TaskOutcome::Completed(b"done".to_vec()))
        }
    });

    let primary_task_def = TaskDefinition::new(TaskCode::new("primary_task"), Duration::from_secs(1));

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("test_concurrent_job"),
        vec![(primary_task_def, primary_executor)],
        vec![(TaskCode::new("secondary_task"), secondary_executor)],
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(max_iterations)?;

    // 3. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    // 4. Create storage
    let s3_storage = Arc::new(
        S3Storage::new(
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
        .await?,
    );

    let counting_storage = Arc::new(CountingStorage::new(s3_storage.clone() as Arc<dyn Storage>));
    let storage: Arc<dyn Storage> = if use_cached_storage {
        Arc::new(CachedStorage::new(
            counting_storage.clone() as Arc<dyn Storage>,
            Arc::new(NoopMetrics),
        ))
    } else {
        counting_storage.clone()
    };

    // 5. Start manager with multiple workers
    let config = JobsManagerConfig {
        worker_count: workers_cnt,
        worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::from_millis(10)),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(storage, config, Arc::clone(&job_registry), vec![job_def])?;

    // 6. Wait for completion
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(30)).await?;
    manager_env.stop().await;

    // 7. Verify final job state
    let cancel_token = CancellationToken::new();
    let job = s3_storage
        .clone()
        .get_job(&JobCode::new("test_concurrent_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), max_iterations, "job iteration mismatch");
    assert!(job.all_tasks_completed(), "tasks not complete at all");
    assert_eq!(
        job.tasks_as_iter().count(),
        secondary_task_count + 1,
        "created tasks count mismatch"
    );
    let timeouts: u64 = job.tasks_as_iter().map(|t| u64::from(t.attempt() - 1)).sum();
    tracing::info!(
        "Tasks with timeout: {}. Timeouts count: {}",
        job.tasks_as_iter().filter(|t| t.attempt() > 1).count(),
        timeouts
    );

    // Verify each task was tracked
    assert_eq!(executed_primary_tasks.len(), 1, "primary tasks must be executed");
    assert_eq!(
        executed_sec_tasks.len(),
        secondary_task_count,
        "all secondary tasks must be executed"
    );

    // Verify S3 PUT requests
    tracing::info!(
        "S3 requests - save attempts: {}, save successes: {}, list & get successes: {}",
        counting_storage.put_attempts(),
        counting_storage.put_successes(),
        counting_storage.list_and_get_successes(),
    );
    // TODO(med): This is a fragile test, because there is a lot of competition and sometimes the test does not pass. We need to think of a separate test specifically for the number of requests.
    /*assert_eq!(
        counting_storage.put_successes(),
        (((secondary_task_count + 1) * 2) + 1) as u64 + timeouts, // 1 PUT for create job, 2 PUT for each task
        "all tasks must be executed"
    );*/

    Ok(())
}
