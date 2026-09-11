use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::common::provider_harness::{ProviderHarness, ProviderStorageRequest};
use super::common::{manager_env::ManagerEnv, storage_wrapper::CountingStorage};
use crate::{
    CachedStorage, JobCode, JobDefinition, JobDefinitionId, JobDefinitionRegistry, JobRegistry, JobStateCodecKind,
    JobStatus, JobsManagerConfig, NoopMetrics, Storage, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

// TODO(med): Add a check for the absence of errors in the logs. It won't be easy to do this, because when subscribing to errors and parallel tests, we catch errors from all tests and it's difficult to account for errors only in a specific test.

/// Verifies that multiple workers can process tasks from the same job concurrently.
async fn run_concurrent_workers_test(
    harness: &dyn ProviderHarness,
    use_cached_storage: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let secondary_task_count = 10;
    let max_iterations = 1u64;
    let workers_cnt = 10;

    // Track execution
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

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    let object_storage = harness
        .build_storage(&ProviderStorageRequest::new(
            "concurrent-workers",
            JobStateCodecKind::Json,
            Arc::clone(&job_registry) as Arc<dyn JobDefinitionRegistry>,
            Arc::new(NoopMetrics),
        ))
        .await?;

    let counting_storage = Arc::new(CountingStorage::new(Arc::clone(&object_storage)));
    let storage: Arc<dyn Storage> = if use_cached_storage {
        Arc::new(CachedStorage::new(
            counting_storage.clone() as Arc<dyn Storage>,
            Arc::new(NoopMetrics),
        ))
    } else {
        counting_storage.clone()
    };

    let config = JobsManagerConfig {
        worker_count: workers_cnt,
        worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::from_millis(10)),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(storage, config, Arc::clone(&job_registry), vec![job_def])?;

    manager_env.wait_for_all_jobs_completion(Duration::from_secs(30)).await?;
    manager_env.stop().await;

    let cancel_token = CancellationToken::new();
    let job = object_storage
        .get_job(&JobCode::new("test_concurrent_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), max_iterations, "job iteration mismatch");
    assert!(job.all_tasks_resolved(), "tasks not resolved at all");
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

    tracing::info!(
        "storage calls - save attempts: {}, save successes: {}, list & get successes: {}",
        counting_storage.put_attempts(),
        counting_storage.put_successes(),
        counting_storage.list_and_get_successes(),
    );
    Ok(())
}

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::run_concurrent_workers_test;
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn concurrent_workers_share_a_job_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_concurrent_workers_test(&S3ProviderHarness::start().await?, false).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn concurrent_workers_share_a_job_behind_the_cache_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_concurrent_workers_test(&S3ProviderHarness::start().await?, true).await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::run_concurrent_workers_test;
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn concurrent_workers_share_a_job_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_concurrent_workers_test(&AzureProviderHarness::start().await?, false).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn concurrent_workers_share_a_job_behind_the_cache_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_concurrent_workers_test(&AzureProviderHarness::start().await?, true).await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::run_concurrent_workers_test;
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn concurrent_workers_share_a_job_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_concurrent_workers_test(&GcsProviderHarness::start().await?, false).await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn concurrent_workers_share_a_job_behind_the_cache_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_concurrent_workers_test(&GcsProviderHarness::start().await?, true).await
    }
}
