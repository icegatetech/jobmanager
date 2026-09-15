use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::provider_harness::{ProviderHarness, ProviderStorageRequest};
use crate::{
    Error, JobCode, JobDefinition, JobDefinitionId, JobDefinitionRegistry, JobRegistry, JobStateCodecKind, JobStatus,
    JobsManagerConfig, NoopMetrics, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// A task whose executor refuses once is picked up again and completes on the next attempt, which
/// is what the attempt budget is spent on.
async fn run_task_failure_and_retry(harness: &dyn ProviderHarness) -> Result<(), Box<dyn std::error::Error>> {
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

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    let storage = harness
        .build_storage(&ProviderStorageRequest::new(
            "task-failure",
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
        attempt_count.load(Ordering::SeqCst) >= 2,
        "task should be retried after failure"
    );

    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_retry_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);

    Ok(())
}

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::run_task_failure_and_retry;
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test]
    async fn a_refused_task_is_retried_and_completes_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_task_failure_and_retry(&S3ProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::run_task_failure_and_retry;
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test]
    async fn a_refused_task_is_retried_and_completes_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_task_failure_and_retry(&AzureProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::run_task_failure_and_retry;
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test]
    async fn a_refused_task_is_retried_and_completes_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_task_failure_and_retry(&GcsProviderHarness::start().await?).await
    }
}
