use std::{
    sync::{
        Arc,
        atomic::{AtomicU32, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::provider_harness::{ProviderHarness, ProviderStorageRequest};
use crate::storage::Storage;
use crate::{
    CachedStorage, JobCode, JobDefinition, JobDefinitionId, JobDefinitionRegistry, JobRegistry, JobStateCodecKind,
    JobStatus, JobsManagerConfig, NoopMetrics, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// Storage timeout the case runs with: a request that outlived the deadline it is racing would let
/// a takeover be decided by the store rather than by the deadline.
const DEADLINE_RACE_REQUEST_TIMEOUT: Duration = Duration::from_millis(100);

/// A task started by one worker is picked up again by another once its deadline expires, and the
/// takeover spends no attempt.
async fn run_task_deadline_expiry(harness: &dyn ProviderHarness) -> Result<(), Box<dyn std::error::Error>> {
    let expected_executions: u32 = 4;

    // Track task executions, which the takeovers no longer show up in the attempt counter as
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

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    let object_storage = harness
        .build_storage(
            &ProviderStorageRequest::new(
                "deadline-expiry",
                JobStateCodecKind::Json,
                Arc::clone(&job_registry) as Arc<dyn JobDefinitionRegistry>,
                Arc::new(NoopMetrics),
            )
            .with_request_timeout(DEADLINE_RACE_REQUEST_TIMEOUT),
        )
        .await?;
    let storage = Arc::new(CachedStorage::new(object_storage, Arc::new(NoopMetrics))) as Arc<dyn Storage>;

    let config = JobsManagerConfig {
        worker_count: 3, // need more concurrency for small resources system
        worker_config: super::common::build_worker_config(Duration::from_millis(10), Duration::ZERO),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(storage, config, Arc::clone(&job_registry), vec![job_def])?;

    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    assert_eq!(
        execution_count.load(Ordering::SeqCst),
        expected_executions,
        "task should be executed {expected_executions} times due to deadline expiry"
    );

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

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::run_task_deadline_expiry;
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn an_expired_deadline_hands_the_task_to_another_worker_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_task_deadline_expiry(&S3ProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::run_task_deadline_expiry;
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn an_expired_deadline_hands_the_task_to_another_worker_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_task_deadline_expiry(&AzureProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::run_task_deadline_expiry;
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn an_expired_deadline_hands_the_task_to_another_worker_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_task_deadline_expiry(&GcsProviderHarness::start().await?).await
    }
}
