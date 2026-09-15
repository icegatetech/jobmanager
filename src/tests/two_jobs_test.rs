use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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

/// Storage timeout the case runs with, kept from the original S3 configuration: two jobs polled by
/// two workers must not have a pass held up by a request the store is slow to answer.
const TWO_JOBS_REQUEST_TIMEOUT: Duration = Duration::from_millis(100);

/// Two jobs run in parallel under two workers, each reaching its iteration budget with its own
/// tasks and nothing crossing between them.
async fn run_two_jobs_concurrent(harness: &dyn ProviderHarness) -> Result<(), Box<dyn std::error::Error>> {
    let tasks_per_iter = 3usize;
    let max_iterations = 3u64;

    let primary_job_code = JobCode::new("test_two_jobs_a");
    let secondary_job_code = JobCode::new("test_two_jobs_b");
    let task_code = TaskCode::new("simple_task");

    // 2. Track executions per job
    let primary_job_count = Arc::new(AtomicU64::new(0));
    let secondary_job_count = Arc::new(AtomicU64::new(0));

    let primary_job_count_clone = Arc::clone(&primary_job_count);
    let secondary_job_count_clone = Arc::clone(&secondary_job_count);

    let executor = task_fn(move |ctx| {
        let primary_job_count = Arc::clone(&primary_job_count_clone);
        let secondary_job_count = Arc::clone(&secondary_job_count_clone);
        let payload = ctx.input().to_vec();

        async move {
            match payload.as_slice() {
                b"job_a" => {
                    primary_job_count.fetch_add(1, Ordering::SeqCst);
                }
                b"job_b" => {
                    secondary_job_count.fetch_add(1, Ordering::SeqCst);
                }
                _ => {}
            }

            Ok(TaskOutcome::Completed(b"done".to_vec()))
        }
    });

    let mut primary_tasks = Vec::new();
    for _ in 0..tasks_per_iter {
        primary_tasks.push((
            TaskDefinition::new(task_code.clone(), Duration::from_secs(2)).with_input(b"job_a".to_vec()),
            Arc::clone(&executor),
        ));
    }

    let mut secondary_tasks = Vec::new();
    for _ in 0..tasks_per_iter {
        secondary_tasks.push((
            TaskDefinition::new(task_code.clone(), Duration::from_secs(2)).with_input(b"job_b".to_vec()),
            Arc::clone(&executor),
        ));
    }

    let primary_job_def = JobDefinition::new(
        JobDefinitionId::new(),
        primary_job_code.clone(),
        primary_tasks,
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(max_iterations)?;
    let secondary_job_def = JobDefinition::new(
        JobDefinitionId::new(),
        secondary_job_code.clone(),
        secondary_tasks,
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(max_iterations)?;

    let job_registry = Arc::new(JobRegistry::new(vec![
        primary_job_def.clone(),
        secondary_job_def.clone(),
    ])?);

    let object_storage = harness
        .build_storage(
            &ProviderStorageRequest::new(
                "two-jobs",
                JobStateCodecKind::Json,
                Arc::clone(&job_registry) as Arc<dyn JobDefinitionRegistry>,
                Arc::new(NoopMetrics),
            )
            .with_request_timeout(TWO_JOBS_REQUEST_TIMEOUT),
        )
        .await?;
    let storage = Arc::new(CachedStorage::new(object_storage, Arc::new(NoopMetrics))) as Arc<dyn Storage>;

    let config = JobsManagerConfig {
        worker_count: 2,
        worker_config: super::common::build_worker_config(Duration::from_millis(100), Duration::from_millis(10)),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(
        storage,
        config,
        Arc::clone(&job_registry),
        vec![primary_job_def.clone(), secondary_job_def.clone()],
    )?;

    manager_env.wait_for_all_jobs_completion(Duration::from_secs(30)).await?;
    manager_env.stop().await;

    // Due to concurrency, the actual task execution may exceed the expected one.
    let expected_executions = (tasks_per_iter as u64) * max_iterations;
    assert!(
        primary_job_count.load(Ordering::SeqCst) >= expected_executions,
        "job A tasks should be executed for all iterations"
    );
    assert!(
        secondary_job_count.load(Ordering::SeqCst) >= expected_executions,
        "job B tasks should be executed for all iterations"
    );

    let cancel_token = CancellationToken::new();
    let primary_job_state = manager_env.storage().get_job(&primary_job_code, &cancel_token).await?;
    assert_eq!(*primary_job_state.status(), JobStatus::Completed);
    assert_eq!(primary_job_state.iter_num(), max_iterations, "job A iteration mismatch");
    assert_eq!(
        primary_job_state.tasks_as_iter().count(),
        tasks_per_iter,
        "job A tasks count mismatch"
    );
    let primary_job_timeouts: u64 = primary_job_state.tasks_as_iter().map(|t| u64::from(t.attempt() - 1)).sum();
    assert_eq!(primary_job_timeouts, 0, "job A should not have timeouts");

    let secondary_job_state = manager_env.storage().get_job(&secondary_job_code, &cancel_token).await?;
    assert_eq!(*secondary_job_state.status(), JobStatus::Completed);
    assert_eq!(
        secondary_job_state.iter_num(),
        max_iterations,
        "job B iteration mismatch"
    );
    assert_eq!(
        secondary_job_state.tasks_as_iter().count(),
        tasks_per_iter,
        "job B tasks count mismatch"
    );
    let secondary_job_timeouts: u64 = secondary_job_state.tasks_as_iter().map(|t| u64::from(t.attempt() - 1)).sum();
    assert_eq!(secondary_job_timeouts, 0, "job B should not have timeouts");

    Ok(())
}

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::run_two_jobs_concurrent;
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn two_jobs_run_side_by_side_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_two_jobs_concurrent(&S3ProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::run_two_jobs_concurrent;
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn two_jobs_run_side_by_side_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_two_jobs_concurrent(&AzureProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::run_two_jobs_concurrent;
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test(flavor = "multi_thread", worker_threads = 10)]
    async fn two_jobs_run_side_by_side_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_two_jobs_concurrent(&GcsProviderHarness::start().await?).await
    }
}
