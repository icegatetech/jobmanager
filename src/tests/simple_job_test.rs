use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::provider_harness::{ProviderHarness, ProviderStorageRequest};
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobDefinitionRegistry, JobRegistry, JobStateCodecKind, JobStatus,
    JobsManagerConfig, NoopMetrics, Storage, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// One task runs, receives the input its definition carried, and the finished iteration is the one
/// the store holds afterwards. Run under both codecs, because the stored format is what the
/// round-trip is about.
async fn run_simple_job_execution(
    harness: &dyn ProviderHarness,
    codec: JobStateCodecKind,
) -> Result<(), Box<dyn std::error::Error>> {
    let max_iterations = 1u64;

    let executed = Arc::new(AtomicBool::new(false));
    let task_input_captured = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let executed_clone = Arc::clone(&executed);
    let task_input_clone = Arc::clone(&task_input_captured);

    let executor = task_fn(move |ctx| {
        let executed = Arc::clone(&executed_clone);
        let task_input = Arc::clone(&task_input_clone);
        let input = ctx.input().to_vec();

        async move {
            *task_input.lock() = input;
            executed.store(true, Ordering::SeqCst);
            Ok(TaskOutcome::Completed(b"result".to_vec()))
        }
    });

    let task_def =
        TaskDefinition::new(TaskCode::new("simple_task"), Duration::from_secs(5)).with_input(b"test-input".to_vec());

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("test_simple_job"),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(max_iterations)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    let storage = harness
        .build_storage(&ProviderStorageRequest::new(
            "simple-job",
            codec,
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

    manager_env.wait_for_all_jobs_completion(Duration::from_secs(10)).await?;
    manager_env.stop().await;

    assert!(executed.load(Ordering::SeqCst), "task should be executed");
    assert_eq!(
        *task_input_captured.lock(),
        b"test-input".to_vec(),
        "task should receive correct input"
    );

    // Verify job state in storage
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_simple_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), max_iterations);

    Ok(())
}

/// `TestMultiTaskSequence` verifies a job with sequential tasks.
///
/// The subject is that an executor's task reaches its successor with the payload it was given, so
/// the in-memory backend stands in for the object store; the round-trip of several runtime tasks
/// through real storage is asserted in `dynamic_task_test`.
#[tokio::test]
async fn test_multi_task_sequence() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let max_iterations = 1u64;

    // 1. Define job with two sequential tasks
    let first_task_executed = Arc::new(AtomicBool::new(false));
    let second_task_executed = Arc::new(AtomicBool::new(false));
    // Read by the second task and asserted after the wait: a failed assertion inside an executor
    // is caught by the worker and would only surface as a task failure.
    let second_task_input = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let first_executed_clone = Arc::clone(&first_task_executed);
    let first_task_executor = task_fn(move |ctx| {
        let executed = Arc::clone(&first_executed_clone);

        async move {
            executed.store(true, Ordering::SeqCst);

            // Create second task
            let second_task_def = TaskDefinition::new(TaskCode::new("second_task"), Duration::from_secs(5))
                .with_input(b"from-first".to_vec());

            ctx.job().add_task(second_task_def)?;
            Ok(TaskOutcome::Completed(b"first-done".to_vec()))
        }
    });

    let second_executed_clone = Arc::clone(&second_task_executed);
    let second_input_clone = Arc::clone(&second_task_input);
    let second_task_executor = task_fn(move |ctx| {
        let executed = Arc::clone(&second_executed_clone);
        let seen_input = Arc::clone(&second_input_clone);
        let input = ctx.input().to_vec();

        async move {
            executed.store(true, Ordering::SeqCst);
            *seen_input.lock() = input;
            Ok(TaskOutcome::Completed(b"second-done".to_vec()))
        }
    });

    let first_task_def = TaskDefinition::new(TaskCode::new("first_task"), Duration::from_secs(5));

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("test_sequence_job"),
        vec![(first_task_def, first_task_executor)],
        vec![(TaskCode::new("second_task"), second_task_executor)],
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(max_iterations)?;

    // 2. Create job definitions
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);

    // 3. Start manager
    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(100), Duration::from_millis(10)),
        ..Default::default()
    };

    let mut manager_env = ManagerEnv::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>,
        config,
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    // 4. Wait for completion
    manager_env.wait_for_all_jobs_completion(Duration::from_secs(15)).await?;
    manager_env.stop().await;

    // 5. Verify
    assert!(first_task_executed.load(Ordering::SeqCst), "first task should execute");
    assert!(
        second_task_executed.load(Ordering::SeqCst),
        "second task should execute"
    );
    assert_eq!(
        *second_task_input.lock(),
        b"from-first".to_vec(),
        "the second task should receive the payload the first one gave it"
    );

    // Verify job state in storage
    let cancel_token = CancellationToken::new();
    let job = manager_env
        .storage()
        .get_job(&JobCode::new("test_sequence_job"), &cancel_token)
        .await?;
    assert_eq!(*job.status(), JobStatus::Completed);
    assert_eq!(job.iter_num(), max_iterations);

    Ok(())
}

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::run_simple_job_execution;
    use crate::JobStateCodecKind;
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test]
    async fn a_single_task_job_runs_and_is_stored_as_json_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_simple_job_execution(&S3ProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_single_task_job_runs_and_is_stored_as_cbor_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_simple_job_execution(&S3ProviderHarness::start().await?, JobStateCodecKind::Cbor).await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::run_simple_job_execution;
    use crate::JobStateCodecKind;
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test]
    async fn a_single_task_job_runs_and_is_stored_as_json_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_simple_job_execution(&AzureProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_single_task_job_runs_and_is_stored_as_cbor_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_simple_job_execution(&AzureProviderHarness::start().await?, JobStateCodecKind::Cbor).await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::run_simple_job_execution;
    use crate::JobStateCodecKind;
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test]
    async fn a_single_task_job_runs_and_is_stored_as_json_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_simple_job_execution(&GcsProviderHarness::start().await?, JobStateCodecKind::Json).await
    }

    #[tokio::test]
    async fn a_single_task_job_runs_and_is_stored_as_cbor_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_simple_job_execution(&GcsProviderHarness::start().await?, JobStateCodecKind::Cbor).await
    }
}
