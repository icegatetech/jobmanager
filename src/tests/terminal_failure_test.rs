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
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobDefinitionRegistry, JobRegistry, JobStateCodecKind, JobStatus,
    JobsManagerConfig, NoopMetrics, Storage, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, TaskRetry, task_fn,
};

/// Bound on every wait here, so a job that never finishes fails the test instead of hanging it.
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);

/// A refusal the executor declared terminal is not retried: the task ends on its first attempt and
/// the iteration fails, instead of the executor being called five times over.
async fn run_a_terminal_refusal_is_not_retried(
    harness: &dyn ProviderHarness,
) -> Result<(), Box<dyn std::error::Error>> {
    let attempts = Arc::new(AtomicI32::new(0));

    let executor = {
        let attempts = Arc::clone(&attempts);
        task_fn(move |_ctx| {
            let attempts = Arc::clone(&attempts);
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                Ok(TaskOutcome::TerminallyFailed("decode detect input".to_string()))
            }
        })
    };

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        JobCode::new("terminal_failure_job"),
        vec![(
            TaskDefinition::new(TaskCode::new("detect"), Duration::from_secs(5)),
            executor,
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage = harness
        .build_storage(&ProviderStorageRequest::new(
            "terminal-failure",
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
    let mut env = ManagerEnv::new(storage, config, Arc::clone(&job_registry), vec![job_def])?;

    env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    env.stop().await;

    assert_eq!(
        attempts.load(Ordering::SeqCst),
        1,
        "a terminal refusal must be executed once, whatever the attempt budget allows"
    );
    let job = env
        .storage()
        .get_job(&JobCode::new("terminal_failure_job"), &CancellationToken::new())
        .await?;
    assert_eq!(*job.status(), JobStatus::Failed);
    Ok(())
}

/// The other way to declare a refusal final: the executor closes its own task through the handle
/// and returns `Deferred`. The declaration has to survive the whole path - handle, worker, stored
/// state - or the task is picked up again and spends the budget the declaration exists to save.
///
/// The break that proves it: passing `TaskRetry::WhileBudgetLasts` from `JobHandleImpl::fail_task`,
/// which makes the task pickable again and the executor run five times.
#[tokio::test]
async fn a_terminal_refusal_declared_through_the_handle_is_not_retried() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();
    let executions = Arc::new(AtomicI32::new(0));
    let job_code = JobCode::new("terminal_failure_handle_job");

    let executor = {
        let executions = Arc::clone(&executions);
        task_fn(move |ctx| {
            let executions = Arc::clone(&executions);
            async move {
                executions.fetch_add(1, Ordering::SeqCst);
                ctx.job().fail_task(ctx.id(), "decode detect input", TaskRetry::Never)?;
                Ok(TaskOutcome::Deferred)
            }
        })
    };

    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            TaskDefinition::new(TaskCode::new("detect"), Duration::from_secs(5)),
            executor,
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage = Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>;
    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
        ..Default::default()
    };
    let mut env = ManagerEnv::new(Arc::clone(&storage), config, Arc::clone(&job_registry), vec![job_def])?;

    env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    env.stop().await;

    assert_eq!(
        executions.load(Ordering::SeqCst),
        1,
        "a refusal declared terminal through the handle must be executed once, whatever the budget allows"
    );
    let job = storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(*job.status(), JobStatus::Failed);
    let task = job.tasks_as_iter().next().ok_or("the stored iteration must hold its task")?;
    assert_eq!(task.retry(), TaskRetry::Never, "the declaration must survive the save");
    assert_eq!(task.attempt(), 1, "with the rest of the budget unspent");
    Ok(())
}

#[cfg(feature = "storage-s3")]
mod on_s3 {
    use super::run_a_terminal_refusal_is_not_retried;
    use crate::tests::common::provider_harness::S3ProviderHarness;

    #[tokio::test]
    async fn a_terminal_refusal_is_not_retried_on_s3() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_terminal_refusal_is_not_retried(&S3ProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-azure")]
mod on_azure {
    use super::run_a_terminal_refusal_is_not_retried;
    use crate::tests::common::provider_harness::AzureProviderHarness;

    #[tokio::test]
    async fn a_terminal_refusal_is_not_retried_on_azure() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_terminal_refusal_is_not_retried(&AzureProviderHarness::start().await?).await
    }
}

#[cfg(feature = "storage-gcs")]
mod on_gcs {
    use super::run_a_terminal_refusal_is_not_retried;
    use crate::tests::common::provider_harness::GcsProviderHarness;

    #[tokio::test]
    async fn a_terminal_refusal_is_not_retried_on_gcs() -> Result<(), Box<dyn std::error::Error>> {
        crate::tests::common::init_tracing();
        run_a_terminal_refusal_is_not_retried(&GcsProviderHarness::start().await?).await
    }
}
