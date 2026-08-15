use std::{sync::Arc, time::Duration};

use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::s3_container::S3TestContainer;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStateCodecKind, JobStatus, JobsManagerConfig, NoopMetrics,
    S3Storage, S3StorageConfig, Storage, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

const PLAN_TASK_CODE: &str = "plan";
const PLANNED_TASK_CODE: &str = "planned";
const TASK_TIMEOUT: Duration = Duration::from_secs(5);
/// Maximum lifetime the planning task is defined with. Deliberately not the default multiple of the
/// deadline, so the assertion tells a value read out of the stored object apart from one recomputed
/// from the task's definition; a field dropped on the way out comes back as `0` through the
/// `serde` default the stored representation carries.
const PLAN_TASK_MAX_LIFETIME: Duration = Duration::from_secs(20);
/// Bound on every wait, so a job that never finishes fails the test instead of hanging it.
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
async fn task_lifetime_survives_the_round_trip_through_storage_json() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_task_lifetime_round_trip(JobStateCodecKind::Json).await
}

#[tokio::test]
async fn task_lifetime_survives_the_round_trip_through_storage_cbor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_task_lifetime_round_trip(JobStateCodecKind::Cbor).await
}

/// The stored state is what every worker but the first one reads the lifetime from, and losing it
/// there disables the bound silently: the moment a task's lifetime expires is set only while it is
/// unset, so a takeover reading `None` would start the count again and the task could be taken over
/// forever.
///
/// The other half of the round trip is the task a running execution created: parentage lives no
/// longer than that execution, so a task read back from storage must carry none - a stored parent
/// would let a later failure of that task roll back work the iteration already owns.
async fn run_task_lifetime_round_trip(codec: JobStateCodecKind) -> Result<(), Box<dyn std::error::Error>> {
    let store = S3TestContainer::start().await?;
    let job_code = JobCode::new("lifetime_persistence_job");

    let plan_executor = task_fn(|ctx| async move {
        ctx.job()
            .add_task(TaskDefinition::new(TaskCode::new(PLANNED_TASK_CODE), TASK_TIMEOUT))?;
        Ok(TaskOutcome::empty())
    });
    let planned_executor = task_fn(|_ctx| async { Ok(TaskOutcome::empty()) });

    let plan_def =
        TaskDefinition::new(TaskCode::new(PLAN_TASK_CODE), TASK_TIMEOUT).with_max_lifetime(PLAN_TASK_MAX_LIFETIME);
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(plan_def, plan_executor)],
        vec![(TaskCode::new(PLANNED_TASK_CODE), planned_executor)],
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage = Arc::new(
        S3Storage::new(
            S3StorageConfig::new(
                store.endpoint(),
                store.username(),
                store.password(),
                "test-jobs",
                "us-east-1",
            )
            .with_job_state_codec(codec),
            job_registry.clone(),
            Arc::new(NoopMetrics),
        )
        .await?,
    ) as Arc<dyn Storage>;

    let config = JobsManagerConfig {
        worker_count: 1,
        worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
        ..Default::default()
    };
    let mut manager_env = ManagerEnv::new(Arc::clone(&storage), config, Arc::clone(&job_registry), vec![job_def])?;
    manager_env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    manager_env.stop().await;

    let job = storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(*job.status(), JobStatus::Completed);

    let plan_task = job
        .tasks_as_iter()
        .find(|task| task.code() == &TaskCode::new(PLAN_TASK_CODE))
        .ok_or("the stored iteration must hold its planning task")?;
    assert_eq!(
        plan_task.max_lifetime(),
        chrono::Duration::seconds(20),
        "the maximum lifetime must come back as the definition declared it"
    );
    let started_at = plan_task.started_at().ok_or("the stored task must carry its start")?;
    assert_eq!(
        plan_task.lifetime_deadline_at(),
        Some(started_at + chrono::Duration::seconds(20)),
        "the moment the lifetime expires must come back as the first start placed it"
    );

    let planned_task = job
        .tasks_as_iter()
        .find(|task| task.code() == &TaskCode::new(PLANNED_TASK_CODE))
        .ok_or("the stored iteration must hold the task its planning created")?;
    assert_eq!(
        planned_task.created_by_task(),
        None,
        "a task read back from storage belongs to no open execution"
    );

    Ok(())
}
