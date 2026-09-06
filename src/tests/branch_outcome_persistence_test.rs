use std::{sync::Arc, time::Duration};

use chrono::Utc;
use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use super::common::s3_container::S3TestContainer;
use crate::core::task::SkipCause;
use crate::{
    DependencyTolerance, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStateCodecKind, JobStatus,
    JobsManagerConfig, NoopMetrics, S3Storage, S3StorageConfig, Storage, Task, TaskCode, TaskDefinition, TaskLimits,
    TaskOutcome, TaskRef, TaskRetry, task_fn,
};

const DETECT_TASK_CODE: &str = "detect";
const RULES_TASK_CODE: &str = "rules";
const PREPARE_TASK_CODE: &str = "prepare";
const ARCHIVE_TASK_CODE: &str = "archive";
const REPORT_TASK_CODE: &str = "report";
const TASK_TIMEOUT: Duration = Duration::from_secs(30);
/// Bound on every wait, so a job that never finishes fails the test instead of hanging it.
const WAIT_TIMEOUT: Duration = Duration::from_secs(15);

/// What `prepare` declares, stated here so the assertion compares against the literal the
/// description was written with rather than against the description itself.
const PREPARE_TOLERANCE: DependencyTolerance = DependencyTolerance {
    allows_failed: true,
    allows_skipped: false,
};

#[tokio::test]
async fn branch_outcomes_survive_the_round_trip_through_storage_json() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_branch_outcome_round_trip(JobStateCodecKind::Json).await
}

#[tokio::test]
async fn branch_outcomes_survive_the_round_trip_through_storage_cbor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_branch_outcome_round_trip(JobStateCodecKind::Cbor).await
}

/// What the branch outcomes added to the stored state, none of it written in the ordinary case:
/// what a refusal declared about repeating it, why a task was skipped - every one of the three
/// causes, which ride in the task's status - and what a task tolerates. Each is read back by a
/// worker that did not write it, and losing one fails silently: a terminal refusal retried until
/// its budget burns, an iteration that stops seeing a failure it inherited, a tolerant task waiting
/// forever for what it declared it survives.
///
/// Three breaks cover the assertions here: dropping `retry` or `tolerance` from `StoredTask` leaves
/// the corresponding assertion reading the `serde` default, serializing `TaskStatus::Skipped`
/// without its cause leaves the state unreadable, and mapping every cause to
/// `SkipCause::ExecutorDecision` in `StoredTask::from_task` stores a decision where the cascade
/// wrote a failure it inherited.
async fn run_branch_outcome_round_trip(codec: JobStateCodecKind) -> Result<(), Box<dyn std::error::Error>> {
    let store = S3TestContainer::start().await?;
    let job_code = JobCode::new("branch_outcome_persistence_job");
    let definition_id = JobDefinitionId::new();

    let job_def = JobDefinition::new(
        definition_id,
        job_code.clone(),
        vec![
            (
                TaskDefinition::new(TaskCode::new(DETECT_TASK_CODE), TASK_TIMEOUT),
                task_fn(|_ctx| async { Ok(TaskOutcome::TerminallyFailed("decode detect input".to_string())) }),
            ),
            (
                TaskDefinition::new(TaskCode::new(RULES_TASK_CODE), TASK_TIMEOUT),
                task_fn(|_ctx| async { Ok(TaskOutcome::SkippedBranch("no rules matched".to_string())) }),
            ),
            (
                TaskDefinition::new(TaskCode::new(PREPARE_TASK_CODE), TASK_TIMEOUT)
                    .with_dependencies(vec![TaskRef::initial(definition_id, 0)])
                    .with_dependency_tolerance(PREPARE_TOLERANCE),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
            (
                TaskDefinition::new(TaskCode::new(ARCHIVE_TASK_CODE), TASK_TIMEOUT)
                    .with_dependencies(vec![TaskRef::initial(definition_id, 1)]),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
            (
                TaskDefinition::new(TaskCode::new(REPORT_TASK_CODE), TASK_TIMEOUT)
                    .with_dependencies(vec![TaskRef::initial(definition_id, 0)]),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
        ],
        Vec::new(),
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
                "branch-outcomes",
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
    assert_eq!(
        *job.status(),
        JobStatus::Failed,
        "the refusal reached a dependent that never declared it survives one, and a branch put out \
         by a failure carries it into the verdict"
    );

    let detect = find_task_by_code(&job, DETECT_TASK_CODE)?;
    assert_eq!(
        detect.retry(),
        TaskRetry::Never,
        "a refusal declared terminal must come back declared terminal"
    );
    assert_eq!(
        detect.attempt(),
        1,
        "and terminal on the attempt it was refused on, with the rest of the budget unspent"
    );
    assert!(
        detect.is_terminally_failed_at(Utc::now()),
        "which is what makes it terminal"
    );

    let rules = find_task_by_code(&job, RULES_TASK_CODE)?;
    assert_eq!(
        rules.skip_cause(),
        Some(SkipCause::ExecutorDecision),
        "the branch its executor gave up on must come back as that executor's own decision"
    );

    let archive = find_task_by_code(&job, ARCHIVE_TASK_CODE)?;
    assert_eq!(
        archive.skip_cause(),
        Some(SkipCause::SkippedDependency),
        "the task the cascade put out must come back as put out, not as a decision of its own"
    );

    let report = find_task_by_code(&job, REPORT_TASK_CODE)?;
    assert_eq!(
        report.skip_cause(),
        Some(SkipCause::FailedDependency),
        "the task a failure put out must come back carrying that failure, not a bare decision"
    );

    let prepare = find_task_by_code(&job, PREPARE_TASK_CODE)?;
    assert!(prepare.is_completed(), "the tolerant dependent must have run");
    assert_eq!(
        prepare.tolerance(),
        PREPARE_TOLERANCE,
        "what a task tolerates must come back as it was declared"
    );

    Ok(())
}

/// The one task of the stored iteration carrying `code`, or an error naming what was missing - the
/// description gives every task a code of its own.
fn find_task_by_code<'a>(job: &'a crate::Job, code: &str) -> Result<&'a Task, Box<dyn std::error::Error>> {
    job.tasks_as_iter()
        .find(|task| task.code() == &TaskCode::new(code))
        .ok_or_else(|| format!("the stored iteration must hold its '{code}' task").into())
}
