use std::{
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;
use tokio_util::sync::CancellationToken;

use super::common::counting_metrics::CountingMetrics;
use super::common::manager_env::ManagerEnv;
use super::common::storage_wrapper::{ContendedSave, ContendingStorage};
use crate::core::task::SkipCause;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    DependencyTolerance, Error, JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStatus, JobsManager,
    JobsManagerConfig, MetricsSink, Storage, TaskCode, TaskDefinition, TaskLimits, TaskOutcome, TaskRef, TaskStatus,
    task_fn,
};

/// Bound on every wait here: a job that never finishes must fail the test rather than hang the
/// suite.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// The whole of the feature end to end: an executor calls its branch pointless, what waited on that
/// branch never runs, and the iteration is still a success - a decision is not a failure.
#[tokio::test]
async fn a_skipped_branch_puts_out_its_dependent_and_completes_the_iteration() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();

    let prepare_runs = Arc::new(AtomicI32::new(0));
    let sink = Arc::new(CountingMetrics::default());
    let manager = JobsManager::builder()
        .in_memory()
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(20))
        .job("skipped_branch_job", {
            let prepare_runs = Arc::clone(&prepare_runs);
            move |j| {
                j.max_iterations(1);
                let detect = j.add_task(
                    TaskDefinition::new("detect", Duration::from_secs(5)),
                    task_fn(|_ctx| async { Ok(TaskOutcome::SkippedBranch("nothing to do".to_string())) }),
                );
                let prepare = j.add_task(
                    TaskDefinition::new("prepare", Duration::from_secs(5)),
                    task_fn(move |_ctx| {
                        let prepare_runs = Arc::clone(&prepare_runs);
                        async move {
                            prepare_runs.fetch_add(1, Ordering::SeqCst);
                            Ok(TaskOutcome::empty())
                        }
                    }),
                );
                j.depends_on(prepare, &[detect]);
            }
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("skipped_branch_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert_eq!(
        prepare_runs.load(Ordering::SeqCst),
        0,
        "a task waiting behind a skipped branch must never be executed"
    );
    assert_eq!(
        sink.skipped_tasks(),
        1,
        "the skipped branch itself is what a worker processed"
    );
    assert_eq!(sink.completed_iterations(), 1);
    assert_eq!(sink.failed_iterations(), 0, "a decision is not a failure");
    Ok(())
}

/// What `prepare` read off `detect`: whether it completed, whether it was skipped, and the reason
/// recorded on it.
type ObservedDependency = (bool, bool, String);

/// Runs a job of two tasks - `detect` ending as `dependency_outcome`, and `prepare` declaring
/// `tolerance` and reading `detect` through `ImmutableTask` - and returns what `prepare` read
/// together with the sink the run was measured through.
///
/// What `prepare` saw travels out through the `Arc<Mutex<..>>` and is asserted by the caller after
/// the run: an assertion inside an executor is caught by the worker and would only fail the task.
async fn run_tolerant_consumer_job(
    job_code: &str,
    dependency_outcome: TaskOutcome,
    tolerance: DependencyTolerance,
) -> Result<(ObservedDependency, Arc<CountingMetrics>), Box<dyn std::error::Error>> {
    let observed: Arc<Mutex<Option<ObservedDependency>>> = Arc::new(Mutex::new(None));
    let sink = Arc::new(CountingMetrics::default());
    let manager = JobsManager::builder()
        .in_memory()
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(20))
        .job(job_code, {
            let observed = Arc::clone(&observed);
            move |j| {
                j.max_iterations(1);
                let detect = j.add_task(
                    TaskDefinition::new("detect", Duration::from_secs(5)),
                    task_fn(move |_ctx| {
                        let dependency_outcome = dependency_outcome.clone();
                        async move { Ok(dependency_outcome) }
                    }),
                );
                let prepare = j.add_task(
                    TaskDefinition::new("prepare", Duration::from_secs(5)).with_dependency_tolerance(tolerance),
                    task_fn(move |ctx| {
                        let observed = Arc::clone(&observed);
                        async move {
                            let dependencies = ctx.job().get_tasks_by_code(&TaskCode::new("detect"))?;
                            let detect =
                                dependencies.first().ok_or_else(|| Error::Other("no detect task".to_string()))?;
                            observed.lock().replace((
                                detect.is_completed(),
                                detect.is_skipped(),
                                detect.get_resolution_reason().to_string(),
                            ));
                            Ok(TaskOutcome::empty())
                        }
                    }),
                );
                j.depends_on(prepare, &[detect]);
            }
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(WAIT_TIMEOUT, handle.wait_for_job_completion(&JobCode::new(job_code))).await??;
    handle.shutdown().await?;

    let observed = observed.lock().clone().ok_or("the tolerant consumer must have run")?;
    Ok((observed, sink))
}

/// What a task declares tolerance for: it runs on a dependency that refused for good, reads the
/// state that dependency ended in, and takes its degraded path. The failure is handled, so the
/// iteration is a success even though it holds a task that failed.
#[tokio::test]
async fn a_tolerant_consumer_runs_on_a_terminally_failed_dependency() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let ((is_completed, is_skipped, error), sink) = run_tolerant_consumer_job(
        "degraded_consumer_job",
        TaskOutcome::TerminallyFailed("decode input".to_string()),
        DependencyTolerance {
            allows_failed: true,
            allows_skipped: false,
        },
    )
    .await?;

    assert!(!is_completed, "the dependency refused rather than finished");
    assert!(!is_skipped, "a refusal is not a decision to skip");
    assert_eq!(error, "decode input", "the reason is what the consumer degrades on");
    assert_eq!(sink.completed_iterations(), 1, "the failure was handled");
    assert_eq!(sink.failed_iterations(), 0);
    Ok(())
}

/// The other state a tolerant consumer has to tell apart: a dependency given up on by its own
/// executor is skipped rather than failed, and the words that executor gave are what the consumer
/// degrades on. This is the whole path the reason travels - executor, worker, domain, and back out
/// through `ImmutableTask` to a second executor.
#[tokio::test]
async fn a_tolerant_consumer_reads_the_reason_a_skipped_branch_was_given_up_on()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let ((is_completed, is_skipped, reason), sink) = run_tolerant_consumer_job(
        "skipped_consumer_job",
        TaskOutcome::SkippedBranch("nothing to detect".to_string()),
        DependencyTolerance {
            allows_failed: false,
            allows_skipped: true,
        },
    )
    .await?;

    assert!(!is_completed, "the branch was given up on rather than finished");
    assert!(is_skipped, "a branch its executor gave up on is skipped");
    assert_eq!(
        reason, "nothing to detect",
        "the reason is what the consumer degrades on"
    );
    assert_eq!(sink.completed_iterations(), 1, "a decision is not a failure");
    Ok(())
}

/// The `Deferred` path for a decision: an executor that skipped its own task through the handle has
/// resolved it, so the worker stores that state instead of refusing the outcome.
#[tokio::test]
async fn skipping_a_branch_through_the_handle_is_accepted() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let sink = Arc::new(CountingMetrics::default());
    let manager = JobsManager::builder()
        .in_memory()
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(20))
        .job("deferred_skip_job", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("detect", Duration::from_secs(5)),
                task_fn(|ctx| async move {
                    ctx.job().skip_task_branch("nothing to do")?;
                    Ok(TaskOutcome::Deferred)
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("deferred_skip_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert_eq!(
        sink.skipped_tasks(),
        1,
        "a task the executor skipped must be stored as skipped, not refused as an unresolved Deferred"
    );
    assert_eq!(sink.failed_tasks(), 0);
    assert_eq!(sink.completed_iterations(), 1);
    Ok(())
}

/// The conflict path of a decision: the save carrying the skipped branch loses its race, and what
/// the merge inherits from the stored state is a dependent still waiting behind that branch. The
/// cascade the worker applied to its own copy is not part of what a merge carries, so it has to be
/// derived again on the merged state - otherwise the iteration is stored closed with a task left
/// blocked in it, on a verdict passed over a state nobody judged.
///
/// The break that proves it: removing the `try_settle_iteration` call from the end of
/// `Job::merge_with_processed_task`, which stores the iteration with the dependent still blocked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cascade_lost_to_a_race_is_derived_again_on_the_stored_state() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("contended_skip_job");
    let definition_id = JobDefinitionId::new();
    let job_def = JobDefinition::new(
        definition_id,
        job_code.clone(),
        vec![
            (
                TaskDefinition::new(TaskCode::new("detect"), Duration::from_secs(30)),
                task_fn(|_ctx| async { Ok(TaskOutcome::SkippedBranch("nothing to do".to_string())) }),
            ),
            (
                TaskDefinition::new(TaskCode::new("prepare"), Duration::from_secs(30))
                    .with_dependencies(vec![TaskRef::initial(definition_id, 0)]),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            ),
        ],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;

    let storage = Arc::new(
        ContendingStorage::new(Arc::new(InMemoryStorage::new())).with_contended_save(ContendedSave::OfTask {
            code: TaskCode::new("detect"),
            status: TaskStatus::Skipped(SkipCause::ExecutorDecision),
        }),
    );
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let mut env = ManagerEnv::new(
        Arc::clone(&storage) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
            ..Default::default()
        },
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    env.wait_for_all_jobs_completion(WAIT_TIMEOUT).await?;
    env.stop().await;

    assert_eq!(
        storage.interferences(),
        1,
        "the fixture must have made the save of the skipped branch lose its race"
    );
    let stored = storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(*stored.status(), JobStatus::Completed, "a decision is not a failure");
    let prepare = stored
        .tasks_as_iter()
        .find(|task| task.code() == &TaskCode::new("prepare"))
        .ok_or("the stored iteration must hold its dependent")?;
    assert!(
        prepare.is_skipped(),
        "the dependent must be put out in the state that was stored, not only in the copy that lost the race"
    );
    Ok(())
}

/// A decision rolls nothing back, so the work this execution registered outside the branch it gave
/// up on still runs - which is what tells a skip from a refusal.
#[tokio::test]
async fn a_skipped_branch_leaves_the_plan_outside_it_running() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let planned_runs = Arc::new(AtomicI32::new(0));
    let sink = Arc::new(CountingMetrics::default());
    let manager = JobsManager::builder()
        .in_memory()
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(20))
        .job("planning_skip_job", {
            let planned_runs = Arc::clone(&planned_runs);
            move |j| {
                j.max_iterations(1);
                j.add_task(
                    TaskDefinition::new("detect", Duration::from_secs(5)),
                    task_fn(|ctx| async move {
                        ctx.job().add_task(TaskDefinition::new("planned", Duration::from_secs(5)))?;
                        Ok(TaskOutcome::SkippedBranch("nothing to detect".to_string()))
                    }),
                );
                j.add_task_executor(
                    "planned",
                    task_fn(move |_ctx| {
                        let planned_runs = Arc::clone(&planned_runs);
                        async move {
                            planned_runs.fetch_add(1, Ordering::SeqCst);
                            Ok(TaskOutcome::empty())
                        }
                    }),
                );
            }
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("planning_skip_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert_eq!(
        planned_runs.load(Ordering::SeqCst),
        1,
        "a skip is not a refusal, so it takes back nothing the execution planned"
    );
    assert_eq!(sink.completed_iterations(), 1);
    Ok(())
}
