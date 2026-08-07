use std::{sync::Arc, time::Duration};

use super::common::counting_metrics::CountingMetrics;
use super::common::storage_wrapper::ContendingStorage;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobsManager, JobsManagerConfig, MetricsSink, Storage,
    TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// Bound on every wait here: a job that never finishes must fail the test rather than hang the
/// suite.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// A pool measures nothing until a sink is registered, so this is what the registration is for: the
/// two measurements a finished iteration produces arrive at the caller's own sink.
#[tokio::test]
async fn a_registered_sink_receives_the_measurements_of_a_completed_iteration() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();

    let sink = Arc::new(CountingMetrics::default());
    let manager = JobsManager::builder()
        .in_memory()
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(20))
        .job("measured_job", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(5)),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("measured_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert_eq!(sink.completed_tasks(), 1);
    assert_eq!(sink.completed_iterations(), 1);
    assert_eq!(sink.failed_iterations(), 0);
    Ok(())
}

/// The other status a finished iteration is reported under: an iteration whose task spent its
/// attempt budget ends as failed, and the sink has to be told so rather than left with nothing.
#[tokio::test]
async fn a_registered_sink_receives_a_failed_iteration() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let sink = Arc::new(CountingMetrics::default());
    let manager = JobsManager::builder()
        .in_memory()
        .metrics(Arc::clone(&sink) as Arc<dyn MetricsSink>)
        .poll_interval(Duration::from_millis(20))
        .job("failing_measured_job", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(5)).with_max_attempts(1),
                task_fn(|_ctx| async { Err("always fails".into()) }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("failing_measured_job")),
    )
    .await??;
    handle.shutdown().await?;

    assert_eq!(sink.failed_tasks(), 1);
    assert_eq!(sink.failed_iterations(), 1);
    assert_eq!(sink.completed_iterations(), 0);
    Ok(())
}

/// A conflict is what the retry measurement counts, so the fixture has to produce exactly one and
/// the measurement has to arrive exactly once.
///
/// The manager is assembled directly rather than through the builder: a storage double cannot be
/// handed to `JobsManagerBuilder`, which only takes `s3()` or `in_memory()`. What the builder does
/// with a sink is covered by the two tests above; this one covers the worker writing into it.
#[tokio::test]
async fn a_save_that_lost_a_race_is_measured_once_as_a_retry() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("contended_measured_job");
    let job_def = JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(
            TaskDefinition::new("t", Duration::from_secs(5)),
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        )],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_max_iterations(1)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);

    let contending = Arc::new(ContendingStorage::new(
        Arc::new(InMemoryStorage::new()) as Arc<dyn Storage>
    ));
    let sink = Arc::new(CountingMetrics::default());

    let manager = JobsManager::new(
        Arc::clone(&contending) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: super::common::build_worker_config(Duration::from_millis(20), Duration::ZERO),
            ..Default::default()
        },
        job_registry,
        Arc::clone(&sink) as Arc<dyn MetricsSink>,
    )?;

    let handle = manager.start()?;
    tokio::time::timeout(WAIT_TIMEOUT, handle.wait_for_job_completion(&job_code)).await??;
    handle.shutdown().await?;

    assert_eq!(
        contending.interferences(),
        1,
        "the fixture must really have produced a conflict"
    );
    assert_eq!(sink.save_conflict_retries(), 1);
    assert_eq!(
        sink.stolen_tasks(),
        0,
        "the interfering write leaves the task with this worker, so the conflict is a merge"
    );
    Ok(())
}
