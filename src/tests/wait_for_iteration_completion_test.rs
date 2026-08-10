use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use super::common::s3_container::S3TestContainer;
use crate::{JobCode, JobStateCodecKind, JobsManager, S3StorageConfig, TaskDefinition, TaskOutcome, task_fn};

/// Bound on every wait here: a broken wait must fail the test rather than hang the suite.
const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// The wait returns on the iteration actually finishing, not on a timer: the executor is slower
/// than any poll interval, so a wait that returned early would find the counter still at zero.
#[tokio::test]
async fn wait_for_iteration_completion_returns_when_the_iteration_finishes() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let runs = Arc::new(AtomicUsize::new(0));
    let runs_in_executor = Arc::clone(&runs);

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(100))
        .job("awaited_job", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("slow", Duration::from_secs(10)),
                task_fn(move |_ctx| {
                    let runs = Arc::clone(&runs_in_executor);
                    async move {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        runs.fetch_add(1, Ordering::SeqCst);
                        Ok(TaskOutcome::empty())
                    }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    let job_code = JobCode::new("awaited_job");

    let iter_num = tokio::time::timeout(WAIT_TIMEOUT, handle.wait_for_iteration_completion(&job_code)).await??;

    assert_eq!(iter_num, 1, "the first iteration must report its own number");
    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the wait must return only after the executor finished"
    );

    handle.shutdown().await?;
    Ok(())
}

/// An iteration that ends in failure has to end the wait too; without the report from the
/// exhausted-attempts path this would run into the timeout.
#[tokio::test]
async fn wait_for_iteration_completion_returns_on_a_failed_iteration() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(50))
        .job("failing_job", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("always_fails", Duration::from_secs(10)).with_max_attempts(1),
                task_fn(|_ctx| async { Err("always fails".into()) }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;

    tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_iteration_completion(&JobCode::new("failing_job")),
    )
    .await??;

    handle.shutdown().await?;
    Ok(())
}

/// The second wait is the assertion: it asks about a job that has certainly finished already, which
/// a wait listening only for further reports would never answer.
#[tokio::test]
async fn wait_for_job_completion_returns_for_an_already_finished_job() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let manager = JobsManager::builder()
        .in_memory()
        .poll_interval(Duration::from_millis(1))
        .job("finished_job", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(5)),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    let job_code = JobCode::new("finished_job");

    tokio::time::timeout(WAIT_TIMEOUT, handle.wait_for_job_completion(&job_code)).await??;
    tokio::time::timeout(WAIT_TIMEOUT, handle.wait_for_job_completion(&job_code)).await??;

    handle.shutdown().await?;
    Ok(())
}

/// A pool learns which iterations finished from its own workers, and a restarted pool has none of
/// that history: the iteration it must report is the one already sitting in storage. Without
/// reporting it on pickup the second wait here runs into its timeout.
#[tokio::test]
async fn wait_for_job_completion_returns_for_a_job_finished_by_an_earlier_pool()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let store = S3TestContainer::start().await?;
    let job_code = JobCode::new("restarted_job");
    let runs = Arc::new(AtomicUsize::new(0));

    // Both pools describe the same job over the same prefix, which is what a restart of one process
    // looks like to storage.
    let build_manager = || {
        let runs_in_executor = Arc::clone(&runs);
        JobsManager::builder()
            .s3(S3StorageConfig::new(
                store.endpoint(),
                store.username(),
                store.password(),
                "test-jobs",
                "us-east-1",
            )
            .with_bucket_prefix("wait-after-restart")
            .with_job_state_codec(JobStateCodecKind::Json))
            .poll_interval(Duration::from_millis(50))
            .job("restarted_job", move |j| {
                j.max_iterations(1);
                j.add_task(
                    TaskDefinition::new("t", Duration::from_secs(10)),
                    task_fn(move |_ctx| {
                        let runs = Arc::clone(&runs_in_executor);
                        async move {
                            runs.fetch_add(1, Ordering::SeqCst);
                            Ok(TaskOutcome::empty())
                        }
                    }),
                );
            })
            .build()
    };

    let first_handle = build_manager().await?.start()?;
    tokio::time::timeout(WAIT_TIMEOUT, first_handle.wait_for_job_completion(&job_code)).await??;
    first_handle.shutdown().await?;

    let second_handle = build_manager().await?.start()?;
    tokio::time::timeout(WAIT_TIMEOUT, second_handle.wait_for_job_completion(&job_code)).await??;
    second_handle.shutdown().await?;

    assert_eq!(
        runs.load(Ordering::SeqCst),
        1,
        "the second pool must observe the stored iteration, not run a new one"
    );
    Ok(())
}

/// Nothing finishes an iteration once the pool is gone, so the wait must fail rather than outlive
/// the workers it observes.
#[tokio::test]
async fn wait_for_iteration_completion_fails_when_the_pool_stopped() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let manager = JobsManager::builder()
        .in_memory()
        .job("aborted_job", |j| {
            j.max_iterations(1);
            // Longer than the test can take, so the pool is aborted with the iteration unfinished.
            j.add_task(
                TaskDefinition::new("endless", Duration::from_mins(10)),
                task_fn(|_ctx| async {
                    tokio::time::sleep(Duration::from_mins(10)).await;
                    Ok(TaskOutcome::empty())
                }),
            );
        })
        .build()
        .await?;

    let mut handle = manager.start()?;
    handle.abort();

    let error = tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_iteration_completion(&JobCode::new("aborted_job")),
    )
    .await?
    .err()
    .ok_or("waiting on a stopped pool must fail")?;

    assert!(error.to_string().contains("worker pool stopped"), "got: {error}");
    Ok(())
}

/// A pool only records the iterations of the jobs it runs, so a wait for any other job would never
/// end. A mistyped code is the ordinary way to get here, and it has to come back as an error rather
/// than as a wait that never returns.
#[tokio::test]
async fn wait_for_iteration_completion_refuses_a_job_this_pool_does_not_run() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();

    let manager = build_one_job_pool("known_job").await?;
    let handle = manager.start()?;

    let error = tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_iteration_completion(&JobCode::new("job_of_another_pool")),
    )
    .await?
    .err()
    .ok_or("waiting for a job outside the pool must fail")?;

    assert!(error.to_string().contains("is not run by this pool"), "got: {error}");

    handle.shutdown().await?;
    Ok(())
}

/// The same mistake on the other wait is caught one step earlier, by the registry, and says so:
/// the job has no description here at all, not merely no finished iteration.
#[tokio::test]
async fn wait_for_job_completion_refuses_a_job_this_pool_does_not_run() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let manager = build_one_job_pool("known_job").await?;
    let handle = manager.start()?;

    let error = tokio::time::timeout(
        WAIT_TIMEOUT,
        handle.wait_for_job_completion(&JobCode::new("job_of_another_pool")),
    )
    .await?
    .err()
    .ok_or("waiting for a job outside the pool must fail")?;

    assert!(error.to_string().contains("not found"), "got: {error}");

    handle.shutdown().await?;
    Ok(())
}

/// A pool running exactly one job, for the two tests that ask it about a different one.
async fn build_one_job_pool(job_code: &str) -> Result<JobsManager, Box<dyn std::error::Error>> {
    let manager = JobsManager::builder()
        .in_memory()
        .job(job_code, |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(5)),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            );
        })
        .build()
        .await?;
    Ok(manager)
}

/// An endless job never completes, so waiting for its completion would hang; the request is
/// refused instead.
#[tokio::test]
async fn wait_for_job_completion_refuses_a_job_without_an_iteration_limit() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let manager = JobsManager::builder()
        .in_memory()
        .job("endless_job", |j| {
            j.add_task(
                TaskDefinition::new("t", Duration::from_secs(5)),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    let error = handle
        .wait_for_job_completion(&JobCode::new("endless_job"))
        .await
        .err()
        .ok_or("waiting for an endless job must be refused")?;

    assert!(error.to_string().contains("no iteration limit"), "got: {error}");

    handle.shutdown().await?;
    Ok(())
}
