// A task that runs even though what it depends on never produced anything.
//
// By default a task waits for every dependency to complete, so a dependency that failed for good
// takes its dependents down with it and the iteration ends as failed.
// `TaskDefinition::with_dependency_tolerance` declares the exception: `enrich` below starts on a
// `detect` that refused for good, reads the state that refusal left, and takes a degraded path.
//
// Tolerance is declared per state - one that survives a failure does not thereby survive a branch
// somebody skipped - and it is about a dependency that will never run again: one that failed with
// attempts to spare is still coming, and is waited for either way.
//
// A failure a dependent declared it survives and then resolved is a handled failure, so the
// iteration ends as completed even though it holds a task that failed.

#![allow(missing_docs)]

mod harness;

use jobmanager::prelude::*;

const DETECT_TASK_CODE: &str = "detect";
const ENRICH_TASK_CODE: &str = "enrich";

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing()?;

    let job_code = JobCode::new("degraded-dependency");
    let manager = JobsManager::builder()
        .s3(harness::build_run_scoped_s3_config("degraded-dependency")?)
        .workers(2)
        .poll_interval(Duration::from_millis(300))
        .job(job_code.clone(), |j| {
            j.max_iterations(1);

            let detect = j.add_task(
                TaskDefinition::new(DETECT_TASK_CODE, Duration::from_secs(30)),
                task_fn(detect_work),
            );
            let enrich = j.add_task(
                TaskDefinition::new(ENRICH_TASK_CODE, Duration::from_secs(30)).with_dependency_tolerance(
                    DependencyTolerance {
                        allows_failed: true,
                        allows_skipped: false,
                    },
                ),
                task_fn(enrich_on_what_detect_left),
            );
            j.depends_on(enrich, &[detect]);
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!("example finished: the iteration completed on a degraded path");
    Ok(())
}

/// Refuses for good, so the dependent below has something unreachable to start on. An ordinary
/// `Err` would be retried and the dependent would keep waiting for it.
async fn detect_work(_ctx: TaskContext) -> TaskResult {
    tracing::warn!("the reference catalogue is not published for today");

    Ok(TaskOutcome::TerminallyFailed(
        "reference catalogue missing for today".to_string(),
    ))
}

/// Reads the state its dependency ended in and decides for itself what it can still produce. A
/// tolerant task is handed no result, so this is where the degraded path is chosen.
async fn enrich_on_what_detect_left(ctx: TaskContext) -> TaskResult {
    let dependencies = ctx.job().get_tasks_by_code(&TaskCode::new(DETECT_TASK_CODE))?;
    let detect = dependencies
        .first()
        .ok_or_else(|| Error::Other("the job must hold its detect task".to_string()))?;

    if detect.is_completed() {
        tracing::info!("enriching with the reference catalogue");
        return Ok(b"enriched".to_vec().into());
    }
    if detect.is_skipped() {
        tracing::info!("the reference branch was skipped, enriching without it");
        return Ok(b"plain".to_vec().into());
    }

    tracing::warn!(
        reason = detect.get_resolution_reason(),
        "the reference catalogue is unavailable"
    );
    Ok(b"plain".to_vec().into())
}
