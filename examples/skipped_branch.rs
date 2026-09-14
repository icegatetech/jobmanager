// A branch that turned out to have no work in it.
//
// `detect` looks for something to process and finds nothing. That is not a failure - nothing went
// wrong, there is simply nothing for this branch to do - and reporting it as one would end the
// iteration as failed and put an error in front of an operator who has nothing to fix.
//
// `TaskOutcome::SkippedBranch` is the answer for that: the task ends without a result, whatever
// waits on it is put out with it, and an iteration whose remaining work finished ends as completed.
// `report` here waits on nothing and runs either way, which is what tells the branch apart from the
// iteration.

#![allow(missing_docs)]

mod harness;

use jobmanager::prelude::*;

const DETECT_TASK_CODE: &str = "detect";
const PREPARE_TASK_CODE: &str = "prepare";
const REPORT_TASK_CODE: &str = "report";

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing()?;

    let job_code = JobCode::new("skipped-branch");
    let manager = JobsManager::builder()
        .s3(harness::build_run_scoped_s3_config("skipped-branch")?)
        .workers(2)
        .poll_interval(Duration::from_millis(300))
        .job(job_code.clone(), |j| {
            j.max_iterations(1);

            let detect = j.add_task(
                TaskDefinition::new(DETECT_TASK_CODE, Duration::from_secs(30)),
                task_fn(detect_work),
            );
            let prepare = j.add_task(
                TaskDefinition::new(PREPARE_TASK_CODE, Duration::from_secs(30)),
                task_fn(prepare_work),
            );
            j.depends_on(prepare, &[detect]);

            j.add_task(
                TaskDefinition::new(REPORT_TASK_CODE, Duration::from_secs(30)),
                task_fn(report_run),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!("example finished: the iteration completed with a branch that never ran");
    Ok(())
}

/// Finds nothing to process and calls its branch pointless. The string is the reason a dependent
/// would read through `ImmutableTask::get_resolution_reason`.
async fn detect_work(_ctx: TaskContext) -> TaskResult {
    tracing::info!("nothing to process in this run");

    Ok(TaskOutcome::SkippedBranch("no input files for today".to_string()))
}

/// Never runs: it waits on the branch that was given up on, and the domain puts it out.
async fn prepare_work(_ctx: TaskContext) -> TaskResult {
    tracing::error!("a task waiting behind a skipped branch must never run");
    Ok(().into())
}

/// Runs whatever `detect` decided: it waits on nothing, so it is outside the branch that was
/// skipped.
async fn report_run(_ctx: TaskContext) -> TaskResult {
    tracing::info!("reporting the run, as every iteration does");
    Ok(().into())
}
