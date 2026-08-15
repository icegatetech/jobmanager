// Long-running work that stops promptly on shutdown.
//
// A task that ignores its cancellation token delays the whole pool's shutdown by however long its
// work takes. Selecting on the token instead lets the worker drain immediately, and returning
// `TaskOutcome::Cancelled` persists nothing: the task stays open and is picked up again on a later
// run. Nothing about the task is written back, so the token is checked between units of work rather
// than only at the start.
//
// That is also why this example wears out, and why two runs in a row do not both show the same
// thing. Its state lives under a fixed prefix - a prefix unique per run would hide the very thing
// the example shows, an unfinished task outliving the process - and the cancelled task is left
// started, holding the deadline it was given. So:
//
// - a run started before `TASK_TIMEOUT` has passed finds the task still held and prints no step and
//   no cancellation;
// - a run after it takes the expired task over, which spends no attempt - nothing refused the task;
// - once the task's maximum lifetime has passed the expired task is failed, the iteration ends
//   failed, and - the job being capped at one iteration - every further run is silent for good.
//   Without that cap the next iteration would be planned from scratch and the example would start
//   over instead.
//
// That lifetime runs by the wall clock from the task's first start, so it is spent by time rather
// than by runs: `TASK_LIFETIME_IN_DEADLINES` is an upper bound on the runs that still show a
// cancellation, reached only by starting each run right after the previous deadline passed. Run the
// example rarely enough and the task is already failed on the second run.
//
// Clearing the store starts the count over:
//
//     make examples-infra-down && make examples-infra-up

#![allow(missing_docs)]

mod harness;

use jobmanager::prelude::*;

const TASK_CODE: &str = "import";

/// Units of work the task walks through, so cancellation lands in the middle of the task.
const STEP_COUNT: u32 = 40;
const STEP_DURATION: Duration = Duration::from_millis(500);
/// How long the example lets the job run before asking the pool to stop.
const RUN_BEFORE_SHUTDOWN: Duration = Duration::from_secs(3);
/// Deadline the import task is started with, and so how long a cancelled one stays untouchable.
const TASK_TIMEOUT: Duration = Duration::from_mins(2);
/// Maximum lifetime of the import task, stated in whole deadlines: how many takeovers fit into it
/// at most, since a takeover becomes possible once a deadline has passed.
const TASK_LIFETIME_IN_DEADLINES: u32 = 5;
/// Longest the import task may occupy its iteration, counted by the wall clock from its first
/// start: past it the task is failed and the iteration ends failed.
const TASK_MAX_LIFETIME: Duration = TASK_TIMEOUT.saturating_mul(TASK_LIFETIME_IN_DEADLINES);

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing();

    let manager = JobsManager::builder()
        .s3(harness::build_s3_config("graceful-shutdown"))
        .job("graceful shutdown", |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new(TASK_CODE, TASK_TIMEOUT).with_max_lifetime(TASK_MAX_LIFETIME),
                task_fn(import_rows_in_steps),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tracing::info!(seconds = RUN_BEFORE_SHUTDOWN.as_secs(), "running before shutdown");
    tokio::time::sleep(RUN_BEFORE_SHUTDOWN).await;

    tracing::info!("requesting shutdown");
    handle.shutdown().await?;
    tracing::info!("pool drained");

    Ok(())
}

/// Imports in steps, giving up cleanly the moment a shutdown is requested.
async fn import_rows_in_steps(ctx: TaskContext) -> TaskResult {
    for step in 0..STEP_COUNT {
        tokio::select! {
            () = tokio::time::sleep(STEP_DURATION) => {
                tracing::info!(step, "step done");
            }
            () = ctx.cancel_token().cancelled() => {
                // Nothing is persisted for this task: it stays open on the deadline it already has.
                tracing::warn!(step, "shutdown observed; leaving the task open");
                return Ok(TaskOutcome::Cancelled);
            }
        }
    }

    tracing::info!("import finished");
    Ok(().into())
}
