// Long-running work that stops promptly on shutdown.
//
// A task that ignores its cancellation token delays the whole pool's shutdown by however long its
// work takes. Selecting on the token instead lets the worker drain immediately, and returning
// `TaskOutcome::Cancelled` persists nothing: the task stays open and is picked up again on a later
// run. The attempt it already spent is not returned, which is why the token is checked between units
// of work rather than only at the start.
//
// That is also why this example wears out, and why two runs in a row do not both show the same
// thing. Its state lives under a fixed prefix - a prefix unique per run would hide the very thing
// the example shows, an unfinished task outliving the process - and the cancelled task is left
// started, holding the deadline it was given. So:
//
// - a run started before `IMPORT_TIMEOUT` has passed finds the task still held and prints no step
//   and no cancellation;
// - a run after it takes the expired task over, which spends one of `RUNS_BEFORE_BUDGET_SPENT`
//   attempts;
// - once those are spent the expired task is failed, the iteration ends failed, and every further
//   run is silent for good.
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
/// Attempts the import task is given, and so the number of runs that still show a cancellation:
/// each pickup spends one attempt, and a cancelled task does not get it back.
const RUNS_BEFORE_BUDGET_SPENT: u32 = 5;

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing();

    let manager = JobsManager::builder()
        .s3(harness::build_s3_config("graceful-shutdown"))
        .job("graceful shutdown", |j| {
            j.add_task(
                TaskDefinition::new(TASK_CODE, TASK_TIMEOUT).with_max_attempts(RUNS_BEFORE_BUDGET_SPENT),
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
