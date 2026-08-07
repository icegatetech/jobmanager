// An attempt budget running out.
//
// Every start spends an attempt - a failure and a takeover of an expired task alike. Once the budget
// is gone the task is terminal: it is never picked up again, tasks blocked behind it never run, and
// the iteration ends as failed. The job itself is not over - the next iteration starts on its normal
// schedule and is planned from scratch, which is what keeps one broken task from blocking its
// dependents forever.
//
// The default budget is five (`DEFAULT_MAX_ATTEMPTS`); two is used here so the example is quick.

#![allow(missing_docs)]

mod support;

use jobmanager::prelude::*;

const BROKEN_TASK_CODE: &str = "broken";
const DEPENDENT_TASK_CODE: &str = "dependent";

const MAX_ATTEMPTS: u32 = 2;

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    let job_code = JobCode::new("attempt-budget");
    let manager = JobsManager::builder()
        .s3(support::build_run_scoped_s3_config("attempt-budget"))
        .workers(2)
        .poll_interval(Duration::from_millis(300))
        .job(job_code.clone(), |j| {
            // Two iterations, so the replan after the failed one is visible.
            j.max_iterations(2);
            j.every(Duration::from_secs(3));

            let broken = j.add_task(
                TaskDefinition::new(BROKEN_TASK_CODE, Duration::from_secs(30)).with_max_attempts(MAX_ATTEMPTS),
                task_fn(fail_every_attempt),
            );
            let dependent = j.add_task(
                TaskDefinition::new(DEPENDENT_TASK_CODE, Duration::from_secs(30)),
                task_fn(run_dependent_task),
            );
            j.depends_on(dependent, &[broken]);
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!("example finished");
    Ok(())
}

/// Fails on every attempt, reporting how much of the budget is left.
async fn fail_every_attempt(ctx: TaskContext) -> TaskResult {
    let attempt = ctx.task().attempts();
    tracing::warn!(attempt, max_attempts = MAX_ATTEMPTS, "failing on purpose");
    Err("this task never succeeds".into())
}

/// Never runs: it is blocked behind a task that never completes.
async fn run_dependent_task(_ctx: TaskContext) -> TaskResult {
    tracing::error!("the dependent task must never run while its dependency has not completed");
    Ok(().into())
}
