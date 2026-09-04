// A refusal that will not become a success on a later attempt.
//
// The ordinary way to refuse a task is to return `Err`: the refusal spends one attempt and the task
// is picked up again while its attempt budget and its maximum lifetime last. That is the right
// answer for a flaky call, and the wrong one for input that will not parse - the fifth attempt
// decodes it no better than the first, and four executions are spent finding that out.
//
// `TaskOutcome::TerminallyFailed` is the other answer: the task is terminal at once, whatever is
// left of its budget, and the iteration ends as failed. The log below shows the executor called
// once, not five times over.
//
// The job itself is not over - the next iteration starts on its normal schedule and is planned from
// scratch. Two iterations are run here so that replan is visible.

#![allow(missing_docs)]

mod harness;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use jobmanager::prelude::*;

const DECODE_TASK_CODE: &str = "decode";

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing();

    let executions = Arc::new(AtomicU32::new(0));
    let job_code = JobCode::new("terminal-failure");
    let manager = JobsManager::builder()
        .s3(harness::build_run_scoped_s3_config("terminal-failure"))
        .workers(2)
        .poll_interval(Duration::from_millis(300))
        .job(job_code.clone(), {
            let executions = Arc::clone(&executions);
            move |j| {
                j.max_iterations(2);
                j.every(Duration::from_secs(3));
                j.add_task(
                    TaskDefinition::new(DECODE_TASK_CODE, Duration::from_secs(30)).with_input(b"<not json>".to_vec()),
                    task_fn(move |ctx| {
                        let executions = Arc::clone(&executions);
                        async move { Ok(decode_input(&ctx, &executions)) }
                    }),
                );
            }
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!(
        executions = executions.load(Ordering::SeqCst),
        "example finished: one execution per iteration, not one per attempt"
    );
    Ok(())
}

/// Refuses the input for good: the payload is not the JSON this task is written for, and no retry
/// changes that.
fn decode_input(ctx: &TaskContext, executions: &Arc<AtomicU32>) -> TaskOutcome {
    let execution = executions.fetch_add(1, Ordering::SeqCst) + 1;
    let input = String::from_utf8_lossy(ctx.input()).to_string();
    tracing::warn!(
        execution,
        attempt = ctx.task().attempts(),
        max_attempts = ctx.task().max_attempts(),
        "refusing input that will never parse: {input:?}"
    );

    TaskOutcome::TerminallyFailed("input is not the JSON this task decodes".to_string())
}
