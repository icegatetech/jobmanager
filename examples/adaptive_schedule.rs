// A job that sets its own next start time.
//
// `set_next_start_at` takes precedence over the iteration interval in both directions: it can hold
// the next iteration past the interval or release it earlier. That is what a backlog drainer wants -
// poll hard while there is a queue, idle cheaply when there is not - and it is consulted once the
// current iteration finishes, while the iteration budget is not spent.

#![allow(missing_docs)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use chrono::{Duration as ChronoDuration, Utc};
use jobmanager::prelude::*;

const TASK_CODE: &str = "drain";

/// How many items the stand-in queue starts with.
const QUEUE_DEPTH: u64 = 12;
/// How many are taken per iteration.
const BATCH_SIZE: u64 = 5;
/// Fallback cadence the executor overrides in both directions.
const ITERATION_INTERVAL: Duration = Duration::from_secs(3);
/// Delay asked for once the queue is empty. Longer than [`ITERATION_INTERVAL`], so a run that waits
/// this long proves the override held the next iteration back rather than the interval releasing it.
const IDLE_DELAY_SECONDS: i64 = 10;

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    // Stands in for a queue depth read from a broker or a table.
    let remaining = Arc::new(AtomicU64::new(QUEUE_DEPTH));
    let job_code = JobCode::new("adaptive-schedule");

    let manager = JobsManager::builder()
        .s3(support::build_run_scoped_s3_config("adaptive-schedule"))
        .job(job_code.clone(), |j| {
            j.every(ITERATION_INTERVAL);
            j.max_iterations(4);
            let remaining = Arc::clone(&remaining);
            j.add_task(
                TaskDefinition::new(TASK_CODE, Duration::from_secs(30)),
                task_fn(move |ctx: TaskContext| {
                    let remaining = Arc::clone(&remaining);
                    async move { drain_queue(&remaining, &ctx) }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!("example finished");
    Ok(())
}

/// Takes one batch off the queue and schedules the next iteration to match what is left.
fn drain_queue(remaining: &AtomicU64, ctx: &TaskContext) -> TaskResult {
    // The update closure always returns `Some`, so the `Err` arm is unreachable; it carries the
    // unchanged depth, which is the same value the success arm reports.
    let queue_depth = remaining
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |depth| {
            Some(depth.saturating_sub(BATCH_SIZE))
        })
        .unwrap_or_else(|unchanged_depth| unchanged_depth);
    let taken = queue_depth.min(BATCH_SIZE);
    let left = queue_depth.saturating_sub(BATCH_SIZE);

    if left > 0 {
        // Work is left: release the next iteration now rather than waiting out the interval.
        ctx.job().set_next_start_at(Utc::now())?;
        tracing::info!(taken, left, "batch drained; next iteration released immediately");
    } else {
        ctx.job()
            .set_next_start_at(Utc::now() + ChronoDuration::seconds(IDLE_DELAY_SECONDS))?;
        tracing::info!(taken, "queue empty; next iteration pushed out");
    }

    Ok(().into())
}
