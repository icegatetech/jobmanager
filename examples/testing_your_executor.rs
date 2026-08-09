// How to test an executor of your own.
//
// `.in_memory()` swaps the S3 backend for a no-persistence one: no container, no bucket,
// nothing to clean up between runs. Cap the job at one iteration and wait for it, and driving the
// pool becomes an ordinary function call - which is what turns this from a demo into a test.
//
// This file is both:
//
//   cargo run  --example testing_your_executor    # walks through it with logging
//   cargo test --example testing_your_executor    # runs the assertions
//
// The second one works because `Cargo.toml` gives this example `test = true`.
//
// Reaching an executor's effect from outside is the problem a test has to solve. Holding that state
// behind an `Arc` the caller also holds is the simplest answer, and a struct executor
// (see `struct_executor.rs`) gives it for free.

#![allow(missing_docs)]

mod harness;

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use jobmanager::prelude::*;

const SUM_TASK_CODE: &str = "sum";

/// Input the task under test is given.
const SUM_INPUT: &[u8] = b"1,2,3,4";

/// Runs one iteration of the job under test on the in-memory backend and returns once it finished.
///
/// This is the whole harness. Copy it into your own `tests/`, swap the executor, and assert on
/// whatever state you handed it.
async fn run_one_iteration(total: Arc<AtomicU64>) -> Result<()> {
    let job_code = JobCode::new("sum job");
    let manager = JobsManager::builder()
        .in_memory()
        .workers(1)
        .job(job_code.clone(), move |j| {
            // The iteration cap is what makes the run finite, and what `wait_for_job_completion`
            // needs: a job with no cap never completes and waiting on it would hang.
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new(SUM_TASK_CODE, Duration::from_secs(5)).with_input(SUM_INPUT.to_vec()),
                task_fn(move |ctx: TaskContext| {
                    let total = Arc::clone(&total);
                    async move { sum_input(&total, &ctx) }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await
}

/// The executor under test: sums the comma-separated numbers in its input into the shared total.
fn sum_input(total: &AtomicU64, ctx: &TaskContext) -> TaskResult {
    let input = String::from_utf8_lossy(ctx.input()).to_string();
    let mut sum: u64 = 0;
    for part in input.split(',') {
        sum += part.trim().parse::<u64>()?;
    }

    total.fetch_add(sum, Ordering::SeqCst);
    tracing::info!(sum, "summed the input");
    Ok(sum.to_string().into_bytes().into())
}

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing();

    let total = Arc::new(AtomicU64::new(0));
    run_one_iteration(Arc::clone(&total)).await?;

    tracing::info!(total = total.load(Ordering::SeqCst), "example finished");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Asserting is the point here, and `docs/RUST.md` bans `unwrap` outside tests, not in them.
    #![allow(clippy::unwrap_used)]

    use super::*;

    /// The sum `SUM_INPUT` must produce.
    const EXPECTED_SUM: u64 = 10;

    #[tokio::test]
    async fn executor_sums_its_input() {
        let total = Arc::new(AtomicU64::new(0));

        run_one_iteration(Arc::clone(&total)).await.unwrap();

        assert_eq!(total.load(Ordering::SeqCst), EXPECTED_SUM);
    }

    /// The harness has to be re-runnable: the in-memory backend keeps nothing between pools, so a
    /// second run starts from a clean job rather than seeing the first run's completed iteration.
    #[tokio::test]
    async fn each_run_starts_from_a_clean_job() {
        let total = Arc::new(AtomicU64::new(0));

        run_one_iteration(Arc::clone(&total)).await.unwrap();
        run_one_iteration(Arc::clone(&total)).await.unwrap();

        assert_eq!(total.load(Ordering::SeqCst), EXPECTED_SUM * 2);
    }
}
