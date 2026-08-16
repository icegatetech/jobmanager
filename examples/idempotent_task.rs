// A task whose deadline expires while it is still running.
//
// A deadline cancels the executor's token but does not stop it: once the deadline passes another
// worker may take the task over, and an executor that does not select on its token keeps running.
// Two executors running the same task is a legal state, so the side effect at the end has to be
// guarded - here by a compare-and-set on the committed offset.
//
// The takeover spends no attempt: nothing refused the task. What bounds the takeovers is the task's
// maximum lifetime, five deadlines by default.

#![allow(missing_docs)]

mod harness;

use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use jobmanager::prelude::*;
use parking_lot::Mutex;

const TASK_CODE: &str = "commit";

/// Deadline of the task. Shorter than [`STALLED_WORK_DURATION`]: the point is the takeover.
const TASK_DEADLINE: Duration = Duration::from_secs(3);
/// How long the first execution takes. It stands in for a stalled upstream call, and it is what
/// makes the deadline pass while an executor is still running.
const STALLED_WORK_DURATION: Duration = Duration::from_secs(5);
/// How long every later execution takes. Inside the deadline, so the takeover happens exactly once
/// and the example does not thrash between workers. Both durations together stay well inside the
/// default lifetime of five deadlines, so nothing here is failed for outliving it.
const NORMAL_WORK_DURATION: Duration = Duration::from_secs(1);
/// The offset this iteration is meant to reach.
const TARGET_OFFSET: u64 = 42;

/// Stands in for the durable state a side effect writes - a table, a row, an object.
struct CommittedOffset {
    value: Mutex<u64>,
}

impl CommittedOffset {
    const fn new() -> Self {
        Self { value: Mutex::new(0) }
    }

    /// Whether `target` was already reached. Read before doing the work, so a second executor that
    /// arrives after the first one committed does nothing at all.
    fn has_reached_offset(&self, target: u64) -> bool {
        *self.value.lock() >= target
    }

    /// Advances to `target` only if it is still ahead. Returns whether this call was the one that
    /// moved it - the guard that makes a duplicate execution harmless.
    fn advance_offset(&self, target: u64) -> bool {
        let mut value = self.value.lock();
        if *value >= target {
            return false;
        }
        *value = target;
        true
    }

    /// The offset reached so far, for the caller that outlives the pool.
    fn current_offset(&self) -> u64 {
        *self.value.lock()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing();

    let committed = Arc::new(CommittedOffset::new());
    // Counts executions rather than reading the task's attempts: a takeover spends no attempt, so
    // the attempt counter no longer tells the first executor from the one that replaced it.
    let executions = Arc::new(AtomicU32::new(0));
    let job_code = JobCode::new("idempotent-task");

    let manager = JobsManager::builder()
        .s3(harness::build_run_scoped_s3_config("idempotent-task"))
        // Two workers, so the second one can take the expired task over.
        .workers(2)
        .poll_interval(Duration::from_millis(300))
        .job(job_code.clone(), |j| {
            j.max_iterations(1);
            let committed = Arc::clone(&committed);
            let executions = Arc::clone(&executions);
            j.add_task(
                TaskDefinition::new(TASK_CODE, TASK_DEADLINE),
                task_fn(move |_ctx: TaskContext| {
                    let committed = Arc::clone(&committed);
                    let execution = executions.fetch_add(1, Ordering::SeqCst) + 1;
                    async move { commit_offset(&committed, execution).await }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!(
        committed_offset = committed.current_offset(),
        "example finished; the offset advanced exactly once however many executors ran"
    );
    Ok(())
}

/// Commits the target offset, tolerating being run more than once for the same task.
async fn commit_offset(committed: &CommittedOffset, execution: u32) -> TaskResult {
    // Guard one: the work itself is expensive, so skip it when the effect already happened.
    if committed.has_reached_offset(TARGET_OFFSET) {
        tracing::info!(execution, "offset already committed; nothing to do");
        return Ok(().into());
    }

    let work_duration = if execution == 1 {
        STALLED_WORK_DURATION
    } else {
        NORMAL_WORK_DURATION
    };
    tracing::info!(execution, work_seconds = work_duration.as_secs(), "starting work");
    tokio::time::sleep(work_duration).await;

    // Guard two: another executor may have committed while this one was working.
    if committed.advance_offset(TARGET_OFFSET) {
        tracing::info!(execution, offset = TARGET_OFFSET, "committed the offset");
    } else {
        tracing::warn!(execution, "another executor committed first; this run was a duplicate");
    }

    Ok(().into())
}
