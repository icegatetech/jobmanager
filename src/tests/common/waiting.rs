// Bounded waits shared by the tests that measure what a pool costs.

use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::{JobCode, JobStatus, Storage};

/// Bound on every wait here, so a fixture that never reaches its condition fails with a diagnostic
/// instead of hanging.
pub const CONDITION_TIMEOUT: Duration = Duration::from_secs(20);

/// How long a counter must stay put before a baseline is read off it, on top of the step below.
pub const QUIET_PERIOD: Duration = Duration::from_millis(300);

/// How long counters are watched once a baseline is taken. Well above the interval the test pools
/// poll at, so a pass that must not happen has room to happen many times over.
pub const OBSERVATION_WINDOW: Duration = Duration::from_millis(600);

/// How often a condition is re-checked while it does not hold.
const POLL_STEP: Duration = Duration::from_millis(10);

/// Polls `condition` until it holds, and fails with `what` if it never does.
pub async fn wait_until<F: FnMut() -> bool>(mut condition: F, what: &str) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + CONDITION_TIMEOUT;
    while !condition() {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting until {what}").into());
        }
        tokio::time::sleep(POLL_STEP).await;
    }

    Ok(())
}

/// Waits until the stored job carries a terminal status - the state that makes it wait for its next
/// iteration instead of being worked on.
///
/// `store` is read directly rather than through whatever a measurement counts, so proving the
/// iteration finished costs nothing the measurement is about.
pub async fn wait_until_job_is_processed(
    store: &dyn Storage,
    job_code: &JobCode,
    cancel_token: &CancellationToken,
) -> Result<(), Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + CONDITION_TIMEOUT;
    loop {
        let stored_status = store.get_job(job_code, cancel_token).await.map(|job| job.status().clone());
        if matches!(stored_status, Ok(JobStatus::Completed | JobStatus::Failed)) {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting for the iteration to finish, got {stored_status:?}").into());
        }
        tokio::time::sleep(POLL_STEP).await;
    }
}

/// Waits until `count_requests` stops growing, then a quiet period on top, and answers with the
/// value the counter settled at - the baseline an observation window is measured against.
///
/// A measurement cannot start the moment its condition holds: the passes already in flight still
/// have their requests to issue, and a worker that lost a race re-reads the job through a cache its
/// conflict just invalidated, which costs a listing. Waiting for the counter itself is what tells
/// those requests apart from the ones the window is about, and the counter is compared across a
/// step rather than against itself, so a counter read twice in a row is not mistaken for a settled
/// one. A request issued during the quiet period restarts the wait rather than ending it: the pass
/// it belongs to has the rest of its requests still to issue, and those would land in the window
/// and be counted against it.
pub async fn measure_settled_requests<F: FnMut() -> u64>(
    mut count_requests: F,
    what: &str,
) -> Result<u64, Box<dyn std::error::Error>> {
    let deadline = tokio::time::Instant::now() + CONDITION_TIMEOUT;
    let mut last_seen = count_requests();
    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(format!("timed out waiting until {what}").into());
        }
        tokio::time::sleep(POLL_STEP).await;
        let issued = count_requests();
        if issued != last_seen {
            last_seen = issued;
            continue;
        }

        tokio::time::sleep(QUIET_PERIOD).await;
        let settled = count_requests();
        if settled == issued {
            return Ok(settled);
        }
        last_seen = settled;
    }
}
