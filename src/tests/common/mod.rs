// Common test utilities

use std::sync::OnceLock;
use std::time::Duration;

use tracing_subscriber::{EnvFilter, fmt};

use crate::WorkerConfig;

pub mod counting_metrics;
pub mod manager_env;
pub mod s3_container;
pub mod storage_wrapper;

/// Worker settings a test polls by: an interval short enough that a test does not wait out the
/// defaults, plus the jitter it wants the workers spread by.
pub fn build_worker_config(poll_interval: Duration, poll_jitter: Duration) -> WorkerConfig {
    WorkerConfig::new()
        .with_poll_interval(poll_interval)
        .expect("a positive poll interval is accepted")
        .with_poll_jitter(poll_jitter)
}

pub fn init_tracing() {
    static INIT: OnceLock<()> = OnceLock::new();

    let () = INIT.get_or_init(|| {
        let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("jobmanager=debug"));
        let _ = fmt().with_env_filter(filter).with_test_writer().try_init();
    });
}
