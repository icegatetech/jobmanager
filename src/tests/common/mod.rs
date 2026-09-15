// Common test utilities

use std::sync::OnceLock;
use std::time::Duration;

use tracing_subscriber::{EnvFilter, fmt};

use crate::{Error, Job, JobCode, JobDefinition, JobDefinitionRegistry, JobMeta, WorkerConfig};

#[cfg(feature = "storage-azure")]
pub mod azure_container;
pub mod counting_metrics;
#[cfg(feature = "storage-gcs")]
pub mod gcs_container;
pub mod manager_env;
pub mod provider_harness;
#[cfg(feature = "storage-s3")]
pub mod s3_container;
pub mod scripted_endpoint;
pub mod silent_endpoint;
pub mod storage_wrapper;
pub mod waiting;

/// The iteration and version `job` stands at - what a conditional read asks the store about.
pub fn meta_of(job: &Job) -> JobMeta {
    JobMeta {
        code: job.code().clone(),
        iter_num: job.iter_num(),
        version: job.version().to_string(),
    }
}

/// Persists `iter_nums` as completed iterations of `job_code`, so a case can start from a job that
/// already has a tail without running a manager to produce one.
///
/// The iterations are written through `storage`, which is what lets a case that counts requests
/// build its fixture through a store of its own and leave the counters to the code under test.
///
/// # Errors
///
/// Returns whatever `storage` refuses a write with.
pub async fn persist_completed_iterations(
    storage: &dyn crate::Storage,
    job_code: &JobCode,
    iter_nums: impl IntoIterator<Item = u64> + Send,
) -> Result<(), Box<dyn std::error::Error>> {
    let cancel_token = crate::CancellationToken::new();
    let job_id = uuid::Uuid::from_u128(7_001);
    let worker_id = uuid::Uuid::from_u128(7_002);

    for iter_num in iter_nums {
        let mut job = Job::restore(
            job_id,
            job_code.clone(),
            String::new(),
            iter_num,
            crate::JobStatus::Completed,
            Vec::new(),
            worker_id,
            chrono::Utc::now(),
            None,
            Some(chrono::Utc::now()),
            None,
            std::collections::HashMap::new(),
            None,
            None,
            crate::TaskLimits::default(),
        );
        storage.save_job(&mut job, &cancel_token).await?;
    }

    Ok(())
}

/// Registry double for a path that must never resolve a job definition - connecting to a store,
/// deleting a state object. Reaching for one there is a defect, so it fails instead of returning
/// something plausible.
pub struct UnusedJobRegistry;

impl JobDefinitionRegistry for UnusedJobRegistry {
    fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error> {
        Err(Error::Other(format!(
            "this path must not need the definition of job {code}"
        )))
    }
}

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
