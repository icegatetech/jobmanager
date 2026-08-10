use std::time::Duration;

use crate::{JobCode, JobStatus, TaskCode, TaskStatus};

/// Where the crate writes its operational measurements.
///
/// Every method has an empty default body, so an implementation overrides only the measurements it
/// cares about. Implementations are called from worker and storage code paths and must not block:
/// record and return.
pub trait MetricsSink: Send + Sync {
    /// The wall-clock duration of one job iteration, tagged with the job code and its final status.
    ///
    /// Called once per iteration, when it reaches a terminal state. `status` therefore takes both
    /// `Completed` (every task finished) and `Failed` (a task spent its attempt budget).
    fn record_job_iteration_complete(&self, _code: &JobCode, _status: &JobStatus, _duration: Duration) {}

    /// The wall-clock duration of a single task execution, tagged with job code, task code and the
    /// task's final status.
    ///
    /// Called once per task, after it finishes processing, whether it completed or failed.
    fn record_task_processed(
        &self,
        _job_code: &JobCode,
        _task_code: &TaskCode,
        _status: &TaskStatus,
        _duration: Duration,
    ) {
    }

    /// The latency of one storage request, tagged with the operation name and its outcome.
    ///
    /// Called after every request the storage layer issues, on success and on failure alike.
    fn record_storage_operation(&self, _operation: &str, _status: &str, _duration: Duration) {}

    /// A storage read served from the in-memory cache, tagged with the calling method.
    fn record_cache_hit(&self, _method: &str) {}

    /// A storage read that had to fall through to the backing store, tagged with the calling method.
    fn record_cache_miss(&self, _method: &str) {}

    /// A task this worker lost because another worker's conditional write won the race, tagged with
    /// job code, task code and the phase the conflict was detected in.
    fn record_task_stolen(&self, _job_code: &JobCode, _task_code: &TaskCode, _phase: &'static str) {}

    /// A job save retried after an optimistic concurrency conflict, tagged with job code and the
    /// phase that triggered the retry.
    fn record_save_conflict_retry(&self, _job_code: &JobCode, _phase: &'static str) {}
}

/// Sink that records nothing.
///
/// The default of [`JobsManagerBuilder`](crate::JobsManagerBuilder): a pool measures nothing until
/// a sink is supplied.
pub struct NoopMetrics;

impl MetricsSink for NoopMetrics {}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    #[derive(Default)]
    struct CountingSink {
        cache_hits: AtomicUsize,
    }

    impl MetricsSink for CountingSink {
        fn record_cache_hit(&self, _method: &str) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The point of the default bodies: a sink that cares about one measurement does not have to
    /// implement the rest, and the ones it skipped stay callable.
    #[test]
    fn sink_records_only_what_it_overrides() {
        let sink = CountingSink::default();

        sink.record_cache_hit("get_job");
        sink.record_cache_miss("get_job");
        sink.record_storage_operation("GET", "OK", Duration::from_millis(1));

        assert_eq!(sink.cache_hits.load(Ordering::Relaxed), 1);
    }
}
