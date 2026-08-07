use std::time::Duration;

use opentelemetry::{
    KeyValue,
    metrics::{Counter, Histogram, Meter},
};

use crate::{JobCode, JobStatus, MetricsSink, TaskCode, TaskStatus};

/// [`MetricsSink`] that records through `OpenTelemetry`.
///
/// Register it with [`JobsManagerBuilder::metrics`](crate::JobsManagerBuilder::metrics). The
/// instrument names are part of the contract with whatever scrapes them, so they are listed on the
/// methods that write them.
pub struct OtelMetrics {
    job_duration: Histogram<f64>,
    task_duration: Histogram<f64>,
    storage_latency: Histogram<f64>,
    cache_hits: Counter<u64>,
    cache_misses: Counter<u64>,
    task_stolen: Counter<u64>,
    save_conflict_retries: Counter<u64>,
}

impl OtelMetrics {
    /// Builds every instrument on `meter`.
    pub fn new(meter: &Meter) -> Self {
        let job_duration = meter
            .f64_histogram("jobmanager_job_duration")
            .with_description("Duration of job execution from start to finish")
            .with_unit("s")
            .build();

        let task_duration = meter
            .f64_histogram("jobmanager_task_duration")
            .with_description("Duration of individual task execution")
            .with_unit("s")
            .build();

        // The instrument name still says `s3` because it is what dashboards and alerts already
        // query; renaming it would break them, unlike renaming the method.
        let storage_latency = meter
            .f64_histogram("jobmanager_storage_s3_latency")
            .with_description("Latency of storage read and write operations")
            .with_unit("s")
            .build();

        let cache_hits = meter
            .u64_counter("jobmanager_storage_cache_hits")
            .with_description("Total number of cache hits")
            .with_unit("1")
            .build();

        let cache_misses = meter
            .u64_counter("jobmanager_storage_cache_misses")
            .with_description("Total number of cache misses")
            .with_unit("1")
            .build();

        let task_stolen = meter
            .u64_counter("jobmanager_task_stolen")
            .with_description("Total number of task steal events due to worker mismatch")
            .with_unit("1")
            .build();

        let save_conflict_retries = meter
            .u64_counter("jobmanager_save_conflict_retry")
            .with_description("Total number of save retries caused by optimistic concurrency conflicts")
            .with_unit("1")
            .build();

        Self {
            job_duration,
            task_duration,
            storage_latency,
            cache_hits,
            cache_misses,
            task_stolen,
            save_conflict_retries,
        }
    }
}

impl MetricsSink for OtelMetrics {
    /// Records `jobmanager_job_duration`.
    fn record_job_iteration_complete(&self, code: &JobCode, status: &JobStatus, duration: Duration) {
        self.job_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("code", code.to_string()),
                KeyValue::new("status", format!("{status:?}")),
            ],
        );
    }

    /// Records `jobmanager_task_duration`.
    fn record_task_processed(&self, job_code: &JobCode, task_code: &TaskCode, status: &TaskStatus, duration: Duration) {
        self.task_duration.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("job_code", job_code.to_string()),
                KeyValue::new("task_code", task_code.to_string()),
                KeyValue::new("status", format!("{status:?}")),
            ],
        );
    }

    /// Records `jobmanager_storage_s3_latency`.
    fn record_storage_operation(&self, operation: &str, status: &str, duration: Duration) {
        self.storage_latency.record(
            duration.as_secs_f64(),
            &[
                KeyValue::new("operation", operation.to_string()),
                KeyValue::new("http_status", status.to_string()),
            ],
        );
    }

    /// Records `jobmanager_storage_cache_hits`.
    fn record_cache_hit(&self, method: &str) {
        self.cache_hits.add(1, &[KeyValue::new("method", method.to_string())]);
    }

    /// Records `jobmanager_storage_cache_misses`.
    fn record_cache_miss(&self, method: &str) {
        self.cache_misses.add(1, &[KeyValue::new("method", method.to_string())]);
    }

    /// Records `jobmanager_task_stolen`.
    fn record_task_stolen(&self, job_code: &JobCode, task_code: &TaskCode, phase: &'static str) {
        self.task_stolen.add(
            1,
            &[
                KeyValue::new("job_code", job_code.to_string()),
                KeyValue::new("task_code", task_code.to_string()),
                KeyValue::new("phase", phase),
            ],
        );
    }

    /// Records `jobmanager_save_conflict_retry`.
    fn record_save_conflict_retry(&self, job_code: &JobCode, phase: &'static str) {
        self.save_conflict_retries.add(
            1,
            &[
                KeyValue::new("job_code", job_code.to_string()),
                KeyValue::new("phase", phase),
            ],
        );
    }
}
