use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::{JobCode, JobStatus, MetricsSink, TaskCode, TaskStatus};

/// `MetricsSink` that counts what it was told, split by the final status where the measurement
/// carries one.
///
/// Counting rather than recording the whole call lets a test state its expectation as a number,
/// which is what distinguishes "the measurement was taken once" from "no error came back".
#[derive(Default)]
pub struct CountingMetrics {
    completed_iterations: AtomicU64,
    failed_iterations: AtomicU64,
    completed_tasks: AtomicU64,
    failed_tasks: AtomicU64,
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    save_conflict_retries: AtomicU64,
    stolen_tasks: AtomicU64,
    storage_operations: parking_lot::Mutex<std::collections::HashMap<(String, String), u64>>,
}

impl CountingMetrics {
    /// Iterations reported as `Completed`.
    pub fn completed_iterations(&self) -> u64 {
        self.completed_iterations.load(Ordering::SeqCst)
    }

    /// Iterations reported as `Failed`.
    pub fn failed_iterations(&self) -> u64 {
        self.failed_iterations.load(Ordering::SeqCst)
    }

    /// Tasks reported as `Completed`.
    pub fn completed_tasks(&self) -> u64 {
        self.completed_tasks.load(Ordering::SeqCst)
    }

    /// Tasks reported as `Failed`.
    pub fn failed_tasks(&self) -> u64 {
        self.failed_tasks.load(Ordering::SeqCst)
    }

    /// Reads served from the read cache.
    pub fn cache_hits(&self) -> u64 {
        self.cache_hits.load(Ordering::SeqCst)
    }

    /// Reads that fell through the read cache to the backend.
    pub fn cache_misses(&self) -> u64 {
        self.cache_misses.load(Ordering::SeqCst)
    }

    /// Saves retried after an optimistic concurrency conflict.
    pub fn save_conflict_retries(&self) -> u64 {
        self.save_conflict_retries.load(Ordering::SeqCst)
    }

    /// Tasks lost to another worker's conditional write.
    pub fn stolen_tasks(&self) -> u64 {
        self.stolen_tasks.load(Ordering::SeqCst)
    }

    /// How many storage operations were recorded in total, under whatever operation and status.
    ///
    /// A request is billed by the store whatever it answered, so a test asserting on the number of
    /// them counts every pair rather than the ones it thought to name: a status nobody listed - a
    /// `500`, a `429` - would otherwise disappear from the oracle.
    pub fn storage_operations_total(&self) -> u64 {
        self.storage_operations.lock().values().sum()
    }

    /// How many storage operations were recorded under any pair other than `operation`/`status`.
    ///
    /// Both halves are read under one lock, so a request recorded between two separate reads cannot
    /// show up in the total a test compares and not in the pair it excludes.
    pub fn storage_operations_besides(&self, operation: &str, status: &str) -> u64 {
        let storage_operations = self.storage_operations.lock();
        let excluded = storage_operations
            .get(&(operation.to_string(), status.to_string()))
            .copied()
            .unwrap_or(0);

        storage_operations.values().sum::<u64>() - excluded
    }

    /// How many times `operation` was recorded under `status` - the pair the storage metric is
    /// labelled with.
    pub fn storage_operations(&self, operation: &str, status: &str) -> u64 {
        self.storage_operations
            .lock()
            .get(&(operation.to_string(), status.to_string()))
            .copied()
            .unwrap_or(0)
    }
}

impl MetricsSink for CountingMetrics {
    fn record_job_iteration_complete(&self, _code: &JobCode, status: &JobStatus, _duration: Duration) {
        let counter = match *status {
            JobStatus::Completed => &self.completed_iterations,
            JobStatus::Failed => &self.failed_iterations,
            JobStatus::Started | JobStatus::Running => return,
        };
        counter.fetch_add(1, Ordering::SeqCst);
    }

    fn record_task_processed(
        &self,
        _job_code: &JobCode,
        _task_code: &TaskCode,
        status: &TaskStatus,
        _duration: Duration,
    ) {
        let counter = match *status {
            TaskStatus::Completed => &self.completed_tasks,
            TaskStatus::Failed => &self.failed_tasks,
            TaskStatus::Todo | TaskStatus::Blocked | TaskStatus::Started => return,
        };
        counter.fetch_add(1, Ordering::SeqCst);
    }

    fn record_storage_operation(&self, operation: &str, status: &str, _duration: Duration) {
        *self
            .storage_operations
            .lock()
            .entry((operation.to_string(), status.to_string()))
            .or_insert(0) += 1;
    }

    fn record_cache_hit(&self, _method: &str) {
        self.cache_hits.fetch_add(1, Ordering::SeqCst);
    }

    fn record_cache_miss(&self, _method: &str) {
        self.cache_misses.fetch_add(1, Ordering::SeqCst);
    }

    fn record_task_stolen(&self, _job_code: &JobCode, _task_code: &TaskCode, _phase: &'static str) {
        self.stolen_tasks.fetch_add(1, Ordering::SeqCst);
    }

    fn record_save_conflict_retry(&self, _job_code: &JobCode, _phase: &'static str) {
        self.save_conflict_retries.fetch_add(1, Ordering::SeqCst);
    }
}
