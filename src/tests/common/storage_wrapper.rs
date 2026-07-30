use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{
    Job, JobCode,
    storage::{JobMeta, Storage, StorageResult},
};

/// `CountingStorage` wraps Storage and tracks `save_job` calls.
pub struct CountingStorage {
    inner: Arc<dyn Storage>,
    put_attempts: AtomicU64,
    put_successes: AtomicU64,
    list_and_get_successes: AtomicU64,
    find_meta_calls: AtomicU64,
    list_outdated_calls: AtomicU64,
    delete_iterations_calls: AtomicU64,
    deleted_iterations_total: AtomicU64,
}

impl CountingStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            put_attempts: AtomicU64::new(0),
            put_successes: AtomicU64::new(0),
            list_and_get_successes: AtomicU64::new(0),
            find_meta_calls: AtomicU64::new(0),
            list_outdated_calls: AtomicU64::new(0),
            delete_iterations_calls: AtomicU64::new(0),
            deleted_iterations_total: AtomicU64::new(0),
        }
    }

    pub fn put_attempts(&self) -> u64 {
        self.put_attempts.load(Ordering::SeqCst)
    }

    pub fn put_successes(&self) -> u64 {
        self.put_successes.load(Ordering::SeqCst)
    }

    pub fn list_and_get_successes(&self) -> u64 {
        self.list_and_get_successes.load(Ordering::SeqCst)
    }

    /// Counts `find_job_meta` calls, which cost a `LIST` on an object store just like
    /// `list_outdated_iterations` does.
    pub fn find_meta_calls(&self) -> u64 {
        self.find_meta_calls.load(Ordering::SeqCst)
    }

    pub fn list_outdated_calls(&self) -> u64 {
        self.list_outdated_calls.load(Ordering::SeqCst)
    }

    pub fn delete_iterations_calls(&self) -> u64 {
        self.delete_iterations_calls.load(Ordering::SeqCst)
    }

    pub fn deleted_iterations_total(&self) -> u64 {
        self.deleted_iterations_total.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Storage for CountingStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        let result = self.inner.get_job(job_code, cancel_token).await;
        if result.is_ok() {
            self.list_and_get_successes.fetch_add(1, Ordering::SeqCst);
        }
        result
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.find_meta_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        self.put_attempts.fetch_add(1, Ordering::SeqCst);
        tracing::info!(
            "CountingStorage::save_job attempt {} - job: {}, iter: {}, version: {}",
            self.put_attempts.load(Ordering::SeqCst),
            job.code(),
            job.iter_num(),
            job.version()
        );
        let result = self.inner.save_job(job, cancel_token).await;
        if result.is_ok() {
            self.put_successes.fetch_add(1, Ordering::SeqCst);
            tracing::info!(
                "CountingStorage::save_job SUCCESS {} - job: {}, iter: {}, version: {}",
                self.put_successes.load(Ordering::SeqCst),
                job.code(),
                job.iter_num(),
                job.version()
            );
        } else {
            tracing::info!(
                "CountingStorage::save_job CONFLICT - job: {}, iter: {}, version: {}",
                job.code(),
                job.iter_num(),
                job.version()
            );
        }
        result
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
        self.list_outdated_calls.fetch_add(1, Ordering::SeqCst);
        self.inner
            .list_job_outdated_iterations(job_code, retention_boundary, cancel_token)
            .await
    }

    async fn delete_job_iterations(
        &self,
        job_code: &JobCode,
        iter_nums: &[u64],
        cancel_token: &CancellationToken,
    ) -> StorageResult<()> {
        self.delete_iterations_calls.fetch_add(1, Ordering::SeqCst);
        let result = self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await;
        if result.is_ok() {
            self.deleted_iterations_total
                .fetch_add(u64::try_from(iter_nums.len()).unwrap_or(u64::MAX), Ordering::SeqCst);
        }
        result
    }
}
