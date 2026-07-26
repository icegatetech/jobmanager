use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::{Job, JobCode, JobMeta, Storage, StorageError, StorageResult};

/// In-memory `Storage` for tests: holds exactly one job, not a collection.
///
/// Counts read calls so tests can assert how often a wrapping `CachedStorage` actually reached
/// the backend. Not intended for production use - state is lost with the process.
pub struct InMemoryStorage {
    job: RwLock<Option<Job>>,
    version_counter: AtomicU64,
    find_meta_calls: AtomicU64,
    get_by_meta_calls: AtomicU64,
}

impl Default for InMemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryStorage {
    /// Creates an empty storage holding no job.
    pub fn new() -> Self {
        Self {
            job: RwLock::new(None),
            version_counter: AtomicU64::new(0),
            find_meta_calls: AtomicU64::new(0),
            get_by_meta_calls: AtomicU64::new(0),
        }
    }

    /// Number of times `save_job` has succeeded so far.
    ///
    /// Each successful save assigns the job's new version from this counter, so the returned
    /// value also equals the current job's version as a number.
    pub fn version(&self) -> u64 {
        self.version_counter.load(Ordering::SeqCst)
    }

    /// Number of `find_job_meta` calls served so far.
    pub fn find_meta_calls(&self) -> u64 {
        self.find_meta_calls.load(Ordering::SeqCst)
    }

    /// Number of `get_job_by_meta` calls served so far.
    pub fn get_by_meta_calls(&self) -> u64 {
        self.get_by_meta_calls.load(Ordering::SeqCst)
    }
}

#[async_trait::async_trait]
#[allow(private_interfaces)]
impl Storage for InMemoryStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let job_guard = self.job.read().await;
        match job_guard.as_ref() {
            Some(job) if job.code() == job_code => Ok(job.clone()),
            _ => Err(StorageError::NotFound("Job is absent in in-memory storage".to_string())),
        }
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        self.get_by_meta_calls.fetch_add(1, Ordering::SeqCst);
        let job_guard = self.job.read().await;
        match job_guard.as_ref() {
            Some(job) if job.code() == &job_meta.code && job.version() == job_meta.version => Ok(job.clone()),
            Some(job) if job.code() == &job_meta.code => Err(StorageError::ConcurrentModification(
                "Version mismatch in in-memory storage".to_string(),
            )),
            Some(_) | None => Err(StorageError::NotFound("Job is absent in in-memory storage".to_string())),
        }
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        self.find_meta_calls.fetch_add(1, Ordering::SeqCst);
        let job_guard = self.job.read().await;
        match job_guard.as_ref() {
            Some(job) if job.code() == job_code => Ok(JobMeta {
                code: job.code().clone(),
                iter_num: job.iter_num(),
                version: job.version().to_string(),
            }),
            _ => Err(StorageError::NotFound("Job is absent in in-memory storage".to_string())),
        }
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let next_version = self.version_counter.fetch_add(1, Ordering::SeqCst) + 1;
        job.update_version(format!("{next_version}"));
        *self.job.write().await = Some(job.clone());
        Ok(())
    }
}
