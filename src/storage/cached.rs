use std::sync::Arc;

use async_trait::async_trait;
use dashmap::DashMap;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{Job, JobCode, JobMeta, MetricsSink, Storage, StorageError, StorageResult};

// Internal structure to hold the job state
#[derive(Clone)]
struct CachedJob {
    job: Option<Job>,
    // TODO(med): use TTL or LRU cache to avoid limitless memory growth
}

impl CachedJob {
    const fn empty() -> Self {
        Self { job: None }
    }

    fn replace_job(&mut self, job: Job) {
        self.job = Some(job);
    }

    fn forget_job(&mut self) {
        self.job = None;
    }

    /// Whether the entry holds exactly the state `meta` describes.
    ///
    /// The iteration and the version are compared as a pair: while a conditional read was in
    /// flight the entry could have moved not only to another version of the same iteration but to
    /// the next iteration entirely.
    fn is_unchanged_since(&self, meta: &JobMeta) -> bool {
        self.job
            .as_ref()
            .is_some_and(|job| job.iter_num() == meta.iter_num && job.version() == meta.version)
    }
}

/// Caching wrapper around a storage backend.
pub struct CachedStorage {
    inner: Arc<dyn Storage>,
    cache: DashMap<JobCode, Arc<Mutex<CachedJob>>>,
    metrics: Arc<dyn MetricsSink>,
}

impl CachedStorage {
    /// Wraps `inner` with a per-job-code cache, starting empty.
    ///
    /// A cached entry is checked out against `inner` on every read, so a stale one never serves a
    /// job the backend has since moved on from: an unfinished iteration is checked by a conditional
    /// read of the iteration itself, anything else by re-reading the job's metadata.
    pub fn new(inner: Arc<dyn Storage>, metrics: Arc<dyn MetricsSink>) -> Self {
        Self {
            inner,
            cache: DashMap::new(),
            metrics,
        }
    }

    // update_cache_if_newer updates cache only if passed job iteration is newer or equal to current in
    // cache
    fn update_cache_if_newer(storage_job: &Job, cached_job: &mut CachedJob) {
        if let Some(ref current_job) = cached_job.job {
            if storage_job.iter_num() >= current_job.iter_num() {
                cached_job.replace_job(storage_job.clone());
            }
        } else {
            cached_job.replace_job(storage_job.clone());
        }
    }

    fn record_cache_hit(&self, method: &str) {
        self.metrics.record_cache_hit(method);
    }

    fn record_cache_miss(&self, method: &str) {
        self.metrics.record_cache_miss(method);
    }
}

#[async_trait]
#[allow(clippy::significant_drop_tightening)]
impl Storage for CachedStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let cache_entry = self
            .cache
            .entry(job_code.clone())
            .or_insert_with(|| Arc::new(Mutex::new(CachedJob::empty())))
            .clone();

        // The snapshot is taken and the entry released before the request. Holding it across the
        // read would queue every worker of the pool behind one round trip, on every poll.
        let unfinished_cached_job = {
            let cached_job = cache_entry.lock().await;
            cached_job
                .job
                .as_ref()
                // A conditional read asks about one iteration, and a finished one never changes
                // again. Asking about it is pointless: it answers "unmoved" forever while the job
                // has already moved on. The iteration that replaced it is found by the cold read
                // below.
                .filter(|job| !job.is_processed())
                // TODO(med): move the cache and `Storage::get_job` to `Arc<Job>`, so the hot path
                // copies no job metadata at all.
                .cloned()
        };

        if let Some(cached) = unfinished_cached_job {
            let meta = JobMeta {
                code: cached.code().clone(),
                iter_num: cached.iter_num(),
                version: cached.version().to_string(),
            };
            match self.inner.get_changed_job(&meta, cancel_token).await {
                Ok(None) => {
                    self.record_cache_hit("get_job");
                    debug!(
                        "Get job '{}' from cache (id: {}, iter: {}, version: {})",
                        cached.code(),
                        cached.id(),
                        cached.iter_num(),
                        cached.version()
                    );
                    return Ok(cached);
                }
                Ok(Some(job)) => {
                    self.record_cache_miss("get_job");
                    let mut cached_job = cache_entry.lock().await;
                    // A read that lost the race to a save is dropped rather than written: the entry
                    // already holds another state, and which of the two is newer is not something
                    // versions answer. The caller still gets what it read, and writes conditionally
                    // anyway.
                    if cached_job.is_unchanged_since(&meta) {
                        cached_job.replace_job(job.clone());
                    }
                    return Ok(job);
                }
                // The iteration is gone from the store, so its number is no longer current: fall
                // through to the cold read that discovers the current one.
                Err(StorageError::NotFound(_)) => {}
                Err(e) => return Err(e),
            }
        }

        // Cold read: discover the current iteration, then fetch it.
        let mut meta = self.inner.find_job_meta(job_code, cancel_token).await?;
        let mut cached_job = cache_entry.lock().await;

        if let Some(ref job) = cached_job.job
            && job.iter_num() == meta.iter_num
            && job.version() == meta.version
        {
            self.record_cache_hit("get_job");
            debug!(
                "Get job '{}' from cache (id: {}, iter: {}, version: {}, tasks: {:?})",
                job.code(),
                job.id(),
                job.iter_num(),
                job.version(),
                job.tasks_as_string()
            );
            return Ok(job.clone());
        }

        self.record_cache_miss("get_job");

        // Keep job lock while fetching to avoid get/save races.
        // TODO(med): fetch outside the lock and write the result under it against
        // `CachedJob::is_unchanged_since`, as the hot path does - the entry held across the
        // requests below queues every other worker of the pool behind them.
        let mut job = None;
        for _ in 0..2 {
            match self.inner.get_job_by_meta(&meta, cancel_token).await {
                Ok(found) => {
                    job = Some(found);
                    break;
                }
                Err(e) if e.is_conflict() => {
                    debug!("Retry find job {job_code} meta in storage");
                    meta = self.inner.find_job_meta(job_code, cancel_token).await?;
                }
                Err(e) => return Err(e),
            }
        }

        let job = job.ok_or_else(|| {
            StorageError::ConcurrentModification("Failed to read consistent job state after retries".to_string())
        })?;

        cached_job.replace_job(job.clone());
        Ok(job)
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn get_job_by_meta(&self, meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let cached_job = self
            .cache
            .entry(meta.code.clone())
            .or_insert_with(|| Arc::new(Mutex::new(CachedJob::empty())))
            .clone();

        let mut cached_job = cached_job.lock().await;

        if let Some(ref job) = cached_job.job
            && job.iter_num() == meta.iter_num
            && job.version() == meta.version
        {
            self.record_cache_hit("get_job_by_meta");
            debug!(
                "Get job '{}' from cache (id: {}, iter: {}, version: {}, tasks: {:?})",
                job.code(),
                job.id(),
                job.iter_num(),
                job.version(),
                job.tasks_as_string()
            );
            return Ok(job.clone());
        }
        self.record_cache_miss("get_job_by_meta");

        let job = match self.inner.get_job_by_meta(meta, cancel_token).await {
            Ok(job) => job,
            Err(e) if e.is_conflict() => {
                // Invalidate cache so next read fetches fresh state
                cached_job.forget_job();
                return Err(e);
            }
            Err(e) => return Err(e),
        };

        Self::update_cache_if_newer(&job, &mut cached_job);
        Ok(job)
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    /// Delegates without touching the cache: the cache is what *calls* this, in [`Self::get_job`],
    /// rather than what answers it.
    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        self.inner.get_changed_job(job_meta, cancel_token).await
    }

    #[allow(clippy::significant_drop_tightening)]
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let cached_job = self
            .cache
            .entry(job.code().clone())
            .or_insert_with(|| Arc::new(Mutex::new(CachedJob::empty())))
            .clone();

        let mut cached_job = cached_job.lock().await;

        match self.inner.save_job(job, cancel_token).await {
            Ok(()) => {
                // The copy the worker just wrote, cloned as it stands: the cache is this pool's
                // record of its own write, and what a backend keeps of that state is the backend's
                // own business. A clone shares the tasks, so caching a save stays cheap.
                cached_job.replace_job(job.clone());
                debug!(
                    "Job '{}' saved to storage and cache (id: {}, iter: {}, version: {}, tasks: {:?})",
                    job.code(),
                    job.id(),
                    job.iter_num(),
                    job.version(),
                    job.tasks_as_string()
                );
                Ok(())
            }
            Err(e) if e.is_conflict() => {
                // Invalidate cache so next get_job fetches fresh state from S3
                cached_job.forget_job();
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        self.inner
            .list_job_outdated_iterations(job_code, retention_boundary, cancel_token)
            .await
    }

    /// Delegates to the wrapped backend and leaves the cache alone: it only ever holds a job's
    /// current iteration, which is never among the deleted ones.
    async fn delete_job_iterations(
        &self,
        job_code: &JobCode,
        iter_nums: &[u64],
        cancel_token: &CancellationToken,
    ) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::{JobStatus, TaskLimits};

    /// A job state carrying the iteration and version the comparison under test reads, and nothing
    /// else it looks at.
    fn build_job(iter_num: u64, version: &str) -> Job {
        Job::restore(
            Uuid::from_u128(1),
            JobCode::new("job"),
            version.to_string(),
            iter_num,
            JobStatus::Running,
            Vec::new(),
            Uuid::from_u128(2),
            Utc::now(),
            None,
            None,
            None,
            HashMap::new(),
            None,
            None,
            TaskLimits::default(),
        )
    }

    fn build_meta(iter_num: u64, version: &str) -> JobMeta {
        JobMeta {
            code: JobCode::new("job"),
            iter_num,
            version: version.to_string(),
        }
    }

    fn build_entry(job: Option<Job>) -> CachedJob {
        CachedJob { job }
    }

    #[test]
    fn an_entry_still_holding_the_read_state_is_unchanged() {
        let entry = build_entry(Some(build_job(1, "first")));

        assert!(entry.is_unchanged_since(&build_meta(1, "first")));
    }

    #[test]
    fn an_entry_moved_to_another_version_of_the_iteration_is_changed() {
        let entry = build_entry(Some(build_job(1, "second")));

        assert!(!entry.is_unchanged_since(&build_meta(1, "first")));
    }

    /// The case that makes the comparison a pair rather than a version alone: a store hands out
    /// versions per object, so the next iteration can carry the version this one was read at.
    #[test]
    fn an_entry_moved_to_the_next_iteration_is_changed() {
        let entry = build_entry(Some(build_job(2, "first")));

        assert!(!entry.is_unchanged_since(&build_meta(1, "first")));
    }

    /// A conflicting save empties the entry, and a read that raced it must not fill it back in.
    #[test]
    fn an_emptied_entry_is_changed() {
        let entry = build_entry(None);

        assert!(!entry.is_unchanged_since(&build_meta(1, "first")));
    }
}
