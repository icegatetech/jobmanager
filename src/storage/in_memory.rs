use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;

use crate::{Job, JobCode, JobMeta, Storage, StorageError, StorageResult};

/// In-memory `Storage` for tests and examples: holds the current iteration of every job it was
/// given, and no history.
///
/// Writes are conditional exactly as the object-store backend's are - a save carrying a version
/// other than the stored one, or creating an iteration that is not the next one, is refused with
/// [`StorageError::ConcurrentModification`] - so a component test sees the same conflicts a worker
/// meets in production. Not intended for production use: state is lost with the process. How many
/// requests a wrapping backend actually issued is counted by `CountingStorage`, not here.
pub struct InMemoryStorage {
    jobs_by_code: RwLock<HashMap<JobCode, Job>>,
    version_counter: AtomicU64,
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
            jobs_by_code: RwLock::new(HashMap::new()),
            version_counter: AtomicU64::new(0),
        }
    }

    /// Whether `job` may replace `stored`, which is what the store holds for its code, if anything.
    ///
    /// A job carrying no version is the first save of an iteration and may only create the
    /// iteration after the stored one - or the very first iteration, when nothing is stored yet.
    /// Any other save is an update of the stored iteration and has to carry its exact version.
    fn is_save_allowed(job: &Job, stored: Option<&Job>) -> bool {
        match (stored, job.version().is_empty()) {
            (None, true) => job.iter_num() == 1,
            (None, false) => false,
            (Some(stored), true) => stored.iter_num() + 1 == job.iter_num(),
            (Some(stored), false) => stored.iter_num() == job.iter_num() && stored.version() == job.version(),
        }
    }
}

#[async_trait::async_trait]
#[allow(private_interfaces)]
impl Storage for InMemoryStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let jobs = self.jobs_by_code.read().await;
        jobs.get(job_code)
            .cloned()
            .ok_or_else(|| StorageError::NotFound("Job is absent in in-memory storage".to_string()))
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let jobs = self.jobs_by_code.read().await;
        match jobs.get(&job_meta.code) {
            Some(job) if job.version() == job_meta.version => Ok(job.clone()),
            Some(_) => Err(StorageError::ConcurrentModification(
                "Version mismatch in in-memory storage".to_string(),
            )),
            None => Err(StorageError::NotFound("Job is absent in in-memory storage".to_string())),
        }
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let jobs = self.jobs_by_code.read().await;
        jobs.get(job_code)
            .map(|job| JobMeta {
                code: job.code().clone(),
                iter_num: job.iter_num(),
                version: job.version().to_string(),
            })
            .ok_or_else(|| StorageError::NotFound("Job is absent in in-memory storage".to_string()))
    }

    /// Refuses a save that would overwrite state written since `job` was read; see
    /// [`InMemoryStorage`] for which saves are allowed. The version is only handed to `job` once
    /// the store accepted it, so a refused save leaves the caller holding the version it read.
    // The guard is held from the check to the insert on purpose: releasing it earlier is what would
    // let two savers pass the same check and only one of them survive.
    #[allow(clippy::significant_drop_tightening)]
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        let mut jobs = self.jobs_by_code.write().await;
        if !Self::is_save_allowed(job, jobs.get(job.code())) {
            return Err(StorageError::ConcurrentModification(format!(
                "job '{}' iteration {} was modified in in-memory storage",
                job.code(),
                job.iter_num()
            )));
        }

        let next_version = self.version_counter.fetch_add(1, Ordering::SeqCst) + 1;
        job.update_version(format!("{next_version}"));
        jobs.insert(job.code().clone(), job.clone());
        Ok(())
    }

    /// Always empty: only the current iteration of a job is held, so no iteration is ever outdated.
    async fn list_job_outdated_iterations(
        &self,
        _job_code: &JobCode,
        _retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        Ok(Vec::new())
    }

    /// No-op: there is no iteration history to delete from.
    async fn delete_job_iterations(
        &self,
        _job_code: &JobCode,
        _iter_nums: &[u64],
        cancel_token: &CancellationToken,
    ) -> StorageResult<()> {
        if cancel_token.is_cancelled() {
            return Err(StorageError::Cancelled);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use uuid::Uuid;

    use super::*;
    use crate::{JobStatus, TaskLimits};

    /// A shutdown reaches a worker in the middle of its storage call, and every method has to
    /// answer it the same way the object-store backend does - otherwise a component test would see
    /// a cancellation the production backend turns into an error.
    #[tokio::test]
    async fn every_method_refuses_a_cancelled_token() -> Result<(), Box<dyn std::error::Error>> {
        let storage = InMemoryStorage::new();
        let job_code = JobCode::new("cancelled_job");
        // Restored rather than built from a `JobDefinition`: a definition carries executors, and a
        // backend must not know that workers exist.
        let mut job = Job::restore(
            Uuid::from_u128(1),
            job_code.clone(),
            String::new(),
            1,
            JobStatus::Started,
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
        );
        let job_meta = JobMeta {
            code: job_code.clone(),
            iter_num: job.iter_num(),
            version: job.version().to_string(),
        };

        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let refusals = [
            storage.get_job(&job_code, &cancel_token).await.err(),
            storage.get_job_by_meta(&job_meta, &cancel_token).await.err(),
            storage.find_job_meta(&job_code, &cancel_token).await.err(),
            storage.save_job(&mut job, &cancel_token).await.err(),
            storage.list_job_outdated_iterations(&job_code, 1, &cancel_token).await.err(),
            storage.delete_job_iterations(&job_code, &[1], &cancel_token).await.err(),
        ];
        for refusal in refusals {
            let error = refusal.ok_or("a cancelled token must be refused")?;
            assert!(matches!(error, StorageError::Cancelled), "got: {error}");
        }

        Ok(())
    }
}
