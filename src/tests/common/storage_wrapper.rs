use std::collections::HashMap;
use std::sync::{
    Arc, OnceLock, Weak,
    atomic::{AtomicBool, AtomicU64, Ordering},
};

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::Barrier;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    Job, JobCode, TaskCode, TaskPickup, TaskStatus,
    storage::{JobMeta, Storage, StorageError, StorageResult},
};

/// `CountingStorage` wraps Storage and tracks `save_job` calls.
pub struct CountingStorage {
    inner: Arc<dyn Storage>,
    put_attempts: AtomicU64,
    put_successes: AtomicU64,
    list_and_get_successes: AtomicU64,
    find_meta_calls: AtomicU64,
    conditional_read_calls: AtomicU64,
    unchanged_reads: AtomicU64,
    get_by_meta_calls: AtomicU64,
    list_outdated_calls: AtomicU64,
    delete_iterations_calls: AtomicU64,
    deleted_iterations_total: AtomicU64,
    read_calls_by_job_code: parking_lot::Mutex<HashMap<JobCode, u64>>,
}

impl CountingStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            put_attempts: AtomicU64::new(0),
            put_successes: AtomicU64::new(0),
            list_and_get_successes: AtomicU64::new(0),
            find_meta_calls: AtomicU64::new(0),
            conditional_read_calls: AtomicU64::new(0),
            unchanged_reads: AtomicU64::new(0),
            get_by_meta_calls: AtomicU64::new(0),
            list_outdated_calls: AtomicU64::new(0),
            delete_iterations_calls: AtomicU64::new(0),
            deleted_iterations_total: AtomicU64::new(0),
            read_calls_by_job_code: parking_lot::Mutex::new(HashMap::new()),
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

    /// Counts `get_changed_job` calls, which cost a `GET` on an object store.
    pub fn conditional_read_calls(&self) -> u64 {
        self.conditional_read_calls.load(Ordering::SeqCst)
    }

    /// Counts the conditional reads that found the state unmoved - the ones a `304` answers.
    pub fn unchanged_reads(&self) -> u64 {
        self.unchanged_reads.load(Ordering::SeqCst)
    }

    /// Counts `get_job_by_meta` calls, which cost a `GET` on an object store.
    pub fn get_by_meta_calls(&self) -> u64 {
        self.get_by_meta_calls.load(Ordering::SeqCst)
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

    /// How many times the store was read for `job_code` - a listing, a conditional read or a fetch
    /// by meta, which are the three ways a pass reaches it.
    ///
    /// One pool holds one wrapper, so the totals above cannot tell a job's own requests from its
    /// neighbour's; this is what a test asserting about one job of several counts on.
    pub fn read_calls_of(&self, job_code: &JobCode) -> u64 {
        self.read_calls_by_job_code.lock().get(job_code).copied().unwrap_or(0)
    }

    fn record_read_of(&self, job_code: &JobCode) {
        *self.read_calls_by_job_code.lock().entry(job_code.clone()).or_insert(0) += 1;
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
        self.get_by_meta_calls.fetch_add(1, Ordering::SeqCst);
        self.record_read_of(&job_meta.code);
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.find_meta_calls.fetch_add(1, Ordering::SeqCst);
        self.record_read_of(job_code);
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        self.conditional_read_calls.fetch_add(1, Ordering::SeqCst);
        self.record_read_of(&job_meta.code);
        let result = self.inner.get_changed_job(job_meta, cancel_token).await;
        if matches!(result, Ok(None)) {
            self.unchanged_reads.fetch_add(1, Ordering::SeqCst);
        }
        result
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

/// Which save of an existing iteration [`ContendingStorage`] gets its own write in ahead of.
pub enum ContendedSave {
    /// The first save of an existing iteration, whatever it carries.
    First,
    /// The first save carrying a task of this code in this status - the save an execution's own
    /// result rides on.
    OfTask { code: TaskCode, status: TaskStatus },
}

/// Storage double that makes exactly one save of an existing iteration lose a race: another
/// worker's write lands first, so the save that follows finds the version it read already moved.
///
/// The interference leaves `processing_by_worker` untouched, so the conflict resolves as a merge
/// and a retry, never as a steal. What it does move is the moment the next iteration is due - the
/// one thing a rival can change without taking a task over, and the reason the write really is a
/// different one: a store that versions by content answers an identical write with the version it
/// already had, and the caller's save would then not be refused at all.
pub struct ContendingStorage {
    inner: Arc<dyn Storage>,
    rival_store: Option<Arc<dyn Storage>>,
    contended_save: ContendedSave,
    is_interference_pending: AtomicBool,
    interferences: AtomicU64,
}

impl ContendingStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            rival_store: None,
            contended_save: ContendedSave::First,
            is_interference_pending: AtomicBool::new(true),
            interferences: AtomicU64::new(0),
        }
    }

    /// Holds the interference back until the save `contended_save` names, instead of contending
    /// with the first save of any kind.
    pub fn with_contended_save(mut self, contended_save: ContendedSave) -> Self {
        self.contended_save = contended_save;
        self
    }

    /// Sends the rival's read and write through `rival_store` instead of the wrapped one.
    ///
    /// A test that measures what its caller is billed for gives the rival a store of its own, so
    /// the requests of the double do not land in the caller's count.
    pub fn with_rival_store(mut self, rival_store: Arc<dyn Storage>) -> Self {
        self.rival_store = Some(rival_store);
        self
    }

    /// How many times the double really got a write in ahead of its caller, so a test can prove its
    /// fixture reached the conflict it asserts on.
    pub fn interferences(&self) -> u64 {
        self.interferences.load(Ordering::SeqCst)
    }

    /// Store the rival acts through: the one named by [`Self::with_rival_store`], or the wrapped
    /// one when no separate store was given.
    fn rival_store(&self) -> &Arc<dyn Storage> {
        self.rival_store.as_ref().unwrap_or(&self.inner)
    }

    /// Whether this is the save the double was told to contend with.
    fn is_contended_save(&self, job: &Job) -> bool {
        // A save carrying no version creates an iteration and has no version to invalidate.
        if job.version().is_empty() {
            return false;
        }
        match &self.contended_save {
            ContendedSave::First => true,
            ContendedSave::OfTask { code, status } => {
                job.tasks_as_iter().any(|task| task.code() == code && task.status() == status)
            }
        }
    }
}

#[async_trait]
impl Storage for ContendingStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job(job_code, cancel_token).await
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        self.inner.get_changed_job(job_meta, cancel_token).await
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if self.is_contended_save(job) && self.is_interference_pending.swap(false, Ordering::SeqCst) {
            let rival_store = self.rival_store();
            let mut stored = rival_store.get_job(job.code(), cancel_token).await?;
            stored.set_next_start_at(Utc::now());
            rival_store.save_job(&mut stored, cancel_token).await?;
            self.interferences.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.save_job(job, cancel_token).await
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
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
        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}

/// Storage double that lets a rival worker take and finish a sibling task, exactly once, right
/// before the save carrying the caller's own result of a task with `processed_task_code`.
///
/// The interference hands the sibling to another owner, so the caller's save is refused and the
/// merge that follows meets a task the caller created earlier and no longer owns - the state
/// `ContendingStorage` deliberately cannot produce, since its interference re-saves what is
/// already stored.
pub struct SiblingCompletingStorage {
    inner: Arc<dyn Storage>,
    rival_worker_id: Uuid,
    processed_task_code: TaskCode,
    is_interference_pending: AtomicBool,
    completed_sibling_id: OnceLock<Uuid>,
    interference_failure: OnceLock<String>,
    interferences: AtomicU64,
}

impl SiblingCompletingStorage {
    pub fn new(inner: Arc<dyn Storage>, rival_worker_id: Uuid, processed_task_code: TaskCode) -> Self {
        Self {
            inner,
            rival_worker_id,
            processed_task_code,
            is_interference_pending: AtomicBool::new(true),
            completed_sibling_id: OnceLock::new(),
            interference_failure: OnceLock::new(),
            interferences: AtomicU64::new(0),
        }
    }

    /// How many times the rival really finished a sibling ahead of its caller, so a test can prove
    /// its fixture reached the conflict it asserts on.
    pub fn interferences(&self) -> u64 {
        self.interferences.load(Ordering::SeqCst)
    }

    /// Identifier of the sibling the rival finished, once it has one.
    pub fn completed_sibling_id(&self) -> Option<Uuid> {
        self.completed_sibling_id.get().copied()
    }

    /// Why the interference could not happen, if it could not. A test asserts on this before
    /// anything else: a fixture that broke otherwise shows up only as a wait that ran out.
    pub fn interference_failure(&self) -> Option<&str> {
        self.interference_failure.get().map(String::as_str)
    }

    /// Records a broken expectation of the fixture and turns it into an error the worker logs,
    /// rather than a panic inside the worker's own task, which the pool swallows.
    fn refuse_interference(&self, reason: String) -> StorageError {
        let _ = self.interference_failure.set(reason.clone());
        StorageError::Other(reason)
    }

    /// Whether this save carries a finished task of the awaited code, which is what marks it as the
    /// save of a result rather than of a pickup.
    fn has_processed_task(&self, job: &Job) -> bool {
        job.tasks_as_iter()
            .any(|task| task.code() == &self.processed_task_code && task.is_completed())
    }

    /// Takes and finishes a sibling through the production API, then stores it, which moves the
    /// version the caller read.
    async fn complete_sibling(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<()> {
        let mut stored = self.inner.get_job(job_code, cancel_token).await?;
        let sibling_id = match stored.pick_task_to_execute(&self.rival_worker_id) {
            Ok(TaskPickup::Ready(task_id)) => task_id,
            Ok(pickup) => {
                return Err(self.refuse_interference(format!(
                    "the fixture must leave the rival a sibling to finish, got {pickup:?}"
                )));
            }
            Err(e) => {
                return Err(
                    self.refuse_interference(format!("the rival must be able to pick in the stored iteration: {e}"))
                );
            }
        };

        if let Err(e) = stored.start_task(&sibling_id, self.rival_worker_id) {
            return Err(self.refuse_interference(format!("a pickable sibling must start: {e}")));
        }
        if let Err(e) = stored.complete_task(&sibling_id, b"shifted by the rival".to_vec()) {
            return Err(self.refuse_interference(format!("a started sibling must complete: {e}")));
        }
        self.inner.save_job(&mut stored, cancel_token).await?;

        let _ = self.completed_sibling_id.set(sibling_id);
        self.interferences.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Storage for SiblingCompletingStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job(job_code, cancel_token).await
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        self.inner.get_changed_job(job_meta, cancel_token).await
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        if self.has_processed_task(job) && self.is_interference_pending.swap(false, Ordering::SeqCst) {
            self.complete_sibling(job.code(), cancel_token).await?;
        }
        self.inner.save_job(job, cancel_token).await
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
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
        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}

/// Storage double that lands a later save through the read cache while a conditional read of the
/// same job is in flight - the one order in which a read comes back carrying a state the cache has
/// already moved past.
///
/// The interference re-saves what is already stored, which leaves `processing_by_worker` untouched,
/// and happens once, only for a read the backend answers as changed - the read whose result the
/// cache would otherwise write.
pub struct CacheRacingStorage {
    inner: Arc<dyn Storage>,
    cache: OnceLock<Weak<dyn Storage>>,
    is_interference_pending: AtomicBool,
    interferences: AtomicU64,
}

impl CacheRacingStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            cache: OnceLock::new(),
            is_interference_pending: AtomicBool::new(true),
            interferences: AtomicU64::new(0),
        }
    }

    /// Names the cache the interference saves through. It is the cache wrapping this double, so it
    /// exists only after the double does and cannot be handed to the constructor.
    pub fn attach_cache(&self, cache: &Arc<dyn Storage>) {
        let _ = self.cache.set(Arc::downgrade(cache));
    }

    /// How many times the double really got a save in ahead of a read's own write, so a test can
    /// prove its fixture reached the race it asserts on.
    pub fn interferences(&self) -> u64 {
        self.interferences.load(Ordering::SeqCst)
    }

    async fn save_through_cache(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<()> {
        let cache = self
            .cache
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| StorageError::Other("the double must be given the cache wrapping it".to_string()))?;

        let mut stored = self.inner.get_job(job_code, cancel_token).await?;
        cache.save_job(&mut stored, cancel_token).await
    }
}

#[async_trait]
impl Storage for CacheRacingStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job(job_code, cancel_token).await
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        let read = self.inner.get_changed_job(job_meta, cancel_token).await?;

        if read.is_some() && self.is_interference_pending.swap(false, Ordering::SeqCst) {
            self.save_through_cache(&job_meta.code, cancel_token).await?;
            self.interferences.fetch_add(1, Ordering::SeqCst);
        }

        Ok(read)
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        self.inner.save_job(job, cancel_token).await
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
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
        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}

/// Storage double that holds every conditional read until as many of them are in flight as it was
/// built for, so a caller that lets its readers queue behind one another never gets an answer.
pub struct RendezvousReadStorage {
    inner: Arc<dyn Storage>,
    rendezvous: Barrier,
}

impl RendezvousReadStorage {
    pub fn new(inner: Arc<dyn Storage>, awaited_readers: usize) -> Self {
        Self {
            inner,
            rendezvous: Barrier::new(awaited_readers),
        }
    }
}

#[async_trait]
impl Storage for RendezvousReadStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job(job_code, cancel_token).await
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        self.rendezvous.wait().await;
        self.inner.get_changed_job(job_meta, cancel_token).await
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        self.inner.save_job(job, cancel_token).await
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
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
        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}

/// Storage double standing in for a backend that cannot answer a conditional read at all: the
/// store is unwell rather than moved on, so the read fails with a retryable error instead of
/// reporting anything about where the job stands.
pub struct ConditionalReadFailingStorage {
    inner: Arc<dyn Storage>,
}

impl ConditionalReadFailingStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Storage for ConditionalReadFailingStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job(job_code, cancel_token).await
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn get_changed_job(
        &self,
        _job_meta: &JobMeta,
        _cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        Err(StorageError::ServiceUnavailable)
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        self.inner.save_job(job, cancel_token).await
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
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
        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}

/// Storage double that refuses every save with a failure that is neither a conflict nor worth
/// retrying - the one kind a worker can do nothing about, so the pass that met it ends having
/// written nothing.
pub struct SaveRefusingStorage {
    inner: Arc<dyn Storage>,
    refused_saves: AtomicU64,
}

impl SaveRefusingStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            refused_saves: AtomicU64::new(0),
        }
    }

    /// How many saves the double really refused, so a test can prove its fixture reached the write
    /// it asserts on.
    pub fn refused_saves(&self) -> u64 {
        self.refused_saves.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl Storage for SaveRefusingStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job(job_code, cancel_token).await
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        self.inner.get_changed_job(job_meta, cancel_token).await
    }

    async fn save_job(&self, _job: &mut Job, _cancel_token: &CancellationToken) -> StorageResult<()> {
        self.refused_saves.fetch_add(1, Ordering::SeqCst);
        Err(StorageError::Other("the store refuses this save".to_string()))
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
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
        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}

/// Storage double standing in for a backend that ignores `If-None-Match`: a conditional read of an
/// unmoved state comes back carrying the state itself rather than nothing. What is lost is the
/// saving, not the correctness - which is what a test using this double is for.
pub struct UnconditionalReadStorage {
    inner: Arc<dyn Storage>,
}

impl UnconditionalReadStorage {
    pub fn new(inner: Arc<dyn Storage>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl Storage for UnconditionalReadStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job(job_code, cancel_token).await
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.inner.find_job_meta(job_code, cancel_token).await
    }

    /// Answers about the iteration that was asked for and no other, exactly as a backend must: a
    /// store that ignores the precondition still keys its objects by iteration.
    async fn get_changed_job(
        &self,
        job_meta: &JobMeta,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<Job>> {
        let job = self.inner.get_job(&job_meta.code, cancel_token).await?;
        if job.iter_num() != job_meta.iter_num {
            return Err(StorageError::NotFound(
                "the named iteration is no longer the current one".to_string(),
            ));
        }

        Ok(Some(job))
    }

    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()> {
        self.inner.save_job(job, cancel_token).await
    }

    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>> {
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
        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}
