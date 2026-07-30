use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::Duration as ChronoDuration;
use parking_lot::Mutex;
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

use super::common::manager_env::ManagerEnv;
use crate::execution::job_cleaner::JobIterationStarted;
use crate::storage::in_memory::InMemoryStorage;
use crate::{
    CachedStorage, InternalError, Job, JobCleaner, JobCleanerConfig, JobCode, JobDefinition, JobRegistry,
    JobsManagerConfig, Metrics, Storage, StorageError, TaskCode, TaskDefinition, TaskExecutorFn, WorkerConfig,
    storage::{JobMeta, StorageResult},
};

/// Bound on every wait in this file: cleanup is asynchronous, so tests poll for its effect instead
/// of assuming a delay is enough.
const CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

/// Storage double that scripts what the cleaner reads and records what it deletes.
///
/// A new double is needed because [`InMemoryStorage`] keeps no iteration history, so neither the
/// start-up reconciliation nor a delete can be observed on it. Reads are scripted only when a test
/// sets them; otherwise they reach `inner`, which lets the same double sit under a real
/// `JobsManager`.
struct RecordingCleanupStorage {
    inner: Arc<dyn Storage>,
    /// Iteration number `find_job_meta` reports per job. `Some` map means it is the whole truth:
    /// a job missing from it is reported as `NotFound`.
    scripted_iter_nums: Option<HashMap<JobCode, u64>>,
    /// Iterations `list_outdated_iterations` reports per job, with the same "whole truth" rule.
    scripted_outdated_iter_nums: Option<HashMap<JobCode, Vec<u64>>>,
    /// Jobs whose `list_outdated_iterations` fails, and with which error.
    list_errors_by_job: HashMap<JobCode, StorageError>,
    /// Number of `find_job_meta` calls, which cost a paid `LIST` on a real backend just like
    /// `list_outdated_iterations` does.
    find_meta_calls: AtomicU64,
    /// Number of `list_outdated_iterations` calls, which cost a paid `LIST` on a real backend.
    list_outdated_calls: AtomicU64,
    /// Error every `delete_iterations` fails with while set.
    delete_error: Mutex<Option<StorageError>>,
    /// Gate that holds every `delete_iterations` until permits are added.
    delete_gate: Option<Arc<Semaphore>>,
    /// Number of `delete_iterations` calls counted *before* the gate, so a test can wait for a
    /// cleaner parked inside a delete that `delete_attempts` will never record.
    delete_entries: AtomicU64,
    delete_attempts: Mutex<Vec<(JobCode, Vec<u64>)>>,
}

impl RecordingCleanupStorage {
    fn new(inner: Arc<dyn Storage>) -> Self {
        Self {
            inner,
            scripted_iter_nums: None,
            scripted_outdated_iter_nums: None,
            list_errors_by_job: HashMap::new(),
            find_meta_calls: AtomicU64::new(0),
            list_outdated_calls: AtomicU64::new(0),
            delete_error: Mutex::new(None),
            delete_gate: None,
            delete_entries: AtomicU64::new(0),
            delete_attempts: Mutex::new(Vec::new()),
        }
    }

    fn with_scripted_iter_nums(mut self, scripted_iter_nums: HashMap<JobCode, u64>) -> Self {
        self.scripted_iter_nums = Some(scripted_iter_nums);
        self
    }

    fn with_scripted_outdated_iter_nums(mut self, scripted_outdated_iter_nums: HashMap<JobCode, Vec<u64>>) -> Self {
        self.scripted_outdated_iter_nums = Some(scripted_outdated_iter_nums);
        self
    }

    fn with_list_error(mut self, job_code: JobCode, error: StorageError) -> Self {
        self.list_errors_by_job.insert(job_code, error);
        self
    }

    fn with_delete_error(self, error: StorageError) -> Self {
        *self.delete_error.lock() = Some(error);
        self
    }

    fn with_blocked_deletes(mut self, gate: Arc<Semaphore>) -> Self {
        self.delete_gate = Some(gate);
        self
    }

    fn clear_delete_error(&self) {
        *self.delete_error.lock() = None;
    }

    fn delete_attempts(&self) -> Vec<(JobCode, Vec<u64>)> {
        self.delete_attempts.lock().clone()
    }

    fn find_meta_calls(&self) -> u64 {
        self.find_meta_calls.load(Ordering::SeqCst)
    }

    fn list_outdated_calls(&self) -> u64 {
        self.list_outdated_calls.load(Ordering::SeqCst)
    }

    fn delete_entries(&self) -> u64 {
        self.delete_entries.load(Ordering::SeqCst)
    }

    async fn wait_for_delete_attempts(
        &self,
        expected_count: usize,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        wait_until(
            || self.delete_attempts.lock().len() >= expected_count,
            timeout,
            &format!("{expected_count} delete attempts"),
        )
        .await
    }
}

#[async_trait]
impl Storage for RecordingCleanupStorage {
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job(job_code, cancel_token).await
    }

    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
        self.inner.get_job_by_meta(job_meta, cancel_token).await
    }

    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
        self.find_meta_calls.fetch_add(1, Ordering::SeqCst);

        let Some(ref scripted_iter_nums) = self.scripted_iter_nums else {
            return self.inner.find_job_meta(job_code, cancel_token).await;
        };

        scripted_iter_nums.get(job_code).map_or_else(
            || Err(StorageError::NotFound(format!("no scripted iteration for {job_code}"))),
            |iter_num| {
                Ok(JobMeta {
                    code: job_code.clone(),
                    iter_num: *iter_num,
                    version: "scripted".to_string(),
                })
            },
        )
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
        self.list_outdated_calls.fetch_add(1, Ordering::SeqCst);

        if let Some(error) = self.list_errors_by_job.get(job_code) {
            return Err(error.clone());
        }

        let Some(ref scripted_outdated_iter_nums) = self.scripted_outdated_iter_nums else {
            return self
                .inner
                .list_job_outdated_iterations(job_code, retention_boundary, cancel_token)
                .await;
        };

        Ok(scripted_outdated_iter_nums.get(job_code).cloned().unwrap_or_default())
    }

    async fn delete_job_iterations(
        &self,
        job_code: &JobCode,
        iter_nums: &[u64],
        cancel_token: &CancellationToken,
    ) -> StorageResult<()> {
        self.delete_entries.fetch_add(1, Ordering::SeqCst);

        if let Some(ref gate) = self.delete_gate {
            let _permit = gate
                .acquire()
                .await
                .map_err(|e| StorageError::Other(format!("delete gate closed: {e}")))?;
        }

        self.delete_attempts.lock().push((job_code.clone(), iter_nums.to_vec()));

        let delete_error = self.delete_error.lock().clone();
        if let Some(error) = delete_error {
            return Err(error);
        }

        self.inner.delete_job_iterations(job_code, iter_nums, cancel_token).await
    }
}

/// Owns the cleaner task under test and stops it on every exit path.
struct CleanerEnv {
    /// `None` once the report channel has been closed on purpose, which is one of the two ways the
    /// cleaner can end.
    sender: Option<mpsc::Sender<JobIterationStarted>>,
    cancel_token: CancellationToken,
    cleaner_task: Option<JoinHandle<Result<(), InternalError>>>,
}

impl CleanerEnv {
    fn start(
        job_registry: Arc<JobRegistry>,
        storage: Arc<dyn Storage>,
        config: &JobCleanerConfig,
        channel_capacity: usize,
    ) -> Self {
        Self::start_with_token(
            job_registry,
            storage,
            config,
            channel_capacity,
            CancellationToken::new(),
        )
    }

    /// Starts the cleaner on a token that is already cancelled, as a pool whose shutdown beat the
    /// cleaner's first step would.
    fn start_with_cancelled_token(
        job_registry: Arc<JobRegistry>,
        storage: Arc<dyn Storage>,
        config: &JobCleanerConfig,
        channel_capacity: usize,
    ) -> Self {
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();
        Self::start_with_token(job_registry, storage, config, channel_capacity, cancel_token)
    }

    fn start_with_token(
        job_registry: Arc<JobRegistry>,
        storage: Arc<dyn Storage>,
        config: &JobCleanerConfig,
        channel_capacity: usize,
        cancel_token: CancellationToken,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let cleaner = JobCleaner::new(job_registry, storage, config);
        let token = cancel_token.clone();

        let cleaner_task = tokio::spawn(async move { cleaner.start(receiver, token).await });

        Self {
            sender: Some(sender),
            cancel_token,
            cleaner_task: Some(cleaner_task),
        }
    }

    async fn report_iteration(&self, job_code: &JobCode, iter_num: u64) -> Result<(), Box<dyn std::error::Error>> {
        self.sender
            .as_ref()
            .ok_or("the report channel is already closed")?
            .send(JobIterationStarted {
                job_code: job_code.clone(),
                iter_num,
            })
            .await?;
        Ok(())
    }

    /// Free slots left in the report channel, or 0 once it is closed.
    fn report_capacity(&self) -> usize {
        self.sender.as_ref().map_or(0, mpsc::Sender::capacity)
    }

    /// Cancels the cleaner and fails the test if it did not stop cleanly within `timeout`.
    async fn stop(&mut self, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
        self.cancel_token.cancel();
        self.wait_for_stop(timeout).await
    }

    /// Closes the report channel and waits for the cleaner to end on its own, leaving the token
    /// uncancelled so nothing but the closed channel can stop it.
    ///
    /// The cleaner ends only once *both* of its phases are done, so this also waits out the whole
    /// start-up reconciliation.
    async fn stop_by_closing_reports(&mut self, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
        self.sender = None;
        self.wait_for_stop(timeout).await
    }

    async fn wait_for_stop(&mut self, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
        let Some(cleaner_task) = self.cleaner_task.take() else {
            return Ok(());
        };
        tokio::time::timeout(timeout, cleaner_task)
            .await??
            .map_err(|e| format!("job cleaner stopped with error: {e}"))?;
        Ok(())
    }
}

impl Drop for CleanerEnv {
    fn drop(&mut self) {
        self.cancel_token.cancel();
        if let Some(cleaner_task) = self.cleaner_task.take() {
            cleaner_task.abort();
        }
    }
}

async fn wait_until<F>(mut condition: F, timeout: Duration, diagnostic: &str) -> Result<(), Box<dyn std::error::Error>>
where
    F: FnMut() -> bool,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition() {
        if tokio::time::Instant::now() > deadline {
            return Err(format!("timeout waiting for {diagnostic}").into());
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

fn build_job_definition(
    job_code: &JobCode,
    iteration_retention: u64,
) -> Result<JobDefinition, Box<dyn std::error::Error>> {
    let executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
        let task_id = *task.id();
        Box::pin(async move { manager.complete_task(&task_id, b"done".to_vec()) })
    });
    let task_def = TaskDefinition::new(TaskCode::new("cleanup_task"), Vec::new(), ChronoDuration::seconds(5))?;
    let mut executors = HashMap::new();
    executors.insert(TaskCode::new("cleanup_task"), executor);

    Ok(JobDefinition::new(job_code.clone(), vec![task_def], executors)?
        .with_iteration_retention(iteration_retention)?)
}

fn build_recording_storage(storage: RecordingCleanupStorage) -> Arc<RecordingCleanupStorage> {
    Arc::new(storage)
}

/// A retry policy short enough that an exhausted cleanup is observable inside a test.
fn cleaner_config(max_attempts: usize) -> JobCleanerConfig {
    JobCleanerConfig {
        retrier_config: crate::RetrierConfig {
            max_attempts,
            rand_delay: Duration::from_millis(1),
            delays: vec![Duration::from_millis(1)],
        },
        ..Default::default()
    }
}

#[tokio::test]
async fn test_reported_iteration_past_retention_deletes_only_boundary_iteration()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("cleanup_boundary_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    let storage = build_recording_storage(RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())));

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );
    cleaner_env.report_iteration(&job_code, 7).await?;

    storage.wait_for_delete_attempts(1, CLEANUP_TIMEOUT).await?;
    assert_eq!(storage.delete_attempts(), vec![(job_code, vec![2])]);

    cleaner_env.stop(CLEANUP_TIMEOUT).await?;
    Ok(())
}

#[tokio::test]
async fn test_reported_iteration_within_retention_deletes_nothing() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("cleanup_within_retention_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    let storage = build_recording_storage(RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())));

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );

    // Reports are handled in order, so once the third one has deleted, the two before it are known
    // to have been handled without deleting anything.
    cleaner_env.report_iteration(&job_code, 4).await?;
    cleaner_env.report_iteration(&job_code, 5).await?;
    cleaner_env.report_iteration(&job_code, 6).await?;

    storage.wait_for_delete_attempts(1, CLEANUP_TIMEOUT).await?;
    assert_eq!(storage.delete_attempts(), vec![(job_code, vec![1])]);

    cleaner_env.stop(CLEANUP_TIMEOUT).await?;
    Ok(())
}

#[tokio::test]
async fn test_reported_iteration_of_unregistered_job_is_skipped_and_next_report_handled()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("cleanup_registered_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    let storage = build_recording_storage(RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())));

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );

    cleaner_env
        .report_iteration(&JobCode::new("job_absent_from_registry"), 30)
        .await?;
    cleaner_env.report_iteration(&job_code, 8).await?;

    storage.wait_for_delete_attempts(1, CLEANUP_TIMEOUT).await?;
    assert_eq!(storage.delete_attempts(), vec![(job_code, vec![3])]);

    cleaner_env.stop(CLEANUP_TIMEOUT).await?;
    Ok(())
}

#[tokio::test]
async fn test_exhausted_delete_retries_drop_report_and_keep_handling_next_report()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let max_attempts = 3;
    let job_code = JobCode::new("cleanup_retry_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new()))
            .with_delete_error(StorageError::ServiceUnavailable),
    );

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &cleaner_config(max_attempts),
        4,
    );

    cleaner_env.report_iteration(&job_code, 7).await?;
    storage.wait_for_delete_attempts(max_attempts, CLEANUP_TIMEOUT).await?;

    let retried_attempts = storage.delete_attempts();
    assert_eq!(
        retried_attempts.len(),
        max_attempts,
        "a retryable delete failure must be retried exactly max_attempts times"
    );
    assert!(
        retried_attempts.iter().all(|attempt| attempt == &(job_code.clone(), vec![2])),
        "every retry must target the same iteration: {retried_attempts:?}"
    );

    storage.clear_delete_error();
    cleaner_env.report_iteration(&job_code, 8).await?;
    storage.wait_for_delete_attempts(max_attempts + 1, CLEANUP_TIMEOUT).await?;
    assert_eq!(
        storage.delete_attempts().last(),
        Some(&(job_code, vec![3])),
        "a spent cleanup must not stop the cleaner from handling later reports"
    );

    cleaner_env.stop(CLEANUP_TIMEOUT).await?;
    Ok(())
}

#[tokio::test]
async fn test_startup_reconciliation_deletes_outdated_tail_of_every_registered_job()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let tail_job = JobCode::new("cleanup_reconcile_tail_job");
    let absent_job = JobCode::new("cleanup_reconcile_absent_job");
    let failing_job = JobCode::new("cleanup_reconcile_failing_job");
    let job_registry = Arc::new(JobRegistry::new(vec![
        build_job_definition(&tail_job, 5)?,
        build_job_definition(&absent_job, 5)?,
        build_job_definition(&failing_job, 5)?,
    ])?);

    let scripted_iter_nums = HashMap::from([(tail_job.clone(), 12), (failing_job.clone(), 20)]);
    let scripted_outdated_iter_nums = HashMap::from([(tail_job.clone(), vec![1, 2, 3, 4, 5, 6, 7])]);
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new()))
            .with_scripted_iter_nums(scripted_iter_nums)
            .with_scripted_outdated_iter_nums(scripted_outdated_iter_nums)
            .with_list_error(failing_job, StorageError::Other("listing is broken".to_string())),
    );

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );

    // The registry is walked in HashMap order, so this also proves the failing job did not abort
    // the walk: the tail job is reconciled whichever of them comes first.
    storage.wait_for_delete_attempts(1, CLEANUP_TIMEOUT).await?;
    assert_eq!(
        storage.delete_attempts(),
        vec![(tail_job, vec![1, 2, 3, 4, 5, 6, 7])],
        "only the job with a scripted tail may be deleted from"
    );

    cleaner_env.stop(CLEANUP_TIMEOUT).await?;
    Ok(())
}

/// A job whose whole history still fits inside its retention window must cost nothing but the
/// `find_job_meta` every start-up already pays.
///
/// Closing the report channel is what makes this deterministic: the cleaner ends only when both of
/// its phases are done, so the counters below are final rather than sampled mid-walk.
#[tokio::test]
async fn test_startup_reconciliation_lists_no_tail_for_job_inside_retention_window()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("cleanup_young_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new()))
            .with_scripted_iter_nums(HashMap::from([(job_code, 3)])),
    );

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );
    cleaner_env.stop_by_closing_reports(CLEANUP_TIMEOUT).await?;

    assert_eq!(
        storage.find_meta_calls(),
        1,
        "the reconciliation must have reached the job at all"
    );
    assert_eq!(
        storage.list_outdated_calls(),
        0,
        "a job with nothing outside its retention window must not cost a LIST"
    );
    assert_eq!(storage.delete_entries(), 0, "nothing may be deleted from such a job");

    Ok(())
}

/// The tail of a job another instance has already trimmed lists as empty, and an empty tail must
/// not turn into a `DELETE` of nothing.
#[tokio::test]
async fn test_startup_reconciliation_deletes_nothing_when_the_tail_is_already_empty()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("cleanup_trimmed_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new()))
            .with_scripted_iter_nums(HashMap::from([(job_code, 12)]))
            .with_scripted_outdated_iter_nums(HashMap::new()),
    );

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );
    cleaner_env.stop_by_closing_reports(CLEANUP_TIMEOUT).await?;

    assert_eq!(
        storage.list_outdated_calls(),
        1,
        "the job is past its retention window, so its tail must have been listed"
    );
    assert_eq!(storage.delete_entries(), 0, "an empty tail must not be deleted");

    Ok(())
}

/// A delete that cannot succeed by being repeated is dropped after a single attempt, so a job with
/// broken credentials cannot hold up the reports queued behind it.
#[tokio::test]
async fn test_non_retryable_delete_error_is_attempted_once_and_next_report_handled()
-> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("cleanup_non_retryable_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new()))
            .with_delete_error(StorageError::Auth("credentials rejected".to_string())),
    );

    // A budget of three attempts: were the error treated as retryable, the first report alone would
    // produce three deletes of iteration 2.
    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &cleaner_config(3),
        4,
    );

    cleaner_env.report_iteration(&job_code, 7).await?;
    storage.wait_for_delete_attempts(1, CLEANUP_TIMEOUT).await?;

    storage.clear_delete_error();
    cleaner_env.report_iteration(&job_code, 8).await?;
    // Reports are handled one at a time, so the second delete proves the first one is over.
    storage.wait_for_delete_attempts(2, CLEANUP_TIMEOUT).await?;

    assert_eq!(
        storage.delete_attempts(),
        vec![(job_code.clone(), vec![2]), (job_code, vec![3])],
        "a non-retryable failure must be attempted exactly once"
    );

    cleaner_env.stop(CLEANUP_TIMEOUT).await?;
    Ok(())
}

/// Cancellation reaches a reconciliation that is parked inside a delete, and the walk stops there
/// instead of moving on to the next job.
#[tokio::test]
async fn test_cancellation_stops_reconciliation_parked_inside_a_delete() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let first_job = JobCode::new("cleanup_parked_first_job");
    let second_job = JobCode::new("cleanup_parked_second_job");
    let job_registry = Arc::new(JobRegistry::new(vec![
        build_job_definition(&first_job, 5)?,
        build_job_definition(&second_job, 5)?,
    ])?);

    // Both jobs have a tail, so whichever the walk takes first parks on the closed gate and the
    // other one is only reachable if cancellation failed to stop the walk.
    let scripted_iter_nums = HashMap::from([(first_job.clone(), 12), (second_job.clone(), 12)]);
    let scripted_outdated_iter_nums = HashMap::from([(first_job, vec![1, 2]), (second_job, vec![1, 2])]);
    let delete_gate = Arc::new(Semaphore::new(0));
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new()))
            .with_scripted_iter_nums(scripted_iter_nums)
            .with_scripted_outdated_iter_nums(scripted_outdated_iter_nums)
            .with_blocked_deletes(Arc::clone(&delete_gate)),
    );

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );

    // `delete_attempts` is recorded behind the gate and stays empty, so the entry counter is the
    // only signal that the cleaner is parked inside the delete.
    wait_until(
        || storage.delete_entries() >= 1,
        CLEANUP_TIMEOUT,
        "the reconciliation to park inside a delete",
    )
    .await?;

    cleaner_env.stop(CLEANUP_TIMEOUT).await?;

    assert_eq!(
        storage.delete_entries(),
        1,
        "the cancelled walk must not go on to the second job"
    );
    assert_eq!(
        storage.find_meta_calls(),
        1,
        "the second job must not even be read after cancellation"
    );
    assert!(
        storage.delete_attempts().is_empty(),
        "the gate must still hold the only delete: {:?}",
        storage.delete_attempts()
    );

    Ok(())
}

/// A pool cancelled between spawning the cleaner and its first step leaves the cleaner with
/// nothing to do - and it must not spend a single request finding that out.
#[tokio::test]
async fn test_cleaner_started_on_a_cancelled_token_sends_no_request() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let first_job = JobCode::new("cleanup_cancelled_first_job");
    let second_job = JobCode::new("cleanup_cancelled_second_job");
    let job_registry = Arc::new(JobRegistry::new(vec![
        build_job_definition(&first_job, 5)?,
        build_job_definition(&second_job, 5)?,
    ])?);

    let scripted_iter_nums = HashMap::from([(first_job.clone(), 12), (second_job.clone(), 12)]);
    let scripted_outdated_iter_nums = HashMap::from([(first_job, vec![1, 2]), (second_job, vec![1, 2])]);
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new()))
            .with_scripted_iter_nums(scripted_iter_nums)
            .with_scripted_outdated_iter_nums(scripted_outdated_iter_nums),
    );

    let mut cleaner_env = CleanerEnv::start_with_cancelled_token(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );
    cleaner_env.stop(CLEANUP_TIMEOUT).await?;

    assert_eq!(storage.find_meta_calls(), 0, "a cancelled walk must read no job");
    assert_eq!(storage.list_outdated_calls(), 0, "a cancelled walk must list no tail");
    assert_eq!(storage.delete_entries(), 0, "a cancelled walk must delete nothing");

    Ok(())
}

/// Every worker holding a clone of the report sender has stopped, which closes the channel: the
/// cleaner must end by itself, without anyone cancelling it.
#[tokio::test]
async fn test_closed_report_channel_stops_the_cleaner() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("cleanup_closed_channel_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    let storage = build_recording_storage(RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())));

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );

    // The handled report proves the cleaner really is waiting on the channel when it is closed,
    // rather than having stopped earlier for another reason.
    cleaner_env.report_iteration(&job_code, 7).await?;
    storage.wait_for_delete_attempts(1, CLEANUP_TIMEOUT).await?;

    cleaner_env.stop_by_closing_reports(CLEANUP_TIMEOUT).await?;
    Ok(())
}

#[tokio::test]
async fn test_cancellation_stops_cleaner_with_reports_still_queued() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let job_code = JobCode::new("cleanup_shutdown_job");
    let job_registry = Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)?])?);
    // Deletes never complete, so the reports behind the first one are guaranteed to still be
    // queued when the cleaner is cancelled.
    let delete_gate = Arc::new(Semaphore::new(0));
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())).with_blocked_deletes(Arc::clone(&delete_gate)),
    );

    let mut cleaner_env = CleanerEnv::start(
        Arc::clone(&job_registry),
        Arc::clone(&storage) as Arc<dyn Storage>,
        &JobCleanerConfig::default(),
        4,
    );

    for iter_num in 7..=10 {
        cleaner_env.report_iteration(&job_code, iter_num).await?;
    }
    wait_until(
        || cleaner_env.report_capacity() < 4,
        CLEANUP_TIMEOUT,
        "reports to be queued",
    )
    .await?;
    assert!(
        storage.delete_attempts().is_empty(),
        "the gate must hold the first delete before cancellation"
    );

    cleaner_env.stop(CLEANUP_TIMEOUT).await?;
    Ok(())
}

#[tokio::test]
async fn test_started_iterations_delete_one_iteration_each_without_listing() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let max_iterations = 7;
    let iteration_retention = 5;
    let job_code = JobCode::new("cleanup_wired_job");
    let job_def = build_job_definition(&job_code, iteration_retention)?.with_max_iterations(max_iterations)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage = build_recording_storage(RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())));

    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: WorkerConfig {
                poll_interval: Duration::from_millis(20),
                poll_interval_randomization: Duration::from_millis(0),
                ..Default::default()
            },
            ..Default::default()
        },
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    manager_env.wait_for_all_jobs_completion(CLEANUP_TIMEOUT).await?;
    // Iterations 6 and 7 are the only ones that push an iteration out of a window of 5.
    storage.wait_for_delete_attempts(2, CLEANUP_TIMEOUT).await?;
    manager_env.stop().await;

    let attempts = storage.delete_attempts();
    assert_eq!(
        attempts,
        vec![(job_code.clone(), vec![1]), (job_code, vec![2])],
        "each started iteration must delete exactly the iteration that left the retention window"
    );
    // The steady-state request budget: one free single-key DELETE per iteration and not a single
    // paid LIST. The start-up reconciliation adds none either - it runs before the first iteration
    // is persisted, and a job absent from storage has no tail to list.
    assert_eq!(
        storage.list_outdated_calls(),
        0,
        "reports must delete by iteration number instead of listing the tail"
    );

    Ok(())
}

#[tokio::test]
async fn test_full_cleanup_channel_drops_reports_and_keeps_worker_iterating() -> Result<(), Box<dyn std::error::Error>>
{
    super::common::init_tracing();

    let max_iterations = 12;
    let iteration_retention = 5;
    let job_code = JobCode::new("cleanup_full_channel_job");
    let job_def = build_job_definition(&job_code, iteration_retention)?.with_max_iterations(max_iterations)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    // One worker and one job give the report channel a capacity of 5 - `JobsManager` sizes it for
    // five passes of every worker over every job - and the closed gate parks the cleaner on its
    // first delete. Iterations 2 to 5 are reported and consumed at once, because a retention window
    // of 5 leaves them nothing to delete; iterations 6 to 12 all leave the window, so seven reports
    // compete for the parked cleaner plus five queued slots and at least one of them is dropped.
    // Exactly how many are dropped depends on whether the cleaner had already taken the report of
    // iteration 6 out of the channel when the next one arrived, which is why the assertions below
    // state the shape of the result rather than its length.
    let delete_gate = Arc::new(Semaphore::new(0));
    let storage = build_recording_storage(
        RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())).with_blocked_deletes(Arc::clone(&delete_gate)),
    );

    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: WorkerConfig {
                poll_interval: Duration::from_millis(20),
                poll_interval_randomization: Duration::from_millis(0),
                ..Default::default()
            },
            ..Default::default()
        },
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    manager_env.wait_for_all_jobs_completion(CLEANUP_TIMEOUT).await?;
    assert!(
        storage.delete_attempts().is_empty(),
        "the gate must still hold the cleaner once the job is done, which is what kept the channel \
         full - not yet that reports were dropped: {:?}",
        storage.delete_attempts()
    );

    // Releasing the gate lets the cleaner drain whatever survived: the report it parked on plus the
    // queued ones. The permit returns to the semaphore when the guard drops, so one is enough for
    // every remaining report. This has to happen before `stop()`, which cancels the cleaner and
    // discards the queue.
    delete_gate.add_permits(1);
    storage.wait_for_delete_attempts(5, CLEANUP_TIMEOUT).await?;
    manager_env.stop().await;

    let job = storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        job.iter_num(),
        max_iterations,
        "a worker must keep iterating while its reports are being dropped"
    );
    // Reports are handled in order and each deletes the single iteration that left the retention
    // window, so the n-th delete targets iteration n. Which of the last reports were dropped is a
    // race between the worker and the parked cleaner; that they are dropped from the end is not.
    let deletions = storage.delete_attempts();
    for (index, deletion) in deletions.iter().enumerate() {
        assert_eq!(
            *deletion,
            (job_code.clone(), vec![u64::try_from(index)? + 1]),
            "delete {index} must target the iteration that left the retention window: {deletions:?}"
        );
    }
    // Seven iterations left the retention window, and a channel of five plus the parked report can
    // carry at most six of them - an unbounded channel would have cleaned up all seven.
    assert!(
        (5..7).contains(&deletions.len()),
        "a full channel must drop at least one report and keep the rest: {deletions:?}"
    );

    Ok(())
}

/// The production wiring puts a `CachedStorage` between the pool and the backend, and the cleaner
/// is handed that same wrapper: its deletes must reach the backend, and the cache it does not
/// invalidate must keep serving the job's current iteration afterwards.
#[tokio::test]
async fn test_cleanup_through_cached_storage_reaches_the_backend() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let max_iterations = 7;
    let job_code = JobCode::new("cleanup_cached_job");
    let job_def = build_job_definition(&job_code, 5)?.with_max_iterations(max_iterations)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let backend = build_recording_storage(RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())));
    let cached_storage: Arc<dyn Storage> = Arc::new(CachedStorage::new(
        Arc::clone(&backend) as Arc<dyn Storage>,
        Metrics::new_disabled(),
    ));

    let mut manager_env = ManagerEnv::new(
        Arc::clone(&cached_storage),
        JobsManagerConfig {
            worker_count: 1,
            worker_config: WorkerConfig {
                poll_interval: Duration::from_millis(20),
                poll_interval_randomization: Duration::from_millis(0),
                ..Default::default()
            },
            ..Default::default()
        },
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    manager_env.wait_for_all_jobs_completion(CLEANUP_TIMEOUT).await?;
    // Iterations 6 and 7 are the only ones that push an iteration out of a window of 5.
    backend.wait_for_delete_attempts(2, CLEANUP_TIMEOUT).await?;
    manager_env.stop().await;

    assert_eq!(
        backend.delete_attempts(),
        vec![(job_code.clone(), vec![1]), (job_code.clone(), vec![2])],
        "a delete must pass through the cache down to the backend"
    );
    let job = cached_storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        job.iter_num(),
        max_iterations,
        "deleting outdated iterations must not disturb the cached current one"
    );

    Ok(())
}

#[tokio::test]
async fn test_disabled_cleaner_deletes_no_iteration() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    let max_iterations = 7;
    let job_code = JobCode::new("cleanup_disabled_job");
    let job_def = build_job_definition(&job_code, 5)?.with_max_iterations(max_iterations)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def.clone()])?);
    let storage = build_recording_storage(RecordingCleanupStorage::new(Arc::new(InMemoryStorage::new())));

    let mut manager_env = ManagerEnv::new(
        Arc::clone(&storage) as Arc<dyn Storage>,
        JobsManagerConfig {
            worker_count: 1,
            worker_config: WorkerConfig {
                poll_interval: Duration::from_millis(20),
                poll_interval_randomization: Duration::from_millis(0),
                ..Default::default()
            },
            cleaner_config: JobCleanerConfig {
                enabled: false,
                ..Default::default()
            },
        },
        Arc::clone(&job_registry),
        vec![job_def],
    )?;

    manager_env.wait_for_all_jobs_completion(CLEANUP_TIMEOUT).await?;
    manager_env.stop().await;

    let job = storage.get_job(&job_code, &CancellationToken::new()).await?;
    assert_eq!(
        job.iter_num(),
        max_iterations,
        "the job must still run all its iterations with cleanup disabled"
    );
    assert!(
        storage.delete_attempts().is_empty(),
        "a disabled cleaner must not delete anything: {:?}",
        storage.delete_attempts()
    );

    Ok(())
}
