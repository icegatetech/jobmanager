use std::{any::Any, collections::HashMap, future::Future, panic::AssertUnwindSafe, sync::Arc};

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::execution::job_cleaner::JobIterationStarted;
use crate::execution::job_handle::{JobHandleImpl, JobHandleState};
use crate::{
    Error, InternalError, IterationStep, Job, JobCode, JobError, JobHandle, JobRegistry, JobStatus, MetricsSink,
    Retrier, RetrierConfig, RetryStep, Storage, StorageError, TaskCode, TaskContext, TaskOutcome, TaskPickup,
    TaskResult,
};
// TODO(low): implement subscription mechanism for job updates between workers - if worker received/saved job, other workers should update their state to reduce races.
// Can be done via storage wrapper.

/// Polling and retry behavior applied to every worker spawned by a
/// [`JobsManager`](crate::JobsManager), set through
/// [`JobsManagerBuilder::poll_interval`](crate::JobsManagerBuilder::poll_interval),
/// [`poll_jitter`](crate::JobsManagerBuilder::poll_jitter),
/// [`max_poll_interval`](crate::JobsManagerBuilder::max_poll_interval) and
/// [`retrier`](crate::JobsManagerBuilder::retrier).
///
/// Every setting is checked as it is set, so an assembled `WorkerConfig` is always a set of
/// intervals a worker can poll by: there is no separate validation step and no state in between.
#[derive(Clone)]
pub struct WorkerConfig {
    poll_interval: Duration,
    poll_jitter: Duration,
    max_poll_interval: Option<Duration>,
    retrier_config: RetrierConfig,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkerConfig {
    /// How many times above the base interval the backoff ceiling sits when no ceiling was named.
    const DEFAULT_MAX_POLL_INTERVAL_FACTOR: u32 = 10;
    const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(200);
    /// Longest interval a worker accepts, either as its base or as its backoff ceiling.
    // The product spells the year out in the units the bound is stated in; the `from_hours(8760)`
    // the lint asks for names the same value in a number nobody reads as a year.
    #[allow(clippy::duration_suboptimal_units)]
    const LONGEST_ACCEPTED_POLL_INTERVAL: Duration = Duration::from_secs(365 * 24 * 60 * 60);
    const DEFAULT_POLL_JITTER: Duration = Duration::from_millis(50);

    /// Settings a worker polls by unless a `with_*` method replaces one of them.
    #[must_use]
    pub fn new() -> Self {
        Self {
            poll_interval: Self::DEFAULT_POLL_INTERVAL,
            poll_jitter: Self::DEFAULT_POLL_JITTER,
            max_poll_interval: None,
            retrier_config: RetrierConfig::default(),
        }
    }

    /// Replaces the base interval a worker polls storage at after a pass that found work.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `interval` is zero - a worker would then poll storage in a loop
    /// without ever pausing - if it is above [`Self::LONGEST_ACCEPTED_POLL_INTERVAL`], or if it is
    /// above a ceiling already named through [`Self::with_max_poll_interval`], which would turn the
    /// backoff into a speed-up.
    pub fn with_poll_interval(mut self, interval: Duration) -> Result<Self, Error> {
        if interval.is_zero() {
            return Err(Error::Other("poll interval must be positive".to_string()));
        }
        if interval > Self::LONGEST_ACCEPTED_POLL_INTERVAL {
            return Err(Error::Other("poll interval must not exceed a year".to_string()));
        }
        if self.max_poll_interval.is_some_and(|ceiling| ceiling < interval) {
            return Err(Error::Other(
                "max poll interval must not be below the poll interval".to_string(),
            ));
        }

        self.poll_interval = interval;
        Ok(self)
    }

    /// Replaces the upper bound of the random delay added to each poll, which keeps workers from
    /// polling in lockstep. A zero bound adds nothing.
    #[must_use]
    pub const fn with_poll_jitter(mut self, jitter: Duration) -> Self {
        self.poll_jitter = jitter;
        self
    }

    /// Replaces the ceiling the poll interval backs off to while there is no work.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `ceiling` is below the poll interval, which would turn the
    /// backoff into a speed-up, or above [`Self::LONGEST_ACCEPTED_POLL_INTERVAL`].
    pub fn with_max_poll_interval(mut self, ceiling: Duration) -> Result<Self, Error> {
        if ceiling < self.poll_interval {
            return Err(Error::Other(
                "max poll interval must not be below the poll interval".to_string(),
            ));
        }
        if ceiling > Self::LONGEST_ACCEPTED_POLL_INTERVAL {
            return Err(Error::Other("max poll interval must not exceed a year".to_string()));
        }

        self.max_poll_interval = Some(ceiling);
        Ok(self)
    }

    /// Replaces the retry policy a worker applies to its storage operations.
    #[must_use]
    pub fn with_retrier(mut self, config: RetrierConfig) -> Self {
        self.retrier_config = config;
        self
    }

    /// Base interval a worker polls storage at after a pass that found work.
    ///
    /// Applies per job: a job with nothing to wait for is polled at this interval, while one whose
    /// next iteration is not due yet is not polled until it is.
    pub const fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Upper bound of the random delay added to each poll.
    ///
    /// Also spreads the moment jobs sharing a schedule are picked up: without it every worker of
    /// every pool wakes at the same instant when an interval-scheduled job becomes due, and all but
    /// one pay a rejected write. A fleet of many workers on interval-scheduled jobs wants this
    /// raised.
    pub const fn poll_jitter(&self) -> Duration {
        self.poll_jitter
    }

    /// Ceiling the poll interval backs off to while there is no work.
    ///
    /// A ceiling nobody named is [`Self::DEFAULT_MAX_POLL_INTERVAL_FACTOR`] times the poll
    /// interval, so the backoff spans the same number of doublings whatever the base interval is;
    /// a ceiling named through [`Self::with_max_poll_interval`] replaces it outright. A pool that
    /// wants a rare poll while idle without a frequent one while busy names both.
    pub fn max_poll_interval(&self) -> Duration {
        self.max_poll_interval
            .unwrap_or(self.poll_interval * Self::DEFAULT_MAX_POLL_INTERVAL_FACTOR)
    }

    /// Retry policy a worker applies to its storage operations.
    pub const fn retrier_config(&self) -> &RetrierConfig {
        &self.retrier_config
    }
}

struct JobCacheEntry {
    next_poll: std::time::Instant,
    // Backoff step of this job alone: a busy job must not hold an idle neighbour at the base
    // interval, nor an idle one drag a busy neighbour up to the ceiling.
    poll_interval: Duration,
    exhausted: bool, // true if job reached maxIterations
}

/// How one pass over a job ended, which is what decides when polling it again can change anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum JobPassOutcome {
    /// The job spent its iteration budget: no later pass can change anything.
    IterationBudgetSpent,
    /// The iteration finished; the next one is not due before the given moment.
    NextIterationDueAt(DateTime<Utc>),
    /// The pass moved the job.
    Processed,
    /// The pass changed nothing, or failed.
    Stalled,
}

/// Time left until `moment`, or zero if it has already passed, and never more than
/// [`WorkerConfig::LONGEST_ACCEPTED_POLL_INTERVAL`].
///
/// The cap holds the scheduler's arithmetic inside the range configured intervals are checked
/// against: a moment set through [`JobHandle::set_next_start_at`] passes no such check, and adding a
/// far enough one to an `Instant` panics. A job polled a year before it is due pays one pass, which
/// schedules it again.
fn calculate_duration_until(moment: DateTime<Utc>) -> Duration {
    std::cmp::min(
        (moment - Utc::now()).to_std().unwrap_or(Duration::ZERO),
        WorkerConfig::LONGEST_ACCEPTED_POLL_INTERVAL,
    )
}

struct JobMergeContext<'a> {
    current_job: &'a Job,
    saved_job: Job,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SaveOutcome {
    Saved,
    Skipped,
    Stolen,
}

enum MergeDecision {
    Retry(Job),
    Done(Job, SaveOutcome),
}

fn panic_payload_to_string(panic: &(dyn Any + Send)) -> String {
    #[allow(clippy::option_if_let_else)]
    if let Some(message) = panic.downcast_ref::<&str>() {
        (*message).to_string()
    } else if let Some(message) = panic.downcast_ref::<String>() {
        message.clone()
    } else {
        "unknown panic".to_string()
    }
}

/// Where a worker publishes the end of a job iteration. What the report is kept in is the pool's
/// business, not the worker's.
pub(crate) trait FinishedIterationSink: Send + Sync {
    /// Records `iter_num` as a finished iteration of `job_code`. Called while a worker is doing its
    /// own work - both when it finishes an iteration and when it picks up one that already ended -
    /// so it must neither block nor fail, and must tolerate the same iteration being reported more
    /// than once.
    fn record_finished_iteration(&self, job_code: &JobCode, iter_num: u64);
}

/// Iterates through jobs known to the [`JobRegistry`], polling storage for each and executing
/// at most one task per job per pass. A single `Worker` runs tasks strictly sequentially;
/// concurrency across tasks is achieved by running multiple workers (see
/// [`JobsManagerConfig::worker_count`](crate::JobsManagerConfig::worker_count)).
pub(crate) struct Worker {
    id: Uuid,
    job_registry: Arc<JobRegistry>,
    storage: Arc<dyn Storage>,
    config: WorkerConfig,
    retrier: Retrier,
    metrics: Arc<dyn MetricsSink>,
    iteration_notifier: Option<mpsc::Sender<JobIterationStarted>>,
    finished_iterations: Arc<dyn FinishedIterationSink>,

    // Cache to minimize S3 poll requests
    job_cache: RwLock<HashMap<JobCode, JobCacheEntry>>,
    // TODO(med): combine metrics, iteration_notifier, finished_iterations so that the worker simply publishes the event, and subscribers process the event themselves.
}

// TODO(low): separate the job polling logic into a separate module

impl Worker {
    pub fn new(
        job_registry: Arc<JobRegistry>,
        storage: Arc<dyn Storage>,
        config: WorkerConfig,
        metrics: Arc<dyn MetricsSink>,
        cleanup_notifier: Option<mpsc::Sender<JobIterationStarted>>,
        finished_iterations: Arc<dyn FinishedIterationSink>,
    ) -> Self {
        let retrier = Retrier::new(config.retrier_config().clone());

        Self {
            id: Uuid::new_v4(),
            job_registry,
            storage,
            config,
            retrier,
            metrics,
            iteration_notifier: cleanup_notifier,
            finished_iterations,
            job_cache: RwLock::new(HashMap::new()),
        }
    }

    // Do not add trace instrumentation here, it will cause an infinite trace.
    pub async fn start(&self, cancel_token: CancellationToken) -> Result<(), InternalError> {
        info!("Starting worker {}", self.id);

        loop {
            let wait_duration = self.calculate_wait_duration();
            tokio::select! {
                () = cancel_token.cancelled() => {
                    info!("Stopping worker {}", self.id);
                    return Ok(());
                }
                () = sleep(wait_duration) => {}
            }

            self.process_jobs(&cancel_token).await;
        }
    }

    /// Interval for the pass after an idle one: twice the last, capped at the configured maximum.
    fn calculate_backed_off_poll_interval(config: &WorkerConfig, last_poll_interval: Duration) -> Duration {
        std::cmp::min(last_poll_interval * 2, config.max_poll_interval())
    }

    /// Spreads `poll_interval` over a random share of the configured jitter, which is what keeps
    /// workers from polling in lockstep. A zero jitter draws nothing, so the wait is the interval
    /// itself.
    fn calculate_jittered_wait(config: &WorkerConfig, poll_interval: Duration) -> Duration {
        let max_jitter_nanos = u64::try_from(config.poll_jitter().as_nanos()).unwrap_or(u64::MAX);
        let jitter = if max_jitter_nanos == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(rand::rng().random_range(0..max_jitter_nanos))
        };

        poll_interval + jitter
    }

    /// How long to wait before the next pass: until the nearest job is due for a poll, plus jitter.
    ///
    /// A job this worker has not polled yet has no scheduled moment, so the base interval stands in
    /// for it. A worker whose jobs have all spent their iteration budget polls nothing at all and
    /// waits the ceiling rather than waking on the base interval to do nothing.
    fn calculate_wait_duration(&self) -> Duration {
        let job_count = self.job_registry.jobs_count();
        let poll_interval = {
            let cache = self.job_cache.read();
            let now = std::time::Instant::now();
            if cache.len() < job_count {
                self.config.poll_interval()
            } else {
                cache
                    .values()
                    .filter(|entry| !entry.exhausted)
                    .map(|entry| entry.next_poll.saturating_duration_since(now))
                    .min()
                    .unwrap_or_else(|| self.config.max_poll_interval())
            }
        };

        Self::calculate_jittered_wait(&self.config, poll_interval)
    }

    async fn process_jobs(&self, cancel_token: &CancellationToken) {
        for job_code in self.job_registry.list_jobs() {
            if !self.should_poll_job(&job_code) {
                continue;
            }
            let pass_outcome = self.process_job(&job_code, cancel_token).await;
            debug!("Job processed: {} ({:?})", job_code, pass_outcome);
            self.schedule_next_poll(&job_code, pass_outcome);
        }
    }

    /// Records when polling `job_code` can change anything again, given how its last pass ended.
    fn schedule_next_poll(&self, job_code: &JobCode, pass_outcome: JobPassOutcome) {
        let base_interval = self.config.poll_interval();
        let now = std::time::Instant::now();
        let mut cache = self.job_cache.write();
        let entry = cache.entry(job_code.clone()).or_insert_with(|| JobCacheEntry {
            next_poll: now,
            poll_interval: base_interval,
            exhausted: false,
        });

        match pass_outcome {
            JobPassOutcome::IterationBudgetSpent => entry.exhausted = true,
            JobPassOutcome::Processed => {
                entry.poll_interval = base_interval;
                entry.next_poll = now + base_interval;
            }
            JobPassOutcome::Stalled => {
                entry.poll_interval = Self::calculate_backed_off_poll_interval(&self.config, entry.poll_interval);
                entry.next_poll = now + entry.poll_interval;
            }
            JobPassOutcome::NextIterationDueAt(due) => {
                entry.poll_interval = base_interval;
                entry.next_poll = now + calculate_duration_until(due);
            }
        }
        drop(cache);
    }

    /// One pass over `job_code`.
    async fn process_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> JobPassOutcome {
        let job = match self.storage.get_job(job_code, cancel_token).await {
            Ok(job) => job,
            Err(StorageError::NotFound(_)) => match self.create_new_job(job_code, cancel_token).await {
                Ok(job) => job,
                Err(InternalError::Cancelled) => {
                    debug!("Job creation cancelled");
                    return JobPassOutcome::Stalled;
                }
                Err(e) => {
                    error!("Failed to create new job {}: {}", job_code, e);
                    return JobPassOutcome::Stalled;
                }
            },
            // go to process job
            Err(StorageError::Cancelled) => {
                debug!("Job processing cancelled");
                return JobPassOutcome::Stalled;
            }
            Err(e) => {
                error!("Failed to get job {} for processing: {}", job_code, e);
                return JobPassOutcome::Stalled;
            }
        };

        self.try_process_job(job, cancel_token).await
    }

    fn should_poll_job(&self, job_code: &JobCode) -> bool {
        let cache = self.job_cache.read();
        if let Some(entry) = cache.get(job_code) {
            // Don't poll jobs that exhausted iteration limit
            if entry.exhausted {
                return false;
            }
            std::time::Instant::now() > entry.next_poll
        } else {
            true
        }
    }

    // Jobs can only be created from code, creation outside code makes no sense since there won't be
    // task handlers in code.
    #[tracing::instrument(skip(self, cancel_token), fields(worker_id = %self.id, job_code = %code))]
    async fn create_new_job(&self, code: &JobCode, cancel_token: &CancellationToken) -> Result<Job, InternalError> {
        let job_def = self.job_registry.get_job(code)?;

        let job = Job::new(&job_def, HashMap::new(), self.id)?;

        let (job, outcome) = self
            .save_job_state(job, cancel_token, move |ctx| {
                let JobMergeContext { saved_job, .. } = ctx;
                debug!(
                    "Job '{}' already created (id: {}) by worker '{}'",
                    code,
                    saved_job.id(),
                    saved_job.updated_by_worker_id()
                );
                // TODO(low): in saveJobState on ConcurrentModification error we re-read job, which is unnecessary in this case.
                Ok(MergeDecision::Done(saved_job, SaveOutcome::Skipped)) // Someone beat us to it
            })
            .await?;

        if outcome == SaveOutcome::Saved {
            info!("New job '{}' created (id: {}) by worker '{}'", code, job.id(), self.id);
        }

        Ok(job)
    }

    #[tracing::instrument(
        skip(self, job, cancel_token),
        fields(
            worker_id = %self.id,
            job_code = %job.code(),
            job_id = %job.id(),
            job_start_at = %job.started_at(),
            iter = job.iter_num(),
            otel.name = %format!("try_process_job-{}", job.code())
        )
    )]
    async fn try_process_job(&self, mut job: Job, cancel_token: &CancellationToken) -> JobPassOutcome {
        // An iteration this worker did not finish itself - one from a previous run of the pool, or
        // one another worker completed - is only observable here, on the pickup. Reporting it
        // before the branches below, because starting the next iteration overwrites the number and
        // a job that spent its iteration budget stops being polled at all.
        if job.is_processed() {
            self.report_finished_iteration(&job);
        }

        // The next-iteration gate anchors on the persisted started_at of the current
        // iteration, which survives process restarts. Log the inputs so a restart that
        // appears to "shift" the schedule can be traced back to the actual anchor.
        let iteration_step = job.pick_iteration_step();
        debug!(
            status = %job.status(),
            started_at = %job.started_at(),
            completed_at = ?job.completed_at(),
            next_start_at = ?job.next_start_at(),
            ?iteration_step,
            "Evaluating job scheduling on pickup"
        );

        match iteration_step {
            IterationStep::IterationInProgress => {}
            IterationStep::NextIterationReady => {
                job = match self.start_new_job_iteration(job, cancel_token).await {
                    Ok(job) => job,
                    Err(InternalError::Cancelled) => {
                        debug!("Job iteration start cancelled");
                        return JobPassOutcome::Stalled;
                    }
                    Err(e) => {
                        error!("Failed to start job iteration: {}", e);
                        return JobPassOutcome::Stalled;
                    }
                };
            }
            IterationStep::NextIterationDueAt(due) => return JobPassOutcome::NextIterationDueAt(due),
            IterationStep::IterationBudgetSpent => return JobPassOutcome::IterationBudgetSpent,
            // Nothing to wait for and nothing terminal about the state, so the job falls back to
            // the ordinary backoff rather than to a moment nobody can compute.
            IterationStep::NextIterationUnscheduled => return JobPassOutcome::Stalled,
        }

        // The start above can come back with the state another worker saved, which may already be
        // an iteration this worker has nothing left to do in.
        if !job.is_ready_for_processing() {
            return JobPassOutcome::Stalled;
        }

        debug!(started_at = %job.started_at(), "Job ready for processing");
        match self.pick_and_execute_task(job, cancel_token).await {
            Ok(true) => JobPassOutcome::Processed,
            Ok(false) => JobPassOutcome::Stalled,
            Err(InternalError::Cancelled) => {
                debug!("Job processing cancelled");
                JobPassOutcome::Stalled
            }
            Err(e) => {
                error!("Failed to execute task: {}", e);
                JobPassOutcome::Stalled
            }
        }
    }

    // This can be either first job run or next iteration of job run.
    #[tracing::instrument(
        skip(self, job, cancel_token),
        fields(
            worker_id = %self.id,
            job_code = %job.code(),
            job_id = %job.id(),
            iter = job.iter_num()
        )
    )]
    async fn start_new_job_iteration(
        &self,
        mut job: Job,
        cancel_token: &CancellationToken,
    ) -> Result<Job, InternalError> {
        let job_def = self.job_registry.get_job(job.code())?;

        job.next_iteration(&job_def, self.id)?;

        let (job, outcome) = self
            .save_job_state(job, cancel_token, move |ctx| {
                let JobMergeContext { saved_job, .. } = ctx;
                debug!(
                    "Job '{}' already started new iteration (id: {}, iter: {}) by worker '{}'",
                    saved_job.code(),
                    saved_job.id(),
                    saved_job.iter_num(),
                    saved_job.updated_by_worker_id()
                );
                // TODO(low): in saveJobState on ErrConcurrentModification we re-read job, which is unnecessary in
                // this case.
                Ok(MergeDecision::Done(saved_job, SaveOutcome::Skipped)) // Someone beat us to it
            })
            .await?;

        if outcome == SaveOutcome::Saved {
            info!(
                "New job '{}' iteration started (id: {}, iter: {}) by worker '{}'",
                job.code(),
                job.id(),
                job.iter_num(),
                self.id
            );
            self.report_started_iteration(job.code(), job.iter_num());
        }

        Ok(job)
    }

    fn report_started_iteration(&self, job_code: &JobCode, iter_num: u64) {
        let Some(notifier) = self.iteration_notifier.as_ref() else {
            return;
        };

        let message = JobIterationStarted {
            job_code: job_code.clone(),
            iter_num,
        };
        // Never blocks and never fails the iteration: a report that does not fit in the channel, or
        // arrives after the cleaner is gone, is dropped. The cleanup it would have triggered is
        // picked up by the next start-up reconciliation instead.
        if let Err(e) = notifier.try_send(message) {
            warn!("Job '{job_code}' iteration {iter_num} not reported for cleanup: {e}");
        }
    }

    async fn pick_and_execute_task(
        &self,
        mut job: Job,
        cancel_token: &CancellationToken,
    ) -> Result<bool, InternalError> {
        // TODO(low): think about what to do here, likely we have invalid job state
        let task_id = match job.pick_task_to_execute(&self.id)? {
            TaskPickup::Ready(task_id) => task_id,
            TaskPickup::Waiting => {
                debug!("Tasks for job {} not found", job.code());
                return Ok(false);
            }
            TaskPickup::Exhausted => {
                return self.save_failed_iteration(job, cancel_token).await;
            }
        };
        let task_code = job.get_task(&task_id)?.code().clone();
        let job_code = job.code().clone();

        match job.start_task(&task_id, self.id) {
            Ok(()) => {}
            Err(JobError::TaskWorkerMismatch) => {
                self.metrics.record_task_stolen(&job_code, &task_code, "pick_start_local");
                return Ok(true);
            }
            Err(e) => return Err(InternalError::from(e)),
        }

        debug!("Task '{}' started", task_id);

        let task_id_clone = task_id;
        let worker_id = self.id;
        let metrics = self.metrics.clone();
        let task_code_for_metrics = task_code.clone();
        let job_code_for_metrics = job_code.clone();
        let (job, outcome) = self
            .save_job_state(job, cancel_token, move |ctx| {
                let JobMergeContext {
                    current_job,
                    mut saved_job,
                } = ctx;
                match saved_job.merge_with_picked_task(current_job, &worker_id, &task_id_clone) {
                    Ok(()) => {
                        metrics.record_save_conflict_retry(&job_code_for_metrics, "pick_start_conflict");
                        debug!("Job has concurrent modification when picking task - retry");
                        Ok(MergeDecision::Retry(saved_job))
                    }
                    Err(JobError::TaskWorkerMismatch) => {
                        metrics.record_task_stolen(
                            &job_code_for_metrics,
                            &task_code_for_metrics,
                            "pick_start_conflict",
                        );
                        debug!("Job has concurrent modification when picking task - skip");
                        Ok(MergeDecision::Done(saved_job, SaveOutcome::Stolen))
                    }
                    Err(e) => {
                        Err(InternalError::from(e)) // Don't retry
                    }
                }
            })
            .await?;

        if outcome == SaveOutcome::Stolen {
            info!("Job was stolen");
            return Ok(true);
        }

        info!("Started processing task '{}' (job iter: {})", task_id, job.iter_num());

        self.execute_task(job, &task_id, cancel_token.clone()).await?;

        Ok(true)
    }

    #[allow(clippy::map_unwrap_or)]
    #[tracing::instrument(
        skip(self, job, cancel_token),
        fields(
            worker_id = %self.id,
            job_code = %job.code(),
            job_id = %job.id(),
            iter = job.iter_num(),
            task_id = %task_id,
            task_code = %job
                .get_task(task_id)
                .map(|task| task.code().clone())
                .unwrap_or_else(|_| TaskCode::new("unknown"))
        )
    )]
    async fn execute_task(
        &self,
        job: Job,
        task_id: &Uuid,
        cancel_token: CancellationToken,
    ) -> Result<(), InternalError> {
        let task_id = *task_id;
        let task = job.get_task(&task_id)?;
        let task_code = task.code().clone();
        let job_code = job.code().clone();

        let executor = self.job_registry.get_task_executor(job.code(), &task.code().clone())?;

        // The deadline is what the executor's cancellation is scheduled by, so it is read from the
        // domain state rather than from the view the executor itself is given. It already accounts
        // for the task's maximum lifetime - the domain caps it there - so there is no second
        // boundary for a worker to work out.
        let deadline_at = job.find_task(&task_id)?.deadline_at();

        // The handle the executor is given owns the job, so it can be moved into an async closure.
        // Ownership comes back below, once the handle is closed and dropped.
        let shared_state = Arc::new(RwLock::new(JobHandleState::new(job)));
        let job_handle = Arc::new(JobHandleImpl::new(Arc::clone(&shared_state), self.id, task_id));
        // Two tokens rather than one, because the deadline is a verdict about the execution and the
        // executor holds what it is given: cancelling the executor's own token does not reach the
        // deadline token, so a self-cancelled executor cannot report its refusal as a deadline. The
        // deadline token carries the pool's shutdown, and the task's deadline on top of it.
        let deadline_cancel_token = cancel_token.child_token();
        let executor_cancel_token = deadline_cancel_token.child_token();
        let outcome = {
            let ctx = TaskContext::new(
                task,
                Arc::clone(&job_handle) as Arc<dyn JobHandle>,
                executor_cancel_token,
            );
            Self::run_executor_until_deadline(executor.execute(ctx), deadline_at, &deadline_cancel_token).await
        };
        job_handle.close();
        // Dropping the worker's own reference is what makes the reclaim below succeed; without it
        // the strong count never reaches one and every execution would take the clone path.
        drop(job_handle);

        // A shutdown requested while the executor was running leaves the task untouched, exactly as
        // a cancelled storage call does: nothing is persisted and the task is picked up again
        // later. The outcome the executor did return is dropped with it.
        if cancel_token.is_cancelled() {
            return Err(InternalError::Cancelled);
        }

        let mut job = match Arc::try_unwrap(shared_state) {
            Ok(lock) => lock.into_inner().into_job(),
            // The executor moved the handle into a task that outlived its own call. The handle is
            // already closed, so the escaped copy can no longer mutate anything; copying the state
            // out from under the lock is enough to carry on.
            Err(shared) => {
                warn!("Executor of task '{}' outlived its own call", task_id);
                shared.read().job().clone()
            }
        };

        let result = match outcome {
            // The worker closes the task itself, so an executor cannot leave one hanging by
            // forgetting to.
            Ok(Ok(TaskOutcome::Completed(output))) => job.complete_task(&task_id, output).map_err(InternalError::from),
            // The executor resolved the task through its handle; touching it again would fail.
            Ok(Ok(TaskOutcome::Deferred)) => Self::check_task_resolution(&job, &task_id),
            // A shutdown was ruled out above, so the executor was released by its own deadline. The
            // task stays as it is for another worker to take over, and the pass counts as work done:
            // backing the poll interval off before a takeover that is already due would only delay
            // it.
            Ok(Ok(TaskOutcome::Cancelled)) if deadline_cancel_token.is_cancelled() => {
                info!("Task '{}' execution cancelled by its deadline", task_id);
                return Ok(());
            }
            // Neither the deadline nor a shutdown released this execution, so the outcome is a
            // broken contract rather than a cancellation - including when the executor cancelled
            // its own token. Honouring it would leave the task held until its deadline runs out, on
            // an attempt already spent, and report a cancellation that never happened.
            Ok(Ok(TaskOutcome::Cancelled)) => Err(InternalError::Other(
                "executor returned Cancelled without a cancellation of its execution: return the outcome of \
                 the work, or select on the token and return Cancelled once it fires"
                    .to_string(),
            )),
            Ok(Err(e)) => Err(InternalError::Other(e.to_string())),
            Err(panic) => Err(InternalError::Other(format!(
                "executor panicked: {}",
                panic_payload_to_string(&*panic)
            ))),
        };

        // TODO(low): think about to fail the task when expired
        if result.is_ok() && job.get_task(&task_id)?.is_expired() {
            info!("Task '{}' exceeded deadline", task_id);
        }

        // The two outcomes are told apart by what they record on the task and by the reason the save
        // is labelled with; what follows is the same for both, because an execution that failed
        // after its executor resolved its own task leaves a resolved task behind, and that result
        // completes the iteration exactly like a successful one.
        let save_reason = match result {
            Err(e) => {
                let rolled_back_tasks = job.record_task_execution_failure(&task_id, &e.to_string())?;
                info!(rolled_back_tasks, "Task '{}' execution failed: {}", task_id, e);
                "save_failed_task"
            }
            Ok(()) => {
                info!("Task '{}' handled successfully", task_id);
                "save_completed_task"
            }
        };

        let worker_id = self.id;
        let task_id_clone = task_id;
        let metrics = self.metrics.clone();
        let task_code_for_metrics = task_code.clone();
        let job_code_for_metrics = job_code.clone();

        job.try_to_complete(&worker_id)?;

        let job = self
            .save_processed_task(job, &task_id, &cancel_token, move |ctx| {
                let JobMergeContext {
                    current_job,
                    mut saved_job,
                } = ctx;
                match saved_job.merge_with_processed_task(current_job, &worker_id, &task_id_clone) {
                    Ok(()) => {
                        metrics.record_save_conflict_retry(&job_code_for_metrics, save_reason);
                        // conditions for job completion might have been met (another worker completed task)
                        saved_job.try_to_complete(&worker_id)?;
                        debug!("Retry to save processed task ({save_reason})");
                        Ok(MergeDecision::Retry(saved_job))
                    }
                    Err(JobError::TaskWorkerMismatch) => {
                        metrics.record_task_stolen(&job_code_for_metrics, &task_code_for_metrics, save_reason);
                        debug!("Task has stolen when try to save processed task ({save_reason}) - skip");
                        Ok(MergeDecision::Done(saved_job, SaveOutcome::Stolen))
                    }
                    Err(e) => Err(InternalError::from(e)),
                }
            })
            .await?;

        if job.is_processed() {
            self.job_completed(&job);
        }

        Ok(())
    }

    async fn save_job_state<F>(
        &self,
        job: Job,
        cancel_token: &CancellationToken,
        concurrent_modification_handler: F,
    ) -> Result<(Job, SaveOutcome), InternalError>
    where
        F: for<'a> Fn(JobMergeContext<'a>) -> Result<MergeDecision, InternalError> + Send + Sync,
    {
        let storage = Arc::clone(&self.storage);
        let job_code = job.code().clone();
        let handler = Arc::new(concurrent_modification_handler);
        // Keep job state here so retrier doesn't need to carry it.
        let wrapped_job = Arc::new(Mutex::new(Some(job)));

        let outcome = self
            .retrier
            .retry(
                {
                    let storage = Arc::clone(&storage);
                    let job_code = job_code.clone();
                    let handler = Arc::clone(&handler);
                    let wrapped_job = Arc::clone(&wrapped_job);
                    move || {
                        let storage = Arc::clone(&storage);
                        let job_code = job_code.clone();
                        let handler = Arc::clone(&handler);
                        let wrapped_job = Arc::clone(&wrapped_job);
                        async move {
                            // Take job ownership for this attempt without holding the lock across await.
                            let mut current_job = {
                                let mut guard = wrapped_job.lock();
                                guard.take().ok_or_else(|| InternalError::Other("job state missing".into()))?
                            };
                            match storage.save_job(&mut current_job, cancel_token).await {
                                Ok(()) => {
                                    // Save succeeded: store updated job and stop retrying.
                                    *wrapped_job.lock() = Some(current_job);
                                    Ok(RetryStep::Done(SaveOutcome::Saved))
                                }
                                Err(e) if e.is_conflict() => {
                                    let conflict = InternalError::from(e);
                                    // Conflict: refresh from storage, merge (if needed), and decide if we retry.
                                    let saved_job = storage.get_job(&job_code, cancel_token).await?; // TODO(med): getting a job is not always necessary, for example, it is not necessary when taking a task to work.
                                    match handler(JobMergeContext {
                                        current_job: &current_job,
                                        saved_job,
                                    })? {
                                        MergeDecision::Retry(updated_job) => {
                                            *wrapped_job.lock() = Some(updated_job);
                                            Ok(RetryStep::Retry(conflict))
                                        }
                                        MergeDecision::Done(updated_job, outcome) => {
                                            *wrapped_job.lock() = Some(updated_job);
                                            Ok(RetryStep::Done(outcome))
                                        }
                                    }
                                }
                                Err(e) => Err(InternalError::from(e)),
                            }
                        }
                    }
                },
                cancel_token,
            )
            .await?;

        // Return the last job state after retries.
        let updated_job = wrapped_job
            .lock()
            .take()
            .ok_or_else(|| InternalError::Other("job state missing".into()))?;
        Ok((updated_job, outcome))
    }

    async fn save_processed_task<F>(
        &self,
        job: Job,
        task_id: &Uuid,
        cancel_token: &CancellationToken,
        concurrent_modification_handler: F,
    ) -> Result<Job, InternalError>
    where
        F: for<'a> Fn(JobMergeContext<'a>) -> Result<MergeDecision, InternalError> + Send + Sync,
    {
        let (job, outcome) = self.save_job_state(job, cancel_token, concurrent_modification_handler).await?;

        if outcome == SaveOutcome::Stolen {
            info!("Task '{}' was stolen during save, skipping merge", task_id);
            return Ok(job);
        }

        debug!(
            "Job '{}' saved with processed task '{}' (iter: {}, version: {})",
            job.code(),
            task_id,
            job.iter_num(),
            job.version()
        );

        if let Ok(task) = job.find_task(task_id) {
            // Calculate duration if start/complete times are available
            let duration = match (task.completed_at(), task.started_at()) {
                (Some(completed), Some(started)) => completed
                    .signed_duration_since(started)
                    .to_std()
                    .unwrap_or(Duration::from_secs(0)),
                _ => Duration::from_secs(0),
            };

            self.metrics
                .record_task_processed(job.code(), task.code(), task.status(), duration);
        }

        Ok(job)
    }

    /// Persist an iteration that cannot progress because tasks ran out of either limit - their
    /// attempt budget or their maximum lifetime.
    ///
    /// [`Job::pick_task_to_execute`] has already moved the job to `Failed`;
    /// saving that state is what lets the scheduler start the next iteration,
    /// which replans from scratch. A concurrent modification means another
    /// worker advanced the job, so this worker drops its verdict and re-derives
    /// it on the next poll.
    async fn save_failed_iteration(&self, job: Job, cancel_token: &CancellationToken) -> Result<bool, InternalError> {
        error!(
            "Job {} iteration {} failed - tasks ran out of their attempt budget or maximum lifetime ({})",
            job.code(),
            job.iter_num(),
            job.tasks_as_string()
        );

        let (job, outcome) = self
            .save_job_state(job, cancel_token, |ctx| {
                debug!("Job has concurrent modification when failing iteration - skip");
                Ok(MergeDecision::Done(ctx.saved_job, SaveOutcome::Skipped))
            })
            .await?;

        if outcome == SaveOutcome::Saved {
            self.record_job_iteration(&job, &JobStatus::Failed);
            self.report_finished_iteration(&job);
        }

        Ok(true)
    }

    fn job_completed(&self, job: &Job) {
        info!("Job {} completed (iter: {})", job.code(), job.iter_num());
        self.record_job_iteration(job, &JobStatus::Completed);
        self.report_finished_iteration(job);
    }

    /// Publishes the end of an iteration to whoever is waiting for it.
    fn report_finished_iteration(&self, job: &Job) {
        self.finished_iterations.record_finished_iteration(job.code(), job.iter_num());
    }

    /// Record the duration of a finished job iteration under its final `status`.
    fn record_job_iteration(&self, job: &Job, status: &JobStatus) {
        let duration = job.completed_at().map_or_else(
            || Duration::from_secs(0),
            |completed| {
                completed
                    .signed_duration_since(job.started_at())
                    .to_std()
                    .unwrap_or(Duration::from_secs(0))
            },
        );

        self.metrics.record_job_iteration_complete(job.code(), status, duration);
    }

    /// Runs `execution` to completion, cancelling `deadline_cancel_token` once `deadline_at` has
    /// passed - or right away if it already has.
    ///
    /// The future is never dropped, only signalled: dropping it would tear the executor down at
    /// whatever await point it sits on, leaving it no chance to clean up. Cancellation is
    /// therefore cooperative, and an executor that does not select on its token keeps running
    /// past the deadline while another worker takes its task over.
    async fn run_executor_until_deadline(
        execution: impl Future<Output = TaskResult> + Send,
        deadline_at: Option<DateTime<Utc>>,
        deadline_cancel_token: &CancellationToken,
    ) -> Result<TaskResult, Box<dyn Any + Send>> {
        let execution = AssertUnwindSafe(execution).catch_unwind();
        tokio::pin!(execution);

        if let Some(deadline_at) = deadline_at {
            let time_left = (deadline_at - Utc::now()).to_std().unwrap_or(Duration::ZERO);
            tokio::select! {
                outcome = &mut execution => return outcome,
                () = sleep(time_left) => {
                    debug!("Task deadline reached, cancelling the execution token");
                    deadline_cancel_token.cancel();
                }
            }
        }

        execution.await
    }

    /// Checks that the task an executor returned [`TaskOutcome::Deferred`] for is no longer open.
    /// Completing and failing are the only ways to resolve one, so anything else was left for
    /// nobody to close.
    fn check_task_resolution(job: &Job, task_id: &Uuid) -> Result<(), InternalError> {
        if job.find_task(task_id)?.is_resolved() {
            return Ok(());
        }

        Err(InternalError::Other(format!(
            "executor returned Deferred without resolving task '{task_id}': complete or fail it through \
         the job handle, or return the outcome instead"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::common::storage_wrapper::SaveRefusingStorage;
    use crate::{
        InMemoryStorage, JobDefinition, JobDefinitionId, NoopMetrics, TaskDefinition, TaskExecutor, TaskLimits, task_fn,
    };

    /// How many draws a test takes when it asserts on the spread of the jitter rather than on a
    /// single value. A range of milliseconds has millions of nanoseconds in it, so this many equal
    /// draws does not happen for any reason other than a jitter that is not drawn at all.
    const JITTER_DRAWS: usize = 50;

    fn config_with_jitter(jitter: Duration) -> WorkerConfig {
        config_polled_every(Duration::from_millis(100)).with_poll_jitter(jitter)
    }

    fn config_polled_every(interval: Duration) -> WorkerConfig {
        WorkerConfig::new()
            .with_poll_interval(interval)
            .expect("a positive poll interval is accepted")
    }

    /// Turning the jitter off must leave the poll interval as it is - a worker that waits longer
    /// than it was configured to polls storage at half the rate its settings promise.
    #[test]
    fn a_zero_jitter_leaves_the_poll_interval_alone() {
        let config = config_with_jitter(Duration::ZERO);

        assert_eq!(
            Worker::calculate_jittered_wait(&config, Duration::from_millis(100)),
            Duration::from_millis(100)
        );
    }

    /// The bound of the jitter is a `Duration` the caller picks freely, so the three cases around
    /// the millisecond it used to be counted in: below it, exactly it, and above it.
    #[test]
    fn a_sub_millisecond_jitter_stays_within_its_bound() {
        assert_jitter_within_bound(Duration::from_micros(500));
    }

    #[test]
    fn a_jitter_of_exactly_one_millisecond_stays_within_its_bound() {
        assert_jitter_within_bound(Duration::from_millis(1));
    }

    #[test]
    fn a_multi_millisecond_jitter_stays_within_its_bound() {
        assert_jitter_within_bound(Duration::from_millis(5));
    }

    /// Draws repeatedly, because one draw says nothing about a bound: it asserts that every wait
    /// lands in `[poll_interval, poll_interval + bound)` and that the jitter is really drawn.
    fn assert_jitter_within_bound(bound: Duration) {
        let poll_interval = Duration::from_millis(100);
        let config = config_with_jitter(bound);

        let waits: Vec<Duration> = (0..JITTER_DRAWS)
            .map(|_| Worker::calculate_jittered_wait(&config, poll_interval))
            .collect();

        for wait in &waits {
            assert!(
                (poll_interval..poll_interval + bound).contains(wait),
                "a wait of {wait:?} is outside the bound of {bound:?} on top of {poll_interval:?}"
            );
        }
        assert!(
            waits.iter().any(|wait| *wait != waits[0]),
            "every draw returned {:?}, so no jitter was added",
            waits[0]
        );
    }

    #[test]
    fn a_pass_without_work_doubles_the_poll_interval() {
        let config = config_with_jitter(Duration::ZERO);

        assert_eq!(
            Worker::calculate_backed_off_poll_interval(&config, Duration::from_millis(100)),
            Duration::from_millis(200)
        );
    }

    /// The backoff is what the maximum poll interval is a ceiling for, so it must stop there
    /// instead of doubling past it.
    #[test]
    fn the_backoff_stops_at_the_maximum_poll_interval() {
        let config = config_with_jitter(Duration::ZERO)
            .with_max_poll_interval(Duration::from_millis(300))
            .expect("a ceiling above the base interval is accepted");

        assert_eq!(
            Worker::calculate_backed_off_poll_interval(&config, Duration::from_millis(200)),
            Duration::from_millis(300)
        );
    }

    /// A worker given a zero interval polls storage without ever pausing, which saturates a core
    /// and bills for an unbounded number of requests.
    #[test]
    fn a_zero_poll_interval_is_rejected() {
        let Err(error) = WorkerConfig::new().with_poll_interval(Duration::ZERO) else {
            panic!("a zero poll interval must be rejected")
        };

        assert!(
            error.to_string().contains("poll interval must be positive"),
            "got: {error}"
        );
    }

    /// The maximum is the ceiling the backoff climbs to, so a value below the base would turn the
    /// backoff into a speed-up.
    #[test]
    fn a_maximum_poll_interval_below_the_poll_interval_is_rejected() {
        let Err(error) =
            config_polled_every(Duration::from_millis(200)).with_max_poll_interval(Duration::from_millis(100))
        else {
            panic!("a ceiling below the base interval must be rejected")
        };

        assert!(
            error.to_string().contains("max poll interval must not be below"),
            "got: {error}"
        );
    }

    /// The same pair named in the other order: a config accepting it either way would depend on
    /// which setting its caller happened to name first.
    #[test]
    fn a_poll_interval_above_a_named_maximum_is_rejected() {
        let named_ceiling = config_polled_every(Duration::from_millis(50))
            .with_max_poll_interval(Duration::from_millis(100))
            .expect("a ceiling above the base interval is accepted");

        let Err(error) = named_ceiling.with_poll_interval(Duration::from_millis(200)) else {
            panic!("a base interval above the named ceiling must be rejected")
        };

        assert!(
            error.to_string().contains("max poll interval must not be below"),
            "got: {error}"
        );
    }

    /// A ceiling equal to the base is what a job polled exactly as rarely as it backs off asks for,
    /// so it has to pass rather than sit on the wrong side of the comparison.
    #[test]
    fn a_maximum_poll_interval_equal_to_the_poll_interval_is_accepted() {
        let config = config_polled_every(Duration::from_secs(10))
            .with_max_poll_interval(Duration::from_secs(10))
            .expect("a ceiling equal to the base interval is accepted");

        assert_eq!(config.max_poll_interval(), Duration::from_secs(10));
    }

    /// An unnamed ceiling is the poll interval multiplied, so a rarely polled pool still has room
    /// to back off; a fixed ceiling would sit at or below such an interval and cancel the backoff.
    #[test]
    fn an_unnamed_maximum_poll_interval_scales_with_a_long_poll_interval() {
        let config = config_polled_every(Duration::from_secs(5));

        assert_eq!(config.max_poll_interval(), Duration::from_secs(50));
    }

    /// The same rule at the other end of the range: a frequently polled pool gets a ceiling as
    /// close as its interval is, rather than one named for a pool polled at another rate.
    #[test]
    fn an_unnamed_maximum_poll_interval_scales_with_a_short_poll_interval() {
        let config = config_polled_every(Duration::from_millis(10));

        assert_eq!(config.max_poll_interval(), Duration::from_millis(100));
    }

    /// The scheduler multiplies, doubles and adds intervals to an `Instant`, and every one of those
    /// panics on overflow - so an interval it cannot compute with is refused at the boundary rather
    /// than taken down the pass that crashes the worker.
    #[test]
    fn a_poll_interval_above_the_longest_accepted_one_is_rejected() {
        let Err(error) = WorkerConfig::new().with_poll_interval(Duration::MAX) else {
            panic!("a poll interval past the accepted range must be rejected")
        };

        assert!(
            error.to_string().contains("poll interval must not exceed"),
            "got: {error}"
        );
    }

    /// The ceiling is an interval the scheduler computes with just as the base one is, so it is
    /// held to the same range - naming it is not a way around the bound.
    #[test]
    fn a_maximum_poll_interval_above_the_longest_accepted_one_is_rejected() {
        let Err(error) = WorkerConfig::new().with_max_poll_interval(Duration::MAX) else {
            panic!("a ceiling past the accepted range must be rejected")
        };

        assert!(
            error.to_string().contains("max poll interval must not exceed"),
            "got: {error}"
        );
    }

    /// The accepted range and the default factor are two halves of one rule: the ceiling derived
    /// from the longest accepted interval is doubled again by the backoff, and neither step may
    /// leave `Duration` behind. Raising either constant without the other panics here.
    #[test]
    fn the_longest_accepted_poll_interval_survives_the_backoff_arithmetic() {
        let config = config_polled_every(WorkerConfig::LONGEST_ACCEPTED_POLL_INTERVAL);
        let ceiling = config.max_poll_interval();

        assert_eq!(Worker::calculate_backed_off_poll_interval(&config, ceiling), ceiling);
    }

    /// Regression: with a fixed default ceiling, a pool polled less often than that ceiling had
    /// `min(interval * 2, ceiling)` collapse to the interval itself - the backoff never grew at all.
    #[test]
    fn a_backoff_grows_at_a_poll_interval_above_the_old_default() {
        let config = config_polled_every(Duration::from_secs(5));

        assert_eq!(
            Worker::calculate_backed_off_poll_interval(&config, Duration::from_secs(5)),
            Duration::from_secs(10)
        );
    }

    /// Deadline of the task the pass below runs, and so how long its executor waits for the token.
    const CANCELLED_TASK_TIMEOUT: Duration = Duration::from_millis(50);

    /// Longest the pass below may take: two orders of magnitude above the task's deadline, so a
    /// loaded machine does not fail the test, while a cancellation that never arrives does - with a
    /// diagnostic rather than a hung run.
    const PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);

    /// Sink for a worker whose iterations nobody waits for.
    struct IgnoredIterations;

    impl FinishedIterationSink for IgnoredIterations {
        fn record_finished_iteration(&self, _job_code: &JobCode, _iter_num: u64) {}
    }

    fn worker_running(job_def: JobDefinition, storage: Arc<dyn Storage>) -> Result<Worker, Error> {
        Ok(Worker::new(
            Arc::new(JobRegistry::new(vec![job_def])?),
            storage,
            config_polled_every(Duration::from_millis(10)),
            Arc::new(NoopMetrics),
            None,
            Arc::new(IgnoredIterations),
        ))
    }

    /// A description of one task running `executor`, for the two passes below.
    fn job_running(job_code: &str, timeout: Duration, executor: Arc<dyn TaskExecutor>) -> JobDefinition {
        JobDefinition::new(
            JobDefinitionId::new(),
            JobCode::new(job_code),
            vec![(TaskDefinition::new(TaskCode::new("cancelled"), timeout), executor)],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )
        .expect("the test description must be legal")
    }

    /// A cancellation the executor reports after its own deadline passed is not the pool shutting
    /// down - the shutdown is ruled out before the outcome is read. The pass picked, started and
    /// persisted the task, and the task is due to be taken over, so it counts as work done: treated
    /// as a shutdown it would instead double the wait before the next poll.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pass_whose_executor_was_cancelled_by_the_deadline_counts_as_work_done() {
        let executor = task_fn(|ctx| async move {
            ctx.cancel_token().cancelled().await;
            Ok(TaskOutcome::Cancelled)
        });
        let job_def = job_running("deadline_cancelled_job", CANCELLED_TASK_TIMEOUT, executor);
        let job_code = job_def.code().clone();
        let worker =
            worker_running(job_def, Arc::new(InMemoryStorage::new())).expect("the test worker must be constructible");

        let outcome = tokio::time::timeout(
            PROGRESS_TIMEOUT,
            worker.process_job(&job_code, &CancellationToken::new()),
        )
        .await
        .expect("the deadline must release the executor and let the pass finish");

        assert_eq!(
            outcome,
            JobPassOutcome::Processed,
            "a pass released by the task's own deadline must count as progress, not as a stall"
        );
    }

    /// Deadline of the task below: long enough that nothing cancels the execution, so the outcome the
    /// executor returns is the only thing under test.
    const UNCANCELLED_TASK_TIMEOUT: Duration = Duration::from_secs(30);

    /// `Cancelled` returned while nothing cancelled the execution is a broken contract, not a
    /// deadline. Honouring it would leave the task open on an attempt already spent, idle until its
    /// deadline ran out, so it is failed like any other refusal - and the recorded reason has to say
    /// which of the two happened.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_outcome_without_a_cancellation_fails_the_task() {
        let executor = task_fn(|_ctx| async { Ok(TaskOutcome::Cancelled) });

        assert_task_failed_by("uncancelled_job", executor).await;
    }

    /// The same contract, closed against the one way an executor could argue itself out of it: the
    /// token it is given is its own, so cancelling it must not be readable as the deadline. Taken
    /// for one, the refusal would leave the task held until its real deadline ran out.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_self_cancelled_executor_cannot_report_its_refusal_as_a_deadline() {
        let executor = task_fn(|ctx| async move {
            ctx.cancel_token().cancel();
            Ok(TaskOutcome::Cancelled)
        });

        assert_task_failed_by("self_cancelled_job", executor).await;
    }

    /// Stores `job_def`'s first iteration already finished, which is the state a pass reads as
    /// "start the next one": built through the production API, so no state a legal run cannot
    /// reach is persisted.
    async fn store_with_a_finished_iteration(
        job_def: &JobDefinition,
        worker_id: Uuid,
    ) -> Result<Arc<InMemoryStorage>, Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemoryStorage::new());
        let cancel_token = CancellationToken::new();
        let mut job = Job::new(job_def, HashMap::new(), worker_id)?;
        job.work(&worker_id)?;
        let TaskPickup::Ready(task_id) = job.pick_task_to_execute(&worker_id)? else {
            return Err("the fixture must leave a task to finish".into());
        };
        job.start_task(&task_id, worker_id)?;
        job.complete_task(&task_id, Vec::new())?;
        job.try_to_complete(&worker_id)?;
        storage.save_job(&mut job, &cancel_token).await?;

        Ok(storage)
    }

    /// A pass whose write of the new iteration was refused moved nothing, so it has to back off
    /// like any other empty pass. Counted as work done - which is what it used to be - the worker
    /// would keep the base interval and repeat the same refused write at full rate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_pass_that_cannot_save_the_iteration_it_started_counts_as_stalled()
    -> Result<(), Box<dyn std::error::Error>> {
        let job_def = job_running(
            "unsavable_iteration_job",
            UNCANCELLED_TASK_TIMEOUT,
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        );
        let job_code = job_def.code().clone();
        let worker_id = Uuid::from_u128(1);
        let storage = store_with_a_finished_iteration(&job_def, worker_id).await?;
        let refusing_storage = Arc::new(SaveRefusingStorage::new(Arc::clone(&storage) as Arc<dyn Storage>));
        let worker = worker_running(job_def, Arc::clone(&refusing_storage) as Arc<dyn Storage>)?;
        let cancel_token = CancellationToken::new();

        let outcome = tokio::time::timeout(PROGRESS_TIMEOUT, worker.process_job(&job_code, &cancel_token))
            .await
            .map_err(|_| "a refused save must fail the pass rather than be retried forever")?;

        assert_eq!(
            refusing_storage.refused_saves(),
            1,
            "the pass must have reached the write of the new iteration, and reached it once"
        );
        assert_eq!(
            outcome,
            JobPassOutcome::Stalled,
            "a pass that wrote nothing is not a pass that moved the job"
        );
        let stored = storage.get_job(&job_code, &cancel_token).await?;
        assert_eq!(stored.iter_num(), 1, "the refused iteration must not be stored");
        assert_eq!(*stored.status(), JobStatus::Completed, "got: {}", stored.status());
        Ok(())
    }

    /// Base interval every scheduling test below measures against.
    const BASE_POLL_INTERVAL: Duration = Duration::from_millis(100);

    /// Slack allowed when comparing a scheduled moment: the moment is taken before the assertion,
    /// so the delay measured is always a little shorter than the interval it was set from.
    const SCHEDULING_SLACK: Duration = Duration::from_millis(50);

    /// A legal one-task description whose code is all the scheduling tests below need: they call
    /// the scheduler directly and never let a worker reach storage.
    fn idle_job(job_code: &str) -> JobDefinition {
        job_running(
            job_code,
            UNCANCELLED_TASK_TIMEOUT,
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        )
    }

    /// Jitter is off here: it has tests of its own, and a random addend would make every scheduled
    /// moment below a range rather than a value.
    fn worker_over_jobs(job_defs: Vec<JobDefinition>, storage: Arc<dyn Storage>) -> Worker {
        worker_over_jobs_with_config(
            job_defs,
            storage,
            config_polled_every(BASE_POLL_INTERVAL).with_poll_jitter(Duration::ZERO),
        )
    }

    /// The same worker built with settings the test names itself, for the two rules whose oracle is
    /// a setting: a ceiling or a jitter read back out of the config is the value the scheduler takes
    /// it from, so the comparison would hold whatever that value became.
    fn worker_over_jobs_with_config(
        job_defs: Vec<JobDefinition>,
        storage: Arc<dyn Storage>,
        config: WorkerConfig,
    ) -> Worker {
        Worker::new(
            Arc::new(JobRegistry::new(job_defs).expect("the test registry must build")),
            storage,
            config,
            Arc::new(NoopMetrics),
            None,
            Arc::new(IgnoredIterations),
        )
    }

    /// How long from now the worker will wait before polling `job_code` again.
    fn scheduled_delay(worker: &Worker, job_code: &JobCode) -> Duration {
        worker
            .job_cache
            .read()
            .get(job_code)
            .expect("the job must have been scheduled")
            .next_poll
            .saturating_duration_since(std::time::Instant::now())
    }

    fn assert_delay_near(actual: Duration, expected: Duration, what: &str) {
        assert!(
            actual <= expected && actual + SCHEDULING_SLACK >= expected,
            "{what}: got {actual:?}, expected about {expected:?}"
        );
    }

    #[test]
    fn a_pass_with_work_schedules_the_next_poll_at_the_base_interval() {
        let job_def = idle_job("worked_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);
        worker.schedule_next_poll(&job_code, JobPassOutcome::Processed);

        assert_delay_near(
            scheduled_delay(&worker, &job_code),
            BASE_POLL_INTERVAL,
            "a pass that moved the job must schedule the next poll at the base interval",
        );
    }

    /// The backoff step a job carries is dropped back to the base interval by a pass that moved it,
    /// and only the next stalled pass reads that step: without the reset, a job that was idle for a
    /// while would go on backing off from where it left off, however much work it has since done.
    #[test]
    fn a_pass_with_work_resets_the_backoff_step_of_its_job() {
        let job_def = idle_job("resumed_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);
        worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);
        worker.schedule_next_poll(&job_code, JobPassOutcome::Processed);
        worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);

        assert_delay_near(
            scheduled_delay(&worker, &job_code),
            BASE_POLL_INTERVAL * 2,
            "the stalled pass must back off from the base interval the pass with work restored",
        );
    }

    #[test]
    fn a_stalled_pass_doubles_the_interval_of_that_job() {
        let job_def = idle_job("idle_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);

        assert_delay_near(
            scheduled_delay(&worker, &job_code),
            BASE_POLL_INTERVAL * 2,
            "a stalled pass must back the job's own interval off",
        );
    }

    /// Ceiling the two tests below name for themselves, so what they compare against is a value of
    /// the test rather than one the scheduler derives from the same config it reads. Three times
    /// [`BASE_POLL_INTERVAL`], so the second stalled pass is already held down to it.
    const NAMED_CEILING: Duration = Duration::from_millis(300);

    /// Settings whose backoff stops at [`NAMED_CEILING`].
    fn config_capped_at_the_named_ceiling() -> WorkerConfig {
        config_polled_every(BASE_POLL_INTERVAL)
            .with_max_poll_interval(NAMED_CEILING)
            .expect("a ceiling above the base interval is accepted")
            .with_poll_jitter(Duration::ZERO)
    }

    /// The ceiling bounds the job's own step, not just the formula: a scheduler that doubled the
    /// step itself would push a quiet job past the rate its pool was configured to poll at.
    #[test]
    fn the_backoff_step_stops_at_the_ceiling_the_config_names() {
        let job_def = idle_job("capped_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs_with_config(
            vec![job_def],
            Arc::new(InMemoryStorage::new()),
            config_capped_at_the_named_ceiling(),
        );

        // Four passes: the step reaches the ceiling on the second and has to stay there for the
        // two after it.
        for _ in 0..4 {
            worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);
        }

        assert_delay_near(
            scheduled_delay(&worker, &job_code),
            NAMED_CEILING,
            "the backoff must stop at the ceiling the config was given",
        );
    }

    /// The backoff of one job must not reach another: a pool running a busy job beside an idle one
    /// used to hold both at the base interval, and holding both at the ceiling would be the same
    /// mistake mirrored.
    #[test]
    fn the_backoff_of_an_idle_job_does_not_reach_a_busy_one() {
        let idle = idle_job("idle_neighbour");
        let busy = idle_job("busy_neighbour");
        let idle_code = idle.code().clone();
        let busy_code = busy.code().clone();
        let worker = worker_over_jobs(vec![idle, busy], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(&idle_code, JobPassOutcome::Stalled);
        worker.schedule_next_poll(&idle_code, JobPassOutcome::Stalled);
        worker.schedule_next_poll(&busy_code, JobPassOutcome::Processed);

        assert_delay_near(
            scheduled_delay(&worker, &busy_code),
            BASE_POLL_INTERVAL,
            "the busy job keeps the base interval",
        );
        assert!(
            scheduled_delay(&worker, &idle_code) > BASE_POLL_INTERVAL,
            "the idle job must have backed off independently"
        );
    }

    /// The whole point of the change: a completed job costs nothing until its next iteration is
    /// allowed to begin.
    #[test]
    fn a_completed_job_is_polled_when_its_next_iteration_is_due() {
        let job_def = idle_job("scheduled_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(
            &job_code,
            JobPassOutcome::NextIterationDueAt(Utc::now() + chrono::Duration::seconds(30)),
        );

        assert_delay_near(
            scheduled_delay(&worker, &job_code),
            Duration::from_secs(30),
            "a job due in thirty seconds must not be polled before then",
        );
    }

    /// A moment already past is polled at once: the job is due, and the pass that starts its
    /// iteration is the one thing that can move it.
    #[test]
    fn a_due_moment_in_the_past_schedules_an_immediate_poll() {
        let job_def = idle_job("overdue_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(
            &job_code,
            JobPassOutcome::NextIterationDueAt(Utc::now() - chrono::Duration::seconds(30)),
        );

        assert_eq!(
            scheduled_delay(&worker, &job_code),
            Duration::ZERO,
            "an overdue job must not be made to wait"
        );
    }

    /// Moment the job below is due at: inside the base interval, which is what the regression was
    /// about, and far enough from it that the slack of the comparison cannot bridge the two.
    const DUE_SOONER_THAN_BASE: Duration = Duration::from_millis(10);

    /// Regression: the delay used to be raised to the base interval, so a job whose iterations come
    /// round faster than the pool polls started every one of them late, by the difference.
    #[test]
    fn a_moment_due_sooner_than_the_base_interval_is_not_delayed_to_it() {
        let job_def = idle_job("soon_due_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(
            &job_code,
            JobPassOutcome::NextIterationDueAt(
                Utc::now() + chrono::Duration::from_std(DUE_SOONER_THAN_BASE).expect("the test delay must convert"),
            ),
        );

        assert_delay_near(
            scheduled_delay(&worker, &job_code),
            DUE_SOONER_THAN_BASE,
            "a job due before the next base interval must be polled when it is due",
        );
    }

    /// A moment an executor sets through `JobHandle::set_next_start_at` passes no boundary check,
    /// unlike a configured interval, and the scheduler adds it to an `Instant`, which panics on
    /// overflow. The furthest moment a `DateTime` can hold is therefore scheduled as the longest
    /// interval a worker accepts.
    #[test]
    fn a_moment_beyond_the_longest_accepted_interval_is_scheduled_at_it() {
        let job_def = idle_job("far_future_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(&job_code, JobPassOutcome::NextIterationDueAt(DateTime::<Utc>::MAX_UTC));

        assert_delay_near(
            scheduled_delay(&worker, &job_code),
            WorkerConfig::LONGEST_ACCEPTED_POLL_INTERVAL,
            "a moment past the accepted range must be capped at it",
        );
    }

    /// The counterpart of the reset a pass with work makes: a pass that found the next iteration
    /// scheduled moved nothing either, but it learned exactly when the job is worth polling again,
    /// so the step it carried into that pass has answered for nothing and must not survive it.
    #[test]
    fn a_pass_that_found_the_next_iteration_scheduled_resets_the_backoff_step() {
        let job_def = idle_job("rescheduled_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);
        worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);
        worker.schedule_next_poll(
            &job_code,
            JobPassOutcome::NextIterationDueAt(Utc::now() + chrono::Duration::seconds(30)),
        );
        worker.schedule_next_poll(&job_code, JobPassOutcome::Stalled);

        assert_delay_near(
            scheduled_delay(&worker, &job_code),
            BASE_POLL_INTERVAL * 2,
            "the stalled pass must back off from the base interval the scheduled pass restored",
        );
    }

    #[test]
    fn an_exhausted_job_is_not_polled_again() {
        let job_def = idle_job("exhausted_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(&job_code, JobPassOutcome::IterationBudgetSpent);

        assert!(
            !worker.should_poll_job(&job_code),
            "an exhausted job must not be polled"
        );
    }

    /// The gate is what turns a scheduled moment into a pass that never reaches storage, so both
    /// sides of the moment are asserted: a job scheduled ahead is skipped, one whose moment has
    /// passed is polled.
    #[test]
    fn a_job_is_polled_only_once_its_scheduled_moment_has_passed() {
        let job_def = idle_job("gated_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs(vec![job_def], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(
            &job_code,
            JobPassOutcome::NextIterationDueAt(Utc::now() + chrono::Duration::seconds(30)),
        );
        assert!(
            !worker.should_poll_job(&job_code),
            "a job due in thirty seconds must not be polled yet"
        );

        worker.schedule_next_poll(
            &job_code,
            JobPassOutcome::NextIterationDueAt(Utc::now() - chrono::Duration::seconds(1)),
        );
        assert!(
            worker.should_poll_job(&job_code),
            "a job whose moment has passed must be polled"
        );
    }

    /// The wait is what turns per-job scheduling into saved requests: a worker that kept waking on
    /// a fixed interval would poll nothing and still burn a pass.
    #[test]
    fn the_wait_lasts_until_the_nearest_scheduled_poll() {
        let near = idle_job("near_job");
        let far = idle_job("far_job");
        let near_code = near.code().clone();
        let far_code = far.code().clone();
        let worker = worker_over_jobs(vec![near, far], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(
            &far_code,
            JobPassOutcome::NextIterationDueAt(Utc::now() + chrono::Duration::seconds(30)),
        );
        worker.schedule_next_poll(&near_code, JobPassOutcome::Processed);

        assert_delay_near(
            worker.calculate_wait_duration(),
            BASE_POLL_INTERVAL,
            "the wait must end when the nearest job is due",
        );
    }

    /// A job the worker has not polled yet has no scheduled moment, so it must not be waited past.
    #[test]
    fn an_unpolled_job_keeps_the_base_wait() {
        let scheduled = idle_job("scheduled_neighbour");
        let unpolled = idle_job("unpolled_neighbour");
        let scheduled_code = scheduled.code().clone();
        let worker = worker_over_jobs(vec![scheduled, unpolled], Arc::new(InMemoryStorage::new()));

        worker.schedule_next_poll(
            &scheduled_code,
            JobPassOutcome::NextIterationDueAt(Utc::now() + chrono::Duration::seconds(30)),
        );

        assert_delay_near(
            worker.calculate_wait_duration(),
            BASE_POLL_INTERVAL,
            "a job that was never polled must be polled on the next step",
        );
    }

    /// Jitter of the wait below: half the base interval, which is orders of magnitude above the
    /// time the draws themselves take, so a wait that was not spread cannot pass for a spread one.
    const WAIT_JITTER: Duration = Duration::from_millis(50);

    /// The jitter is not cosmetic: it is the only thing that spreads the moment every worker of
    /// every pool wakes to one scheduled job, and all but one of them pay a rejected write for
    /// waking together (see [`WorkerConfig::poll_jitter`]).
    ///
    /// One job is left unpolled, which is what makes the wait the configured interval itself rather
    /// than the remainder until a scheduled moment: the remainder is measured against the clock on
    /// every call, so the draws would differ with no jitter at all.
    #[test]
    fn the_wait_between_passes_is_spread_by_the_jitter() {
        let scheduled = idle_job("jittered_neighbour");
        let unpolled = idle_job("unpolled_jittered_neighbour");
        let scheduled_code = scheduled.code().clone();
        let worker = worker_over_jobs_with_config(
            vec![scheduled, unpolled],
            Arc::new(InMemoryStorage::new()),
            config_polled_every(BASE_POLL_INTERVAL).with_poll_jitter(WAIT_JITTER),
        );
        worker.schedule_next_poll(&scheduled_code, JobPassOutcome::Processed);

        let waits: Vec<Duration> = (0..JITTER_DRAWS).map(|_| worker.calculate_wait_duration()).collect();

        for wait in &waits {
            assert!(
                (BASE_POLL_INTERVAL..BASE_POLL_INTERVAL + WAIT_JITTER).contains(wait),
                "a wait of {wait:?} is outside the jitter of {WAIT_JITTER:?} on top of \
                 {BASE_POLL_INTERVAL:?}"
            );
        }
        assert!(
            waits.iter().any(|wait| *wait != waits[0]),
            "every wait was {:?}, so the workers were not spread at all",
            waits[0]
        );
    }

    #[test]
    fn a_worker_whose_jobs_are_all_exhausted_waits_the_ceiling() {
        let job_def = idle_job("spent_job");
        let job_code = job_def.code().clone();
        let worker = worker_over_jobs_with_config(
            vec![job_def],
            Arc::new(InMemoryStorage::new()),
            config_capped_at_the_named_ceiling(),
        );

        worker.schedule_next_poll(&job_code, JobPassOutcome::IterationBudgetSpent);

        assert_delay_near(
            worker.calculate_wait_duration(),
            NAMED_CEILING,
            "a worker with nothing to poll must wait the ceiling it was given",
        );
    }

    /// Runs one pass of a worker over a job whose only task `executor` refuses, and asserts the
    /// refusal was recorded as a failure rather than honoured as a cancellation.
    async fn assert_task_failed_by(job_code: &str, executor: Arc<dyn TaskExecutor>) {
        let job_def = job_running(job_code, UNCANCELLED_TASK_TIMEOUT, executor);
        let job_code = job_def.code().clone();
        let storage = Arc::new(InMemoryStorage::new());
        let worker = worker_running(job_def, Arc::clone(&storage) as Arc<dyn Storage>)
            .expect("the test worker must be constructible");
        let cancel_token = CancellationToken::new();

        let outcome = tokio::time::timeout(PROGRESS_TIMEOUT, worker.process_job(&job_code, &cancel_token))
            .await
            .expect("the pass must finish without waiting for the deadline");

        assert_eq!(
            outcome,
            JobPassOutcome::Processed,
            "a pass that failed a task is not an idle one"
        );
        let job = storage
            .get_job(&job_code, &cancel_token)
            .await
            .expect("the failed task must have been persisted");
        let task = job.tasks_as_iter().next().expect("the iteration must still hold its task");
        assert!(task.is_failed(), "got status {}", task.status());
        assert!(
            task.error_msg().contains("without a cancellation"),
            "got: {}",
            task.error_msg()
        );
    }
}
