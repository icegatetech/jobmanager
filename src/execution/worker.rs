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
    Error, InternalError, Job, JobCode, JobError, JobHandle, JobRegistry, JobStatus, MetricsSink, Retrier,
    RetrierConfig, RetryStep, Storage, StorageError, TaskCode, TaskContext, TaskOutcome, TaskPickup, TaskResult,
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
    const DEFAULT_MAX_POLL_INTERVAL: Duration = Duration::from_secs(2);
    const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(200);
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
    /// without ever pausing - or if it is above a ceiling already named through
    /// [`Self::with_max_poll_interval`], which would turn the backoff into a speed-up.
    pub fn with_poll_interval(mut self, interval: Duration) -> Result<Self, Error> {
        if interval.is_zero() {
            return Err(Error::Other("poll interval must be positive".to_string()));
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
    /// backoff into a speed-up.
    pub fn with_max_poll_interval(mut self, ceiling: Duration) -> Result<Self, Error> {
        if ceiling < self.poll_interval {
            return Err(Error::Other(
                "max poll interval must not be below the poll interval".to_string(),
            ));
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
    pub const fn poll_interval(&self) -> Duration {
        self.poll_interval
    }

    /// Upper bound of the random delay added to each poll.
    pub const fn poll_jitter(&self) -> Duration {
        self.poll_jitter
    }

    /// Ceiling the poll interval backs off to while there is no work.
    ///
    /// A ceiling nobody named follows the poll interval whenever that is the larger of the two, so
    /// a job polled less often than the default ceiling backs off upwards rather than down to it.
    pub fn max_poll_interval(&self) -> Duration {
        self.max_poll_interval
            .unwrap_or_else(|| Self::DEFAULT_MAX_POLL_INTERVAL.max(self.poll_interval))
    }

    /// Retry policy a worker applies to its storage operations.
    pub const fn retrier_config(&self) -> &RetrierConfig {
        &self.retrier_config
    }
}

struct JobCacheEntry {
    next_poll: std::time::Instant,
    exhausted: bool, // true if job reached maxIterations
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

        let mut poll_interval = self.config.poll_interval();
        let mut wait_duration = poll_interval;

        loop {
            tokio::select! {
                () = cancel_token.cancelled() => {
                    info!("Stopping worker {}", self.id);
                    return Ok(());
                }
                () = sleep(wait_duration) => {}
            }

            let work_done = self.process_jobs(&cancel_token).await;

            poll_interval = Self::calculate_poll_interval(&self.config, poll_interval, work_done);
            wait_duration = Self::calculate_wait_duration(&self.config, poll_interval);
        }
    }

    /// Poll interval for the pass after one that ended with `work_done`: the configured base when
    /// there was work, and twice `last_poll_interval` - capped at the configured maximum - when
    /// there was none.
    fn calculate_poll_interval(config: &WorkerConfig, last_poll_interval: Duration, work_done: bool) -> Duration {
        if work_done {
            config.poll_interval()
        } else {
            std::cmp::min(last_poll_interval * 2, config.max_poll_interval())
        }
    }

    /// How long a worker waits before its next pass: `poll_interval` plus a random share of the
    /// configured jitter, which is what keeps workers from polling in lockstep. A zero jitter adds
    /// nothing, so the wait is the poll interval itself.
    fn calculate_wait_duration(config: &WorkerConfig, poll_interval: Duration) -> Duration {
        let max_jitter_nanos = u64::try_from(config.poll_jitter().as_nanos()).unwrap_or(u64::MAX);
        let jitter = if max_jitter_nanos == 0 {
            Duration::ZERO
        } else {
            Duration::from_nanos(rand::rng().random_range(0..max_jitter_nanos))
        };

        poll_interval + jitter
    }

    async fn process_jobs(&self, cancel_token: &CancellationToken) -> bool {
        let job_codes = self.job_registry.list_jobs();
        let mut work_done = false;

        for job_code in job_codes {
            if self.process_job(&job_code, cancel_token).await {
                work_done = true;
                debug!("Job processed: {}", job_code);
                continue;
            }
            debug!("Job processing skipped: {}", job_code);
        }

        work_done
    }

    async fn process_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> bool {
        if !self.should_poll_job(job_code) {
            return false;
        }

        let job = match self.storage.get_job(job_code, cancel_token).await {
            Ok(job) => job,
            Err(StorageError::NotFound(_)) => match self.create_new_job(job_code, cancel_token).await {
                Ok(job) => job,
                Err(InternalError::Cancelled) => {
                    debug!("Job creation cancelled");
                    return false;
                }
                Err(e) => {
                    error!("Failed to create new job {}: {}", job_code, e);
                    return false;
                }
            },
            // go to process job
            Err(StorageError::Cancelled) => {
                debug!("Job processing cancelled");
                return false;
            }
            Err(e) => {
                error!("Failed to get job {} for processing: {}", job_code, e);
                return false;
            }
        };

        self.update_cache(job_code.clone(), false);

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
    async fn try_process_job(&self, mut job: Job, cancel_token: &CancellationToken) -> bool {
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
        let ready_for_next = job.is_ready_to_next_iteration();
        debug!(
            status = %job.status(),
            started_at = %job.started_at(),
            completed_at = ?job.completed_at(),
            next_start_at = ?job.next_start_at(),
            ready_for_next,
            "Evaluating job scheduling on pickup"
        );

        if ready_for_next {
            job = match self.start_new_job_iteration(job, cancel_token).await {
                Ok(job) => job,
                Err(InternalError::Cancelled) => {
                    debug!("Job iteration start cancelled");
                    return false;
                }
                Err(e) => {
                    error!("Failed to start job iteration: {}", e);
                    return true;
                }
            };
        } else if job.is_processed() && job.is_iteration_limit_reached() {
            self.update_cache(job.code().clone(), true);
            return false;
        } else if job.is_processed() {
            debug!(
                started_at = %job.started_at(),
                next_start_at = ?job.next_start_at(),
                "Job completed; next iteration not yet due, waiting for schedule"
            );
        }

        if job.is_ready_for_processing() {
            debug!(
                started_at = %job.started_at(),
                "Job ready for processing"
            );
            self.pick_and_execute_task(job, cancel_token).await.unwrap_or_else(|e| {
                if matches!(e, InternalError::Cancelled) {
                    debug!("Job processing cancelled");
                    return false;
                }
                error!("Failed to execute task: {}", e);
                true
            })
        } else {
            false
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

    fn update_cache(&self, job_code: JobCode, exhausted: bool) {
        let mut cache = self.job_cache.write();
        cache.insert(
            job_code,
            JobCacheEntry {
                next_poll: std::time::Instant::now() + self.config.poll_interval(),
                exhausted,
            },
        );
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
            Worker::calculate_wait_duration(&config, Duration::from_millis(100)),
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
            .map(|_| Worker::calculate_wait_duration(&config, poll_interval))
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
    fn a_pass_that_found_work_polls_again_at_the_configured_interval() {
        let config = config_with_jitter(Duration::ZERO);

        assert_eq!(
            Worker::calculate_poll_interval(&config, Duration::from_millis(800), true),
            config.poll_interval()
        );
    }

    #[test]
    fn a_pass_without_work_doubles_the_poll_interval() {
        let config = config_with_jitter(Duration::ZERO);

        assert_eq!(
            Worker::calculate_poll_interval(&config, Duration::from_millis(100), false),
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
            Worker::calculate_poll_interval(&config, Duration::from_millis(200), false),
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

    /// The default ceiling is two seconds, so a pool polled less often than that would otherwise be
    /// held to a setting its caller never named - and its backoff would shorten the wait instead of
    /// lengthening it.
    #[test]
    fn an_unnamed_maximum_poll_interval_follows_a_poll_interval_above_the_default() {
        let config = config_polled_every(Duration::from_secs(10));

        assert_eq!(config.max_poll_interval(), Duration::from_secs(10));
    }

    /// Below the default the ceiling stays where it is: the backoff is what lets a worker polled
    /// often fall back to a rare poll while there is no work.
    #[test]
    fn an_unnamed_maximum_poll_interval_keeps_the_default_above_the_poll_interval() {
        let config = config_polled_every(Duration::from_millis(10));

        assert_eq!(config.max_poll_interval(), WorkerConfig::new().max_poll_interval());
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

        let work_done = tokio::time::timeout(
            PROGRESS_TIMEOUT,
            worker.process_job(&job_code, &CancellationToken::new()),
        )
        .await
        .expect("the deadline must release the executor and let the pass finish");

        assert!(
            work_done,
            "a pass released by the task's own deadline must not be counted as an idle one"
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

    /// Runs one pass of a worker over a job whose only task `executor` refuses, and asserts the
    /// refusal was recorded as a failure rather than honoured as a cancellation.
    async fn assert_task_failed_by(job_code: &str, executor: Arc<dyn TaskExecutor>) {
        let job_def = job_running(job_code, UNCANCELLED_TASK_TIMEOUT, executor);
        let job_code = job_def.code().clone();
        let storage = Arc::new(InMemoryStorage::new());
        let worker = worker_running(job_def, Arc::clone(&storage) as Arc<dyn Storage>)
            .expect("the test worker must be constructible");
        let cancel_token = CancellationToken::new();

        let work_done = tokio::time::timeout(PROGRESS_TIMEOUT, worker.process_job(&job_code, &cancel_token))
            .await
            .expect("the pass must finish without waiting for the deadline");

        assert!(work_done, "a pass that failed a task is not an idle one");
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
