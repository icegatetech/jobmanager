use std::{any::Any, collections::HashMap, future::Future, panic::AssertUnwindSafe, sync::Arc};

use chrono::{DateTime, Utc};
use futures_util::FutureExt;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use tokio::time::{Duration, sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::core::error::{InternalError, JobError};
use crate::core::job::{IterationStatus, IterationVerdict, Job, JobCode, NextIterationStatus, TaskPickup};
use crate::core::registry::JobRegistry;
use crate::core::task::{Task, TaskCode, TaskResolution, TaskRetry};
use crate::execution::executor::{TaskOutcome, TaskResult};
use crate::execution::job_cleaner::JobIterationStarted;
use crate::execution::job_handle::{JobHandle, JobHandleImpl, JobHandleState};
use crate::execution::task_context::TaskContext;
use crate::infra::metrics::MetricsSink;
use crate::infra::retrier::{Retrier, RetrierConfig, RetryStep};
use crate::storage::{Storage, StorageError};

struct JobMergeContext<'a> {
    current_job: &'a Job,
    saved_job: Job,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SaveOutcome {
    Saved,
    /// Another worker got there first, so this worker's state was dropped rather than written.
    JobStolen,
    TaskStolen,
}

enum MergeDecision {
    Retry(Job),
    Done(Job, SaveOutcome),
}

/// What the run of an executor left: the state its job handle held, and how the run ended.
struct ExecutorRun {
    job: Job,
    result: ExecutorResult,
}

/// How the run of an executor ended.
enum ExecutorResult {
    /// The executor returned an outcome, or panicked: what it says is recorded on the task.
    Returned(Result<TaskResult, Box<dyn Any + Send>>),
    /// The task's deadline released the executor: the task stays as it is for another worker to
    /// take over, and nothing is recorded.
    ReleasedByDeadline,
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

/// What every worker is given alike.
#[derive(Clone)]
pub(crate) struct WorkerContext {
    pub(crate) job_registry: Arc<JobRegistry>,
    pub(crate) storage: Arc<dyn Storage>,
    pub(crate) metrics: Arc<dyn MetricsSink>,
    pub(crate) iteration_notifier: Option<mpsc::Sender<JobIterationStarted>>,
    pub(crate) finished_iterations: Arc<dyn FinishedIterationSink>,
}

/// A task whose start [`Worker::start_task`] wrote, together with the state that write stored. The
/// state is the worker's alone: whoever holds a started task only hands it back to
/// [`Worker::execute_task`].
pub(crate) struct StartedTask {
    // Do not publish; it needs to be passed to `execution` in `Worker`.
    job: Job,
    task_id: Uuid,
}

impl StartedTask {
    pub(crate) const fn task_id(&self) -> Uuid {
        self.task_id
    }

    /// The deadline the written start set on the task.
    ///
    /// # Errors
    ///
    /// Returns [`JobError::TaskNotFound`] if the stored state does not hold the started task.
    pub(crate) fn deadline_at(&self) -> Result<Option<DateTime<Utc>>, JobError> {
        self.job.find_task(&self.task_id).map(Task::deadline_at)
    }
}

/// What the state an execution ended with leaves of the job's iteration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExecutionEnd {
    /// The execution's own write closed the iteration; the next one stands at this status.
    IterationClosed(NextIterationStatus),
    /// The iteration is still open.
    TaskEnded {
        /// Whether a task may be started at once; a failed one still within its retry pause is
        /// not one.
        has_pickable_task_left: bool,
    },
}

/// How [`Worker::start_task`] ended.
// Moved once into the pool's report; boxing the started task would add an allocation to every task
// start to shrink that one move.
#[allow(clippy::large_enum_variant)]
pub(crate) enum TaskStart {
    /// The worker wrote the start of a task, which it executes when the pool asks.
    Started {
        started_task: StartedTask,
        /// Whether the state that start wrote holds another task that may be started at once; a
        /// failed one still within its retry pause is not one.
        has_pickable_task_left: bool,
    },
    /// The iteration the job is in holds no task this worker may start: every one left is running,
    /// waits for another, or waits out its retry pause.
    NoTaskAvailable {
        /// The earliest moment a failed task of the iteration may be started again; `None` when no
        /// failed task waits for one.
        retry_at: Option<DateTime<Utc>>,
    },
    /// The iteration the job is in is closed - it was read so, or this worker's own write of its
    /// verdict closed it - and the next one stands at this status.
    IterationClosed(NextIterationStatus),
    /// Another worker got the task, or the verdict, this worker was writing.
    LostToAnotherWorker,
    /// The worker moved nothing: a storage call failed or was cancelled, or the state it got back
    /// had nothing left for it.
    Stalled,
}

impl TaskStart {
    pub(crate) fn into_started_task(self) -> Option<StartedTask> {
        match self {
            Self::Started { started_task, .. } => Some(started_task),
            Self::NoTaskAvailable { .. } | Self::IterationClosed(_) | Self::LostToAnotherWorker | Self::Stalled => None,
        }
    }
}

/// Where a job's iteration stands once a worker moved it on as far as its state allows.
// Lives only for the step from the move to the task start; boxing the job would add an allocation
// to every task start to shrink that one move.
#[allow(clippy::large_enum_variant)]
enum IterationAdvance {
    /// The iteration is open, and a task of it may be started.
    Open(Job),
    /// The iteration is closed, and the next one stands at this status.
    Closed(NextIterationStatus),
    /// The worker moved nothing: a storage call failed or was cancelled, or the state it got back
    /// had nothing left for it.
    Stalled,
}

/// Starts at most one task of the job a [`WorkerPool`](crate::execution::worker_pool::WorkerPool)
/// hands it, and executes a started task when the pool asks. A single `Worker` does one of the two
/// at a time; concurrency across tasks is achieved by running multiple workers (see
/// [`JobsManagerConfig::worker_count`](crate::execution::jobs_manager::JobsManagerConfig::worker_count)).
pub(crate) struct Worker {
    id: Uuid,
    job_registry: Arc<JobRegistry>,
    storage: Arc<dyn Storage>,
    retrier: Retrier,
    metrics: Arc<dyn MetricsSink>,
    iteration_notifier: Option<mpsc::Sender<JobIterationStarted>>,
    finished_iterations: Arc<dyn FinishedIterationSink>,
    // TODO(med): combine metrics, iteration_notifier, finished_iterations so that the worker simply publishes the event, and subscribers process the event themselves.
}

impl Worker {
    pub fn new(id: Uuid, retrier_config: RetrierConfig, context: WorkerContext) -> Self {
        let retrier = Retrier::new(retrier_config);
        let WorkerContext {
            job_registry,
            storage,
            metrics,
            iteration_notifier,
            finished_iterations,
        } = context;

        Self {
            id,
            job_registry,
            storage,
            retrier,
            metrics,
            iteration_notifier,
            finished_iterations,
        }
    }

    /// Reads the job, moves it on as far as its state allows, and writes the start of at most one
    /// task; the execution is left to [`Self::execute_task`].
    #[tracing::instrument(
        skip(self, cancel_token),
        fields(
            worker_id = %self.id,
            job_code = %job_code,
            job_id = tracing::field::Empty,
            job_start_at = tracing::field::Empty,
            iter = tracing::field::Empty,
            otel.name = %format!("start_task-{job_code}")
        )
    )]
    pub(crate) async fn start_task(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> TaskStart {
        let job = match self.storage.get_job(job_code, cancel_token).await {
            Ok(job) => job,
            Err(StorageError::NotFound(_)) => match self.create_new_job(job_code, cancel_token).await {
                Ok(job) => job,
                Err(InternalError::Cancelled) => {
                    debug!("Job creation cancelled");
                    return TaskStart::Stalled;
                }
                Err(e) => {
                    error!("Failed to create new job {}: {}", job_code, e);
                    return TaskStart::Stalled;
                }
            },
            // go to process job
            Err(StorageError::Cancelled) => {
                debug!("Job processing cancelled");
                return TaskStart::Stalled;
            }
            Err(e) => {
                error!("Failed to get job {} for processing: {}", job_code, e);
                return TaskStart::Stalled;
            }
        };
        // Recorded as read, before the iteration moves on: what this start found, not what it left.
        tracing::Span::current()
            .record("job_id", tracing::field::display(job.id()))
            .record("job_start_at", tracing::field::display(job.started_at()))
            .record("iter", job.iter_num());

        let job = match self.advance_job_iteration(job, cancel_token).await {
            IterationAdvance::Open(job) => job,
            IterationAdvance::Closed(next_iteration_status) => {
                return TaskStart::IterationClosed(next_iteration_status);
            }
            IterationAdvance::Stalled => return TaskStart::Stalled,
        };

        match self.start_task_in_iteration(job, cancel_token).await {
            Ok(task_start) => task_start,
            Err(InternalError::Cancelled) => {
                debug!("Job processing cancelled");
                TaskStart::Stalled
            }
            Err(e) => {
                error!("Failed to start a task: {}", e);
                TaskStart::Stalled
            }
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
                Ok(MergeDecision::Done(saved_job, SaveOutcome::JobStolen)) // Someone beat us to it
            })
            .await?;

        if outcome == SaveOutcome::Saved {
            info!("New job '{}' created (id: {}) by worker '{}'", code, job.id(), self.id);
        }

        Ok(job)
    }

    async fn advance_job_iteration(&self, mut job: Job, cancel_token: &CancellationToken) -> IterationAdvance {
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
        let iteration_status = job.find_iteration_status();
        debug!(
            status = %job.status(),
            started_at = %job.started_at(),
            completed_at = ?job.completed_at(),
            next_start_at = ?job.next_start_at(),
            ?iteration_status,
            "Evaluating job scheduling on pickup"
        );

        match iteration_status {
            IterationStatus::InProgress => {}
            IterationStatus::Closed(NextIterationStatus::Ready) => {
                job = match self.start_new_job_iteration(job, cancel_token).await {
                    Ok(job) => job,
                    Err(InternalError::Cancelled) => {
                        debug!("Job iteration start cancelled");
                        return IterationAdvance::Stalled;
                    }
                    Err(e) => {
                        error!("Failed to start job iteration: {}", e);
                        return IterationAdvance::Stalled;
                    }
                };
            }
            IterationStatus::Closed(
                next_iteration_status @ (NextIterationStatus::DueAt(_)
                | NextIterationStatus::BudgetSpent
                | NextIterationStatus::Unscheduled),
            ) => return IterationAdvance::Closed(next_iteration_status),
        }

        // The start above can come back with the state another worker saved, which may already be
        // an iteration this worker has nothing left to do in.
        if !job.is_ready_for_processing() {
            return IterationAdvance::Stalled;
        }

        debug!(started_at = %job.started_at(), "Job ready for processing");
        IterationAdvance::Open(job)
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
                Ok(MergeDecision::Done(saved_job, SaveOutcome::JobStolen)) // Someone beat us to it
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

    /// Picks a task of the job's open iteration and writes its start; the execution is left to the
    /// caller, outside the claim.
    async fn start_task_in_iteration(
        &self,
        mut job: Job,
        cancel_token: &CancellationToken,
    ) -> Result<TaskStart, InternalError> {
        // TODO(low): think about what to do here, likely we have invalid job state
        let task_id = match job.pick_task_to_execute(&self.id)? {
            TaskPickup::Ready(task_id) => task_id,
            TaskPickup::Waiting { retry_at } => {
                debug!("Tasks for job {} not found", job.code());
                return Ok(TaskStart::NoTaskAvailable { retry_at });
            }
            TaskPickup::IterationSettled => return self.save_settled_job_iteration(job, cancel_token).await,
        };
        let task_code = job.get_task(&task_id)?.code().clone();
        let job_code = job.code().clone();

        match job.start_task(&task_id, self.id) {
            Ok(()) => {}
            Err(JobError::TaskWorkerMismatch) => {
                self.metrics.record_task_stolen(&job_code, &task_code, "pick_start_local");
                return Ok(TaskStart::LostToAnotherWorker);
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
                        Ok(MergeDecision::Done(saved_job, SaveOutcome::TaskStolen))
                    }
                    Err(e) => {
                        Err(InternalError::from(e)) // Don't retry
                    }
                }
            })
            .await?;

        if outcome == SaveOutcome::TaskStolen {
            info!("Task was stolen");
            return Ok(TaskStart::LostToAnotherWorker);
        }

        info!("Started processing task '{}' (job iter: {})", task_id, job.iter_num());

        Ok(TaskStart::Started {
            // A walk over the state the write returned: a merge may have brought in tasks the pick
            // never saw, and a deadline or a retry pause may have run out while the write was in flight.
            has_pickable_task_left: job.has_pickable_task(),
            started_task: StartedTask { job, task_id },
        })
    }

    /// Executes a task whose start [`Self::start_task`] wrote, and saves the result it ended with.
    /// `None` when the execution was cancelled or failed before it ended with a state; the failure
    /// is logged here.
    #[allow(clippy::map_unwrap_or)]
    #[tracing::instrument(
        skip(self, started_task, cancel_token),
        fields(
            worker_id = %self.id,
            job_code = %started_task.job.code(),
            job_id = %started_task.job.id(),
            iter = started_task.job.iter_num(),
            task_id = %started_task.task_id,
            task_code = %started_task
                .job
                .get_task(&started_task.task_id)
                .map(|task| task.code().clone())
                .unwrap_or_else(|_| TaskCode::new("unknown"))
        )
    )]
    pub(crate) async fn execute_task(
        &self,
        started_task: StartedTask,
        cancel_token: &CancellationToken,
    ) -> Option<ExecutionEnd> {
        let StartedTask { job, task_id } = started_task;
        let execution = async {
            let ExecutorRun { mut job, result } = self.run_task_executor(job, task_id, cancel_token).await?;
            match result {
                ExecutorResult::Returned(outcome) => {
                    let resolution = self.apply_executor_outcome(&mut job, task_id, outcome)?;
                    self.save_task(job, task_id, resolution, cancel_token).await
                }
                // The state returned shows the task takeable: waiting for the poll interval before a
                // takeover that is already due would only delay it.
                ExecutorResult::ReleasedByDeadline => Ok(ExecutionEnd::TaskEnded {
                    has_pickable_task_left: job.has_pickable_task(),
                }),
            }
        };
        match execution.await {
            Ok(execution_end) => Some(execution_end),
            Err(InternalError::Cancelled) => {
                debug!("Task execution cancelled");
                None
            }
            Err(e) => {
                error!("Failed to execute task: {}", e);
                None
            }
        }
    }

    /// Runs the executor of `task_id` until it returns, and takes back the state its job handle
    /// held; [`InternalError::Cancelled`] when the pool's shutdown came while the executor ran.
    async fn run_task_executor(
        &self,
        job: Job,
        task_id: Uuid,
        cancel_token: &CancellationToken,
    ) -> Result<ExecutorRun, InternalError> {
        let task = job.get_task(&task_id)?;
        let executor = self.job_registry.get_task_executor(job.code(), task.code())?;

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

        let job = match Arc::try_unwrap(shared_state) {
            Ok(lock) => lock.into_inner().into_job(),
            // The executor moved the handle into a task that outlived its own call. The handle is
            // already closed, so the escaped copy can no longer mutate anything; copying the state
            // out from under the lock is enough to carry on.
            Err(shared) => {
                warn!("Executor of task '{}' outlived its own call", task_id);
                shared.read().job().clone()
            }
        };

        let end = match outcome {
            // A shutdown was ruled out above, so the executor was released by its own deadline.
            Ok(Ok(TaskOutcome::Cancelled)) if deadline_cancel_token.is_cancelled() => {
                info!("Task '{}' execution cancelled by its deadline", task_id);
                ExecutorResult::ReleasedByDeadline
            }
            outcome => ExecutorResult::Returned(outcome),
        };
        Ok(ExecutorRun { job, result: end })
    }

    /// Records on `task_id` what its executor returned: the task's resolution, or the failure of
    /// its execution.
    fn apply_executor_outcome(
        &self,
        job: &mut Job,
        task_id: Uuid,
        outcome: Result<TaskResult, Box<dyn Any + Send>>,
    ) -> Result<TaskResolution, InternalError> {
        // The resolution rides on the domain accepting it: one it refused is recorded below as a
        // failure of the execution instead.
        let result: Result<TaskResolution, InternalError> = match outcome {
            // The worker closes the task itself, so an executor cannot leave one hanging by
            // forgetting to.
            Ok(Ok(TaskOutcome::Completed(output))) => job
                .complete_task(&task_id, output, self.id)
                .map(|()| TaskResolution::Completed)
                .map_err(InternalError::from),
            // A refusal the executor ruled out repeating: recorded here rather than below, because
            // below is where a refusal that may be repeated is recorded.
            Ok(Ok(TaskOutcome::TerminallyFailed(reason))) => job
                .fail_task(&task_id, &reason, TaskRetry::Never, self.id)
                .map(|rolled_back_tasks| {
                    info!(rolled_back_tasks, "Task '{}' refused terminally: {}", task_id, reason);
                    TaskResolution::Failed
                })
                .map_err(InternalError::from),
            Ok(Ok(TaskOutcome::SkippedBranch(reason))) => job
                .skip_task_by_executor(&task_id, &reason, self.id)
                .map(|()| TaskResolution::Skipped)
                .map_err(InternalError::from),
            // The executor resolved the task through its handle; touching it again would fail, so
            // the resolution is read off the state it left rather than decided here.
            Ok(Ok(TaskOutcome::Deferred)) => {
                Self::check_task_resolution(job, &task_id).and_then(|()| Self::find_task_resolution(job, &task_id))
            }
            // Neither the deadline nor a shutdown released this execution, so the outcome is a
            // broken contract rather than a cancellation - including when the executor cancelled
            // its own token. Honouring it would leave the task held until its deadline runs out, on
            // an attempt already spent, and report a cancellation that never happened.
            Ok(Ok(TaskOutcome::Cancelled)) => Err(InternalError::Other(
                "executor returned Cancelled without a cancellation of its execution: return the outcome of the \
                 work, or select on the token and return Cancelled once it fires"
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

        // The two outcomes are told apart by what they record on the task; what follows is the same
        // for both, because an execution that failed after its executor resolved its own task
        // leaves a resolved task behind, and that result completes the iteration exactly like a
        // successful one.
        let resolution = match result {
            Err(e) => {
                let rolled_back_tasks =
                    job.record_task_execution_failure(&task_id, &e.to_string(), TaskRetry::WhileBudgetLasts, self.id)?;
                info!(rolled_back_tasks, "Task '{}' execution failed: {}", task_id, e);
                // The failure may have found the task already resolved by its own executor, in
                // which case that resolution is what the save carries.
                Self::find_task_resolution(job, &task_id)?
            }
            Ok(resolution) => {
                info!("Task '{}' handled successfully", task_id);
                resolution
            }
        };
        Ok(resolution)
    }

    /// Saves the result of `task_id`, merged into the stored state on a conflict, in the write that
    /// also closes the iteration when the result settles it.
    async fn save_task(
        &self,
        mut job: Job,
        task_id: Uuid,
        resolution: TaskResolution,
        cancel_token: &CancellationToken,
    ) -> Result<ExecutionEnd, InternalError> {
        let save_reason = match resolution {
            TaskResolution::Completed => "save_completed_task",
            TaskResolution::Failed => "save_failed_task",
            TaskResolution::Skipped => "save_skipped_task",
        };
        let worker_id = self.id;
        let task_id_clone = task_id;
        let metrics = self.metrics.clone();
        let task_code_for_metrics = job.find_task(&task_id)?.code().clone();
        let job_code_for_metrics = job.code().clone();

        // The iteration is closed in the write that saves the result of its last task; left to the
        // next poll, the verdict would cost a write of its own.
        //
        // A verdict that cannot be reached must not cost the result: propagating the error with `?`
        // would drop the save, the work was done once, and a task this save does not carry stays
        // `Started` in storage for takeover after takeover to repeat it. An iteration nothing can
        // move is left for a later task start to answer for.
        if let Err(e) = job.try_settle_iteration(&worker_id) {
            error!("Job {} iteration {} left open: {}", job.code(), job.iter_num(), e);
        }

        let (job, outcome) = self
            .save_processed_task(job, &task_id, cancel_token, move |ctx| {
                let JobMergeContext {
                    current_job,
                    mut saved_job,
                } = ctx;
                match saved_job.merge_with_processed_task(current_job, &worker_id, &task_id_clone) {
                    Ok(()) => debug!("Retry to save processed task ({save_reason})"),
                    // The merge carried the result over and left the iteration open, which a later
                    // task start answers for; costing the retry the result it carries would leave
                    // the task `Started` in storage instead.
                    Err(e @ JobError::IterationDeadlock { .. }) => error!(
                        "Job {} iteration {} left open after a merge: {e}",
                        saved_job.code(),
                        saved_job.iter_num()
                    ),
                    // The stored iteration is closed, so it takes no result: the next iteration is
                    // planned from scratch, and the task is lost to the worker that closed it
                    // exactly as a taken-over one is - which is what it is measured as.
                    Err(JobError::IterationAlreadySettled { job_code }) => {
                        metrics.record_task_stolen(&job_code_for_metrics, &task_code_for_metrics, "settled_iteration");
                        info!("Job {job_code} iteration was settled by another worker - the result is dropped");
                        return Ok(MergeDecision::Done(saved_job, SaveOutcome::JobStolen));
                    }
                    Err(JobError::TaskWorkerMismatch) => {
                        metrics.record_task_stolen(&job_code_for_metrics, &task_code_for_metrics, save_reason);
                        debug!("Task has stolen when try to save processed task ({save_reason}) - skip");
                        return Ok(MergeDecision::Done(saved_job, SaveOutcome::TaskStolen));
                    }
                    Err(e) => return Err(InternalError::from(e)),
                }

                // The two arms that reach here repeat the write, so one repetition is one
                // measurement however the merge that led to it ended.
                metrics.record_save_conflict_retry(&job_code_for_metrics, save_reason);
                Ok(MergeDecision::Retry(saved_job))
            })
            .await?;

        if outcome == SaveOutcome::Saved {
            self.job_iteration_settled(&job);
        }

        if outcome == SaveOutcome::Saved
            && let IterationStatus::Closed(next_iteration_status) = job.find_iteration_status()
        {
            return Ok(ExecutionEnd::IterationClosed(next_iteration_status));
        }
        Ok(ExecutionEnd::TaskEnded {
            // A walk of its own: the state saved is what the settling unblocked and what a merge
            // brought in from another worker, and no walk before the save saw either.
            has_pickable_task_left: job.has_pickable_task(),
        })
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
    ) -> Result<(Job, SaveOutcome), InternalError>
    where
        F: for<'a> Fn(JobMergeContext<'a>) -> Result<MergeDecision, InternalError> + Send + Sync,
    {
        let (job, outcome) = self.save_job_state(job, cancel_token, concurrent_modification_handler).await?;

        // Neither outcome put the result in storage, so the task it carries was processed by nobody
        // as far as the stored state is concerned.
        if matches!(outcome, SaveOutcome::TaskStolen | SaveOutcome::JobStolen) {
            info!("Task '{task_id}' did not reach storage ({outcome:?}), so it is not measured as processed");
            return Ok((job, outcome));
        }

        debug!(
            "Job '{}' saved with processed task '{}' (iter: {}, version: {})",
            job.code(),
            task_id,
            job.iter_num(),
            job.version()
        );

        if let Some((task, resolution)) = job
            .find_task(task_id)
            .ok()
            .and_then(|task| task.resolution().map(|resolution| (task, resolution)))
        {
            // Calculate duration if start/complete times are available
            let duration = match (task.completed_at(), task.started_at()) {
                (Some(completed), Some(started)) => completed
                    .signed_duration_since(started)
                    .to_std()
                    .unwrap_or(Duration::from_secs(0)),
                _ => Duration::from_secs(0),
            };

            self.metrics
                .record_task_processed(job.code(), task.code(), resolution, duration);
        }

        Ok((job, outcome))
    }

    /// Persists an iteration the domain has already closed, under the verdict it closed with.
    ///
    /// [`Job::pick_task_to_execute`] settled it; saving that state is what lets the scheduler start
    /// the next iteration, which replans from scratch. A concurrent modification means another
    /// worker advanced the job, so this worker drops its verdict and re-derives it on the next
    /// poll.
    async fn save_settled_job_iteration(
        &self,
        job: Job,
        cancel_token: &CancellationToken,
    ) -> Result<TaskStart, InternalError> {
        let (job, outcome) = self
            .save_job_state(job, cancel_token, |ctx| {
                debug!("Job has concurrent modification when settling the iteration - skip");
                Ok(MergeDecision::Done(ctx.saved_job, SaveOutcome::JobStolen))
            })
            .await?;

        if outcome != SaveOutcome::Saved {
            return Ok(TaskStart::LostToAnotherWorker);
        }
        self.job_iteration_settled(&job);
        match job.find_iteration_status() {
            IterationStatus::InProgress => Err(InternalError::Other(format!(
                "job '{}' iteration {} is open after the write that settled it",
                job.code(),
                job.iter_num()
            ))),
            IterationStatus::Closed(next_iteration_status) => Ok(TaskStart::IterationClosed(next_iteration_status)),
        }
    }

    /// Reports an iteration the domain closed, under the verdict it actually carries.
    ///
    /// Called by the worker whose write closed the iteration, so a verdict is logged and measured
    /// once however many workers read that state afterwards.
    fn job_iteration_settled(&self, job: &Job) {
        let Some(verdict) = job.iteration_verdict() else {
            return;
        };
        match verdict {
            IterationVerdict::Failed => error!(
                "Job {} iteration {} failed - a failure was left unhandled ({})",
                job.code(),
                job.iter_num(),
                job.tasks_as_string()
            ),
            IterationVerdict::Completed => info!("Job {} completed (iter: {})", job.code(), job.iter_num()),
        }

        self.record_job_iteration(job, verdict);
        self.report_finished_iteration(job);
    }

    /// Publishes the end of an iteration to whoever is waiting for it.
    fn report_finished_iteration(&self, job: &Job) {
        self.finished_iterations.record_finished_iteration(job.code(), job.iter_num());
    }

    /// Record the duration of a finished job iteration under the `verdict` it ended with.
    fn record_job_iteration(&self, job: &Job, verdict: IterationVerdict) {
        let duration = job.completed_at().map_or_else(
            || Duration::from_secs(0),
            |completed| {
                completed
                    .signed_duration_since(job.started_at())
                    .to_std()
                    .unwrap_or(Duration::from_secs(0))
            },
        );

        self.metrics.record_job_iteration_complete(job.code(), verdict, duration);
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

    /// The resolution of a task its own executor resolved, which is the state that is about to be
    /// stored rather than the outcome the executor returned.
    fn find_task_resolution(job: &Job, task_id: &Uuid) -> Result<TaskResolution, InternalError> {
        job.find_task(task_id)?
            .resolution()
            .ok_or_else(|| InternalError::Other(format!("task '{task_id}' is left unresolved")))
    }

    /// Checks that the task an executor returned [`TaskOutcome::Deferred`] for is no longer open.
    /// Completing, failing and skipping are the ways to resolve one, so anything else was left for
    /// nobody to close.
    fn check_task_resolution(job: &Job, task_id: &Uuid) -> Result<(), InternalError> {
        if job.find_task(task_id)?.is_resolved() {
            return Ok(());
        }

        Err(InternalError::Other(format!(
            "executor returned Deferred without resolving task '{task_id}': complete it, fail it, or skip its \
         branch through the job handle, or return the outcome instead"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::error::Error;
    use crate::core::job::{JobDefinition, JobDefinitionId, JobStatus, TaskLimits};
    use crate::core::task::{TaskAvailability, TaskDefinition, TaskRef, TaskStatus};
    use crate::execution::executor::{TaskExecutor, task_fn};
    use crate::execution::outcome::JobProgress;
    use crate::infra::metrics::NoopMetrics;
    use crate::storage::in_memory::InMemoryStorage;
    use crate::tests::common::counting_metrics::CountingMetrics;
    use crate::tests::common::storage_wrapper::{ContendingStorage, DeadlockPlantingStorage, SaveRefusingStorage};

    /// Deadline of the task the tests below run, and so how long its executor waits for the token.
    const CANCELLED_TASK_TIMEOUT: Duration = Duration::from_millis(50);

    /// Longest a task start or an execution below may take: two orders of magnitude above the
    /// task's deadline, so a loaded machine does not fail the test, while a cancellation that never
    /// arrives does - with a diagnostic rather than a hung run.
    const PROGRESS_TIMEOUT: Duration = Duration::from_secs(5);

    /// Sink for a worker whose iterations nobody waits for.
    struct IgnoredIterations;

    impl FinishedIterationSink for IgnoredIterations {
        fn record_finished_iteration(&self, _job_code: &JobCode, _iter_num: u64) {}
    }

    /// Whether `progress` names a task another worker may start at once.
    fn is_task_pickable_now(progress: JobProgress) -> bool {
        matches!(progress, JobProgress::TaskPickable)
    }

    fn worker_running(job_def: JobDefinition, storage: Arc<dyn Storage>) -> Result<Worker, Error> {
        worker_measured_by(job_def, storage, Arc::new(NoopMetrics))
    }

    /// The same worker recording into `metrics`, for a test whose measurement is what is asserted.
    fn worker_measured_by(
        job_def: JobDefinition,
        storage: Arc<dyn Storage>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<Worker, Error> {
        Ok(Worker::new(
            Uuid::new_v4(),
            RetrierConfig::default(),
            WorkerContext {
                job_registry: Arc::new(JobRegistry::new(vec![job_def])?),
                storage,
                metrics,
                iteration_notifier: None,
                finished_iterations: Arc::new(IgnoredIterations),
            },
        ))
    }

    /// One task start of `worker` on `job_code`, bounded so a start that never ends fails the test.
    async fn start_task_of(worker: &Worker, job_code: &JobCode, cancel_token: &CancellationToken) -> TaskStart {
        tokio::time::timeout(PROGRESS_TIMEOUT, worker.start_task(job_code, cancel_token))
            .await
            .expect("the task start must finish")
    }

    /// The execution of the task `task_start` started, bounded the same way, as the schedule reads
    /// its end.
    async fn execute_task_of(worker: &Worker, task_start: TaskStart, cancel_token: &CancellationToken) -> JobProgress {
        let TaskStart::Started { started_task, .. } = task_start else {
            panic!("the task start must have started a task");
        };
        let execution_end = tokio::time::timeout(PROGRESS_TIMEOUT, worker.execute_task(started_task, cancel_token))
            .await
            .expect("the execution must finish");
        JobProgress::from_job_execution(execution_end)
    }

    /// A description of one task running `executor`, for the tests below.
    fn job_running(job_code: &str, timeout: Duration, executor: Arc<dyn TaskExecutor>) -> JobDefinition {
        JobDefinition::new(
            JobDefinitionId::new(),
            JobCode::new(job_code),
            vec![(TaskDefinition::new(TaskCode::new(RUNNING_TASK_CODE), timeout), executor)],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )
        .expect("the test description must be legal")
    }

    /// The label a save of a `Deferred` outcome carries names the state the executor left, not the
    /// outcome it returned: an executor that gave up on its branch through the handle stores a
    /// skipped task, and a save labelled as a completion would send every conflict and every
    /// takeover of that execution into the metrics under the wrong phase.
    ///
    /// The break that proves it: returning a fixed `TaskResolution::Completed` from
    /// `Worker::find_task_resolution`.
    #[test]
    fn a_task_its_executor_resolved_is_labelled_by_the_state_it_left() {
        let job_def = job_running(
            "labelled_job",
            CANCELLED_TASK_TIMEOUT,
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        );
        let worker_id = Uuid::from_u128(1);

        for resolution in [
            TaskResolution::Completed,
            TaskResolution::Failed,
            TaskResolution::Skipped,
        ] {
            let mut job = Job::new(&job_def, HashMap::new(), worker_id).expect("the description must build a job");
            let task_id = *job.tasks_as_iter().next().expect("the description declares one task").id();
            job.start_work(&worker_id).expect("a new job starts running");
            job.start_task(&task_id, worker_id).expect("a todo task must start");
            match resolution {
                TaskResolution::Completed => job
                    .complete_task(&task_id, Vec::new(), worker_id)
                    .expect("a started task completes"),
                TaskResolution::Failed => {
                    job.fail_task(&task_id, "refused", TaskRetry::WhileBudgetLasts, worker_id)
                        .map(|_| ())
                        .expect("a started task fails");
                }
                TaskResolution::Skipped => job
                    .skip_task_by_executor(&task_id, "pointless", worker_id)
                    .expect("a started task is skipped"),
            }

            assert_eq!(
                Worker::find_task_resolution(&job, &task_id).expect("the task is resolved"),
                resolution
            );
        }
    }

    /// A cancellation the executor reports after its own deadline passed is not the pool shutting
    /// down - the shutdown is ruled out before the outcome is read. The task stays `Started` past
    /// its deadline, which another worker takes over at once: reported as nothing to take, the
    /// takeover would wait for the poll interval.
    ///
    /// Checked by returning `JobProgress::NothingPickable` for every execution that ended without
    /// an error.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_execution_released_by_its_deadline_leaves_its_task_pickable_at_once() {
        let executor = task_fn(|ctx| async move {
            ctx.cancel_token().cancelled().await;
            Ok(TaskOutcome::Cancelled)
        });
        let job_def = job_running("deadline_cancelled_job", CANCELLED_TASK_TIMEOUT, executor);
        let job_code = job_def.code().clone();
        let worker =
            worker_running(job_def, Arc::new(InMemoryStorage::new())).expect("the test worker must be constructible");
        let cancel_token = CancellationToken::new();

        let task_start = start_task_of(&worker, &job_code, &cancel_token).await;
        assert!(
            matches!(task_start, TaskStart::Started { .. }),
            "the task start must start the task"
        );
        let execution_end = execute_task_of(&worker, task_start, &cancel_token).await;

        assert!(
            is_task_pickable_now(execution_end),
            "a task released by its own deadline must be takeable at once, got {execution_end:?}"
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

    /// Stores `job_def`'s first iteration already finished, which is the state a task start reads
    /// as "start the next one": built through the production API, so no state a legal run cannot
    /// reach is persisted.
    async fn store_with_a_finished_iteration(
        job_def: &JobDefinition,
        worker_id: Uuid,
    ) -> Result<Arc<InMemoryStorage>, Box<dyn std::error::Error>> {
        let storage = Arc::new(InMemoryStorage::new());
        let cancel_token = CancellationToken::new();
        let mut job = Job::new(job_def, HashMap::new(), worker_id)?;
        job.start_work(&worker_id)?;
        let TaskPickup::Ready(task_id) = job.pick_task_to_execute(&worker_id)? else {
            return Err("the fixture must leave a task to finish".into());
        };
        job.start_task(&task_id, worker_id)?;
        job.complete_task(&task_id, Vec::new(), worker_id)?;
        job.try_settle_iteration(&worker_id)?;
        storage.save_job(&mut job, &cancel_token).await?;

        Ok(storage)
    }

    /// A task start whose write of the new iteration was refused moved nothing, so it has to back
    /// off like any other that moved nothing. Counted as work done - which is what it used to be -
    /// the worker would keep the base interval and repeat the same refused write at full rate.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_task_start_that_cannot_save_the_iteration_it_started_counts_as_stalled()
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

        let task_start = tokio::time::timeout(PROGRESS_TIMEOUT, worker.start_task(&job_code, &cancel_token))
            .await
            .map_err(|_| "a refused save must fail the task start rather than be retried forever")?;

        assert_eq!(
            refusing_storage.refused_saves(),
            1,
            "the task start must have reached the write of the new iteration, and reached it once"
        );
        assert!(
            matches!(task_start, TaskStart::Stalled),
            "a task start that wrote nothing is not one that moved the job"
        );
        let stored = storage.get_job(&job_code, &cancel_token).await?;
        assert_eq!(stored.iter_num(), 1, "the refused iteration must not be stored");
        assert_eq!(*stored.status(), JobStatus::Completed, "got: {}", stored.status());
        Ok(())
    }

    /// Code of the task every job built by [`job_running`] holds, which is the result the double
    /// below plants its blocked task ahead of.
    const RUNNING_TASK_CODE: &str = "cancelled";

    /// A merge that leaves the iteration unable to progress must not cost the result it carried: the
    /// task the executor finished is written and the iteration is left open for a later task start
    /// to answer for. Dropping the save instead would leave the task `Started` in storage, for
    /// takeover after takeover to repeat work that was already done. The repeated write is measured
    /// like any other retry after a conflict, since the write is repeated for the same reason.
    ///
    /// Two breaks prove it: propagating the deadlock with `?` from the merge handler in
    /// `Worker::execute_task` loses the result, and recording the conflict retry in the `Ok(())` arm
    /// alone leaves this path unmeasured.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_merge_that_leaves_the_iteration_deadlocked_keeps_the_result_it_carried()
    -> Result<(), Box<dyn std::error::Error>> {
        let job_def = job_running(
            "deadlocked_merge_job",
            UNCANCELLED_TASK_TIMEOUT,
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        );
        let job_code = job_def.code().clone();
        let inner: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let storage = Arc::new(DeadlockPlantingStorage::new(
            Arc::clone(&inner),
            TaskCode::new("orphan"),
            TaskCode::new(RUNNING_TASK_CODE),
        ));
        let metrics = Arc::new(CountingMetrics::default());
        let worker = worker_measured_by(
            job_def,
            Arc::clone(&storage) as Arc<dyn Storage>,
            Arc::clone(&metrics) as Arc<dyn MetricsSink>,
        )?;
        let cancel_token = CancellationToken::new();

        let task_start = start_task_of(&worker, &job_code, &cancel_token).await;
        assert!(
            matches!(task_start, TaskStart::Started { .. }),
            "the task start must start the task"
        );
        execute_task_of(&worker, task_start, &cancel_token).await;

        assert_eq!(
            storage.interferences(),
            1,
            "the fixture must have planted the blocked task ahead of the save carrying the result"
        );
        assert_eq!(
            metrics.save_conflict_retries(),
            1,
            "the write repeated after the deadlocked merge is a conflict retry"
        );

        let stored = inner.get_job(&job_code, &cancel_token).await?;
        assert_eq!(
            *stored.status(),
            JobStatus::Running,
            "an iteration nothing can move stays open, got: {}",
            stored.status()
        );
        let processed = stored
            .tasks_as_iter()
            .find(|task| task.code().as_str() == RUNNING_TASK_CODE)
            .ok_or("the stored iteration must still hold the task the executor ran")?;
        assert!(
            processed.is_completed(),
            "the result the merge carried must be stored, got status {}",
            processed.status()
        );
        let planted = stored
            .tasks_as_iter()
            .find(|task| task.code() == storage.planted_task_code())
            .ok_or("the merge must keep the stored task that deadlocked the iteration")?;
        assert_eq!(*planted.status(), TaskStatus::Blocked, "got: {}", planted.status());
        Ok(())
    }

    /// Runs one task start and its execution on a job whose only task `executor` refuses, and
    /// asserts the refusal was recorded as a failure rather than honoured as a cancellation.
    async fn assert_task_failed_by(job_code: &str, executor: Arc<dyn TaskExecutor>) {
        let job_def = job_running(job_code, UNCANCELLED_TASK_TIMEOUT, executor);
        let job_code = job_def.code().clone();
        let storage = Arc::new(InMemoryStorage::new());
        let worker = worker_running(job_def, Arc::clone(&storage) as Arc<dyn Storage>)
            .expect("the test worker must be constructible");
        let cancel_token = CancellationToken::new();

        let task_start = start_task_of(&worker, &job_code, &cancel_token).await;
        assert!(
            matches!(task_start, TaskStart::Started { .. }),
            "the task start must start the task"
        );
        execute_task_of(&worker, task_start, &cancel_token).await;

        let job = storage
            .get_job(&job_code, &cancel_token)
            .await
            .expect("the failed task must have been persisted");
        let task = job.tasks_as_iter().next().expect("the iteration must still hold its task");
        assert!(task.is_failed(), "got status {}", task.status());
        assert!(
            task.resolution_reason().contains("without a cancellation"),
            "got: {}",
            task.resolution_reason()
        );
    }

    /// A job of two tasks running `executor`; the second depends on the first when
    /// `is_second_dependent` is set, and runs alongside it otherwise.
    fn job_of_two_tasks(job_code: &str, is_second_dependent: bool, executor: Arc<dyn TaskExecutor>) -> JobDefinition {
        let definition_id = JobDefinitionId::new();
        let declared_dependencies = if is_second_dependent {
            vec![(
                TaskRef::initial(definition_id, 1),
                vec![TaskRef::initial(definition_id, 0)],
            )]
        } else {
            Vec::new()
        };
        JobDefinition::new(
            definition_id,
            JobCode::new(job_code),
            vec![
                (
                    TaskDefinition::new(TaskCode::new("first"), UNCANCELLED_TASK_TIMEOUT),
                    Arc::clone(&executor),
                ),
                (
                    TaskDefinition::new(TaskCode::new("second"), UNCANCELLED_TASK_TIMEOUT),
                    executor,
                ),
            ],
            Vec::new(),
            declared_dependencies,
            TaskLimits::default(),
        )
        .expect("the test description must be legal")
    }

    /// What decides whether the second task of a fan-out waits for the poll interval. Checked by
    /// reporting `JobProgress::NothingPickable` for every started task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn starting_one_of_two_tasks_reports_a_pickable_task_left() {
        let job_def = job_of_two_tasks("fan_out_job", false, task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }));
        let job_code = job_def.code().clone();
        let worker =
            worker_running(job_def, Arc::new(InMemoryStorage::new())).expect("the test worker must be constructible");

        let task_start = start_task_of(&worker, &job_code, &CancellationToken::new()).await;

        assert!(matches!(task_start, TaskStart::Started { .. }));
        let progress = JobProgress::from_task_start(&task_start);
        assert!(is_task_pickable_now(progress), "got {progress:?}");
    }

    /// Checked by reporting `JobProgress::TaskPickable` for every started task.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn starting_the_only_task_reports_nothing_pickable_left() {
        let job_def = job_running(
            "single_task_job",
            UNCANCELLED_TASK_TIMEOUT,
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        );
        let job_code = job_def.code().clone();
        let worker =
            worker_running(job_def, Arc::new(InMemoryStorage::new())).expect("the test worker must be constructible");

        let task_start = start_task_of(&worker, &job_code, &CancellationToken::new()).await;

        assert!(matches!(task_start, TaskStart::Started { .. }));
        assert_eq!(JobProgress::from_task_start(&task_start), JobProgress::NothingPickable);
    }

    /// The save of a result settles the iteration first, which unblocks the dependent waiting for
    /// it; reported as nothing to take, the dependent would wait for the poll interval.
    ///
    /// Checked by returning `JobProgress::NothingPickable` for every execution.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_execution_whose_result_unblocks_a_dependent_reports_it_pickable() {
        let job_def = job_of_two_tasks(
            "dependent_job",
            true,
            task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
        );
        let job_code = job_def.code().clone();
        let worker =
            worker_running(job_def, Arc::new(InMemoryStorage::new())).expect("the test worker must be constructible");
        let cancel_token = CancellationToken::new();

        let task_start = start_task_of(&worker, &job_code, &cancel_token).await;
        assert!(matches!(task_start, TaskStart::Started { .. }));
        assert_eq!(
            JobProgress::from_task_start(&task_start),
            JobProgress::NothingPickable,
            "the dependent must still be blocked when the first task starts"
        );
        let execution_end = execute_task_of(&worker, task_start, &cancel_token).await;

        assert!(is_task_pickable_now(execution_end), "got {execution_end:?}");
    }

    /// A refusal is retried no sooner than `FAILED_TASK_RETRY_PAUSE`, so the failed task it leaves is not
    /// work to take at once. Checked by dropping the pause from `Task::check_availability_at`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_execution_that_failed_its_task_with_attempts_left_reports_nothing_pickable() {
        let job_def = job_running(
            "refused_job",
            UNCANCELLED_TASK_TIMEOUT,
            task_fn(|_ctx| async { Err("refused".into()) }),
        );
        let job_code = job_def.code().clone();
        let storage = Arc::new(InMemoryStorage::new());
        let worker = worker_running(job_def, Arc::clone(&storage) as Arc<dyn Storage>)
            .expect("the test worker must be constructible");
        let cancel_token = CancellationToken::new();

        let task_start = start_task_of(&worker, &job_code, &cancel_token).await;
        let execution_end = execute_task_of(&worker, task_start, &cancel_token).await;

        let stored = storage
            .get_job(&job_code, &cancel_token)
            .await
            .expect("the refusal must have been persisted");
        let task = stored.tasks_as_iter().next().expect("the iteration must still hold its task");
        assert!(
            matches!(
                task.check_availability_at(Utc::now()),
                TaskAvailability::AwaitingRetry { .. }
            ),
            "the fixture must leave a failed task that may still be retried, got status {}",
            task.status()
        );
        assert_eq!(execution_end, JobProgress::NothingPickable);
    }

    /// Deadline and maximum lifetime of the task held elsewhere below, which the task start finds
    /// outlived.
    const OUTLIVED_TASK_LIFETIME: Duration = Duration::from_millis(50);

    /// A task start whose write of the verdict lost to another worker learned nothing about a task
    /// to take, so it must not make its job due at once - only certainty of a task to take polls a
    /// job sooner than the base interval. The task start fails a task outlived elsewhere, closes the
    /// iteration, and the double gets a write in ahead of it.
    ///
    /// Checked by mapping `TaskStart::LostToAnotherWorker` to `JobProgress::TaskPickable`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_task_start_that_lost_the_verdict_to_another_worker_does_not_make_its_job_due_at_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let job_def = JobDefinition::new(
            JobDefinitionId::new(),
            JobCode::new("lost_verdict_job"),
            vec![(
                TaskDefinition::new(TaskCode::new(RUNNING_TASK_CODE), OUTLIVED_TASK_LIFETIME)
                    .with_max_lifetime(OUTLIVED_TASK_LIFETIME),
                task_fn(|_ctx| async { Ok(TaskOutcome::empty()) }),
            )],
            Vec::new(),
            Vec::new(),
            TaskLimits::default(),
        )?;
        let job_code = job_def.code().clone();
        let inner: Arc<dyn Storage> = Arc::new(InMemoryStorage::new());
        let cancel_token = CancellationToken::new();
        let foreign_worker_id = Uuid::from_u128(7_201);
        let mut job = Job::new(&job_def, HashMap::new(), foreign_worker_id)?;
        let TaskPickup::Ready(task_id) = job.pick_task_to_execute(&foreign_worker_id)? else {
            return Err("the fixture must leave a task to hold".into());
        };
        job.start_task(&task_id, foreign_worker_id)?;
        inner.save_job(&mut job, &cancel_token).await?;
        let outlived_at = Utc::now() + chrono::Duration::from_std(OUTLIVED_TASK_LIFETIME * 2)?;
        tokio::time::timeout(PROGRESS_TIMEOUT, async {
            while Utc::now() < outlived_at {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| "the system clock must pass the lifetime of the held task")?;
        let contending = Arc::new(ContendingStorage::new(Arc::clone(&inner)));
        let worker = worker_running(job_def, Arc::clone(&contending) as Arc<dyn Storage>)?;

        let task_start = start_task_of(&worker, &job_code, &cancel_token).await;

        assert_eq!(
            contending.interferences(),
            1,
            "the double must have got its write in ahead of the verdict"
        );
        assert!(
            matches!(task_start, TaskStart::LostToAnotherWorker),
            "the task start must have lost the verdict"
        );
        assert_eq!(JobProgress::from_task_start(&task_start), JobProgress::NothingPickable);
        Ok(())
    }
}
