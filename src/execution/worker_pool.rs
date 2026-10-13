//! The loop of one worker pool: which worker starts a task of which due job, and when the pool
//! sleeps.

use std::panic::AssertUnwindSafe;
use std::sync::Arc;

use futures_util::FutureExt;
use tokio::task::{JoinError, JoinSet};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::core::job::JobCode;
use crate::execution::outcome::JobProgress;
use crate::execution::scheduler::{ClaimAttempt, JobClaim, PollScheduler, ScheduleError};
use crate::execution::worker::{StartedTask, Worker};

/// How a task start ended, carrying back the worker that ran it and the claim it held the job
/// by.
struct TaskStartOutcome {
    worker: Arc<Worker>,
    claim: JobClaim,
    progress: JobProgress,
    /// `None` when the task start wrote the start of no task, or panicked.
    started_task: Option<StartedTask>,
}

/// How an execution ended, carrying back the worker that ran it.
struct ExecutionOutcome {
    worker: Arc<Worker>,
    job_code: JobCode,
    progress: JobProgress,
}

/// What the pool waits for once every free worker has been handed the jobs that were due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PoolWait {
    /// The earliest unclaimed job falls due at this moment, unless a task start or an execution
    /// ends first.
    Until(Instant),
    /// No worker is free, or every job is claimed or exhausted: only a task start or an
    /// execution ending changes either. With every job exhausted and nothing in flight, only the
    /// shutdown comes.
    ForPoolEvent,
}

/// A task whose start was written, waiting for the worker that wrote it to execute it.
struct TaskToExecute {
    worker: Arc<Worker>,
    job_code: JobCode,
    started_task: StartedTask,
}

/// Runs the task starts and executions of one pool on its workers, one at a time per worker,
/// and is the only owner of the pool's [`PollScheduler`].
///
/// A due job is claimed only while a worker is free; one that finds every worker busy waits for the
/// first one to finish. The claim covers the task start alone: it goes back to the schedule before
/// the task whose start it wrote is executed, so another worker can start a second task of the
/// same job meanwhile.
pub(crate) struct WorkerPool {
    scheduler: PollScheduler,
    idle_workers: Vec<Arc<Worker>>,
    tasks_to_execute: Vec<TaskToExecute>,
    task_starts: JoinSet<TaskStartOutcome>,
    executions: JoinSet<ExecutionOutcome>,
}

impl WorkerPool {
    pub(crate) fn new(scheduler: PollScheduler, workers: Vec<Worker>) -> Self {
        let worker_count = workers.len();
        Self {
            scheduler,
            idle_workers: workers.into_iter().map(Arc::new).collect(),
            tasks_to_execute: Vec::with_capacity(worker_count),
            task_starts: JoinSet::new(),
            executions: JoinSet::new(),
        }
    }

    /// Runs until `cancel_token` is cancelled, then waits for every task start and execution in
    /// flight before it returns. A started task whose execution the pool has not launched by the
    /// time it sees the cancellation is not executed, whether its start was written before the
    /// cancellation or after it: its result would be dropped by the shutdown anyway, so it stays
    /// `Started` in storage, for a takeover once its deadline passes.
    ///
    /// A task start or an execution that panicked gives its worker back to the pool; a panicked
    /// task start releases its job as one that changed nothing, and a panicked execution counts as
    /// one that ended without a state.
    pub(crate) async fn run(mut self, cancel_token: CancellationToken) {
        info!("Starting worker pool of {} workers", self.idle_workers.len());
        while !cancel_token.is_cancelled() {
            self.start_executions(&cancel_token);
            let pool_wait = self.spawn_task_starts(&cancel_token);
            // The cancellation branch is the one that is never disabled: an empty `JoinSet` disables
            // its own, and a `select!` left with no branch and no `else` panics.
            tokio::select! {
                () = cancel_token.cancelled() => {}
                Some(join_result) = self.task_starts.join_next() => self.close_task_start(join_result),
                Some(join_result) = self.executions.join_next() => self.close_execution(join_result),
                () = Self::sleep_until_wake(pool_wait) => {}
            }
        }

        info!("Stopping worker pool");
        // TODO(med): hand the tasks a shutdown leaves `Started` back to `Todo` - the ones whose
        // start is never executed below and the ones whose execution the shutdown cancelled - so
        // another instance takes them at once rather than after their deadline. Needs a domain
        // transition `Started` -> `Todo` open to the task's owner alone that charges no attempt and
        // keeps the moment of the first start the maximum lifetime counts from; a conditional write
        // merged on `412` like the save of a result, dropped on `TaskWorkerMismatch`; a token of
        // its own bounded in time, since the pool's is already cancelled; and a quota for the
        // shutdown scenario in docs/tests.md - one `PUT` per task handed back.
        loop {
            tokio::select! {
                Some(join_result) = self.task_starts.join_next() => self.close_task_start(join_result),
                Some(join_result) = self.executions.join_next() => self.close_execution(join_result),
                else => break,
            }
        }
        for TaskToExecute {
            job_code, started_task, ..
        } in self.tasks_to_execute.drain(..)
        {
            let task_id = started_task.task_id();
            match started_task.deadline_at() {
                Ok(Some(deadline_at)) => warn!(
                    "Started task '{task_id}' of job '{job_code}' is not executed: the worker pool is stopping, so \
                     it stays `Started` in storage until its deadline {deadline_at}"
                ),
                Ok(None) | Err(_) => warn!(
                    "Started task '{task_id}' of job '{job_code}' is not executed: the worker pool is stopping, so \
                     it stays `Started` in storage until its deadline, which is unknown"
                ),
            }
        }
    }

    /// Hands due jobs to free workers until either runs out, and returns what the pool waits for
    /// next.
    fn spawn_task_starts(&mut self, cancel_token: &CancellationToken) -> PoolWait {
        while let Some(worker) = self.idle_workers.pop() {
            match self.scheduler.try_claim_due_job() {
                Ok(ClaimAttempt::Claimed(claim)) => self.spawn_task_start(worker, claim, cancel_token),
                Ok(ClaimAttempt::WaitUntil(wake_at)) => {
                    self.idle_workers.push(worker);
                    return PoolWait::Until(wake_at);
                }
                Ok(ClaimAttempt::WaitForPoolEvent) => {
                    self.idle_workers.push(worker);
                    return PoolWait::ForPoolEvent;
                }
                // The entry is gone from the queue with the error, so the next look moves on.
                Err(error) => {
                    Self::report_schedule_error(&error);
                    self.idle_workers.push(worker);
                }
            }
        }
        PoolWait::ForPoolEvent
    }

    // A worker keeps no state of its own a panic could leave half-written, which is what makes
    // handing it back after one sound.
    fn spawn_task_start(&mut self, worker: Arc<Worker>, claim: JobClaim, cancel_token: &CancellationToken) {
        let token = cancel_token.clone();
        self.task_starts.spawn(async move {
            let task_start = AssertUnwindSafe(worker.start_task(claim.job_code(), &token))
                .catch_unwind()
                .await;
            let (progress, started_task) = task_start.map_or_else(
                |_| {
                    error!("A task start of job '{}' panicked", claim.job_code());
                    (JobProgress::Stalled, None)
                },
                |task_start| {
                    (
                        JobProgress::from_task_start(&task_start),
                        task_start.into_started_task(),
                    )
                },
            );
            TaskStartOutcome {
                worker,
                claim,
                progress,
                started_task,
            }
        });
    }

    /// Starts the execution of every task whose start was written since the last look.
    fn start_executions(&mut self, cancel_token: &CancellationToken) {
        for TaskToExecute {
            worker,
            job_code,
            started_task,
        } in self.tasks_to_execute.drain(..)
        {
            let token = cancel_token.clone();
            self.executions.spawn(async move {
                let execution_end = AssertUnwindSafe(worker.execute_task(started_task, &token))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| {
                        error!("An execution of a task of job '{job_code}' panicked");
                        None
                    });
                ExecutionOutcome {
                    worker,
                    job_code,
                    progress: JobProgress::from_job_execution(execution_end),
                }
            });
        }
    }

    /// Returns the job of a finished task start to the schedule, and queues the task it started
    /// for its worker to execute - or gives the worker back to the pool when it started none.
    fn close_task_start(&mut self, join_result: Result<TaskStartOutcome, JoinError>) {
        // A panic is caught inside the task, so only an abort lands here - and the pool's tasks are
        // aborted only together with the pool.
        let TaskStartOutcome {
            worker,
            claim,
            progress,
            started_task,
        } = match join_result {
            Ok(report) => report,
            Err(join_error) => {
                error!("A task start of the worker pool ended without a report: {join_error}");
                return;
            }
        };

        match started_task {
            Some(started_task) => {
                // The release consumes the claim, and the execution still needs the code.
                let job_code = claim.job_code().clone();
                self.release_job(claim, progress);
                self.tasks_to_execute.push(TaskToExecute {
                    worker,
                    job_code,
                    started_task,
                });
            }
            None => {
                self.release_job(claim, progress);
                self.idle_workers.push(worker);
            }
        }
    }

    fn close_execution(&mut self, join_result: Result<ExecutionOutcome, JoinError>) {
        let ExecutionOutcome {
            worker,
            job_code,
            progress,
        } = match join_result {
            Ok(report) => report,
            Err(join_error) => {
                error!("An execution of the worker pool ended without a report: {join_error}");
                return;
            }
        };
        if let Err(error) = self.scheduler.reschedule_job(&job_code, progress) {
            Self::report_schedule_error(&error);
        }
        self.idle_workers.push(worker);
    }

    fn release_job(&mut self, claim: JobClaim, progress: JobProgress) {
        if let Err(error) = self.scheduler.release_job(claim, progress) {
            Self::report_schedule_error(&error);
        }
    }

    fn report_schedule_error(error: &ScheduleError) {
        error!("The poll schedule refused a call of its own pool: {error}");
    }

    /// Sleeps until the moment `pool_wait` names; the wait for a pool event is left to the other
    /// branches of the loop's `select!`.
    async fn sleep_until_wake(pool_wait: PoolWait) {
        match pool_wait {
            PoolWait::Until(wake_at) => tokio::time::sleep_until(wake_at).await,
            PoolWait::ForPoolEvent => std::future::pending().await,
        }
    }
}
