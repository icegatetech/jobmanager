use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use crate::core::error::{Error, InternalError};
use crate::core::job::JobCode;
use crate::core::registry::JobRegistry;
use crate::execution::builder::JobsManagerBuilder;
use crate::execution::job_cleaner::JobIterationStarted;
use crate::execution::job_cleaner::{JobCleaner, JobCleanerConfig};
use crate::execution::scheduler::{PollScheduler, PollSchedulerConfig};
use crate::execution::worker::{FinishedIterationSink, Worker, WorkerContext};
use crate::execution::worker_pool::WorkerPool;
use crate::infra::metrics::MetricsSink;
use crate::infra::retrier::RetrierConfig;
use crate::storage::Storage;

/// Settings a [`JobsManager`] is assembled with, filled in by [`JobsManagerBuilder`] - the only way
/// a caller reaches them.
#[derive(Clone)]
pub struct JobsManagerConfig {
    /// Number of workers in the pool. Each worker runs at most one task start or one execution at a time, so
    /// this bounds how many tasks run concurrently across all jobs. Must be at least 1;
    /// [`JobsManager::new`] rejects 0.
    pub worker_count: usize,
    /// Polling settings of the pool's poll schedule.
    pub scheduler_config: PollSchedulerConfig,
    /// Retry policy every worker applies to its storage operations.
    pub retrier_config: RetrierConfig,
    /// Old-iteration cleanup behavior.
    pub cleaner_config: JobCleanerConfig,
}

impl Default for JobsManagerConfig {
    fn default() -> Self {
        Self {
            worker_count: 1,
            scheduler_config: PollSchedulerConfig::default(),
            retrier_config: RetrierConfig::default(),
            cleaner_config: JobCleanerConfig::default(),
        }
    }
}

/// Orchestrates a pool of workers that poll storage for jobs and execute their tasks.
///
/// Construct with [`JobsManager::builder`], then call [`JobsManager::start`] to spawn the worker
/// tasks and obtain a [`JobsManagerHandle`] for controlling their lifecycle.
pub struct JobsManager {
    job_registry: Arc<JobRegistry>,
    storage: Arc<dyn Storage>,
    config: JobsManagerConfig,
    metrics: Arc<dyn MetricsSink>,
}

/// Last finished iteration of every job the pool runs, recorded for whoever waits for one.
///
/// A [`watch`] channel rather than a stream of events, because a caller cannot start observing
/// before the pool does: the number stays readable, so a wait called after the iteration finished
/// still sees it.
///
/// The pool's workers must be its only owners: a sender held elsewhere would leave a wait hanging
/// after the pool stopped.
pub(crate) struct FinishedIterations {
    last_finished_by_job: HashMap<JobCode, watch::Sender<Option<u64>>>,
}

impl FinishedIterations {
    /// Opens a channel per job code, each starting with no finished iteration.
    fn new(job_codes: Vec<JobCode>) -> Self {
        Self {
            last_finished_by_job: job_codes
                .into_iter()
                .map(|job_code| (job_code, watch::channel(None).0))
                .collect(),
        }
    }

    /// Subscribes to every job at once. A receiver reads what was recorded before it existed, so
    /// subscribing late loses nothing.
    fn subscribe_to_jobs(&self) -> HashMap<JobCode, watch::Receiver<Option<u64>>> {
        self.last_finished_by_job
            .iter()
            .map(|(job_code, last_finished_iteration)| (job_code.clone(), last_finished_iteration.subscribe()))
            .collect()
    }
}

impl FinishedIterationSink for FinishedIterations {
    /// The number only moves forward, so a report older than one already recorded does not rewind
    /// what a waiter sees. A job absent here is not run by this pool.
    fn record_finished_iteration(&self, job_code: &JobCode, iter_num: u64) {
        let Some(last_finished_iteration) = self.last_finished_by_job.get(job_code) else {
            return;
        };

        last_finished_iteration.send_if_modified(|recorded| {
            if recorded.is_none_or(|recorded_iter_num| recorded_iter_num < iter_num) {
                *recorded = Some(iter_num);
                return true;
            }
            false
        });
    }
}

/// Handle for controlling a running `JobsManager`.
///
/// Owns the pool: dropping it aborts every worker, so it has to stay alive for as long as the pool
/// should run.
#[must_use = "dropping the handle aborts the worker pool"]
pub struct JobsManagerHandle {
    cancel_token: CancellationToken,
    join_set: JoinSet<Result<(), InternalError>>,
    /// Receivers only - see [`FinishedIterations`] for why a sender must not be kept here.
    finished_iterations: HashMap<JobCode, watch::Receiver<Option<u64>>>,
    job_registry: Arc<JobRegistry>,
}

impl JobsManagerHandle {
    /// Gracefully stop workers and wait for completion.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.cancel_token.cancel();
        self.wait().await
    }

    /// Waits for `SIGINT` (Ctrl-C), then shuts the pool down gracefully.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if the signal handler cannot be installed.
    pub async fn shutdown_on_signal(self) -> Result<(), Error> {
        tokio::signal::ctrl_c()
            .await
            .map_err(|e| Error::Other(format!("failed to listen for shutdown signal: {e}")))?;
        self.shutdown().await
    }

    /// Waits until an iteration of `job_code` has reached a terminal state, and returns the number
    /// of the most recent one.
    ///
    /// Part of the public API because without it a caller is left polling storage or sleeping to
    /// find out that an iteration is over. An iteration that finished before this call is reported
    /// rather than waited for, so repeating the call returns the same number until a further one
    /// finishes. The outcome does not matter - a failed iteration ends the wait just as a completed
    /// one does.
    ///
    /// An iteration this pool did not finish itself - one left in storage by an earlier pool, or
    /// one another pool completed - ends the wait on the poll that picks it up. A job whose next
    /// iteration is not due yet is not polled at all until it is, so such an iteration is observed
    /// when the job next becomes due rather than within a poll interval of ending.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if `job_code` is not run by this pool, or if the pool stopped
    /// before any iteration of it finished.
    pub async fn wait_for_iteration_completion(&self, job_code: &JobCode) -> Result<u64, Error> {
        self.wait_for_matching_iteration(job_code, |_| true).await
    }

    /// Waits until `job_code` has run out its iteration budget.
    ///
    /// Public for the same reason as [`Self::wait_for_iteration_completion`], and reports an
    /// already-finished job the same way. It returns nothing, because the iteration it waits for is
    /// the job's `max_iterations` and is therefore known in advance.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] if the job has no `max_iterations` - an endless job never completes
    /// and waiting for it would hang - or for the reasons
    /// [`Self::wait_for_iteration_completion`] lists.
    pub async fn wait_for_job_completion(&self, job_code: &JobCode) -> Result<(), Error> {
        let max_iterations = self.job_registry.get_job(job_code)?.max_iterations().ok_or_else(|| {
            Error::Other(format!(
                "job '{job_code}' has no iteration limit, so it never completes"
            ))
        })?;

        self.wait_for_matching_iteration(job_code, |iter_num| iter_num >= max_iterations)
            .await?;
        Ok(())
    }

    /// Waits until the last finished iteration of `job_code` satisfies `is_awaited`, and returns its
    /// number.
    async fn wait_for_matching_iteration<F>(&self, job_code: &JobCode, is_awaited: F) -> Result<u64, Error>
    where
        F: Fn(u64) -> bool,
    {
        let mut last_finished_iteration = self
            .finished_iterations
            .get(job_code)
            .ok_or_else(|| Error::Other(format!("job '{job_code}' is not run by this pool")))?
            .clone();

        loop {
            let finished_iter_num = *last_finished_iteration.borrow_and_update();
            if let Some(iter_num) = finished_iter_num
                && is_awaited(iter_num)
            {
                return Ok(iter_num);
            }

            last_finished_iteration.changed().await.map_err(|_| {
                Error::Other(format!(
                    "the worker pool stopped before an iteration of job '{job_code}' finished"
                ))
            })?;
        }
    }

    /// Force-abort worker tasks.
    pub fn abort(&mut self) {
        self.cancel_token.cancel();
        self.join_set.abort_all();
    }

    async fn wait(&mut self) -> Result<(), Error> {
        while let Some(result) = self.join_set.join_next().await {
            #[allow(clippy::match_same_arms)]
            match result {
                Ok(Ok(())) => {}
                Ok(Err(_)) => {
                    // Error is already logged inside the worker task.
                }
                Err(e) => {
                    error!("A task of the worker pool panicked: {}", e);
                }
            }
        }

        Ok(())
    }
}

impl Drop for JobsManagerHandle {
    fn drop(&mut self) {
        self.abort();
    }
}

impl JobsManager {
    /// Starts describing a pool: its storage backend, its jobs, and their executors.
    ///
    /// This is the entry point of the crate; see [`JobsManagerBuilder`].
    pub fn builder() -> JobsManagerBuilder {
        JobsManagerBuilder::new()
    }

    // TODO(med): we need to allocate an asynchronous API for managing and configuring jobs. So the
    // client will be able to change job settings without restarting. TODO(low): consider reducing
    // the number of parameters for manager startup
    /// Creates a `JobsManager` bound to the given storage backend and job registry.
    ///
    /// Workers are not spawned until [`JobsManager::start`] is called.
    ///
    /// # Errors
    ///
    /// Returns an error if `config.worker_count` is 0. The polling settings are checked by
    /// [`PollSchedulerConfig`] as they are set, so one that arrives here is already valid.
    pub(crate) fn new(
        storage: Arc<dyn Storage>,
        config: JobsManagerConfig,
        job_registry: Arc<JobRegistry>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<Self, Error> {
        if config.worker_count == 0 {
            error!("JobsManager initialization requested with worker_count=0");
            return Err(Error::Other("worker count must be at least 1".to_string()));
        }

        Ok(Self {
            job_registry,
            storage,
            config,
            metrics,
        })
    }

    /// Start worker tasks and return a handle for lifecycle control.
    // `Result` is part of the public signature; dropping it is a breaking change.
    #[allow(clippy::unnecessary_wraps)]
    pub fn start(&self) -> Result<JobsManagerHandle, Error> {
        info!("Starting jobmanager with {} workers", self.config.worker_count);

        let cancel_token = CancellationToken::new();
        let mut join_set = JoinSet::new();

        // Goes out of scope here, leaving the workers as its only owners.
        let finished_iterations = Arc::new(FinishedIterations::new(self.job_registry.list_jobs()));
        let cleanup_notifier = self.spawn_job_cleaner(&mut join_set, &cancel_token);
        let scheduler = PollScheduler::new(self.job_registry.list_jobs(), self.config.scheduler_config.clone());

        let worker_context = WorkerContext {
            job_registry: Arc::clone(&self.job_registry),
            storage: Arc::clone(&self.storage),
            metrics: self.metrics.clone(),
            iteration_notifier: cleanup_notifier,
            finished_iterations: Arc::clone(&finished_iterations) as Arc<dyn FinishedIterationSink>,
        };

        let workers = (0..self.config.worker_count)
            .map(|_| {
                Worker::new(
                    Uuid::new_v4(),
                    self.config.retrier_config.clone(),
                    worker_context.clone(),
                )
            })
            .collect();
        let worker_pool = WorkerPool::new(scheduler, workers);
        let pool_token = cancel_token.clone();
        join_set.spawn(async move {
            worker_pool.run(pool_token).await;
            Ok(())
        });

        Ok(JobsManagerHandle {
            cancel_token,
            join_set,
            finished_iterations: finished_iterations.subscribe_to_jobs(),
            job_registry: Arc::clone(&self.job_registry),
        })
    }

    /// Spawns the [`JobCleaner`] into the pool and returns the sender workers report iterations
    /// through, or `None` when cleanup is disabled.
    ///
    /// The channel holds five reports per job, so a burst of reports arriving while the start-up
    /// reconciliation is still running does not push reports out. The worker count is no factor: a
    /// report is sent only by the worker whose write of the iteration start was `SaveOutcome::Saved`,
    /// so each iteration is reported once however many workers the pool has, and the iterations of
    /// one job start one after another. A cleaner that falls further behind than that has its
    /// reports dropped by [`Worker`], and the tail they would have trimmed waits for the next
    /// start-up reconciliation. A pool holds at least one job, so the capacity never reaches the
    /// value `mpsc::channel` panics on.
    fn spawn_job_cleaner(
        &self,
        join_set: &mut JoinSet<Result<(), InternalError>>,
        cancel_token: &CancellationToken,
    ) -> Option<mpsc::Sender<JobIterationStarted>> {
        if !self.config.cleaner_config.enabled {
            info!("Job cleaner is disabled: outdated job iterations are kept");
            return None;
        }

        let (sender, receiver) = mpsc::channel(self.job_registry.jobs_count() * 5);
        let cleaner = JobCleaner::new(
            Arc::clone(&self.job_registry),
            Arc::clone(&self.storage),
            &self.config.cleaner_config,
        );
        let token = cancel_token.clone();

        join_set.spawn(async move {
            if let Err(e) = cleaner.start(receiver, token).await {
                error!("Job cleaner stopped with error: {}", e);
                Err(e)
            } else {
                Ok(())
            }
        });

        Some(sender)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The job every fixture below records against.
    const RECORDED_JOB: &str = "recorded_job";

    fn finished_iterations() -> FinishedIterations {
        FinishedIterations::new(vec![JobCode::new(RECORDED_JOB)])
    }

    fn subscribe_to_recorded_job(finished_iterations: &FinishedIterations) -> watch::Receiver<Option<u64>> {
        finished_iterations
            .subscribe_to_jobs()
            .remove(&JobCode::new(RECORDED_JOB))
            .expect("the pool runs this job")
    }

    #[test]
    fn the_first_finished_iteration_is_recorded() {
        let finished_iterations = finished_iterations();
        let mut last_finished = subscribe_to_recorded_job(&finished_iterations);

        finished_iterations.record_finished_iteration(&JobCode::new(RECORDED_JOB), 1);

        assert_eq!(*last_finished.borrow_and_update(), Some(1));
    }

    #[test]
    fn a_later_iteration_moves_the_recorded_number_forward() {
        let finished_iterations = finished_iterations();
        let mut last_finished = subscribe_to_recorded_job(&finished_iterations);

        finished_iterations.record_finished_iteration(&JobCode::new(RECORDED_JOB), 1);
        finished_iterations.record_finished_iteration(&JobCode::new(RECORDED_JOB), 2);

        assert_eq!(*last_finished.borrow_and_update(), Some(2));
    }

    /// A worker that read an older state on pickup reports the iteration it found there. Letting
    /// that rewind the number would un-satisfy a condition a waiter has already met, and the wait
    /// would hang until some further iteration finished.
    #[test]
    fn an_older_iteration_does_not_rewind_the_recorded_number() {
        let finished_iterations = finished_iterations();
        let mut last_finished = subscribe_to_recorded_job(&finished_iterations);

        finished_iterations.record_finished_iteration(&JobCode::new(RECORDED_JOB), 5);
        finished_iterations.record_finished_iteration(&JobCode::new(RECORDED_JOB), 3);

        assert_eq!(*last_finished.borrow_and_update(), Some(5));
    }

    /// The same iteration is reported by every worker that picks it up, so a repeat has to be a
    /// no-op rather than a change waiters are woken for.
    #[test]
    fn repeating_the_same_iteration_is_not_a_change() {
        let finished_iterations = finished_iterations();
        let mut last_finished = subscribe_to_recorded_job(&finished_iterations);

        finished_iterations.record_finished_iteration(&JobCode::new(RECORDED_JOB), 4);
        assert_eq!(*last_finished.borrow_and_update(), Some(4));

        finished_iterations.record_finished_iteration(&JobCode::new(RECORDED_JOB), 4);

        assert!(
            !last_finished.has_changed().expect("the channel outlives the receiver"),
            "a repeated report must not wake the waiters again"
        );
    }

    /// A worker reports every iteration it finishes, and a pool that does not run that job has
    /// nowhere to put the report - which is a fact about this pool, not an error.
    #[test]
    fn a_report_about_a_job_outside_the_pool_is_ignored() {
        let finished_iterations = finished_iterations();
        let mut last_finished = subscribe_to_recorded_job(&finished_iterations);

        finished_iterations.record_finished_iteration(&JobCode::new("job_of_another_pool"), 1);

        assert_eq!(*last_finished.borrow_and_update(), None);
    }

    /// A caller cannot start waiting before the pool starts running, so the number has to stay
    /// readable rather than be delivered once and lost.
    #[test]
    fn a_subscriber_that_arrives_after_the_report_still_sees_it() {
        let finished_iterations = finished_iterations();

        finished_iterations.record_finished_iteration(&JobCode::new(RECORDED_JOB), 7);
        let mut last_finished = subscribe_to_recorded_job(&finished_iterations);

        assert_eq!(*last_finished.borrow_and_update(), Some(7));
    }
}
