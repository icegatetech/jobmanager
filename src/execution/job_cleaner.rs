use std::{future::Future, sync::Arc, time::Duration};

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::{
    InternalError, JobCode, JobRegistry, Retrier, RetrierConfig, RetryStep, Storage, StorageError, StorageResult,
    storage::JobMeta,
};

/// Behavior of the background cleanup that deletes iterations which fell out of a job's retention
/// window, set through [`JobsManagerConfig::cleaner_config`](crate::JobsManagerConfig::cleaner_config).
#[derive(Clone)]
pub struct JobCleanerConfig {
    /// Whether the cleaner runs at all. Enabled by default; turning it off leaves every
    /// iteration in storage forever.
    ///
    /// Cleanup never affects the correctness of a job: a delete that is dropped or fails only
    /// leaves old state objects in the bucket, and the next start-up of any instance picks them
    /// up.
    pub enabled: bool,
    /// Retry policy for the cleaner's storage operations.
    pub retrier_config: RetrierConfig,
}

impl Default for JobCleanerConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            // Deliberately far shorter than `RetrierConfig::default()`: a cleanup that keeps
            // retrying for minutes stalls the queue of iterations waiting to be cleaned, and a
            // dropped cleanup is recovered by the next start-up reconciliation anyway.
            retrier_config: RetrierConfig {
                max_attempts: 3,
                rand_delay: Duration::from_millis(50),
                delays: vec![
                    Duration::from_millis(50),
                    Duration::from_millis(300),
                    Duration::from_secs(2),
                ],
            },
        }
    }
}

/// Report from a worker that it persisted the start of iteration `iter_num` of `job_code`.
///
/// A fact, not a command: the worker states what happened and never waits for the cleanup.
pub(crate) struct JobIterationStarted {
    pub job_code: JobCode,
    pub iter_num: u64,
}

/// Deletes the state objects of iterations that fell outside their job's retention window
/// ([`JobDefinition::with_iteration_retention`](crate::JobDefinition::with_iteration_retention)).
///
/// # Guarantees
///
/// Only iterations at or below the *retention boundary* - the job's current iteration number minus
/// its retention - are ever deleted. The boundary is strictly below the current iteration, and
/// iteration numbers only grow, so the newest state object is undeletable under any interleaving:
/// [`Storage::find_job_meta`] always finds the job, and no worker can recreate it from
/// `iter_num = 1`.
///
/// This rests on the same assumption as the conditional writes the crate coordinates with - that
/// the store lists strongly consistently (AWS S3 has since 2020). A store whose `LIST` lags could
/// hide the current iteration from `find_job_meta` regardless of cleanup.
///
/// # Limits
///
/// - Only jobs present in the [`JobRegistry`] are cleaned. Renaming a job's code orphans its old
///   prefix, which then needs deleting by hand.
/// - Iterations written under a different [`JobStateCodecKind`](crate::JobStateCodecKind) are not
///   recognized as iterations and stay.
/// - A report can be lost: the channel is full, the process is shutting down, or the retries are
///   spent. Nothing is corrupted - the tail merely stays longer, until the next start-up
///   reconciliation of any instance picks it up.
// TODO(med): defend against a resurrected iteration - re-check `find_job_meta` after a successful
// save and let a lagging worker delete the object it just recreated. Until then the mitigation is
// the retention floor of 5 plus the start-up reconciliation.
pub(crate) struct JobCleaner {
    job_registry: Arc<JobRegistry>,
    storage: Arc<dyn Storage>,
    retrier: Retrier,
}

impl JobCleaner {
    pub fn new(job_registry: Arc<JobRegistry>, storage: Arc<dyn Storage>, config: &JobCleanerConfig) -> Self {
        Self {
            job_registry,
            storage,
            retrier: Retrier::new(config.retrier_config.clone()),
        }
    }

    /// Reconciles the registry once and then cleans up as workers report new iterations, until
    /// `cancel_token` is cancelled or every sender is dropped.
    ///
    /// Both run concurrently: reconciliation walks all jobs and must not delay the reports coming
    /// in meanwhile. A failure to clean one job is logged and never propagated - cleanup must not
    /// take the pool down with it.
    pub async fn start(
        &self,
        mut receiver: mpsc::Receiver<JobIterationStarted>,
        cancel_token: CancellationToken,
    ) -> Result<(), InternalError> {
        info!("Starting job cleaner");

        tokio::join!(
            self.reconcile_registry_jobs(&cancel_token),
            self.process_iteration_messages(&mut receiver, &cancel_token)
        );

        info!("Stopping job cleaner");

        Ok(())
    }

    /// One pass over every registered job, deleting whatever tail is already outdated.
    ///
    /// This is what closes the gaps reports cannot: iterations whose report was lost, tails left
    /// by another instance, and jobs that stopped iterating altogether - a job that reached
    /// `max_iterations` is no longer polled, so no worker will ever report it again.
    async fn reconcile_registry_jobs(&self, cancel_token: &CancellationToken) {
        for job_code in self.job_registry.list_jobs() {
            if cancel_token.is_cancelled() {
                return;
            }

            match self.delete_outdated_iterations(&job_code, cancel_token).await {
                Ok(0) => debug!("Job {job_code} has no outdated iterations to reconcile"),
                Ok(deleted_count) => info!("Reconciled job {job_code}: {deleted_count} outdated iterations deleted"),
                Err(InternalError::Cancelled) => return,
                Err(e) => error!("Failed to reconcile outdated iterations of job {job_code}: {e}"),
            }
        }
    }

    /// Handles worker reports one at a time, deleting the single iteration each report pushes out
    /// of the retention window.
    async fn process_iteration_messages(
        &self,
        receiver: &mut mpsc::Receiver<JobIterationStarted>,
        cancel_token: &CancellationToken,
    ) {
        loop {
            let next_message = tokio::select! {
                () = cancel_token.cancelled() => return,
                next_message = receiver.recv() => next_message,
            };

            // Every sender is gone, so no further iteration can be reported.
            let Some(message) = next_message else { return };

            match self.delete_iteration(&message, cancel_token).await {
                Ok(()) => {}
                Err(InternalError::Cancelled) => return,
                Err(e) => error!(
                    "Failed to delete outdated iteration of job {} reported at iteration {}: {}",
                    message.job_code, message.iter_num, e
                ),
            }
        }
    }

    /// Deletes the one iteration that `message` pushed past the retention boundary.
    ///
    /// A single key per started iteration keeps the steady-state cost at one free `DELETE`; any
    /// iteration missed this way is collected by the next reconciliation.
    async fn delete_iteration(
        &self,
        message: &JobIterationStarted,
        cancel_token: &CancellationToken,
    ) -> Result<(), InternalError> {
        let Some(retention_boundary) = self.calculate_retention_boundary(&message.job_code, message.iter_num) else {
            return Ok(());
        };
        let outdated_iter_nums = [retention_boundary];

        self.retry_storage_call(
            || {
                self.storage
                    .delete_job_iterations(&message.job_code, &outdated_iter_nums, cancel_token)
            },
            cancel_token,
        )
        .await?;

        debug!(
            "Deleted iteration {} of job {} left behind by iteration {}",
            retention_boundary, message.job_code, message.iter_num
        );

        Ok(())
    }

    /// Deletes the whole outdated tail of `job_code` and reports how many iterations went.
    async fn delete_outdated_iterations(
        &self,
        job_code: &JobCode,
        cancel_token: &CancellationToken,
    ) -> Result<usize, InternalError> {
        let job_meta = match self.find_job_meta(job_code, cancel_token).await {
            Ok(job_meta) => job_meta,
            // A job no worker has created yet has no tail at all.
            Err(InternalError::Storage(StorageError::NotFound(_))) => return Ok(0),
            Err(e) => return Err(e),
        };

        let Some(retention_boundary) = self.calculate_retention_boundary(job_code, job_meta.iter_num) else {
            return Ok(0);
        };

        let outdated_iter_nums = self
            .retry_storage_call(
                || {
                    self.storage
                        .list_job_outdated_iterations(job_code, retention_boundary, cancel_token)
                },
                cancel_token,
            )
            .await?;

        if outdated_iter_nums.is_empty() {
            return Ok(0);
        }

        self.retry_storage_call(
            || self.storage.delete_job_iterations(job_code, &outdated_iter_nums, cancel_token),
            cancel_token,
        )
        .await?;

        Ok(outdated_iter_nums.len())
    }

    /// Newest iteration number of `job_code` that may be deleted while its current iteration is
    /// `iter_num`, per the job's retention window.
    ///
    /// `None` means nothing may be deleted: either the window still covers the whole history, or
    /// the job left the registry and its retention is therefore unknown.
    fn calculate_retention_boundary(&self, job_code: &JobCode, iter_num: u64) -> Option<u64> {
        match self.job_registry.get_job(job_code) {
            Ok(job_def) => job_def.calculate_retention_boundary(iter_num),
            Err(e) => {
                warn!("Skipping cleanup of job {job_code} that is not registered: {e}");
                None
            }
        }
    }

    async fn find_job_meta(
        &self,
        job_code: &JobCode,
        cancel_token: &CancellationToken,
    ) -> Result<JobMeta, InternalError> {
        self.retry_storage_call(|| self.storage.find_job_meta(job_code, cancel_token), cancel_token)
            .await
    }

    /// Runs one storage call under the cleaner's own retry policy.
    ///
    /// The cleanup methods of [`Storage`] deliberately do not retry internally, so the bound on
    /// how long a failing cleanup keeps trying lives here - see [`JobCleanerConfig`].
    async fn retry_storage_call<F, Fut, T>(
        &self,
        mut call: F,
        cancel_token: &CancellationToken,
    ) -> Result<T, InternalError>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = StorageResult<T>>,
    {
        self.retrier
            .retry(
                move || {
                    let attempt = call();
                    async move {
                        match attempt.await {
                            Ok(value) => Ok(RetryStep::Done(value)),
                            Err(e) if e.is_retryable() => Ok(RetryStep::Retry(InternalError::from(e))),
                            Err(e) => Err(InternalError::from(e)),
                        }
                    }
                },
                cancel_token,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use chrono::Duration as ChronoDuration;

    use super::*;
    use crate::storage::in_memory::InMemoryStorage;
    use crate::{Job, JobDefinition, TaskCode, TaskDefinition, TaskExecutorFn};

    /// Storage whose deletes always fail with the given error; every other call is delegated, so
    /// the double stays an implementation of the production trait.
    struct FailingDeleteStorage {
        inner: InMemoryStorage,
        delete_error: StorageError,
    }

    #[async_trait::async_trait]
    impl Storage for FailingDeleteStorage {
        async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job> {
            self.inner.get_job(job_code, cancel_token).await
        }

        async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job> {
            self.inner.get_job_by_meta(job_meta, cancel_token).await
        }

        async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta> {
            self.inner.find_job_meta(job_code, cancel_token).await
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
            _job_code: &JobCode,
            _iter_nums: &[u64],
            _cancel_token: &CancellationToken,
        ) -> StorageResult<()> {
            Err(self.delete_error.clone())
        }
    }

    fn build_job_definition(job_code: &JobCode, iteration_retention: u64) -> JobDefinition {
        let executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
            let task_id = *task.id();
            Box::pin(async move { manager.complete_task(&task_id, b"done".to_vec()) })
        });
        let task_def = TaskDefinition::new(TaskCode::new("cleanup_task"), Vec::new(), ChronoDuration::seconds(5))
            .expect("task definition must build");
        let mut executors = HashMap::new();
        executors.insert(TaskCode::new("cleanup_task"), executor);

        JobDefinition::new(job_code.clone(), vec![task_def], executors)
            .expect("job definition must build")
            .with_iteration_retention(iteration_retention)
            .expect("iteration retention must be accepted")
    }

    #[tokio::test]
    async fn test_spent_delete_retries_report_the_last_storage_error() {
        let job_code = JobCode::new("cleaner_retry_cause_job");
        let job_registry =
            Arc::new(JobRegistry::new(vec![build_job_definition(&job_code, 5)]).expect("registry must build"));
        let storage = Arc::new(FailingDeleteStorage {
            inner: InMemoryStorage::new(),
            delete_error: StorageError::ServiceUnavailable,
        });
        let cleaner_config = JobCleanerConfig {
            retrier_config: RetrierConfig {
                max_attempts: 2,
                rand_delay: Duration::from_millis(1),
                delays: vec![Duration::from_millis(1)],
            },
            ..Default::default()
        };
        let cleaner = JobCleaner::new(job_registry, storage, &cleaner_config);
        let message = JobIterationStarted { job_code, iter_num: 7 };

        let error = cleaner
            .delete_iteration(&message, &CancellationToken::new())
            .await
            .expect_err("a delete that always fails must not report success");

        // Without the cause the log would only say "attempts spent", and the log is the only
        // observability the cleaner has.
        assert!(
            matches!(
                &error,
                InternalError::MaxAttemptsReached(cause)
                    if matches!(**cause, InternalError::Storage(StorageError::ServiceUnavailable))
            ),
            "spent retries must carry the failure that caused them, got: {error:?}"
        );
    }
}
