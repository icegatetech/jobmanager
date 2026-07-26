use std::sync::Arc;

use tokio::task::JoinSet;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

use crate::{Error, InternalError, JobRegistry, Metrics, Storage, Worker, WorkerConfig};

/// Configuration for a [`JobsManager`].
#[derive(Clone)]
pub struct JobsManagerConfig {
    /// Number of worker tasks to spawn. Each worker executes at most one task at a time, so
    /// this bounds how many tasks run concurrently across all jobs. Must be at least 1;
    /// [`JobsManager::new`] rejects 0.
    pub worker_count: usize,
    /// Polling interval, backoff, and retry policy applied by every spawned worker.
    pub worker_config: WorkerConfig,
}

impl Default for JobsManagerConfig {
    fn default() -> Self {
        Self {
            worker_count: 1,
            worker_config: WorkerConfig::default(),
        }
    }
}

/// Orchestrates a pool of workers that poll storage for jobs and execute their tasks.
///
/// Construct with [`JobsManager::new`], then call [`JobsManager::start`] to spawn the worker
/// tasks and obtain a [`JobsManagerHandle`] for controlling their lifecycle.
pub struct JobsManager {
    job_registry: Arc<JobRegistry>,
    storage: Arc<dyn Storage>,
    config: JobsManagerConfig,
    metrics: Metrics,
}

/// Handle for controlling a running `JobsManager`.
pub struct JobsManagerHandle {
    cancel_token: CancellationToken,
    join_set: JoinSet<Result<(), InternalError>>,
}

impl JobsManagerHandle {
    /// Gracefully stop workers and wait for completion.
    pub async fn shutdown(mut self) -> Result<(), Error> {
        self.cancel_token.cancel();
        self.wait().await
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
                    error!("Worker panicked: {}", e);
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
    const WORKER_START_STAGGER_MS: u64 = 50;

    // TODO(med): we need to allocate an asynchronous API for managing and configuring jobs. So the
    // client will be able to change job settings without restarting. TODO(low): consider reducing
    // the number of parameters for manager startup
    /// Creates a `JobsManager` bound to the given storage backend and job registry.
    ///
    /// Workers are not spawned until [`JobsManager::start`] is called.
    ///
    /// # Errors
    ///
    /// Returns an error if `config.worker_count` is 0.
    pub fn new(
        storage: Arc<dyn Storage>,
        config: JobsManagerConfig,
        job_registry: Arc<JobRegistry>,
        metrics: Metrics,
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
    pub fn start(&self) -> Result<JobsManagerHandle, Error> {
        info!("Starting jobmanager with {} workers", self.config.worker_count);

        let cancel_token = CancellationToken::new();
        let mut join_set = JoinSet::new();

        // TODO(med): dynamic worker count - reduce workers when there's little work to minimize storage
        // requests
        for i in 0..self.config.worker_count {
            let worker = Worker::new(
                Arc::clone(&self.job_registry),
                Arc::clone(&self.storage),
                self.config.worker_config.clone(),
                self.metrics.clone(),
            );

            let token = cancel_token.clone();
            let worker_id = i;
            let worker_start_delay = worker_start_delay(i, Self::WORKER_START_STAGGER_MS);

            join_set.spawn(async move {
                if !worker_start_delay.is_zero() {
                    tokio::select! {
                        () = token.cancelled() => return Ok(()),
                        () = sleep(worker_start_delay) => {}
                    }
                }

                // TODO(high): decide what to do with the panic, now the worker is dying.
                if let Err(e) = worker.start(token).await {
                    tracing::error!("Worker {} stopped with error: {}", worker_id, e);
                    Err(e)
                } else {
                    Ok(())
                }
            });
        }

        Ok(JobsManagerHandle { cancel_token, join_set })
    }
}

fn worker_start_delay(worker_index: usize, stagger_ms: u64) -> std::time::Duration {
    let worker_index_u64 = u64::try_from(worker_index).unwrap_or(u64::MAX);
    std::time::Duration::from_millis(worker_index_u64.saturating_mul(stagger_ms))
}
