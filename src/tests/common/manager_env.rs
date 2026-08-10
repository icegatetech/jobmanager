use std::{collections::HashMap, sync::Arc, time::Duration};

use crate::{
    JobCode, JobDefinition, JobRegistry, JobsManager, JobsManagerConfig, JobsManagerHandle, NoopMetrics, Storage,
};

/// `ManagerEnv` manages `JobsManager` lifecycle for integration tests
pub struct ManagerEnv {
    storage: Arc<dyn Storage>,
    job_registry: HashMap<JobCode, JobDefinition>,
    manager_handle: Option<JobsManagerHandle>,
}

impl ManagerEnv {
    /// Create a new `ManagerEnv`
    pub fn new(
        storage: Arc<dyn Storage>,
        config: JobsManagerConfig,
        job_registry_handle: Arc<JobRegistry>,
        job_registry: Vec<JobDefinition>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let manager = JobsManager::new(storage.clone(), config, job_registry_handle, Arc::new(NoopMetrics))?;
        let manager_handle = manager.start()?;

        // Store job definitions for later checking
        let job_defs_map: HashMap<_, _> = job_registry.into_iter().map(|def| (def.code().clone(), def)).collect();

        Ok(Self {
            storage,
            job_registry: job_defs_map,
            manager_handle: Some(manager_handle),
        })
    }

    /// Wait until every job with an iteration limit has run it out.
    ///
    /// Jobs without a limit are skipped - they never complete by definition. The pool's record of
    /// finished iterations covers the ones that ended before this call, so the timeout is a
    /// diagnostic for a job that never finishes, not a race the harness has to win.
    pub async fn wait_for_all_jobs_completion(&self, timeout: Duration) -> Result<(), Box<dyn std::error::Error>> {
        let handle = self.manager_handle.as_ref().ok_or("the manager was already stopped")?;

        let waits = self
            .job_registry
            .iter()
            .filter(|(_, job_def)| job_def.max_iterations().is_some())
            .map(|(job_code, _)| handle.wait_for_job_completion(job_code));

        tokio::time::timeout(timeout, futures_util::future::try_join_all(waits))
            .await
            .map_err(|_| "timeout waiting for jobs to complete")??;

        tracing::info!("All jobs completed");
        Ok(())
    }

    /// Stop the manager
    pub async fn stop(&mut self) {
        if let Some(handle) = self.manager_handle.take() {
            let _ = handle.shutdown().await;
        }
    }

    /// Get the storage instance
    pub fn storage(&self) -> &Arc<dyn Storage> {
        &self.storage
    }
}

// Automatically stop manager when dropped
impl Drop for ManagerEnv {
    fn drop(&mut self) {
        if let Some(mut handle) = self.manager_handle.take() {
            handle.abort();
        }
    }
}
