//! Distributed job and task manager with S3-backed state.
//!
//! A *job* is a named unit of work made of one or more *tasks*. Each [`JobsManager`] runs a pool
//! of workers that poll a storage backend for pickable jobs, execute their tasks one at a time
//! per worker (multiple workers may run different tasks of the same job concurrently), and
//! persist the updated state back to storage.
//!
//! Job state is a single object per job, so any number of workers can operate on the same job
//! without an external lock service: [`S3Storage`] round-trips it as one S3 object and resolves
//! concurrent writers with conditional writes (`If-Match`/`If-None-Match` on the object's `ETag`),
//! surfacing a lost race as [`Error`] rather than silently overwriting another worker's update.
//! [`CachedStorage`] wraps any `Storage` to skip redundant reads when the cached state is still
//! current, and [`InMemoryStorage`] is a single-job, no-persistence backend for tests.
//!
//! # Example
//!
//! ```no_run
//! use std::{collections::HashMap, sync::Arc, time::Duration};
//!
//! use chrono::Duration as ChronoDuration;
//! use jobmanager::{
//!     Error, JobCode, JobDefinition, JobRegistry, JobStateCodecKind, JobsManager,
//!     JobsManagerConfig, Metrics, RetrierConfig, S3Storage, S3StorageConfig, TaskCode,
//!     TaskDefinition, TaskExecutorFn,
//! };
//!
//! # async fn run() -> Result<(), Error> {
//! // 1. Define a task and map it to an executor.
//! let task_def = TaskDefinition::new(
//!     TaskCode::new("my task code"),
//!     Vec::new(),
//!     ChronoDuration::seconds(5),
//! )?;
//! let task_executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
//!     let task_id = *task.id();
//!     Box::pin(async move { manager.complete_task(&task_id, b"done".to_vec()) })
//! });
//! let mut task_executors = HashMap::new();
//! task_executors.insert(TaskCode::new("my task code"), task_executor);
//!
//! // 2. Define the job and register it.
//! let job_def = JobDefinition::new(JobCode::new("simple job"), vec![task_def], task_executors)?;
//! let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
//!
//! // 3. Point at S3-backed storage for job state.
//! let storage = S3Storage::new(
//!     S3StorageConfig {
//!         endpoint: "http://localhost:9000".to_string(),
//!         access_key_id: "rustfsadmin".to_string(),
//!         secret_access_key: "rustfsadmin".to_string(),
//!         bucket_name: "jobs".to_string(),
//!         use_ssl: false,
//!         region: "us-east-1".to_string(),
//!         bucket_prefix: "simple-json".to_string(),
//!         job_state_codec: JobStateCodecKind::Json,
//!         request_timeout: Duration::from_secs(5),
//!         retrier_config: RetrierConfig::default(),
//!     },
//!     job_registry.clone(),
//!     Metrics::new_disabled(),
//! )
//! .await?;
//!
//! // 4. Start the manager; it polls storage and runs tasks until shut down.
//! let manager = JobsManager::new(
//!     Arc::new(storage),
//!     JobsManagerConfig::default(),
//!     job_registry,
//!     Metrics::new_disabled(),
//! )?;
//! let handle = manager.start()?;
//! handle.shutdown().await?;
//! # Ok(())
//! # }
//! ```
#![allow(clippy::redundant_pub_crate)]

pub(crate) mod core;
mod execution;
mod infra;
mod storage;

// `pub` re-exports form the crate's public API; `pub(crate)` ones are short paths used
// across the crate. rustfmt sorts them into one alphabetical list, so the two kinds are
// interleaved on purpose — read the visibility, not the position.
pub use crate::core::error::{Error, Result};
pub(crate) use crate::core::error::{InternalError, JobError};
pub(crate) use crate::core::job::Job;
pub(crate) use crate::core::job::TaskPickup;
pub use crate::core::job::{JobCode, JobDefinition, JobStatus, TaskLimits};
pub use crate::core::registry::{JobRegistry, TaskExecutorFn};
pub(crate) use crate::core::task::Task;
pub use crate::core::task::{DEFAULT_MAX_ATTEMPTS, ImmutableTask, TaskCode, TaskDefinition, TaskStatus};
pub use crate::execution::job_manager::JobManager;
pub use crate::execution::jobs_manager::{JobsManager, JobsManagerConfig, JobsManagerHandle};
pub(crate) use crate::execution::worker::Worker;
pub use crate::execution::worker::WorkerConfig;
pub use crate::infra::metrics::Metrics;
pub(crate) use crate::infra::retrier::Retrier;
pub use crate::infra::retrier::RetrierConfig;
pub use crate::storage::JobDefinitionRegistry;
pub use crate::storage::cached::CachedStorage;
pub use crate::storage::in_memory::InMemoryStorage;
pub use crate::storage::s3::{JobStateCodecKind, S3Storage, S3StorageConfig};
pub(crate) use crate::storage::{JobMeta, Storage, StorageError, StorageResult};

#[cfg(test)]
mod tests;
