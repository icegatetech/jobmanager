//! Distributed job and task manager that keeps its state in an object store.
//!
//! A *job* is a named unit of work made of one or more *tasks*. Each [`JobsManager`] runs a pool
//! of workers that poll a storage backend for pickable jobs, execute their tasks one at a time
//! per worker (multiple workers may run different tasks of the same job concurrently), and
//! persist the updated state back to storage.
//!
//! Job state is a single object per job, so any number of workers can operate on the same job
//! without an external lock service: an object backend round-trips it as one object and resolves
//! concurrent writers with conditional writes (`If-Match`/`If-None-Match` on the object's `ETag`),
//! surfacing a lost race as [`Error`] rather than silently overwriting another worker's update.
//! A read cache sits in front of it by default, skipping the fetch while the cached state is still
//! current; [`JobsManagerBuilder::no_cache`] removes it and
//! [`JobsManagerBuilder::in_memory`] swaps the whole backend for a no-persistence one.
//!
//! Each backend is a Cargo feature, and at least one has to be selected: `storage-s3` is the one
//! the default set carries and what `JobsManagerBuilder::s3` needs, while `storage-azure` and
//! `storage-gcs` carry the Azure Blob Storage and Google Cloud Storage backends behind
//! `JobsManagerBuilder::azure` and `JobsManagerBuilder::gcs`. The README lists them.
//!
//! # Example
//!
//! [`examples/`] holds a worked version of every shape the crate is used in, including one per
//! storage backend.
//!
//! [`examples/`]: https://github.com/icegatetech/jobmanager/tree/main/examples
// Each example is gated from the outside because a `#[cfg]` written inside a doc test names the
// features of the doc test's own crate rather than this crate's: a block selecting a backend from
// within would compile against whichever features the test binary happens to carry. A build
// carrying no backend at all is refused below, so at least one example is always shown.
#![cfg_attr(
    feature = "storage-s3",
    doc = r#"
```no_run
use jobmanager::prelude::*;
use jobmanager::S3Config;

# async fn run() -> Result<()> {
let manager = JobsManager::builder()
    .s3(S3Config::new(
        "http://localhost:9000",
        "access key",
        "secret key",
        "jobs",
        "us-east-1",
    ))
    .workers(4)
    .job("simple job", |j| {
        j.add_task(
            TaskDefinition::new("my task code", Duration::from_secs(5)),
            task_fn(|_ctx| async move { Ok(b"done".to_vec().into()) }),
        );
    })
    .build()
    .await?;

let handle = manager.start()?;
handle.shutdown().await?;
# Ok(())
# }
```
"#
)]
#![cfg_attr(
    feature = "storage-azure",
    doc = r#"
```no_run
use jobmanager::prelude::*;
use jobmanager::AzureConfig;

# async fn run() -> Result<()> {
let manager = JobsManager::builder()
    .azure(AzureConfig::new(
        "http://localhost:10000/devstoreaccount1",
        "devstoreaccount1",
        "account key",
        "jobs",
    ))
    .workers(4)
    .job("simple job", |j| {
        j.add_task(
            TaskDefinition::new("my task code", Duration::from_secs(5)),
            task_fn(|_ctx| async move { Ok(b"done".to_vec().into()) }),
        );
    })
    .build()
    .await?;

let handle = manager.start()?;
handle.shutdown().await?;
# Ok(())
# }
```
"#
)]
#![cfg_attr(
    feature = "storage-gcs",
    doc = r#"
```no_run
use jobmanager::prelude::*;
use jobmanager::GcsConfig;

# async fn run() -> Result<()> {
let manager = JobsManager::builder()
    .gcs(
        GcsConfig::new("https://storage.googleapis.com", "jobs")
            .with_project_id("my project"),
    )
    .workers(4)
    .job("simple job", |j| {
        j.add_task(
            TaskDefinition::new("my task code", Duration::from_secs(5)),
            task_fn(|_ctx| async move { Ok(b"done".to_vec().into()) }),
        );
    })
    .build()
    .await?;

let handle = manager.start()?;
handle.shutdown().await?;
# Ok(())
# }
```
"#
)]
#![allow(clippy::redundant_pub_crate)]

// `in_memory()` persists nothing, so a build that selects no object backend cannot run work at all.
// Refusing it here names the cause; without it the same build fails as a pile of unused items.
#[cfg(not(any(feature = "storage-s3", feature = "storage-azure", feature = "storage-gcs")))]
compile_error!("enable a storage backend: storage-s3, storage-azure or storage-gcs");

pub(crate) mod core;
mod execution;
mod infra;
mod storage;

// Types of other crates that the public API hands out or takes in. They are re-exported so a
// consumer has one right path to each of them; the version of any of these is part of this crate's
// contract, so raising one is a breaking change of this crate.
pub use chrono::{DateTime, Utc};
#[cfg(feature = "metrics-otel")]
pub use opentelemetry::metrics::Meter;
pub use tokio_util::sync::CancellationToken;
pub use uuid::Uuid;

// `pub` re-exports form the crate's public API; `pub(crate)` ones are short paths used
// across the crate. rustfmt sorts them into one alphabetical list, so the two kinds are
// interleaved on purpose — read the visibility, not the position.
//
// A type is public only if the public API can produce or consume it. Everything reachable solely
// through `JobsManagerBuilder`'s internals - the registry, the job definition, the assembled
// backends, the config structs the builder fills in - stays crate-internal, because the builder
// took over their construction and `Storage` itself is not public (see `storage/mod.rs`).
pub use crate::core::error::{Error, Result, TaskError};
pub(crate) use crate::core::error::{InternalError, JobError};
pub use crate::core::job::{DEFAULT_ITERATION_RETENTION, IterationVerdict, JobCode, TaskLimits};
pub(crate) use crate::core::job::{IterationStep, Job, JobDefinition, JobDefinitionId, JobStatus, TaskPickup};
pub(crate) use crate::core::registry::JobRegistry;
pub use crate::core::task::{
    DEFAULT_LIFETIME_MULTIPLIER, DEFAULT_MAX_ATTEMPTS, DependencyTolerance, ImmutableTask, TaskCode, TaskDefinition,
    TaskRef, TaskResolution, TaskRetry,
};
pub(crate) use crate::core::task::{RestoredTask, Task, TaskStatus};
pub use crate::execution::builder::{JobBuilder, JobsManagerBuilder};
pub use crate::execution::executor::{TaskExecutor, TaskOutcome, TaskResult, task_fn};
pub(crate) use crate::execution::job_cleaner::JobCleaner;
pub use crate::execution::job_cleaner::JobCleanerConfig;
pub use crate::execution::job_handle::JobHandle;
pub(crate) use crate::execution::jobs_manager::JobsManagerConfig;
pub use crate::execution::jobs_manager::{JobsManager, JobsManagerHandle};
pub use crate::execution::task_context::TaskContext;
pub(crate) use crate::execution::worker::{FinishedIterationSink, Worker, WorkerConfig};
pub use crate::infra::metrics::{MetricsSink, NoopMetrics};
#[cfg(feature = "metrics-otel")]
pub use crate::infra::metrics_otel::OtelMetrics;
pub use crate::infra::retrier::RetrierConfig;
pub(crate) use crate::infra::retrier::{Retrier, RetryStep};
pub(crate) use crate::storage::JobDefinitionRegistry;
#[cfg(feature = "storage-azure")]
pub(crate) use crate::storage::azure::AzureBackend;
#[cfg(feature = "storage-azure")]
pub use crate::storage::azure::AzureConfig;
pub(crate) use crate::storage::cached::CachedStorage;
#[cfg(feature = "storage-gcs")]
pub(crate) use crate::storage::gcs::GcsBackend;
#[cfg(feature = "storage-gcs")]
pub use crate::storage::gcs::GcsConfig;
pub(crate) use crate::storage::in_memory::InMemoryStorage;
#[cfg(feature = "storage-s3")]
pub(crate) use crate::storage::s3::S3Backend;
#[cfg(feature = "storage-s3")]
pub use crate::storage::s3::S3Config;
pub use crate::storage::state_codec::JobStateCodecKind;
pub(crate) use crate::storage::{JobMeta, Storage, StorageError, StorageResult};

#[cfg(test)]
mod tests;

/// Everything a typical user needs, in one import.
///
/// ```
/// use jobmanager::prelude::*;
/// ```
///
/// A storage backend's configuration is not here: which one a pool uses is a deliberate choice,
/// so it is spelled out by its full path.
pub mod prelude {
    pub use std::time::Duration;

    pub use crate::{
        DependencyTolerance, Error, JobCode, JobHandle, JobsManager, JobsManagerHandle, Result, TaskCode, TaskContext,
        TaskDefinition, TaskError, TaskExecutor, TaskOutcome, TaskResult, TaskRetry, task_fn,
    };
}
