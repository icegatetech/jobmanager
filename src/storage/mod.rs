pub(crate) mod cached;
pub(crate) mod in_memory;
pub(crate) mod s3;
pub(crate) mod s3_error;

use async_trait::async_trait;
use thiserror::Error as ThisError;
use tokio_util::sync::CancellationToken;

use crate::infra::retrier::RetryError;
use crate::{Error, Job, JobCode, JobDefinition};

// StorageError - storage-specific errors
#[derive(ThisError, Debug, Clone)]
pub(crate) enum StorageError {
    #[error("job not found: {0}")]
    NotFound(String),

    #[error("concurrent modification detected: {0}")]
    ConcurrentModification(String),

    #[error("serialization error: {0}")]
    Serialization(String),

    #[error("s3 error: {0}")]
    S3(String),

    #[error("timeout")]
    Timeout,

    #[error("operation cancelled")]
    Cancelled,

    #[error("rate limited")]
    RateLimited,

    #[error("service unavailable")]
    ServiceUnavailable,

    #[error("auth error: {0}")]
    Auth(String),

    #[error("retry attempts spent: {0}")]
    AttemptsSpent(#[source] Box<Self>),

    #[error("{0}")]
    Other(String),
}

impl StorageError {
    pub const fn is_retryable(&self) -> bool {
        matches!(self, Self::Timeout | Self::RateLimited | Self::ServiceUnavailable)
    }

    pub const fn is_conflict(&self) -> bool {
        matches!(self, Self::ConcurrentModification(_))
    }
}

impl RetryError for StorageError {
    fn from_cancellation() -> Self {
        Self::Cancelled
    }

    fn from_spent_attempts(last_cause: Self) -> Self {
        Self::AttemptsSpent(Box::new(last_cause))
    }
}

pub(crate) type StorageResult<T> = Result<T, StorageError>;

// JobMeta - lightweight job metadata without full job state
#[derive(Debug, Clone)]
pub(crate) struct JobMeta {
    pub code: JobCode,
    pub iter_num: u64,
    pub version: String,
}

/// Storage backend trait for persisting job state.
#[async_trait]
#[allow(private_interfaces)] // TODO(low): think about how to open Job to public
pub trait Storage: Send + Sync {
    /// Get latest job by code
    async fn get_job(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<Job>;

    /// Get job by specific metadata
    async fn get_job_by_meta(&self, job_meta: &JobMeta, cancel_token: &CancellationToken) -> StorageResult<Job>;

    /// Find job metadata without loading full job
    async fn find_job_meta(&self, job_code: &JobCode, cancel_token: &CancellationToken) -> StorageResult<JobMeta>;

    /// Save job with optimistic locking. Updates job.version on success. Returns `ConcurrentModification` if version mismatch.
    async fn save_job(&self, job: &mut Job, cancel_token: &CancellationToken) -> StorageResult<()>;

    /// List the persisted iteration numbers of `job_code` that are not newer than
    /// `retention_boundary`.
    ///
    /// An empty result means there is nothing to delete. Persisted objects the backend cannot
    /// attribute to an iteration - an unparsable name, or the extension of a different state
    /// codec - are skipped rather than reported, so iterations written under another codec are
    /// never listed and therefore never cleaned up.
    ///
    /// Unlike [`Storage::get_job`] and [`Storage::save_job`], this does **not** retry internally:
    /// the caller owns the retry policy, because cleanup must stay strictly bounded and must not
    /// hold up the work that triggered it.
    ///
    /// The caller guarantees `retention_boundary` is strictly below the job's current iteration
    /// number; the backend does not re-derive it.
    async fn list_job_outdated_iterations(
        &self,
        job_code: &JobCode,
        retention_boundary: u64,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u64>>;

    /// Delete the given iterations of `job_code`.
    ///
    /// Idempotent: an iteration that is already gone is not an error, and an empty `iter_nums` is
    /// a no-op. Does **not** retry internally, for the same reason as
    /// [`Storage::list_job_outdated_iterations`].
    async fn delete_job_iterations(
        &self,
        job_code: &JobCode,
        iter_nums: &[u64],
        cancel_token: &CancellationToken,
    ) -> StorageResult<()>;
}

/// Provides job definitions for storage enrichment.
pub trait JobDefinitionRegistry: Send + Sync {
    /// Looks up the definition registered for `code`.
    ///
    /// Storage backends call this while deserializing persisted state, to recover the
    /// iteration/task limits that are not themselves stored in the job object. Returns an error
    /// if no definition was registered for `code`.
    fn get_job(&self, code: &JobCode) -> Result<JobDefinition, Error>;
}
