use thiserror::Error;

use crate::{JobStatus, infra::retrier::RetryError, storage::StorageError};

/// Error type an executor returns.
///
/// Boxing is what makes `?` work on any foreign error inside an executor: the standard library
/// provides `From<E> for Box<dyn Error + Send + Sync>` for every `E: Error + Send + Sync`, while an
/// equivalent blanket impl on [`enum@Error`] would collide with the reflexive `From<T> for T`.
/// [`enum@Error`] itself satisfies those bounds, so an error of this crate converts through the
/// same operator.
pub type TaskError = Box<dyn std::error::Error + Send + Sync>;

/// Public error type for jobmanager operations.
#[derive(Error, Debug)]
pub enum Error {
    /// Serialization or deserialization failed.
    #[error(transparent)]
    Serialization(#[from] serde_json::Error),

    /// Operation was canceled.
    #[error("operation cancelled")]
    Cancelled,

    /// Task execution failed, carrying the error the executor returned.
    #[error(transparent)]
    TaskExecution(#[from] TaskError),

    /// Generic error message.
    #[error("{0}")]
    Other(String),
    // TODO(med): add config error
}

impl Error {
    /// Whether retrying the operation that produced this error can succeed.
    ///
    /// Always false today: every variant that reaches a caller is either terminal or already the
    /// result of the storage layer's own retries. It exists so callers branch on a decision rather
    /// than on variants, matching `StorageError::is_retryable`.
    pub const fn is_retryable(&self) -> bool {
        false
    }
}

/// Result type for public APIs.
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Error, Debug)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) enum InternalError {
    #[error(transparent)]
    Storage(StorageError),

    #[error(transparent)]
    Job(JobError),

    #[error("operation cancelled")]
    Cancelled,

    #[error("max retry attempts reached: {0}")]
    MaxAttemptsReached(#[source] Box<Self>),

    #[error("{0}")]
    Other(String),
}

#[derive(Error, Debug)]
#[allow(clippy::redundant_pub_crate)]
pub(crate) enum JobError {
    #[error("task not found")]
    TaskNotFound,

    #[error("task worker mismatch")]
    TaskWorkerMismatch,

    #[error("invalid job status transition from {from} to {to}")]
    InvalidStatusTransition { from: JobStatus, to: JobStatus },

    #[error("{0}")]
    Other(String),
}

impl From<JobError> for InternalError {
    fn from(err: JobError) -> Self {
        Self::Job(err)
    }
}

impl From<StorageError> for InternalError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::Cancelled => Self::Cancelled,
            _ => Self::Storage(err),
        }
    }
}

impl From<Error> for InternalError {
    fn from(err: Error) -> Self {
        match err {
            Error::Cancelled => Self::Cancelled,
            Error::Serialization(e) => Self::Other(e.to_string()),
            Error::TaskExecution(e) => Self::Other(e.to_string()),
            Error::Other(msg) => Self::Other(msg),
        }
    }
}

impl RetryError for InternalError {
    fn from_cancellation() -> Self {
        Self::Cancelled
    }

    fn from_spent_attempts(last_cause: Self) -> Self {
        Self::MaxAttemptsReached(Box::new(last_cause))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Error)]
    #[error("boom")]
    struct ForeignError;

    /// The point of boxing: an executor propagates an error type this crate has never heard of.
    #[test]
    fn foreign_error_converts_via_question_mark() {
        fn failing() -> std::result::Result<(), TaskError> {
            Err(ForeignError)?;
            Ok(())
        }

        let error = Error::from(failing().unwrap_err());
        assert!(matches!(error, Error::TaskExecution(_)));
        assert_eq!(error.to_string(), "boom");
    }

    /// The same operator has to keep working on this crate's own errors, which is what lets an
    /// executor write `ctx.job().add_task(..)?`.
    #[test]
    fn crate_error_converts_into_task_error() {
        fn failing() -> std::result::Result<(), TaskError> {
            Err(Error::Other("inner".into()))?;
            Ok(())
        }

        assert_eq!(failing().unwrap_err().to_string(), "inner");
    }

    #[test]
    fn no_public_error_is_retryable() {
        assert!(!Error::Cancelled.is_retryable());
        assert!(!Error::Other("x".into()).is_retryable());
        assert!(!Error::TaskExecution(Box::new(ForeignError)).is_retryable());
    }

    /// The boxed cause must survive the conversion into the internal error, not be flattened to a
    /// placeholder: the worker records that text as the task's failure reason.
    #[test]
    fn task_execution_keeps_its_message_in_the_internal_error() {
        let internal = InternalError::from(Error::TaskExecution(Box::new(ForeignError)));
        assert!(matches!(internal, InternalError::Other(ref msg) if msg == "boom"));
    }
}
