//! Translation of what an S3 backend reports into [`StorageError`].
//!
//! Kept apart from [`S3Storage`](super::s3::S3Storage), which owns talking to the store in terms of
//! jobs and iterations: deciding what a failure *means* - above all whether repeating the request
//! can clear it - is a property of the error, not of the conversation that produced it.
//!
//! S3 reports a failure in two shapes, and both land here: the status of a failed request, and the
//! per-key entries a multi-object delete puts inside a `200`.

use std::fmt::Write as _;

use aws_sdk_s3::{error::SdkError, types::Error as ObjectDeleteError};
use tracing::warn;

use crate::StorageError;

/// Failure entries a description names one by one before summarising the rest.
///
/// One multi-object delete carries up to a thousand keys and can fail on every one of them, while
/// its description lands both in a log line and in the error the caller finally gives up with -
/// naming a thousand keys would put tens of kilobytes into each, on every attempt.
const MAX_DESCRIBED_DELETE_FAILURES: usize = 10;

/// Storage error a failed S3 request amounts to.
///
/// The mapping exists so that [`StorageError::is_retryable`] can answer for an S3 backend: a status
/// the store itself will clear (`429`, `5xx`, a timeout, a dropped connection) becomes a retryable
/// variant, and everything else becomes one that is not.
pub(crate) fn map_s3_error<E: std::fmt::Debug>(err: &SdkError<E>) -> StorageError {
    // TODO(med): add job context to errors
    match err {
        SdkError::ServiceError(service_err) => {
            let status = service_err.raw().status().as_u16();
            let details = err.to_string();
            match status {
                401 | 403 => StorageError::Auth(details),
                404 => StorageError::NotFound(details),
                408 => StorageError::Timeout,
                412 => StorageError::ConcurrentModification(details),
                429 => StorageError::RateLimited,
                500 | 502 | 503 | 504 => StorageError::ServiceUnavailable,
                _ => StorageError::S3(format!("S3 SDK error: {err:?}")),
            }
        }
        SdkError::TimeoutError(_) => StorageError::Timeout,
        SdkError::DispatchFailure(_) => {
            // Network/connection errors are transient and should be retried.
            StorageError::ServiceUnavailable
        }
        _ => StorageError::S3(format!("S3 SDK error: {err:?}")),
    }
}

/// Whether a failed `GetObject` is the store answering "not modified" to a conditional read.
///
/// Kept apart from [`map_s3_error`] on purpose: `304` is not a failure to translate but the answer
/// the conditional read asks for, so it is recognised before the mapping and never reaches it.
pub(crate) fn is_not_modified<E: std::fmt::Debug>(err: &SdkError<E>) -> bool {
    matches!(err, SdkError::ServiceError(service_err) if service_err.raw().status().as_u16() == 304)
}

/// Storage error the per-object failures of one multi-object delete amount to, or `None` when the
/// response reported no failure at all.
///
/// A batch is repeated whole rather than key by key - `DELETE` is idempotent, so a key that already
/// went costs nothing to send again - which is why a single transient failure makes the whole batch
/// retryable. Without one the batch maps to [`StorageError::S3`], which
/// [`StorageError::is_retryable`] rejects, so a permanent failure is attempted exactly once and the
/// caller's attempt budget is left for failures that repeating can actually clear.
pub(crate) fn classify_delete_failures(failures: &[ObjectDeleteError]) -> Option<StorageError> {
    if failures.is_empty() {
        return None;
    }

    let details = describe_delete_failures(failures);

    if let Some(transient_error) = failures.iter().find_map(|failure| map_delete_failure_code(failure.code())) {
        // The transient variants carry no details of their own, and the caller logs only the error
        // it finally gives up on, so the failing keys are named here or nowhere.
        warn!("Transient failures in multi-object delete of job state objects: {details}");
        return Some(transient_error);
    }

    Some(StorageError::S3(format!(
        "Failed to delete job state objects: {details}"
    )))
}

/// The transient storage error a per-object delete failure `code` means, or `None` when that code
/// is permanent.
///
/// The transient set is the throttling and server-side subset of the S3 error codes. An unknown or
/// absent code counts as permanent, so a backend reporting something unexpected cannot make cleanup
/// spend its whole attempt budget on a failure no repetition clears.
fn map_delete_failure_code(code: Option<&str>) -> Option<StorageError> {
    match code? {
        "SlowDown" | "RequestLimitExceeded" => Some(StorageError::RateLimited),
        "RequestTimeout" | "RequestTimeoutException" => Some(StorageError::Timeout),
        "InternalError" | "ServiceUnavailable" | "PriorRequestNotComplete" => Some(StorageError::ServiceUnavailable),
        _ => None,
    }
}

/// Names the failure entries of a multi-object delete for a human reading a log or an error,
/// capped at [`MAX_DESCRIBED_DELETE_FAILURES`] and closed with the count of whatever it left out.
///
/// The code is spelled out next to the key because it, not the key, is what decides whether the
/// batch is worth repeating; a backend that supplies neither still yields a countable entry.
fn describe_delete_failures(failures: &[ObjectDeleteError]) -> String {
    let mut description = failures
        .iter()
        .take(MAX_DESCRIBED_DELETE_FAILURES)
        .map(|failure| {
            format!(
                "{} ({}: {})",
                failure.key().unwrap_or("unnamed object"),
                failure.code().unwrap_or("no code"),
                failure.message().unwrap_or("no message")
            )
        })
        .collect::<Vec<String>>()
        .join(", ");

    let omitted_count = failures.len().saturating_sub(MAX_DESCRIBED_DELETE_FAILURES);
    if omitted_count > 0 {
        // Writing into a String cannot fail, and swallowing that is what keeps a description
        // builder from returning a Result nobody can act on.
        let _ = write!(description, " (+{omitted_count} more)");
    }

    description
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds the entry a backend reports for one key it could not delete.
    fn build_delete_failure(key: &str, code: Option<&str>) -> ObjectDeleteError {
        let builder = ObjectDeleteError::builder().key(key).message("delete failed");
        match code {
            Some(code) => builder.code(code).build(),
            None => builder.build(),
        }
    }

    /// Classifies a batch that a test states does hold failures.
    fn classify_reported_failures(failures: &[ObjectDeleteError]) -> StorageError {
        classify_delete_failures(failures).expect("a reported failure must classify as an error")
    }

    /// A throttled key can succeed on the next attempt, so the whole batch has to stay retryable -
    /// otherwise the cleaner's bounded retry never runs and the tail waits for the next start-up.
    #[test]
    fn test_transient_partial_delete_failure_is_retryable() {
        let error = classify_reported_failures(&[build_delete_failure(
            "jobs/job/state-00000000000000000001.json",
            Some("SlowDown"),
        )]);

        assert!(
            matches!(error, StorageError::RateLimited),
            "a throttled key must map to the rate-limit error, got: {error:?}"
        );
        assert!(error.is_retryable(), "a throttled key must be retried");
    }

    #[test]
    fn test_permanent_partial_delete_failure_is_not_retryable() {
        let error = classify_reported_failures(&[build_delete_failure(
            "jobs/job/state-00000000000000000001.json",
            Some("AccessDenied"),
        )]);

        assert!(
            matches!(error, StorageError::S3(_)),
            "a rejected key must map to the plain S3 error, got: {error:?}"
        );
        assert!(!error.is_retryable(), "a rejected key must be attempted exactly once");
    }

    /// The batch is repeated whole, so one transient entry decides for the entries beside it.
    #[test]
    fn test_mixed_partial_delete_failures_are_retryable() {
        let error = classify_reported_failures(&[
            build_delete_failure("jobs/job/state-00000000000000000001.json", Some("AccessDenied")),
            build_delete_failure("jobs/job/state-00000000000000000002.json", Some("InternalError")),
        ]);

        assert!(
            matches!(error, StorageError::ServiceUnavailable),
            "a batch holding a server-side failure must map to it, got: {error:?}"
        );
        assert!(
            error.is_retryable(),
            "a batch holding a transient failure must be retried"
        );
    }

    /// A backend that reports a failure without a code says nothing about repeating it, and
    /// guessing would spend the cleaner's whole budget on requests that cannot start succeeding.
    #[test]
    fn test_partial_delete_failure_without_a_code_is_not_retryable() {
        let error =
            classify_reported_failures(&[build_delete_failure("jobs/job/state-00000000000000000001.json", None)]);

        assert!(!error.is_retryable(), "an unclassifiable failure must not be retried");
    }

    /// An unknown code is treated the same way as an absent one.
    #[test]
    fn test_unknown_partial_delete_failure_code_is_not_retryable() {
        let error = classify_reported_failures(&[build_delete_failure(
            "jobs/job/state-00000000000000000001.json",
            Some("SomeBackendSpecificCode"),
        )]);

        assert!(!error.is_retryable(), "an unknown code must not be retried");
    }

    /// The delete that reported nothing is the ordinary case, and it must not be turned into an
    /// error describing an empty list of failures.
    #[test]
    fn test_delete_without_reported_failures_is_not_an_error() {
        assert!(
            classify_delete_failures(&[]).is_none(),
            "a delete that reported no failure is a success"
        );
    }

    /// A whole batch can fail at once, and the description reaches both a log line and the error the
    /// caller gives up with - hence the cap, asserted on the text because the text *is* the payload
    /// being bounded.
    #[test]
    fn test_description_of_a_large_failure_batch_names_a_capped_number_of_keys() {
        let failures: Vec<ObjectDeleteError> = (1..=25)
            .map(|iter_num| build_delete_failure(&format!("jobs/job/state-{iter_num:020}.json"), Some("SlowDown")))
            .collect();

        let description = describe_delete_failures(&failures);

        assert_eq!(
            description.matches("jobs/job/").count(),
            MAX_DESCRIBED_DELETE_FAILURES,
            "the description must name no more keys than the cap allows: {description}"
        );
        assert!(
            description.ends_with("(+15 more)"),
            "the keys left out must still be counted: {description}"
        );
    }
}
