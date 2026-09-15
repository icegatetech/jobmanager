//! HTTP response classification shared by object-store backends.

use crate::StorageError;

/// What a response status amounts to for the caller that issued the request.
///
/// `304` is an outcome and not a failure, so it is a variant here rather than an error a backend
/// has to remember to intercept: a `match` over this type has to name it.
pub(crate) enum HttpOutcome {
    /// A conditional read found the object still at the version the condition named.
    NotModified,
    /// The request failed, and this is what the failure means.
    Failed(StorageError),
}

/// Classifies a response status; provider-specific diagnostics are supplied by the caller.
pub(crate) fn classify_http_status(status: u16, details: String) -> HttpOutcome {
    match status {
        304 => HttpOutcome::NotModified,
        401 | 403 => HttpOutcome::Failed(StorageError::Auth(details)),
        404 => HttpOutcome::Failed(StorageError::NotFound(details)),
        408 => HttpOutcome::Failed(StorageError::Timeout),
        412 => HttpOutcome::Failed(StorageError::ConcurrentModification(details)),
        429 => HttpOutcome::Failed(StorageError::RateLimited),
        500 | 502 | 503 | 504 => HttpOutcome::Failed(StorageError::ServiceUnavailable),
        _ => HttpOutcome::Failed(StorageError::Backend(details)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The error a status the test states is a failure classifies to.
    fn classify_failed_status(status: u16) -> StorageError {
        match classify_http_status(status, "scripted".to_string()) {
            HttpOutcome::Failed(error) => error,
            HttpOutcome::NotModified => panic!("status {status} must classify as a failure"),
        }
    }

    #[test]
    fn a_conditional_read_of_the_same_version_is_not_a_failure() {
        assert!(matches!(
            classify_http_status(304, "scripted".to_string()),
            HttpOutcome::NotModified
        ));
    }

    #[test]
    fn the_status_decides_the_error_and_whether_it_is_repeated() {
        let cases: [(u16, fn(&StorageError) -> bool, bool); 11] = [
            (401, |e| matches!(e, StorageError::Auth(_)), false),
            (403, |e| matches!(e, StorageError::Auth(_)), false),
            (404, |e| matches!(e, StorageError::NotFound(_)), false),
            (408, |e| matches!(e, StorageError::Timeout), true),
            (412, |e| matches!(e, StorageError::ConcurrentModification(_)), false),
            (429, |e| matches!(e, StorageError::RateLimited), true),
            (500, |e| matches!(e, StorageError::ServiceUnavailable), true),
            (502, |e| matches!(e, StorageError::ServiceUnavailable), true),
            (503, |e| matches!(e, StorageError::ServiceUnavailable), true),
            (504, |e| matches!(e, StorageError::ServiceUnavailable), true),
            (418, |e| matches!(e, StorageError::Backend(_)), false),
        ];

        for (status, is_expected_variant, is_retryable) in cases {
            let error = classify_failed_status(status);

            assert!(is_expected_variant(&error), "status {status} mapped to {error:?}");
            assert_eq!(error.is_retryable(), is_retryable, "status {status} gave {error:?}");
        }
    }

    #[test]
    fn a_failed_precondition_reads_as_a_conflict() {
        assert!(classify_failed_status(412).is_conflict());
    }
}
