//! Translation of what Azure Blob Storage reports into [`StorageError`].
//!
//! Kept apart from [`AzureBackend`](super::azure::AzureBackend) for the same reason
//! [`s3_error`](super::s3_error) is kept apart from the S3 backend: what a failure *means* - above
//! all whether repeating the request can clear it - is a property of the error, not of the
//! conversation that produced it.

use azure_core::error::ErrorKind;
use azure_storage_blob::models::StorageErrorCode;

use crate::StorageError;
use crate::storage::backend::{ProviderError, RequestStatus};
use crate::storage::http_error::{HttpOutcome, classify_http_status};

impl ProviderError for azure_core::Error {
    /// The mapping exists so that [`StorageError::is_retryable`] can answer for this backend: a
    /// status the service itself will clear (`429`, `5xx`, a dropped connection) becomes a retryable
    /// variant, and everything else becomes one that is not.
    fn into_storage_error(self) -> StorageError {
        let err = &self;
        match err.kind() {
            ErrorKind::HttpResponse { status, error_code, .. } => {
                let status = u16::from(*status);
                // A conditional write the service refuses is answered `409 BlobAlreadyExists` as
                // often as `412`, and both mean the same thing to a worker: somebody else got there
                // first. Taken through the shared table instead, that `409` would become a permanent
                // failure and the loser of a race would stop rather than re-read and merge.
                if status == 409 && is_lost_creation_race(error_code.as_deref()) {
                    return StorageError::ConcurrentModification(format!("Azure storage error: {err}"));
                }
                // And `412` carries more than the version race the shared table reads it as - the
                // lease codes arrive under it too. Left to the table, a lease this crate never took
                // would be re-read and merged against for the whole attempt budget of a save.
                if status == 412 && !is_refused_condition(error_code.as_deref()) {
                    return StorageError::Backend(format!("Azure storage error: {err}"));
                }

                match classify_http_status(status, format!("Azure storage error: {err}")) {
                    HttpOutcome::Failed(error) => error,
                    // Arriving here means a caller that issued a conditional request does not name
                    // the answer as one it accepts - see `ProviderError::is_not_modified`.
                    HttpOutcome::NotModified => {
                        StorageError::Other(format!("conditional read answer reached error mapping: {err}"))
                    }
                }
            }
            // The request never reached the service, or its answer never came back whole.
            ErrorKind::Connection | ErrorKind::Io => StorageError::ServiceUnavailable,
            ErrorKind::Credential => StorageError::Auth(format!("Azure storage error: {err}")),
            ErrorKind::DataConversion | ErrorKind::Other => {
                StorageError::Backend(format!("Azure storage error: {err}"))
            }
        }
    }

    /// Every failure here reached the service or tried to, so none of them is unbilled: the two
    /// answers the store had no part in - a token that stopped a request before it was sent, and a
    /// timeout - are told by [`send_request`](crate::storage::backend::send_request)
    /// and never arrive as an SDK error.
    fn recorded_status(&self) -> RequestStatus {
        self.http_status().map_or(RequestStatus::Failed, |status| {
            RequestStatus::Answered(u16::from(status))
        })
    }

    /// A read this backend issues conditionally is answered `304` while the blob still stands at the
    /// version the condition named, which is an answer rather than a failure.
    fn is_not_modified(&self) -> bool {
        matches!(self.kind(), ErrorKind::HttpResponse { status, .. }
        if matches!(
            classify_http_status(u16::from(*status), String::new()),
            HttpOutcome::NotModified
        ))
    }

    /// `Delete Blob` answers `404` for a key that is no longer there, where S3 answers `204`, so
    /// this is what keeps iteration cleanup idempotent. A `404` the service attributes to the
    /// *container* is not that: the container is a precondition of the whole backend, and a delete
    /// that reports it missing is a failure to surface rather than to swallow.
    fn is_object_gone(&self) -> bool {
        matches!(self.kind(), ErrorKind::HttpResponse { status, error_code, .. }
            if u16::from(*status) == 404
                && error_code
                    .as_ref()
                    .is_none_or(|code| code == StorageErrorCode::BlobNotFound.as_ref()))
    }
}

/// Whether the error code of a `409` names a creation somebody else reached first.
///
/// The service spends `409` on more than a lost creation: an archived blob, a blob of the wrong
/// type, a container being deleted - none of which another read and another merge clears, so each
/// of them read as a race costs a save the whole attempt budget of its `Retrier` before surfacing.
///
/// The code is the `x-ms-error-code` header, which the service and the emulator both send. An
/// answer that carries a body but no such header is given its status line as a code rather than
/// none, so a store omitting the header reads as a failure here; `None` is left to the bodiless
/// answer, and counts as a race - the reading under which a worker re-reads and merges rather than
/// gives its iteration up.
fn is_lost_creation_race(error_code: Option<&str>) -> bool {
    error_code.is_none_or(|code| {
        code == StorageErrorCode::BlobAlreadyExists.as_ref()
            || code == StorageErrorCode::ContainerAlreadyExists.as_ref()
    })
}

/// Whether the error code of a `412` names the condition of the request as what was refused.
///
/// The lease codes arrive under `412` as well, and a lease is held by somebody outside this crate -
/// nothing a re-read and a merge reach. An absent code counts as the condition, and gets here the
/// way [`is_lost_creation_race`] describes.
fn is_refused_condition(error_code: Option<&str>) -> bool {
    error_code.is_none_or(|code| code == StorageErrorCode::ConditionNotMet.as_ref())
}

#[cfg(test)]
mod tests {
    use azure_core::http::{RawResponse, StatusCode, headers::Headers};

    use super::*;

    /// The error the SDK produces for a response the service refused, carrying the status and the
    /// error code a caller decides on.
    fn build_http_error(status: u16, error_code: Option<&str>) -> azure_core::Error {
        let status = StatusCode::from(status);
        ErrorKind::HttpResponse {
            status,
            error_code: error_code.map(ToString::to_string),
            raw_response: Some(Box::new(RawResponse::from_bytes(status, Headers::new(), "scripted"))),
        }
        .into_error()
    }

    /// The error the SDK produces when the request never reached the service at all.
    fn build_transport_error() -> azure_core::Error {
        ErrorKind::Connection.into_error()
    }

    #[test]
    fn the_status_decides_the_error_and_whether_it_is_repeated() {
        let cases: [(u16, fn(&StorageError) -> bool, bool); 12] = [
            (401, |e| matches!(e, StorageError::Auth(_)), false),
            (403, |e| matches!(e, StorageError::Auth(_)), false),
            (404, |e| matches!(e, StorageError::NotFound(_)), false),
            (408, |e| matches!(e, StorageError::Timeout), true),
            (409, |e| matches!(e, StorageError::ConcurrentModification(_)), false),
            (412, |e| matches!(e, StorageError::ConcurrentModification(_)), false),
            (429, |e| matches!(e, StorageError::RateLimited), true),
            (500, |e| matches!(e, StorageError::ServiceUnavailable), true),
            (502, |e| matches!(e, StorageError::ServiceUnavailable), true),
            (503, |e| matches!(e, StorageError::ServiceUnavailable), true),
            (504, |e| matches!(e, StorageError::ServiceUnavailable), true),
            (418, |e| matches!(e, StorageError::Backend(_)), false),
        ];

        for (status, is_expected_variant, is_retryable) in cases {
            let error = build_http_error(status, None).into_storage_error();

            assert!(is_expected_variant(&error), "status {status} mapped to {error:?}");
            assert_eq!(error.is_retryable(), is_retryable, "status {status} gave {error:?}");
        }
    }

    /// The two statuses a lost race arrives as have to be one outcome for the caller: a worker
    /// re-reads and merges on a conflict, and stops on anything else. Each status names the race in
    /// a code of its own - the creation the other writer won, the version this one no longer holds.
    #[test]
    fn both_statuses_of_a_refused_condition_read_as_a_conflict() {
        for (status, error_code) in [(409_u16, "BlobAlreadyExists"), (412_u16, "ConditionNotMet")] {
            assert!(
                build_http_error(status, Some(error_code)).into_storage_error().is_conflict(),
                "status {status} must reach the caller as a conflict"
            );
        }
    }

    /// The two creations a worker can lose - a job's first iteration, and the container a pool
    /// starts against - are what a `409` means when it names one of them.
    #[test]
    fn a_conflict_naming_a_creation_race_is_read_as_a_lost_race() {
        for error_code in ["BlobAlreadyExists", "ContainerAlreadyExists"] {
            assert!(
                build_http_error(409, Some(error_code)).into_storage_error().is_conflict(),
                "{error_code} must reach the caller as a conflict"
            );
        }
    }

    /// A `409` naming anything else is not a race, and reading it as one spends the whole attempt
    /// budget of a save on re-reads and merges that no repetition clears.
    ///
    /// Checked by breaking it: letting [`is_lost_creation_race`] answer every code brings the
    /// conflict back and this case fails.
    #[test]
    fn a_conflict_naming_no_creation_race_is_attempted_once() {
        let error = build_http_error(409, Some("BlobArchived")).into_storage_error();

        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
        assert!(!error.is_conflict(), "an archived blob is not a lost race");
        assert!(!error.is_retryable(), "an archived blob must be attempted exactly once");
    }

    /// A `412` naming a lease is the same class of defect on the other status: the lease is held
    /// outside this crate, and no re-read and no merge takes it away.
    ///
    /// Checked by breaking it: letting [`is_refused_condition`] answer every code sends this back
    /// through the shared table, where `412` is the version race, and the case fails.
    #[test]
    fn a_refused_condition_naming_a_lease_is_attempted_once() {
        let error = build_http_error(412, Some("LeaseIdMissing")).into_storage_error();

        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
        assert!(!error.is_conflict(), "a lease somebody else holds is not a lost race");
        assert!(
            !error.is_retryable(),
            "a lease somebody else holds must be attempted once"
        );
    }

    #[test]
    fn a_conditional_read_of_the_same_version_is_recognised_before_the_mapping() {
        let not_modified = build_http_error(304, None);

        assert!(not_modified.is_not_modified());
        assert!(
            matches!(build_http_error(304, None).into_storage_error(), StorageError::Other(_)),
            "a 304 that reached the mapping means a conditional read that does not read its answer"
        );
    }

    #[test]
    fn a_failure_without_a_status_is_repeated() {
        let error = build_transport_error().into_storage_error();

        assert!(matches!(error, StorageError::ServiceUnavailable), "got: {error:?}");
        assert!(error.is_retryable());
    }

    /// Cleanup deletes iterations one by one and two instances may reconcile the same tail, so a
    /// blob that is already gone is the ordinary case.
    #[test]
    fn a_delete_of_an_absent_blob_is_recognised() {
        assert!(build_http_error(404, Some("BlobNotFound")).is_object_gone());
        assert!(build_http_error(404, None).is_object_gone());
    }

    #[test]
    fn a_delete_reporting_an_absent_container_is_not_a_missing_blob() {
        assert!(!build_http_error(404, Some("ContainerNotFound")).is_object_gone());
    }
}
