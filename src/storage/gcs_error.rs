//! Translation of what the Google Cloud Storage JSON API reports into [`StorageError`].
//!
//! Kept apart from [`GcsBackend`](super::gcs::GcsBackend) for the same reason
//! [`azure_error`](super::azure_error) is kept apart from the Azure backend: what a failure *means* -
//! above all whether repeating the request can clear it - is a property of the error, not of the
//! conversation that produced it.

use crate::StorageError;
use crate::storage::backend::{ProviderError, RequestStatus};
use crate::storage::gcs_client::{GcsRequestError, JsonApiResponse};
use crate::storage::http_error::{HttpOutcome, classify_http_status};

/// Bytes of a refused answer a description quotes.
///
/// The JSON API puts its reason in the body, and the description reaches both a log line and the
/// error a caller finally gives up with - a store answering with an HTML error page would otherwise
/// put the whole page into each of them, on every attempt.
const MAX_DESCRIBED_BODY: usize = 512;

/// What one request of the JSON API failed with.
///
/// A refusal by the store is a status inside an answer that arrived whole, so it is a variant here
/// rather than something the transport reported: the three ways one request of this provider can
/// fail are one type because
/// [`send_request`](crate::storage::backend::send_request) bounds one request with
/// one error.
pub(crate) enum GcsFailure {
    /// The request never came back with an answer at all.
    Unanswered(GcsRequestError),
    /// The store answered, and the status it answered with is not one the operation asked for.
    Refused {
        status: u16,
        /// What the store said, as a consumer reads it - see [`describe_refusal`].
        details: String,
    },
    /// The store answered, and the answer did not carry what the operation asked for - a listing
    /// whose page does not parse, a write whose metadata does not. Carries the whole storage error
    /// because what such an answer amounts to is decided per operation.
    Unusable(StorageError),
}

impl ProviderError for GcsFailure {
    /// The mapping exists so that [`StorageError::is_retryable`] can answer for this backend: a
    /// status the store itself will clear (`429`, `5xx`) becomes a retryable variant, and everything
    /// else becomes one that is not.
    fn into_storage_error(self) -> StorageError {
        match self {
            Self::Unanswered(error) => map_request_error(&error),
            Self::Refused { status, details } => map_response_status(status, details),
            Self::Unusable(error) => error,
        }
    }

    fn recorded_status(&self) -> RequestStatus {
        match self {
            // The authorization a request needs was never produced, so nothing reached the store,
            // and a quota states what the store was asked for.
            Self::Unanswered(GcsRequestError::Unauthorized { .. }) => RequestStatus::NotSent,
            Self::Refused { status, .. } => RequestStatus::Answered(*status),
            // A transport that gave up has no status to name, and an unusable answer has one the
            // request must not carry: it was a successful status, and labelling the request with it
            // would say the answer was the one the operation asked for.
            Self::Unanswered(GcsRequestError::Transport(_)) | Self::Unusable(_) => RequestStatus::Failed,
        }
    }

    /// See [`is_not_modified`].
    fn is_not_modified(&self) -> bool {
        matches!(self, Self::Refused { status, .. } if is_not_modified(*status))
    }

    /// See [`is_absent_status`].
    fn is_object_gone(&self) -> bool {
        matches!(self, Self::Refused { status, .. } if is_absent_status(*status))
    }
}

/// Whether the store answered the request rather than refusing it.
pub(crate) const fn is_successful(status: u16) -> bool {
    200 <= status && status < 300
}

/// Whether a status is the store answering "not modified" to a conditional read.
///
/// Recognised before [`map_response_status`] rather than inside it, for the reason
/// [`HttpOutcome::NotModified`] carries.
pub(crate) fn is_not_modified(status: u16) -> bool {
    matches!(classify_http_status(status, String::new()), HttpOutcome::NotModified)
}

/// Whether a status is the store saying what the request addressed was not there. Which of the two
/// - the bucket or the object - is left to the caller, the status naming neither.
///
/// Both callers need it: a bucket check reads it as a bucket that has yet to be created, and a
/// delete as a key that is already gone, where S3 answers `204` - which is what keeps iteration
/// cleanup idempotent on this provider.
pub(crate) fn is_absent_status(status: u16) -> bool {
    matches!(
        classify_http_status(status, String::new()),
        HttpOutcome::Failed(StorageError::NotFound(_))
    )
}

/// Whether a status is the store saying somebody created the bucket first.
///
/// The shared table has no entry for `409`, because on this API it answers for nothing else: a
/// refused write condition arrives as `412`, so the only creation this crate can lose is the
/// bucket's - and losing it is the outcome the pool wanted anyway.
pub(crate) const fn is_lost_creation_race(status: u16) -> bool {
    status == 409
}

/// Storage error the status of a refused answer amounts to.
///
/// The mapping exists so that [`StorageError::is_retryable`] can answer for this backend: a status
/// the store itself will clear (`429`, `5xx`) becomes a retryable variant, and everything else
/// becomes one that is not.
pub(crate) fn map_response_status(status: u16, details: String) -> StorageError {
    match classify_http_status(status, details) {
        HttpOutcome::Failed(error) => error,
        // Arriving here means a caller that issued a conditional request does not read its answer -
        // see `is_not_modified`.
        HttpOutcome::NotModified => StorageError::Other(format!(
            "conditional read answer reached error mapping under status {status}"
        )),
    }
}

/// Storage error a request that never came back with an answer amounts to.
pub(crate) fn map_request_error(error: &GcsRequestError) -> StorageError {
    match error {
        // Credentials that failed on the way to the token endpoint rather than on their own
        // content: the refresh behind them runs on, so the next attempt can carry headers this one
        // had none of. Reported as `Auth`, one lost connection there ends the call that asked.
        GcsRequestError::Unauthorized { is_transient: true, .. } => StorageError::ServiceUnavailable,
        // What the credentials answer for themselves - a key that cannot sign, a file that is not
        // there. The text names the remedy, and no attempt changes it.
        GcsRequestError::Unauthorized { details, .. } => StorageError::Auth(details.clone()),
        // A request the transport gave up on is one the store never refused: a connection that was
        // not made, or an answer that did not arrive whole, and both clear on their own.
        GcsRequestError::Transport(error) if error.is_timeout() => StorageError::Timeout,
        // A request the client could not build reached no store, and repeating it changes nothing:
        // it carries the transport's own text, where a retryable variant would carry none.
        GcsRequestError::Transport(error) if error.is_builder() => {
            StorageError::Backend(format!("gcs request could not be built: {error}"))
        }
        GcsRequestError::Transport(_) => StorageError::ServiceUnavailable,
    }
}

/// Names what the store answered, for an error a consumer reads and for the log line before it.
pub(crate) fn describe_refusal(operation: &str, key: &str, response: &JsonApiResponse) -> String {
    let described_body = String::from_utf8_lossy(&response.body[..response.body.len().min(MAX_DESCRIBED_BODY)]);

    format!(
        "gcs {operation} of {key} answered {}: {described_body}",
        response.status
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An address no request can be built from is not a store that is briefly away: repeating it
    /// spends the whole attempt budget on an answer that cannot change, and the retryable variant
    /// carries no text saying what the address was.
    #[test]
    fn a_request_that_could_not_be_built_is_not_repeated_and_names_its_cause() {
        let builder_failure = reqwest::Client::new()
            .get("storage.googleapis.com/storage/v1/b/jobs")
            .build()
            .expect_err("an address without a scheme must not build into a request");
        assert!(builder_failure.is_builder(), "got: {builder_failure:?}");

        let error = map_request_error(&GcsRequestError::Transport(builder_failure));

        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
        assert!(!error.is_retryable(), "got: {error:?}");
    }

    /// Headers the credentials could not produce because the way to the token endpoint failed are
    /// the store being out of reach and not a pool whose key is wrong: the refresh behind them runs
    /// on, so the attempt after this one can carry a token.
    ///
    /// Checked by breaking it: mapping every unauthorized request to `Auth`, as this backend did
    /// before, fails this case - the call would end on the attempt that lost the connection.
    #[test]
    fn credentials_that_failed_on_the_way_to_the_token_endpoint_are_repeated() {
        let error = map_request_error(&GcsRequestError::Unauthorized {
            details: "a refresh that lost its connection".to_string(),
            is_transient: true,
        });

        assert!(error.is_retryable(), "got: {error:?}");
    }

    /// The other half of the same answer: credentials that answer for themselves name what a
    /// consumer has to fix, and no attempt of the `Retrier` changes a key that cannot sign.
    #[test]
    fn credentials_that_cannot_sign_are_not_repeated_and_name_their_cause() {
        let error = map_request_error(&GcsRequestError::Unauthorized {
            details: "a key that cannot sign".to_string(),
            is_transient: false,
        });

        let StorageError::Auth(details) = &error else {
            panic!("a permanent credentials failure must reach the caller as an auth failure, got: {error:?}")
        };
        assert!(details.contains("a key that cannot sign"), "got: {details}");
        assert!(!error.is_retryable(), "got: {error:?}");
    }

    /// A write the store refused is the ordinary end of a lost race, and a worker re-reads and
    /// merges on a conflict where it stops on anything else.
    #[test]
    fn a_refused_condition_reads_as_a_conflict() {
        assert!(map_response_status(412, "scripted".to_string()).is_conflict());
    }

    #[test]
    fn a_conditional_read_of_the_same_version_is_recognised_before_the_mapping() {
        assert!(is_not_modified(304));
        assert!(
            matches!(map_response_status(304, "scripted".to_string()), StorageError::Other(_)),
            "a 304 that reached the mapping means a conditional read that does not read its answer"
        );
    }

    /// Cleanup deletes iterations one by one and two instances may reconcile the same tail, so an
    /// object that is already gone is the ordinary case.
    #[test]
    fn a_delete_of_an_absent_object_is_recognised() {
        assert!(is_absent_status(404));
        assert!(!is_absent_status(200));
        assert!(!is_absent_status(412));
    }

    /// The predicate draws its line between `299` and `300`, and a status on either side of it is
    /// what says where the line is: `200`, `204`, `304` and `404` all hold under a predicate that
    /// widened to `status < 400`, which would read a redirect as an answer and its empty body as a
    /// state.
    #[test]
    fn only_the_answered_statuses_read_as_success() {
        assert!(is_successful(200));
        assert!(is_successful(204));
        assert!(is_successful(299));
        assert!(!is_successful(300));
        assert!(!is_successful(304));
        assert!(!is_successful(404));
    }

    /// The reason the API puts in the body is what a consumer reads, and a store answering with a
    /// page instead must not put the page into every attempt's error.
    #[test]
    fn a_refusal_is_described_with_a_bounded_part_of_its_body() {
        let response = JsonApiResponse {
            status: 500,
            generation: None,
            body: vec![b'x'; MAX_DESCRIBED_BODY * 2],
        };

        let description = describe_refusal("GET", "jobs/job/state-1.json", &response);

        assert!(description.contains("500"), "got: {description}");
        assert!(description.contains("jobs/job/state-1.json"), "got: {description}");
        assert_eq!(
            description.matches('x').count(),
            MAX_DESCRIBED_BODY,
            "the description must quote no more of the body than the cap allows"
        );
    }
}
