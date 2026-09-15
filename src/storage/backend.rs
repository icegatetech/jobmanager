//! What a provider is asked to do, and the wrapper that bounds one request.
//!
//! This is the lower half of the split `AGENTS.md` states as "a backend maps state, it does not
//! decide it". A [`Backend`] answers requests; the decisions those requests carry out belong to
//! [`ObjectStorage`](super::object_storage::ObjectStorage), which is the only caller.
//!
//! What a backend owns: one provider operation per method and one request per call, the operation
//! that request is recorded under, which refusal that call accepts as the answer it asked for, what
//! its provider's failures mean - through [`ProviderError`] - and the numbers of its own provider:
//! page size, delete batch size, the kind of listing boundary.
//!
//! What every backend has in common lives in [`send_request`]: the timeout and the
//! cancellation around one request, the pair it is recorded under, and the point where a provider's
//! failure becomes either the accepted answer or a [`StorageError`](crate::StorageError). Three
//! copies of that ordering were three chances to record one request under two pairs.
//!
//! What it never owns: which key an iteration lives under, what a write is conditioned on, which
//! listed keys are outdated, when a call is retried. A backend that decided any of those would be
//! deciding it a second time, differently from the other two.

use std::{
    borrow::Cow,
    future::Future,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use tokio_util::sync::CancellationToken;

use crate::{MetricsSink, StorageError, StorageResult};

/// Status a request the store answered is recorded under.
const SUCCESS_STATUS: &str = "OK";

/// Status a failure that has no status of its own is recorded under.
const FAILED_STATUS: &str = "ERR";

/// Status a request abandoned on cancellation is recorded under. Apart from [`FAILED_STATUS`],
/// which answers for a store that failed: a pool being stopped is not an outage, and one status for
/// both would make every shutdown read as one.
const CANCELLED_STATUS: &str = "CANCELLED";

/// Removes the whitespace and quotes a provider returns around an object version, before the
/// version is held against a condition.
///
/// Belongs to the providers that version an object by its entity-tag; Google Cloud Storage versions
/// one by a generation, which carries no quotes and is not passed through here.
#[cfg(any(feature = "storage-s3", feature = "storage-azure"))]
pub(crate) fn normalize_etag(etag: &str) -> String {
    etag.trim().trim_matches('"').to_string()
}

/// Condition a state object is written under.
///
/// There is no unconditional variant: an unconditional write destroys a concurrent update with no
/// error anywhere, which is the first invariant in `AGENTS.md`.
pub(crate) enum PutCondition<'a> {
    /// The object must not exist yet - how a job's first iteration is created.
    CreateOnly,
    /// The stored version must equal the one named - how the current iteration is updated.
    MatchVersion(&'a str),
}

/// One state object a listing named.
pub(crate) struct ListedObject {
    pub key: String,
    /// Version the listing reported, absent where the provider left it out. The caller that needs
    /// it answers for the absence itself: a backend has no basis for deciding whether an entry
    /// without a version is a state object, and only `find_job_meta` reads the version at all.
    pub version: Option<String>,
}

/// One page of a listing.
pub(crate) struct ListedPage {
    pub objects: Vec<ListedObject>,
    /// What the next request has to carry to continue this listing. `None` means there are no more
    /// pages, so a caller that asks anyway pays for a request nobody owed.
    pub next_cursor: Option<String>,
}

/// What one request of a listing asks for.
pub(crate) struct ListingRequest<'a> {
    pub prefix: &'a str,
    /// Key of the listing boundary, as the provider's own bound understands it; `None` lists from
    /// the start of the prefix, which is what a listing after the newest object asks for.
    pub start_key: Option<&'a str>,
    pub page_size: i32,
    pub cursor: Option<String>,
}

/// Where a provider begins a listing relative to the boundary key it was given.
///
/// The difference is what keeps one boundary calculation serving three providers: the caller builds
/// the key this answer asks for, and nothing else about cleanup differs between them.
///
/// A store that drops the boundary it was given answers a wider listing and nothing else - which
/// iterations are deletable is decided over the listed keys, not here.
// Which variants a build constructs is decided by the backends its features selected, so a build
// carrying one of them leaves the other providers' bound unconstructed. Gating a variant per
// feature instead would make the `match` that has to answer for both grow arms only some builds
// can see.
#[allow(dead_code)]
pub(crate) enum ListingStartBound {
    /// The boundary key itself comes back in the answer.
    Inclusive,
    /// The boundary key is left out of the answer.
    Exclusive,
}

/// One provider, in the operations job state is kept with.
///
/// Every method is one request. An implementation maps an operation onto its provider and reports
/// what came back; it decides nothing about jobs or iterations, and nothing here takes a `Job`,
/// a `JobCode` or an iteration number - a key and a version are as far as the domain reaches.
#[async_trait]
pub(crate) trait Backend: Send + Sync {
    fn listing_start_bound(&self) -> ListingStartBound;

    /// Objects one listing page asks for, which is what this provider's configuration named.
    fn list_page_size(&self) -> i32;

    /// Writes `body` under `condition`, answering with the version the store gave it.
    ///
    /// # Errors
    ///
    /// Returns [`StorageError::ConcurrentModification`](crate::StorageError::ConcurrentModification)
    /// when the store refused the condition. A store that accepted the write and named no version
    /// is a failure as well - there is nothing left to hold the next condition against, and an empty
    /// version would make the next save an unconditional one.
    async fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        condition: PutCondition<'_>,
        cancel_token: &CancellationToken,
    ) -> StorageResult<String>;

    /// Reads the object whose stored version is `expected_version`.
    async fn get_object(
        &self,
        key: &str,
        expected_version: &str,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u8>>;

    /// Reads the object if its stored version differs from `known_version`, answering with that
    /// version and the body. `None` means the store still holds `known_version`.
    ///
    /// Kept apart from [`Backend::get_object`] rather than folded into it under a
    /// condition: a read of a named version cannot answer "unchanged", and one type for both would
    /// make every caller handle a state it cannot reach.
    async fn get_changed_object(
        &self,
        key: &str,
        known_version: &str,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<(String, Vec<u8>)>>;

    async fn list_objects(
        &self,
        request: ListingRequest<'_>,
        cancel_token: &CancellationToken,
    ) -> StorageResult<ListedPage>;

    /// Deletes the named objects. Idempotent: an object that is already gone is not a failure.
    ///
    /// Takes the whole set rather than one key because how many requests it becomes is the
    /// provider's own - a batch of a thousand on S3, one per key where the client has no batch -
    /// and that number is what the backend declares as its quota.
    async fn delete_objects(&self, keys: &[String], cancel_token: &CancellationToken) -> StorageResult<()>;
}

/// What a provider's own failure answers, so that one wrapper records, accepts and translates a
/// request the same way on every backend.
///
/// The translation belongs to the failure rather than to the call site because what a failure
/// *means* - above all whether repeating the request can clear it - is a property of the error and
/// not of the conversation that produced it. The two predicates are here for the reason a failure
/// stays untranslated until this point at all: [`StorageError`] keeps neither a status nor a
/// provider's error code, and both answers that are not failures are told apart by exactly those.
pub(crate) trait ProviderError {
    /// Storage error this failure amounts to, so that [`StorageError::is_retryable`] can answer for
    /// it.
    fn into_storage_error(self) -> StorageError;

    /// What this failure is recorded as.
    fn recorded_status(&self) -> RequestStatus;

    /// Whether this is the store answering "not modified" to a conditional read.
    fn is_not_modified(&self) -> bool;

    /// Whether this is the store saying the object was already gone.
    fn is_object_gone(&self) -> bool;
}

/// What one failed request is billed as.
///
/// The store names a status for a request it refused and none for one it never answered, and the
/// difference between "no status" and "no request" is what keeps a quota stating what the store was
/// asked for: a request that was never sent is not one the store bills.
// Which variants a build constructs is decided by the backends its features selected: only the
// Google Cloud Storage one can fail before it reaches the transport at all. Gating a variant per
// feature instead would make the `match` that has to answer for all of them grow arms only some
// builds can see.
#[allow(dead_code)]
pub(crate) enum RequestStatus {
    /// The store answered, naming this status.
    Answered(u16),
    /// The store failed the request without naming a status of its own.
    Failed,
    /// Nothing was sent, so the store never received the request and never bills it.
    NotSent,
}

impl RequestStatus {
    /// Label this status is recorded under, or `None` where nothing is recorded at all.
    fn into_label(self) -> Option<Cow<'static, str>> {
        match self {
            Self::Answered(status) => Some(Cow::Owned(status.to_string())),
            Self::Failed => Some(Cow::Borrowed(FAILED_STATUS)),
            Self::NotSent => None,
        }
    }
}

/// Beside a successful answer, the refusal the caller settled on as the answer it asked for - and
/// the value that stands for it, a refusal carrying none of its own.
///
/// Named before the request rather than matched on after it: what makes a refusal acceptable is the
/// operation that issued it, and stating it here is what keeps [`send_request`] the whole
/// of what a backend does with a request - one place that records, accepts and translates, instead
/// of a call site that repeats all three.
// Which variants a build constructs is decided by the backends its features selected, so a build
// carrying one of them leaves the other providers' accepted answers unconstructed. Gating a variant
// per feature instead would make the `match` that has to answer for all of them grow arms only some
// builds can see.
#[allow(dead_code)]
pub(crate) enum ExpectedAnswer<T> {
    /// Nothing beside a successful answer; every refusal is a failure.
    Success,
    /// A conditional read that found the object still at the version the condition named. Recorded
    /// under the store's own status rather than [`SUCCESS_STATUS`], because it is an outcome and
    /// not a success, and the two are counted apart on every backend.
    NotModified(T),
    /// An object that is already gone, which is the success cleanup asked for and is billed as one.
    ObjectGone(T),
}

/// What one request is sent and recorded under.
pub(crate) struct RequestOptions<'a> {
    /// Operation half of the pair a request is recorded under - `GET`, `PUT`, `LIST`, `DELETE`.
    pub operation: &'a str,
    pub metrics: &'a dyn MetricsSink,
    pub request_timeout: Duration,
}

/// What a refusal amounts to for a caller that named `accepted_answer`: the value it settled on or
/// the error the refusal translates to, and the status each of those is recorded under.
fn settle_refusal<T, E: ProviderError>(
    error: E,
    accepted_answer: ExpectedAnswer<T>,
) -> (Option<Cow<'static, str>>, StorageResult<T>) {
    match accepted_answer {
        ExpectedAnswer::NotModified(answer) if error.is_not_modified() => {
            (error.recorded_status().into_label(), Ok(answer))
        }
        ExpectedAnswer::ObjectGone(answer) if error.is_object_gone() => {
            (Some(Cow::Borrowed(SUCCESS_STATUS)), Ok(answer))
        }
        ExpectedAnswer::Success | ExpectedAnswer::NotModified(_) | ExpectedAnswer::ObjectGone(_) => {
            (error.recorded_status().into_label(), Err(error.into_storage_error()))
        }
    }
}

/// What one request can end as, once the timeout put around it is accounted for.
///
/// Kept apart from [`StorageError`] because two of the answers a request comes back with are not
/// failures at all - a conditional read that found nothing new, a delete of an object that is
/// already gone - and telling them apart needs what the provider reported rather than its
/// translation. Private to this module: [`send_request`] is where a request stops being
/// one of these and becomes an answer or a [`StorageError`].
enum CallError<E> {
    /// The request did not finish within the configured timeout.
    TimedOut,
    /// The caller's token was already cancelled when the request was handed over, so nothing was
    /// sent. Kept apart from [`CallError::Cancelled`] because a request the provider never received
    /// is one it never bills, and recording it would count a request nobody made.
    NotSent,
    /// The caller's token was cancelled while the request was in flight, so the request was dropped
    /// without an answer.
    Cancelled,
    /// The provider answered, and this is what it answered with.
    Refused(E),
}

/// Runs one request under `request_timeout` and `cancel_token`, answering with what the provider
/// said and nothing more: which of those answers is a failure is decided by
/// [`send_request`], and so is the pair the request is recorded under.
///
/// Cancellation drops the request where it stands rather than waiting the timeout out, which is what
/// keeps a pool shutting down from being held by whatever each of its workers had in flight.
/// `request` must therefore carry everything one request costs - the reading of a body, the parsing
/// of a listing - because what this abandons is the whole of it.
///
/// A token that is already cancelled when `request` arrives ends it as [`CallError::NotSent`]
/// instead: the two are told apart because one of them cost a request and the other did not.
async fn send_request_internal<T, E>(
    request: impl Future<Output = Result<T, E>> + Send,
    request_timeout: Duration,
    cancel_token: &CancellationToken,
) -> Result<T, CallError<E>> {
    if cancel_token.is_cancelled() {
        return Err(CallError::NotSent);
    }

    let answer = tokio::select! {
        // Biased so that a token cancelled under the request wins without the request being polled
        // again: the random order would otherwise let a shutting-down worker still reach the store.
        biased;
        () = cancel_token.cancelled() => return Err(CallError::Cancelled),
        answer = tokio::time::timeout(request_timeout, request) => answer,
    };

    match answer {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => Err(CallError::Refused(error)),
        Err(_) => Err(CallError::TimedOut),
    }
}

/// Sends one request under `options`, records it under the pair it amounts to, and answers with
/// what the store handed over or with the storage error the request failed as.
///
/// `request` must carry everything one request costs - the reading of a body, the parsing of a
/// listing, the check that the answer carried what the operation asked for - because an answer that
/// carried none of it is a request that failed, and a `SUCCESS_STATUS` already recorded cannot be
/// taken back. One request under two pairs is what every quota is counted on.
///
/// A refusal `accepted_answer` names is not a failure: it yields the value that variant carries,
/// under the status that variant documents.
///
/// A call made through `Retrier` - `get_job` and `save_job` - has a token check above the one made
/// here, and a request its `tokio::select!` abandons is recorded under nothing at all.
// TODO(low): record a request the `Retrier` let go of. A drop-guard around the await, recording
// `CANCELLED_STATUS` unless an answer got there first, is what closes it; `biased` on the
// `tokio::select!` in `Retrier::retry` is not, because that loop is shared with `execution` and
// would start letting a ready `Done` beat a cancellation.
// TODO(low): the request each backend checks its container with does not come through here, and the
// timeout around it is written in three forms - inline in `S3Backend::build`, as
// `AzureBackend::send_container_request`, as `GcsBackend::send_bucket_request`. What is unresolved
// is the name a shared one would take: it has to tell that request apart from one of the working
// pass, and naming it by the phase of the life cycle it belongs to is a direction, not a decision.
pub(crate) async fn send_request<T, E: ProviderError>(
    request: impl Future<Output = Result<T, E>> + Send,
    options: RequestOptions<'_>,
    accepted_answer: ExpectedAnswer<T>,
    cancel_token: &CancellationToken,
) -> StorageResult<T> {
    let started = Instant::now();
    let (status, outcome) = match send_request_internal(request, options.request_timeout, cancel_token).await {
        Ok(answer) => (Some(Cow::Borrowed(SUCCESS_STATUS)), Ok(answer)),
        // A request the token stopped before it was sent is recorded under nothing: there is no
        // request for the store to bill. One cancelled in flight was received and will be billed
        // whether or not this worker waited for the answer, so leaving that one out would
        // undercount - and a status of its own is what keeps it from reading as a refusal.
        Err(CallError::NotSent) => (None, Err(StorageError::Cancelled)),
        Err(CallError::Cancelled) => (Some(Cow::Borrowed(CANCELLED_STATUS)), Err(StorageError::Cancelled)),
        Err(CallError::TimedOut) => (Some(Cow::Borrowed(FAILED_STATUS)), Err(StorageError::Timeout)),
        Err(CallError::Refused(error)) => settle_refusal(error, accepted_answer),
    };

    if let Some(status) = status {
        options
            .metrics
            .record_storage_operation(options.operation, &status, started.elapsed());
    }

    outcome
}

#[cfg(all(test, any(feature = "storage-s3", feature = "storage-azure")))]
mod tests {
    use super::*;

    #[test]
    fn a_quoted_version_loses_its_quotes() {
        assert_eq!(normalize_etag("\"abc123\""), "abc123");
        assert_eq!(normalize_etag("  \"abc123\"  "), "abc123");
        assert_eq!(normalize_etag("abc123"), "abc123");
    }
}
