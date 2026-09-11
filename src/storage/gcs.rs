use std::{future::Future, sync::Arc, time::Duration};

use google_cloud_auth::credentials::Credentials;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::storage::backend::{
    Backend, ExpectedAnswer, ListedObject, ListedPage, ListingRequest, ListingStartBound, PutCondition, RequestOptions,
    send_request,
};
use crate::storage::gcs_client::{GcsHttpClient, GcsRequestError, JsonApiResponse, ListedObjectsPage, StateObjectMeta};
use crate::storage::gcs_error::{
    GcsFailure, describe_refusal, is_absent_status, is_lost_creation_race, is_successful, map_request_error,
    map_response_status,
};
use crate::storage::object_storage::ObjectStorage;
use crate::storage::paths::{DEFAULT_STATE_PREFIX, JobPaths};
use crate::storage::state_codec::JobStateCodecKind;
use crate::{
    Error, JobDefinitionRegistry, MetricsSink, Retrier, RetrierConfig, RetryStep, StorageError, StorageResult,
};

/// Timeout of a single Google Cloud Storage operation unless overridden. `reqwest` bounds neither
/// the request as a whole nor the reading of its body, so without this a hung answer holds the
/// worker that asked for it for as long as the connection lives.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Objects requested per `LIST` page while scanning a job's outdated iterations. 1000 is the maximum
/// a single listing response of the JSON API carries, so the common empty-tail scan costs one
/// request. Being the protocol maximum, it doubles as the upper bound
/// [`GcsConfig::with_list_page_size`] accepts.
const DEFAULT_LIST_PAGE_SIZE: i32 = 1000;

/// Generation `0`, which the creation of a job's first iteration is conditioned on: the JSON API
/// spells "this object must not exist" as a generation no object can have.
const CREATE_ONLY_GENERATION: &str = "0";

/// Where a pool's credentials come from.
///
/// Google's own default - Application Default Credentials - is the one this backend takes without
/// being asked, which is why [`GcsConfig::new`] names no credential: unlike S3 and Azure,
/// this provider has a way in that needs nothing spelled out.
enum GcsCredentialsSource {
    /// The chain Google's own tooling establishes: the environment, a `gcloud` login, the metadata
    /// service of the machine.
    ApplicationDefault,
    /// The JSON of a service account key.
    ServiceAccountKey(String),
    /// No credentials at all, which is what an emulator serving a local bucket accepts.
    Anonymous,
}

/// What a pool does about a bucket that is not there.
///
/// One type rather than a flag beside an optional project, because the project is needed exactly
/// when a bucket may be created and never otherwise: kept apart, the pair has a state -
/// "create it, under no project" - that no configuration should be able to reach.
enum MissingBucketPolicy {
    /// Create the bucket under this project.
    Create(String),
    /// Refuse to start.
    Refuse,
}

/// Connection details of the Google Cloud Storage bucket a pool keeps job state in, passed to
/// [`JobsManagerBuilder::gcs`](crate::JobsManagerBuilder::gcs).
///
/// Build with [`GcsConfig::new`] and override the optional parts with the `with_*` methods.
/// Unlike the other two backends this one names no credential in its constructor: left alone it
/// authenticates with Application Default Credentials, and
/// [`GcsConfig::with_service_account_key`] or [`GcsConfig::with_anonymous_access`]
/// replace that.
///
/// What this backend does differently from the S3 one:
///
/// - **Iterations are deleted one request per iteration.** The JSON API does have a batch endpoint,
///   but reaching it means building a `multipart/mixed` body of sub-requests, which the client this
///   backend is written on does not do. So there is no setting here describing a batch size.
/// - **Cleanup lists a job from the key of the oldest iteration it may delete**, the boundary of
///   this provider being inclusive where the S3 one excludes the key it is given.
/// - **A delete cannot tell an absent iteration from an absent bucket.** `DELETE` answers `404` to
///   both, naming `notFound` in `error.errors[0].reason` either way, so cleanup reads both as an
///   iteration that is already gone: a sweep against a bucket that was removed under a running pool
///   reports clean work. What the pool is doing besides that sweep is where the cause surfaces -
///   `save_job` and `get_job` of the same pool fail with `NotFound` on their first pass. The S3
///   backend has no such reading, its `DELETE` answering `204` for a key that is already gone, and
///   the Azure one draws the line by error code.
///
/// Authentication against the service itself has not been run: of the three credential sources only
/// the anonymous one is exercised end to end, against an emulator.
pub struct GcsConfig {
    endpoint: String,
    bucket_name: String,
    credentials_source: GcsCredentialsSource,
    project_id: Option<String>,
    state_prefix: String,
    job_state_codec: JobStateCodecKind,
    request_timeout: Duration,
    retrier_config: RetrierConfig,
    list_page_size: i32,
    is_container_creation_allowed: bool,
}

impl GcsConfig {
    /// Connection details of the bucket holding job state. Everything else takes a default that
    /// the `with_*` methods override.
    ///
    /// `endpoint` is the JSON API address - `https://storage.googleapis.com` against the service, or
    /// the address of an emulator. Pass an `https://` endpoint to use TLS - there is no separate
    /// switch for it.
    pub fn new(endpoint: impl Into<String>, bucket_name: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            bucket_name: bucket_name.into(),
            credentials_source: GcsCredentialsSource::ApplicationDefault,
            project_id: None,
            state_prefix: DEFAULT_STATE_PREFIX.to_string(),
            job_state_codec: JobStateCodecKind::Json,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            retrier_config: RetrierConfig::default(),
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            is_container_creation_allowed: true,
        }
    }

    /// Authenticates with the JSON of a service account key instead of Application Default
    /// Credentials. The key is parsed when the backend is built, not here.
    #[must_use]
    pub fn with_service_account_key(mut self, service_account_key: impl Into<String>) -> Self {
        self.credentials_source = GcsCredentialsSource::ServiceAccountKey(service_account_key.into());
        self
    }

    /// Sends no credentials at all, which is what a local emulator accepts and the service does not.
    #[must_use]
    pub fn with_anonymous_access(mut self) -> Self {
        self.credentials_source = GcsCredentialsSource::Anonymous;
        self
    }

    /// Project a bucket is created under. Needed only while
    /// [`GcsConfig::with_container_creation`] is on, which it is by default: the API asks for
    /// a project when creating a bucket and for nothing else.
    #[must_use]
    pub fn with_project_id(mut self, project_id: impl Into<String>) -> Self {
        self.project_id = Some(project_id.into());
        self
    }

    /// Prefix every job's state objects live under. Defaults to `jobs`.
    #[must_use]
    pub fn with_state_prefix(mut self, state_prefix: impl Into<String>) -> Self {
        self.state_prefix = state_prefix.into();
        self
    }

    /// Serialization format of persisted job state. Defaults to [`JobStateCodecKind::Json`].
    ///
    /// Changing this on a bucket that already holds state leaves the previously written objects
    /// unreadable *and* invisible to cleanup - see [`JobStateCodecKind`].
    #[must_use]
    pub const fn with_job_state_codec(mut self, job_state_codec: JobStateCodecKind) -> Self {
        self.job_state_codec = job_state_codec;
        self
    }

    /// Timeout applied to a single operation, the reading of its body included. Defaults to 5s.
    #[must_use]
    pub const fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = request_timeout;
        self
    }

    /// Retry policy of the storage operations that retry internally. Defaults to
    /// [`RetrierConfig::default`].
    #[must_use]
    pub fn with_retrier_config(mut self, retrier_config: RetrierConfig) -> Self {
        self.retrier_config = retrier_config;
        self
    }

    /// Objects one cleanup `LIST` asks for. Defaults to 1000, which is the maximum a single listing
    /// response carries; lower it only to bound the size of a single response.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] outside `1..=1000`.
    pub fn with_list_page_size(mut self, list_page_size: i32) -> Result<Self, Error> {
        if !(1..=DEFAULT_LIST_PAGE_SIZE).contains(&list_page_size) {
            return Err(Error::Other(format!(
                "gcs list page size must be within 1..={DEFAULT_LIST_PAGE_SIZE}"
            )));
        }
        self.list_page_size = list_page_size;
        Ok(self)
    }

    /// Creates the bucket when it is missing. Defaults to `true`, and then
    /// [`GcsConfig::with_project_id`] has to name the project it is created under.
    ///
    /// Turn it off where the process has no permission to create one: the backend then only makes
    /// sure the bucket is reachable, and refuses to start when it is not.
    #[must_use]
    pub const fn with_container_creation(mut self, is_container_creation_allowed: bool) -> Self {
        self.is_container_creation_allowed = is_container_creation_allowed;
        self
    }

    /// What this configuration says about a bucket that is not there.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] when creation is allowed and no project was named, because the API
    /// creates a bucket under a project and under nothing else.
    fn build_missing_bucket_policy(&self) -> Result<MissingBucketPolicy, Error> {
        if !self.is_container_creation_allowed {
            return Ok(MissingBucketPolicy::Refuse);
        }

        self.project_id.clone().map(MissingBucketPolicy::Create).ok_or_else(|| {
            Error::Other(format!(
                "gcs bucket {} may be created but no project id was named: call with_project_id or with_container_creation(false)",
                self.bucket_name
            ))
        })
    }
}

/// State objects of one Google Cloud Storage bucket: each job iteration is one object.
///
/// Concurrent writers are resolved with conditional writes - `ifGenerationMatch` on the current
/// generation for an update, on `0` for a job's first iteration - rather than an external lock
/// service. The version this backend reports is that generation, which the JSON API spells as a
/// string just as the other two spell an entity-tag.
pub(crate) struct GcsBackend {
    client: GcsHttpClient,
    /// Content type state objects are written with, which is what the codec of this backend names.
    content_type: &'static str,
    metrics: Arc<dyn MetricsSink>,
    list_page_size: i32,
    request_timeout: Duration,
}

impl GcsBackend {
    /// Connects to `config.endpoint`, makes sure `config.bucket_name` is reachable - creating it
    /// when it is missing and `config.is_container_creation_allowed` allows it - and hands out the
    /// storage built over the bucket.
    ///
    /// The check and the creation are retried per `config.retrier_config`; a persistent failure
    /// (including one from a concurrent creator returning a non-409 status, and a missing bucket the
    /// configuration forbids creating) is returned as [`Error::Other`](crate::Error::Other).
    pub(crate) async fn build(
        config: GcsConfig,
        registry: Arc<dyn JobDefinitionRegistry>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<ObjectStorage<Self>, Error> {
        info!("Starting jobmanager with gcs storage {}", config.endpoint);

        let missing_bucket_policy = config.build_missing_bucket_policy()?;
        let client = GcsHttpClient::try_new(
            &config.endpoint,
            config.bucket_name.clone(),
            build_credentials(&config.credentials_source)?,
        )?;
        let retrier = Retrier::new(config.retrier_config.clone());

        Self::reach_bucket(
            &client,
            &config.bucket_name,
            &missing_bucket_policy,
            config.request_timeout,
            &retrier,
        )
        .await?;

        let codec = config.job_state_codec.build();
        let state_object_keys = JobPaths::new(config.state_prefix, codec.as_ref());

        Ok(ObjectStorage::new(
            Self {
                client,
                content_type: codec.content_type(),
                metrics,
                list_page_size: config.list_page_size,
                request_timeout: config.request_timeout,
            },
            state_object_keys,
            codec,
            registry,
            retrier,
        ))
    }

    /// Makes sure the bucket is there, creating it when `missing_bucket_policy` says so.
    ///
    /// Nothing here is recorded: the other two backends do not bill their container check either,
    /// and a request counted here would land in every quota that builds a backend.
    async fn reach_bucket(
        client: &GcsHttpClient,
        bucket_name: &str,
        missing_bucket_policy: &MissingBucketPolicy,
        request_timeout: Duration,
        retrier: &Retrier,
    ) -> Result<(), Error> {
        let cancel_token = CancellationToken::new();

        retrier
            .retry(
                move || async move {
                    // Each answer goes through `into_retry_step` rather than out on `?`: a start-up
                    // that reached a store which is briefly unavailable is what the retrier exists
                    // for, and `?` would hand that to the caller as a pool that cannot start.
                    let bucket_answer = match Self::send_bucket_request(client.find_bucket(), request_timeout).await {
                        Ok(bucket_answer) => bucket_answer,
                        Err(refusal) => return refusal.into_retry_step(),
                    };
                    if is_successful(bucket_answer.status) {
                        info!("Bucket {} exists", bucket_name);
                        return Ok(RetryStep::Done(()));
                    }
                    if !is_absent_status(bucket_answer.status) {
                        return map_response_status(
                            bucket_answer.status,
                            describe_refusal("GET", bucket_name, &bucket_answer),
                        )
                        .into_retry_step();
                    }

                    let MissingBucketPolicy::Create(project_id) = missing_bucket_policy else {
                        // Nothing to repeat: the bucket is absent and this pool may not create one.
                        // Not `NotFound`, whose text names a job - and at start-up there is none.
                        return Err(StorageError::Backend(format!(
                            "bucket {bucket_name} does not exist and container creation is turned off"
                        )));
                    };

                    let creation_answer =
                        match Self::send_bucket_request(client.create_bucket(project_id), request_timeout).await {
                            Ok(creation_answer) => creation_answer,
                            Err(refusal) => return refusal.into_retry_step(),
                        };
                    if is_successful(creation_answer.status) {
                        info!("Created bucket {}", bucket_name);
                        return Ok(RetryStep::Done(()));
                    }
                    if is_lost_creation_race(creation_answer.status) {
                        // A concurrent creator got there first, which is the outcome this pool
                        // wanted anyway.
                        info!("Bucket {} already exists", bucket_name);
                        return Ok(RetryStep::Done(()));
                    }

                    map_response_status(
                        creation_answer.status,
                        describe_refusal("POST", bucket_name, &creation_answer),
                    )
                    .into_retry_step()
                },
                &cancel_token,
            )
            .await
            .map_err(|e| Error::Other(format!("Failed to init bucket: {e}")))
    }

    /// Runs one request of the bucket check under `request_timeout`.
    ///
    /// Separate from [`GcsBackend::send_object_request`] because the check runs before there
    /// is a `GcsBackend` to run it through, and without a timeout of its own a connection that
    /// is accepted and never answered holds [`GcsBackend::build`] - and with it the
    /// whole pool's start-up - for as long as it lives.
    async fn send_bucket_request(
        request: impl Future<Output = Result<JsonApiResponse, GcsRequestError>> + Send,
        request_timeout: Duration,
    ) -> StorageResult<JsonApiResponse> {
        match tokio::time::timeout(request_timeout, request).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(error)) => Err(map_request_error(&error)),
            Err(_) => Err(StorageError::Timeout),
        }
    }

    /// Sends one request of the JSON API under `operation` and records it, handing an answer the
    /// store accepted to `answer_reader` and a status it refused to `accepted_answer`.
    ///
    /// Kept between the call sites and
    /// [`send_request`](crate::storage::backend::send_request) because a refusal on
    /// this provider is a status inside an answer rather than an error of the transport, and naming
    /// it takes the `operation` and the `key` that only this call knows.
    ///
    /// The answer is read inside the request rather than after it: an answer whose body does not
    /// carry what the operation asked for is a request that failed, and an `OK` recorded before the
    /// body was looked at cannot be taken back - leaving one request under two pairs, which is what
    /// every quota is counted on.
    async fn send_object_request<T>(
        &self,
        operation: &str,
        key: &str,
        request: impl Future<Output = Result<JsonApiResponse, GcsRequestError>> + Send,
        accepted_answer: ExpectedAnswer<T>,
        answer_reader: impl FnOnce(JsonApiResponse) -> StorageResult<T> + Send,
        cancel_token: &CancellationToken,
    ) -> StorageResult<T> {
        send_request(
            async move {
                let response = request.await.map_err(GcsFailure::Unanswered)?;
                if !is_successful(response.status) {
                    return Err(GcsFailure::Refused {
                        status: response.status,
                        details: describe_refusal(operation, key, &response),
                    });
                }

                answer_reader(response).map_err(GcsFailure::Unusable)
            },
            RequestOptions {
                operation,
                metrics: self.metrics.as_ref(),
                request_timeout: self.request_timeout,
            },
            accepted_answer,
            cancel_token,
        )
        .await
    }
}

/// Credentials the configured source produces.
///
/// # Errors
///
/// Returns [`Error::Other`] when the service account key is not JSON the credentials crate accepts,
/// and when Application Default Credentials cannot be located.
fn build_credentials(source: &GcsCredentialsSource) -> Result<Credentials, Error> {
    match source {
        GcsCredentialsSource::ApplicationDefault => google_cloud_auth::credentials::Builder::default()
            .build()
            .map_err(|e| Error::Other(format!("Failed to load gcs application default credentials: {e}"))),
        GcsCredentialsSource::ServiceAccountKey(service_account_key) => {
            let key = serde_json::from_str(service_account_key)
                .map_err(|e| Error::Other(format!("gcs service account key is not json: {e}")))?;
            google_cloud_auth::credentials::service_account::Builder::new(key)
                .build()
                .map_err(|e| Error::Other(format!("Failed to build gcs service account credentials: {e}")))
        }
        GcsCredentialsSource::Anonymous => Ok(google_cloud_auth::credentials::anonymous::Builder::new().build()),
    }
}

#[async_trait::async_trait]
impl Backend for GcsBackend {
    /// The boundary of this provider is `startOffset`, which answers with the key it is given:
    /// checked against `storage-testbench`, where a listing from the key of the middle object of
    /// three came back with that object and the one after it.
    fn listing_start_bound(&self) -> ListingStartBound {
        ListingStartBound::Inclusive
    }

    fn list_page_size(&self) -> i32 {
        self.list_page_size
    }

    async fn put_object(
        &self,
        key: &str,
        body: Vec<u8>,
        condition: PutCondition<'_>,
        cancel_token: &CancellationToken,
    ) -> StorageResult<String> {
        let if_generation_match = match &condition {
            PutCondition::CreateOnly => CREATE_ONLY_GENERATION,
            PutCondition::MatchVersion(version) => version,
        };

        let stored_object = self
            .send_object_request(
                "PUT",
                key,
                self.client.put_object(key, body, self.content_type, if_generation_match),
                ExpectedAnswer::Success,
                // The answer to a write is the object's metadata, so the generation it was given is
                // in the body; a read answers with the state itself and names the generation in a
                // header instead.
                |response| {
                    serde_json::from_slice::<StateObjectMeta>(&response.body).map_err(|e| {
                        StorageError::Backend(format!(
                            "gcs answered a write of {key} with metadata that does not parse: {e}"
                        ))
                    })
                },
                cancel_token,
            )
            .await?;

        Ok(stored_object.generation)
    }

    async fn get_object(
        &self,
        key: &str,
        expected_version: &str,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u8>> {
        self.send_object_request(
            "GET",
            key,
            self.client.get_object(key, expected_version),
            ExpectedAnswer::Success,
            |response| Ok(response.body),
            cancel_token,
        )
        .await
    }

    async fn get_changed_object(
        &self,
        key: &str,
        known_version: &str,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<(String, Vec<u8>)>> {
        self.send_object_request(
            "GET",
            key,
            self.client.get_changed_object(key, known_version),
            ExpectedAnswer::NotModified(None),
            |response| {
                let Some(version) = response.generation else {
                    // Asking for the metadata separately would make a poll cost two requests, so a
                    // store that names no generation on the body it just handed over is a failure
                    // and not a second round trip.
                    return Err(StorageError::Backend(format!("Missing generation reading {key}")));
                };

                Ok(Some((version, response.body)))
            },
            cancel_token,
        )
        .await
    }

    async fn list_objects(
        &self,
        request: ListingRequest<'_>,
        cancel_token: &CancellationToken,
    ) -> StorageResult<ListedPage> {
        self.send_object_request(
            "LIST",
            request.prefix,
            self.client.list_objects(
                request.prefix,
                request.start_key,
                request.page_size,
                request.cursor.as_deref(),
            ),
            ExpectedAnswer::Success,
            |response| {
                let listed_page: ListedObjectsPage = serde_json::from_slice(&response.body).map_err(|e| {
                    StorageError::Backend(format!(
                        "gcs answered a listing of {} with a page that does not parse: {e}",
                        request.prefix
                    ))
                })?;

                Ok(ListedPage {
                    objects: listed_page
                        .items
                        .into_iter()
                        .map(|item| ListedObject {
                            key: item.name,
                            version: Some(item.generation),
                        })
                        .collect(),
                    next_cursor: listed_page.next_page_token.filter(|token| !token.is_empty()),
                })
            },
            cancel_token,
        )
        .await
    }

    // TODO(med): delete a tail in one batch request rather than one request per iteration - what is
    // missing is a `multipart/mixed` body of sub-requests in `gcs_client`, and a number for
    // `trimming_a_tail_of_two_iterations_costs_four_requests_on_gcs` the emulator can state.
    // TODO(med): tell a `404` the API attributes to the *bucket* from one it attributes to the
    // object - see `GcsConfig` for what cleanup reads today.
    async fn delete_objects(&self, keys: &[String], cancel_token: &CancellationToken) -> StorageResult<()> {
        for key in keys {
            self.send_object_request(
                "DELETE",
                key,
                self.client.delete_object(key),
                ExpectedAnswer::ObjectGone(()),
                |_response| Ok(()),
                cancel_token,
            )
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use super::*;
    use crate::tests::common::UnusedJobRegistry;
    use crate::tests::common::counting_metrics::CountingMetrics;
    use crate::tests::common::scripted_endpoint::{ScriptedEndpoint, build_http_response};
    use crate::tests::common::silent_endpoint::{cancel_after_delay, start_silent_endpoint};
    use crate::{JobCode, NoopMetrics, Storage};

    /// Timeout the cases against a local endpoint give a request. Long enough that no scheduling
    /// hiccup reaches it, short enough that a case which really waits it out still ends quickly.
    const LOCAL_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(1);

    /// Iteration the read and write cases name.
    const TESTED_ITER_NUM: u64 = 1;

    /// Retention boundary the cleanup listing case asks about: the newest iteration cleanup may
    /// delete.
    const TESTED_RETENTION_BOUNDARY: u64 = 2;

    /// Key of the iteration equal to [`TESTED_RETENTION_BOUNDARY`], as it travels in a query
    /// parameter. Stated rather than built so the expectation does not come from the key builder
    /// under test; the escaping is this provider's, an object name being one path segment.
    const BOUNDARY_ITERATION_TARGET_KEY: &str = "jobs%2Fjob%2Fstate-18446744073709551613.json";

    /// A service account key the credentials crate accepts and then cannot sign with, because its
    /// private key is not one. The crate reads the key when it signs rather than when it is built,
    /// so this is what reaches the refusal that happens before a request is sent - without a token
    /// endpoint, and without a network of any kind.
    const UNSIGNABLE_SERVICE_ACCOUNT_KEY: &str = concat!(
        r#"{"type":"service_account","project_id":"a-project","private_key_id":"a-key-id","#,
        r#""private_key":"-----BEGIN PRIVATE KEY-----\nbm90YWtleQ==\n-----END PRIVATE KEY-----\n","#,
        r#""client_email":"a-pool@a-project.iam.gserviceaccount.com","client_id":"1","#,
        r#""token_uri":"https://oauth2.googleapis.com/token"}"#
    );

    /// Address the cases that never send a request point their client at: nothing listens there, so
    /// a refusal that stopped happening before the send would fail the case rather than hang it.
    const UNREACHED_ENDPOINT: &str = "http://127.0.0.1:1";

    fn build_config() -> GcsConfig {
        GcsConfig::new("http://localhost:9000", "jobs")
    }

    /// Builds a store pointed at `endpoint` field by field.
    ///
    /// Constructed this way rather than through [`GcsBackend::build`], which checks the
    /// bucket before handing one out and therefore cannot be reached against an endpoint serving a
    /// script of its own; adding a way past that check to the production path would be a second way
    /// to configure the same thing.
    fn build_store_against(endpoint: &str, metrics: Arc<dyn MetricsSink>) -> GcsBackend {
        build_store_with_credentials(
            endpoint,
            metrics,
            build_credentials(&GcsCredentialsSource::Anonymous).expect("anonymous credentials always build"),
        )
    }

    /// The same store, signed by `credentials` rather than by the anonymous ones every other case
    /// uses - which is what lets a case reach the refusal that happens before a request is sent.
    fn build_store_with_credentials(
        endpoint: &str,
        metrics: Arc<dyn MetricsSink>,
        credentials: Credentials,
    ) -> GcsBackend {
        GcsBackend {
            client: GcsHttpClient::try_new(endpoint, "jobs", credentials)
                .expect("the test endpoint must form a client"),
            content_type: JobStateCodecKind::Json.build().content_type(),
            metrics,
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            request_timeout: LOCAL_ENDPOINT_TIMEOUT,
        }
    }

    /// The storage a case reaching `Storage` goes through, over `store`.
    fn build_storage_over_store(store: GcsBackend) -> ObjectStorage<GcsBackend> {
        let codec = JobStateCodecKind::Json.build();

        ObjectStorage::new(
            store,
            JobPaths::new(DEFAULT_STATE_PREFIX.to_string(), codec.as_ref()),
            codec,
            Arc::new(UnusedJobRegistry),
            Retrier::new(RetrierConfig::default()),
        )
    }

    /// A quota states what one call of `Storage` costs, and a client that follows a redirect on its
    /// own makes that statement false without failing anything: the second request is neither
    /// counted nor bounded by anything this crate owns.
    ///
    /// Checked by breaking it: taking `redirect::Policy::none()` off the client in `gcs_client`
    /// makes the read follow the `Location` and the request count becomes two.
    #[tokio::test]
    async fn a_redirect_is_refused_rather_than_followed() {
        let endpoint = ScriptedEndpoint::start(vec![
            String::new(),
            build_http_response(200, &[("x-goog-generation", "42")], "stored state"),
        ])
        .await;
        // Written after the endpoint is bound, because the redirect has to name that endpoint.
        endpoint.rewrite_first_response(build_http_response(
            302,
            &[(
                "location",
                &format!("{}/storage/v1/b/jobs/o/moved?alt=media", endpoint.endpoint()),
            )],
            "",
        ));
        let store = build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics));

        let read_result = store.get_object("jobs/job/state-1.json", "41", &CancellationToken::new()).await;

        assert!(
            read_result.is_err(),
            "a redirect must reach the caller as a refusal, got: {read_result:?}"
        );
        assert_eq!(
            endpoint.requested_targets().len(),
            1,
            "the client must not spend a second request following the redirect"
        );
    }

    /// The bucket check runs before the pool has anything that could cancel it, so the timeout is
    /// the only bound it has that answers in time - and a store that did not answer is a store that
    /// may answer on the next attempt, so the check is repeated rather than given up on.
    ///
    /// Both bounds are asserted: the lower one says the second attempt really ran, the upper one
    /// that the start-up ended rather than waiting the transport out.
    ///
    /// Checked by breaking it twice. Letting the refusal out of the retry closure on `?` instead of
    /// through `into_retry_step` ends the start-up after one attempt and the lower bound fails.
    /// Taking `send_bucket_request` off the request leaves this waiting on the connection, and the
    /// upper bound never comes.
    #[tokio::test]
    async fn a_bucket_check_a_silent_endpoint_never_answers_is_repeated_and_then_given_up() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let config = GcsConfig::new(endpoint, "jobs")
            .with_anonymous_access()
            .with_project_id("a-project")
            .with_request_timeout(LOCAL_ENDPOINT_TIMEOUT)
            .with_retrier_config(RetrierConfig {
                max_attempts: 2,
                rand_delay: Duration::from_millis(1),
                delays: vec![Duration::from_millis(1)],
            });

        let started = Instant::now();
        let storage = GcsBackend::build(config, Arc::new(UnusedJobRegistry), Arc::new(NoopMetrics)).await;
        let elapsed = started.elapsed();

        accepting.abort();
        assert!(
            storage.is_err(),
            "an endpoint that never answers must not read as a reachable bucket"
        );
        assert!(
            elapsed >= LOCAL_ENDPOINT_TIMEOUT * 2,
            "a check that timed out must be attempted again, took {elapsed:?}"
        );
        assert!(
            elapsed < LOCAL_ENDPOINT_TIMEOUT * 4,
            "and the attempts must be given up once each rather than waited on, took {elapsed:?}"
        );
    }

    /// A pool that found no bucket and then lost the creation of it to somebody else still starts:
    /// the bucket it needs is there, which is the whole of what the check asked for. This is the arm
    /// every deployment of more than one process hits on its first start, and no case against a real
    /// emulator can reach it deterministically - two builds racing may each see the bucket absent,
    /// or the second may see it present.
    ///
    /// Checked by breaking it: answering the lost race with `map_response_status(...)` instead of
    /// `RetryStep::Done` spends the whole attempt budget on a `409` no repetition clears, and the
    /// build ends as an error.
    #[tokio::test]
    async fn a_pool_that_lost_the_creation_of_its_bucket_still_starts() {
        let endpoint = ScriptedEndpoint::start(vec![
            build_http_response(
                404,
                &[("content-type", "application/json")],
                r#"{"error":{"code":404,"message":"Not Found"}}"#,
            ),
            build_http_response(
                409,
                &[("content-type", "application/json")],
                r#"{"error":{"code":409,"message":"You already own this bucket."}}"#,
            ),
        ])
        .await;
        let config = GcsConfig::new(endpoint.endpoint(), "jobs")
            .with_anonymous_access()
            .with_project_id("a-project")
            .with_request_timeout(LOCAL_ENDPOINT_TIMEOUT);

        let storage = GcsBackend::build(config, Arc::new(UnusedJobRegistry), Arc::new(NoopMetrics)).await;

        storage.expect("a bucket somebody else created first is the outcome this pool wanted");
        let requested_targets = endpoint.requested_targets();
        assert_eq!(
            requested_targets.len(),
            2,
            "the check must have been followed by the creation that lost, got: {requested_targets:?}"
        );
    }

    /// An address no request can be built from is refused before the bucket check runs. Left to the
    /// first send it reaches the `Retrier` as a transport that gave up, which spends the whole
    /// attempt budget on an answer that cannot change and ends naming neither the address nor the
    /// cause.
    ///
    /// Checked by breaking it: taking the `Url::parse` out of `GcsHttpClient::try_new` ends this as
    /// `service unavailable` once the attempts are spent, and the assertion on the text fails.
    #[tokio::test]
    async fn an_endpoint_that_is_not_a_url_is_refused_before_the_bucket_is_checked() {
        let config = GcsConfig::new("storage.googleapis.com", "jobs")
            .with_anonymous_access()
            .with_project_id("a-project")
            .with_request_timeout(LOCAL_ENDPOINT_TIMEOUT)
            .with_retrier_config(RetrierConfig {
                max_attempts: 2,
                rand_delay: Duration::from_millis(1),
                delays: vec![Duration::from_millis(1)],
            });

        let started = Instant::now();
        let storage = GcsBackend::build(config, Arc::new(UnusedJobRegistry), Arc::new(NoopMetrics)).await;
        let elapsed = started.elapsed();

        let Err(error) = storage else {
            panic!("an address that is not a url must not read as a reachable bucket")
        };
        assert!(
            error.to_string().contains("storage.googleapis.com"),
            "the refusal must name the address it was given, got: {error}"
        );
        assert!(
            elapsed < LOCAL_ENDPOINT_TIMEOUT,
            "the address must be refused without a request, took {elapsed:?}"
        );
    }

    /// The answer to a write is the object's metadata, and the generation in it is the version the
    /// next condition is held against. Read from anywhere else - a header the emulator does not
    /// send, a second request - this would be either empty or a doubled quota.
    #[tokio::test]
    async fn a_write_takes_its_version_from_the_metadata_it_is_answered_with() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            200,
            &[("content-type", "application/json")],
            r#"{"name":"jobs/job/state-1.json","generation":"1789314575620"}"#,
        )])
        .await;
        let store = build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics));

        let version = store
            .put_object(
                "jobs/job/state-1.json",
                b"state".to_vec(),
                PutCondition::CreateOnly,
                &CancellationToken::new(),
            )
            .await
            .expect("a store answering with metadata must read as a write");

        assert_eq!(version, "1789314575620");
        assert!(
            endpoint.requested_targets()[0].contains("ifGenerationMatch=0"),
            "the creation of an iteration must be conditioned on the object not existing, got: {:?}",
            endpoint.requested_targets()
        );
    }

    /// A store that accepted the write and named no generation leaves the worker with nothing to
    /// condition its next save on, so the save has to fail rather than carry an empty version into
    /// the next write - which would be an unconditional one.
    ///
    /// And it is billed as the failure it is: the `200` the status line claimed is what the answer
    /// stopped being once the body was read, and a request recorded under two pairs is what every
    /// quota is counted on.
    #[tokio::test]
    async fn a_write_answered_without_a_generation_is_refused() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            200,
            &[("content-type", "application/json")],
            r#"{"name":"jobs/job/state-1.json"}"#,
        )])
        .await;
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_against(endpoint.endpoint(), Arc::clone(&metrics) as Arc<dyn MetricsSink>);

        let error = store
            .put_object(
                "jobs/job/state-1.json",
                b"state".to_vec(),
                PutCondition::MatchVersion("41"),
                &CancellationToken::new(),
            )
            .await
            .expect_err("a write answered without a version must not read as a save");

        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
        assert_eq!(metrics.storage_operations("PUT", "ERR"), 1);
        assert_eq!(metrics.storage_operations_total(), 1);
    }

    /// A write the store refused because somebody else got there first is the ordinary end of a
    /// race, and a worker re-reads and merges on a conflict where it stops on anything else.
    #[tokio::test]
    async fn a_refused_write_condition_reaches_the_caller_as_a_conflict() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            412,
            &[("content-type", "application/json")],
            r#"{"error":{"code":412,"message":"Precondition Failed"}}"#,
        )])
        .await;
        let store = build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics));

        let error = store
            .put_object(
                "jobs/job/state-1.json",
                b"state".to_vec(),
                PutCondition::MatchVersion("41"),
                &CancellationToken::new(),
            )
            .await
            .expect_err("a store refusing the condition must not read as a save");

        assert!(error.is_conflict(), "got: {error:?}");
        assert!(!error.is_retryable(), "a lost race is re-read and merged, not repeated");
    }

    /// `304` is the answer a conditional read asks for, not a failure to translate. Taken for one,
    /// every poll of an unmoved iteration would come back as an error and cost the cold read the
    /// conditional read exists to avoid. It is recorded under its own status rather than as `OK`,
    /// because a poll that found nothing and a poll that read a state are what the quotas tell
    /// apart.
    #[tokio::test]
    async fn a_conditional_read_of_an_unmoved_iteration_returns_nothing() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(304, &[], "")]).await;
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_against(endpoint.endpoint(), Arc::clone(&metrics) as Arc<dyn MetricsSink>);

        let read = store
            .get_changed_object("jobs/job/state-1.json", "41", &CancellationToken::new())
            .await
            .expect("a store answering 304 must not report a failure");

        assert!(read.is_none(), "an unmoved iteration must read as nothing to apply");
        assert_eq!(metrics.storage_operations("GET", "304"), 1);
        assert_eq!(metrics.storage_operations_total(), 1);
    }

    /// The body of a read is the state, so the version it now stands at arrives in a header. Asking
    /// for the metadata separately would make every poll that found work cost two requests.
    #[tokio::test]
    async fn a_changed_iteration_takes_its_version_from_the_header_of_the_same_answer() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            200,
            &[("x-goog-generation", "1789314575621")],
            "stored state",
        )])
        .await;
        let store = build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics));

        let read = store
            .get_changed_object("jobs/job/state-1.json", "41", &CancellationToken::new())
            .await
            .expect("a store answering with a state must read as one");

        let Some((version, state_bytes)) = read else {
            panic!("a moved iteration must read as a state")
        };
        assert_eq!(version, "1789314575621");
        assert_eq!(state_bytes, b"stored state");
    }

    /// And a store that handed the state over without naming its generation leaves the worker
    /// unable to condition anything on what it just read, so the read is refused and billed as a
    /// request that failed rather than as the `OK` its status line claimed.
    ///
    /// Checked by breaking it: taking the reading of the generation back out of the recorded
    /// request, as this backend did before, records the `GET` under `OK` and leaves `ERR` at zero.
    #[tokio::test]
    async fn a_changed_iteration_answered_without_a_generation_is_refused() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(200, &[], "stored state")]).await;
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_against(endpoint.endpoint(), Arc::clone(&metrics) as Arc<dyn MetricsSink>);

        let error = store
            .get_changed_object("jobs/job/state-1.json", "41", &CancellationToken::new())
            .await
            .expect_err("a state handed over without a version must not read as one");

        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
        assert_eq!(metrics.storage_operations("GET", "ERR"), 1);
        assert_eq!(metrics.storage_operations_total(), 1);
    }

    /// The same for a listing whose body is not a page this crate can read: the request failed, and
    /// the pair it is billed under has to say so - otherwise a store answering `200` with anything
    /// at all reads as a cleanup pass that found nothing to delete.
    #[tokio::test]
    async fn a_listing_answered_with_a_body_that_does_not_parse_is_refused() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            200,
            &[("content-type", "application/json")],
            "not a listing",
        )])
        .await;
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_against(endpoint.endpoint(), Arc::clone(&metrics) as Arc<dyn MetricsSink>);

        let listing_result = store
            .list_objects(
                ListingRequest {
                    prefix: "jobs/job/",
                    start_key: None,
                    page_size: DEFAULT_LIST_PAGE_SIZE,
                    cursor: None,
                },
                &CancellationToken::new(),
            )
            .await;

        let Err(error) = listing_result else {
            panic!("a body that is not a page must not read as a listing")
        };
        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
        assert_eq!(metrics.storage_operations("LIST", "ERR"), 1);
        assert_eq!(metrics.storage_operations_total(), 1);
    }

    /// An iteration cleanup has already removed is the ordinary case - two instances reconcile the
    /// same tail - so the delete has to succeed, and to be billed as the success it asked for
    /// rather than as a store that refused something.
    #[tokio::test]
    async fn a_delete_of_an_absent_iteration_succeeds() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            404,
            &[("content-type", "application/json")],
            r#"{"error":{"code":404,"message":"Not Found"}}"#,
        )])
        .await;
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_against(endpoint.endpoint(), Arc::clone(&metrics) as Arc<dyn MetricsSink>);

        store
            .delete_objects(&["jobs/job/state-1.json".to_string()], &CancellationToken::new())
            .await
            .expect("an object that is already gone is the success cleanup asked for");

        assert_eq!(metrics.storage_operations("DELETE", "OK"), 1);
        assert_eq!(metrics.storage_operations_total(), 1);
    }

    /// The same status on a read is a failure: the iteration a worker was told to read is not there.
    #[tokio::test]
    async fn a_read_of_an_absent_iteration_is_not_found() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            404,
            &[("content-type", "application/json")],
            r#"{"error":{"code":404,"message":"Not Found"}}"#,
        )])
        .await;
        let store = build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics));

        let error = store
            .get_object("jobs/job/state-1.json", "41", &CancellationToken::new())
            .await
            .expect_err("an iteration that is not stored must not read as a state");

        assert!(matches!(error, StorageError::NotFound(_)), "got: {error:?}");
    }

    /// A tail longer than one listing page has to be paged through whole: an iteration left off the
    /// first page is one cleanup would never delete. The provider perimeter cannot state this on
    /// this backend - `storage-testbench` answers a listing with every match and no page token,
    /// whatever `maxResults` asks for - so the two pages are scripted here.
    ///
    /// Checked by breaking it: dropping `next_cursor` from the listing leaves the second page
    /// unasked for, and both the collected iterations and the request count fail.
    #[tokio::test]
    async fn a_listing_answered_in_two_pages_is_asked_for_whole() {
        let endpoint = ScriptedEndpoint::start(vec![
            build_http_response(
                200,
                &[("content-type", "application/json")],
                // Iterations 2 and 1, whose keys carry the inverted numbers; stated literally so
                // the expectation does not come from the key builder under test.
                r#"{"items":[{"name":"jobs/job/state-18446744073709551613.json","generation":"3"}],
                    "nextPageToken":"page-two"}"#,
            ),
            build_http_response(
                200,
                &[("content-type", "application/json")],
                r#"{"items":[{"name":"jobs/job/state-18446744073709551614.json","generation":"2"}]}"#,
            ),
        ])
        .await;
        let storage = build_storage_over_store(build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics)));

        let mut outdated_iter_nums = storage
            .list_job_outdated_iterations(&JobCode::new("job"), 5, &CancellationToken::new())
            .await
            .expect("a scripted listing must read as a tail");
        outdated_iter_nums.sort_unstable();

        assert_eq!(outdated_iter_nums, vec![1, 2]);
        let requested_targets = endpoint.requested_targets();
        assert_eq!(
            requested_targets.len(),
            2,
            "a page token must be followed by a request for that page, got: {requested_targets:?}"
        );
        assert!(
            requested_targets[1].contains("pageToken=page-two"),
            "the second request must ask for the page the first one named, got: {requested_targets:?}"
        );
    }

    /// The boundary the cleanup listing was calculated for has to reach the store in the parameter
    /// this provider reads it from. A listing that carried none would ask for the job's whole prefix
    /// on every sweep, which no assertion over the deleted iterations can see: the boundary check
    /// over the listed keys absorbs a listing wider than asked for.
    ///
    /// The store refuses the listing on purpose - what is under test is the address the request
    /// carried, and the target is recorded whatever the answer was.
    ///
    /// Checked by breaking it: dropping `start_key` from `GcsHttpClient::list_objects` leaves the
    /// parameter off the target and the assertion fails.
    #[tokio::test]
    async fn a_cleanup_listing_asks_the_store_to_start_at_the_boundary_key() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(500, &[], "")]).await;
        let storage = build_storage_over_store(build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics)));

        let listing_result = storage
            .list_job_outdated_iterations(
                &JobCode::new("job"),
                TESTED_RETENTION_BOUNDARY,
                &CancellationToken::new(),
            )
            .await;

        assert!(
            listing_result.is_err(),
            "the scripted store refuses every listing, got: {listing_result:?}"
        );
        let requested_targets = endpoint.requested_targets();
        assert_eq!(
            requested_targets.len(),
            1,
            "a refused listing must not be repeated, got: {requested_targets:?}"
        );
        assert!(
            requested_targets[0].contains(&format!("startOffset={BOUNDARY_ITERATION_TARGET_KEY}")),
            "this provider answers with the boundary key, so the listing starts at the boundary itself, got: {requested_targets:?}"
        );
    }

    /// A quota states what one call of `Storage` costs, and a client that repeats a request on its
    /// own makes that statement false without failing anything: the repetition is neither counted
    /// nor bounded by `Retrier`, which owns the retry policy of this crate. `503` is the case,
    /// because it is the status a retrying HTTP client repeats.
    ///
    /// The other two backends state this as a setting of their SDK - S3 as a configured number of
    /// attempts, Azure as a count of requests its transport was handed. The client here is written
    /// in this crate and carries no retry at all, so a count is the only place the same statement
    /// fits.
    ///
    /// Checked by breaking it: adding a retry layer to the `reqwest` client in
    /// `GcsHttpClient::try_new` sends a second request, which the endpoint answers `500` as an
    /// unscripted one, and the count fails.
    #[tokio::test]
    async fn the_production_client_does_not_repeat_a_refused_request() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            503,
            &[("content-type", "application/json")],
            r#"{"error":{"code":503,"message":"Service Unavailable"}}"#,
        )])
        .await;
        let store = build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics));

        let read_result = store.get_object("jobs/job/state-1.json", "41", &CancellationToken::new()).await;

        assert!(
            matches!(read_result, Err(StorageError::ServiceUnavailable)),
            "a store answering 503 must reach the caller as an unavailable service, got: {read_result:?}"
        );
        let requested_targets = endpoint.requested_targets();
        assert_eq!(
            requested_targets.len(),
            1,
            "the production client must attempt a request once, got: {requested_targets:?}"
        );
    }

    /// A worker standing on a read must let go when its pool is asked to stop, rather than hold on
    /// until the request times out. Five of the seven `Storage` methods reach the store without a
    /// `Retrier` around them, so the token has to be observed inside the request itself.
    ///
    /// Checked by breaking it: dropping the `tokio::select!` from `send_request` leaves
    /// this waiting the whole of `LOCAL_ENDPOINT_TIMEOUT` and the elapsed assertion fails.
    #[tokio::test]
    async fn a_cancelled_token_drops_a_conditional_read_in_flight() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_against(&endpoint, Arc::clone(&metrics) as Arc<dyn MetricsSink>);
        let cancel_token = CancellationToken::new();
        cancel_after_delay(&cancel_token);

        let started = Instant::now();
        let read_result = store.get_changed_object("jobs/job/state-1.json", "41", &cancel_token).await;
        let elapsed = started.elapsed();

        accepting.abort();
        let Err(error) = read_result else {
            panic!("an endpoint that never answers must not read as a state")
        };
        assert!(
            matches!(error, StorageError::Cancelled),
            "a cancelled read must reach the caller as a cancellation, got: {error:?}"
        );
        assert!(
            elapsed < LOCAL_ENDPOINT_TIMEOUT,
            "the read must end on the token rather than on the timeout, took {elapsed:?}"
        );
        assert_eq!(
            // Stated rather than read off `CANCELLED_STATUS`: the label is what a dashboard groups
            // by, so a test taking it from the constant would follow a rename instead of catching
            // one.
            metrics.storage_operations("GET", "CANCELLED"),
            1,
            "the abandoned read must be recorded as one this worker let go of"
        );
    }

    /// The same for cleanup, where a tail of any length would otherwise hold a stopping pool for one
    /// whole timeout - the one its first delete is standing in. This provider deletes one request per
    /// iteration, so the loop is what has to let go, not just the request inside it.
    ///
    /// The count is what says the tail was abandoned rather than worked through: a cancellation the
    /// loop swallowed would leave the second iteration asked for all the same. It is counted under
    /// the pair the request was recorded with, so a cancelled request cannot pass for a store that
    /// refused one.
    ///
    /// Checked by breaking it: letting the loop carry on past a cancelled delete - ignoring what
    /// `send_object_request` answered instead of ending on it - asks for the second iteration and
    /// the total fails.
    #[tokio::test]
    async fn a_cancelled_token_stops_a_delete_before_the_next_iteration() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let metrics = Arc::new(CountingMetrics::default());
        let storage = build_storage_over_store(build_store_against(
            &endpoint,
            Arc::clone(&metrics) as Arc<dyn MetricsSink>,
        ));
        let cancel_token = CancellationToken::new();
        cancel_after_delay(&cancel_token);

        let cleanup_result = storage
            .delete_job_iterations(&JobCode::new("job"), &[1, 2], &cancel_token)
            .await;

        accepting.abort();
        assert!(
            matches!(cleanup_result, Err(StorageError::Cancelled)),
            "a cancelled cleanup must reach the caller as a cancellation, got: {cleanup_result:?}"
        );
        assert_eq!(
            // Stated rather than read off `CANCELLED_STATUS`: the label is what a dashboard groups
            // by, so a test taking it from the constant would follow a rename instead of catching
            // one.
            metrics.storage_operations("DELETE", "CANCELLED"),
            1,
            "the abandoned delete must be recorded as one this worker let go of"
        );
        assert_eq!(
            metrics.storage_operations_total(),
            1,
            "and the iteration after it must not have been asked for at all"
        );
    }

    /// And a request nobody cancels is bounded by the timeout, because `reqwest` bounds neither the
    /// request nor the reading of its body on its own.
    ///
    /// Checked by breaking it: taking `send_request` off the request leaves this
    /// waiting on the connection, and the case never ends.
    #[tokio::test]
    async fn a_request_a_silent_endpoint_never_answers_is_given_up_on() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let store = build_store_against(&endpoint, Arc::new(NoopMetrics));

        let started = Instant::now();
        let read_result = store.get_object("jobs/job/state-1.json", "41", &CancellationToken::new()).await;
        let elapsed = started.elapsed();

        accepting.abort();
        assert!(
            matches!(read_result, Err(StorageError::Timeout)),
            "a request that was never answered must reach the caller as a timeout, got: {read_result:?}"
        );
        assert!(
            elapsed < LOCAL_ENDPOINT_TIMEOUT * 3,
            "the request must end on its own timeout, took {elapsed:?}"
        );
    }

    /// A token already cancelled when the request is handed over stops it before anything is sent,
    /// and what was never sent is never billed: counting it would put a request nobody made into
    /// the very metric the quotas are counted on.
    ///
    /// Checked by breaking it: recording every answer of `send_request`, as this backend did before,
    /// puts one `GET` under `CANCELLED` in the counters.
    #[tokio::test]
    async fn a_request_the_token_stopped_before_it_was_sent_is_recorded_under_nothing() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_against(&endpoint, Arc::clone(&metrics) as Arc<dyn MetricsSink>);
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let read_result = store.get_changed_object("jobs/job/state-1.json", "41", &cancel_token).await;

        accepting.abort();
        let Err(error) = read_result else {
            panic!("a cancelled token must not answer with a state")
        };
        assert!(
            matches!(error, StorageError::Cancelled),
            "a read stopped by the token must reach the caller as a cancellation, got: {error:?}"
        );
        assert_eq!(
            metrics.storage_operations_total(),
            0,
            "a request the store never received must be recorded under no pair at all"
        );
    }

    /// Credentials that cannot sign end the call before anything is sent, the same as a cancelled
    /// token does. Recording it as `ERR` would charge the store for a refusal it never issued, in
    /// the metric the quotas are counted on - and would read as an outage of the store in a
    /// dashboard, for a failure whose remedy is the pool's own credentials.
    ///
    /// Checked by breaking it: answering `ERR` for `GcsRequestError::Unauthorized`, as this backend
    /// did before, puts one `GET` under `ERR` in the counters.
    #[tokio::test]
    async fn a_request_the_credentials_could_not_sign_is_recorded_under_nothing() {
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_with_credentials(
            UNREACHED_ENDPOINT,
            Arc::clone(&metrics) as Arc<dyn MetricsSink>,
            build_credentials(&GcsCredentialsSource::ServiceAccountKey(
                UNSIGNABLE_SERVICE_ACCOUNT_KEY.to_string(),
            ))
            .expect("a key of this shape must build credentials"),
        );

        let read_result = store
            .get_changed_object("jobs/job/state-1.json", "41", &CancellationToken::new())
            .await;

        let Err(error) = read_result else {
            panic!("credentials that cannot sign must not answer with a state")
        };
        assert!(
            matches!(error, StorageError::Auth(_)),
            "a request that could not be signed must reach the caller as an auth failure, got: {error:?}"
        );
        assert_eq!(
            metrics.storage_operations_total(),
            0,
            "a request the store never received must be recorded under no pair at all"
        );
    }

    /// The key a read asks for is the state object of the named iteration, and a name whose slashes
    /// reached the URL unescaped would address a different resource - a `404` for an object that is
    /// there.
    #[tokio::test]
    async fn a_read_asks_for_the_state_object_of_the_named_iteration() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(
            200,
            &[("x-goog-generation", "42")],
            "stored state",
        )])
        .await;
        let storage = build_storage_over_store(build_store_against(endpoint.endpoint(), Arc::new(NoopMetrics)));

        let _ = storage
            .get_changed_job(
                &crate::JobMeta {
                    code: JobCode::new("job"),
                    iter_num: TESTED_ITER_NUM,
                    version: "41".to_string(),
                },
                &CancellationToken::new(),
            )
            .await;

        let requested_targets = endpoint.requested_targets();
        assert!(
            requested_targets[0].starts_with("/storage/v1/b/jobs/o/jobs%2Fjob%2Fstate-18446744073709551614.json?"),
            "got: {requested_targets:?}"
        );
        assert!(
            requested_targets[0].contains("ifGenerationNotMatch=41"),
            "a conditional read must name the version the worker holds, got: {requested_targets:?}"
        );
    }

    #[test]
    fn a_list_page_size_below_one_is_rejected() {
        assert!(build_config().with_list_page_size(0).is_err());
    }

    #[test]
    fn a_list_page_size_above_the_protocol_maximum_is_rejected() {
        assert!(build_config().with_list_page_size(1001).is_err());
    }

    #[test]
    fn a_list_page_size_at_the_accepted_bounds_is_applied() {
        let lowest = build_config().with_list_page_size(1).expect("1 is inside the accepted range");
        let highest = build_config().with_list_page_size(1000).expect("1000 is the protocol maximum");

        assert_eq!(lowest.list_page_size, 1);
        assert_eq!(highest.list_page_size, 1000);
    }

    /// A bucket may be created only under a project, so a configuration that allows the creation and
    /// names no project describes a start-up that cannot succeed. Refusing it here names the cause;
    /// left to the API it would surface as a refused creation of a bucket nobody could explain.
    #[test]
    fn allowing_a_bucket_creation_without_a_project_is_refused() {
        let error = build_config()
            .build_missing_bucket_policy()
            .err()
            .expect("creation is on by default and no project was named");

        assert!(error.to_string().contains("project id"), "got: {error}");
    }

    /// The same configuration with the creation turned off is legal: a pool that may not create a
    /// bucket has nothing to name a project for.
    #[test]
    fn refusing_a_bucket_creation_needs_no_project() {
        let policy = build_config()
            .with_container_creation(false)
            .build_missing_bucket_policy()
            .expect("a pool that may not create a bucket names no project");

        assert!(matches!(policy, MissingBucketPolicy::Refuse));
    }

    #[test]
    fn a_named_project_is_what_a_creation_is_made_under() {
        let policy = build_config()
            .with_project_id("a-project")
            .build_missing_bucket_policy()
            .expect("a named project allows the creation");

        let MissingBucketPolicy::Create(project_id) = policy else {
            panic!("a configuration allowing the creation must carry the project it creates under")
        };
        assert_eq!(project_id, "a-project");
    }
}
