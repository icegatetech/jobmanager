use std::{future::Future, pin::Pin, sync::Arc, time::Duration};

use aws_sdk_s3::{
    Client,
    config::retry::RetryConfig,
    error::SdkError,
    operation::{
        delete_objects::{DeleteObjectsError, builders::DeleteObjectsFluentBuilder},
        get_object::{GetObjectError, builders::GetObjectFluentBuilder},
        put_object::{PutObjectError, builders::PutObjectFluentBuilder},
    },
    primitives::ByteStream,
    types::{Delete, ObjectIdentifier},
};
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::storage::backend::{
    Backend, ExpectedAnswer, ListedObject, ListedPage, ListingRequest, ListingStartBound, PutCondition, RequestOptions,
    normalize_etag, send_request,
};
use crate::storage::object_storage::ObjectStorage;
use crate::storage::paths::{DEFAULT_STATE_PREFIX, JobPaths};
use crate::storage::state_codec::JobStateCodecKind;
use crate::{
    Error, JobDefinitionRegistry, MetricsSink, Retrier, RetrierConfig, RetryStep, StorageError, StorageResult,
    storage::s3_error::{S3Failure, classify_delete_failures, map_s3_error},
};

// TODO(high): add test s3 storage with Toxiproxy for testing network problems (chaos test))

/// Timeout of a single S3 request unless overridden, the reading of the answer's body included -
/// the SDK's own operation timeout ends once that answer's headers have landed.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// One request, in the shape the wrapper around it takes.
///
/// Boxed because the SDK's own futures run to tens of kilobytes: inlined, one would widen the poll
/// loop's future for the whole life of a worker, where boxed it is allocated for the length of the
/// request and freed with it.
type BoxedRequest<'a, T, E> = Pin<Box<dyn Future<Output = Result<T, S3Failure<E>>> + Send + 'a>>;

/// Keys requested per `LIST` page while scanning a job's outdated iterations. 1000 is the maximum
/// a single `ListObjectsV2` response can carry, so the common empty-tail scan costs one request.
/// Being the protocol maximum, it doubles as the upper bound
/// [`S3Config::with_list_page_size`] accepts.
const DEFAULT_LIST_PAGE_SIZE: i32 = 1000;

/// Keys sent per multi-object delete request; 1000 is the maximum `DeleteObjects` accepts. Being
/// the protocol maximum, it doubles as the upper bound
/// [`S3Config::with_delete_batch_size`] accepts.
const DEFAULT_DELETE_BATCH_SIZE: usize = 1000;

/// Version `*`, which the creation of a job's first iteration is conditioned on: it stands for any
/// version rather than naming one.
const ANY_VERSION: &str = "*";

/// Connection details of the S3-compatible bucket a pool keeps job state in, passed to
/// [`JobsManagerBuilder::s3`](crate::JobsManagerBuilder::s3).
///
/// Build with [`S3Config::new`] and override the optional parts with the `with_*` methods.
pub struct S3Config {
    endpoint: String,
    access_key_id: String,
    secret_access_key: String,
    bucket_name: String,
    region: String,
    state_prefix: String,
    job_state_codec: JobStateCodecKind,
    request_timeout: Duration,
    retrier_config: RetrierConfig,
    list_page_size: i32,
    delete_batch_size: usize,
    is_container_creation_allowed: bool,
}

impl S3Config {
    /// Connection details of the bucket holding job state. Everything else takes a default that
    /// the `with_*` methods override.
    ///
    /// Pass an `https://` endpoint to use TLS - there is no separate switch for it.
    pub fn new(
        endpoint: impl Into<String>,
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        bucket_name: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            bucket_name: bucket_name.into(),
            region: region.into(),
            state_prefix: DEFAULT_STATE_PREFIX.to_string(),
            job_state_codec: JobStateCodecKind::Json,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            retrier_config: RetrierConfig::default(),
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            delete_batch_size: DEFAULT_DELETE_BATCH_SIZE,
            is_container_creation_allowed: true,
        }
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

    /// Timeout applied to one S3 request, the reading of its body included. Defaults to 5s.
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

    /// Keys one cleanup `LIST` asks for. Defaults to 1000, which is the maximum a single
    /// `ListObjectsV2` response carries; lower it only to bound the size of a single response.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] outside `1..=1000`.
    pub fn with_list_page_size(mut self, list_page_size: i32) -> Result<Self, Error> {
        if !(1..=DEFAULT_LIST_PAGE_SIZE).contains(&list_page_size) {
            return Err(Error::Other(format!(
                "s3 list page size must be within 1..={DEFAULT_LIST_PAGE_SIZE}"
            )));
        }
        self.list_page_size = list_page_size;
        Ok(self)
    }

    /// Keys one multi-object delete carries. Defaults to 1000, the maximum `DeleteObjects`
    /// accepts on AWS; lower it for a backend whose limit is smaller.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] outside `1..=1000`. Zero would make cleanup split its work into
    /// empty chunks and panic.
    pub fn with_delete_batch_size(mut self, delete_batch_size: usize) -> Result<Self, Error> {
        if !(1..=DEFAULT_DELETE_BATCH_SIZE).contains(&delete_batch_size) {
            return Err(Error::Other(format!(
                "s3 delete batch size must be within 1..={DEFAULT_DELETE_BATCH_SIZE}"
            )));
        }
        self.delete_batch_size = delete_batch_size;
        Ok(self)
    }

    /// Creates the bucket when it is missing. Defaults to `true`.
    ///
    /// Turn it off where the process has no permission to create one: the backend then only makes
    /// sure the bucket is reachable, and refuses to start when it is not.
    #[must_use]
    pub const fn with_container_creation(mut self, is_container_creation_allowed: bool) -> Self {
        self.is_container_creation_allowed = is_container_creation_allowed;
        self
    }
}

/// Builds the SDK client configuration a pool reaches the store through.
///
/// Kept out of [`S3Backend::build`], which reaches the endpoint to check the bucket: the
/// configuration is otherwise unobservable without a store to talk to.
async fn build_client_config(config: &S3Config) -> aws_sdk_s3::Config {
    // TODO(med): add creds options
    let credentials = aws_sdk_s3::config::Credentials::new(
        config.access_key_id.clone(),
        config.secret_access_key.clone(),
        None,
        None,
        "static",
    );

    // No operation timeout: the SDK's ends when an answer's headers land, leaving the reading of a
    // body outside it, and a second bound over the one `send_request` puts around the whole request
    // would be a parallel mechanism for the same job. The connect timeout the defaults carry stays,
    // and so does stalled-stream protection - neither bounds what this one bounds.
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new(config.region.clone()))
        .credentials_provider(credentials)
        .load()
        .await;

    aws_sdk_s3::config::Builder::from(&sdk_config)
        .endpoint_url(config.endpoint.clone())
        .force_path_style(true)
        // Retries belong to `Retrier` - see the invariant on what a call of `Storage` may cost in
        // `AGENTS.md`.
        .retry_config(RetryConfig::disabled())
        .build()
}

/// State objects of one S3-compatible bucket: each job iteration is one object.
///
/// Concurrent writers are resolved with conditional PUTs (`If-Match` on the current `ETag` for
/// updates, `If-None-Match: *` for a job's first iteration) rather than an external lock service.
pub(crate) struct S3Backend {
    // TODO(med): save job settings separately and provide an API for changing settings
    client: Client,
    bucket_name: String,
    /// Content type state objects are written with, which is what the codec of this backend names.
    content_type: &'static str,
    metrics: Arc<dyn MetricsSink>,
    list_page_size: i32,
    delete_batch_size: usize,
    request_timeout: Duration,
}

impl S3Backend {
    /// Connects to `config.endpoint`, makes sure `config.bucket_name` is reachable - creating it
    /// when it is missing and `config.is_container_creation_allowed` allows it - and hands out the
    /// storage built over the bucket.
    ///
    /// The check and the creation are retried per `config.retrier_config`; a persistent failure
    /// (including one from a concurrent creator returning a non-409 error, and a missing bucket the
    /// configuration forbids creating) is returned as [`Error::Other`](crate::Error::Other).
    pub(crate) async fn build(
        config: S3Config,
        registry: Arc<dyn JobDefinitionRegistry>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<ObjectStorage<Self>, Error> {
        info!("Starting jobmanager with s3 storage {}", config.endpoint);

        let client = Client::from_conf(build_client_config(&config).await);

        // TODO(med): add check that conditional requests work for specific S3.
        // For example, some S3 backends ignore the header and atomicity breaks.

        let retrier = Retrier::new(config.retrier_config.clone());
        let cancel_token = CancellationToken::new();
        let request_timeout = config.request_timeout;

        // Check if bucket exists, create if needed
        let bucket_name = config.bucket_name.clone();
        let is_container_creation_allowed = config.is_container_creation_allowed;
        let client_for_retry = client.clone();
        retrier
            .retry(
                move || {
                    let client = client_for_retry.clone();
                    let bucket_name = bucket_name.clone();
                    async move {
                        let Ok(head_answer) =
                            tokio::time::timeout(request_timeout, client.head_bucket().bucket(&bucket_name).send())
                                .await
                        else {
                            return Ok(RetryStep::Retry(StorageError::Timeout));
                        };
                        match head_answer {
                            Ok(_) => {
                                info!("Bucket {} exists", bucket_name);
                                Ok(RetryStep::Done(()))
                            }
                            Err(aws_sdk_s3::error::SdkError::ServiceError(se)) if se.raw().status().as_u16() == 404 => {
                                if !is_container_creation_allowed {
                                    // Nothing to repeat: the bucket is absent and this pool may not
                                    // create one. Not `NotFound`, whose text names a job - and at
                                    // start-up there is none.
                                    return Err(StorageError::Backend(format!(
                                        "bucket {bucket_name} does not exist and container creation is turned off"
                                    )));
                                }
                                let Ok(creation_answer) = tokio::time::timeout(
                                    request_timeout,
                                    client.create_bucket().bucket(&bucket_name).send(),
                                )
                                .await
                                else {
                                    return Ok(RetryStep::Retry(StorageError::Timeout));
                                };
                                match creation_answer {
                                    Ok(_) => {
                                        info!("Created bucket {}", bucket_name);
                                        Ok(RetryStep::Done(()))
                                    }
                                    Err(aws_sdk_s3::error::SdkError::ServiceError(se))
                                        if se.raw().status().as_u16() == 409 =>
                                    {
                                        info!("Bucket {} already exists", bucket_name);
                                        Ok(RetryStep::Done(()))
                                    }
                                    Err(e) => map_s3_error(&e).into_retry_step(),
                                }
                            }
                            Err(e) => map_s3_error(&e).into_retry_step(),
                        }
                    }
                },
                &cancel_token,
            )
            .await
            .map_err(|e| Error::Other(format!("Failed to init bucket: {e}")))?;

        let codec = config.job_state_codec.build();
        let state_object_keys = JobPaths::new(config.state_prefix, codec.as_ref());

        Ok(ObjectStorage::new(
            Self {
                client,
                bucket_name: config.bucket_name,
                content_type: codec.content_type(),
                metrics,
                list_page_size: config.list_page_size,
                delete_batch_size: config.delete_batch_size,
                request_timeout,
            },
            state_object_keys,
            codec,
            registry,
            retrier,
        ))
    }

    /// What every request of this backend is sent and recorded under.
    fn build_request_options<'a>(&'a self, operation: &'a str) -> RequestOptions<'a> {
        RequestOptions {
            operation,
            metrics: self.metrics.as_ref(),
            request_timeout: self.request_timeout,
        }
    }

    /// One read: the request and the reading of the body it is answered with.
    ///
    /// Both live in one call because the SDK's operation timeout ends once the answer's headers have
    /// landed: a body read outside this future answers to neither the configured timeout nor the
    /// token, what a cancellation abandons being only what the future handed to
    /// [`send_request`] covers. The SDK's stalled-stream protection does reach a body left
    /// outside, but it answers a stream that stopped and not the bound this backend was configured
    /// with.
    fn read_object(request: GetObjectFluentBuilder) -> BoxedRequest<'static, Vec<u8>, GetObjectError> {
        Box::pin(async move {
            let output = request.send().await.map_err(S3Failure::from_refusal)?;
            let state_bytes = output.body.collect().await.map_err(S3Failure::Unread)?.into_bytes();

            Ok(Vec::from(state_bytes))
        })
    }

    /// The same read where the version the answer stands at is part of what was asked for, in the
    /// shape a caller that accepts "not modified" takes: `Some` is the state the object answered
    /// with, and the `None` an unchanged object amounts to is supplied by
    /// [`ExpectedAnswer::NotModified`] instead - the refusal never reaches here.
    ///
    /// The entity-tag is checked inside the request: an answer naming none is a request that failed,
    /// and recording it as the `OK` its status line claimed would put one request under two pairs.
    fn read_changed_object(
        request: GetObjectFluentBuilder,
        key: &str,
    ) -> BoxedRequest<'_, Option<(String, Vec<u8>)>, GetObjectError> {
        Box::pin(async move {
            let output = request.send().await.map_err(S3Failure::from_refusal)?;
            let version = output.e_tag().map(normalize_etag);
            let state_bytes = output.body.collect().await.map_err(S3Failure::Unread)?.into_bytes();

            let Some(version) = version else {
                return Err(S3Failure::Unusable(StorageError::Backend(format!(
                    "Missing etag reading iteration {key}"
                ))));
            };

            Ok(Some((version, Vec::from(state_bytes))))
        })
    }

    /// One write: the request and the version its answer has to name, which is what the next
    /// condition is held against.
    ///
    /// The entity-tag is checked inside the request for the reason
    /// [`S3Backend::read_changed_object`] carries.
    fn write_object(request: PutObjectFluentBuilder, key: &str) -> BoxedRequest<'_, String, PutObjectError> {
        Box::pin(async move {
            let output = request.send().await.map_err(S3Failure::from_refusal)?;

            output
                .e_tag()
                .map(normalize_etag)
                .ok_or_else(|| S3Failure::Unusable(StorageError::Backend(format!("Missing etag writing {key}"))))
        })
    }

    /// One multi-object delete: the request, and the per-key report its `200` carries.
    ///
    /// The report is read inside the request for the reason [`S3Backend::read_changed_object`]
    /// carries. Any reported failure is an error, including one whose entry names no key; whether it
    /// is retryable is decided by [`classify_delete_failures`].
    fn delete_object_batch(request: DeleteObjectsFluentBuilder) -> BoxedRequest<'static, (), DeleteObjectsError> {
        Box::pin(async move {
            let output = request.send().await.map_err(S3Failure::from_refusal)?;

            classify_delete_failures(output.errors()).map_or(Ok(()), |failure| Err(S3Failure::Unusable(failure)))
        })
    }

    /// Deletes one state object. `DELETE` answers 204 for a key that is already gone, which is
    /// what makes iteration cleanup idempotent.
    async fn delete_object(&self, key: &str, cancel_token: &CancellationToken) -> StorageResult<()> {
        send_request(
            box_sdk_request(self.client.delete_object().bucket(&self.bucket_name).key(key).send()),
            self.build_request_options("DELETE"),
            ExpectedAnswer::Success,
            cancel_token,
        )
        .await
        .map(|_| ())
    }

    /// The keys of one batch in the payload `DeleteObjects` takes.
    fn build_delete_payload(keys: &[String]) -> StorageResult<Delete> {
        let mut objects = Vec::with_capacity(keys.len());
        for key in keys {
            objects.push(
                ObjectIdentifier::builder()
                    .key(key)
                    .build()
                    .map_err(|e| StorageError::Other(format!("Failed to build delete entry for {key}: {e}")))?,
            );
        }

        Delete::builder()
            .set_objects(Some(objects))
            .build()
            .map_err(|e| StorageError::Other(format!("Failed to build delete request: {e}")))
    }
}

#[async_trait::async_trait]
impl Backend for S3Backend {
    fn listing_start_bound(&self) -> ListingStartBound {
        ListingStartBound::Exclusive
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
        // TODO(med): to check - set {PutObjectOptions{DisableMultipart: true} and send correct file.
        // When multipart is on there may be problems with the etag. Atomic write with If-Match
        let write = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(key)
            .body(ByteStream::from(body))
            .content_type(self.content_type);
        let write = match condition {
            PutCondition::CreateOnly => write.if_none_match(ANY_VERSION),
            PutCondition::MatchVersion(version) => write.if_match(version),
        };

        send_request(
            Self::write_object(write, key),
            self.build_request_options("PUT"),
            ExpectedAnswer::Success,
            cancel_token,
        )
        .await
    }

    async fn get_object(
        &self,
        key: &str,
        expected_version: &str,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Vec<u8>> {
        let read = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key)
            .if_match(expected_version);

        send_request(
            Self::read_object(read),
            self.build_request_options("GET"),
            ExpectedAnswer::Success,
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
        let read = self
            .client
            .get_object()
            .bucket(&self.bucket_name)
            .key(key)
            .if_none_match(known_version);

        send_request(
            Self::read_changed_object(read, key),
            self.build_request_options("GET"),
            ExpectedAnswer::NotModified(None),
            cancel_token,
        )
        .await
    }

    async fn list_objects(
        &self,
        request: ListingRequest<'_>,
        cancel_token: &CancellationToken,
    ) -> StorageResult<ListedPage> {
        let mut listing = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket_name)
            .prefix(request.prefix)
            .max_keys(request.page_size)
            .set_continuation_token(request.cursor);
        if let Some(start_key) = request.start_key {
            listing = listing.start_after(start_key);
        }

        let output = send_request(
            box_sdk_request(listing.send()),
            self.build_request_options("LIST"),
            ExpectedAnswer::Success,
            cancel_token,
        )
        .await?;

        let next_cursor = output.next_continuation_token().map(ToString::to_string);
        let objects = output
            .contents()
            .iter()
            .filter_map(|object| {
                object.key().map(|key| ListedObject {
                    key: key.to_string(),
                    version: object.e_tag().map(normalize_etag),
                })
            })
            .collect();

        Ok(ListedPage { objects, next_cursor })
    }

    async fn delete_objects(&self, keys: &[String], cancel_token: &CancellationToken) -> StorageResult<()> {
        // The steady-state case is a single iteration falling out of the retention window, and a
        // single-object DELETE is free at every provider, while a multi-object one is not.
        if let [key] = keys {
            return self.delete_object(key, cancel_token).await;
        }

        for chunk in keys.chunks(self.delete_batch_size) {
            let delete = Self::build_delete_payload(chunk)?;

            send_request(
                Self::delete_object_batch(self.client.delete_objects().bucket(&self.bucket_name).delete(delete)),
                self.build_request_options("DELETE"),
                ExpectedAnswer::Success,
                cancel_token,
            )
            .await?;
        }

        Ok(())
    }
}

/// Puts one SDK request into the shape its wrapper takes.
///
/// For the operations whose whole answer is what the SDK hands back - a single delete, a listing -
/// so there is no body left to fail separately and nothing in the answer left to check:
/// [`S3Failure::Refused`] is the only shape such a request can take.
fn box_sdk_request<'a, T: 'a, E: 'a>(
    request: impl Future<Output = Result<T, SdkError<E>>> + Send + 'a,
) -> BoxedRequest<'a, T, E> {
    Box::pin(async move { request.await.map_err(S3Failure::from_refusal) })
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
    use aws_smithy_http_client::test_util::{ReplayEvent, StaticReplayClient};
    use aws_smithy_types::body::SdkBody;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        task::JoinHandle,
    };

    use super::*;
    use crate::tests::common::UnusedJobRegistry;
    use crate::tests::common::counting_metrics::CountingMetrics;
    use crate::tests::common::scripted_endpoint::{ScriptedEndpoint, build_http_response};
    use crate::tests::common::silent_endpoint::{cancel_after_delay, start_silent_endpoint};
    use crate::{JobCode, JobMeta, Storage};

    /// Timeout the cases against a local endpoint give a request. Long enough that no scheduling
    /// hiccup reaches it, short enough that a case which really waits it out still ends quickly.
    const LOCAL_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(1);

    /// Retention boundary the cleanup listing case asks about: the newest iteration cleanup may
    /// delete.
    const TESTED_RETENTION_BOUNDARY: u64 = 2;

    /// Key of the oldest iteration that must be kept - the one after
    /// [`TESTED_RETENTION_BOUNDARY`] - as it travels in a query parameter. Stated rather than built
    /// so the expectation does not come from the key builder under test.
    const OLDEST_KEPT_ITERATION_TARGET_KEY: &str = "jobs%2Fjob%2Fstate-18446744073709551612.json";

    fn build_config() -> S3Config {
        S3Config::new("http://localhost:9000", "key", "secret", "jobs", "us-east-1")
    }

    /// Builds a store whose S3 client answers with `response`, recording every request it makes
    /// into `metrics`.
    ///
    /// Constructed field by field rather than through [`S3Backend::build`], which
    /// reaches a real endpoint to check the bucket and builds a client of its own; there is no
    /// other seam for a canned response, and adding one to the production configuration for a test
    /// would be a second way to configure the same thing.
    fn build_store_recording(response: http::Response<SdkBody>, metrics: Arc<dyn MetricsSink>) -> S3Backend {
        let request = http::Request::builder()
            .uri("http://localhost:9000/jobs")
            .body(SdkBody::empty())
            .expect("the scripted request must build");
        let http_client = StaticReplayClient::new(vec![ReplayEvent::new(request, response)]);

        let s3_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("key", "secret", None, None, "static"))
            .endpoint_url("http://localhost:9000")
            .force_path_style(true)
            .http_client(http_client)
            // The retry budget under test is the cleaner's; an SDK-level retry would spend requests
            // the request quota of this crate does not account for.
            .retry_config(RetryConfig::disabled())
            .build();

        S3Backend {
            client: Client::from_conf(s3_config),
            bucket_name: "jobs".to_string(),
            content_type: JobStateCodecKind::Json.build().content_type(),
            metrics,
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            delete_batch_size: DEFAULT_DELETE_BATCH_SIZE,
            request_timeout: LOCAL_ENDPOINT_TIMEOUT,
        }
    }

    /// Builds a store pointed at `endpoint` field by field, reaching it over a real connection.
    ///
    /// Constructed this way rather than through [`S3Backend::build`], which checks the bucket before
    /// handing one out and therefore cannot be reached against an endpoint that answers nothing;
    /// adding a way past that check to the production path would be a second way to configure the
    /// same thing.
    fn build_store_against(endpoint: &str, metrics: Arc<dyn MetricsSink>) -> S3Backend {
        let s3_config = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new("us-east-1"))
            .credentials_provider(Credentials::new("key", "secret", None, None, "static"))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .build();

        S3Backend {
            client: Client::from_conf(s3_config),
            bucket_name: "jobs".to_string(),
            content_type: JobStateCodecKind::Json.build().content_type(),
            metrics,
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            delete_batch_size: DEFAULT_DELETE_BATCH_SIZE,
            request_timeout: LOCAL_ENDPOINT_TIMEOUT,
        }
    }

    /// A listener that answers the headers of a `200` and never sends the body they promise.
    ///
    /// This is the failure the SDK's own operation timeout does not catch: that timeout ends once
    /// these headers have landed, leaving the reading of a body nobody will send as the only thing
    /// still standing.
    async fn start_headed_endpoint() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("a loopback listener must bind");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("a bound listener has an address")
        );
        let accepting = tokio::spawn(async move {
            while let Ok((mut connection, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut chunk = [0_u8; 1024];
                    let _read = connection.read(&mut chunk).await;
                    let _written = connection
                        .write_all(b"HTTP/1.1 200 OK\r\nETag: \"an-etag\"\r\nContent-Length: 16\r\n\r\n")
                        .await;
                    // The promised body is never written and the connection is held open, so no end
                    // of stream arrives to close the read in its place.
                    std::future::pending::<()>().await;
                });
            }
        });

        (endpoint, accepting)
    }

    /// Builds a store whose S3 client answers with `response` and whose requests nobody counts.
    fn build_store_answering(response: http::Response<SdkBody>) -> S3Backend {
        build_store_recording(response, Arc::new(crate::NoopMetrics))
    }

    /// The storage a case reaching `Storage` goes through, over `store`.
    fn build_storage_over(store: S3Backend) -> ObjectStorage<S3Backend> {
        let codec = JobStateCodecKind::Json.build();

        ObjectStorage::new(
            store,
            JobPaths::new(DEFAULT_STATE_PREFIX.to_string(), codec.as_ref()),
            codec,
            Arc::new(UnusedJobRegistry),
            Retrier::new(RetrierConfig::default()),
        )
    }

    /// The `200` a multi-object delete answers with when it deleted one key and could not delete
    /// the other, as documented for `DeleteObjects`.
    fn build_delete_response(failure: Option<&str>) -> http::Response<SdkBody> {
        let failure_element = failure.map_or_else(String::new, |code| {
            format!(
                "<Error><Key>jobs/job/state-00000000000000000002.json</Key><Code>{code}</Code>\
                 <Message>scripted failure</Message></Error>"
            )
        });
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <DeleteResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
             <Deleted><Key>jobs/job/state-00000000000000000001.json</Key></Deleted>\
             {failure_element}</DeleteResult>"
        );

        http::Response::builder()
            .status(200)
            .header("content-type", "application/xml")
            .body(SdkBody::from(body))
            .expect("the scripted response must build")
    }

    fn build_deleted_keys() -> Vec<String> {
        vec![
            "jobs/job/state-00000000000000000001.json".to_string(),
            "jobs/job/state-00000000000000000002.json".to_string(),
        ]
    }

    /// A per-key failure arrives inside a `200`, so nothing but the parsed body distinguishes this
    /// from a clean delete - and a transient one has to come back retryable for the cleaner to
    /// retry it at all.
    #[tokio::test]
    async fn test_multi_object_delete_reporting_a_transient_failure_returns_a_retryable_error() {
        let store = build_store_answering(build_delete_response(Some("SlowDown")));

        let error = store
            .delete_objects(&build_deleted_keys(), &CancellationToken::new())
            .await
            .expect_err("a body reporting a failed key must not be reported as success");

        assert!(
            matches!(error, StorageError::RateLimited),
            "a throttled key must reach the caller as the rate-limit error, got: {error:?}"
        );
        assert!(error.is_retryable(), "a throttled key must be retried");
    }

    #[tokio::test]
    async fn test_multi_object_delete_reporting_a_permanent_failure_returns_a_non_retryable_error() {
        let store = build_store_answering(build_delete_response(Some("AccessDenied")));

        let error = store
            .delete_objects(&build_deleted_keys(), &CancellationToken::new())
            .await
            .expect_err("a body reporting a failed key must not be reported as success");

        assert!(!error.is_retryable(), "a rejected key must be attempted exactly once");
    }

    #[tokio::test]
    async fn test_multi_object_delete_without_reported_failures_succeeds() {
        let store = build_store_answering(build_delete_response(None));

        store
            .delete_objects(&build_deleted_keys(), &CancellationToken::new())
            .await
            .expect("a body reporting no failure is a successful delete");
    }

    /// The `304` a store answers a conditional read with: a status and no body, exactly as the
    /// protocol prescribes.
    fn build_not_modified_response() -> http::Response<SdkBody> {
        http::Response::builder()
            .status(304)
            .body(SdkBody::empty())
            .expect("the scripted response must build")
    }

    /// A failed request carrying the error document S3 answers with, so the status under test is
    /// what decides the mapping rather than an unparsable body.
    fn build_failure_response(status: u16) -> http::Response<SdkBody> {
        http::Response::builder()
            .status(status)
            .header("content-type", "application/xml")
            .body(SdkBody::from(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <Error><Code>Scripted</Code><Message>scripted failure</Message></Error>",
            ))
            .expect("the scripted response must build")
    }

    /// A store that accepted the write and named no `ETag` leaves the worker with nothing to
    /// condition its next save on, so the save is refused - and billed as the request that failed
    /// rather than as the `OK` its status line claimed, because a quota is counted on that pair.
    ///
    /// Checked by breaking it: recording the write through `send_request` again, as this
    /// backend did before, records the `PUT` under `OK` and leaves `ERR` at zero.
    #[tokio::test]
    async fn a_write_answered_without_an_etag_is_refused() {
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_recording(
            http::Response::builder()
                .status(200)
                .body(SdkBody::empty())
                .expect("the scripted response must build"),
            Arc::clone(&metrics) as Arc<dyn MetricsSink>,
        );

        let write_result = store
            .put_object(
                "jobs/job/state-1.json",
                b"state".to_vec(),
                PutCondition::MatchVersion("an-etag"),
                &CancellationToken::new(),
            )
            .await;

        let Err(error) = write_result else {
            panic!("a write answered without a version must not read as a save")
        };
        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
        assert_eq!(metrics.storage_operations("PUT", "ERR"), 1);
        assert_eq!(metrics.storage_operations_total(), 1);
    }

    /// The iteration every conditional read below asks about.
    fn build_read_meta() -> JobMeta {
        JobMeta {
            code: JobCode::new("job"),
            iter_num: 1,
            version: "an-etag".to_string(),
        }
    }

    /// `304` is the answer a conditional read asks for, not a failure to translate. Taken for one,
    /// every poll of an unmoved iteration would come back as an error and cost the cold read the
    /// conditional read exists to avoid.
    #[tokio::test]
    async fn a_conditional_read_of_an_unmoved_iteration_returns_nothing() {
        let storage = build_storage_over(build_store_answering(build_not_modified_response()));

        let read = storage
            .get_changed_job(&build_read_meta(), &CancellationToken::new())
            .await
            .expect("a store answering 304 must not report a failure");

        assert!(read.is_none(), "an unmoved iteration must read as nothing to apply");
    }

    /// The table [`StorageError::is_retryable`] answers a backend's failures from, asserted through
    /// the read that produces them: a status the store itself clears has to reach the caller as a
    /// retryable variant, and everything else as one that is attempted exactly once. A status
    /// nobody mapped is the case that decides what an unfamiliar backend costs.
    #[tokio::test]
    async fn the_status_of_a_failed_conditional_read_decides_the_error_it_becomes() {
        let cases: Vec<(u16, fn(&StorageError) -> bool, bool, &str)> = vec![
            (401, |error| matches!(error, StorageError::Auth(_)), false, "Auth"),
            (403, |error| matches!(error, StorageError::Auth(_)), false, "Auth"),
            (
                404,
                |error| matches!(error, StorageError::NotFound(_)),
                false,
                "NotFound",
            ),
            (408, |error| matches!(error, StorageError::Timeout), true, "Timeout"),
            (
                412,
                |error| matches!(error, StorageError::ConcurrentModification(_)),
                false,
                "ConcurrentModification",
            ),
            (
                429,
                |error| matches!(error, StorageError::RateLimited),
                true,
                "RateLimited",
            ),
            (
                500,
                |error| matches!(error, StorageError::ServiceUnavailable),
                true,
                "ServiceUnavailable",
            ),
            (
                502,
                |error| matches!(error, StorageError::ServiceUnavailable),
                true,
                "ServiceUnavailable",
            ),
            (
                503,
                |error| matches!(error, StorageError::ServiceUnavailable),
                true,
                "ServiceUnavailable",
            ),
            (
                504,
                |error| matches!(error, StorageError::ServiceUnavailable),
                true,
                "ServiceUnavailable",
            ),
            (418, |error| matches!(error, StorageError::Backend(_)), false, "Backend"),
        ];

        for (status, is_expected_variant, is_retryable, expected_variant) in cases {
            let storage = build_storage_over(build_store_answering(build_failure_response(status)));

            let Err(error) = storage.get_changed_job(&build_read_meta(), &CancellationToken::new()).await else {
                panic!("status {status} must not read as a job")
            };

            assert!(
                is_expected_variant(&error),
                "status {status} must map to {expected_variant}, got: {error:?}"
            );
            assert_eq!(
                error.is_retryable(),
                is_retryable,
                "status {status} must be repeated: {is_retryable}, got: {error:?}"
            );
        }
    }

    /// A request the store refused is billed exactly like one it answered, so it has to be recorded
    /// under the status it came back with. A run quota states the *whole* of what a scenario cost,
    /// and a request that reached the store without reaching the metric makes that statement false
    /// without failing anything.
    #[tokio::test]
    async fn a_failed_conditional_read_is_recorded_under_the_status_it_came_back_with() {
        let metrics = Arc::new(CountingMetrics::default());

        for status in [503_u16, 429_u16] {
            let storage = build_storage_over(build_store_recording(
                build_failure_response(status),
                Arc::clone(&metrics) as Arc<dyn MetricsSink>,
            ));

            let read = storage.get_changed_job(&build_read_meta(), &CancellationToken::new()).await;

            assert!(read.is_err(), "status {status} must not read as a job");
        }

        assert_eq!(
            metrics.storage_operations("GET", "503"),
            1,
            "a read the store refused as unavailable must be recorded as a GET that returned 503"
        );
        assert_eq!(
            metrics.storage_operations("GET", "429"),
            1,
            "and a throttled one as a GET that returned 429"
        );
        assert_eq!(
            metrics.storage_operations_total(),
            2,
            "and nothing was recorded under any other pair"
        );
    }

    /// A quota states what one call of `Storage` costs, and an SDK that repeats a request on its
    /// own makes that statement false without failing anything: the repetition is neither counted
    /// nor bounded by `Retrier`, which owns the retry policy of this crate.
    #[tokio::test]
    async fn the_production_client_does_not_retry_inside_the_sdk() {
        let client_config = build_client_config(&build_config()).await;

        let attempts = client_config
            .retry_config()
            .map(aws_smithy_types::retry::RetryConfig::max_attempts);

        assert_eq!(attempts, Some(1), "the production client must attempt a request once");
    }

    /// A pool that found no bucket and then lost the creation of it to somebody else still starts:
    /// the bucket it needs is there, which is the whole of what the check asked for. This is the arm
    /// every deployment of more than one process hits on its first start, and no case against a real
    /// store can reach it deterministically - two builds racing may each see the bucket absent, or
    /// the second may see it present.
    ///
    /// Checked by breaking it: answering the lost race with `map_s3_error(&e).into_retry_step()`
    /// instead of `RetryStep::Done` spends the whole attempt budget on a `409` no repetition clears,
    /// and the build ends as an error.
    #[tokio::test]
    async fn a_pool_that_lost_the_creation_of_its_bucket_still_starts() {
        let endpoint = ScriptedEndpoint::start(vec![
            build_http_response(404, &[], ""),
            build_http_response(
                409,
                &[("content-type", "application/xml")],
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
                 <Error><Code>BucketAlreadyOwnedByYou</Code><Message>scripted race</Message></Error>",
            ),
        ])
        .await;
        let config = S3Config::new(endpoint.endpoint(), "key", "secret", "jobs", "us-east-1")
            .with_request_timeout(LOCAL_ENDPOINT_TIMEOUT);

        let storage = S3Backend::build(config, Arc::new(UnusedJobRegistry), Arc::new(crate::NoopMetrics)).await;

        storage.expect("a bucket somebody else created first is the outcome this pool wanted");
        let requested_targets = endpoint.requested_targets();
        assert_eq!(
            requested_targets.len(),
            2,
            "the check must have been followed by the creation that lost, got: {requested_targets:?}"
        );
    }

    #[test]
    fn test_delete_batch_size_of_zero_is_rejected() {
        assert!(build_config().with_delete_batch_size(0).is_err());
    }

    #[test]
    fn test_delete_batch_size_above_the_protocol_maximum_is_rejected() {
        assert!(build_config().with_delete_batch_size(1001).is_err());
    }

    #[test]
    fn test_list_page_size_below_one_is_rejected() {
        assert!(build_config().with_list_page_size(0).is_err());
    }

    #[test]
    fn test_list_page_size_above_the_protocol_maximum_is_rejected() {
        assert!(build_config().with_list_page_size(1001).is_err());
    }

    #[test]
    fn test_delete_batch_size_at_the_accepted_bounds_is_applied() {
        let lowest = build_config()
            .with_delete_batch_size(1)
            .expect("1 is inside the accepted range");
        let highest = build_config()
            .with_delete_batch_size(1000)
            .expect("1000 is the protocol maximum");

        assert_eq!(lowest.delete_batch_size, 1);
        assert_eq!(highest.delete_batch_size, 1000);
    }

    #[test]
    fn test_list_page_size_at_the_accepted_bounds_is_applied() {
        let lowest = build_config().with_list_page_size(1).expect("1 is inside the accepted range");
        let highest = build_config().with_list_page_size(1000).expect("1000 is the protocol maximum");

        assert_eq!(lowest.list_page_size, 1);
        assert_eq!(highest.list_page_size, 1000);
    }
    /// The boundary the cleanup listing was calculated for has to reach the store in the parameter
    /// this provider reads it from. This provider leaves the boundary key out of its answer, so what
    /// it is given is the key of the oldest iteration that must be kept - one key past the boundary.
    ///
    /// The store refuses the listing on purpose - what is under test is the address the request
    /// carried, and the target is recorded whatever the answer was.
    ///
    /// Checked by breaking it: dropping the `start_after` from `S3Backend::list_objects` leaves the
    /// parameter off the target and the assertion fails.
    #[tokio::test]
    async fn a_cleanup_listing_asks_the_store_to_start_after_the_oldest_kept_key() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(500, &[], "")]).await;
        let storage = build_storage_over(build_store_against(endpoint.endpoint(), Arc::new(crate::NoopMetrics)));

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
            requested_targets[0].contains(&format!("start-after={OLDEST_KEPT_ITERATION_TARGET_KEY}")),
            "this provider leaves the boundary key out, so the listing starts one key past it, got: {requested_targets:?}"
        );
    }

    /// A worker standing on a read must let go when its pool is asked to stop, rather than hold on
    /// until the request times out. Five of the seven `Storage` methods reach the service without a
    /// `Retrier` around them, so the token has to be observed inside the request itself.
    ///
    /// Checked by breaking it: dropping the `tokio::select!` from `send_request` leaves this waiting
    /// the whole of [`LOCAL_ENDPOINT_TIMEOUT`] and the elapsed assertion fails.
    #[tokio::test]
    async fn a_cancelled_token_drops_a_conditional_read_in_flight() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let storage = build_storage_over(build_store_against(&endpoint, Arc::new(crate::NoopMetrics)));
        let cancel_token = CancellationToken::new();
        cancel_after_delay(&cancel_token);

        let started = Instant::now();
        let read_result = storage.get_changed_job(&build_read_meta(), &cancel_token).await;
        let elapsed = started.elapsed();

        accepting.abort();
        let Err(error) = read_result else {
            panic!("an endpoint that never answers must not read as a job")
        };
        assert!(
            matches!(error, StorageError::Cancelled),
            "a cancelled read must reach the caller as a cancellation, got: {error:?}"
        );
        assert!(
            elapsed < LOCAL_ENDPOINT_TIMEOUT,
            "the read must end on the token rather than on the timeout, took {elapsed:?}"
        );
    }

    /// The same for cleanup, which would otherwise hold a stopping pool for the whole timeout its
    /// delete is standing in.
    ///
    /// The status is what says the request was abandoned rather than refused: a cancelled request
    /// was received and will be billed, so it is counted - and a pair of its own is what keeps a
    /// pool being stopped from reading as an outage of the store.
    ///
    /// Checked by breaking it: dropping the `tokio::select!` from `send_request` leaves the delete
    /// to run its timeout out, and the cleanup ends as a timeout instead of a cancellation.
    #[tokio::test]
    async fn a_cancelled_token_drops_a_delete_in_flight() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let metrics = Arc::new(CountingMetrics::default());
        let storage = build_storage_over(build_store_against(
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
            "and nothing but that delete may have been asked for"
        );
    }

    /// A token already cancelled when the request is handed over stops it before anything is sent,
    /// and what was never sent is never billed: counting it would put a request nobody made into
    /// the very metric the quotas are counted on.
    ///
    /// Checked by breaking it: recording every answer of `send_request` puts one `GET` under
    /// `CANCELLED` in the counters.
    #[tokio::test]
    async fn a_request_the_token_stopped_before_it_was_sent_is_recorded_under_nothing() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_against(&endpoint, Arc::clone(&metrics) as Arc<dyn MetricsSink>);
        let cancel_token = CancellationToken::new();
        cancel_token.cancel();

        let read_result = store
            .get_changed_object("jobs/job/state-1.json", "an-etag", &cancel_token)
            .await;

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
            "a request that was never sent must be recorded under nothing"
        );
    }

    /// The SDK's operation timeout ends once an answer's headers have landed, so a body left outside
    /// the future `send_request` bounds does not answer to the configured timeout. This is the case
    /// that says the reading of the body is inside it.
    ///
    /// The window is what names the bound that ended the read: the SDK's stalled-stream protection
    /// reaches a stalled body too, and measured against this endpoint it takes about six seconds -
    /// far outside the window below, which only [`LOCAL_ENDPOINT_TIMEOUT`] can land in.
    #[tokio::test]
    async fn a_read_whose_body_never_arrives_ends_on_the_request_timeout() {
        let (endpoint, accepting) = start_headed_endpoint().await;
        let store = build_store_against(&endpoint, Arc::new(crate::NoopMetrics));

        let started = Instant::now();
        let read_result = store
            .get_object("jobs/job/state-1.json", "an-etag", &CancellationToken::new())
            .await;
        let elapsed = started.elapsed();

        accepting.abort();
        assert!(
            matches!(read_result, Err(StorageError::Timeout)),
            "a body that never arrives must end the read as a timeout, got: {read_result:?}"
        );
        assert!(
            elapsed >= LOCAL_ENDPOINT_TIMEOUT,
            "the read must have waited for its own bound rather than been ended by the SDK, took {elapsed:?}"
        );
        assert!(
            elapsed < LOCAL_ENDPOINT_TIMEOUT * 3,
            "and it must end on that bound rather than on the connection, took {elapsed:?}"
        );
    }
}
