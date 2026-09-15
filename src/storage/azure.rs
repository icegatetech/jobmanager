use std::{future::Future, num::NonZero, sync::Arc, time::Duration};

use azure_core::{
    Bytes,
    credentials::Secret,
    error::ErrorKind,
    http::{
        ClientOptions, Etag, PageIterator, RequestContent, Response, RetryOptions, Url, XmlFormat, policies::Policy,
    },
};
use azure_storage_blob::{
    BlobClient, BlobContainerClient, BlobContainerClientOptions,
    models::{
        BlobClientDeleteOptions, BlobClientDownloadOptions, BlobClientUploadOptions,
        BlobContainerClientListBlobsOptions, ListBlobsResponse,
    },
};
use futures_util::TryStreamExt;
use tokio_util::sync::CancellationToken;
use tracing::info;

use crate::storage::azure_signing::SharedKeySigningPolicy;
use crate::storage::backend::{
    Backend, ExpectedAnswer, ListedObject, ListedPage, ListingRequest, ListingStartBound, ProviderError, PutCondition,
    RequestOptions, normalize_etag, send_request,
};
use crate::storage::object_storage::ObjectStorage;
use crate::storage::paths::{DEFAULT_STATE_PREFIX, JobPaths};
use crate::storage::state_codec::JobStateCodecKind;
use crate::{
    Error, JobDefinitionRegistry, MetricsSink, Retrier, RetrierConfig, RetryStep, StorageError, StorageResult,
};

/// Timeout of a single Azure operation unless overridden. The transport the SDK builds bounds
/// connecting and the silence between two reads, but neither bounds the operation as a whole -
/// without this a hung request holds the worker that issued it for whatever the transport allows.
const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Blobs requested per `LIST` page while scanning a job's outdated iterations. 5000 is the maximum
/// a single `List Blobs` response can carry, so the common empty-tail scan costs one request. Being
/// the protocol maximum, it doubles as the upper bound
/// [`AzureConfig::with_list_page_size`] accepts.
const DEFAULT_LIST_PAGE_SIZE: i32 = 5000;

/// Bytes the SDK moves in one request, which is what keeps one call of [`Storage`](crate::Storage)
/// costing one request: this is the size above which the SDK splits a transfer on its own - a write
/// into `Put Block`/`Put Block List` and a read into ranged `Get Blob` requests - and a quota stops
/// being a number. There is no switch that turns the splitting off, so the threshold is set as high
/// as the protocol allows: the value is the largest body one `Put Blob` accepts.
///
/// Declared in `usize`, the narrower of the two units the SDK asks it in, so that a target whose
/// `usize` cannot carry the threshold fails to build rather than transfers in partitions of
/// whatever did fit - a quota moved by the target it was compiled for, and by nothing the code says.
///
/// TODO(med): a state object above this is still stored, at the cost of one request per partition
/// instead of one per call - the very quota shift the threshold exists to prevent, moved to a job
/// nobody measured. The S3 backend reaches the same bound through `PutObject`. Decide for both
/// backends together what happens to a state object that large.
const SINGLE_REQUEST_PARTITION_SIZE: usize = 5000 * 1024 * 1024;

/// Version `*`, which the creation of a job's first iteration is conditioned on: it stands for any
/// version rather than naming one, and is the one condition of this backend that carries no quotes.
const ANY_VERSION: &str = "*";

/// The version of a stored iteration spelled as the entity-tag a condition is compared against.
///
/// A version is held without its quotes - [`normalize_etag`] takes them off whatever the service
/// returned, and that is the form `find_job_meta` reports and `core` compares - while `If-Match`
/// and `If-None-Match` carry an entity-tag, which HTTP and the Blob service's own examples quote.
/// A condition the service does not recognise is refused at best and ignored at worst, and an
/// ignored one is a write that stopped being conditional with nothing anywhere saying so.
fn build_version_condition(version: &str) -> Etag {
    Etag::from(format!("\"{version}\""))
}

/// Connection details of the Azure Blob Storage container a pool keeps job state in, passed to
/// [`JobsManagerBuilder::azure`](crate::JobsManagerBuilder::azure).
pub struct AzureConfig {
    endpoint: String,
    account: String,
    account_key: Secret,
    container_name: String,
    state_prefix: String,
    job_state_codec: JobStateCodecKind,
    request_timeout: Duration,
    retrier_config: RetrierConfig,
    list_page_size: i32,
    is_container_creation_allowed: bool,
}

impl AzureConfig {
    /// Connection details of the container holding job state. Everything else takes a default that
    /// the `with_*` methods override.
    ///
    /// `endpoint` is the blob service address of the account - `https://<account>.blob.core.windows.net`
    /// against the service, or the address with the account in its path against the emulator.
    /// Pass an `https://` endpoint to use TLS - there is no separate switch for it.
    pub fn new(
        endpoint: impl Into<String>,
        account: impl Into<String>,
        account_key: impl Into<String>,
        container_name: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            account: account.into(),
            account_key: Secret::new(account_key.into()),
            container_name: container_name.into(),
            state_prefix: DEFAULT_STATE_PREFIX.to_string(),
            job_state_codec: JobStateCodecKind::Json,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            retrier_config: RetrierConfig::default(),
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
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
    /// Changing this on a container that already holds state leaves the previously written objects
    /// unreadable *and* invisible to cleanup - see [`JobStateCodecKind`].
    #[must_use]
    pub const fn with_job_state_codec(mut self, job_state_codec: JobStateCodecKind) -> Self {
        self.job_state_codec = job_state_codec;
        self
    }

    /// Timeout applied to a single Azure operation, the reading of its body included. Defaults to
    /// 5s.
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

    /// Blobs one cleanup `LIST` asks for. Defaults to 5000, which is the maximum a single
    /// `List Blobs` response carries; lower it only to bound the size of a single response.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] outside `1..=5000`.
    pub fn with_list_page_size(mut self, list_page_size: i32) -> Result<Self, Error> {
        if !(1..=DEFAULT_LIST_PAGE_SIZE).contains(&list_page_size) {
            return Err(Error::Other(format!(
                "azure list page size must be within 1..={DEFAULT_LIST_PAGE_SIZE}"
            )));
        }
        self.list_page_size = list_page_size;
        Ok(self)
    }

    /// Creates the container when it is missing. Defaults to `true`.
    ///
    /// Turn it off where the process has no permission to create one: the backend then only makes
    /// sure the container is reachable, and refuses to start when it is not.
    #[must_use]
    pub const fn with_container_creation(mut self, is_container_creation_allowed: bool) -> Self {
        self.is_container_creation_allowed = is_container_creation_allowed;
        self
    }
}

/// State objects of one Azure Blob Storage container: each job iteration is one block blob.
///
/// Concurrent writers are resolved with conditional writes (`If-Match` on the current `ETag` for
/// updates, `If-None-Match: *` for a job's first iteration) rather than an external lock service.
pub(crate) struct AzureBackend {
    container_client: Arc<BlobContainerClient>,
    /// Content type state objects are written with, which is what the codec of this backend names.
    content_type: &'static str,
    metrics: Arc<dyn MetricsSink>,
    list_page_size: i32,
    request_timeout: Duration,
}

impl AzureBackend {
    /// Connects to `config.endpoint`, makes sure `config.container_name` is reachable - creating it
    /// when it is missing and `config.is_container_creation_allowed` allows it - and hands out the
    /// storage built over the container.
    ///
    /// The check and the creation are retried per `config.retrier_config`; a persistent failure
    /// (including one from a concurrent creator returning a non-409 error, and a missing container
    /// the configuration forbids creating) is returned as [`Error::Other`](crate::Error::Other).
    pub(crate) async fn build(
        config: AzureConfig,
        registry: Arc<dyn JobDefinitionRegistry>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Result<ObjectStorage<Self>, Error> {
        info!("Starting jobmanager with azure storage {}", config.endpoint);

        let container_client = Arc::new(Self::build_client(&config)?);
        let retrier = Retrier::new(config.retrier_config.clone());
        let cancel_token = CancellationToken::new();

        let container_name = config.container_name.clone();
        let is_container_creation_allowed = config.is_container_creation_allowed;
        let request_timeout = config.request_timeout;
        let client_for_retry = Arc::clone(&container_client);
        retrier
            .retry(
                move || {
                    let client = Arc::clone(&client_for_retry);
                    let container_name = container_name.clone();
                    async move {
                        match Self::send_container_request(client.exists(), request_timeout).await {
                            Ok(true) => {
                                info!("Container {} exists", container_name);
                                Ok(RetryStep::Done(()))
                            }
                            Ok(false) => {
                                if !is_container_creation_allowed {
                                    // Nothing to repeat: the container is absent and this pool may
                                    // not create one. Not `NotFound`, whose text names a job - and
                                    // at start-up there is none.
                                    return Err(StorageError::Backend(format!(
                                        "container {container_name} does not exist and container creation is turned off"
                                    )));
                                }
                                match Self::send_container_request(client.create(None), request_timeout).await {
                                    Ok(_) => {
                                        info!("Created container {}", container_name);
                                        Ok(RetryStep::Done(()))
                                    }
                                    // A concurrent creator got there first, which is the outcome
                                    // this pool wanted anyway.
                                    Err(refusal) if refusal.is_conflict() => {
                                        info!("Container {} already exists", container_name);
                                        Ok(RetryStep::Done(()))
                                    }
                                    Err(refusal) => refusal.into_retry_step(),
                                }
                            }
                            Err(refusal) => refusal.into_retry_step(),
                        }
                    }
                },
                &cancel_token,
            )
            .await
            .map_err(|e| Error::Other(format!("Failed to init container: {e}")))?;

        let codec = config.job_state_codec.build();
        let state_object_keys = JobPaths::new(config.state_prefix, codec.as_ref());

        Ok(ObjectStorage::new(
            Self {
                container_client,
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

    /// What every request of this backend is sent and recorded under.
    fn build_request_options<'a>(&'a self, operation: &'a str) -> RequestOptions<'a> {
        RequestOptions {
            operation,
            metrics: self.metrics.as_ref(),
            request_timeout: self.request_timeout,
        }
    }

    /// Options of a read that is answered in one request, conditioned on `if_match` or
    /// `if_none_match`.
    fn build_download_options(
        if_match: Option<Etag>,
        if_none_match: Option<Etag>,
    ) -> BlobClientDownloadOptions<'static> {
        BlobClientDownloadOptions {
            if_match,
            if_none_match,
            partition_size: NonZero::new(SINGLE_REQUEST_PARTITION_SIZE),
            ..Default::default()
        }
    }

    /// Options of a write that is answered in one request, conditioned on `condition`.
    fn build_upload_options(&self, condition: &PutCondition<'_>) -> BlobClientUploadOptions<'static> {
        let (if_match, if_none_match) = match condition {
            PutCondition::CreateOnly => (None, Some(Etag::from(ANY_VERSION))),
            PutCondition::MatchVersion(version) => (Some(build_version_condition(version)), None),
        };

        BlobClientUploadOptions {
            if_match,
            if_none_match,
            blob_content_type: Some(self.content_type.to_string()),
            // Widening: the threshold is declared in the narrower of the two units the SDK asks it
            // in, and a target that could not hold it there does not build.
            partition_size: NonZero::new(SINGLE_REQUEST_PARTITION_SIZE as u64),
            ..Default::default()
        }
    }

    /// One read of a state object: the request and the body it answers with.
    ///
    /// Both live in one call so that the timeout and the recorded request cover the whole of what a
    /// read costs - a body that never arrives holds the worker exactly as a request that never answers
    /// does.
    async fn download_object(
        blob_client: &BlobClient,
        options: BlobClientDownloadOptions<'static>,
    ) -> azure_core::Result<(Option<Etag>, Bytes)> {
        let download_result = blob_client.download(Some(options)).await?;
        let etag = download_result.properties.etag.clone();

        Ok((etag, download_result.body.collect().await?))
    }

    /// The same read where the version the answer stands at is part of what was asked for: the
    /// state, and the entity-tag the next condition is held against.
    ///
    /// The entity-tag is taken inside the request for the reason
    /// [`AzureBackend::read_listed_page`] carries: an answer naming none is a request that failed,
    /// and recording it as the `OK` its status line claimed would put one request under two pairs.
    async fn download_versioned_object(
        blob_client: &BlobClient,
        key: &str,
        options: BlobClientDownloadOptions<'static>,
    ) -> azure_core::Result<(String, Bytes)> {
        let (etag, state_bytes) = Self::download_object(blob_client, options).await?;

        let version = etag.map(|etag| normalize_etag(etag.as_ref())).ok_or_else(|| {
            azure_core::Error::with_message(ErrorKind::DataConversion, format!("Missing etag reading {key}"))
        })?;

        Ok((version, state_bytes))
    }

    /// The same read in the shape a caller that accepts "not modified" takes: `Some` is the state
    /// the blob answered with, and the `None` an unchanged blob amounts to is supplied by
    /// [`ExpectedAnswer::NotModified`] instead - the refusal never reaches here.
    async fn download_changed_object(
        blob_client: &BlobClient,
        key: &str,
        options: BlobClientDownloadOptions<'static>,
    ) -> azure_core::Result<Option<(String, Vec<u8>)>> {
        let (version, state_bytes) = Self::download_versioned_object(blob_client, key, options).await?;

        Ok(Some((version, Vec::from(state_bytes))))
    }

    /// One delete of a state object. A blob that is already gone is refused `404`, which
    /// [`ExpectedAnswer::ObjectGone`] settles as the success cleanup asked for.
    async fn delete_object(blob_client: &BlobClient) -> azure_core::Result<()> {
        blob_client.delete(Some(BlobClientDeleteOptions::default())).await?;

        Ok(())
    }

    /// One write whose answer has to name the version the object now stands at: the request, and
    /// the entity-tag the next condition is held against.
    ///
    /// The entity-tag is taken inside the request for the reason
    /// [`AzureBackend::download_versioned_object`] carries.
    async fn upload_versioned_object(
        blob_client: &BlobClient,
        key: &str,
        body: Vec<u8>,
        options: BlobClientUploadOptions<'static>,
    ) -> azure_core::Result<String> {
        let upload_result = blob_client.upload(RequestContent::from(body), Some(options)).await?;

        upload_result.etag.map(|etag| normalize_etag(etag.as_ref())).ok_or_else(|| {
            azure_core::Error::with_message(ErrorKind::DataConversion, format!("Missing etag writing {key}"))
        })
    }

    /// Runs one request of the container check under `request_timeout`.
    ///
    /// Separate from [`send_request`] because the check runs before there is an
    /// `AzureBackend` to run it through, and without a timeout of its own a connection that is
    /// accepted and never answered holds [`AzureBackend::build`] - and with it the whole pool's
    /// start-up - for as long as it lives. Nothing is recorded here: the S3 backend does not bill
    /// its bucket check either, and a request counted here would land in every quota that builds a
    /// backend.
    async fn send_container_request<T>(
        request: impl Future<Output = azure_core::Result<T>> + Send,
        request_timeout: Duration,
    ) -> StorageResult<T> {
        match tokio::time::timeout(request_timeout, request).await {
            Ok(Ok(value)) => Ok(value),
            Ok(Err(error)) => Err(error.into_storage_error()),
            Err(_) => Err(StorageError::Timeout),
        }
    }

    /// Builds the container client a pool reaches the service through: signed with the account key,
    /// and with the SDK's own retries turned off.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] when the endpoint and container do not form a URL, and when the account
    /// key is not usable for signing.
    fn build_client(config: &AzureConfig) -> Result<BlobContainerClient, Error> {
        let container_url = Url::parse(&format!(
            "{}/{}",
            config.endpoint.trim_end_matches('/'),
            config.container_name
        ))
        .map_err(|e| Error::Other(format!("azure endpoint and container do not form a url: {e}")))?;

        // TODO(med): add creds options. `AzureConfig` carries an account key and nothing else,
        // so the credential slot here is handed `None`, and a consumer whose policy forbids account
        // keys cannot reach the store - although that slot takes the `TokenCredential` the SDK has.
        BlobContainerClient::new(container_url, None, Some(Self::build_client_options(config)?))
            .map_err(|e| Error::Other(format!("Failed to init azure container client: {e}")))
    }

    /// Options the container client of a pool is built with.
    ///
    /// Kept out of [`AzureBackend::build_client`] because a built client hands none of them back, and one
    /// of them - the SDK's own retry policy - is what decides whether a call of
    /// [`Storage`](crate::Storage) costs the one request its quota names.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Other`] when the account key is not usable for signing.
    fn build_client_options(config: &AzureConfig) -> Result<BlobContainerClientOptions, Error> {
        let signing_policy = SharedKeySigningPolicy::try_new(&config.account, &config.account_key)
            .map_err(|e| Error::Other(format!("Failed to init azure signing: {e}")))?;

        Ok(BlobContainerClientOptions {
            client_options: ClientOptions {
                // The signature covers a timestamp the service expires, so it is taken per attempt -
                // which is what a per-try policy is.
                per_try_policies: vec![Arc::new(signing_policy) as Arc<dyn Policy>],
                // Retries belong to `Retrier` - see the invariant on what a call of `Storage` may
                // cost in `AGENTS.md`.
                retry: RetryOptions::none(),
                ..Default::default()
            },
            ..Default::default()
        })
    }

    /// One page of a listing: the request and the parsing of the body it answers with.
    ///
    /// Both live in one call for the same reason [`AzureBackend::download_object`] does,
    /// plus one of its own: a listing whose body does not parse is a request that failed, and
    /// recording it as the `OK` the status line claimed would make that pair mean something
    /// different here than it does in the S3 backend.
    async fn read_listed_page(
        pages: &mut PageIterator<Response<ListBlobsResponse, XmlFormat>>,
    ) -> azure_core::Result<Option<ListBlobsResponse>> {
        let Some(page) = pages.try_next().await? else {
            return Ok(None);
        };

        page.into_model().map(Some)
    }
}

#[async_trait::async_trait]
impl Backend for AzureBackend {
    /// The boundary of this provider is `startFrom`, which the `List Blobs` operation answers with
    /// the key it is given - documented from REST version 2023-05-03, and the version
    /// `azure_storage_blob` sends is well past it. What the emulator this crate is tested against
    /// does with the parameter is pinned by `list_boundary_azure_test`.
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
        let blob_client = self.container_client.blob_client(key);
        // Built before the request rather than into its arguments: the options carry a field per
        // header the operation has, and inlining them leaves both the temporary and the moved copy
        // in the future this awaits.
        let options = self.build_upload_options(&condition);

        send_request(
            Self::upload_versioned_object(&blob_client, key, body, options),
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
        let blob_client = self.container_client.blob_client(key);
        let options = Self::build_download_options(Some(build_version_condition(expected_version)), None);

        let (_, state_bytes) = send_request(
            Self::download_object(&blob_client, options),
            self.build_request_options("GET"),
            ExpectedAnswer::Success,
            cancel_token,
        )
        .await?;

        Ok(Vec::from(state_bytes))
    }

    async fn get_changed_object(
        &self,
        key: &str,
        known_version: &str,
        cancel_token: &CancellationToken,
    ) -> StorageResult<Option<(String, Vec<u8>)>> {
        let blob_client = self.container_client.blob_client(key);
        let options = Self::build_download_options(None, Some(build_version_condition(known_version)));

        send_request(
            Self::download_changed_object(&blob_client, key, options),
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
        let options = BlobContainerClientListBlobsOptions {
            prefix: Some(request.prefix.to_string()),
            start_from: request.start_key.map(str::to_string),
            maxresults: Some(request.page_size),
            marker: request.cursor,
            ..Default::default()
        };
        // Nothing is sent yet, so nothing is recorded: this is the SDK refusing to build the
        // request, not the store refusing to answer it.
        let mut pages = self
            .container_client
            .list_blobs(Some(options))
            .map_err(ProviderError::into_storage_error)?
            .into_pages();

        let listed_page = send_request(
            Self::read_listed_page(&mut pages),
            self.build_request_options("LIST"),
            ExpectedAnswer::Success,
            cancel_token,
        )
        .await?;

        let Some(listed_page) = listed_page else {
            return Ok(ListedPage {
                objects: Vec::new(),
                next_cursor: None,
            });
        };

        // The pager answers the page after the last one without sending anything, so asking it for
        // one would record a request nobody made under the very pair the cleanup quota is counted
        // on. The marker the page carries is what says whether another request is owed.
        let next_cursor = listed_page.next_marker.filter(|marker| !marker.is_empty());
        let objects = listed_page
            .blob_items
            .into_iter()
            .filter_map(|blob| {
                let key = blob.name?;
                let version = blob
                    .properties
                    .and_then(|properties| properties.etag)
                    .map(|etag| normalize_etag(etag.as_ref()));
                Some(ListedObject { key, version })
            })
            .collect();

        Ok(ListedPage { objects, next_cursor })
    }

    // TODO(high): delete a tail in one batch request rather than one request per iteration - what
    // is missing is a client in `azure_storage_blob`, see `AzureConfig`. Doing it here
    // means a `multipart/mixed` body of up to 256 sub-requests, each signed on its own, and a new
    // number for `trimming_a_tail_of_two_iterations_costs_four_requests_on_azure`.
    async fn delete_objects(&self, keys: &[String], cancel_token: &CancellationToken) -> StorageResult<()> {
        for key in keys {
            let blob_client = self.container_client.blob_client(key);
            send_request(
                Self::delete_object(&blob_client),
                self.build_request_options("DELETE"),
                ExpectedAnswer::ObjectGone(()),
                cancel_token,
            )
            .await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    use azure_core::http::{AsyncRawResponse, HttpClient, Request, StatusCode, Transport, headers::Headers};
    use parking_lot::Mutex;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::tests::common::UnusedJobRegistry;
    use crate::tests::common::counting_metrics::CountingMetrics;
    use crate::tests::common::scripted_endpoint::{ScriptedEndpoint, build_http_response};
    use crate::tests::common::silent_endpoint::{cancel_after_delay, start_silent_endpoint as start_silent_listener};
    use crate::{Job, JobCode, JobMeta, NoopMetrics, Storage};

    /// Account key every case signs with: base64, because signing rejects anything else.
    const TEST_ACCOUNT_KEY: &str = "a2V5";

    /// Timeout the cases against the silent endpoint give a request. Long enough that no scheduling
    /// hiccup reaches it, short enough that a case which really waits it out still ends quickly.
    const SILENT_ENDPOINT_TIMEOUT: Duration = Duration::from_secs(1);

    /// Bytes one `Put Blob` accepts, stated here rather than read off the constant under test.
    const MAX_SINGLE_REQUEST_BODY: usize = 5_242_880_000;

    /// Retention boundary the cleanup listing case asks about: the newest iteration cleanup may
    /// delete.
    const TESTED_RETENTION_BOUNDARY: u64 = 2;

    /// Key of the iteration equal to [`TESTED_RETENTION_BOUNDARY`], as it travels in a query
    /// parameter. Stated rather than built so the expectation does not come from the key builder
    /// under test; the escaping is what a query pair is written with.
    const BOUNDARY_ITERATION_TARGET_KEY: &str = "jobs%2Fjob%2Fstate-18446744073709551613.json";

    fn build_config() -> AzureConfig {
        AzureConfig::new(
            "http://localhost:10000/devstoreaccount1",
            "devstoreaccount1",
            TEST_ACCOUNT_KEY,
            "jobs",
        )
    }

    /// The shared silent listener, addressed as this provider's account url.
    async fn start_silent_endpoint() -> (String, JoinHandle<()>) {
        let (endpoint, accepting) = start_silent_listener().await;

        (format!("{endpoint}/devstoreaccount1"), accepting)
    }

    /// Builds a store pointed at `endpoint` field by field.
    ///
    /// Constructed this way rather than through [`AzureBackend::build`], which checks
    /// the container before handing one out and therefore cannot be reached against an endpoint
    /// that answers nothing; adding a way past that check to the production path would be a second
    /// way to configure the same thing.
    fn build_store_against(endpoint: &str, metrics: Arc<dyn MetricsSink>) -> AzureBackend {
        let config = AzureConfig::new(endpoint, "devstoreaccount1", TEST_ACCOUNT_KEY, "jobs");

        AzureBackend {
            container_client: Arc::new(AzureBackend::build_client(&config).expect("the test endpoint must form a url")),
            content_type: JobStateCodecKind::Json.build().content_type(),
            metrics,
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            request_timeout: SILENT_ENDPOINT_TIMEOUT,
        }
    }

    /// The storage a case reaching `Storage` goes through, over `store`.
    fn build_storage_over_store(store: AzureBackend) -> ObjectStorage<AzureBackend> {
        let codec = JobStateCodecKind::Json.build();

        ObjectStorage::new(
            store,
            JobPaths::new(DEFAULT_STATE_PREFIX.to_string(), codec.as_ref()),
            codec,
            Arc::new(UnusedJobRegistry),
            Retrier::new(RetrierConfig::default()),
        )
    }

    /// The iteration the cancellation cases ask about.
    fn build_read_meta() -> JobMeta {
        JobMeta {
            code: JobCode::new("job"),
            iter_num: 1,
            version: "an-etag".to_string(),
        }
    }

    #[test]
    fn a_list_page_size_below_one_is_rejected() {
        assert!(build_config().with_list_page_size(0).is_err());
    }

    #[test]
    fn a_list_page_size_above_the_protocol_maximum_is_rejected() {
        assert!(build_config().with_list_page_size(5001).is_err());
    }

    #[test]
    fn a_list_page_size_at_the_accepted_bounds_is_applied() {
        let lowest = build_config().with_list_page_size(1).expect("1 is inside the accepted range");
        let highest = build_config().with_list_page_size(5000).expect("5000 is the protocol maximum");

        assert_eq!(lowest.list_page_size, 1);
        assert_eq!(highest.list_page_size, 5000);
    }

    /// The container check runs before the pool has anything that could cancel it, so the timeout
    /// is the only bound it has that answers in time. Without one an endpoint that accepts a
    /// connection and answers nothing holds the whole start-up for the transport's read bound - a
    /// minute of silence, once per attempt - with no error until it passes, no metric and no log.
    /// And a store that did not answer is a store that may answer on the next attempt, so the check
    /// is repeated rather than given up on.
    ///
    /// Both bounds are asserted: the lower one says the second attempt really ran, the upper one
    /// that the start-up ended rather than waiting the transport out. The bound is time rather than
    /// the error text: which attempt gave up and what it was called is not the behaviour at risk.
    ///
    /// Checked by breaking it twice. Letting the refusal out of the retry closure on `?` instead of
    /// through `into_retry_step` ends the start-up after one attempt and the lower bound fails.
    /// Taking `send_container_request` back off `client.exists()` leaves this waiting on the
    /// transport's minute per attempt, and the upper bound never comes.
    #[tokio::test]
    async fn a_container_check_a_silent_endpoint_never_answers_is_repeated_and_then_given_up() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let config = AzureConfig::new(endpoint, "devstoreaccount1", TEST_ACCOUNT_KEY, "jobs")
            .with_request_timeout(SILENT_ENDPOINT_TIMEOUT)
            .with_retrier_config(RetrierConfig {
                max_attempts: 2,
                rand_delay: Duration::from_millis(1),
                delays: vec![Duration::from_millis(1)],
            });

        let started = Instant::now();
        let storage = AzureBackend::build(config, Arc::new(UnusedJobRegistry), Arc::new(NoopMetrics)).await;
        let elapsed = started.elapsed();

        accepting.abort();
        assert!(
            storage.is_err(),
            "an endpoint that never answers must not read as a reachable container"
        );
        assert!(
            elapsed >= SILENT_ENDPOINT_TIMEOUT * 2,
            "a check that timed out must be attempted again, took {elapsed:?}"
        );
        assert!(
            elapsed < SILENT_ENDPOINT_TIMEOUT * 4,
            "and the attempts must be given up once each rather than waited on, took {elapsed:?}"
        );
    }

    /// A pool that found no container and then lost the creation of it to somebody else still
    /// starts: the container it needs is there, which is the whole of what the check asked for.
    /// This is the arm every deployment of more than one process hits on its first start, and no
    /// case against a real emulator can reach it deterministically - two builds racing may each see
    /// the container absent, or the second may see it present.
    ///
    /// The `404` names `ContainerNotFound` because `BlobContainerClient::exists` reads a `404`
    /// carrying any other error code as a failure rather than as an absent container.
    ///
    /// Checked by breaking it: answering the lost race with `refusal.into_retry_step()` instead of
    /// `RetryStep::Done` spends the whole attempt budget on a `409` no repetition clears, and the
    /// build ends as an error.
    #[tokio::test]
    async fn a_pool_that_lost_the_creation_of_its_container_still_starts() {
        let endpoint = ScriptedEndpoint::start(vec![
            build_http_response(404, &[("x-ms-error-code", "ContainerNotFound")], ""),
            build_http_response(409, &[("x-ms-error-code", "ContainerAlreadyExists")], ""),
        ])
        .await;
        let config = AzureConfig::new(
            format!("{}/devstoreaccount1", endpoint.endpoint()),
            "devstoreaccount1",
            TEST_ACCOUNT_KEY,
            "jobs",
        )
        .with_request_timeout(SILENT_ENDPOINT_TIMEOUT);

        let storage = AzureBackend::build(config, Arc::new(UnusedJobRegistry), Arc::new(NoopMetrics)).await;

        storage.expect("a container somebody else created first is the outcome this pool wanted");
        let requested_targets = endpoint.requested_targets();
        assert_eq!(
            requested_targets.len(),
            2,
            "the check must have been followed by the creation that lost, got: {requested_targets:?}"
        );
    }

    /// The boundary the cleanup listing was calculated for has to reach the service in the parameter
    /// this provider reads it from. On this provider the parameter is the only place the boundary is
    /// observable at all: Azurite drops `startFrom` and answers the whole prefix either way, which
    /// `list_boundary_azure_test` pins deliberately.
    ///
    /// The store refuses the listing on purpose - what is under test is the address the request
    /// carried, and the target is recorded whatever the answer was.
    ///
    /// Checked by breaking it: dropping `start_from` from the options `AzureBackend::list_objects`
    /// builds leaves the parameter off the target and the assertion fails.
    #[tokio::test]
    async fn a_cleanup_listing_asks_the_service_to_start_at_the_boundary_key() {
        let endpoint = ScriptedEndpoint::start(vec![build_http_response(500, &[], "")]).await;
        let storage = build_storage_over_store(build_store_against(
            &format!("{}/devstoreaccount1", endpoint.endpoint()),
            Arc::new(NoopMetrics),
        ));

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
            requested_targets[0].contains(&format!("startFrom={BOUNDARY_ITERATION_TARGET_KEY}")),
            "this provider answers with the boundary key, so the listing starts at the boundary itself, got: {requested_targets:?}"
        );
    }

    /// A worker standing on a read must let go when its pool is asked to stop, rather than hold on
    /// until the request times out. Five of the seven `Storage` methods reach the service without a
    /// `Retrier` around them, so the token has to be observed inside the request itself.
    ///
    /// Checked by breaking it: dropping the `tokio::select!` from `send_request` leaves
    /// this waiting the whole of `SILENT_ENDPOINT_TIMEOUT` and the elapsed assertion fails.
    #[tokio::test]
    async fn a_cancelled_token_drops_a_conditional_read_in_flight() {
        let (endpoint, accepting) = start_silent_endpoint().await;
        let storage = build_storage_over_store(build_store_against(&endpoint, Arc::new(NoopMetrics)));
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
            elapsed < SILENT_ENDPOINT_TIMEOUT,
            "the read must end on the token rather than on the timeout, took {elapsed:?}"
        );
    }

    /// The same for cleanup, where a tail of any length would otherwise hold a stopping pool for
    /// one whole timeout - the one its first delete is standing in.
    ///
    /// The count is what says the tail was abandoned rather than worked through: a cancellation
    /// that the loop swallowed would leave the second iteration asked for all the same. It is
    /// counted under the pair the request was recorded with, so a cancelled request cannot pass for
    /// a store that refused one.
    ///
    /// Checked by breaking it: dropping the `tokio::select!` from `send_request` leaves
    /// the first delete to run its timeout out, and the cleanup ends as a timeout instead of a
    /// cancellation.
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
            "a request the store never received must be recorded under no pair at all"
        );
    }

    /// Transport that answers every request with `status` and keeps the headers of the last one it
    /// was handed.
    ///
    /// The spelling of a condition is observable nowhere else: the generated client puts the `Etag`
    /// it is given into the header unchanged, and the header is what the service reads - so what
    /// the transport received is the statement under test, not what the options held.
    #[derive(Debug)]
    struct HeaderCapturingTransport {
        status: u16,
        sent_headers: Mutex<Vec<(String, String)>>,
    }

    impl HeaderCapturingTransport {
        fn answering(status: u16) -> Self {
            Self {
                status,
                sent_headers: Mutex::new(Vec::new()),
            }
        }

        /// The value `name` was sent with, or `None` when the request carried no such header.
        fn read_sent_header(&self, name: &str) -> Option<String> {
            self.sent_headers
                .lock()
                .iter()
                .find(|(sent_name, _)| sent_name == name)
                .map(|(_, value)| value.clone())
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for HeaderCapturingTransport {
        async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
            *self.sent_headers.lock() = request
                .headers()
                .iter()
                .map(|(name, value)| (name.as_str().to_string(), value.as_str().to_string()))
                .collect();

            Ok(AsyncRawResponse::from_bytes(
                StatusCode::from(self.status),
                Headers::new(),
                "scripted",
            ))
        }
    }

    /// Builds a store whose requests `http_client` answers, every other setting being the
    /// production one - the signing policy included, since the conditional headers are part of what
    /// a request is signed over.
    fn build_store_over(http_client: Arc<dyn HttpClient>, metrics: Arc<dyn MetricsSink>) -> AzureBackend {
        AzureBackend {
            container_client: Arc::new(build_client_over(http_client)),
            content_type: JobStateCodecKind::Json.build().content_type(),
            metrics,
            list_page_size: DEFAULT_LIST_PAGE_SIZE,
            request_timeout: SILENT_ENDPOINT_TIMEOUT,
        }
    }

    /// The storage a case reaching `Storage` goes through, over that store.
    fn build_storage_over(http_client: Arc<dyn HttpClient>) -> ObjectStorage<AzureBackend> {
        build_storage_over_store(build_store_over(http_client, Arc::new(NoopMetrics)))
    }

    /// The job a save case writes, standing at `version`: empty for the creation of a job's first
    /// iteration, a stored version for an update of one.
    fn build_job_at_version(version: &str) -> Job {
        Job::restore(
            uuid::Uuid::from_u128(9_001),
            JobCode::new("job"),
            version.to_string(),
            1,
            crate::JobStatus::Started,
            Vec::new(),
            uuid::Uuid::from_u128(9_002),
            chrono::Utc::now(),
            None,
            None,
            None,
            std::collections::HashMap::new(),
            None,
            None,
            crate::TaskLimits::default(),
        )
    }

    /// A read of a named iteration states its condition the way [`build_version_condition`]
    /// documents, and the header the transport received is the only place that is observable.
    ///
    /// Checked by breaking it: handing `Etag::from(job_meta.version.clone())` to the read again
    /// sends the version bare and the assertion on the header fails. The two cases below are the
    /// same break on the other two conditions.
    #[tokio::test]
    async fn a_read_of_a_named_iteration_sends_the_stored_version_quoted() {
        let transport = Arc::new(HeaderCapturingTransport::answering(412));
        let storage = build_storage_over(Arc::clone(&transport) as Arc<dyn HttpClient>);

        let read_result = storage.get_job_by_meta(&build_read_meta(), &CancellationToken::new()).await;

        assert!(
            read_result.is_err(),
            "a store refusing the condition must not read as a job"
        );
        assert_eq!(
            transport.read_sent_header("if-match").as_deref(),
            Some("\"an-etag\""),
            "a read must name the stored version as a quoted entity-tag"
        );
    }

    /// The same for the conditional read a poll costs, whose condition is the version the worker
    /// already holds.
    #[tokio::test]
    async fn a_read_of_a_changed_iteration_sends_the_held_version_quoted() {
        let transport = Arc::new(HeaderCapturingTransport::answering(412));
        let storage = build_storage_over(Arc::clone(&transport) as Arc<dyn HttpClient>);

        let read_result = storage.get_changed_job(&build_read_meta(), &CancellationToken::new()).await;

        assert!(
            read_result.is_err(),
            "a store refusing the condition must not read as a job"
        );
        assert_eq!(
            transport.read_sent_header("if-none-match").as_deref(),
            Some("\"an-etag\""),
            "a conditional read must name the held version as a quoted entity-tag"
        );
    }

    /// And for the write every save of a stored iteration is conditioned on - the one place where
    /// an unrecognised condition costs a concurrent update rather than a refusal.
    #[tokio::test]
    async fn a_save_of_a_stored_iteration_sends_its_version_quoted() {
        let transport = Arc::new(HeaderCapturingTransport::answering(412));
        let storage = build_storage_over(Arc::clone(&transport) as Arc<dyn HttpClient>);

        let saved = storage
            .save_job(&mut build_job_at_version("an-etag"), &CancellationToken::new())
            .await;

        assert!(saved.is_err(), "a store refusing the condition must not read as a save");
        assert_eq!(
            transport.read_sent_header("if-match").as_deref(),
            Some("\"an-etag\""),
            "a save must name the version it replaces as a quoted entity-tag"
        );
    }

    /// The creation of a job's first iteration is the one condition naming no version: `*` stands
    /// for any entity-tag and is spelled bare, so quoting it would refuse every creation.
    ///
    /// Checked by breaking it: passing `ANY_VERSION` through `build_version_condition` sends `"*"`
    /// and the assertion fails.
    #[tokio::test]
    async fn a_creation_of_an_iteration_sends_its_condition_bare() {
        let transport = Arc::new(HeaderCapturingTransport::answering(409));
        let storage = build_storage_over(Arc::clone(&transport) as Arc<dyn HttpClient>);

        let saved = storage.save_job(&mut build_job_at_version(""), &CancellationToken::new()).await;

        assert!(saved.is_err(), "a store refusing the creation must not read as a save");
        assert_eq!(
            transport.read_sent_header("if-none-match").as_deref(),
            Some("*"),
            "the creation of an iteration must not quote its condition"
        );
    }

    /// A `200` that hands the state over and names no entity-tag leaves the worker unable to
    /// condition its next save on what it just read, so the read is refused - and billed as the
    /// request that failed rather than as the `OK` its status line claimed, because a quota is
    /// counted on that pair.
    ///
    /// Checked by breaking it: reading the entity-tag after `send_request` instead of
    /// inside the request, as this backend did before, records the `GET` under `OK` and leaves
    /// `ERR` at zero.
    #[tokio::test]
    async fn a_changed_iteration_answered_without_an_etag_is_refused() {
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_over(
            Arc::new(HeaderCapturingTransport::answering(200)) as Arc<dyn HttpClient>,
            Arc::clone(&metrics) as Arc<dyn MetricsSink>,
        );

        let read_result = store
            .get_changed_object("jobs/job/state-1.json", "an-etag", &CancellationToken::new())
            .await;

        let Err(error) = read_result else {
            panic!("a state handed over without a version must not read as one")
        };
        assert!(matches!(error, StorageError::Backend(_)), "got: {error:?}");
        assert_eq!(metrics.storage_operations("GET", "ERR"), 1);
        assert_eq!(metrics.storage_operations_total(), 1);
    }

    /// A store that accepted the write and named no entity-tag leaves the worker with nothing to
    /// condition its next save on, so the save is refused - and billed as the request that failed
    /// rather than as the `OK` its status line claimed, because a quota is counted on that pair.
    ///
    /// Checked by breaking it: reading the entity-tag after `send_request` instead of
    /// inside the request, as this backend did before, records the `PUT` under `OK` and leaves
    /// `ERR` at zero.
    #[tokio::test]
    async fn a_write_answered_without_an_etag_is_refused() {
        let metrics = Arc::new(CountingMetrics::default());
        let store = build_store_over(
            Arc::new(HeaderCapturingTransport::answering(201)) as Arc<dyn HttpClient>,
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

    /// Transport that answers every request with `status` and counts how many it was handed.
    ///
    /// The retry policy of a built client cannot be read back, so what the client does with a
    /// status it would retry is the only way to state the setting as a number.
    #[derive(Debug)]
    struct CountingTransport {
        status: u16,
        requests: AtomicUsize,
    }

    impl CountingTransport {
        const fn answering(status: u16) -> Self {
            Self {
                status,
                requests: AtomicUsize::new(0),
            }
        }

        fn requests(&self) -> usize {
            self.requests.load(Ordering::SeqCst)
        }
    }

    #[async_trait::async_trait]
    impl HttpClient for CountingTransport {
        async fn execute_request(&self, _request: &Request) -> azure_core::Result<AsyncRawResponse> {
            self.requests.fetch_add(1, Ordering::SeqCst);

            Ok(AsyncRawResponse::from_bytes(
                StatusCode::from(self.status),
                Headers::new(),
                "scripted",
            ))
        }
    }

    /// Builds the client the pool uses, with its transport replaced: every other setting is the
    /// production one, which is what makes the retry policy the thing under test.
    fn build_client_over(http_client: Arc<dyn HttpClient>) -> BlobContainerClient {
        let config = build_config();
        let mut options = AzureBackend::build_client_options(&config).expect("the test key must be usable for signing");
        options.client_options.transport = Some(Transport::new(http_client));

        BlobContainerClient::new(
            Url::parse("http://localhost:10000/devstoreaccount1/jobs").expect("the test endpoint must form a url"),
            None,
            Some(options),
        )
        .expect("the container client must build")
    }

    /// A quota states what one call of `Storage` costs, and an SDK that repeats a request on its own
    /// makes that statement false without failing anything: the repetition is neither counted nor
    /// bounded by `Retrier`, which owns the retry policy of this crate. `503` is the case, because
    /// it is a status the SDK's default policy does repeat.
    ///
    /// Checked by breaking it: replacing `RetryOptions::none()` with `RetryOptions::default()` in
    /// `build_client_options` turns the one request into several.
    #[tokio::test]
    async fn the_production_client_does_not_retry_inside_the_sdk() {
        let transport = Arc::new(CountingTransport::answering(503));
        let client = build_client_over(Arc::clone(&transport) as Arc<dyn HttpClient>);

        let read_result = client.blob_client("jobs/job/state.json").download(None).await;

        assert!(read_result.is_err(), "a store answering 503 must not read as a blob");
        assert_eq!(
            transport.requests(),
            1,
            "the production client must attempt a request once"
        );
    }

    /// Above its partition size the SDK splits a transfer on its own, and every quota in the crate
    /// is a number only while it does not: the threshold has to be named in both directions rather
    /// than left at whatever the SDK defaults to.
    ///
    /// Checked by breaking it: taking `partition_size` out of either builder leaves the field
    /// `None`, which is the default this asserts against.
    ///
    /// The two sides are asserted in the units the SDK asks them in, each against the number
    /// literally rather than against the conversion the backend makes: a threshold the target
    /// silently narrowed would otherwise be asserted against itself.
    #[test]
    fn a_transfer_is_asked_to_stay_within_one_request_in_both_directions() {
        let store = build_store_against("http://localhost:10000/devstoreaccount1", Arc::new(NoopMetrics));

        assert_eq!(
            AzureBackend::build_download_options(None, None).partition_size,
            NonZero::new(MAX_SINGLE_REQUEST_BODY),
            "a read must be asked for in one request"
        );
        assert_eq!(
            store.build_upload_options(&PutCondition::CreateOnly).partition_size,
            NonZero::new(5_242_880_000_u64),
            "and a write must be written in one"
        );
    }
}
