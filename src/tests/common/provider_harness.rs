//! One provider, started for a test: the backend under test, and a probe that reads the same
//! container without going through it.
//!
//! The trait exists because every integration test that reaches a real store is owed by every
//! provider, and a second copy of one per provider is what drifts apart.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{JobDefinitionRegistry, JobStateCodecKind, JobsManagerBuilder, MetricsSink, Storage};

/// Container or bucket every harness keeps its state objects in. One per provider is enough:
/// harnesses own their container, so two tests never share a store.
const PERIMETER_CONTAINER_NAME: &str = "provider-perimeter";

/// What a test asks a provider for.
///
/// A struct rather than a parameter list because the backend a case needs is described by several
/// independent choices, and only one case makes each of them.
pub struct ProviderStorageRequest {
    state_prefix: String,
    codec: JobStateCodecKind,
    list_page_size: Option<i32>,
    request_timeout: Option<Duration>,
    registry: Arc<dyn JobDefinitionRegistry>,
    metrics: Arc<dyn MetricsSink>,
}

impl ProviderStorageRequest {
    /// State objects under `state_prefix`, encoded by `codec`, with the provider's own default
    /// listing page size.
    pub fn new(
        state_prefix: impl Into<String>,
        codec: JobStateCodecKind,
        registry: Arc<dyn JobDefinitionRegistry>,
        metrics: Arc<dyn MetricsSink>,
    ) -> Self {
        Self {
            state_prefix: state_prefix.into(),
            codec,
            list_page_size: None,
            request_timeout: None,
            registry,
            metrics,
        }
    }

    /// Asks for a listing page small enough that a case can make cleanup page at all.
    #[must_use]
    pub const fn with_list_page_size(mut self, list_page_size: i32) -> Self {
        self.list_page_size = Some(list_page_size);
        self
    }

    /// Asks for a request timeout shorter than the provider's default, which is what a case about
    /// deadlines needs so a request cannot outlive the deadline it is racing.
    #[must_use]
    pub const fn with_request_timeout(mut self, request_timeout: Duration) -> Self {
        self.request_timeout = Some(request_timeout);
        self
    }
}

#[async_trait]
pub trait ProviderHarness: Send + Sync {
    /// Name of the provider, which a parameterized test puts in its diagnostics.
    fn provider_name(&self) -> &'static str;

    /// The backend under test, connected to this harness's container.
    async fn build_storage(
        &self,
        request: &ProviderStorageRequest,
    ) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>>;

    /// Points `builder` at this harness's container under `state_prefix`.
    ///
    /// The way in for a case about the public builder itself, which constructs the backend from a
    /// provider's own configuration rather than taking a [`Storage`] - naming one provider there
    /// would tie the case to it.
    fn attach_storage(&self, builder: JobsManagerBuilder, state_prefix: &str) -> JobsManagerBuilder;

    /// Keys of the state objects under `state_prefix`, in ascending order, read by a client of the
    /// test itself rather than by the backend under test.
    async fn read_state_keys(&self, state_prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>>;
}

#[cfg(feature = "storage-s3")]
mod s3 {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::{PERIMETER_CONTAINER_NAME, ProviderHarness, ProviderStorageRequest};
    use crate::tests::common::s3_container::S3TestContainer;
    use crate::{JobStateCodecKind, JobsManagerBuilder, S3Backend, S3Config, Storage};

    /// Region the test store is addressed with; an S3-compatible store ignores it, but the SDK
    /// requires one.
    const PERIMETER_REGION: &str = "us-east-1";

    /// An S3-compatible store started for a test, torn down with the harness.
    pub struct S3ProviderHarness {
        container: S3TestContainer,
        probe: aws_sdk_s3::Client,
    }

    impl S3ProviderHarness {
        /// Starts the store the perimeter runs against and builds the probe that reads it.
        ///
        /// # Errors
        ///
        /// Returns an error when the container does not start or become reachable.
        pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
            let container = S3TestContainer::start().await?;
            let probe = build_probe(&container).await;

            Ok(Self { container, probe })
        }
    }

    /// S3 client belonging to the test, so what the backend stored is read without going through
    /// it.
    async fn build_probe(container: &S3TestContainer) -> aws_sdk_s3::Client {
        let credentials =
            aws_sdk_s3::config::Credentials::new(container.username(), container.password(), None, None, "test-probe");
        let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_config::Region::new(PERIMETER_REGION))
            .credentials_provider(credentials)
            .load()
            .await;

        aws_sdk_s3::Client::from_conf(
            aws_sdk_s3::config::Builder::from(&sdk_config)
                .endpoint_url(container.endpoint().to_string())
                .force_path_style(true)
                .build(),
        )
    }

    #[async_trait]
    impl ProviderHarness for S3ProviderHarness {
        fn provider_name(&self) -> &'static str {
            "s3"
        }

        async fn build_storage(
            &self,
            request: &ProviderStorageRequest,
        ) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
            let mut config = S3Config::new(
                self.container.endpoint(),
                self.container.username(),
                self.container.password(),
                PERIMETER_CONTAINER_NAME,
                PERIMETER_REGION,
            )
            .with_state_prefix(&request.state_prefix)
            .with_job_state_codec(request.codec);
            if let Some(list_page_size) = request.list_page_size {
                config = config.with_list_page_size(list_page_size)?;
            }
            if let Some(request_timeout) = request.request_timeout {
                config = config.with_request_timeout(request_timeout);
            }

            Ok(
                Arc::new(S3Backend::build(config, Arc::clone(&request.registry), Arc::clone(&request.metrics)).await?)
                    as Arc<dyn Storage>,
            )
        }

        fn attach_storage(&self, builder: JobsManagerBuilder, state_prefix: &str) -> JobsManagerBuilder {
            builder.s3(S3Config::new(
                self.container.endpoint(),
                self.container.username(),
                self.container.password(),
                PERIMETER_CONTAINER_NAME,
                PERIMETER_REGION,
            )
            .with_state_prefix(state_prefix)
            .with_job_state_codec(JobStateCodecKind::Json))
        }

        async fn read_state_keys(&self, state_prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let mut keys = Vec::new();
            let mut continuation_token = None;

            loop {
                let listed = self
                    .probe
                    .list_objects_v2()
                    .bucket(PERIMETER_CONTAINER_NAME)
                    .prefix(state_prefix)
                    .set_continuation_token(continuation_token)
                    .send()
                    .await?;

                keys.extend(listed.contents().iter().filter_map(|object| object.key().map(String::from)));

                continuation_token = listed.next_continuation_token().map(String::from);
                if continuation_token.is_none() {
                    break;
                }
            }

            keys.sort();
            Ok(keys)
        }
    }
}

#[cfg(feature = "storage-azure")]
mod azure {
    use std::sync::Arc;

    use async_trait::async_trait;
    use azure_storage_blob::{BlobContainerClient, models::BlobContainerClientListBlobsOptions};
    use futures_util::TryStreamExt;

    use super::{PERIMETER_CONTAINER_NAME, ProviderHarness, ProviderStorageRequest};
    use crate::tests::common::azure_container::AzureTestContainer;
    use crate::{AzureBackend, AzureConfig, JobStateCodecKind, JobsManagerBuilder, Storage};

    /// An Azure Blob Storage emulator started for a test, torn down with the harness.
    pub struct AzureProviderHarness {
        container: AzureTestContainer,
        /// Client belonging to the test, so what the backend stored is read without going through
        /// it. What it does not share with the backend is the key layout and the `Storage`
        /// implementation, which is what the assertions are about.
        probe: BlobContainerClient,
    }

    impl AzureProviderHarness {
        /// Starts the emulator the perimeter runs against and builds the probe that reads it.
        ///
        /// # Errors
        ///
        /// Returns an error when the container does not start or become reachable, or when the
        /// probe cannot be built against it.
        pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
            let container = AzureTestContainer::start().await?;
            let probe = container.build_container_client(PERIMETER_CONTAINER_NAME)?;

            Ok(Self { container, probe })
        }
    }

    #[async_trait]
    impl ProviderHarness for AzureProviderHarness {
        fn provider_name(&self) -> &'static str {
            "azure"
        }

        async fn build_storage(
            &self,
            request: &ProviderStorageRequest,
        ) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
            let mut config = AzureConfig::new(
                self.container.endpoint(),
                AzureTestContainer::account(),
                AzureTestContainer::account_key(),
                PERIMETER_CONTAINER_NAME,
            )
            .with_state_prefix(&request.state_prefix)
            .with_job_state_codec(request.codec);
            if let Some(list_page_size) = request.list_page_size {
                config = config.with_list_page_size(list_page_size)?;
            }
            if let Some(request_timeout) = request.request_timeout {
                config = config.with_request_timeout(request_timeout);
            }

            Ok(
                Arc::new(
                    AzureBackend::build(config, Arc::clone(&request.registry), Arc::clone(&request.metrics)).await?,
                ) as Arc<dyn Storage>,
            )
        }

        fn attach_storage(&self, builder: JobsManagerBuilder, state_prefix: &str) -> JobsManagerBuilder {
            builder.azure(
                AzureConfig::new(
                    self.container.endpoint(),
                    AzureTestContainer::account(),
                    AzureTestContainer::account_key(),
                    PERIMETER_CONTAINER_NAME,
                )
                .with_state_prefix(state_prefix)
                .with_job_state_codec(JobStateCodecKind::Json),
            )
        }

        async fn read_state_keys(&self, state_prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let options = BlobContainerClientListBlobsOptions {
                prefix: Some(state_prefix.to_string()),
                ..Default::default()
            };
            let mut pages = self.probe.list_blobs(Some(options))?.into_pages();

            let mut keys = Vec::new();
            while let Some(page) = pages.try_next().await? {
                keys.extend(page.into_model()?.blob_items.into_iter().filter_map(|blob| blob.name));
            }

            keys.sort();
            Ok(keys)
        }
    }
}

#[cfg(feature = "storage-gcs")]
mod gcs {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde::Deserialize;

    use super::{PERIMETER_CONTAINER_NAME, ProviderHarness, ProviderStorageRequest};
    use crate::tests::common::gcs_container::GcsTestContainer;
    use crate::{GcsBackend, GcsConfig, JobStateCodecKind, JobsManagerBuilder, Storage};

    /// Project the perimeter's bucket is created under. The emulator accepts any name; the service
    /// would check it.
    const PERIMETER_PROJECT_ID: &str = "jobmanager-perimeter";

    /// A Google Cloud Storage emulator started for a test, torn down with the harness.
    pub struct GcsProviderHarness {
        container: GcsTestContainer,
        probe: reqwest::Client,
    }

    impl GcsProviderHarness {
        /// Starts the emulator the perimeter runs against and builds the probe that reads it.
        ///
        /// # Errors
        ///
        /// Returns an error when the container does not start or become reachable, or when the
        /// probe cannot be built.
        pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
            let container = GcsTestContainer::start().await?;

            Ok(Self {
                container,
                probe: reqwest::Client::builder().build()?,
            })
        }
    }

    /// One entry of a listing as the probe reads it.
    ///
    /// Declared here rather than taken from `gcs_client`, so what the assertions read is parsed by
    /// the test and not by the code under test - the same principle the Azure probe follows by
    /// sharing neither the key layout nor the `Storage` implementation.
    #[derive(Deserialize)]
    struct ProbeListedObject {
        name: String,
    }

    #[derive(Deserialize)]
    #[serde(rename_all = "camelCase")]
    struct ProbeListedPage {
        #[serde(default)]
        items: Vec<ProbeListedObject>,
        next_page_token: Option<String>,
    }

    /// Configuration of the backend under test: anonymous, because the emulator serves no
    /// credentials, and with the project a bucket is created under.
    fn build_perimeter_config(endpoint: &str, state_prefix: &str, codec: JobStateCodecKind) -> GcsConfig {
        GcsConfig::new(endpoint, PERIMETER_CONTAINER_NAME)
            .with_anonymous_access()
            .with_project_id(PERIMETER_PROJECT_ID)
            .with_state_prefix(state_prefix)
            .with_job_state_codec(codec)
    }

    #[async_trait]
    impl ProviderHarness for GcsProviderHarness {
        fn provider_name(&self) -> &'static str {
            "gcs"
        }

        async fn build_storage(
            &self,
            request: &ProviderStorageRequest,
        ) -> Result<Arc<dyn Storage>, Box<dyn std::error::Error>> {
            let mut config = build_perimeter_config(self.container.endpoint(), &request.state_prefix, request.codec);
            if let Some(list_page_size) = request.list_page_size {
                config = config.with_list_page_size(list_page_size)?;
            }
            if let Some(request_timeout) = request.request_timeout {
                config = config.with_request_timeout(request_timeout);
            }

            Ok(
                Arc::new(GcsBackend::build(config, Arc::clone(&request.registry), Arc::clone(&request.metrics)).await?)
                    as Arc<dyn Storage>,
            )
        }

        fn attach_storage(&self, builder: JobsManagerBuilder, state_prefix: &str) -> JobsManagerBuilder {
            builder.gcs(build_perimeter_config(
                self.container.endpoint(),
                state_prefix,
                JobStateCodecKind::Json,
            ))
        }

        async fn read_state_keys(&self, state_prefix: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
            let mut keys = Vec::new();
            let mut page_token: Option<String> = None;

            loop {
                let mut url = format!(
                    "{}/storage/v1/b/{PERIMETER_CONTAINER_NAME}/o?prefix={state_prefix}",
                    self.container.endpoint()
                );
                if let Some(page_token) = &page_token {
                    url.push_str("&pageToken=");
                    url.push_str(page_token);
                }

                let listed_page: ProbeListedPage = self.probe.get(url).send().await?.error_for_status()?.json().await?;

                keys.extend(listed_page.items.into_iter().map(|object| object.name));

                page_token = listed_page.next_page_token;
                if page_token.is_none() {
                    break;
                }
            }

            keys.sort();
            Ok(keys)
        }
    }
}

#[cfg(feature = "storage-azure")]
pub use azure::AzureProviderHarness;
#[cfg(feature = "storage-gcs")]
pub use gcs::GcsProviderHarness;
#[cfg(feature = "storage-s3")]
pub use s3::S3ProviderHarness;
