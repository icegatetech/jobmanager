//! Azure Blob Storage emulator testcontainer for jobmanager integration tests.
//!
//! The helper names the provider (`AzureTestContainer`) rather than the image, so the emulator can
//! change without touching call sites.

use std::sync::Arc;
use std::time::{Duration, Instant};

use azure_core::{
    credentials::Secret,
    http::{ClientOptions, RetryOptions, Url, policies::Policy},
};
use azure_storage_blob::{BlobContainerClient, BlobContainerClientOptions};
use testcontainers::{
    ContainerAsync, GenericImage, ImageExt,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};

use crate::storage::azure_signing::SharedKeySigningPolicy;

/// Container the readiness probe asks about. It is never created, so the emulator answers that it
/// is not there - an answer only a started service gives, which is the point.
const READINESS_PROBE_CONTAINER: &str = "jobmanager-readiness-probe";

/// Container image backing the Azure Blob Storage emulator.
const AZURITE_IMAGE: &str = "mcr.microsoft.com/azure-storage/azurite";
/// Pinned Azurite image tag.
const AZURITE_TAG: &str = "3.36.0";
/// Account the emulator serves. Fixed by the image, not a choice of this harness.
const STORAGE_ACCOUNT: &str = "devstoreaccount1";
/// Key of that account. Published by Microsoft and identical in every installation of the emulator,
/// which is why it may live in the repository - it authorizes nothing but a local container.
const STORAGE_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
/// Port the emulator serves the blob service on.
const BLOB_SERVICE_PORT: u16 = 10_000;
/// How long the emulator is given to answer the readiness probe.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);

/// Manages an Azure Blob Storage emulator testcontainer for integration tests.
///
/// The container is automatically cleaned up when this struct is dropped.
pub struct AzureTestContainer {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
}

impl AzureTestContainer {
    /// Start a new emulator container and wait until the blob service answers.
    ///
    /// Readiness is an answered request rather than an accepted connection: the mapped port is
    /// forwarded the moment the container exists, so a connection succeeds seconds before the
    /// service behind it is listening - and a backend built in that window fails on a connection the
    /// store reset.
    ///
    /// # Errors
    ///
    /// Returns an error if the container fails to start, the mapped port cannot be determined, the
    /// probe cannot be built, or the emulator does not answer within [`STARTUP_TIMEOUT`].
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        tracing::info!("Starting azure blob storage emulator container...");

        let image = GenericImage::new(AZURITE_IMAGE, AZURITE_TAG)
            .with_wait_for(WaitFor::seconds(1))
            .with_exposed_port(BLOB_SERVICE_PORT.tcp())
            .with_cmd([
                "azurite-blob",
                // The emulator binds to loopback inside the container unless told otherwise, which
                // the mapped port cannot reach.
                "--blobHost",
                "0.0.0.0",
                // The SDK declares the REST version it was generated for, and the emulator refuses
                // any version newer than the one it knows. Relaxing the check here keeps the
                // production client speaking the version the SDK actually implements; pinning an
                // older one on the client would make the tests exercise a request the released
                // crate never sends.
                "--skipApiVersionCheck",
            ]);

        let container: ContainerAsync<GenericImage> = image.start().await?;

        let port = container.get_host_port_ipv4(BLOB_SERVICE_PORT).await?;
        // The emulator addresses accounts by path rather than by host, so the account belongs in
        // the endpoint a client is built from.
        let endpoint = format!("http://127.0.0.1:{port}/{STORAGE_ACCOUNT}");

        tracing::info!("Azure blob storage emulator container started on {}", endpoint);

        let store = Self {
            _container: container,
            endpoint,
        };

        let probe = store.build_container_client(READINESS_PROBE_CONTAINER)?;
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            // The probe is bounded by what is left of the deadline rather than checked after it
            // answers: the mapped port accepts a connection the moment the container exists, so a
            // request can be accepted by a service that is not yet answering - and a probe waited on
            // without a bound of its own holds the whole suite rather than failing it.
            let Ok(probed) =
                tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), probe.exists()).await
            else {
                return Err(format!("Azure blob storage emulator did not answer within {STARTUP_TIMEOUT:?}").into());
            };
            match probed {
                Ok(_) => break,
                Err(err) if Instant::now() < deadline => {
                    tracing::debug!("Azure blob storage emulator not answering yet: {err}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(err) => return Err(format!("Azure blob storage emulator not reachable: {err}").into()),
            }
        }

        Ok(store)
    }

    /// Client of `container_name` on this account, signed the way every client of the account is:
    /// the service refuses an unsigned request before it says anything about the container. The
    /// SDK's own retries are off, so one call of the client costs one request.
    ///
    /// # Errors
    ///
    /// Returns an error when the endpoint and container do not form a URL, when the account key is
    /// not usable for signing, or when the client cannot be built.
    pub fn build_container_client(
        &self,
        container_name: &str,
    ) -> Result<BlobContainerClient, Box<dyn std::error::Error>> {
        let signing_policy =
            SharedKeySigningPolicy::try_new(STORAGE_ACCOUNT, &Secret::new(STORAGE_ACCOUNT_KEY.to_string()))?;
        let options = BlobContainerClientOptions {
            client_options: ClientOptions {
                per_try_policies: vec![Arc::new(signing_policy) as Arc<dyn Policy>],
                retry: RetryOptions::none(),
                ..Default::default()
            },
            ..Default::default()
        };

        Ok(BlobContainerClient::new(
            Url::parse(&format!("{}/{container_name}", self.endpoint))?,
            None,
            Some(options),
        )?)
    }

    /// Host-mapped blob service endpoint of the emulated account, account path included.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    /// Name of the emulated storage account, fixed by the image.
    #[must_use]
    pub const fn account() -> &'static str {
        STORAGE_ACCOUNT
    }

    /// Key of the emulated storage account, fixed by the image.
    #[must_use]
    pub const fn account_key() -> &'static str {
        STORAGE_ACCOUNT_KEY
    }
}
