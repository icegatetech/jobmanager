//! Google Cloud Storage emulator testcontainer for jobmanager integration tests.
//!
//! The helper names the provider (`GcsTestContainer`) rather than the image, so the emulator can
//! change without touching call sites.

use std::time::{Duration, Instant};

use testcontainers::{
    ContainerAsync, GenericImage,
    core::{IntoContainerPort, WaitFor},
    runners::AsyncRunner,
};
/// Bucket the readiness probe asks about. It is never created, so the emulator answers `404` - an
/// answer only a started service gives, which is the point.
const READINESS_PROBE_BUCKET: &str = "jobmanager-readiness-probe";

/// Container image backing the Google Cloud Storage emulator. Google's own test bench for its
/// client libraries, which is what makes it the one emulator honouring `ifGenerationMatch`,
/// `ifGenerationNotMatch` and the `304` a conditional read is answered with.
const STORAGE_TESTBENCH_IMAGE: &str = "gcr.io/cloud-devrel-public-resources/storage-testbench";
/// Pinned test bench image tag.
///
/// At the time it was pinned this tag was
/// `sha256:600fa5c3cfc8be26435c38591cc094fb4ef648f760ffabf77f93237b1ebee027`. The digest is written
/// here rather than sent to the daemon because a container is started by `name:tag` and by nothing
/// else, so a constant holding it would be an item nothing reads.
const STORAGE_TESTBENCH_TAG: &str = "v0.63.0";
/// Port the emulator serves the JSON API on.
const JSON_API_PORT: u16 = 9000;
/// How long the emulator is given to accept connections. Longer than the other two harnesses allow:
/// the image is built for `linux/amd64` only, so on an arm64 host it starts under emulation.
const STARTUP_TIMEOUT: Duration = Duration::from_secs(30);

/// Manages a Google Cloud Storage emulator testcontainer for integration tests.
///
/// The container is automatically cleaned up when this struct is dropped. It holds no bucket: the
/// backend creates the one it was pointed at, which is the branch `container_creation_test` is
/// about.
pub struct GcsTestContainer {
    _container: ContainerAsync<GenericImage>,
    endpoint: String,
}

impl GcsTestContainer {
    /// Start a new emulator container and wait until the JSON API answers.
    ///
    /// Readiness is an answered request rather than an accepted connection: the mapped port is
    /// forwarded the moment the container exists, so a connection succeeds seconds before the
    /// service behind it is listening - and a backend built in that window fails on a connection
    /// the store reset.
    ///
    /// # Errors
    ///
    /// Returns an error if the container fails to start, the mapped port cannot be determined, or
    /// the emulator does not answer within [`STARTUP_TIMEOUT`].
    pub async fn start() -> Result<Self, Box<dyn std::error::Error>> {
        tracing::info!("Starting google cloud storage emulator container...");

        let image = GenericImage::new(STORAGE_TESTBENCH_IMAGE, STORAGE_TESTBENCH_TAG)
            .with_wait_for(WaitFor::seconds(1))
            .with_exposed_port(JSON_API_PORT.tcp());

        let container: ContainerAsync<GenericImage> = image.start().await?;

        let port = container.get_host_port_ipv4(JSON_API_PORT).await?;
        let endpoint = format!("http://127.0.0.1:{port}");

        tracing::info!("Google cloud storage emulator container started on {}", endpoint);

        let probe = reqwest::Client::builder().build()?;
        let probed_url = format!("{endpoint}/storage/v1/b/{READINESS_PROBE_BUCKET}");
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        loop {
            // Bounded by what is left of the deadline for the reason `AzureTestContainer::start`
            // states: a request the emulator accepts and never answers would otherwise hold the
            // whole suite rather than failing it.
            let Ok(probed) = tokio::time::timeout(
                deadline.saturating_duration_since(Instant::now()),
                probe.get(&probed_url).send(),
            )
            .await
            else {
                return Err(format!("Google cloud storage emulator did not answer within {STARTUP_TIMEOUT:?}").into());
            };
            match probed {
                Ok(_) => break,
                Err(err) if Instant::now() < deadline => {
                    tracing::debug!("Google cloud storage emulator not answering yet: {err}");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                Err(err) => return Err(format!("Google cloud storage emulator not reachable: {err}").into()),
            }
        }

        Ok(Self {
            _container: container,
            endpoint,
        })
    }

    /// Host-mapped JSON API endpoint of the emulator.
    #[must_use]
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }
}
