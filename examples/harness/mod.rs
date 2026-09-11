//! Shared setup for the examples: where the store is, how logging is turned on, how a flag is read.
//!
//! This is a directory without a `main.rs`, so cargo does not build it as an example of its own.
//! Every example pulls it in with `mod harness;`.

// Not every example uses every helper, and `dead_code` is denied crate-wide.
#![allow(dead_code)]

#[cfg(feature = "storage-azure")]
use jobmanager::AzureConfig;
#[cfg(feature = "storage-gcs")]
use jobmanager::GcsConfig;
#[cfg(feature = "storage-s3")]
use jobmanager::{JobStateCodecKind, S3Config};
#[cfg(feature = "storage-s3")]
use uuid::Uuid;

/// Endpoint of the S3-compatible store `make examples-infra-up` starts.
#[cfg(feature = "storage-s3")]
const S3_ENDPOINT: &str = "http://localhost:9000";
/// Credentials of that store, from `examples/docker-compose.yml`.
#[cfg(feature = "storage-s3")]
const S3_ACCESS_KEY_ID: &str = "rustfsadmin";
#[cfg(feature = "storage-s3")]
const S3_SECRET_ACCESS_KEY: &str = "rustfsadmin";
/// Bucket the compose file creates on start-up.
#[cfg(feature = "storage-s3")]
const S3_BUCKET_NAME: &str = "jobs";
#[cfg(feature = "storage-s3")]
const S3_REGION: &str = "us-east-1";

/// Endpoint of the Azure Blob Storage emulator `make examples-infra-up` starts. The emulator
/// addresses accounts by path rather than by host, so the account belongs in the endpoint.
#[cfg(feature = "storage-azure")]
const AZURE_ENDPOINT: &str = "http://localhost:10000/devstoreaccount1";
/// Account the emulator serves, fixed by its image.
#[cfg(feature = "storage-azure")]
const AZURE_ACCOUNT: &str = "devstoreaccount1";
/// Key of that account. Published by Microsoft and identical in every installation of the emulator,
/// which is why it may live in the repository - it authorizes nothing but a local container.
#[cfg(feature = "storage-azure")]
const AZURE_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
/// Container the backend creates on start-up, the emulator holding none by default.
#[cfg(feature = "storage-azure")]
const AZURE_CONTAINER_NAME: &str = "jobs";

/// Storage config pointing at the example store, with this example's state under its own prefix.
///
/// The prefix is what keeps examples from reading each other's jobs: two examples sharing one would
/// see each other's iterations and clean up each other's tail.
#[cfg(feature = "storage-s3")]
pub fn build_s3_config(state_prefix: &str) -> S3Config {
    build_s3_config_with_codec(state_prefix, JobStateCodecKind::Json)
}

/// [`build_s3_config`] under a prefix nested one level deeper, unique to this run.
///
/// An example that caps its iterations and waits for them out can only observe a job that still has
/// budget left: a second run against the state the first one persisted would find the limit already
/// reached and wait forever. A fresh prefix per run is what keeps such an example re-runnable.
/// Everything those runs write goes away with `make examples-infra-down`.
#[cfg(feature = "storage-s3")]
pub fn build_run_scoped_s3_config(state_prefix: &str) -> S3Config {
    build_s3_config(&format!("{state_prefix}/{}", Uuid::new_v4()))
}

/// [`build_s3_config`] with the codec spelled out, for the example that contrasts the two.
#[cfg(feature = "storage-s3")]
pub fn build_s3_config_with_codec(state_prefix: &str, codec: JobStateCodecKind) -> S3Config {
    S3Config::new(
        S3_ENDPOINT,
        S3_ACCESS_KEY_ID,
        S3_SECRET_ACCESS_KEY,
        S3_BUCKET_NAME,
        S3_REGION,
    )
    .with_state_prefix(state_prefix)
    .with_job_state_codec(codec)
}

/// Storage config pointing at the example emulator of Azure Blob Storage, with this example's state
/// under its own prefix - the prefix serving the same purpose it does on S3.
#[cfg(feature = "storage-azure")]
pub fn build_azure_config(state_prefix: &str) -> AzureConfig {
    AzureConfig::new(AZURE_ENDPOINT, AZURE_ACCOUNT, AZURE_ACCOUNT_KEY, AZURE_CONTAINER_NAME)
        .with_state_prefix(state_prefix)
}

/// Endpoint of the Google Cloud Storage emulator `make examples-infra-up` starts.
#[cfg(feature = "storage-gcs")]
const GCS_ENDPOINT: &str = "http://localhost:9199";
/// Bucket the backend creates on start-up, the emulator holding none by default.
#[cfg(feature = "storage-gcs")]
const GCS_BUCKET_NAME: &str = "jobs";
/// Project the bucket is created under. The emulator accepts any name; the service would not.
#[cfg(feature = "storage-gcs")]
const GCS_PROJECT_ID: &str = "jobmanager-examples";

/// Storage config pointing at the example emulator of Google Cloud Storage, with this example's
/// state under its own prefix - the prefix serving the same purpose it does on S3.
///
/// Anonymous access is what the emulator serves; against the service the credentials come from
/// Application Default Credentials or from a service account key.
#[cfg(feature = "storage-gcs")]
pub fn build_gcs_config(state_prefix: &str) -> GcsConfig {
    GcsConfig::new(GCS_ENDPOINT, GCS_BUCKET_NAME)
        .with_anonymous_access()
        .with_project_id(GCS_PROJECT_ID)
        .with_state_prefix(state_prefix)
}

/// Installs the subscriber every example logs through. `RUST_LOG` overrides the default filter.
pub fn init_tracing() {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info,jobmanager=info".to_string());
    tracing_subscriber::fmt().with_target(false).with_env_filter(filter).init();
}

/// Value that follows `flag` on the command line, e.g. `--node b` for `find_argument_value("--node")`.
///
/// Returns `None` when the flag is absent or is the last argument, so a caller supplies its own
/// default rather than getting an empty string that looks like a real value.
pub fn find_argument_value(flag: &str) -> Option<String> {
    let mut arguments = std::env::args();
    while let Some(argument) = arguments.next() {
        if argument == flag {
            return arguments.next();
        }
    }
    None
}
