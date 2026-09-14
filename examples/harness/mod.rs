//! Shared setup for the examples: where the store is, how logging is turned on, how a flag is read.
//!
//! This is a directory without a `main.rs`, so cargo does not build it as an example of its own.
//! Every example pulls it in with `mod harness;`.
//!
//! Every connection detail below has a default naming the local store `make examples-infra-up`
//! starts, and a setting that overrides it - see [`SETTINGS_FILE`]. That is what lets the same
//! example run against a provider's own service without a line of it changing.

// Not every example uses every helper, and `dead_code` is denied crate-wide.
#![allow(dead_code)]

use std::time::Duration;

#[cfg(feature = "storage-azure")]
use jobmanager::AzureConfig;
#[cfg(feature = "storage-gcs")]
use jobmanager::GcsConfig;
use jobmanager::{Error, Result};
#[cfg(feature = "storage-s3")]
use jobmanager::{JobStateCodecKind, S3Config};
#[cfg(feature = "storage-s3")]
use uuid::Uuid;

/// File the examples read their connection details from, absent by default.
///
/// Addressed from the manifest directory rather than the working one: an example runs from wherever
/// cargo was invoked, and a relative path would silently fall back to the defaults below whenever
/// that is not the repository root. `examples/.env.example` lists every setting with the default it
/// overrides; copy it to `examples/.env` and edit what a run of yours needs.
const SETTINGS_FILE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/.env");

/// Timeout every example applies to a single storage operation, in whole seconds. Unset, the
/// backend's own default stands - 5s, which is what a container on this machine answers within and
/// a provider's service across the internet does not.
///
/// A setting is a constant here only when its name is needed away from the call that reads it - in
/// an error message, or in a doc comment. The rest are spelled at the single place they are read,
/// where the reader is looking for exactly the string they put in [`SETTINGS_FILE`].
const REQUEST_TIMEOUT_SETTING: &str = "JM_REQUEST_TIMEOUT_SECS";

/// Reads [`SETTINGS_FILE`] into the environment.
///
/// A missing file is the local-store run and is not a failure. A file that exists and does not
/// parse is, because the alternative is a run on the defaults below while its settings sit in a
/// file nobody read.
///
/// # Errors
///
/// Returns [`Error::Other`] when [`SETTINGS_FILE`] exists and `dotenvy` refuses it.
fn load_settings_file() -> Result<()> {
    match dotenvy::from_path(SETTINGS_FILE) {
        Ok(()) => Ok(()),
        Err(e) if e.not_found() => Ok(()),
        Err(e) => Err(Error::Other(format!("{SETTINGS_FILE} was not read: {e}"))),
    }
}

/// Value `name` carries in the environment, or `None` when it carries none.
///
/// [`init_tracing`] is what puts [`SETTINGS_FILE`] into that environment, and every example calls
/// it first - a setting read before it would see the file as absent.
///
/// An empty value counts as none: a setting listed with nothing after the `=` is one the file
/// mentions without setting, and reading it as an empty endpoint would fail far from its cause.
///
/// The environment wins over the file, `dotenvy` setting only the names it finds unset - so a
/// setting given on the command line overrides the one the file carries, and not the other way
/// round.
fn find_setting(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|value| !value.is_empty())
}

/// [`find_setting`], with `default` standing in for a setting that carries no value.
fn read_setting(name: &str, default: &str) -> String {
    find_setting(name).unwrap_or_else(|| default.to_string())
}

/// Timeout [`REQUEST_TIMEOUT_SETTING`] asks for, or `None` when it asks for nothing.
///
/// # Errors
///
/// Returns [`Error::Other`] when the setting is not a whole number of seconds - a mistyped timeout
/// stops the example rather than leaving it on a default the author thought they had replaced.
fn read_request_timeout() -> Result<Option<Duration>> {
    let Some(seconds) = find_setting(REQUEST_TIMEOUT_SETTING) else {
        return Ok(None);
    };

    seconds
        .parse::<u64>()
        .map(|seconds| Some(Duration::from_secs(seconds)))
        .map_err(|e| {
            Error::Other(format!(
                "{REQUEST_TIMEOUT_SETTING} is not a number of seconds: {seconds}: {e}"
            ))
        })
}

/// Default endpoint of the S3-compatible store `make examples-infra-up` starts.
#[cfg(feature = "storage-s3")]
const DEFAULT_S3_ENDPOINT: &str = "http://localhost:9000";
/// Default credentials of that store, from `examples/docker-compose.yml`.
#[cfg(feature = "storage-s3")]
const DEFAULT_S3_ACCESS_KEY_ID: &str = "rustfsadmin";
#[cfg(feature = "storage-s3")]
const DEFAULT_S3_SECRET_ACCESS_KEY: &str = "rustfsadmin";
/// Default bucket, the one the compose file creates on start-up.
#[cfg(feature = "storage-s3")]
const DEFAULT_S3_BUCKET_NAME: &str = "jobs";
#[cfg(feature = "storage-s3")]
const DEFAULT_S3_REGION: &str = "us-east-1";

/// Storage config pointing at the example store, with this example's state under its own prefix.
///
/// The prefix is what keeps examples from reading each other's jobs: two examples sharing one would
/// see each other's iterations and clean up each other's tail.
///
/// # Errors
///
/// Returns [`Error::Other`] when [`REQUEST_TIMEOUT_SETTING`] is not a number of seconds.
#[cfg(feature = "storage-s3")]
pub fn build_s3_config(state_prefix: &str) -> Result<S3Config> {
    build_s3_config_with_codec(state_prefix, JobStateCodecKind::Json)
}

/// [`build_s3_config`] under a prefix nested one level deeper, unique to this run.
///
/// An example that caps its iterations and waits for them out can only observe a job that still has
/// budget left: a second run against the state the first one persisted would find the limit already
/// reached and wait forever. A fresh prefix per run is what keeps such an example re-runnable.
/// Everything those runs write goes away with `make examples-infra-down`.
///
/// # Errors
///
/// Returns [`Error::Other`] when [`REQUEST_TIMEOUT_SETTING`] is not a number of seconds.
#[cfg(feature = "storage-s3")]
pub fn build_run_scoped_s3_config(state_prefix: &str) -> Result<S3Config> {
    build_s3_config(&format!("{state_prefix}/{}", Uuid::new_v4()))
}

/// [`build_s3_config`] with the codec spelled out, for the example that contrasts the two.
///
/// # Errors
///
/// Returns [`Error::Other`] when [`REQUEST_TIMEOUT_SETTING`] is not a number of seconds.
#[cfg(feature = "storage-s3")]
pub fn build_s3_config_with_codec(state_prefix: &str, codec: JobStateCodecKind) -> Result<S3Config> {
    let config = S3Config::new(
        read_setting("JM_S3_ENDPOINT", DEFAULT_S3_ENDPOINT),
        read_setting("JM_S3_ACCESS_KEY_ID", DEFAULT_S3_ACCESS_KEY_ID),
        read_setting("JM_S3_SECRET_ACCESS_KEY", DEFAULT_S3_SECRET_ACCESS_KEY),
        read_setting("JM_S3_BUCKET", DEFAULT_S3_BUCKET_NAME),
        read_setting("JM_S3_REGION", DEFAULT_S3_REGION),
    )
    .with_state_prefix(state_prefix)
    .with_job_state_codec(codec);

    Ok(match read_request_timeout()? {
        Some(request_timeout) => config.with_request_timeout(request_timeout),
        None => config,
    })
}

/// Default endpoint of the Azure Blob Storage emulator `make examples-infra-up` starts. The emulator
/// addresses accounts by path rather than by host, so the account belongs in the endpoint; the
/// service addresses them by host, as `https://<account>.blob.core.windows.net`.
#[cfg(feature = "storage-azure")]
const DEFAULT_AZURE_ENDPOINT: &str = "http://localhost:10000/devstoreaccount1";
/// Default account, the one the emulator serves, fixed by its image.
#[cfg(feature = "storage-azure")]
const DEFAULT_AZURE_ACCOUNT: &str = "devstoreaccount1";
/// Key of that account. Published by Microsoft and identical in every installation of the emulator,
/// which is why it may live in the repository - it authorizes nothing but a local container.
#[cfg(feature = "storage-azure")]
const DEFAULT_AZURE_ACCOUNT_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
/// Default container, created by the backend on start-up, the emulator holding none.
#[cfg(feature = "storage-azure")]
const DEFAULT_AZURE_CONTAINER_NAME: &str = "jobs";

/// Storage config pointing at the Azure Blob Storage container the settings name, with this
/// example's state under its own prefix - the prefix serving the same purpose it does on S3.
///
/// Shared Key is the only way in this backend has, so an account and its key are what the settings
/// carry for the service exactly as they do for the emulator.
///
/// # Errors
///
/// Returns [`Error::Other`] when [`REQUEST_TIMEOUT_SETTING`] is not a number of seconds.
#[cfg(feature = "storage-azure")]
pub fn build_azure_config(state_prefix: &str) -> Result<AzureConfig> {
    let config = AzureConfig::new(
        read_setting("JM_AZURE_ENDPOINT", DEFAULT_AZURE_ENDPOINT),
        read_setting("JM_AZURE_ACCOUNT", DEFAULT_AZURE_ACCOUNT),
        read_setting("JM_AZURE_ACCOUNT_KEY", DEFAULT_AZURE_ACCOUNT_KEY),
        read_setting("JM_AZURE_CONTAINER", DEFAULT_AZURE_CONTAINER_NAME),
    )
    .with_state_prefix(state_prefix);

    Ok(match read_request_timeout()? {
        Some(request_timeout) => config.with_request_timeout(request_timeout),
        None => config,
    })
}

/// Default endpoint of the Google Cloud Storage emulator `make examples-infra-up` starts. The
/// service answers at `https://storage.googleapis.com`.
#[cfg(feature = "storage-gcs")]
const DEFAULT_GCS_ENDPOINT: &str = "http://localhost:9199";
/// Default bucket, created by the backend on start-up, the emulator holding none.
#[cfg(feature = "storage-gcs")]
const DEFAULT_GCS_BUCKET_NAME: &str = "jobs";
/// Default project the bucket is created under. The emulator accepts any name; the service would not.
#[cfg(feature = "storage-gcs")]
const DEFAULT_GCS_PROJECT_ID: &str = "jobmanager-examples";
/// Setting choosing what the examples authenticate to Google Cloud Storage with: `anonymous`, `adc`,
/// or the path of a service account key file.
#[cfg(feature = "storage-gcs")]
const GCS_CREDENTIALS_SETTING: &str = "JM_GCS_CREDENTIALS";
/// Value of [`GCS_CREDENTIALS_SETTING`] asking for no credentials at all, which is what the emulator
/// serves and the default the examples take.
#[cfg(feature = "storage-gcs")]
const GCS_CREDENTIALS_ANONYMOUS: &str = "anonymous";
/// Value of [`GCS_CREDENTIALS_SETTING`] asking for Application Default Credentials, the chain
/// `gcloud` and `GOOGLE_APPLICATION_CREDENTIALS` establish.
#[cfg(feature = "storage-gcs")]
const GCS_CREDENTIALS_APPLICATION_DEFAULT: &str = "adc";

/// Storage config pointing at the Google Cloud Storage bucket the settings name, with this
/// example's state under its own prefix - the prefix serving the same purpose it does on S3.
///
/// # Errors
///
/// Returns [`Error::Other`] when [`REQUEST_TIMEOUT_SETTING`] is not a number of seconds, and when
/// [`GCS_CREDENTIALS_SETTING`] names a service account key file that cannot be read.
#[cfg(feature = "storage-gcs")]
pub fn build_gcs_config(state_prefix: &str) -> Result<GcsConfig> {
    let config = GcsConfig::new(
        read_setting("JM_GCS_ENDPOINT", DEFAULT_GCS_ENDPOINT),
        read_setting("JM_GCS_BUCKET", DEFAULT_GCS_BUCKET_NAME),
    )
    .with_project_id(read_setting("JM_GCS_PROJECT_ID", DEFAULT_GCS_PROJECT_ID))
    .with_state_prefix(state_prefix);
    let config = apply_gcs_credentials(config)?;

    Ok(match read_request_timeout()? {
        Some(request_timeout) => config.with_request_timeout(request_timeout),
        None => config,
    })
}

/// Puts the credentials [`GCS_CREDENTIALS_SETTING`] asks for on `config`.
///
/// Application Default Credentials are what the config takes when nothing is said, so `adc` adds
/// nothing; any value that is neither it nor `anonymous` is read as the path of a service account
/// key, whose JSON - not its path - is what the config is given.
///
/// # Errors
///
/// Returns [`Error::Other`] when the named key file cannot be read.
#[cfg(feature = "storage-gcs")]
fn apply_gcs_credentials(config: GcsConfig) -> Result<GcsConfig> {
    let credentials = read_setting(GCS_CREDENTIALS_SETTING, GCS_CREDENTIALS_ANONYMOUS);

    match credentials.as_str() {
        GCS_CREDENTIALS_ANONYMOUS => Ok(config.with_anonymous_access()),
        GCS_CREDENTIALS_APPLICATION_DEFAULT => Ok(config),
        key_path => {
            let service_account_key = std::fs::read_to_string(key_path).map_err(|e| {
                Error::Other(format!(
                    "{GCS_CREDENTIALS_SETTING} is neither `{GCS_CREDENTIALS_ANONYMOUS}` nor \
                     `{GCS_CREDENTIALS_APPLICATION_DEFAULT}`, and the service account key it names \
                     was not read: {key_path}: {e}"
                ))
            })?;

            Ok(config.with_service_account_key(service_account_key))
        }
    }
}

/// Reads [`SETTINGS_FILE`] and installs the subscriber every example logs through. `RUST_LOG`
/// overrides the default filter and is a setting of that file like any other, which is why the two
/// happen together and first: a setting read before this call would see the file as absent.
///
/// # Errors
///
/// Returns [`Error::Other`] when [`SETTINGS_FILE`] exists and cannot be read.
pub fn init_tracing() -> Result<()> {
    load_settings_file()?;

    let filter = read_setting("RUST_LOG", "info,jobmanager=info");
    tracing_subscriber::fmt().with_target(false).with_env_filter(filter).init();

    Ok(())
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
