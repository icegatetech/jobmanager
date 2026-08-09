//! Shared setup for the examples: where the store is, how logging is turned on, how a flag is read.
//!
//! This is a directory without a `main.rs`, so cargo does not build it as an example of its own.
//! Every example pulls it in with `mod harness;`.

// Not every example uses every helper, and `dead_code` is denied crate-wide.
#![allow(dead_code)]

use jobmanager::{JobStateCodecKind, S3StorageConfig};
use uuid::Uuid;

/// Endpoint of the store `make examples-infra-up` starts.
const ENDPOINT: &str = "http://localhost:9000";
/// Credentials of that store, from `examples/docker-compose.yml`.
const ACCESS_KEY_ID: &str = "rustfsadmin";
const SECRET_ACCESS_KEY: &str = "rustfsadmin";
/// Bucket the compose file creates on start-up.
const BUCKET_NAME: &str = "jobs";
const REGION: &str = "us-east-1";

/// Storage config pointing at the example store, with this example's state under its own prefix.
///
/// The prefix is what keeps examples from reading each other's jobs: two examples sharing one would
/// see each other's iterations and clean up each other's tail.
pub fn build_s3_config(bucket_prefix: &str) -> S3StorageConfig {
    build_s3_config_with_codec(bucket_prefix, JobStateCodecKind::Json)
}

/// [`build_s3_config`] under a prefix nested one level deeper, unique to this run.
///
/// An example that caps its iterations and waits for them out can only observe a job that still has
/// budget left: a second run against the state the first one persisted would find the limit already
/// reached and wait forever. A fresh prefix per run is what keeps such an example re-runnable.
/// Everything those runs write goes away with `make examples-infra-down`.
pub fn build_run_scoped_s3_config(bucket_prefix: &str) -> S3StorageConfig {
    build_s3_config(&format!("{bucket_prefix}/{}", Uuid::new_v4()))
}

/// [`build_s3_config`] with the codec spelled out, for the example that contrasts the two.
pub fn build_s3_config_with_codec(bucket_prefix: &str, codec: JobStateCodecKind) -> S3StorageConfig {
    S3StorageConfig::new(ENDPOINT, ACCESS_KEY_ID, SECRET_ACCESS_KEY, BUCKET_NAME, REGION)
        .with_bucket_prefix(bucket_prefix)
        .with_job_state_codec(codec)
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
