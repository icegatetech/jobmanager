//! Chunking of the multi-object delete, which only the S3 backend has.
//!
//! The rest of iteration cleanup is owed by every provider and lives in
//! [`job_cleanup_provider_test`](super::job_cleanup_provider_test). What is here is the batch size
//! itself: `AzureConfig` carries no such setting, because `azure_storage_blob` exposes no
//! client for the provider's batch operation.

use std::{sync::Arc, time::Duration};

use aws_sdk_s3::Client;
use tokio_util::sync::CancellationToken;

use super::common::persist_completed_iterations;
use super::common::s3_container::S3TestContainer;
use crate::{
    JobCode, JobDefinition, JobDefinitionId, JobRegistry, JobStateCodecKind, NoopMetrics, S3Backend, S3Config, Storage,
    TaskCode, TaskDefinition, TaskLimits, TaskOutcome, task_fn,
};

/// Prefix the cases here keep their state objects under.
const STATE_PREFIX: &str = "jobs";

/// Inverted iteration numbers used in state object names, stated literally so assertions do not
/// depend on the key builder under test. The layout is `state-{u64::MAX - iter_num:020}{ext}`, and
/// the entry at index `n` is the inverted number of iteration `n`.
const INVERTED_ITER_NUMS: [&str; 8] = [
    "18446744073709551615",
    "18446744073709551614",
    "18446744073709551613",
    "18446744073709551612",
    "18446744073709551611",
    "18446744073709551610",
    "18446744073709551609",
    "18446744073709551608",
];

fn build_state_key(job_code: &JobCode, iter_num: usize, extension: &str) -> String {
    format!(
        "{STATE_PREFIX}/{job_code}/state-{}{extension}",
        INVERTED_ITER_NUMS[iter_num]
    )
}

fn build_job_definition(job_code: &JobCode) -> Result<JobDefinition, Box<dyn std::error::Error>> {
    let executor = task_fn(|_ctx| async { Ok(TaskOutcome::Completed(b"done".to_vec())) });
    let task_def = TaskDefinition::new(TaskCode::new("cleanup_task"), Duration::from_secs(5));

    Ok(JobDefinition::new(
        JobDefinitionId::new(),
        job_code.clone(),
        vec![(task_def, executor)],
        Vec::new(),
        Vec::new(),
        TaskLimits::default(),
    )?
    .with_iteration_retention(5)?)
}

/// S3 client of the test itself, used to inspect the bucket without going through the code under
/// test.
async fn build_bucket_probe(store: &S3TestContainer) -> Client {
    let credentials =
        aws_sdk_s3::config::Credentials::new(store.username(), store.password(), None, None, "test-probe");
    let sdk_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region(aws_config::Region::new("us-east-1"))
        .credentials_provider(credentials)
        .load()
        .await;

    Client::from_conf(
        aws_sdk_s3::config::Builder::from(&sdk_config)
            .endpoint_url(store.endpoint().to_string())
            .force_path_style(true)
            .build(),
    )
}

async fn read_bucket_keys(
    probe: &Client,
    bucket_name: &str,
    prefix: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut keys = Vec::new();
    let mut continuation_token = None;

    loop {
        let output = probe
            .list_objects_v2()
            .bucket(bucket_name)
            .prefix(prefix)
            .set_continuation_token(continuation_token)
            .send()
            .await?;

        keys.extend(output.contents().iter().filter_map(|object| object.key().map(String::from)));

        continuation_token = output.next_continuation_token().map(String::from);
        if continuation_token.is_none() {
            break;
        }
    }

    keys.sort();
    Ok(keys)
}

#[tokio::test]
async fn delete_iterations_deletes_every_chunk_and_keeps_the_rest_json() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_delete_iterations_deletes_every_chunk_and_keeps_the_rest(
        "cleanup-chunking-json",
        JobStateCodecKind::Json,
        ".json",
    )
    .await
}

/// The state objects of a `Cbor` job carry a different extension, which both the key builder and
/// the cleanup filter have to agree on.
#[tokio::test]
async fn delete_iterations_deletes_every_chunk_and_keeps_the_rest_cbor() -> Result<(), Box<dyn std::error::Error>> {
    super::common::init_tracing();

    run_delete_iterations_deletes_every_chunk_and_keeps_the_rest(
        "cleanup-chunking-cbor",
        JobStateCodecKind::Cbor,
        ".cbor",
    )
    .await
}

async fn run_delete_iterations_deletes_every_chunk_and_keeps_the_rest(
    bucket_name: &str,
    codec: JobStateCodecKind,
    extension: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let job_code = JobCode::new("cleanup_chunking_job");
    let store = S3TestContainer::start().await?;

    let job_def = build_job_definition(&job_code)?;
    let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);
    let storage = S3Backend::build(
        S3Config::new(
            store.endpoint(),
            store.username(),
            store.password(),
            bucket_name,
            "us-east-1",
        )
        .with_state_prefix(STATE_PREFIX)
        .with_job_state_codec(codec)
        // Five iterations over a batch of two is what makes the chunking observable at all.
        .with_delete_batch_size(2)?,
        job_registry,
        Arc::new(NoopMetrics),
    )
    .await?;
    persist_completed_iterations(&storage, &job_code, 1..=7).await?;

    storage
        .delete_job_iterations(&job_code, &[1, 2, 3, 4, 5], &CancellationToken::new())
        .await?;

    let probe = build_bucket_probe(&store).await;
    let keys = read_bucket_keys(&probe, bucket_name, &format!("{STATE_PREFIX}/{job_code}/")).await?;
    let expected_keys: Vec<String> = (6..=7)
        .rev()
        .map(|iter_num| build_state_key(&job_code, iter_num, extension))
        .collect();

    assert_eq!(keys, expected_keys);
    Ok(())
}
