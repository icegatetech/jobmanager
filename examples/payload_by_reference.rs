// Why a task payload stays small, and what to pass instead.
//
// Every task's input and output live inside the job's single state object, which every worker reads
// on every poll of that job. A large payload is therefore paid for repeatedly, by everyone -
// `TaskLimits` exists to make that failure loud rather than slow. An oversized payload is rejected,
// never truncated: `add_task` returns an error and nothing is created.
//
// The fix is to pass a key and keep the bytes where they already are.

#![allow(missing_docs)]

mod support;

use std::collections::HashMap;
use std::sync::Arc;

use jobmanager::TaskLimits;
use jobmanager::prelude::*;
use parking_lot::Mutex;

const PLAN_TASK_CODE: &str = "plan";
const PROCESS_TASK_CODE: &str = "process";

/// Deliberately small, so the example's oversized payload is rejected without allocating megabytes.
const MAX_PAYLOAD_BYTES: usize = 4 * 1024;
/// Comfortably over the cap.
const OVERSIZED_PAYLOAD_BYTES: usize = 8 * 1024;
/// Key the payload is stored under, and the whole of what the follow-up task carries.
const BLOB_KEY: &str = "iteration/blob-0";

/// Stands in for the store the bytes actually live in - a bucket, a table, a cache.
struct BlobStore {
    blobs: Mutex<HashMap<String, Vec<u8>>>,
}

impl BlobStore {
    fn new() -> Self {
        Self {
            blobs: Mutex::new(HashMap::new()),
        }
    }

    fn put_blob(&self, key: &str, bytes: Vec<u8>) {
        self.blobs.lock().insert(key.to_string(), bytes);
    }

    fn get_blob_size(&self, key: &str) -> Option<usize> {
        self.blobs.lock().get(key).map(Vec::len)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    let blobs = Arc::new(BlobStore::new());
    let job_code = JobCode::new("payload-by-reference");

    let manager = JobsManager::builder()
        .s3(support::build_run_scoped_s3_config("payload-by-reference"))
        .workers(2)
        .job(job_code.clone(), |j| {
            j.max_iterations(1);
            j.task_limits(TaskLimits {
                max_input_bytes: MAX_PAYLOAD_BYTES,
                max_output_bytes: MAX_PAYLOAD_BYTES,
            });

            let plan_blobs = Arc::clone(&blobs);
            j.add_task(
                TaskDefinition::new(PLAN_TASK_CODE, Duration::from_secs(30)),
                task_fn(move |ctx: TaskContext| {
                    let blobs = Arc::clone(&plan_blobs);
                    async move { plan_blob_task(&blobs, &ctx) }
                }),
            );

            let process_blobs = Arc::clone(&blobs);
            j.add_task_executor(
                PROCESS_TASK_CODE,
                task_fn(move |ctx: TaskContext| {
                    let blobs = Arc::clone(&process_blobs);
                    async move { process_blob(&blobs, &ctx) }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!("example finished");
    Ok(())
}

/// Shows both halves: the oversized payload being rejected, then the key being passed instead.
fn plan_blob_task(blobs: &BlobStore, ctx: &TaskContext) -> TaskResult {
    let payload = vec![0_u8; OVERSIZED_PAYLOAD_BYTES];

    // The wrong way. `add_task` rejects it, and the error is what a caller gets in production too -
    // the payload is never truncated to fit.
    let oversized = TaskDefinition::new(PROCESS_TASK_CODE, Duration::from_secs(30)).with_input(payload.clone());
    match ctx.job().add_task(oversized) {
        Ok(_) => return Err("a payload over the limit must not be accepted".into()),
        Err(error) => tracing::warn!(
            payload_bytes = payload.len(),
            limit_bytes = MAX_PAYLOAD_BYTES,
            %error,
            "oversized payload rejected, as it should be"
        ),
    }

    // The right way: the bytes go where bytes go, and the task carries the key.
    blobs.put_blob(BLOB_KEY, payload);
    let by_reference =
        TaskDefinition::new(PROCESS_TASK_CODE, Duration::from_secs(30)).with_input(BLOB_KEY.as_bytes().to_vec());
    ctx.job().add_task(by_reference)?;
    tracing::info!(
        blob_key = BLOB_KEY,
        "handed the follow-up task a key instead of the bytes"
    );

    Ok(().into())
}

/// Resolves the key it was handed and works on what it finds.
fn process_blob(blobs: &BlobStore, ctx: &TaskContext) -> TaskResult {
    let blob_key = String::from_utf8_lossy(ctx.input()).to_string();
    let blob_bytes = blobs
        .get_blob_size(&blob_key)
        .ok_or_else(|| format!("blob '{blob_key}' is missing from the store"))?;

    tracing::info!(%blob_key, blob_bytes, "processed the blob the key pointed at");
    Ok(().into())
}
