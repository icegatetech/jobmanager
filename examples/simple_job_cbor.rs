// The same job as `simple_job`, with job state serialized as CBOR instead of JSON.
//
// Only the codec differs. Switching it on a bucket that already holds state leaves the objects
// written under the previous codec unreadable as iterations, which is why this example writes under
// its own prefix.

#![allow(missing_docs)]

mod support;

use jobmanager::JobStateCodecKind;
use jobmanager::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    tracing::info!("Starting simple job example with CBOR-encoded state");

    let manager = JobsManager::builder()
        .s3(support::build_s3_config_with_codec(
            "simple-cbor",
            JobStateCodecKind::Cbor,
        ))
        .job("simple job", |j| {
            j.add_task(
                TaskDefinition::new("my task code", Duration::from_secs(5)),
                task_fn(|ctx| async move {
                    tracing::info!("Executing task: {}", ctx.id());
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    tracing::info!("Task completed: {}", ctx.id());
                    Ok(b"done".to_vec().into())
                }),
            );
        })
        .build()
        .await?;

    tracing::info!("Starting job manager (press Ctrl+C to stop)");
    let handle = manager.start()?;
    handle.shutdown_on_signal().await?;

    tracing::info!("Job manager stopped");
    Ok(())
}
