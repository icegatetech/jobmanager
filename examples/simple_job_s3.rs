// Minimal example: one job with one task, state kept in an S3-compatible store.

#![allow(missing_docs)]

mod harness;

use jobmanager::prelude::*;

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing();

    tracing::info!("Starting simple job example");

    let manager = JobsManager::builder()
        .s3(harness::build_s3_config("simple-json"))
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
