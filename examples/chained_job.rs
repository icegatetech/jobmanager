// This example demonstrates initial tasks whose order is declared, not built at runtime.
// All three are created with every iteration; `chain` makes each wait for the previous one, so
// several workers still run them strictly in order.

#![allow(missing_docs)]

mod support;

use jobmanager::prelude::*;

/// Builds an executor that logs its start and finish, so the run order is visible in the log.
fn step(name: &'static str) -> std::sync::Arc<dyn TaskExecutor> {
    task_fn(move |ctx| async move {
        tracing::info!("[{}] Started, task_id: {}", name, ctx.id());
        tokio::time::sleep(Duration::from_millis(200)).await;
        tracing::info!("[{}] Finished.", name);
        Ok(().into())
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    tracing::info!("Starting chained job example");

    let manager = JobsManager::builder()
        .s3(support::build_s3_config("chained"))
        .workers(3)
        .job("chained job", |j| {
            // Without an interval the next iteration would start the moment this one ends.
            j.every(Duration::from_secs(10));
            let extract = j.add_task(TaskDefinition::new("extract", Duration::from_secs(5)), step("Extract"));
            let transform = j.add_task(
                TaskDefinition::new("transform", Duration::from_secs(5)),
                step("Transform"),
            );
            let load = j.add_task(TaskDefinition::new("load", Duration::from_secs(5)), step("Load"));
            j.chain(&[extract, transform, load]);
            // An arbitrary graph is spelled out with `depends_on` instead:
            // j.depends_on(load, &[extract, transform]);
        })
        .build()
        .await?;

    tracing::info!("Starting manager with 3 workers (press Ctrl+C to stop)...");
    let handle = manager.start()?;
    handle.shutdown_on_signal().await?;

    tracing::info!("Manager stopped");
    Ok(())
}
