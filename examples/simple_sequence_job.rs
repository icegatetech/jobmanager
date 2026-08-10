// This example demonstrates a job with two sequential tasks.
// The first task 'first_step' simulates intermittent failures.
// Upon success, it creates the 'second_step' task and hands it its input.

#![allow(missing_docs)]

mod harness;

use jobmanager::prelude::*;
use rand::Rng;

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing();

    tracing::info!("Starting simple sequence job example");

    let manager = JobsManager::builder()
        .s3(harness::build_s3_config("simple-sequence"))
        .workers(5)
        .poll_interval(Duration::from_millis(500))
        .job("simple sequence job", |j| {
            // Started with every iteration.
            j.add_task(
                TaskDefinition::new("first_step", Duration::from_secs(5)),
                task_fn(|ctx| async move {
                    tracing::info!("[FirstStep] Started, task_id: {}", ctx.id());

                    // Simulate work
                    tokio::time::sleep(Duration::from_millis(500)).await;

                    // Simulate flaky failure (30% chance)
                    if rand::rng().random_bool(0.3) {
                        tracing::warn!("[FirstStep] Random simulated failure");
                        return Err("random simulated failure".into());
                    }

                    tracing::info!("[FirstStep] Work completed successfully. Scheduling next step.");

                    // Create the second task, handing it the payload it should work on.
                    ctx.job().add_task(
                        TaskDefinition::new("second_step", Duration::from_secs(5))
                            .with_input(b"data from step 1".to_vec()),
                    )?;

                    Ok(b"done".to_vec().into())
                }),
            );
            // Only an executor: the task itself is created at runtime by `first_step`.
            j.add_task_executor(
                "second_step",
                task_fn(|ctx| {
                    let input = String::from_utf8_lossy(ctx.input()).to_string();

                    async move {
                        tracing::info!("[SecondStep] Started, input: {}", input);

                        // Simulate work
                        tokio::time::sleep(Duration::from_millis(200)).await;

                        tracing::info!("[SecondStep] Finished.");
                        Ok(().into())
                    }
                }),
            );
        })
        .build()
        .await?;

    tracing::info!("Starting manager with 5 workers (press Ctrl+C to stop)...");
    let handle = manager.start()?;
    handle.shutdown_on_signal().await?;

    tracing::info!("Manager stopped");
    Ok(())
}
