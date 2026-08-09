// A typed model crossing the opaque bytes the library stores.
//
// A task's input and output are `Vec<u8>` - the crate never looks inside them. Serde is what turns a
// model into those bytes and back: `to_vec` when a definition is built or a task completes,
// `from_slice` when a task reads what it was given.
//
// Nothing here reads the stored output. Handing a payload from one task to the next is a separate
// mechanism, shown by `simple_sequence_job` (a task creating its successor) and `fan_out_join` (a
// join task reading its dependencies' outputs).

#![allow(missing_docs)]

mod harness;

use jobmanager::prelude::*;
use serde::{Deserialize, Serialize};

const TASK_CODE: &str = "record";

/// What the task is given, serialized into its input before the job is built.
#[derive(Serialize, Deserialize, Debug)]
struct TaskInput {
    source: String,
    value: i32,
}

/// What the task reports, serialized into its output when it completes.
#[derive(Serialize, Deserialize, Debug)]
struct TaskOutput {
    source: String,
    value: i32,
    recorded_at: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    harness::init_tracing();

    tracing::info!("Starting JSON model job example");

    // Built outside the job closure: the closure cannot return the serialization error.
    let payload = serde_json::to_vec(&TaskInput {
        source: "sensor-a".to_string(),
        value: 42,
    })?;

    let job_code = JobCode::new("JSON model job");
    let manager = JobsManager::builder()
        .s3(harness::build_run_scoped_s3_config("json-model"))
        .job(job_code.clone(), |j| {
            // One iteration is enough to show the shape, and it lets the example exit by itself.
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new(TASK_CODE, Duration::from_secs(5)).with_input(payload),
                task_fn(record_reading),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tracing::info!("Manager started. Waiting for the job to run out its iterations...");
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!("Example finished.");
    Ok(())
}

/// Decodes the model it was given and completes with a model of its own.
async fn record_reading(ctx: TaskContext) -> TaskResult {
    let reading: TaskInput = serde_json::from_slice(ctx.input())?;
    tracing::info!(?reading, "decoded the model out of the task input");

    let recorded_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_secs();
    let report = TaskOutput {
        source: reading.source,
        value: reading.value,
        recorded_at,
    };

    tracing::info!(?report, "completing with the model as the task output");
    // Returning the payload is what closes the task: the worker stores it.
    Ok(TaskOutcome::Completed(serde_json::to_vec(&report)?))
}
