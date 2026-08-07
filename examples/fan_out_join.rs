// Fan-out/fan-in: one planning task splits the work, creates a task per chunk, and creates a join
// task that waits for all of them.
//
// The join task is created before any chunk task has finished: `with_dependencies` puts it in
// `Blocked` until every id it names reaches `Completed`. It then reads those tasks' outputs through
// its own dependency list, which is the only place the ids survive - a task cannot ask "who fanned
// me out".

#![allow(missing_docs)]

mod support;

use jobmanager::prelude::*;
use serde::{Deserialize, Serialize};

/// Splits the input and fans out one `PROCESS_TASK_CODE` task per chunk.
const PLAN_TASK_CODE: &str = "plan";
/// Processes one chunk. Created at runtime, so only its executor is declared up front.
const PROCESS_TASK_CODE: &str = "process";
/// Waits for every chunk and sums their outputs.
const COLLECT_TASK_CODE: &str = "collect";

const CHUNK_COUNT: u64 = 4;
const ROWS_PER_CHUNK: u64 = 10;

/// What a chunk task is told to work on.
#[derive(Serialize, Deserialize)]
struct ChunkInput {
    first_offset: u64,
    row_count: u64,
}

/// What a chunk task reports back. The join task reads this off each dependency's output.
#[derive(Serialize, Deserialize)]
struct ChunkOutput {
    rows_processed: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    let job_code = JobCode::new("fan-out-join");
    let manager = JobsManager::builder()
        .s3(support::build_run_scoped_s3_config("fan-out-join"))
        .workers(4)
        .job(job_code.clone(), |j| {
            // One iteration is enough to show the shape, and it lets the example exit by itself.
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new(PLAN_TASK_CODE, Duration::from_secs(30)),
                task_fn(plan_chunks),
            );
            j.add_task_executor(PROCESS_TASK_CODE, task_fn(process_chunk));
            j.add_task_executor(COLLECT_TASK_CODE, task_fn(collect_chunk_outputs));
        })
        .build()
        .await?;

    let handle = manager.start()?;
    tracing::info!("waiting for the fan-out to drain");
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!("example finished");
    Ok(())
}

/// Fans out a chunk task per slice of the input, then a join task naming all of them.
async fn plan_chunks(ctx: TaskContext) -> TaskResult {
    let mut chunk_task_refs = Vec::with_capacity(usize::try_from(CHUNK_COUNT)?);
    for chunk_index in 0..CHUNK_COUNT {
        let input = ChunkInput {
            first_offset: chunk_index * ROWS_PER_CHUNK,
            row_count: ROWS_PER_CHUNK,
        };
        let definition =
            TaskDefinition::new(PROCESS_TASK_CODE, Duration::from_secs(30)).with_input(serde_json::to_vec(&input)?);
        chunk_task_refs.push(ctx.job().add_task(definition)?);
    }

    tracing::info!(chunk_count = chunk_task_refs.len(), "fanned out chunk tasks");

    let collect = TaskDefinition::new(COLLECT_TASK_CODE, Duration::from_secs(30)).with_dependencies(chunk_task_refs);
    ctx.job().add_task(collect)?;

    Ok(().into())
}

/// Processes one chunk and reports how much it handled.
async fn process_chunk(ctx: TaskContext) -> TaskResult {
    let input: ChunkInput = serde_json::from_slice(ctx.input())?;
    tracing::info!(first_offset = input.first_offset, "processing chunk");

    tokio::time::sleep(Duration::from_millis(200)).await;

    let output = ChunkOutput {
        rows_processed: input.row_count,
    };
    Ok(TaskOutcome::Completed(serde_json::to_vec(&output)?))
}

/// Sums every chunk's output. The ids come from this task's own dependency list.
async fn collect_chunk_outputs(ctx: TaskContext) -> TaskResult {
    let mut rows_total: u64 = 0;
    for chunk_task_id in ctx.task().depends_on() {
        let chunk_task = ctx.job().get_task(chunk_task_id)?;
        // An empty output is ambiguous on its own, so the completion flag is what distinguishes a
        // task that finished with nothing to say from one that has not finished at all.
        if !chunk_task.is_completed() {
            return Err(format!("chunk task {chunk_task_id} is not completed").into());
        }
        let output: ChunkOutput = serde_json::from_slice(chunk_task.get_output())?;
        rows_total += output.rows_processed;
    }

    tracing::info!(rows_total, "collected every chunk");
    Ok(().into())
}
