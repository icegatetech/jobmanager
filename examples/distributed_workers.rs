// Two processes, one bucket prefix, no lock service.
//
// Run it twice, in two terminals:
//
//   cargo run --example distributed_workers -- --node a
//   cargo run --example distributed_workers -- --node b
//
// Both processes poll the same job. Each task is executed by exactly one of them, because every save
// is conditional on the job object's ETag and the loser of a race gets an error instead of silently
// overwriting the winner. Watch the `node=` field: the work tasks are split across the two.
//
// This example runs until interrupted - press Ctrl+C in each terminal.

#![allow(missing_docs)]

mod support;

use jobmanager::prelude::*;

const PLAN_TASK_CODE: &str = "plan";
const WORK_TASK_CODE: &str = "work";

/// Enough tasks per iteration that the split across nodes is visible rather than a coin flip.
const TASKS_PER_ITERATION: u32 = 20;

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    // Identity of this process, used only for logging: the pool does not need to be told who its
    // peers are, which is the whole point.
    let node = support::find_argument_value("--node").unwrap_or_else(|| "a".to_string());
    tracing::info!(%node, "starting node");

    let manager = JobsManager::builder()
        // Both nodes must share the prefix - a different prefix is a different job.
        .s3(support::build_s3_config("distributed-workers"))
        .workers(2)
        .poll_interval(Duration::from_millis(300))
        .job("distributed job", move |j| {
            j.every(Duration::from_secs(5));
            j.add_task(
                TaskDefinition::new(PLAN_TASK_CODE, Duration::from_secs(30)),
                task_fn(plan_work),
            );
            j.add_task_executor(
                WORK_TASK_CODE,
                task_fn(move |ctx: TaskContext| {
                    // `task_fn` takes an `Fn`, so the closure keeps the name and each call clones
                    // it into the future it returns.
                    let node = node.clone();
                    async move { run_work(&node, &ctx).await }
                }),
            );
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.shutdown_on_signal().await?;

    tracing::info!("node stopped");
    Ok(())
}

/// Creates the iteration's work tasks. Whichever node wins the planning task creates all of them;
/// executing them is then split by whoever polls first.
async fn plan_work(ctx: TaskContext) -> TaskResult {
    for task_index in 0..TASKS_PER_ITERATION {
        let definition = TaskDefinition::new(WORK_TASK_CODE, Duration::from_secs(30))
            .with_input(task_index.to_string().into_bytes());
        ctx.job().add_task(definition)?;
    }
    tracing::info!(task_count = TASKS_PER_ITERATION, "planned work for this iteration");
    Ok(().into())
}

/// Does one unit of work, tagged with the node that picked it up.
async fn run_work(node: &str, ctx: &TaskContext) -> TaskResult {
    let task_index = String::from_utf8_lossy(ctx.input()).to_string();
    tokio::time::sleep(Duration::from_millis(150)).await;
    tracing::info!(%node, %task_index, "executed work task");
    Ok(().into())
}
