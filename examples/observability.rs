// Where the crate's measurements go, and how a fan-out stays correlated.
//
// Two independent things:
//
// 1. `MetricsSink` - nothing is recorded until a sink is registered with `.metrics(..)`. This one
//    logs, so every measurement is readable without a metrics backend. In production register
//    `OtelMetrics` instead, enabling the `metrics-otel` feature for it:
//
//        .metrics(Arc::new(OtelMetrics::new(&opentelemetry::global::meter("my-service"))))
//
//    Storage operations and cache hits are recorded per call and would drown the rest, so this sink
//    logs those three at `debug` - under this example's own target, not the crate's. The default
//    filter shows the other four; run it with `RUST_LOG=info,observability=debug` to see all seven.
//
// 2. Correlation - a task created at runtime knows nothing about the task that created it, so a
//    parent that wants its children's logs to join up puts its own correlation id in their payload
//    and each child logs under it.

#![allow(missing_docs)]

mod support;

use std::sync::Arc;
use std::time::Duration as StdDuration;

use jobmanager::prelude::*;
use jobmanager::{JobStatus, MetricsSink, TaskStatus};
use serde::{Deserialize, Serialize};
use tracing::Instrument;
use uuid::Uuid;

const PLAN_TASK_CODE: &str = "plan";
const WORK_TASK_CODE: &str = "work";
const WORK_TASK_COUNT: u32 = 3;
/// Work a child task does inside its span, so the span covers an await point instead of dodging one.
const WORK_DURATION: Duration = Duration::from_millis(200);

/// Records every measurement to the log.
///
/// Only the methods worth seeing are overridden; the trait's remaining methods keep their empty
/// default bodies, which is why a sink never has to implement all seven.
struct LoggingMetrics;

impl MetricsSink for LoggingMetrics {
    fn record_job_iteration_complete(&self, code: &JobCode, status: &JobStatus, duration: StdDuration) {
        tracing::info!(
            metric = "job_iteration",
            job = %code,
            status = %status,
            millis = duration.as_millis(),
        );
    }

    fn record_task_processed(
        &self,
        job_code: &JobCode,
        task_code: &TaskCode,
        status: &TaskStatus,
        duration: StdDuration,
    ) {
        tracing::info!(
            metric = "task_processed",
            job = %job_code,
            task = %task_code,
            status = %status,
            millis = duration.as_millis(),
        );
    }

    fn record_storage_operation(&self, operation: &str, status: &str, duration: StdDuration) {
        tracing::debug!(
            metric = "storage_operation",
            operation,
            status,
            millis = duration.as_millis(),
        );
    }

    fn record_cache_hit(&self, method: &str) {
        tracing::debug!(metric = "cache_hit", method);
    }

    fn record_cache_miss(&self, method: &str) {
        tracing::debug!(metric = "cache_miss", method);
    }

    fn record_task_stolen(&self, job_code: &JobCode, task_code: &TaskCode, phase: &'static str) {
        tracing::warn!(metric = "task_stolen", job = %job_code, task = %task_code, phase);
    }

    fn record_save_conflict_retry(&self, job_code: &JobCode, phase: &'static str) {
        tracing::warn!(metric = "save_conflict_retry", job = %job_code, phase);
    }
}

/// What the planning task hands each child, so the child's logs join the parent's.
#[derive(Serialize, Deserialize)]
struct WorkInput {
    correlation_id: String,
    work_index: u32,
}

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    let job_code = JobCode::new("observability");
    let manager = JobsManager::builder()
        .s3(support::build_run_scoped_s3_config("observability"))
        .workers(3)
        .metrics(Arc::new(LoggingMetrics))
        .job(job_code.clone(), |j| {
            j.max_iterations(1);
            j.add_task(
                TaskDefinition::new(PLAN_TASK_CODE, Duration::from_secs(30)),
                task_fn(plan_work),
            );
            j.add_task_executor(WORK_TASK_CODE, task_fn(run_work));
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    tracing::info!("example finished");
    Ok(())
}

/// Fans out work under a correlation id the children carry in their input.
async fn plan_work(ctx: TaskContext) -> TaskResult {
    let correlation_id = Uuid::new_v4().to_string();
    tracing::info!(%correlation_id, "planning under a fresh correlation id");

    for work_index in 0..WORK_TASK_COUNT {
        let input = WorkInput {
            correlation_id: correlation_id.clone(),
            work_index,
        };
        let definition =
            TaskDefinition::new(WORK_TASK_CODE, Duration::from_secs(30)).with_input(serde_json::to_vec(&input)?);
        ctx.job().add_task(definition)?;
    }

    Ok(().into())
}

/// Logs under the correlation id it was handed, so a search on that id returns the whole fan-out.
///
/// The span is attached with [`Instrument::instrument`] rather than entered with a guard from
/// `Span::enter`. That guard is bound to the thread, not to the future: held across an `.await` it
/// would leave the span entered on whatever the runtime polls next, and would make the future
/// `!Send` - which a task executor has to be.
async fn run_work(ctx: TaskContext) -> TaskResult {
    let input: WorkInput = serde_json::from_slice(ctx.input())?;
    let span = tracing::info_span!("work", correlation_id = %input.correlation_id);

    async move {
        tracing::info!(work_index = input.work_index, "working");
        tokio::time::sleep(WORK_DURATION).await;
        tracing::info!(work_index = input.work_index, "work finished");
        Ok(().into())
    }
    .instrument(span)
    .await
}
