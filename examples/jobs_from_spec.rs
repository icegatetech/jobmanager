// One job per entry in a spec table.
//
// `job()` takes the builder by value and returns it, so several jobs are added by reassigning it in
// a loop rather than by calling a method on a mutable borrow. Everything the per-job closure needs
// is copied out of the spec first: the closure outlives the loop iteration that created it.

#![allow(missing_docs)]

mod support;

use jobmanager::prelude::*;

const TASK_CODE: &str = "scan";

/// Static identity of one job. `Copy` so the closure captures it by value without lifetime juggling.
#[derive(Clone, Copy)]
struct TableJobSpec {
    /// Code the job is registered under.
    job_code: &'static str,
    /// Table this job scans.
    table: &'static str,
    /// How long to wait between iterations of this job.
    scan_interval: Duration,
}

const TABLE_JOB_SPECS: &[TableJobSpec] = &[
    TableJobSpec {
        job_code: "scan_logs",
        table: "logs",
        scan_interval: Duration::from_secs(2),
    },
    TableJobSpec {
        job_code: "scan_spans",
        table: "spans",
        scan_interval: Duration::from_secs(3),
    },
    TableJobSpec {
        job_code: "scan_metrics",
        table: "metrics",
        scan_interval: Duration::from_secs(5),
    },
];

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    let mut builder = JobsManager::builder()
        .s3(support::build_run_scoped_s3_config("jobs-from-spec"))
        .workers(3);

    for spec in TABLE_JOB_SPECS {
        let spec = *spec;
        builder = builder.job(spec.job_code, move |j| {
            j.every(spec.scan_interval);
            j.max_iterations(2);
            j.add_task(
                TaskDefinition::new(TASK_CODE, Duration::from_secs(30)).with_input(spec.table.as_bytes().to_vec()),
                task_fn(scan_table),
            );
        });
    }

    let manager = builder.build().await?;
    let handle = manager.start()?;

    // A wait reports an iteration that finished before it was called, so a job that ran out while an
    // earlier wait was still running is still seen - waiting one after another loses nothing.
    for spec in TABLE_JOB_SPECS {
        let job_code = JobCode::new(spec.job_code);
        handle.wait_for_job_completion(&job_code).await?;
        tracing::info!(%job_code, "job ran out its iterations");
    }

    handle.shutdown().await?;
    tracing::info!("example finished");
    Ok(())
}

/// Scans whichever table its input names.
async fn scan_table(ctx: TaskContext) -> TaskResult {
    let table = String::from_utf8_lossy(ctx.input()).to_string();
    tokio::time::sleep(Duration::from_millis(100)).await;
    tracing::info!(%table, "scanned table");
    Ok(().into())
}
