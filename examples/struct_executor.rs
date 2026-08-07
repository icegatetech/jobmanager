// A task executor that is a struct, not a closure.
//
// `task_fn` is for work that needs nothing but its input. Anything expensive to build - an HTTP
// client, a connection pool, a catalog handle - is held by a struct that implements `TaskExecutor`,
// and `Arc<Self>` coerces to `Arc<dyn TaskExecutor>` on its own. Two executors sharing one
// dependency is the normal case, so both hold the same `Arc`.

#![allow(missing_docs)]

mod support;

use std::sync::Arc;

use async_trait::async_trait;
use jobmanager::prelude::*;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

const FETCH_TASK_CODE: &str = "fetch_rates";
const PUBLISH_TASK_CODE: &str = "publish_rates";

#[derive(Clone, Serialize, Deserialize)]
struct Rate {
    model: String,
    usd_per_1m: f64,
}

/// The shared dependency: stands in for a client plus the store it reads and writes.
///
/// A real one would hold a `reqwest::Client` and a catalog handle; what matters here is that it is
/// built once and shared, rather than rebuilt per execution.
struct RateStore {
    /// What an upstream fetch would return. Built with the store, which is the cost a struct
    /// executor exists to pay once.
    upstream: Vec<Rate>,
    published: Mutex<Vec<Rate>>,
}

impl RateStore {
    fn new() -> Self {
        Self {
            upstream: vec![
                Rate {
                    model: "opus".to_string(),
                    usd_per_1m: 15.0,
                },
                Rate {
                    model: "sonnet".to_string(),
                    usd_per_1m: 3.0,
                },
            ],
            published: Mutex::new(Vec::new()),
        }
    }

    /// Stands in for an upstream fetch.
    fn fetch_rates(&self) -> Vec<Rate> {
        self.upstream.clone()
    }

    /// Records what was published and returns the running total.
    fn publish_rates(&self, rates: Vec<Rate>) -> usize {
        let mut published = self.published.lock();
        published.extend(rates);
        published.len()
    }

    /// How much has been published so far, for the caller that outlives the pool.
    fn published_count(&self) -> usize {
        self.published.lock().len()
    }
}

/// Fetches the rate card and hands it to the publishing task.
struct FetchRatesExecutor {
    store: Arc<RateStore>,
    publish_timeout: Duration,
}

#[async_trait]
impl TaskExecutor for FetchRatesExecutor {
    async fn execute(&self, ctx: TaskContext) -> TaskResult {
        let rates = self.store.fetch_rates();
        tracing::info!(rate_count = rates.len(), "fetched rates");

        // The follow-up task carries the payload, so the publishing executor needs no store read of
        // its own to learn what this one produced.
        let definition =
            TaskDefinition::new(PUBLISH_TASK_CODE, self.publish_timeout).with_input(serde_json::to_vec(&rates)?);
        ctx.job().add_task(definition)?;

        Ok(().into())
    }
}

/// Publishes whatever it was handed, through the same store the fetching executor holds.
struct PublishRatesExecutor {
    store: Arc<RateStore>,
}

#[async_trait]
impl TaskExecutor for PublishRatesExecutor {
    async fn execute(&self, ctx: TaskContext) -> TaskResult {
        let rates: Vec<Rate> = serde_json::from_slice(ctx.input())?;
        let published_total = self.store.publish_rates(rates);
        tracing::info!(published_total, "published rates");
        Ok(().into())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    support::init_tracing();

    // Built once, before the pool: this is the point of a struct executor.
    let store = Arc::new(RateStore::new());
    let fetch = Arc::new(FetchRatesExecutor {
        store: Arc::clone(&store),
        publish_timeout: Duration::from_secs(30),
    });
    let publish = Arc::new(PublishRatesExecutor {
        store: Arc::clone(&store),
    });

    let job_code = JobCode::new("struct-executor");
    let manager = JobsManager::builder()
        .s3(support::build_run_scoped_s3_config("struct-executor"))
        .workers(2)
        .job(job_code.clone(), |j| {
            j.max_iterations(2);
            j.every(Duration::from_secs(2));
            j.add_task(TaskDefinition::new(FETCH_TASK_CODE, Duration::from_secs(30)), fetch);
            j.add_task_executor(PUBLISH_TASK_CODE, publish);
        })
        .build()
        .await?;

    let handle = manager.start()?;
    handle.wait_for_job_completion(&job_code).await?;
    handle.shutdown().await?;

    // The store outlived the pool, so what the executors did is observable from here - which is
    // also how a test asserts on an executor's effect.
    tracing::info!(published_total = store.published_count(), "example finished");
    Ok(())
}
