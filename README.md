# jobmanager

Distributed job and task manager for Rust, with job state kept in S3.

Workers coordinate through conditional writes on the object store itself — no ZooKeeper, no
etcd, no database. If you already have S3 (or MinIO, RustFS, or any S3-compatible store), you
have everything this needs.

## Model

A **job** is a named unit of work made of one or more **tasks**. A **worker** polls storage for
jobs that have runnable tasks, executes one task at a time, and writes the updated job back.

Each job is polled on its own schedule: `JobsManagerBuilder::poll_interval` sets the interval a job
with work to do is polled at, and a job waiting for its next iteration is not polled until the
moment that iteration is due — set per job with `JobBuilder::every` or moved by an executor.

```
JobsManager ──┬── Worker ──┐
              ├── Worker ──┼──► Storage ──► S3 object per job
              └── Worker ──┘
```

Several workers may run different tasks of the same job concurrently. The whole state of a job
lives in a single object, and every write is conditional on the object's `ETag`: a worker that
loses the race gets an error instead of silently clobbering someone else's update. That is the
entire coordination mechanism.

Tasks can declare dependencies on each other, carry an input and an output payload, and have a
deadline. A job can repeat: it runs iterations on a schedule, and an executor can move the next
iteration earlier or later.

Two limits bound a task, and either one ends it. The **attempt budget**, set with
`TaskDefinition::with_max_attempts` and five by default, is spent by refusals of the executor — an
error or a panic. The **maximum lifetime**, set with `TaskDefinition::with_max_lifetime` and five
deadlines by default, runs from the task's first start and is what bounds takeovers: a worker that
takes an expired task over spends no attempt, so a task whose workers keep dying is retried until
its lifetime runs out rather than after a handful of deaths. That bound is absolute: every deadline
the task is given is capped by it, so the executor holding the task is signalled no later than the
lifetime passes, the task is failed on the first pick afterwards, and a result returned past the
lifetime is refused rather than stored. Once either limit is spent the task is
terminal and its iteration ends as failed — which is not the end of the job: the next iteration is
planned from scratch, so a permanently failing task delays work instead of blocking it forever.

Old iterations do not pile up: each job keeps its most recent ones, set per job with
`JobBuilder::keep_iterations`, and the rest are deleted in the background. Turn it off with
`.no_cleanup()`. Only jobs the builder knows about are cleaned, so renaming a job's code leaves its
old state under the previous prefix for you to delete. Switching `JobStateCodecKind` on a bucket
that already holds state leaves everything written under the previous codec in place: those objects
are no longer recognized as iterations, so delete them yourself. Cleanup is best-effort by design —
a sweep that is dropped or fails leaves the tail in place until the next start-up and never delays,
blocks, or fails an iteration.

## Quick start

The examples run against a local S3-compatible store; [`examples/README.md`](examples/README.md) has
the commands that bring it up and run one.

The shape of it:

```rust
use jobmanager::prelude::*;
use jobmanager::{JobStateCodecKind, S3StorageConfig};

let manager = JobsManager::builder()
    .s3(s3_config)
    .workers(4)
    .job("simple job", |j| {
        j.add_task(
            TaskDefinition::new("my task code", Duration::from_secs(5)),
            task_fn(|_ctx| async move { Ok(b"done".to_vec().into()) }),
        );
    })
    .build()
    .await?;

let handle = manager.start()?;
handle.shutdown_on_signal().await?;
```

`JobsManager::builder()` is the only way in: it takes the storage backend, the jobs, and their
executors, and validates the description — its rustdoc lists what `build()` rejects, all of it
before the first iteration rather than during one.

An executor is anything implementing `TaskExecutor`; wrap an async closure with `task_fn`, or pass
an `Arc<YourStruct>` straight in. Returning a payload is what closes the task, so it cannot be left
hanging by forgetting to complete it; return `TaskOutcome::Deferred` to keep manual control.

The handle owns the pool — dropping it aborts every worker — so it is `#[must_use]`. Besides
`shutdown`, it can wait for work: `wait_for_iteration_completion` and `wait_for_job_completion`
block until the named job reaches the point they name, so nothing has to poll storage or sleep.
What each of them guarantees is on the method.

[`examples/`](examples/) holds a worked example of each shape the crate is used in — a fan-out with a
join task, executors that hold dependencies, several processes over one bucket, deadline takeovers,
attempt budgets, and adaptive scheduling. See [its README](examples/README.md) for the catalogue.

## Storage backends

| Builder call | Backend |
|---|---|
| `.s3(config)` | production; one object per job, conditional writes for concurrency, read cache in front |
| `.s3(config).no_cache()` | the same without the cache |
| `.in_memory()` | tests and examples; conditional writes as above, but only the current iteration and nothing survives the process |

The backends themselves are not part of the public API — the builder constructs and wires them,
including the registry they read job settings from.

Job state serializes as either JSON (readable, debuggable) or CBOR (compact) — pick with
`JobStateCodecKind`.

## Observability

Job and task durations, storage latency, cache hit rate, task takeovers, and save-conflict retries
are written to a `MetricsSink`. Nothing is recorded until one is registered with
`.metrics(...)`. `OtelMetrics` implements it on top of OpenTelemetry and lives behind the
`metrics-otel` feature, which is off by default — it takes a `Meter` you own, so it is only useful
to a consumer that already depends on `opentelemetry`:

```toml
jobmanager = { git = "...", features = ["metrics-otel"] }
```

Without the feature the `opentelemetry` dependency is absent from your tree while every measurement
still reaches a sink of your own.

## Status

Version 0.1.0. The crate is extracted from a production system and is used there, but the public
API is not yet stable and it is not published to crates.io. Depend on it by git revision:

```toml
jobmanager = { git = "https://github.com/icegatetech/jobmanager", rev = "..." }
```

## Development

See [CONTRIBUTING.md](CONTRIBUTING.md). Tests need Docker — they start a real S3-compatible
container.

## License

Apache-2.0. See [LICENSE](LICENSE).
