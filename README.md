# jobmanager

Distributed job and task manager for Rust, with job state kept in S3.

Workers coordinate through conditional writes on the object store itself — no ZooKeeper, no
etcd, no database. If you already have S3 (or MinIO, RustFS, or any S3-compatible store), you
have everything this needs.

## Model

A **job** is a named unit of work made of one or more **tasks**. A **worker** polls storage for
jobs that have runnable tasks, executes one task at a time, and writes the updated job back.

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

Every task also has an attempt budget — five by default, set per task with
`TaskDefinition::with_max_attempts`. Every start spends one, whether the task failed or its
deadline expired and another worker took it over. Once the budget is gone the task is terminal:
it is never picked up again, tasks blocked behind it never run, and the job iteration ends as
failed. That is not the end of the job — the next iteration starts on its normal schedule and is
planned from scratch, so a permanently failing task delays work instead of blocking it forever.

## Quick start

Bring up an S3-compatible store:

```bash
make examples-infra-up
```

Then run an example:

```bash
cargo run --example simple_job
```

The shape of it:

```rust
// 1. Describe a task and the code that runs it.
let task_def = TaskDefinition::new(TaskCode::new("my task code"), Vec::new(), Duration::seconds(5))?;
let task_executor: TaskExecutorFn = Arc::new(|task, manager, _cancel_token| {
    let task_id = *task.id();
    Box::pin(async move { manager.complete_task(&task_id, b"done".to_vec()) })
});

// 2. Bind them into a job and register it.
let job_def = JobDefinition::new(JobCode::new("simple job"), vec![task_def], task_executors)?;
let job_registry = Arc::new(JobRegistry::new(vec![job_def])?);

// 3. Point at storage and start the worker pool.
let storage = S3Storage::new(s3_config, Arc::clone(&job_registry), Metrics::new_disabled()).await?;
let manager = JobsManager::new(
    Arc::new(storage),
    JobsManagerConfig::default(),
    Arc::clone(&job_registry),
    Metrics::new_disabled(),
)?;
let handle = manager.start()?;
```

Full versions live in [`examples/`](examples/): a plain job, the same job with CBOR-encoded
state, a job whose payloads are typed JSON models, and a sequence of dependent tasks.

## Storage backends

| Backend | Use |
|---|---|
| `S3Storage` | production; one object per job, conditional writes for concurrency |
| `CachedStorage` | wraps any backend, skips reads when the cached state is still current |
| `InMemoryStorage` | tests; holds a single job, nothing survives the process |

Job state serializes as either JSON (readable, debuggable) or CBOR (compact) — pick with
`JobStateCodecKind`.

## Observability

`Metrics` records job and task durations, storage latency, cache hit rate, task takeovers, and
save-conflict retries through OpenTelemetry. Pass `Metrics::new_disabled()` if you don't want
any of it.

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
