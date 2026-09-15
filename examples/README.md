# Examples

The default environment is the S3-compatible store from the repository root, which every example
but the three named below runs against:

```bash
make examples-infra-up      # RustFS on localhost:9000, bucket `jobs`
cargo run --example <name>
make examples-infra-down
```

`simple_job_azure` and `simple_job_gcs` run against the emulator of their own provider that the same
compose file starts, and each backend is behind a feature the default set leaves out.

```bash
cargo run --example simple_job_azure --no-default-features --features storage-azure
cargo run --example simple_job_gcs --no-default-features --features storage-gcs
```

`testing_your_executor` is the third exception and needs no store at all: it runs on `.in_memory()`,
so neither the compose file nor a bucket is involved.

Each example writes under its own `state_prefix`, so they never read each other's state. Set
`RUST_LOG` to change the log filter.

`support/` holds the connection details and the tracing setup shared by all of them. It is a
directory without a `main.rs`, so cargo does not build it as an example of its own. An example that
caps its iterations nests one more path segment under its prefix, unique per run — otherwise its
second run would find the iteration budget already spent and wait forever.

## Start here

| Example | What it shows |
|---|---|
| [`simple_job_s3`](simple_job_s3.rs) | The smallest thing that runs: one job, one task. |
| [`simple_job_azure`](simple_job_azure.rs) | The same over an Azure Blob Storage container instead of an S3 bucket. |
| [`simple_job_gcs`](simple_job_gcs.rs) | The same over a Google Cloud Storage bucket instead of an S3 bucket. |
| [`simple_job_cbor`](simple_job_cbor.rs) | The same with CBOR-encoded job state instead of JSON. |
| [`json_model_job`](json_model_job.rs) | Typed payloads: a model decoded from a task's input, and a model returned as its output. |
| [`simple_sequence_job`](simple_sequence_job.rs) | A task that creates its successor at runtime and hands it an input. |
| [`chained_job`](chained_job.rs) | Initial tasks whose order is declared up front with `chain`. |

## Production shapes

| Example | What it shows |
|---|---|
| [`fan_out_join`](fan_out_join.rs) | Plan → a task per chunk → a join task that reads its dependencies' outputs. |
| [`struct_executor`](struct_executor.rs) | Executors as structs holding shared dependencies, rather than closures. |
| [`jobs_from_spec`](jobs_from_spec.rs) | One job per entry of a spec table, generated in a loop. |
| [`distributed_workers`](distributed_workers.rs) | Two processes over one `state_prefix`. Run it twice, with `-- --node a` and `-- --node b`. |
| [`observability`](observability.rs) | A `MetricsSink` of your own, and a correlation id carried through a fan-out. |
| [`payload_by_reference`](payload_by_reference.rs) | `TaskLimits`, and passing a key instead of the bytes. |
| [`testing_your_executor`](testing_your_executor.rs) | Driving a job on `.in_memory()` so an executor can be asserted on. Also runs as `cargo test --example testing_your_executor`. |

## Failure and timing

| Example | What it shows |
|---|---|
| [`idempotent_task`](idempotent_task.rs) | A deadline stops nobody: the takeover, and the guard the side effect needs. |
| [`graceful_shutdown`](graceful_shutdown.rs) | Selecting on the cancellation token, and `TaskOutcome::Cancelled`. |
| [`attempt_budget`](attempt_budget.rs) | An attempt budget running out, and the replan that follows. |
| [`terminal_failure`](terminal_failure.rs) | `TaskOutcome::TerminallyFailed`: a refusal no retry can turn into a success, executed once. |
| [`skipped_branch`](skipped_branch.rs) | `TaskOutcome::SkippedBranch`: a branch with no work in it, and the iteration that still completes. |
| [`degraded_dependency`](degraded_dependency.rs) | `with_dependency_tolerance`: starting on a dependency that failed for good, and reading what it left. |
| [`adaptive_schedule`](adaptive_schedule.rs) | `set_next_start_at` pulling the next iteration in or pushing it out. |

`simple_job_s3`, `simple_job_azure`, `simple_job_gcs`, `simple_job_cbor`, `simple_sequence_job`,
`chained_job` and `distributed_workers` model long-running services and stop on Ctrl+C;
`graceful_shutdown` stops itself after a few seconds, and the rest cap their iterations and exit
once those are spent.
