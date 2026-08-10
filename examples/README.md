# Examples

Every example runs against the S3-compatible store from the repository root:

```bash
make examples-infra-up      # RustFS on localhost:9000, bucket `jobs`
cargo run --example <name>
make examples-infra-down
```

Each example writes under its own bucket prefix, so they never read each other's state. Set
`RUST_LOG` to change the log filter.

`support/` holds the connection details and the tracing setup shared by all of them. It is a
directory without a `main.rs`, so cargo does not build it as an example of its own. An example that
caps its iterations nests one more path segment under its prefix, unique per run — otherwise its
second run would find the iteration budget already spent and wait forever.

## Start here

| Example | What it shows |
|---|---|
| [`simple_job`](simple_job.rs) | The smallest thing that runs: one job, one task. |
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
| [`distributed_workers`](distributed_workers.rs) | Two processes over one bucket prefix. Run it twice, with `-- --node a` and `-- --node b`. |
| [`observability`](observability.rs) | A `MetricsSink` of your own, and a correlation id carried through a fan-out. |
| [`payload_by_reference`](payload_by_reference.rs) | `TaskLimits`, and passing a key instead of the bytes. |
| [`testing_your_executor`](testing_your_executor.rs) | Driving a job on `.in_memory()` so an executor can be asserted on. Also runs as `cargo test --example testing_your_executor`. |

## Failure and timing

| Example | What it shows |
|---|---|
| [`idempotent_task`](idempotent_task.rs) | A deadline cancels nothing: the takeover, and the guard the side effect needs. |
| [`graceful_shutdown`](graceful_shutdown.rs) | Selecting on the cancellation token, and `TaskOutcome::Cancelled`. |
| [`attempt_budget`](attempt_budget.rs) | An attempt budget running out, and the replan that follows. |
| [`adaptive_schedule`](adaptive_schedule.rs) | `set_next_start_at` pulling the next iteration in or pushing it out. |

`simple_job`, `simple_job_cbor`, `simple_sequence_job`, `chained_job` and `distributed_workers` model
long-running services and stop on Ctrl+C; `graceful_shutdown` stops itself after a few seconds, and
the rest cap their iterations and exit once those are spent.
