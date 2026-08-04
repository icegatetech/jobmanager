# AGENTS.md

Instructions for anyone — human or agent — working in this repository. This file holds the rules for
**how to write code** here. Operational detail (commands, ports, credentials) lives in the
executable files and the README — this file points at them, never copies them.

## Operational commands

Do not reproduce these in prose — read the source of truth:

- Build / test / lint / audit / CI: the [`Makefile`](Makefile) targets.
- What CI actually runs: [`.github/workflows/ci.yml`](.github/workflows/ci.yml) and
  [`security-audit.yml`](.github/workflows/security-audit.yml).

### Tools

- Format with `make fmt-fix` (`cargo +nightly fmt`; `rustfmt.toml` uses nightly-only options).
- Lint with `make clippy` (`-D warnings`). Do not disable a lint crate-wide; a targeted `#[allow]`
  needs a comment saying why.
- `make ci` runs `check`, `fmt`, `clippy`, `test`, `audit` — run it only when asked.

### Running rules

- **Do not** run a full `make ci`, a release build, or a whole-crate `cargo test` without an
  explicit request — prefer a targeted `cargo test <name> -- --test-threads=1` for the code you
  touched.
- Never drop `--test-threads=1`; the reason is in the [`Makefile`](Makefile).
- Tests need a running Docker daemon. If it is unavailable, say which test command was not run —
  do not report the suite as passing.
- Format with `make fmt-fix`; plain `cargo fmt` silently ignores `rustfmt.toml`.

## Layers and where code goes

Place a change in the module that owns its responsibility. This is a single-crate library, so the
module tree *is* the architecture.

| Module           | Responsibility                                                                                                                 |
|------------------|--------------------------------------------------------------------------------------------------------------------------------|
| `src/core/`      | Domain: job and task state, status transitions, dependency and attempt rules, merging; the registry and the error types        |
| `src/execution/` | Orchestration: worker-pool lifecycle, the poll → pick → execute → save loop, and the `JobManager` surface an executor is given |
| `src/storage/`   | Persistence: the `Storage` trait and its backends                                                                              |
| `src/infra/`     | Cross-cutting utilities: retry policy, metrics                                                                                 |
| `src/tests/`     | Integration tests that need `pub(crate)` access — see [docs/tests.md](docs/tests.md)                                           |

### Dependency rules

- `core` holds the domain state machine and **MUST NOT** gain a dependency on a concrete backend,
  on `Worker`, or on any I/O. State rules belong in `Job`/`Task`, never in the worker that drives
  them.
- `core` does reference the *traits* of neighbouring layers (`JobDefinitionRegistry`, `JobManager`)
  and their error types. That seam exists; do not widen it.
- `storage` depends on `core` and `infra`, never on `execution`. A backend **MUST NOT** know that
  workers exist.
- `execution` is the only layer allowed to depend on everything below it.
- `infra` depends on `core` only for the identifier and status types it labels metrics with.

### Docs

- **A convention is only what is documented** in these three files. The mere presence of a pattern
  in the code is **NOT** a convention — someone may have committed junk. Do not justify a decision
  with "the existing code does X"; cite the documented rule, or propose adding one if it is missing.
- How a thing behaves is documented in **its own doc comment**, next to the code. `missing_docs` is
  denied, so this is enforced. Do not restate that behavior here or in the README — two copies drift.
- `README.md` explains the crate to someone using it; `CONTRIBUTING.md` explains how to build, test,
  and commit. Do not copy commands or contracts between them.
- `docs/` holds only cross-cutting policy ([RUST.md](docs/RUST.md), [tests.md](docs/tests.md)).
  Never add a `docs/<module>.md` — a deep-dive for a subsystem goes in a README beside its code.
- Design notes, specs, and plans are working artifacts: keep them in `.tmp/`, not in `docs/`.

## Invariants

Coordination between workers rests entirely on conditional writes to a single object per job
iteration. The mechanics are documented on the types themselves; what follows is only what a change
**MUST NOT** break, because breaking it fails silently rather than loudly.

- **Never write unconditionally.** Every save is an `If-Match`/`If-None-Match` `PUT`, and the `412`
  it can return is the only signal that another worker got there first. An unconditional `PUT`
  destroys a concurrent update with no error anywhere.
- **Never resolve a conflict by overwriting.** Re-read the stored state and merge into it; a merge
  carries only the tasks this worker created or is processing. A worker that lost a task drops its
  own result.
- **Never change the object key layout** without a migration story. The inverted `iter_num` is what
  makes the current iteration findable in one `LIST`, for every job already in storage.
- **Never assign a job status directly** — go through the transition check, so an illegal transition
  stays an error rather than a corrupted state.
- **Never assume a deadline cancels anything.** An expired task may be taken over while its original
  executor keeps running; two executors running the same task is a legal state.
- **Never carry failed tasks into the next iteration.** An iteration that cannot progress ends as
  failed and the next one replans from scratch — that is what stops a permanently failing task from
  blocking its dependents forever.
- **Never delete an iteration above the retention boundary**, which is
  `iter_num - iteration_retention` of the iteration a worker has already persisted. The boundary is
  strictly below the job's current iteration, so the newest state object is never deletable;
  deleting it would let `find_job_meta` miss the job and a worker recreate it from `iter_num = 1`.
- Job settings (`max_iterations`, `iteration_interval`, `TaskLimits`, `iteration_retention`) are
  re-read from the `JobDefinition` on every load, so they are changed in code, never by editing a
  stored object.

## Before a change

- Decide which module owns it. Domain rules go to `core`, I/O to `storage`, orchestration to
  `execution` — never the reverse.
- Say which invariant above the change touches, if any.
- Cover the behavior with tests — read [docs/tests.md](docs/tests.md) first.
- The crate is pre-1.0 and unpublished, so a breaking API change is allowed — but state it, do not
  slip it in.

## Before finishing

- Run targeted tests for the affected functionality, and report which test commands were run and
  which required tests were not.
- Ensure each file ends with a single trailing newline.

@docs/RUST.md
