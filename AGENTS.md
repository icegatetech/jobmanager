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
| `src/storage/`   | Persistence: the `Storage` trait, the stored representation every backend maps through, and the backends themselves            |
| `src/infra/`     | Cross-cutting utilities: retry policy, metrics                                                                                 |
| `src/tests/`     | Integration tests that need `pub(crate)` access — see [docs/tests.md](docs/tests.md)                                           |

### Dependency rules

- `core` holds the domain state machine and **MUST NOT** gain a dependency on a concrete backend,
  on `Worker`, or on any I/O. State rules belong in `Job`/`Task`, never in the worker that drives
  them.
- A domain rule lives in `core` and nowhere else. `storage` and `execution` **MUST NOT** re-derive a
  rule `Job`/`Task` already owns: a backend maps state, it does not decide it.
- **The domain does not know it is stored.** `core` is written as if a job and its tasks live on
  whole: which fields a backend writes, drops, or reconstructs is `storage`'s business and appears
  nowhere in `core` — not in a method, not in a doc comment justifying a rule. A domain rule
  **MUST NOT** rest on "this field is not persisted"; state it in the domain's own terms (what a
  value belongs to, and how long that thing lives), and let `storage` decide separately what it keeps.
  The corollary belongs to `storage` alone: a field it drops **MUST** be dropped by every backend
  alike, which is what the single stored representation is for.
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
- **Never assume a deadline stops an executor.** The deadline cancels the token of the execution
  holding the task and nothing more; an executor that does not select on that token keeps running
  while another worker takes the task over, so two executors running one task is a legal state.
- **Never charge a takeover to the attempt budget.** The budget answers for refusals of the executor;
  what bounds takeovers is the task's maximum lifetime, counted from its first start. Charging every
  start was what made a task whose workers kept dying terminally failed without it ever having
  refused anything.
- **Never let work outlast the maximum lifetime.** It is the absolute bound, not one approached a
  deadline at a time: a start caps its deadline at it, a task past it is failed whatever its deadline
  says, and a result returned past it is refused. A rule that waited for the next deadline let a task
  run almost a full deadline longer than the limit it was given.
- **Never let one execution outlive its own rollback.** A failed execution drops the tasks it
  created, so an execution that failed its own task registers nothing more — otherwise the part
  registered afterwards survives the failure that dropped its siblings. The bound is the failure and
  not the end of the execution: an execution that completed its own task rolled nothing back, and
  goes on planning the work its result hands over.
- **Never lose the result of an execution to a task its executor already resolved.** An executor may
  complete or fail its own task and still end in error; the state it wrote is what gets persisted.
  Failing such a task a second time is refused by the domain, and a worker that let that refusal
  through saved nothing at all — leaving the task `Started` in storage for takeover after takeover to
  repeat work that was already done.
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
- When the change touches a domain structure, say which of its fields the stored representation
  carries and which it leaves out, and why — that call is made in `storage`, not in the domain type.
  A field left out **MUST** behave the same under every `Storage` implementation.
- Cover the behavior with tests — read [docs/tests.md](docs/tests.md) first.
- The crate is pre-1.0 and unpublished, so a breaking API change is allowed — but state it, do not
  slip it in.

## Before finishing

- Run targeted tests for the affected functionality, and report which test commands were run and
  which required tests were not.
- Re-read every comment the change added and delete the ones that fail the acid test and the budget
  in [RUST.md](docs/RUST.md). This pass is a separate step because a comment that restates the code
  is written far more easily than it is noticed afterwards.
- Ensure each file ends with a single trailing newline.

@docs/RUST.md
