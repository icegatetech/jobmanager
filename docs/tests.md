# Testing

jobmanager coordinates concurrent workers through conditional writes on an object store. Its
correctness is almost entirely about **races, retries, deadlines, and restarts** — the parts that
look fine in a single-threaded happy path and fail in production. Tests protect observable behavior,
the public API, and the invariants in [AGENTS.md](../AGENTS.md); they are not added to mirror every
production function.

## Where tests live

- Inline `#[cfg(test)]` modules go at the **end of the source file** they test.
- Integration tests live in **`src/tests/`, not `tests/`** — they assert on `Job`, `Task`, and
  `Storage`, which are `pub(crate)`. Do not "fix" this by relocating them or by widening production
  visibility; the visibility boundary is the contract and the test location follows from it.
- The harness already exists — use it rather than rolling your own: `S3TestContainer` (starts and
  tears down the store), `ManagerEnv` (manager lifecycle, bounded wait, aborts workers on drop),
  `CountingStorage` (counts backend calls), `InMemoryStorage` (single job, counts reads),
  `init_tracing`.

## Choosing the boundary

Use the lowest boundary that still contains the behavior and its risk.

- **Unit** — pure state rules: transitions, dependency resolution, attempt accounting, pickup
  selection, merge decisions, key construction, delay calculation. No I/O. Use `Job::restore` /
  `Task::restore` to build a state no legal call sequence reaches (a task already at its cap).
- **Component** — orchestration where the object store is not the point. `InMemoryStorage` and
  `CountingStorage` make "how many requests" assertable. A test double implements the production
  `Storage` trait, never a parallel interface.
- **Integration** — required, and mocks are **not** sufficient, for conditional writes and their
  `412` → conflict mapping, key ordering and iteration discovery, request counts, and persisted-state
  round-trips. A change to the stored format is covered for **both** `Json` and `Cbor` through real
  storage, not `serde` alone.

## Coverage of a change

```text
changed behavior -> possible failure -> test layer -> concrete test
```

- Every changed branch and error path is covered, or its omission is justified.
- Every bug fix carries a regression test that fails without the fix.
- Boundaries get all three cases — below, at, above: attempt budget, payload limits, iteration
  limit, deadline.
- A behavior-preserving refactor needs no new tests, but the existing coverage must be identified.

## Concurrency

- Assert **all allowed outcomes plus the final shared invariant**. Never assert which worker won a
  race, created the job, or ran a given task.
- Losing a task (`TaskWorkerMismatch`) is a **normal outcome**, not an error path: the loser's result
  is dropped and the stored state stays as the winner wrote it.
- A conflict is covered for its full arc — detected, re-read, merged, retried. Asserting only that
  no error came back misses a silent overwrite.
- Create contention deterministically (several workers plus work that cannot finish instantly), never
  by hoping a sleep produces a race.

## Oracles

- Expected values come from the documented contract or are stated literally — never computed by the
  code under test or by its helpers. Do not derive an expected object key from the key builder.
- Arrange code may use production builders; assertions about the result stay independent of them.
- Prove the fixture reaches the condition under test when that is not obvious: that a deadline really
  expired, that a budget is really spent, that two workers really contended.
- Assert counts or non-emptiness before inspecting results — never guard an assertion behind
  `if !result.is_empty()`.
- Task iteration order is `HashMap` order and is **not** a contract. Canonicalize before comparing.
- For errors assert the variant and its retryability, not the message text.

## Determinism and isolation

- **`--test-threads=1` is mandatory** — see the [`Makefile`](../Makefile). Do not drop it to make a
  run faster.
- Real sleeps must not determine correctness or ordering. Coordinate with channels, atomics, or a
  poll-until-condition.
- Every wait has a bounded timeout that fails with a diagnostic. A test that can hang forever is
  broken.
- Tests sharing infrastructure use distinct bucket names or `bucket_prefix` values.
- Harnesses own their containers, managers, and background tasks through RAII guards, and clean up on
  success, error, timeout, and panic.
- A background panic, a failed join, or a failed shutdown fails the test instead of being ignored.
- No dependency on external networks, cloud credentials, or shared remote state.

## Test readability

- Follow Arrange-Act-Assert as a logical structure. Do not add heading comments when the
  phases are already obvious.
- Test names MUST state the condition or trigger and the observable result. Do not list a
  case in the name that the body does not exercise.
- If behavior is triggered implicitly by startup, a callback, a timer, cancellation, or a
  background task, make the trigger explicit in the name or a short comment.
- Comments explain why a case matters, an external specification, a non-obvious trigger,
  or a previous regression. Do not narrate the test body.
- Keep inline `#[cfg(test)]` modules at the end of the source file.
- Do not commit commented-out tests.
- Shared test helpers MUST reduce setup duplication without hiding the inputs and outputs
  that make the case meaningful.

## Disabled and flaky tests

- A required test MUST NOT be made green by retrying it in CI.
- Treat a flaky test as a defect. Replace timing assumptions with deterministic
  synchronization or fix resource isolation.
- `#[ignore]` requires a linked issue, an explanation of the lost coverage, and a clear
  condition for re-enabling the test.
- An ignored, skipped, or environment-gated test does not count as coverage for a change.
- If required integration infrastructure is unavailable locally, report which test command
  was not run. Do not claim the test suite passes and do not silently skip required tests.

## Test review

Review tests by mapping affected behavior to coverage by layer before reviewing individual
assertions. Two tests are not duplicates merely because their final assertions look alike:
a unit test can protect a pure rule while an integration test protects serialization or
orchestration of the same result.

Before completing a code change, verify:

- the changed behavior and failure modes are represented in the coverage map;
- the chosen test layers include every changed boundary;
- regression tests fail for the defect they claim to protect;
- expected values are independent of the implementation;
- relevant boundary, failure, tenant, ordering, and concurrency cases are covered;
- fixtures use canonical schemas and production formats where required;
- tests are deterministic, isolated, and safe under normal parallel execution;
- all applicable feature combinations were tested.

## Running

```bash
cargo test <test_name> -- --test-threads=1
```

```bash
make test
```

Report which commands were run and which required tests were not.
