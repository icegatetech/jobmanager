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
  `CountingStorage` (counts backend calls), `CountingMetrics` (counts the requests a backend was
  billed for, by operation and status), `InMemoryStorage` (current iteration of every job,
  conditional writes, no persistence), `waiting` (bounded waits and `measure_settled_requests`),
  `init_tracing`.

## Choosing the boundary

Use the lowest boundary that still contains the behavior and its risk.

- **Unit** — pure state rules: transitions, dependency resolution, attempt accounting, pickup
  selection, merge decisions, key construction, delay calculation. No I/O. Use `Job::restore` /
  `Task::restore` to build a state no legal call sequence reaches (a task already at its cap).
- **Component** — orchestration where the object store is not the point. `InMemoryStorage` stands in
  for the backend, and `CountingStorage` wrapped around either backend makes "how many requests"
  assertable. A test double implements the production `Storage` trait, never a parallel interface.
- **Integration** — required, and mocks are **not** sufficient, for conditional writes and their
  `412` → conflict mapping, key ordering and iteration discovery, request counts, and persisted-state
  round-trips. A change to the stored format is covered for **both** `Json` and `Cbor` through real
  storage, not `serde` alone.

## Coverage of a change

```text
changed behavior -> possible failure -> test layer -> concrete test
```

- Every changed branch and error path is covered, or its omission is justified.
- A test MUST fail when the behavior it protects is broken — check by breaking that behavior
  deliberately. A test that stays green either asserts the implementation back to itself or never
  reached the case. This applies to every test, not only to the regression test below.
- Every bug fix carries a regression test that fails without the fix.
- A boundary is any input with an ordered domain or a limit, not only the ones named here. Each gets
  all three cases — below, at, above: attempt budget, payload limits, iteration limit, deadline.
- A behavior-preserving refactor needs no new tests, but the existing coverage must be identified.

## Cases

The areas listed below represent a minimum set, not an exhaustive list.

- List all possible areas of change and define the extreme values for each one. A scenario should be recorded
  if you cannot name a reason why it could not occur in a production environment; branch coverage is
  a subsequent check for completeness, but by no means a source of scenarios.
- Degenerate input data: an empty set, a single element, duplicates, zero.
- Time: before, exactly at the moment, after; a clock running backward; a zero-time interval.
- Failure point: any step where an I/O failure could occur—before a write, after a write, a response that was
  lost, or between two writes.
- Concurrency: the object changes between read and write operations.
- Restart: the process terminates at any point between saves.
- Saved format: an object saved by a different version or in a different format.

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
- The number of requests sent to Object Storage is **critical** — see
  [Request quotas](#request-quotas) below.
- Prove the fixture reaches the condition under test when that is not obvious: that a deadline really
  expired, that a budget is really spent, that two workers really contended.
- Assert counts or non-emptiness before inspecting results — never guard an assertion behind
  `if !result.is_empty()`.
- Task iteration order is `HashMap` order and is **not** a contract. Canonicalize before comparing.
- For errors assert the variant and its retryability, not the message text.

## Request quotas

Providers bill per request, and a pass that grew by one is paid on every poll of every worker, for
as long as the job exists. Every scenario the pool repeats therefore has a **quota**: a number of
requests fixed by a test and asserted against a real store. The quotas live together in
[`request_quota_test.rs`](../src/tests/request_quota_test.rs), which is what gives them one address
to review and one command to run — `make quota`. That command prints test names and nothing else,
so the names are what carry the numbers, and no test outside the file is named after what it costs.
Raising a number is agreed before it lands; see the invariant in [AGENTS.md](../AGENTS.md).

A count of `Storage` calls is **not** a quota. It measures whether the cache reached the backend,
which is behavior; it belongs beside the behavior it protects and is named after that behavior —
one call turns into as many requests as its retries and the SDK's make of it.

### Two shapes

- A **run quota** bounds a scenario that ends, and states a total: the whole of what the store was
  asked for, the requests the fixture itself owed included.
- A **steady-state quota** bounds a scenario that does not end — a pool polling a job it must not
  reach the store for. It states what an observation window may add to the class of requests it
  names, which is zero, and is measured as a delta across that window rather than as a total. The
  window is the one place where a real sleep is load-bearing, because the window is what the number
  means; its baseline still comes from waiting for a counter to settle, never from a sleep chosen
  to be long enough.

### Every quota test

A quota is worth its number only if the test cannot be green while the scenario did not happen.
Every quota test therefore:

- **names its number in the test name**, so a number cannot move without the name moving with it,
  and so what `make quota` prints is the register of what the system costs;
- **counts on the storage metric**, under the operation and status pair the request was answered
  with, never on `Storage` calls;
- **asserts the whole of what it counts, not only the pairs it names**, so a status nobody thought
  to name — a `429`, a `500` — cannot disappear from the oracle: a run quota asserts the total, a
  steady-state one asserts everything outside the class it excludes;
- **proves its fixture reached the state under test**: the iteration really finished, the race was
  really lost. A count taken from a scenario that never happened is the failure mode a quota test
  has, and it looks exactly like a pass;
- **keeps the requests of the test itself out of the count**: probes and doubles reach the store
  through one of their own, recorded by nothing;
- **reads the counters only once the requests it bounds have settled**, never the moment its
  condition holds — the passes already in flight still have theirs to issue;
- **fixes what makes the number exact** — how many workers, how many tasks, how many iterations —
  because each of those turns the quota into a range;
- **is checked by breaking the behavior it guards**, once per number, with the break named in the
  test's doc comment. A number that no deliberate break moves is guarding nothing, and a break
  nobody wrote down is a claim. Where two mechanisms hold one number — the wait between passes and
  the poll gate both keep a waiting job off the store — the break disables both: either alone
  leaves the number where it was and reads as a test that guards nothing.

## Determinism and isolation

- **`--test-threads=1` is mandatory** — see the [`Makefile`](../Makefile). Do not drop it to make a
  run faster.
- Real sleeps must not determine correctness or ordering. Coordinate with channels, atomics, or a
  poll-until-condition. The one exception is the observation window of a steady-state quota, where
  the sleep is the measurement itself — its baseline is still taken by poll-until-condition.
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
