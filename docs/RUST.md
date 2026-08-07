# Rust Code Quality

Binding rules for Rust code in this repo, for AI agents and contributors alike. Architecture,
layering, and domain invariants live in [AGENTS.md](../AGENTS.md); testing lives in
[tests.md](tests.md). This file is only about the language.

jobmanager is a **library crate**: every public item is an API promise, and the lints
(`missing_docs`, `unwrap_used`, `expect_used`, `dead_code`, clippy `pedantic`/`nursery`) are denied
crate-wide precisely so that promise stays honest.

## Core principles

- IMPORTANT: the main principle when writing code is separation of responsibility.
- Correctness over cleverness; clarity over brevity.
- No code beyond what the task requires. An unused item does not compile here (`dead_code` is
  denied), so speculative scaffolding is not an option.
- Maximize reuse (DRY) *after* responsibility is separated, never before.
- Before adding a method, struct, or mechanism, look for the existing pattern in the same trait or
  module and follow it. Do not introduce a parallel mechanism for the same job.

## Naming

- **Names** of functions, structs, methods, constants, and variables **MUST** give an unambiguous
  understanding of their responsibility.
- Name by responsibility and subject, not by the current implementation detail (key, mechanism,
  storage) — the detail can change and the name would then lie. Disambiguate similar things by
  subject, not by mechanism. An implementation detail belongs in a name only where it is literally
  that thing's responsibility at its layer.
- **Functions and methods MUST be `verb_object`** — a verb (the action) plus the noun it acts on.
  **NEVER** name a function with a bare adjective, participle, or state word: there is no object and
  the call site is unreadable. Banned: `ensure_staged`, `rebuild_stale`, `validate_inner`. Good:
  `pick_task_to_execute`, `save_job`, `find_job_meta`. Constructors keep Rust convention: `new`,
  `with_<thing>`, `from_<source>`, `try_<verb>`.
- **The verb MUST NOT lie about behavior.** `restore` must not create, `find` must not write.
- **Variables, arguments, and fields MUST be nouns** naming what the value *is* — the type is
  already in the signature, so the name carries meaning, not the type. An adjective is allowed
  **only** as a qualifier on a noun (`expired_to_fail`), **never** standalone. Banned as standalone
  names: `pending`, `staged`, `current`, `data`, `val`, `tmp`. Collections take a plural noun.
- **Booleans MUST read as predicates:** `is_expired`, `has_started_task`, `should_poll_job`.
- **No abbreviations** except domain-canonical ones (`id`, `uri`, `etag`, `iter_num`). The same
  concept **MUST** share one name across the codebase; different concepts **MUST NOT** share a name.
- **Acid test:** the name **MUST** be understandable at the call site without reading the body.

## Documentation

`missing_docs` is denied, so every public item carries a doc comment. Write it as a contract.

- A doc comment on a `pub` item states **guarantees, errors, and invariants**, not a trace of the
  implementation. A method whose body is self-evident needs one line, not a paragraph.
- Document what the caller cannot see: what happens under concurrency, what is *not* durable yet,
  what is validated later rather than here, what a returned empty value actually means.
- `missing_errors_doc`, `missing_panics_doc`, and `must_use_candidate` are allowed at the lint
  level, but an `# Errors` section is still expected wherever the failure modes are not obvious
  from the error type alone.
- `# Arguments` / `# Returns` blocks are required only when a name or signature is genuinely
  ambiguous; do not pad self-evident parameters.
- Include a doc example for a public entry point whose usage is not obvious from its signature.
  Doc code is formatted (`format_code_in_doc_comments = true`) and compiled — keep it building.
- Keep comments up to date with the code they describe.

### Comment why, not what

- A comment **MUST** add what the code cannot state itself: the reason for a decision, a non-obvious
  invariant, a trap, a cross-reference.
- **NEVER** restate the body in prose. If deleting the comment loses no information a reader could
  not get from the code, delete it.
- **Acid test:** a good comment answers a question that arises *after* reading the code. One that
  answers "what does this line do" is noise.
- **Budget:** a comment on a non-public item is one or two lines. Go past that only for an invariant,
  a trap, or a decision whose alternative the code does not show. A doc comment on a `pub` item is as
  long as its contract needs and not a line longer — a justification written for whoever reviews the
  change does not belong in the file the change lands in.
- A shared mechanism is documented **once** at its definition; call sites **MUST NOT** repeat the
  explanation.
- `TODO` markers carry a severity: `TODO(low)`, `TODO(med)`, `TODO(high)`. Keep the prefix when
  adding one, and keep existing markers intact unless the change resolves them.

## Type system

- **MUST** leverage the type system to prevent bugs at compile time.
- Use newtypes to distinguish semantically different values of the same underlying type
  (`JobCode`, `TaskCode`). A raw `String` identifier crossing an API boundary is a defect.
- Prefer `Option<T>` over sentinel values; prefer an enum over a bool pair when the states are not
  independent.
- Make fields private by default; expose accessors. Prefer `const fn` accessors where possible —
  the existing code does, and clippy's `nursery` set will ask for it.
- Derive `Debug`, `Clone`, `PartialEq` where appropriate; `Default` only when a sensible default
  exists.
- Use the builder pattern (`with_*`) for optional construction parameters, returning `Result` when
  the value is validated and `Self` when it is not.
- Group a struct's declaration with its `impl`.
- A component's configuration struct lives in the module of the component it configures, next to
  it — `WorkerConfig` in `worker.rs`, `S3StorageConfig` in `s3.rs`. A config declared in the module
  that merely *holds* the component drifts away from the type whose behavior it describes.
- Domain structures, VO, and POCO must always be in a consistent state.
- Never place logic inside a DTO. DTOs are only data carriers.

## Error handling

- **NEVER** use `.unwrap()` or `.expect()` outside tests — both are denied lints. Propagate with
  `?`, or convert deliberately with `unwrap_or`, `unwrap_or_else`, `ok_or_else`.
- **NEVER** use `panic!`, `todo!`, or `unimplemented!`. A panicking executor is caught and turned
  into a task failure, but that is a safety net, not a design.
- **MUST** use `Result<T, E>` for fallible operations and `thiserror` for error types.
- Respect the existing error boundary: `Error` is the public type, `InternalError` and `JobError`
  are crate-internal, `StorageError` is storage-internal. Do not leak an internal variant into the
  public API, and do not collapse a meaningful variant into `Error::Other`.
- An error type that drives control flow must expose that decision as a method
  (`is_retryable`, `is_conflict`), not force callers to match on variants.
- It is better to return an error than to use, calculate, or persist invalid state.

## Function design

- Keep functions focused on a single responsibility. `too-many-lines-threshold` is 150 and
  `cognitive-complexity-threshold` is 30 — a function fighting those limits wants splitting.
- Prefer borrowing (`&T`, `&mut T`) over ownership when possible.
- Limit parameters to 8 (`too-many-arguments-threshold`); past ~5, prefer a config struct.
- Return early to reduce nesting.
- Use iterators and combinators over explicit loops where clearer.
- **No `#[cfg(test)]` methods on production types.** A test that needs a different value configures
  it through the type's own configuration; a second, test-only way to set the same field is the
  parallel mechanism this file forbids. An exception needs an explicit agreement and a comment
  saying why the configuration path could not carry it.

## Async and concurrency

- `tokio` is the runtime. Do not block the reactor: no synchronous I/O and no long CPU work in an
  async fn.
- **Do not hold a lock across an `.await`.** The existing code takes a value out from under the
  guard first — follow that pattern.
- `parking_lot` locks are for short synchronous critical sections; `tokio::sync` locks are for
  sections that span an await point. Pick by that rule, not by habit.
- Every long-running loop and every retry **MUST** be cancellable through the `CancellationToken`
  it was given, and **MUST** select on it rather than polling a flag.
- Public types crossing task boundaries need `Send + Sync` bounds; state them rather than relying
  on inference through a `dyn` type.
- `unsafe` is forbidden by the crate lints.

## Memory and performance

- Avoid unnecessary allocations; prefer `&str` over `String` and slices over `Vec` in signatures.
- Use `Cow<'_, str>` when ownership is conditionally needed.
- Use `Vec::with_capacity` / `HashMap::with_capacity` when the size is known.
- `.clone()` is explicit and deliberate: cloning an `Arc` is cheap and fine, cloning a whole job
  state is not. Prefer `Arc::make_mut` where copy-on-write already applies.

## Imports and dependencies

- **MUST** avoid wildcard imports except in test modules (`use super::*`).
- Import grouping is `StdExternalCrate`, sorted by rustfmt — do not hand-order imports.
- New dependencies need a version constraint in `Cargo.toml` and a reason. This is a published
  library, so every added dependency lands in every consumer's tree; prefer the standard library
  or an existing dependency first.
- `cargo audit` runs in CI (`make audit`) — a dependency with a live advisory does not land.

## Version control

- Conventional Commits: `feat:`, `fix:`, `refactor:`, `docs:`, `ci:`, `test:`.
- Code and comments are English-only.
- **NEVER** commit commented-out code, debug `println!`/`dbg!`, or credentials.
- Never create branches, commits, or PRs without an explicit instruction.
