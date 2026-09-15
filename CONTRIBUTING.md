# Contributing

## Build

```bash
cargo build
```

The toolchain is pinned in `rust-toolchain.toml` and picked up automatically.

## Tests

Integration tests start real storage containers through testcontainers — RustFS for the S3 backend,
Azurite for the Azure one and Google's `storage-testbench` for the Google Cloud Storage one — so
**Docker must be running**. The test bench image is built for `linux/amd64` only, so on an arm64 host
it runs under emulation and its perimeter is slower than the other two.

```bash
make test
```

`make test` passes `--test-threads=1`. This is not optional: parallel test threads start
parallel containers, and they collide on ports.

If your Docker socket is not in the default location — OrbStack, Colima, rootless Docker — point
`DOCKER_HOST` at it:

```bash
DOCKER_HOST=unix://$HOME/.orbstack/run/docker.sock make test
```

To run the examples you need the same kind of store, but long-lived; the commands for it are in
[examples/README.md](examples/README.md).

A new example that reaches a storage backend needs its own `[[example]]` block in `Cargo.toml`
naming the feature it requires, next to the ones already there. Cargo has no way to declare
`required-features` for auto-discovered examples as a group, so an example added without that block
is built under every feature selection — including one that leaves its backend out, where it fails
to compile and takes that whole selection down with it. Each backend feature has a CI step of its
own — `storage-s3`, `storage-azure` and `storage-gcs` today — and that step is what would report
the breakage.

## Before committing

```bash
make ci
```

That runs `check`, `fmt`, `clippy`, `test` and `audit` — the same set CI runs.

Formatting needs nightly, because `rustfmt.toml` uses nightly-only options:

```bash
make fmt-fix
```

Lints are strict on purpose: `missing_docs` and `dead_code` are denied, clippy runs with
`pedantic` and `nursery`. Every public item needs a doc comment stating its contract — what is
guaranteed, what errors are possible, what breaks at the edges. A doc that restates the
signature is worse than none.

## Commits

Conventional Commits: `feat:`, `fix:`, `refactor:`, `docs:`, `ci:`, `test:`.

Code and comments are English-only.
