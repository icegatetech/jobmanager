# Contributing

## Build

```bash
cargo build
```

The toolchain is pinned in `rust-toolchain.toml` and picked up automatically.

## Tests

Integration tests start a real S3-compatible container (RustFS) through testcontainers, so
**Docker must be running**.

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
