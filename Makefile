.PHONY: test quota check fmt fmt-fix clippy clippy-fix audit install ci \
        examples-infra-up examples-infra-down clean

# Integration tests start a provider container (RustFS, Azurite, storage-testbench) via
# testcontainers.
# --test-threads=1 is mandatory: parallel containers collide on ports.
test:
	cargo test --all-features -- --test-threads=1

# Exact per-scenario request counts, of every provider. A number that moved is agreed, not
# updated - see docs/tests.md.
quota:
	cargo test --lib --all-features tests::request_quota_test -- --test-threads=1

# Both cuts CI checks: the set a consumer gets without asking, and every feature at once - a
# backend behind a feature the default set leaves out is compiled by nothing otherwise.
check:
	cargo check --all-targets
	cargo check --all-targets --all-features

# rustfmt.toml uses nightly-only options, hence +nightly.
fmt:
	cargo +nightly fmt -- --check

fmt-fix:
	cargo +nightly fmt

# Every feature: a lint gate that does not compile a module does not gate it.
clippy:
	cargo clippy --all-targets --all-features -- -D warnings

clippy-fix:
	cargo clippy --all-targets --all-features --fix --allow-dirty

audit:
	cargo audit

install:
	cargo install cargo-audit

examples-infra-up:
	cd ./examples && docker compose up --detach

examples-infra-down:
	cd ./examples && docker compose down

clean:
	cargo clean

ci: check fmt clippy test audit

ci-fast: check fmt clippy audit
