.PHONY: test check fmt fmt-fix clippy clippy-fix audit install ci \
        examples-infra-up examples-infra-down clean

# Integration tests start an S3-compatible container (RustFS) via testcontainers.
# --test-threads=1 is mandatory: parallel containers collide on ports.
test:
	cargo test -- --test-threads=1

check:
	cargo check --all-targets

# rustfmt.toml uses nightly-only options, hence +nightly.
fmt:
	cargo +nightly fmt -- --check

fmt-fix:
	cargo +nightly fmt

clippy:
	cargo clippy --all-targets -- -D warnings

clippy-fix:
	cargo clippy --all-targets --fix --allow-dirty

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
