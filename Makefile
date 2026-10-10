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
# The grep holds the import rule of docs/RUST.md that no formatter or lint can express: crate code
# never imports through a public re-export of lib.rs. Modules are lowercase, so `crate::` followed
# by an uppercase name - or by `task_fn`, the one lowercase re-export - is such a path.
fmt:
	cargo +nightly fmt -- --check
	@! grep -rnE 'crate::([A-Z]|task_fn([^[:alnum:]_]|$$))' src --exclude=lib.rs \
		| grep -vE '^[^:]+:[0-9]+:[[:space:]]*//[/!]' \
		|| { echo "import by the owner module's path, not through lib.rs - see docs/RUST.md"; exit 1; }

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
