
fmt:
	cargo fmt

lint: lint-format check lint-clippy lint-docs lint-cargo-deny

lint-format:
	cargo fmt --check

lint-clippy:
	cargo clippy --all-features -- -D warnings

lint-docs:
	cargo doc --workspace --all-features --no-deps --document-private-items --keep-going

lint-cargo-deny:
	cargo deny check


# Only works with a recent nightly toolchain
lint-minimal-versions:
	mkdir -p ./target/minimal
	cargo +nightly generate-lockfile -Z unstable-options -Z minimal-versions --lockfile-path target/minimal/Cargo.lock
	cargo +nightly check --workspace --all-features --locked --keep-going -Z unstable-options --lockfile-path target/minimal/Cargo.lock

fix:
	cargo clippy --fix --allow-dirty --all-features
	cargo fmt --all

check:
	cargo check --all-features

test:
	cargo test --all --all-features

coverage:
	cargo tarpaulin --out Html --all-features

# Prepare for a PR by running all lints, checks and tests.
ready: fix lint test

start-minio:
	docker run -d --name minio -p 9000:9000 -e "MINIO_ROOT_USER=admin" -e "MINIO_ROOT_PASSWORD=admin"

.PHONY: fmt lint-format lint-clippy lint check test coverage ready
