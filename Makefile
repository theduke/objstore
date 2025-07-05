
fmt:
	cargo fmt

lint-format:
	cargo fmt --check

lint-clippy:
	cargo clippy --all-features -- -D warnings

lint: lint-format check lint-clippy

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
