
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


MINIO_USER ?= minioadmin
MINIO_PASSWORD ?= minioadmin

# Start a local MinIO server through Docker for testing object storage functionality.
start-minio-docker:
	@if [ -z "$(shell docker ps -q -f name=objstore-minio)" ]; then \
		docker run -d --rm \
			--name objstore-minio \
			-p 9000:9000 \
			-p 9001:9001 \
			-e "MINIO_ROOT_USER=$(MINIO_USER)" \
			-e "MINIO_ROOT_PASSWORD=$(MINIO_PASSWORD)" \
			minio/minio server /data --console-address ":9001"; \
	else \
		docker start objstore-minio; \
	fi
	
	# Wait for minio to start
	@echo "Waiting for MinIO server to be ready..."
	while ! curl --fail -s http://localhost:9000/minio/health/live; do sleep 1; done
	@echo "MinIO server is ready."

test-minio-s3-light: start-minio-docker
	@echo "Running S3 Light tests against local MinIO server"
	@{ \
		BUCKET_NAME=test-bucket-$$(date +%s); \
		export TEST_STRICT=1; \
		export TEST_CREATE_BUCKET=1; \
		export S3_TEST_URI="s3://$(MINIO_USER):$(MINIO_PASSWORD)@127.0.0.1:9000/$$BUCKET_NAME?style=path&insecure"; \
		echo "URI: $$S3_TEST_URI"; \
		cargo test -p objstore_s3_light --all-features; \
	}

.PHONY: fmt lint-format lint-clippy lint check test coverage ready start-minio-docker test-minio-s3-light
