set shell := ["bash", "-euo", "pipefail", "-c"]

# Run the complete local contract gates (release/container are separate).
default: all

all: _all-inputs fmt clippy turso postgres sdk-postgres sdk-turso sdk-schema

[private]
_all-inputs:
    : "${SQLREST_TEST_POSTGRES:?Set a disposable PostgreSQL URL for core contracts}"
    : "${SQLREST_EXAMPLES_POSTGRES:?Set a DIFFERENT empty PostgreSQL database URL for examples}"
    : "${OPENAPI_NEXUS_BIN:?Set the pinned openapi-nexus binary path}"
    test "$SQLREST_TEST_POSTGRES" != "$SQLREST_EXAMPLES_POSTGRES" || { echo "Core and example database URLs must be different" >&2; exit 2; }

# Check Rust formatting and whitespace.
fmt:
    just --fmt --check
    cargo fmt --check
    git diff --check

clippy:
    cargo clippy --locked --all-targets -- -D warnings

# Includes backend-neutral contracts, doctests and real Turso.
turso:
    cargo test --locked

# Never run blanket --ignored: restart_worker is a subprocess fixture and
# the old upstream high-level Turso CPU timeout probe is intentionally skipped.
postgres:
    : "${SQLREST_TEST_POSTGRES:?Set a disposable PostgreSQL URL for core contracts}"
    cargo test --locked --lib postgres_tests:: -- --ignored
    cargo test --locked --test migration_contract postgres_ -- --ignored
    cargo test --locked --test postgres_contract --test execution_contract --test commit_contract --test registry_postgres -- --ignored
    cargo test --locked --test http_contract postgres_http_lifecycle -- --ignored

sdk-turso:
    : "${OPENAPI_NEXUS_BIN:?Set the pinned openapi-nexus binary path}"
    cargo test --locked --test examples_contract generated_sdks_call_real_examples -- --ignored

sdk-postgres:
    : "${SQLREST_EXAMPLES_POSTGRES:?Set an empty disposable PostgreSQL database URL for examples}"
    : "${OPENAPI_NEXUS_BIN:?Set the pinned openapi-nexus binary path}"
    SQLREST_TEST_POSTGRES="$SQLREST_EXAMPLES_POSTGRES" cargo test --locked --test examples_contract postgres_examples_and_process_restart -- --ignored

sdk-schema:
    : "${OPENAPI_NEXUS_BIN:?Set the pinned openapi-nexus binary path}"
    cargo test --locked --test sdk_contract -- --ignored

release:
    cargo build --release --locked

# Verify the actual crate archive without uploading anything.
publish-dry-run:
    cargo publish --locked --registry crates-io --dry-run

# Upload the current crate to crates.io. Does not create or push Git tags.
publish:
    cargo publish --locked --registry crates-io

# Package an existing release binary and exercise its container lifecycle.
container:
    docker build --tag sqlrest:ci .
    python3 scripts/e2e.py --image sqlrest:ci

# Download the pinned Linux x86_64 musl binary; never build the generator.
download-openapi-nexus destination:
    mkdir -p -- {{ quote(destination) }}
    curl -fsSL https://github.com/rust-codegen-group/openapi-nexus/releases/download/0.2.3/openapi-nexus-x86_64-unknown-linux-musl.tar.xz | tar -xJ --strip-components=1 -C {{ quote(destination) }}
    {{ quote(destination + "/openapi-nexus") }} --version
