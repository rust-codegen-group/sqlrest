#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# Explicit opt-in dependencies; never silently skip release gates.
: "${SQLREST_TEST_POSTGRES:?Set a disposable PostgreSQL URL for core contracts}"
: "${SQLREST_EXAMPLES_POSTGRES:?Set a DIFFERENT empty PostgreSQL database URL for examples}"
: "${OPENAPI_NEXUS_BIN:?Set the pinned openapi-nexus binary path}"
if [[ "$SQLREST_TEST_POSTGRES" == "$SQLREST_EXAMPLES_POSTGRES" ]]; then
  echo "Core and example database URLs must be different" >&2
  exit 2
fi

cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
# PostgreSQL-dependent library tests belong in postgres_tests modules.
cargo test --locked --lib postgres_tests:: -- --ignored
cargo test --locked --test migration_contract postgres_ -- --ignored
cargo test --locked --test postgres_contract --test execution_contract \
  --test commit_contract --test registry_postgres -- --ignored
cargo test --locked --test http_contract postgres_http_lifecycle -- --ignored
SQLREST_TEST_POSTGRES="$SQLREST_EXAMPLES_POSTGRES" \
  cargo test --locked --test examples_contract postgres_examples_and_process_restart -- --ignored
cargo test --locked --test examples_contract generated_sdks_call_real_examples -- --ignored
cargo test --locked --test sdk_contract -- --ignored
git diff --check
# Do not run blanket --ignored: restart_worker is a fixture-only subprocess,
# and the old upstream high-level Turso CPU timeout probe is intentionally skipped.
