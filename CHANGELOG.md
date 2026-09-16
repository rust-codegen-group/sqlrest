# Changelog

User-visible changes are recorded here.

## Unreleased

### Added

- Embed the data-only HTTP API in an existing host with `http::DataService`,
  using a mountable Axum router or a direct request handler. Reuse SQLRest's
  parsing, deadlines and errors without binding standalone listeners.

## 0.0.1 - 2026-09-15

### Added

- Serve database-backed HTTP APIs from SQL files with explicitly typed parameters
  and validated response schemas. Use SQLRest as a standalone process or embed
  its Rust library. See [Getting started](docs/getting-started.md).
- Use local Turso databases or remote/shared PostgreSQL databases through the
  same publication and request interfaces.
- Publish database configuration, forward migrations and SQL interfaces with
  one operation. Persist configuration and recovery state in a fixed TOML-based
  workspace so healthy databases recover after restart without registration
  replay. Unregister without deleting database or source files.
- Execute requests transactionally, validating result columns, values and JSON
  before commit. Enforce execution deadlines and maximum final rows with errors,
  not silent truncation; enforce read-only GET and HEAD requests.
- Export OpenAPI 3.1 from the published interface snapshot, including
  self-contained recursive response schemas, for client generation.
- Recover edited or missing migration files from saved original SQL history.
  Poll publication/unregistration operations and inspect per-database status
  through the management API.
- Keep public errors sanitized while retaining private driver diagnostics for
  runtime operators; drain accepted work during graceful shutdown.
- Start from working [Todolist](examples/todolist/README.md) and
  [Ledger](examples/ledger/README.md) examples for both backends, generated
  TypeScript client examples, and a distributable
  [Agent runtime skill](skills/sqlrest-runtime/SKILL.md).

### Initial release boundaries

- Linux is the verified delivery platform. Turso dependencies are pinned to
  `0.8.0-pre.10`; PostgreSQL connections are currently explicitly unencrypted.
- Authentication, TLS termination and exposure protection belong to the Agent
  runtime/deployer. Protect both HTTP listeners; SQL is trusted configuration,
  not a sandbox.
- MySQL, down migrations, filesystem watchers and persistent operation queues
  are not implemented. Operation history does not survive process restart.
- Large JSON integers are not automatically stringified for JavaScript clients.
  Database backups remain the deployer's responsibility.

### For contributors

- Run local verification through the Justfile. CI runs quality, backend and SDK
  checks in parallel, then requires every lane and container verification to
  succeed through the aggregate `delivery` check.
- Generate SDKs with the pinned openapi-nexus release binary, without compiling
  the generator from source. See [delivery verification](docs/delivery.md).
