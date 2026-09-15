# Workspace and publication

`Registry::open(workspace).await` acquires a workspace lock and restores persisted
databases. Clones share ownership; keep the Tokio runtime alive until shutdown
finishes. Drop all registry clones before reopening the same workspace.

```rust,no_run
use sqlrest::{
    params::Input,
    registry::{DatabaseConfig, Outcome, PublishRequest, Registry},
};

async fn example() -> Result<(), sqlrest::SqlrestError> {
    // Prepare databases/notes/interfaces and migrations before publishing.
    let registry = Registry::open("/data/sqlrest").await?;
    let id = registry.publish("notes", PublishRequest {
        database: Some(DatabaseConfig::Turso {}),
        ..Default::default()
    })?;
    let operation = registry.wait_operation("notes", id).await?;
    if operation.outcome == Outcome::Succeeded {
        let bytes = registry.execute("notes", "get", &[], Input::default()).await?;
        drop(bytes);
    }
    // Shutdown preserves registrations; unregister explicitly removes one.
    registry.shutdown().await?;
    Ok(())
}
```

## Fixed layout

```text
workspace/
  .sqlrest.lock
  databases/
    notes/
      database.toml
      data.db
      interfaces/
        get.sql
        get.response.yaml
      migrations/
        0001_notes.sql
```

Names are nonempty ASCII letters, digits, underscores or hyphens. The directory
name is the database registration name. Paths cannot be overridden; local Turso
always uses `data.db`. No symlinks are accepted for managed layout paths.
The workspace lock is held for the registry/resources' lifetime. Do not delete
or replace its lock file while in use. Process-wide Turso file identity claims
also reject duplicate ownership through hard links. This is not a hostile
filesystem sandbox; do not independently open or replace managed database files.

SQLRest owns `database.toml`; Agents edit interfaces and migrations, and pass
configuration through publish (HTTP or a harness tool using the same Rust API).
Offline human repair is possible. There is no watcher and no retained source
copy from the last successful publish.

```toml
[database]
kind = "turso"

[state]
recovery = "none"

[limits]
request_timeout_ms = 5000
max_rows = 1000
```

For PostgreSQL use `kind = "postgres_unencrypted"` and a `connection` string in
`[database]`. The constructor is explicitly unencrypted; see `execution.md`.
Secrets are not included in status or OpenAPI. TOML files are written with
owner-only permissions on Unix, atomic same-directory replacement and sync.
Back up database contents independently; config/history is not a data backup.

## Publish

Only `publish` and `unregister` mutate lifecycle state. There are no public
register/migrate/reload/pause/resume methods or routes. Publication consists of:

1. Create or reuse a registration; validate the proposed connection.
2. Load the migration plan once and validate its committed history.
3. If needed, close admission, drain, persist recovery state and apply migrations.
4. Load/validate interfaces, persist effective config, then atomically adopt
   the snapshot and its limits.

First publish requires `database`; later omission reuses the saved connection.
Providing it replaces the entire database configuration, never merges fields.
A changed target is validated before disturbing the old service. Then requests
drain, the new connection/recovery state is persisted, and publication proceeds
against the new database's own migration history. Failures after target adoption
do not silently fall back to the old target. No data is copied or deleted.

`limits` fields independently default to 5000 milliseconds / 1000 rows on **every**
publish, not to the prior configuration. Both must be positive integers.
Explicit null and unknown fields are rejected. Effective values are persisted;
restart uses those values rather than applying defaults again.
`migration_timeout_ms` defaults to 60000 and is per-publish only, not persisted.

Ordinary publication without migrations can keep serving the old snapshot.
If its interface validation or pre-replacement config write fails, old interfaces
and limits remain effective. Once migrations have run, a failed publication
blocks service instead. Configuration replacement followed by failed directory
sync is an uncertain durability result: admission closes and restart is required
to reread authoritative state before another publish.

## Recovery and startup

| `state.recovery` | Meaning |
| --- | --- |
| `none` | No durable publication blocker |
| `migration` | Migrations have not been confirmed complete |
| `reload` | Interfaces still need successful loading, including first publish |

Internal registration starts with `reload`; restart cannot accidentally publish
an unfinished first publication. Before database mutation, persist `migration`;
after migration success persist `reload`; only successful interface loading and
config persistence clear it. Do not edit this state to bypass recovery.

Startup loads valid `none` entries from current files. Blocked entries remain
unavailable until publish succeeds. No migration/task replay happens automatically.
Invalid TOML, missing data files, connection errors and interface errors are
retained by directory name without stopping healthy databases. A missing local
file in a persisted registration is never recreated, even on a publish retry.
Directories without `database.toml` are ignored. Whole-workspace I/O/lock failures
fail startup. Process startup does not imply every database is ready.

Fix interface/connection availability and publish to retry. A malformed TOML
requires repair and restart; publish does not silently replace unreadable config.
Unregister stops admission, drains and durably removes only `database.toml`,
then releases resources. Database, interface and migration files remain.
Republish after unregister requires connection configuration again.
Shutdown drains without deleting registrations or changing recovery flags.

## Operations and admission

Publish/unregister reserve one management slot per name and return immediately.
Conflicts return `operation_in_progress` (409), not a queue. Other names remain
independent. Disconnects and dropped waiters do not cancel accepted work.
IDs are `publish-<uuid-base36>` / `unregister-<uuid-base36>`: lowercase,
full 128-bit random UUIDs, no padding. Treat the whole ID as opaque.

`status` / `statuses` include phase, version, recovery, effective limits, active
request count, current/latest operations and sanitized errors. `openapi` reflects
the retained published snapshot, not necessarily an available data service.
Only `ready` admits requests; old requests hold their original snapshot/limits
until actual execution and cleanup finish. Shutdown closes global admission.

Operation records are in memory only. Only current/latest results are retained;
404 means unavailable, not "failed" or "never ran". Restart cannot reuse a numeric
ID for an unrelated operation. Inspect database state before deciding to retry.
An accepted 202 is not a durable task queue or cross-restart execution promise.

For shared PostgreSQL targets, the runtime still appoints one migrator per real
database and keeps aliases' migration histories coherent. No advisory lock or
distributed coordination is added. SQL and filesystem config remain trusted.
