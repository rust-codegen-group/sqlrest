# Forward migrations and recovery

Each registered database fixes one migration directory. SQLRest reads it once
into an owned plan, validates it against committed history, closes request
admission, drains existing requests, and executes pending files in version order.
Every file is one transaction, including the history INSERT. Only after it commits
does the next file start. Completed earlier files are never undone by a later
failure.

```rust,no_run
use sqlrest::registry::{Outcome, PublishRequest, Registry};

async fn migrate_registered_database(registry: &Registry) -> Result<(), sqlrest::SqlrestError> {
    let id = registry.publish("notes", PublishRequest::default())?;
    let operation = registry.wait_operation("notes", id).await?;
    if operation.outcome == Outcome::Failed {
        // Inspect operation.error and operation.publish, then repair.
        // A failed operation does not imply that earlier files rolled back.
        return Err(operation.error.expect("failed operations carry an error"));
    }
    // Successful publish includes interface loading. Check application behavior.
    Ok(())
}
```

## File contract

Files use names such as `0001_create_todos.sql`: a positive int64 decimal version,
an underscore, a nonempty ASCII alphanumeric/underscore/hyphen description, and
`.sql`. Leading zeros are allowed; versions compare numerically, so `1_a.sql` and
`0001_b.sql` conflict. Gaps are allowed. A newly introduced version must be greater
than every committed version.

The directory is flat and contains only regular UTF-8 SQL files. Nested
directories, symlinks, unknown extensions, empty SQL, duplicate versions, request
parameters and transaction/session-control statements fail preflight. A missing
directory is an error; an existing empty directory is a valid empty plan.
SQL uses the supported transactional statement subset described in
`interfaces.md` (queries, DML, basic table/view/index DDL). This is not a promise
to support every PostgreSQL or SQLite DDL feature. In particular, explicit
BEGIN/COMMIT and PostgreSQL CREATE INDEX CONCURRENTLY are unsupported.

The runtime must finish deployment before publish, keeping all files stable
during the read. SQL text, binding inputs and checksums used for execution are
derived from that same owned plan, never from a second file read. Later disk
edits affect only a future operation. History is rechecked after request drain
without reopening the files.

## Durable history

History lives in `main.__sqlrest_migrations` for Turso and
`public.__sqlrest_migrations` for PostgreSQL. PostgreSQL's role needs the relevant
CREATE/SELECT/INSERT privileges there, as well as privileges for business DDL.
The reserved `__sqlrest_` metadata identifiers cannot be referenced directly
by migration files. Business SQL is trusted configuration, not a SQL sandbox;
do not modify these tables through other connections or interfaces.

Each committed record stores the numeric version, original filename, complete
SQL source and SHA-256 of its exact UTF-8 bytes. File renaming, changing line
endings or editing comments counts as changing history. Metadata creation for
the first file, its business changes and its history row share one transaction.
A history INSERT failure rolls back the file's business changes too.

`registry.export_migrations(name).await` returns ordered `AppliedMigration`
records without writing local files. It checks version/filename/checksum
integrity before returning data and participates in drain. It works in recovery
state when database resources are available, but is unavailable throughout
publish or unregister. Dropping its waiter does not release database resources
ahead of the actual read completion.

For a missing/edited historical file:

1. Export the database's original records.
2. Back up local edits before restoring the exact original filenames and SQL.
3. Put the intended new changes in a higher-version file.
4. Run publish and check the resulting API/data behavior.

There is no ignore-checksum switch, down migration or history overwrite API.
Original SQL may contain sensitive literals: protect exports like database
access. Checksums detect accidental changes, not malicious modification by a
privileged database user. Restoring SQL history is not a data backup and does
not undo a committed destructive migration.

## State and failure contract

Migration is an internal step of publish, sharing its management slot with unregister.
It returns an operation ID immediately, continues independently of its waiter,
and rejects concurrent management changes with 409. Status and the published
OpenAPI remain queryable throughout. Other databases remain independent.

| Event | Data state / next step |
| --- | --- |
| Preflight fails on a healthy database | Old snapshot keeps serving |
| Preflight fails on an already blocked database | Existing recovery blocker is retained |
| Pending migration preflight succeeds | `publishing`; new requests get 503; existing requests drain |
| A file fails or commit is uncertain | `recovery_required`, recovery `migration`; fix and retry publish |
| Migration succeeds | Persist recovery `reload`, then load interfaces in the same publish |
| Interface loading fails after migration | `recovery_required`, recovery `reload`; committed files remain; repair and publish |
| First publication is interrupted | Persisted recovery blocker prevents automatic startup publication |

An empty/no-pending plan still loads interfaces. Without new migrations, a failed
interface load preserves a previously healthy snapshot and its limits.
Success means the engine completed the operation, not that the application's
behavior has been verified. The runtime must check the affected endpoints/data
after successful publication.

Publish progress distinguishes connecting, preflight, draining, applying, loading and
complete. `current_version` identifies an in-flight file, `failed_version` a
failed attempt, and `applied_versions` only those commits confirmed in this
operation—not the entire history.

`migration_timeout_ms` defaults to 60000 on each publish and limits the whole
migration batch, not each file. History validation and applying pending files
share the budget; request draining and connection/interface loading are outside
it. The business `request_timeout_ms` (default 5000) is independent.
Cancellation/rollback cleanup is awaited even after the budget expires.
Business response `max_rows` does not cap migration intermediate results or
history export. There is no extra row/byte/concurrency cap for management work.

`commit_outcome_unknown` must not be reported as a guaranteed rollback. An
explicit publish retry rechecks committed history and skips matching versions;
there is no automatic retry of business SQL. Transaction guarantees do not cover
external effects of SQL functions.

## Restart contract

TOML registration, limits and recovery blockers survive restart. Healthy entries
load their current interfaces; blocked entries remain unavailable. No migration
is automatically replayed. After repair, publish checks history, skips matching
commits and attempts pending files. Operation records are not persisted.
Unregister deletes only registration TOML; later publish needs connection config.

The runtime must designate one migrator per real PostgreSQL database, even when
multiple configurations/processes can reach it. There are no advisory locks,
distributed coordination or durable operation queues. One process owns a workspace
through its filesystem lock; the runtime must still avoid sharing Turso files
across different workspaces or bypassing the Registry. Keep Tokio alive until operations
finish; forced process exit is not graceful shutdown.
