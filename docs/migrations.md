# Forward migrations and recovery

Each registered database fixes one migration directory. SQLRest reads it once
into an owned plan, validates it against committed history, closes request
admission, drains existing requests, and executes pending files in version order.
Every file is one transaction, including the history INSERT. Only after it commits
does the next file start. Completed earlier files are never undone by a later
failure.

```rust,no_run
use sqlrest::registry::{Outcome, Registry};

async fn migrate_registered_database(registry: &Registry) -> Result<(), sqlrest::SqlrestError> {
    let id = registry.migrate("notes")?;
    let operation = registry.wait_operation("notes", id).await?;
    if operation.outcome == Outcome::Failed {
        // Inspect operation.error and operation.migration, then repair.
        // A failed operation does not imply that earlier files rolled back.
        return Err(operation.error.expect("failed operations carry an error"));
    }
    // A database without a previously published snapshot still needs explicit reload.
    let id = registry.reload("notes")?;
    registry.wait_operation("notes", id).await?;
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

The runtime must finish deployment before migrate, keeping all files stable
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
integrity before returning data, works while paused, and participates in drain.
It is unavailable during migration execution or unregister. Dropping its waiter
does not release database resources ahead of the actual read completion.

For a missing/edited historical file:

1. Export the database's original records.
2. Back up local edits before restoring the exact original filenames and SQL.
3. Put the intended new changes in a higher-version file.
4. Run migrate and check the resulting API/data behavior.

There is no ignore-checksum switch, down migration or history overwrite API.
Original SQL may contain sensitive literals: protect exports like database
access. Checksums detect accidental changes, not malicious modification by a
privileged database user. Restoring SQL history is not a data backup and does
not undo a committed destructive migration.

## State and failure contract

Migrate shares the same per-configuration management slot as reload/unregister.
It returns an operation ID immediately, continues independently of its waiter,
and rejects concurrent management changes with 409. Status and the published
OpenAPI remain queryable throughout. Other databases remain independent.

| Event | Data state / next step |
| --- | --- |
| Preflight fails on a healthy database | Old snapshot keeps serving |
| Preflight fails on an already paused database | Existing pause reason is retained |
| Preflight succeeds | `migrating`; new data requests get 503; existing requests drain |
| A file fails or commit is uncertain | `paused`, `migration_failed`; fix and retry migrate; reload alone is rejected |
| Migration succeeds with a published snapshot | Automatic reload and resume inside the same operation |
| Automatic reload fails | `paused`, `reload_failed`; committed files remain committed; repair interfaces and reload |
| Migration succeeds without a published snapshot | `unloaded`; first publication requires explicit reload |

An empty/no-pending plan also follows the success publication rule. A no-op
migrate is not an exemption from automatic reload or its failure handling.
Success means the engine completed the operation, not that the application's
behavior has been verified. The runtime must check the affected endpoints/data
after automatic resume.

Operation progress distinguishes preflight, draining, applying, reloading and
complete. `current_version` identifies an in-flight file, `failed_version` a
failed attempt, and `applied_versions` only those commits confirmed in this
operation—not the entire history. `interfaces_reloaded` distinguishes automatic
publication from the first-use unloaded case.

Every migration-file transaction and history-read transaction uses the configured
execution timeout and the existing driver's cancellation/cleanup semantics.
It is not one aggregate deadline for the entire management operation; filesystem
read/compilation, history verification and draining have no new timeout setting.
Business response `max_rows` does not cap migration intermediate results or
history export. There is no extra row/byte/concurrency cap for management work.

`commit_outcome_unknown` must not be reported as a guaranteed rollback. An
explicit migrate retry rechecks committed history and skips matching versions;
there is no automatic retry of business SQL. Transaction guarantees do not cover
external effects of SQL functions.

## Restart contract

Registry state, pause reasons and operation IDs are in memory only. Both process
restart and unregister/re-register return a database to `unloaded` without
automatically publishing it. A successful V1 record cannot prove whether V2
was never attempted, failed, or was interrupted.

For configurations using migrations, the runtime must restore in this order:
`register → migrate → reload`, then check behavior. Matching committed files
are skipped; uncommitted files are attempted again. Within a registration,
reload cannot bypass a migration-failure pause. Across registrations the server
does **not** persist that failure or prohibit direct reload; correct recovery
order is the runtime's responsibility.

The runtime must designate one migrator per real PostgreSQL database, even when
multiple configurations/processes can reach it. There are no advisory locks,
distributed coordination, durable operation queues or cross-process Turso
ownership guarantees. Keep the Tokio runtime alive until accepted operations
finish; forced process exit is not graceful shutdown.
