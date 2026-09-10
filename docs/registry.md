# Database registry

`registry::Registry` owns database configurations, immutable interface snapshots,
request admission and in-memory management operations. It has no HTTP dependency.
Registry clones share state; independent instances have separate names but share
process-wide Turso file ownership checks.

```rust,no_run
use sqlrest::{
    execution::Limits,
    params::Input,
    registry::{Configuration, Outcome, Registry, Target},
};
use std::{path::PathBuf, time::Duration};

async fn example() -> Result<(), sqlrest::SqlrestError> {
    let registry = Registry::new();
    registry.register("notes", Configuration {
        target: Target::Turso(PathBuf::from("/data/notes.db")),
        interfaces: PathBuf::from("/data/interfaces"),
        migrations: PathBuf::from("/data/migrations"),
        limits: Limits { timeout: Duration::from_secs(5), max_rows: 100 },
    }).await?;
    let id = registry.reload("notes")?;
    let operation = registry.wait_operation("notes", id).await?;
    if operation.outcome == Outcome::Succeeded {
        let bytes = registry.execute("notes", "get", &[], Input::default()).await?;
        // Send these complete JSON bytes to the caller.
        drop(bytes);
    }
    let id = registry.unregister("notes")?;
    registry.wait_operation("notes", id).await?;
    Ok(())
}
```

## Registration

All configuration fields are explicit. Names are nonempty ASCII letters, digits,
underscores or hyphens. Paths become absolute at registration; the current
directory must not be changed concurrently. Configuration equality compares
those paths, the backend configuration and both limits. A different textual
symlink/hard-link path is a different configuration, even if it targets the same
file. No credentials are included in status output.

Registering the same name/configuration is idempotent, including during concurrent
registration. A different configuration returns `configuration_conflict` (409)
without touching the proposed replacement file. An idempotent call during
unregister reports `unregistering`; it does not cancel or reverse unregister.
Failed registration releases its name reservation and can be retried.

Registration opens/creates the Turso file or checks a PostgreSQL connection.
It does not execute migrations or read/publish interfaces. The parent directory
of a new Turso file must exist. Interface/migration directories may be deployed
later. Failed registration never deletes or truncates a preexisting database;
a newly created file may remain if opening fails.

Turso ownership checks use canonical paths and OS file identity, including
hard links. Reservation is serialized before the engine opens the file, covering
concurrent creation through path aliases. The identity handle remains owned until
all admitted requests have cleaned up and database resources are released.
The runtime must not replace/rename/unlink the database file or retarget its
directory/symlinks while registered. This is not an adversarial filesystem
sandbox or cross-process lock. Do not bypass the registry by independently
opening the same file with a raw driver.

`Target::PostgresUnencrypted(Box<tokio_postgres::Config>)` uses the explicit
unencrypted connection contract described in `execution.md`. The connection
check uses the configured execution timeout. Different PostgreSQL databases
can share a server endpoint; each configuration targets one database. No
server-alias deduplication or PostgreSQL advisory locks are added.

## Publication and operations

`reload` and `unregister` synchronously reserve a per-database management slot,
return an opaque operation ID, and continue in the background. They require an
active Tokio runtime. A conflicting operation immediately returns
`operation_in_progress` (409), never queues. Other databases may operate
concurrently. Dropping a registration/operation waiter does not cancel accepted
work. Keep the runtime alive until it finishes.

Reload reads and compiles a complete candidate snapshot without executing
business SQL. On success, one locked swap publishes its routes, SQL, parameter
and response contracts, OpenAPI and version together. Old requests retain the
snapshot admitted with them. On failure, the published snapshot is unchanged;
an initial failure leaves the database unloaded. The runtime must finish file
deployment before reload and keep the files stable during its read.

`status` exposes phase, version, active request count, current operation and
most recent completed operation. `openapi` reads only the published snapshot;
status/OpenAPI remain available while management work is running. `execute`
resolves a route and admits it atomically against publication/unregister.
Resolved path parameters replace any caller-provided `Input.path`.

| Phase | Data requests |
| --- | --- |
| `registering` | 503 |
| `unloaded` | 503 |
| `ready` (including reload in progress) | Published snapshot |
| `unregistering` | 503; admitted requests drain |
| `unregistered` | 503 |

A failed registration can briefly be observable as `registration_failed`
before its reservation is removed.

## Unregister and result retention

Unregister immediately closes admission, waits for admitted request execution
and actual transaction cleanup, then releases the snapshot, database and Turso
file claim. Cancelling a caller does not prematurely decrement the active count.
No extra drain timeout or forced transaction termination is introduced: requests
have their configured deadline and the cleanup behavior described in
`execution.md`. OS/driver stalls are not hard real-time bounded.

Unregister never deletes database files or PostgreSQL data. A lightweight
`unregistered` entry retains the latest operation result, so the ID is still
queryable after completion. Re-registering the name replaces that entry and
clears its old results; registering a different name may reuse the released file.

`operation(name, id)` and `wait_operation(name, id)` expose only the current and
latest completed operation, not an operation history. Older results return
`operation_not_found` (404). An already-waiting call remains bound to its original
registration, even if that name is later reused. Retired names have no time-based
expiry and remain until reused or the Registry is dropped. Registry/process
restart loses all names, snapshots and operation IDs; the runtime must re-register.
No persistence, migration lifecycle or HTTP protocol is implemented here.
