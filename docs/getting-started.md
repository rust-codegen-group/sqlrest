# A persistent Todolist API

Use an unused local directory and two available ports. Prerequisites: the README
build tools plus `curl` and `jq`. These commands run from the repository root.
They use your own local DB; unlike `scripts/e2e.py`, this guide does not delete it.

## Build, copy and start

```sh
cargo build --locked
SQLREST_DEMO="$(mktemp -d)"
cp -R examples/todolist/turso "$SQLREST_DEMO/todolist"
jq -n --arg root "$SQLREST_DEMO/todolist" '{
  database: {kind:"turso", path:($root+"/data.db")},
  interfaces:($root+"/interfaces"),
  migrations:($root+"/migrations"),
  limits:{timeout_ms:5000,max_rows:100}
}' > "$SQLREST_DEMO/registration.json"
echo "Keep this runtime-owned directory: $SQLREST_DEMO"
target/debug/sqlrest --data-listen 127.0.0.1:8080 --management-listen 127.0.0.1:8081
```

Keep the service running in that terminal. In another terminal, set `SQLREST_DEMO`
to the printed absolute directory. The runtime normally persists this config
and supervises the process; SQLRest itself does neither.

## Register, migrate, publish

```sh
curl -fsS -X PUT http://127.0.0.1:8081/databases/todolist \
  -H 'Content-Type: application/json' --data-binary @"$SQLREST_DEMO/registration.json"
curl -fsS -X POST http://127.0.0.1:8081/databases/todolist/migrate
```

Registration returns `unloaded`. The second call returns an operation ID, not
completion. Substitute its ID below and poll until `outcome` is `succeeded` or
`failed` (a client polling deadline is not proof of failure):

```sh
curl -fsS http://127.0.0.1:8081/databases/todolist/operations/1
```

On success, publish the first snapshot:

```sh
curl -fsS -X POST http://127.0.0.1:8081/databases/todolist/reload
```

Poll the **new returned ID**, then check status is `ready`. IDs are global and
not guaranteed sequential for one database. If an acknowledgement is lost, status
contains the current/latest operation:

```sh
curl -fsS http://127.0.0.1:8081/databases/todolist
curl -fsS http://127.0.0.1:8081/databases/todolist/openapi
```

## Create, read, update and query an ID array

```sh
curl -fsS http://127.0.0.1:8080/db/todolist/todos \
  -H 'Content-Type: application/json' \
  -d '{"id":41,"title":"Read the contract","completed":false}'
curl -fsS http://127.0.0.1:8080/db/todolist/todos/41
curl -fsS -X PATCH http://127.0.0.1:8080/db/todolist/todos/41 \
  -H 'Content-Type: application/json' -d '{"title":"Checked","completed":true}'
curl -fsS http://127.0.0.1:8080/db/todolist/todos/lookup \
  -H 'Content-Type: application/json' -d '{"ids":[41,42]}'
```

The boolean must be `true`/`false`, not `"true"` or 1. The lookup returns only
matching records. Missing IDs produce an empty array, not an implicit 404.
For the create retry, use the original stable ID and compare the returned row:
the example uses first-write-wins and never overwrites an existing record on POST.

## Restart and recover

Ctrl-C the service and await its exit. Start the same command again. Status is
now 404 because registration is volatile, but the DB file is still present.
Repeat register → migrate → reload, waiting for each operation to succeed.
GET `/db/todolist/todos/41` must still return the updated record. Do not skip
verification merely because migration automatically resumed a published database.

To delete your example record explicitly:

```sh
curl -fsS -X DELETE http://127.0.0.1:8080/db/todolist/todos/41
```

Deleting a registration does not delete the database file. Preserve that file and
the saved registration config for subsequent restarts.

## PostgreSQL and shared databases

Copy `examples/todolist/postgres` instead and replace the target:

```json
{"kind":"postgres_unencrypted","connection":"postgresql://user:password@host/todolist"}
```

Create the database/role outside SQLRest with your normal provisioning process.
The current constructor is unencrypted: use only an appropriately protected
connection. Do not put this connection string in browser code or OpenAPI.
Different databases may share the same server endpoint.

If Todolist and Ledger share a **single** PG database, deploy their tables under
one ordered migration history: `0001_todos.sql`, then `0002_entries.sql`,
with both interface trees combined. Do not independently apply two `0001_...`
histories to the same database. Multiple aliases need that same coherent history
and one runtime-appointed migrator. `scripts/e2e.py --backend postgres` exercises
this shared-database case against `SQLREST_TEST_POSTGRES`, which must be a fresh,
disposable empty database. The script leaves its sample data there; the caller
owns database teardown. It never drops an existing database to make a test pass.
