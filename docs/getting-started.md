# A persistent Todolist API

Run from the repository root with Rust build tools, `curl` and `jq`.
Use an unused local directory; these commands retain your data.

## Prepare layout and start

```sh
cargo build --locked
SQLREST_DEMO="$(mktemp -d)"
mkdir -p "$SQLREST_DEMO/databases"
cp -R examples/todolist/turso "$SQLREST_DEMO/databases/todolist"
echo "Keep this workspace: $SQLREST_DEMO"
target/debug/sqlrest --workspace "$SQLREST_DEMO" \
  --data-listen 127.0.0.1:8080 --management-listen 127.0.0.1:8081
```

Keep that terminal running. SQLRest manages `database.toml` and `data.db` under
the copied directory. The Agent edits only interfaces and migrations.
The addresses are examples, not a loopback restriction; protect both listeners.

## Publish and check

In another terminal:

```sh
SQLREST_OPERATION=$(curl -fsS -X POST http://127.0.0.1:8081/databases/todolist/publish \
  -H 'Content-Type: application/json' -d '{"database":{"kind":"turso"}}' \
  | jq -r .operation_id)
curl -fsS "http://127.0.0.1:8081/databases/todolist/operations/$SQLREST_OPERATION"
```

Poll the returned ID until `outcome` is `succeeded` or `failed`; 202 only means
accepted. Publish handles first registration, migration and interface loading.
On success check status, OpenAPI and behavior:

```sh
curl -fsS http://127.0.0.1:8081/databases/todolist
curl -fsS http://127.0.0.1:8081/databases/todolist/openapi
curl -fsS http://127.0.0.1:8080/db/todolist/todos \
  -H 'Content-Type: application/json' \
  -d '{"id":41,"title":"Read the contract","completed":false}'
curl -fsS http://127.0.0.1:8080/db/todolist/todos/41
curl -fsS -X PATCH http://127.0.0.1:8080/db/todolist/todos/41 \
  -H 'Content-Type: application/json' -d '{"title":"Checked","completed":true}'
curl -fsS http://127.0.0.1:8080/db/todolist/todos/lookup \
  -H 'Content-Type: application/json' -d '{"ids":[41,42]}'
```

Booleans must be JSON booleans, not strings or 0/1. Missing IDs return an empty
records array. Create retries use the original stable ID and compare returned
values: the example is first-write-wins, not an upsert on changed payload.

## Update, restart and unregister

Finish editing files, then publish again:

```sh
curl -fsS -X POST http://127.0.0.1:8081/databases/todolist/publish \
  -H 'Content-Type: application/json' -d '{}'
```

Omitted limits reset to 5000 ms and 1000 rows on every publish; omitted database
configuration reuses the saved connection. Poll and check affected behavior.
Use `migration_timeout_ms` in this request to override the 60000 ms migration
batch budget; it does not change the business request timeout.

Ctrl-C and wait for exit, then start the same command with the same workspace.
The API and data recover without republishing. A blocked publication remains
blocked; repair the cause and publish. Old operation IDs return 404 after restart,
which is not evidence the operation failed or never ran.

```sh
curl -fsS -X DELETE http://127.0.0.1:8081/databases/todolist
```

Poll this unregister operation too. It removes the registration TOML, not the
database or source files. Later publish must include database configuration again.

## PostgreSQL and shared databases

Copy `examples/todolist/postgres` instead and publish with:

```json
{
  "database": {
    "kind": "postgres_unencrypted",
    "connection": "postgresql://user:password@host/todolist"
  }
}
```

Provision the remote database/role outside SQLRest. This transport is explicitly
unencrypted; keep it protected and never put credentials in browser code.
Providing a changed connection on publish switches targets, not data.

Aliases sharing one PostgreSQL database share one migration history. Each fixed
layout must contain the same coherent history, and the runtime appoints one
migrator. Combine Todolist's `0001_todos.sql` and Ledger's migration renamed to
`0002_entries.sql`; do not independently run conflicting `0001` migrations.
`scripts/e2e.py --backend postgres` tests this with a disposable empty database.
