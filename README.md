# SQLRest

Typed SQL files → database-backed HTTP APIs for agent runtimes.

SQLRest is a Rust library and a standalone HTTP service. An agent writes SQL and
response schemas in a fixed workspace; one publish call registers the database,
applies pending migrations and publishes its interfaces.
Turso is the default local backend; PostgreSQL supports remote/shared databases.
There is no site or user model, authentication, UI renderer, or runtime SDK dependency.

```sql
UPDATE todos
SET title = ${body.title:string}, completed = ${body.completed:boolean}
WHERE id = ${path.id:int64}
RETURNING id, title, completed;
```

The accompanying `patch.response.yaml` defines the exact result columns:

```yaml
type: object
required: [id, title, completed]
additionalProperties: false
properties:
  id: {type: integer, format: int64}
  title: {type: string}
  completed: {type: boolean}
```

Success is always `{"records":[...]}`. SQL values are bound parameters. Requests
are transactional; result validation and serialization happen before commit.
Publication is explicit and atomic, and OpenAPI comes from the same snapshot.

## Try it

Prerequisites: Rust 1.98.0 (pinned by `rust-toolchain.toml`), C build tools,
Clang/libclang, and Python 3.10+ for the executable tutorial/tests.

```sh
cargo build --locked
python3 scripts/e2e.py
```

This executable tutorial starts a real service with fresh temporary files, runs
both Todolist and Ledger examples, verifies CRUD, retry keys, ID arrays and migration
history repair, restarts the process and verifies automatic recovery without replay. It
then removes **only its temporary data**. It does not start a browser or retain
an application for continued use.

For a persistent application, follow [Getting started](docs/getting-started.md).
Run the service with explicit addresses, for example:

```sh
target/debug/sqlrest --workspace /data/sqlrest \
  --data-listen 127.0.0.1:8080 --management-listen 127.0.0.1:8081
```

Any bindable address is allowed. The addresses above are examples, not enforced
restrictions. The runtime must proxy and authorize access to **both** listeners;
do not expose the management port to users. No auth, TLS or CORS is installed.

## Examples and integration

- [Todolist](examples/todolist/README.md): typed booleans, CRUD, `array<int64>` lookup.
- [Ledger](examples/ledger/README.md): integer money, string business IDs, retry checks.
- [Generated TypeScript SDK](examples/sdk/README.md): openapi-nexus generation,
  strict compilation and real requests, without adding a service dependency.
- [Runtime skill](skills/sqlrest-runtime/SKILL.md): copy the entire
  `skills/sqlrest-runtime` directory into your runtime's skill distribution.
  Includes restart, polling, history repair and checking behavior after publish.
- [Container and verification](docs/delivery.md): pinned inputs, local image,
  complete test gates and CI.
- [Changelog](CHANGELOG.md): user-visible changes and release boundaries.

## Contracts

Read [interfaces](docs/interfaces.md), [execution](docs/execution.md),
[registry](docs/registry.md), [migrations](docs/migrations.md), and
[HTTP](docs/http.md) for details. Embedded users call `Registry` from Tokio;
standalone callers use the same core through HTTP.

Important boundaries:

- SQL is trusted configuration, **not a sandbox**. Transactions cannot roll back
  external effects of SQL functions.
- Only execution timeout and maximum final rows are built in. Limits error, not
  silently truncate. There is no implicit byte/concurrency cap; deployment owns
  resource protection.
- JavaScript can lose precision on large JSON integers. No automatic int64/decimal
  stringification is implemented. Use deliberate application constraints/encoding.
- A lost response or `commit_outcome_unknown` is not proof of rollback.
  Check stable business IDs before retrying.
- SQLRest persists configuration and recovery state in its workspace. The runtime
  appoints one migrator per real PG database and avoids sharing Turso files across
  different workspaces. One Registry owns a workspace at a time.
- Migration source history is for recovery, **not a database backup**. Arrange
  independent backups and test restoration before destructive changes.
- MySQL, TLS-enabled PostgreSQL connections, down migrations, background watchers
  and a persistent operation queue are not implemented.

License: MIT OR Apache-2.0.
