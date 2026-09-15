# HTTP and embedded delivery

Run `sqlrest --workspace /data/sqlrest --data-listen 0.0.0.0:8080 --management-listen 127.0.0.1:8081`.
All three flags are required; any bindable socket address is allowed. Both sockets
are bound before serving. The binary reports bound addresses on stderr; port 0
is useful for embedding/testing.

There is no authentication, TLS, CORS middleware or address restriction.
The runtime/deployer must protect **both** listeners. Management can open
database connections, read configured local files and execute trusted SQL.
Never expose it to untrusted callers. SQL is not a sandbox.

## Publication and operations

POST `/databases/{name}/publish` on the management listener accepts:

```json
{
  "database": {"kind": "turso"},
  "limits": {"request_timeout_ms": 5000, "max_rows": 1000},
  "migration_timeout_ms": 60000
}
```

For PostgreSQL replace `database` with
`{"kind":"postgres_unencrypted","connection":"postgresql://user:password@host/db"}`.
This constructor explicitly uses an unencrypted connection. First publish requires
database configuration; later omission reuses it. Explicit configuration replaces
the entire database target. Each omitted limit uses its default independently on
every publish. Explicit null, unknown fields and duplicate keys are rejected.
All paths come from the fixed workspace layout. Publish returns 202, then opens
the database, runs pending migrations and loads interfaces. Conflicting operations
return 409; there is no independent register/migrate/reload/pause/resume route.

| Management request | Result |
| --- | --- |
| GET `/databases` | 200 statuses by name, including startup failures |
| GET `/databases/{name}` | 200 status, current and latest operation |
| DELETE `/databases/{name}` | 202 operation ID; unregister without deleting data |
| POST `/databases/{name}/publish` | 202 operation ID |
| GET `/databases/{name}/operations/{id}` | 200 operation |
| GET `/databases/{name}/openapi` | 200 OpenAPI; default server `/db/{name}` |
| GET `/databases/{name}/migrations` | 200 `{"migrations":[...]}` with saved originals |

202 bodies contain a string ID, e.g. `{"operation_id":"publish-abc123"}` (illustrative
suffix; real IDs encode a full random UUID in lowercase base36). Poll until outcome is no
longer `running`; acceptance is not success. A lost acknowledgement can be
recovered through status. Accepted management work survives handler cancellation.
Only OpenAPI accepts a query parameter: `server_url`, once, URL-encoded.
Other management query parameters are rejected. Follow `migrations.md` for repair.
Restart restores workspace configuration without runtime replay. Old operation
records are lost: 404 is not proof of failure or non-execution.

## Data protocol

Requests use `/db/{name}/...` on the data listener, sharing the embedded
Registry executor and validation. Responses are `{"records":[...]}`.
Application errors use `{"error":{"code":"...","message":"..."}}`, optionally
`parameter`; SQL, credentials and driver diagnostics are not exposed.
Malformed HTTP framing is handled by the transport and is not guaranteed JSON.

Paths are percent-decoded exactly once as UTF-8; malformed escapes and encoded
slashes are rejected. Root `/db/name` and `/db/name/` are equivalent; other
trailing slashes are significant. Nonempty bodies require `application/json`
(parameters such as charset are allowed). Input fields remain strongly typed;
duplicate keys/query parameters are rejected.

Only explicitly defined methods exist: no implicit GET-to-HEAD fallback.
HEAD uses its own SQL but emits no body. 405 includes `Allow`. No implicit
OPTIONS or browser cross-origin support is installed.

Database and snapshot are retained before reading a data upload, so concurrent
reload cannot switch that request to another generation. Its timeout includes
body reading, input parsing and execution; SQL cleanup is still awaited.
The row limit errors rather than truncates. No implicit request/response byte
limit is installed, including Axum's usual body cap. Deployers can impose such
limits at their proxy. Large CPU parsing may finish in the background after
cancellation but cannot subsequently execute SQL.

Management publish uploads have no ordinary upload deadline; server
shutdown cancels incomplete uploads. Other management routes ignore bodies.

## Embedding and shutdown

Open with `Registry::open(workspace).await` in a live Tokio runtime, or pass it to
`http::Server::bind` / `from_listeners`. Await `serve(shutdown_future)` to serve
both listeners. `Registry::shutdown().await` closes global admission and drains
accepted work without deleting TOML registrations. Drop all owners before opening
a new Registry for the same workspace.

The binary handles Ctrl-C and Unix SIGTERM. Graceful server shutdown closes
core admission first, stops listeners, cancels incomplete body reads and waits
for accepted requests, management operations and transaction cleanup. There is
no additional forced drain deadline. Driver/OS stalls are not hard-time-bounded.
Dropping the serve future initiates core cleanup but does not confirm completion;
keep the runtime alive and await Registry shutdown to obtain that guarantee.

A client disconnect is not a rollback acknowledgement: the transport may keep
the handler running, and a confirmed commit cannot be undone. If the executor
caller is dropped, its cleanup supervisor handles cancellation. A lost response
can leave write outcome unknown; do not blindly retry writes.
