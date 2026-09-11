# Compact SQLRest contract

Management and data are different listeners. Name uses ASCII alphanumeric, `_`
or `-`. Paths in registration are service-local, including inside containers.

```json
{
  "database": {"kind": "turso", "path": "/data/todolist.db"},
  "interfaces": "/config/interfaces",
  "migrations": "/config/migrations",
  "limits": {"timeout_ms": 5000, "max_rows": 100}
}
```

PostgreSQL target:
`{"kind":"postgres_unencrypted","connection":"postgresql://user:password@host/database"}`.
No defaults for omitted fields; unknown/duplicate fields fail. Keep secrets in
runtime-controlled configs, not frontend assets or generated SDKs.

| Management call | Result |
| --- | --- |
| PUT `/databases/{name}` with JSON config | 200 status; same config is idempotent |
| GET `/databases/{name}` | 200 phase, pause reason, current/latest operation |
| POST `/databases/{name}/migrate` | 202 `{"operation_id":number}` |
| POST `/databases/{name}/reload` | 202 operation ID |
| DELETE `/databases/{name}` | 202 unregister ID; does not delete DB data |
| GET `/databases/{name}/operations/{id}` | outcome `running`, `succeeded`, `failed` |
| GET `/databases/{name}/openapi` | published OpenAPI; optional `server_url` query |
| GET `/databases/{name}/migrations` | `{"migrations":[{"version":1,"filename":"0001_init.sql","source":"...","checksum":"..."}]}` |

Only current/latest operation results are retained. An older ID can return 404.
Status may change between reads. Data requests use `/db/{name}/...`.
All application errors use `{"error":{"code":"...","message":"...", "parameter":"optional"}}`.

## Files

```text
interfaces/
  todos/
    post.sql
    post.response.yaml
    [id]/
      patch.sql
      patch.response.yaml
migrations/
  0001_todos.sql
```

SQL filename is an explicit lowercase method. No implicit HEAD or OPTIONS.
Dynamic directory names are `[id]`. Non-root trailing slash is significant.
Interfaces contain only SQL/schema files; migrations contain only flat ordinary
UTF-8 `.sql` files. No symlinks inside either tree.

Migration versions are positive int64, numerically ordered; a new version must
be above all applied versions. One transaction per file; do not include
BEGIN/COMMIT, session control or nontransactional administration. Supported SQL
subset: queries, INSERT/UPDATE/DELETE, basic CREATE/ALTER/DROP TABLE/VIEW/INDEX.

```sql
UPDATE todos
SET title = ${body.title:string}, completed = ${body.completed:boolean}
WHERE id = ${path.id:int64}
RETURNING id, title, completed;
```

```yaml
type: object
required: [id, title, completed]
additionalProperties: false
properties:
  id: {type: integer, format: int64}
  title: {type: string}
  completed: {type: boolean}
```

The sidecar describes **one record**, not the envelope. Runtime matches exact
column names and validates values before committing. A SELECT/RETURNING requires
a sidecar, including for zero rows. A no-result endpoint may omit it.
Unmatched single-ID lookup normally returns `{"records":[]}`, not implicit 404.

Placeholder scalar types: string, boolean, int64, float64. Body additionally
supports nullable scalars, arrays of non-null scalars, nullable arrays:
`${body.note:nullable<string>}`, `${body.ids:array<int64>}`.
All referenced fields are required; nullable does not mean optional.
Body booleans must be JSON booleans, not 0/1 or strings. Query/path values are
strictly parsed; no inferred conversions. Structural result fields decode
explicit JSON; Turso boolean result schema decodes only integer 0/1.

Turso array membership:

```sql
WHERE id IN (SELECT value FROM json_each(${body.ids:array<int64>}))
```

PostgreSQL:

```sql
WHERE id IN (
  SELECT CAST(value AS BIGINT)
  FROM jsonb_array_elements_text(${body.ids:array<int64>})
)
```

GET and HEAD execute read-only. Row limit applies to the final result, not
intermediate statements. Input/response/SQL errors before commit roll back the
whole request; an unconfirmed COMMIT outcome is not proof of rollback.

Schemas use self-contained JSON Schema 2020-12/OpenAPI 3.1 annotations; local
refs including productive recursion are supported, external refs are not.
Generated model names derive from method/path, e.g. `PatchTodosByIdRecord`.
Normalized collisions fail instead of being silently renamed. SDK generation
is optional, not a service dependency.
