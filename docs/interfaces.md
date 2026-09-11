# Interface compilation contract

The library can compile a directory of SQL interfaces without opening a database.
`Snapshot::load(path, backend)` reads a candidate once. `Snapshot::from_files`
accepts an owned map of relative paths to UTF-8 contents. A snapshot exposes
read-only endpoints and exports OpenAPI from memory; modifying disk afterwards
does not modify the snapshot. Publishing and request execution are separate.

## Files and routes

```text
queries/
  todos/
    post.sql
    post.response.yaml
    [id]/
      patch.sql
      patch.response.yaml
```

- Explicit lowercase methods: `get`, `head`, `post`, `put`, `patch`, `delete`,
  `options`. No implicit HEAD or OPTIONS route is generated.
- Static segments contain ASCII letters, digits, `_`, `-`, `.`; `.` and `..`
  segments are invalid. Dynamic segments are `[name]` with an ASCII identifier.
- The interface tree contains only supported SQL/schema files and ordinary
  directories. Unexpected files, orphan schemas, symlinks and devices are errors.
  Keep documentation and migrations outside this directory. The directory root
  itself may be reached through a deployment symlink.
- Static routes take precedence at the first differing segment. The path is
  selected before its method: a missing method on a static path is 405, not a
  fallback to a dynamic route.
- Dynamic names cannot repeat within one path, and structurally identical
  dynamic prefixes cannot use different names, even across methods.
- Every dynamic segment must have a typed `path` reference in the SQL; no
  undeclared or inferred path parameter type is introduced.
- `resolve(method, segments)` takes already-decoded segments, not an encoded URL.
  The transport must decode exactly once and reject encoded path separators.

The publisher must keep files stable throughout a load. There is no watcher,
database probe, or cross-file filesystem transaction. Content and backend form
a deterministic SHA-256 snapshot version; timestamps and deployment names do not.

## Parameters and SQL

```sql
UPDATE todos
SET title = ${body.input.title:string},
    completed = ${body.input.completed:boolean}
WHERE id = ${path.id:int64}
RETURNING id, title, completed;
```

Types are `string`, `boolean`, `int64`, `float64`; body fields additionally accept
`nullable<T>`, `array<scalar>` and `nullable<array<scalar>>`. Arrays are bound as
one JSON value, never expanded into SQL syntax. Array items are non-null scalars.

All referenced fields are required. Nullable means explicit null is accepted,
not that a field may be absent. Extra fields are ignored. Repeated references
must agree on type; a field cannot be both a leaf and an object parent.

Body values are not coerced from strings. Query/path integers use signed decimal
digits (leading zeroes accepted), booleans only `true` or `false`, and floats use
JSON number syntax and must be finite. Duplicate query/JSON keys, malformed
percent encodings and invalid decoded UTF-8 are rejected. Integer values retain
the full i64 range; JavaScript clients may lose precision for large JSON numbers.

SQL strings, comments and quoted identifiers remain opaque. Typed references
become positional parameters, restarting at `$1` in each statement. Other source
text is retained. PostgreSQL dollar quotes and JSON `?` operators are supported.
Native placeholders, identifier interpolation and explicit transaction/session
control are rejected. The parser checks syntax, not whether tables exist.

The compiled statement subset is SELECT/VALUES queries, INSERT, UPDATE, DELETE,
CREATE TABLE/VIEW/INDEX (not CONCURRENTLY), ALTER TABLE and DROP TABLE/VIEW/INDEX.
This permits transactional DDL without the earlier draft's DML-only restriction.
Other administration/procedure/extension syntax is explicitly unsupported.
This is not a sandbox for untrusted SQL or a guarantee against function side
effects. Database-level read-only enforcement belongs to execution.

## Response schemas

`post.response.yaml` describes one record, not the response envelope.

```yaml
type: object
required: [id, title, completed]
additionalProperties: false
properties:
  id: {type: integer, format: int64}
  title: {type: string}
  completed: {type: boolean}
```

Recognized final query/RETURNING statements require a schema at load time, even
if they will return zero rows. Runtime column metadata remains authoritative:
exact names (not order), duplicate columns and actual values must still be
checked before commit. Schema absence declares only `{"records":[]}` as success.
An unnecessary schema on a no-result statement is retained as a declaration of
record shape; an empty record array still satisfies it.

The supported vocabulary is JSON Schema 2020-12 validation plus OpenAPI 3.1
annotations. Explicit OAS 3.1 dialect declarations use the bundled 2020-12
metaschema for validation; there is no network schema retrieval. `format`,
content encoding, `discriminator`, examples and documentation keywords are
annotations, not implicit conversions or extra format assertions. Unknown
keywords fail (except `x-` annotations), so spelling mistakes do not disappear.
YAML duplicate keys and non-JSON-compatible values fail.

Schemas are self-contained. Local JSON Pointer and `$anchor` references,
including property/item recursion, are supported. External references, `$id`
resource scopes, `$dynamicRef`/`$dynamicAnchor`, and other dialects are explicitly
unsupported. A reference cycle that never descends into a property or array item
is rejected; productive recursive schemas remain intact.

The record must explicitly describe an object with a fixed property set. `allOf`
can combine fields; root `anyOf`/`oneOf` must preserve that fixed set. Runtime
validation checks the complete original composition, including `$ref` siblings.
Use explicit field types to determine database decoding. Property names are
literal column names; dots do not construct nested objects.

Structural fields decode JSON text/native JSON, and Turso boolean fields decode
integer 0/1. Nested JSON is validated without coercion. Unions that admit both
string and object/array, or both numeric and boolean values, are rejected because
their database encoding is ambiguous. Other unions retain full schema
validation. Constraints do not turn a string field into a JSON-decoded field.

## OpenAPI and SDKs

`snapshot.openapi("/db/personal")` returns a document with relative paths.
Use `"/api"` for a proxy mount; endpoint/model names are unchanged.

Names use PascalCase method/path segments with `By` before dynamic names, e.g.
`PatchTodosById`, `PatchTodosByIdInput`, `PatchTodosByIdRecord`,
`PatchTodosByIdResponse`. All normalized name collisions fail; no suffix/hash
repair is applied.

Referenced schema nodes are lifted to named components, with their reference
path appended to the record name (e.g. `GetTreeRecordDefsNode`). References are
rewritten as a graph, not expanded. Nodes must normalize to ASCII component
identifiers. Duplicate normalized names fail. Schema examples/defaults containing
literal `$ref` keys are ordinary data and are not rewritten.

HTTP HEAD descriptions omit response bodies. Other successes use a `records`
array; errors describe the shared error envelope. Generated schemas are derived
from the same immutable SQL/schema snapshot, never re-read from disk.

Verification:

```sh
cargo test --lib --test loading_contract
OPENAPI_NEXUS_BIN=/absolute/path/to/openapi-nexus \
  cargo test --test sdk_contract -- --ignored
```

The SDK test requires `tsc` on PATH. It generates TypeScript in a temporary
directory, compiles it strictly, and checks that recursive child fields retain
their types. Real generated-client calls against the checked-in examples are
covered by `examples_contract` / `scripts/e2e.py --sdk`; see `delivery.md`.
