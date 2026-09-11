# Request execution

`execution::Executor` executes an `Arc<loader::Endpoint>` with `params::Input`
and explicit `Limits { timeout, max_rows }`. Its async `execute` method returns
fully serialized JSON bytes: `{"records":[...]}`. It requires a running Tokio
runtime with time and I/O enabled.

Construct a Turso executor with `Executor::turso(turso_driver::open(path)?)`.
PostgreSQL currently exposes only `Executor::postgres_unencrypted(config)`;
use a trusted connection or an external TLS proxy. Native TLS is not implemented.
`sslmode=require` fails; it is not silently downgraded.
The endpoint must have been compiled for the same backend.

## Transaction contract

All declared parameters are validated before database work. Each request owns
a fresh connection and one transaction. Intermediate statement results are
consumed and discarded; only the final result is returned. Its exact column
names, values, schema and JSON encoding are checked before COMMIT.
Errors before commit cause rollback, including excess final rows. Results are
never silently truncated. `max_rows: 0` permits statements without returned rows.
The limit does not apply to discarded intermediate results.

GET and HEAD use database-enforced read-only mode. Turso connections enable
foreign keys. STRICT tables are recommended, not required. Binding follows the
declared parameter type, not the incidental JSON number representation. Arrays
are one JSON value (Turso text / PostgreSQL JSONB), not SQL parameter lists.
PostgreSQL result types outside the supported primitive/JSON set require explicit
SQL conversion, even when no rows are returned. See `interfaces.md`.

## Timeouts and cancellation

One deadline covers input validation, worker/connection waiting, SQL execution,
result conversion, serialization and commit. CPU-bound Turso work uses blocking
workers and a connection-interrupt watchdog; PostgreSQL uses statement deadlines
and protocol cancellation. Dropping the caller future requests cancellation;
a supervisor retains ownership until transaction cleanup completes.

The timeout is not a hard real-time return guarantee. Cleanup is allowed its own
five-second budget, and synchronous driver/OS calls may take longer to return.
Pure CPU validation tasks may finish in the background after cancellation but
cannot subsequently start SQL or commit. Keep the Tokio runtime alive for cleanup;
await `Registry::shutdown()` (or graceful HTTP server completion) before stopping
the runtime. Abrupt runtime/process termination is not graceful drain.

Connections are closed/discarded, never pooled or reused. SQLRest does not retry
business statements. A cancellation request alone is not proof of rollback.

## Errors

Serialized errors and `Display` contain stable codes and sanitized messages, not
SQL, database diagnostics or credentials. Driver errors retain private details in
`SqlrestError::diagnostic()` (also visible in `Debug`), accessible to an embedded runtime.
At conversion, SQLRest writes a JSON diagnostic event to process stderr, including
the backend, public code and original driver details. This also preserves evidence
when cancellation or transaction cleanup replaces the returned error. Both HTTP
listeners keep error serialization sanitized; there is no HTTP diagnostic archive.
Runtime operators must collect and protect stderr: database diagnostics may contain
SQL, paths, connection details and user values. Never proxy these logs or `Debug`
output to site visitors. Principal execution codes:

| Code | Meaning |
| --- | --- |
| `execution_timeout` | Deadline exceeded before a confirmed commit |
| `request_cancelled` | Caller cancellation before a confirmed commit |
| `row_limit_exceeded` | Final result exceeded `max_rows`; transaction rolled back |
| `response_contract_mismatch` | Invalid result metadata/value/encoding |
| `read_only_violation` | Write attempted by a read-only request |
| `constraint_violation` | Database constraint rejected the operation |
| `database_busy` | Concurrent operation conflicted |
| `transaction_cleanup_failed` | Rollback could not be confirmed; connection discarded |
| `commit_outcome_unknown` | COMMIT was attempted but success could not be confirmed |

Never blindly retry a write after `commit_outcome_unknown`: it may already be
durable. Use application-level stable IDs and unique constraints when designing
retriable operations. A transaction does not undo external effects caused by SQL
functions; endpoint SQL is trusted configuration, not a sandbox.
