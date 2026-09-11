# Personal ledger

`turso/` and `postgres/` are complete backend-specific interfaces and migrations.
This is a small editable personal notebook, not an accounting system.

Store signed integer **USD cents**: `-425` is an expense of $4.25, `1000` an
income of $10.00. There is only one currency; do not mix currencies or use floats.
Use a stable string business ID (for example a caller-generated UUID) per entry.

| Method and route | Body / behavior |
| --- | --- |
| GET `/entries` | All entries ordered by business ID |
| POST `/entries` | `{id, amount_minor, note}`; first-write-wins |
| GET `/entries/{id}` | Matching row or empty array |
| PATCH `/entries/{id}` | `{amount_minor, note}`; replaces both fields |
| DELETE `/entries/{id}` | Returns deleted record |
| POST `/entries/lookup` | `{ids: ["expense-001"]}` |
| GET `/balance` | One record with signed `balance_minor` |

POST runs INSERT ON CONFLICT DO NOTHING followed by SELECT in one transaction.
A retry never adds another copy or increments the balance. Compare returned
values to the original request: reusing an ID for a different payload is not
success. Patches/deletes are editable-notebook operations, not immutable ledger
events, and have no optimistic versioning. A deleted ID can be reused.

Individual amounts and the exposed balance are limited to safe JS integers.
Balances outside the schema range error; they are not rounded. Backend numeric
overflow also errors. This is an application choice, not SQLRest's int64 policy.

Run `python3 scripts/e2e.py` from the repo root to verify both examples,
including duplicate retries and exact balances. For shared PG deployment,
follow `docs/getting-started.md`: two apps in one database need one migration history.
