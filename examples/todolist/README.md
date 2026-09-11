# Todolist

`turso/` and `postgres/` each contain complete loadable interfaces and migrations.
Follow the repository getting-started guide to register either directory.

| Method and route | Body / behavior |
| --- | --- |
| GET `/todos` | List, ordered by ID |
| POST `/todos` | `{id, title, completed}`; stable-ID first-write-wins |
| GET `/todos/{id}` | Matching record or empty array |
| PATCH `/todos/{id}` | `{title, completed}`; both fields required |
| DELETE `/todos/{id}` | Deleted record or empty array |
| POST `/todos/lookup` | `{ids: [41, 42]}`; one JSON array bind |

IDs are caller-chosen positive integers capped at JavaScript's safe maximum.
Keep an ID stable across retries; its unique constraint prevents duplicate
inserts. POST returns the existing record, which the caller must compare with its
intended payload. This is not full concurrent-update or permanent deduplication
support: deleting the row frees its ID, and PATCH has no version check.

Turso uses STRICT tables, an INTEGER 0/1 check, a JSON boolean input placeholder
and a boolean response schema. PG uses native BOOLEAN. Lookup uses `json_each`
in Turso and `jsonb_array_elements_text` plus explicit BIGINT cast in PG.

There is no pagination policy in this small example. More than configured
`max_rows` errors instead of silently returning only the first rows.
