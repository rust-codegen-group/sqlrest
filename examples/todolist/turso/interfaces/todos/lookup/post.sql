SELECT id, title, completed FROM todos
WHERE id IN (SELECT value FROM json_each(${body.ids:array<int64>}))
ORDER BY id;
