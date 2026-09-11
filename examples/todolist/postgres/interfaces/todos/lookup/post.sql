SELECT id, title, completed FROM todos
WHERE id IN (SELECT CAST(value AS BIGINT) FROM jsonb_array_elements_text(${body.ids:array<int64>}))
ORDER BY id;
