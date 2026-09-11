SELECT id, amount_minor, note FROM entries
WHERE id IN (SELECT value FROM json_each(${body.ids:array<string>}))
ORDER BY id;
