SELECT id, amount_minor, note FROM entries
WHERE id IN (SELECT value FROM jsonb_array_elements_text(${body.ids:array<string>}))
ORDER BY id;
