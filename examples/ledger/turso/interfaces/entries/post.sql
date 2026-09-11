INSERT INTO entries (id, amount_minor, note)
VALUES (${body.id:string}, ${body.amount_minor:int64}, ${body.note:string})
ON CONFLICT (id) DO NOTHING;
SELECT id, amount_minor, note FROM entries WHERE id = ${body.id:string};
