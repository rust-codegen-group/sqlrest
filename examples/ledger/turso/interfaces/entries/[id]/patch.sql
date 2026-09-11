UPDATE entries SET amount_minor = ${body.amount_minor:int64}, note = ${body.note:string}
WHERE id = ${path.id:string}
RETURNING id, amount_minor, note;
