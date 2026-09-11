DELETE FROM entries WHERE id = ${path.id:string} RETURNING id, amount_minor, note;
