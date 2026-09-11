DELETE FROM todos WHERE id = ${path.id:int64} RETURNING id, title, completed;
