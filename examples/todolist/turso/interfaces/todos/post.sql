INSERT INTO todos (id, title, completed)
VALUES (${body.id:int64}, ${body.title:string}, ${body.completed:boolean})
ON CONFLICT (id) DO NOTHING;
SELECT id, title, completed FROM todos WHERE id = ${body.id:int64};
