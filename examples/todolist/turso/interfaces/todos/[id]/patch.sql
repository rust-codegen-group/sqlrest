UPDATE todos SET title = ${body.title:string}, completed = ${body.completed:boolean}
WHERE id = ${path.id:int64}
RETURNING id, title, completed;
