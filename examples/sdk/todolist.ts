// Generated SDK goes in ./sdk. This file is compiled and executed by scripts/e2e.py.
import { DefaultApi } from './sdk/apis/DefaultApi';
import { Configuration } from './sdk/runtime/runtime';
declare const process: { argv: string[]; exitCode?: number };

function equal(actual: unknown, expected: unknown): void {
  const canonical = (value: unknown): unknown => Array.isArray(value)
    ? value.map(canonical)
    : value !== null && typeof value === 'object'
      ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b))
          .map(([key, item]) => [key, canonical(item)]))
      : value;
  if (JSON.stringify(canonical(actual)) !== JSON.stringify(canonical(expected))) {
    throw new Error(`Unexpected result: ${JSON.stringify(actual)}`);
  }
}

async function main(): Promise<void> {
  const api = new DefaultApi(new Configuration({ basePath: `${process.argv[2]}/db/todolist` }));
  const todo = { id: 77, title: 'Generated SDK', completed: false };
  equal((await api.postTodos({ body: todo })).records, [todo]);
  equal((await api.postTodos({ body: todo })).records, [todo]); // stable ID retry
  equal((await api.getTodosById({ id: 77 })).records, [todo]);
  equal((await api.postTodosLookup({ body: { ids: [77] } })).records, [todo]);
  const updated = { ...todo, completed: true };
  equal((await api.patchTodosById({ id: 77, body: { title: todo.title, completed: true } })).records, [updated]);
  equal((await api.deleteTodosById({ id: 77 })).records, [updated]);
  equal((await api.getTodosById({ id: 77 })).records, []);
  console.log('PASS generated todolist SDK: CRUD, retry, ID array');
}
main().catch(error => { console.error(error); process.exitCode = 1; });
