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
  const api = new DefaultApi(new Configuration({ basePath: `${process.argv[2]}/db/ledger` }));
  // String business ID is stable across retries; money is integer cents, not float.
  const entry = { id: 'sdk-income-001', amount_minor: 1000, note: 'Income' };
  equal((await api.postEntries({ body: entry })).records, [entry]);
  equal((await api.postEntries({ body: entry })).records, [entry]);
  equal((await api.getEntriesById({ id: entry.id })).records, [entry]);
  equal((await api.postEntriesLookup({ body: { ids: [entry.id] } })).records, [entry]);
  equal((await api.getBalance()).records, [{ balance_minor: 575 }]);
  const updated = { ...entry, amount_minor: 1200 };
  equal((await api.patchEntriesById({ id: entry.id, body: { amount_minor: 1200, note: entry.note } })).records, [updated]);
  equal((await api.deleteEntriesById({ id: entry.id })).records, [updated]);
  equal((await api.getBalance()).records, [{ balance_minor: -425 }]);
  console.log('PASS generated ledger SDK: CRUD, stable-ID retry, integer money');
}
main().catch(error => { console.error(error); process.exitCode = 1; });
