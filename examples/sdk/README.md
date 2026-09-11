# Generate and call TypeScript clients

The checked-in `todolist.ts` and `ledger.ts` import **generated** clients. Generated
output lives in the test's temporary directory, not in this repository. They call
actual HTTP endpoints, check CRUD, first-write-wins retries, typed ID arrays and
integer balances, and delete their own SDK test records.

Verified inputs:

- openapi-nexus `1f8e1d8a3264d697c3aca8db7db01148d878115a`
  from `https://github.com/rust-codegen-group/openapi-nexus.git`
- Node.js 24.15.0 and TypeScript 6.0.3

Build the generator from that revision with `cargo build --locked --bin
openapi-nexus`. Install TypeScript in a disposable tools directory or use an
existing matching installation; put its `tsc` on PATH.

```sh
cargo build --locked
OPENAPI_NEXUS_BIN=/absolute/path/to/openapi-nexus \
  python3 scripts/e2e.py --sdk
```

For PostgreSQL, also set `SQLREST_TEST_POSTGRES` to a **fresh disposable empty**
database URL and add `--backend postgres`. These runs register/load the examples,
fetch their live OpenAPI, generate, strictly compile, then execute both clients:

```sh
openapi-nexus generate -i openapi.json -o sdk --generators typescript-fetch
```

Compilation targets ES2022/CommonJS in a separate output directory; the generated
package metadata does not leak ESM semantics into that runnable fixture.
Production applications can use their own bundler/module configuration.

The examples pass an explicit `Configuration({basePath})` at runtime. In a real
site use the runtime's authorized proxy mount, not an exposed management endpoint
or database connection string. The generator is optional and is not linked into
SQLRest or required to serve requests.

The existing `sdk_contract` test separately verifies recursive schema retention
and strict compile-time errors. No SDK recursion or primitive-field capability
is trimmed to simplify these examples.
