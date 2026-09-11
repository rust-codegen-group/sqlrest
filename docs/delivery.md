# Build and verify delivery

## Build inputs

The tested source build uses Rust **1.98.0**, locked Cargo dependencies, and a
C compiler plus Clang/libclang for native dependencies. `rust-toolchain.toml`
selects Rust; `Cargo.lock` selects the dependency graph, including
Turso / turso_sdk_kit / turso_core **0.8.0-pre.10**. The pinned crate suffix is
reported as-is; this repository makes no blanket upstream production-readiness
claim or automatic upgrade.

Linux is the verified delivery platform. CI runs Ubuntu 22.04, tests against
PostgreSQL **18.3** from pinned `postgres:18-alpine` digest
`sha256:54451ecb8ab38c24c3ec123f2fd501303a3a1856a5c66e98cecf2460d5e1e9d7`,
and uses Node **24.15.0**, TypeScript **6.0.3**, and openapi-nexus revision
`1f8e1d8a3264d697c3aca8db7db01148d878115a`.

On Ubuntu 22.04:

```sh
sudo apt-get update
sudo apt-get install -y build-essential clang libclang-dev pkg-config python3 curl jq
cargo build --locked
cargo test --locked
```

This is a source-build recipe with versioned language/dependency inputs, not a
bit-for-bit hermetic build: host C compiler/system packages remain build inputs.
Do not claim reproducibility on a different platform without verifying it.

## Local container

The Dockerfile **packages a prebuilt release binary**; it does not compile or
download Rust inside the image:

```sh
cargo build --release --locked
docker build -t sqlrest:local .
python3 scripts/e2e.py --image sqlrest:local
```

Build on Ubuntu 22.04/glibc 2.35 or Debian 12/glibc 2.36, for the **same Linux
architecture** as the image. macOS/Windows executables and binaries requiring a
newer glibc cannot be copied into this image. CI uses the compatible Ubuntu runner.
The pinned runtime is Debian bookworm-slim digest
`sha256:4724b8cc51e33e398f0e2e15e18d5ec2851ff0c2280647e1310bc1642182655d`.
No claim of a multiarchitecture release is made. Only the binary, license texts
and Dockerfile enter the build context, not database files, credentials, source
workspace or test dependencies.

The executable tutorial creates and removes only its own containers and temporary
files; the image remains available locally. It does not push or publish anything.
For persistent operation, supply mounts and explicit listeners:

```sh
docker run --name sqlrest-demo \
  --user "$(id -u):$(id -g)" \
  --mount "type=bind,src=$SQLREST_DEMO,dst=/workspace" \
  -p 127.0.0.1:8080:8080 -p 127.0.0.1:8081:8081 \
  sqlrest:local --data-listen 0.0.0.0:8080 --management-listen 0.0.0.0:8081
```

`SQLREST_DEMO` is the absolute persistent directory from the getting-started guide.
Adjust registration paths to `/workspace/todolist/...` **inside the container**.
Default image user is numeric 65532; the example uses the caller's UID/GID for
bind-mount access. This does not restrict socket addresses. Proxy authorization
and exposure policy still belong to the runtime/deployer.

Stop gracefully and inspect termination before removing a container; mounted data
is separate. Docker's stop timeout can force-kill a stalled process, which is not
a successful drain. No container image `CMD` silently selects listener addresses.

## Complete local gates

`cargo test` includes Rust unit/contracts, real Turso HTTP, binary SIGTERM,
example CRUD/history repair, and process restart tests. PG and SDK tests are
opt-in dependencies, **not evidence of passing** when reported ignored.

Provision two disposable PG databases: a core-test database and a separate
**empty** examples database. The latter is populated by the examples and must be
new for another run. Then:

```sh
SQLREST_TEST_POSTGRES='postgresql://user:password@host/core_test' \
SQLREST_EXAMPLES_POSTGRES='postgresql://user:password@host/examples_test' \
OPENAPI_NEXUS_BIN=/absolute/path/to/openapi-nexus \
  bash scripts/check.sh
```

The script requires all inputs and fails on missing dependencies/errors. It runs
format, Clippy, normal tests, selected PG migration/execution/HTTP tests,
both-backend SDK HTTP examples, and the recursive SDK compile test. It does not
delete the databases. Never use production or user-data databases.

Do **not** run all ignored tests indiscriminately: `restart_worker` is an internal
subprocess fixture, and the upstream high-level Turso CPU timeout probe is
intentionally skipped. The actual SDK-driver deadline path has separate passing
tests; skipping the old probe does not mean it was repaired.

The CI workflow runs these gates, builds the release artifact, builds the image
and invokes the container restart tutorial. No remote CI run or release is implied
by checking in a workflow; local verification and hosted CI are distinct evidence.

## Embedding, backups and trust

`Registry` and `http::Server` are public Rust entrypoints. Keep Tokio alive until
`Registry::shutdown().await` or graceful `Server::serve` completion. A forcibly
stopped runtime cannot attest rollback or commit state.

Keep runtime configuration and database backups independently. Recovering original
migration source repairs history mismatch; it cannot recover deleted business
data. Designate one migrator for each PG database and one process owner for a
Turso file. Shared DB aliases do not create isolation or separate migration history.

The bundled skill is self-contained: distribute `skills/sqlrest-runtime` as a
directory. It directs the agent to check actual behavior after automatic resume,
preserve edited historical files before restoration, poll asynchronous operations
and avoid blind retries after an unknown commit. It is guidance, not a security
boundary or a substitute for runtime authorization.
