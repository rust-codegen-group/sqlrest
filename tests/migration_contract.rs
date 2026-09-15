use serde_json::{Value, json};
use sqlrest::{
    params::Input,
    registry::{
        DatabaseConfig, Operation, Outcome, Phase, PublishRequest, PublishStep, Recovery, Registry,
    },
    sql::Backend,
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

static NEXT: AtomicU64 = AtomicU64::new(1);
const SCHEMA: &str = r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#;
const FIRST: &str = "CREATE TABLE items(value TEXT NOT NULL); INSERT INTO items VALUES('one'); SELECT value FROM items; -- original tail\n";
const BROKEN: &str = "CREATE TABLE rolled_back(id BIGINT); INSERT INTO absent_table VALUES(1);";
const SECOND: &str = "CREATE TABLE rolled_back(id BIGINT); INSERT INTO items VALUES('two');";

struct Harness {
    registry: Registry,
    config: Fixture,
    directory: tempfile::TempDir,
}

struct Fixture {
    interfaces: PathBuf,
    migrations: PathBuf,
    request: PublishRequest,
}

impl Harness {
    async fn new(backend: Backend) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let target = match backend {
            Backend::Turso => DatabaseConfig::Turso {},
            Backend::Postgres => {
                let mut config: tokio_postgres::Config = std::env::var("SQLREST_TEST_POSTGRES")
                    .expect("Set SQLREST_TEST_POSTGRES to a disposable database")
                    .parse()
                    .unwrap();
                let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                let name = format!(
                    "sqlrest_migrations_{}_{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                );
                client
                    .batch_execute(&format!("CREATE DATABASE {name}"))
                    .await
                    .unwrap();
                config.dbname(&name);
                let mut url =
                    url::Url::parse(&std::env::var("SQLREST_TEST_POSTGRES").unwrap()).unwrap();
                url.set_path(&name);
                DatabaseConfig::PostgresUnencrypted {
                    connection: url.into(),
                }
            }
        };
        let config = Fixture {
            interfaces: directory.path().join("databases/db/interfaces"),
            migrations: directory.path().join("databases/db/migrations"),
            request: PublishRequest {
                database: Some(target),
                ..Default::default()
            },
        };
        fs::create_dir_all(&config.interfaces).unwrap();
        fs::create_dir_all(&config.migrations).unwrap();
        let registry = Registry::open(directory.path()).await.unwrap();
        let harness = Self {
            registry,
            config,
            directory,
        };
        harness.interfaces("SELECT value FROM items ORDER BY value");
        succeeded(harness.migrate().await);
        harness
    }

    fn file(&self, name: &str, source: &str) {
        fs::write(self.config.migrations.join(name), source).unwrap();
    }

    fn interfaces(&self, source: &str) {
        fs::write(self.config.interfaces.join("get.sql"), source).unwrap();
        fs::write(self.config.interfaces.join("get.response.yaml"), SCHEMA).unwrap();
    }

    async fn migrate(&self) -> Operation {
        let id = self
            .registry
            .publish("db", self.config.request.clone())
            .unwrap();
        tokio::time::timeout(
            Duration::from_secs(10),
            self.registry.wait_operation("db", id),
        )
        .await
        .unwrap()
        .unwrap()
    }

    async fn reload(&self) {
        let id = self
            .registry
            .publish("db", self.config.request.clone())
            .unwrap();
        assert_eq!(
            self.registry
                .wait_operation("db", id)
                .await
                .unwrap()
                .outcome,
            Outcome::Succeeded
        );
    }

    async fn values(&self) -> Value {
        let bytes = self
            .registry
            .execute("db", "get", &[], Input::default())
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }
}

fn succeeded(op: Operation) -> Operation {
    assert_eq!(op.outcome, Outcome::Succeeded, "{op:?}");
    op
}

fn failure(op: Operation, code: &str) {
    assert_eq!(op.outcome, Outcome::Failed, "{op:?}");
    assert_eq!(op.error.unwrap().code, code);
}

async fn workflow(backend: Backend) {
    let h = Harness::new(backend).await;
    assert!(h.registry.export_migrations("db").await.unwrap().is_empty());
    let empty = succeeded(h.migrate().await);
    assert!(empty.publish.unwrap().applied_versions.is_empty());
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Ready);
    h.file("0001_initial.sql", FIRST);
    let first = succeeded(h.migrate().await);
    assert_eq!(first.publish.unwrap().applied_versions, vec![1]);
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Ready);
    assert_eq!(h.values().await, json!({"records":[{"value":"one"}]}));
    let history = h.registry.export_migrations("db").await.unwrap();
    assert_eq!(history[0].source, FIRST);
    assert_eq!(history[0].filename, "0001_initial.sql");
    h.reload().await;
    let old = h.registry.status("db").unwrap().version;
    h.file(
        "0003_add_column.sql",
        "ALTER TABLE items ADD COLUMN extra TEXT DEFAULT 'new';",
    );
    h.interfaces("SELECT extra AS value FROM items");
    let op = succeeded(h.migrate().await);
    let progress = op.publish.unwrap();
    assert_eq!(progress.applied_versions, vec![3]);
    assert_eq!(progress.step, PublishStep::Complete);
    assert_ne!(h.registry.status("db").unwrap().version, old);
    assert_eq!(h.values().await, json!({"records":[{"value":"new"}]}));
    h.file("0001_initial.sql", "SELECT 'edited';");
    failure(h.migrate().await, "migration_history_mismatch");
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Ready);
    assert_eq!(h.values().await, json!({"records":[{"value":"new"}]}));
    // Export contains original SQL, not the edited disk contents.
    for record in h.registry.export_migrations("db").await.unwrap() {
        h.file(&record.filename, &record.source);
    }
    h.file(
        "0002_inserted_late.sql",
        "INSERT INTO items(value) VALUES('late');",
    );
    failure(h.migrate().await, "invalid_migration");
    fs::remove_file(h.config.migrations.join("0002_inserted_late.sql")).unwrap();
    fs::remove_file(h.config.migrations.join("0001_initial.sql")).unwrap();
    failure(h.migrate().await, "migration_history_mismatch");
    for record in history {
        h.file(&record.filename, &record.source);
    }
    assert!(
        succeeded(h.migrate().await)
            .publish
            .unwrap()
            .applied_versions
            .is_empty()
    );
}

async fn failures(backend: Backend) {
    let h = Harness::new(backend).await;
    h.file("0001_initial.sql", FIRST);
    h.file("0002_broken.sql", BROKEN);
    let failed = h.migrate().await;
    assert_eq!(failed.outcome, Outcome::Failed);
    let progress = failed.publish.unwrap();
    assert_eq!(progress.applied_versions, vec![1]);
    assert_eq!(progress.failed_version, Some(2));
    assert_eq!(
        h.registry.status("db").unwrap().recovery,
        Some(Recovery::Migration)
    );
    let history = h.registry.export_migrations("db").await.unwrap();
    assert_eq!(history.len(), 1);
    h.file("0002_broken.sql", "BEGIN; COMMIT;");
    failure(h.migrate().await, "invalid_migration");
    assert_eq!(
        h.registry.status("db").unwrap().recovery,
        Some(Recovery::Migration)
    );
    h.file("0002_broken.sql", SECOND);
    // Successful CREATE TABLE proves that the failed file's DDL rolled back.
    succeeded(h.migrate().await);
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Ready);
    h.reload().await;
    assert_eq!(
        h.values().await,
        json!({"records":[{"value":"one"},{"value":"two"}]})
    );
    h.file("0003_committed.sql", "INSERT INTO items VALUES('three');");
    h.interfaces("SELECT ${invalid}");
    assert_eq!(h.migrate().await.outcome, Outcome::Failed);
    assert_eq!(
        h.registry.status("db").unwrap().recovery,
        Some(Recovery::Reload)
    );
    assert_eq!(h.registry.export_migrations("db").await.unwrap().len(), 3);
    assert_eq!(
        h.registry
            .execute("db", "get", &[], Input::default())
            .await
            .unwrap_err()
            .status,
        503
    );
    h.interfaces("SELECT value FROM items ORDER BY value");
    h.reload().await;
    assert_eq!(
        h.registry.status("db").unwrap().recovery,
        Some(Recovery::None)
    );
    assert_eq!(h.values().await["records"].as_array().unwrap().len(), 3);
}

async fn snapshot_and_drain(backend: Backend) {
    let h = Harness::new(backend).await;
    h.file("0001_initial.sql", FIRST);
    succeeded(h.migrate().await);
    let slow = match backend {
        Backend::Turso => {
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT CAST(sum(x) AS TEXT) AS value FROM n"
        }
        Backend::Postgres => "SELECT sum(x)::text AS value FROM generate_series(1,1000000000) x",
    };
    h.interfaces(slow);
    h.reload().await;
    let registry = h.registry.clone();
    let request =
        tokio::spawn(async move { registry.execute("db", "get", &[], Input::default()).await });
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.registry.status("db").unwrap().active_requests == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let original = "INSERT INTO items VALUES('snapshot');";
    h.file("0002_snapshot.sql", original);
    let id = h.registry.publish("db", h.config.request.clone()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while h
            .registry
            .status("db")
            .unwrap()
            .current_operation
            .as_ref()
            .and_then(|op| op.publish.as_ref())
            .unwrap()
            .step
            != PublishStep::Draining
        {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        h.registry
            .publish("db", h.config.request.clone())
            .unwrap_err()
            .code,
        "operation_in_progress"
    );
    assert_eq!(
        h.registry.unregister("db").unwrap_err().code,
        "operation_in_progress"
    );
    assert_eq!(
        h.registry
            .publish("db", h.config.request.clone())
            .unwrap_err()
            .code,
        "operation_in_progress"
    );
    assert!(h.registry.openapi("db", "/db/db").is_ok());
    assert_eq!(
        h.registry
            .execute("db", "get", &[], Input::default())
            .await
            .unwrap_err()
            .status,
        503
    );
    // Files are changed only after the fixed snapshot has reached drain.
    h.file("0002_snapshot.sql", "INSERT INTO items VALUES('disk');");
    h.interfaces("SELECT value FROM items ORDER BY value");
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    succeeded(
        tokio::time::timeout(Duration::from_secs(8), h.registry.wait_operation("db", id))
            .await
            .unwrap()
            .unwrap(),
    );
    assert_eq!(
        h.values().await,
        json!({"records":[{"value":"one"},{"value":"snapshot"}]})
    );
    assert_eq!(
        h.registry.export_migrations("db").await.unwrap()[1].source,
        original
    );
    failure(h.migrate().await, "migration_history_mismatch");
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Ready);
}

async fn history_atomicity(backend: Backend) {
    let h = Harness::new(backend).await;
    let schema = if backend == Backend::Turso {
        "main"
    } else {
        "public"
    };
    h.interfaces("SELECT 'ready' AS value");
    // An intentionally restrictive fixture makes the final history INSERT fail,
    // after the migration's business DDL/DML have already executed.
    fs::write(h.config.interfaces.join("post.sql"), format!(
        "CREATE TABLE {schema}.__sqlrest_migrations(version BIGINT NOT NULL PRIMARY KEY CHECK(version=1), filename TEXT NOT NULL UNIQUE, source TEXT NOT NULL, checksum TEXT NOT NULL)"
    )).unwrap();
    h.reload().await;
    h.registry
        .execute("db", "post", &[], Input::default())
        .await
        .unwrap();
    h.file("0001_initial.sql", FIRST);
    h.file("0002_history_failure.sql", SECOND);
    failure(h.migrate().await, "constraint_violation");
    assert_eq!(h.registry.export_migrations("db").await.unwrap().len(), 1);
    // Diagnostic re-registration deliberately bypasses the runtime recovery
    // protocol to inspect rollback, not to claim the failed migration recovered.
    let id = h.registry.unregister("db").unwrap();
    succeeded(h.registry.wait_operation("db", id).await.unwrap());
    fs::remove_file(h.config.migrations.join("0002_history_failure.sql")).unwrap();
    fs::write(
        h.config.interfaces.join("post.sql"),
        "CREATE TABLE rolled_back(id BIGINT)",
    )
    .unwrap();
    h.interfaces("SELECT value FROM items ORDER BY value");
    h.reload().await;
    h.registry
        .execute("db", "post", &[], Input::default())
        .await
        .unwrap();
    assert_eq!(h.values().await, json!({"records":[{"value":"one"}]}));
    // Corruption must not be exported as trustworthy original history.
    fs::write(
        h.config.interfaces.join("post.sql"),
        format!("UPDATE {schema}.__sqlrest_migrations SET source='tampered'"),
    )
    .unwrap();
    h.reload().await;
    h.registry
        .execute("db", "post", &[], Input::default())
        .await
        .unwrap();
    assert_eq!(
        h.registry.export_migrations("db").await.unwrap_err().code,
        "migration_history_corrupt"
    );
    failure(h.migrate().await, "migration_history_corrupt");
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Ready);
}

async fn migration_timeout(backend: Backend) {
    let mut h = Harness::new(backend).await;
    let id = h.registry.unregister("db").unwrap();
    succeeded(h.registry.wait_operation("db", id).await.unwrap());
    h.config.request.migration_timeout_ms = 1000;
    h.config.request.limits.request_timeout_ms = 1;
    h.config.request.limits.max_rows = 1;
    h.file("0001_initial.sql", FIRST);
    let slow = match backend {
        Backend::Turso => {
            "CREATE TABLE rolled_back(id BIGINT); WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n;"
        }
        Backend::Postgres => {
            "CREATE TABLE rolled_back(id BIGINT); SELECT sum(x) FROM generate_series(1,1000000000) x;"
        }
    };
    h.file("0002_timeout.sql", slow);
    failure(h.migrate().await, "execution_timeout");
    assert_eq!(
        h.registry.status("db").unwrap().recovery,
        Some(Recovery::Migration)
    );
    // Business max_rows=1 and request timeout do not limit migration SELECT results or
    // prevent exporting nonempty durable history.
    assert_eq!(h.registry.export_migrations("db").await.unwrap().len(), 1);
    h.file("0002_timeout.sql", SECOND);
    succeeded(h.migrate().await);
    assert_eq!(h.registry.export_migrations("db").await.unwrap().len(), 2);
}

#[tokio::test]
async fn turso_migration_timeout() {
    migration_timeout(Backend::Turso).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_migration_timeout() {
    migration_timeout(Backend::Postgres).await;
}

#[tokio::test]
async fn turso_history_atomicity() {
    history_atomicity(Backend::Turso).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_history_atomicity() {
    history_atomicity(Backend::Postgres).await;
}

#[tokio::test]
async fn turso_workflow() {
    workflow(Backend::Turso).await;
}

#[tokio::test]
async fn turso_failures() {
    failures(Backend::Turso).await;
}

#[tokio::test]
async fn turso_snapshot_and_drain() {
    snapshot_and_drain(Backend::Turso).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_workflow() {
    workflow(Backend::Postgres).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_failures() {
    failures(Backend::Postgres).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_snapshot_and_drain() {
    snapshot_and_drain(Backend::Postgres).await;
}

#[tokio::test]
async fn invalid_files_fail_preflight_without_partial_migrations() {
    let h = Harness::new(Backend::Turso).await;
    h.file("0001_initial.sql", FIRST);
    for (filename, source) in [
        ("1_duplicate.sql", "SELECT 1"),
        ("0000_zero.sql", "SELECT 1"),
        ("9223372036854775808_overflow.sql", "SELECT 1"),
        ("notes.txt", "SELECT 1"),
        ("0002_parameters.sql", "SELECT ${body.value:int64}"),
        (
            "0002_metadata.sql",
            "DROP TABLE main.\"__sqlrest_migrations\"",
        ),
        ("0002_transaction.sql", "COMMIT"),
    ] {
        h.file(filename, source);
        failure(h.migrate().await, "invalid_migration");
        assert_eq!(h.registry.status("db").unwrap().phase, Phase::Ready);
        assert!(h.registry.export_migrations("db").await.unwrap().is_empty());
        fs::remove_file(h.config.migrations.join(filename)).unwrap();
    }
    #[cfg(unix)]
    {
        let alias = h.config.migrations.join("0002_alias.sql");
        std::os::unix::fs::symlink(h.config.migrations.join("0001_initial.sql"), &alias).unwrap();
        failure(h.migrate().await, "invalid_migration");
        fs::remove_file(alias).unwrap();
    }
    succeeded(h.migrate().await);
}

// Separate process, deliberately exits without dropping the registry. Its
// durable migration history is the only recovery evidence for the next process.
#[test]
#[ignore = "subprocess helper; invoked only by process recovery tests"]
fn restart_worker() {
    let root = std::env::var("SQLREST_RECOVERY_ROOT").expect("helper requires recovery fixture");
    let mode = std::env::var("SQLREST_RECOVERY_MODE").unwrap();
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let registry = Registry::open(&root).await.unwrap();
        if mode == "recover" {
            assert_eq!(
                registry.status("db").unwrap().phase,
                Phase::RecoveryRequired
            );
            assert_eq!(
                registry.status("db").unwrap().recovery,
                Some(Recovery::Migration)
            );
        }
        // No connection details or registration replay from the parent.
        let id = registry.publish("db", PublishRequest::default()).unwrap();
        if mode == "crash" {
            tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let status = registry.status("db").unwrap();
                    let progress = status.current_operation.unwrap().publish.unwrap();
                    if progress.current_version == Some(2) && progress.applied_versions == vec![1] {
                        // No registry drop, cancellation, or graceful shutdown.
                        std::process::exit(0);
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await
            .unwrap();
            unreachable!();
        }
        let op = registry.wait_operation("db", id).await.unwrap();
        if mode == "fail" {
            assert_eq!(op.outcome, Outcome::Failed);
            assert_eq!(registry.export_migrations("db").await.unwrap().len(), 1);
            assert_eq!(
                registry.status("db").unwrap().recovery,
                Some(Recovery::Migration)
            );
            std::process::exit(0);
        }
        succeeded(op);
        assert_eq!(registry.export_migrations("db").await.unwrap().len(), 2);
        assert_eq!(registry.status("db").unwrap().phase, Phase::Ready);
        let response = registry
            .execute("db", "get", &[], Input::default())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&response).unwrap(),
            json!({"records":[{"value":"one"},{"value":"two"}]})
        );
    });
}

async fn process_recovery(backend: Backend, crash: bool) {
    let h = Harness::new(backend).await;
    h.file("0001_initial.sql", FIRST);
    let slow = match backend {
        Backend::Turso => {
            "CREATE TABLE rolled_back(id BIGINT); WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT sum(x) FROM n;"
        }
        Backend::Postgres => {
            "CREATE TABLE rolled_back(id BIGINT); SELECT sum(x) FROM generate_series(1,1000000000) x;"
        }
    };
    h.file("0002_broken.sql", if crash { slow } else { BROKEN });
    h.registry.shutdown().await.unwrap();
    let Harness {
        registry,
        config,
        directory,
    } = h;
    drop(registry);
    for mode in [if crash { "crash" } else { "fail" }, "recover"] {
        if mode == "recover" {
            fs::write(config.migrations.join("0002_broken.sql"), SECOND).unwrap();
        }
        let mut child = tokio::process::Command::new(std::env::current_exe().unwrap());
        child
            .args(["--ignored", "--exact", "restart_worker", "--nocapture"])
            .env("SQLREST_RECOVERY_ROOT", directory.path())
            .env("SQLREST_RECOVERY_MODE", mode)
            .env_remove("SQLREST_RECOVERY_POSTGRES")
            .kill_on_drop(true);
        let output = tokio::time::timeout(Duration::from_secs(20), child.output())
            .await
            .unwrap()
            .unwrap();
        assert!(
            output.status.success(),
            "child {mode} failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}

#[tokio::test]
async fn turso_process_recovery() {
    process_recovery(Backend::Turso, false).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_process_recovery() {
    process_recovery(Backend::Postgres, false).await;
}

#[tokio::test]
async fn turso_process_exit_during_migration() {
    process_recovery(Backend::Turso, true).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_process_exit_during_migration() {
    process_recovery(Backend::Postgres, true).await;
}
