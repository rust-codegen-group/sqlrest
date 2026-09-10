use serde_json::{Value, json};
use sqlrest::{
    execution::Limits,
    params::Input,
    registry::{Configuration, OperationId, Outcome, Phase, Registry, Target},
};
use std::{fs, path::Path, time::Duration};

const SCHEMA: &str = r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#;

struct Fixture {
    directory: tempfile::TempDir,
    config: Configuration,
}

impl Fixture {
    fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let config = Configuration {
            target: Target::Turso(directory.path().join("data.db")),
            interfaces: directory.path().join("interfaces"),
            migrations: directory.path().join("migrations"),
            limits: Limits {
                timeout: Duration::from_secs(5),
                max_rows: 10,
            },
        };
        Self { directory, config }
    }

    fn deploy(&self, sql: &str) {
        fs::create_dir_all(&self.config.interfaces).unwrap();
        fs::write(self.config.interfaces.join("get.sql"), sql).unwrap();
        fs::write(self.config.interfaces.join("get.response.yaml"), SCHEMA).unwrap();
    }

    fn db_path(&self) -> &Path {
        match &self.config.target {
            Target::Turso(path) => path,
            _ => unreachable!(),
        }
    }
}

async fn successful(registry: &Registry, name: &str, id: OperationId) {
    let op = tokio::time::timeout(Duration::from_secs(5), registry.wait_operation(name, id))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(op.outcome, Outcome::Succeeded, "{op:?}");
}

async fn get(registry: &Registry, name: &str) -> Value {
    serde_json::from_slice(
        &registry
            .execute(name, "get", &[], Input::default())
            .await
            .unwrap(),
    )
    .unwrap()
}

async fn active(registry: &Registry, name: &str) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while registry.status(name).unwrap().active_requests == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn registration_is_explicit_idempotent_and_conflicts_do_not_touch_new_files() {
    let registry = Registry::new();
    let fixture = Fixture::new();
    fixture.deploy("SELECT 'old' AS value");
    let state = registry
        .register("db", fixture.config.clone())
        .await
        .unwrap();
    assert_eq!(state.phase, Phase::Unloaded);
    assert!(state.version.is_none());
    assert!(state.current_operation.is_none());
    assert!(state.last_operation.is_none());
    assert!(!fixture.config.migrations.exists());
    assert_eq!(
        registry
            .execute("db", "get", &[], Input::default())
            .await
            .unwrap_err()
            .status,
        503
    );
    assert_eq!(registry.openapi("db", "/db/db").unwrap_err().status, 503);
    assert_eq!(
        registry
            .register("db", fixture.config.clone())
            .await
            .unwrap()
            .phase,
        Phase::Unloaded
    );

    let mut changed = fixture.config.clone();
    let untouched = fixture.directory.path().join("must-not-create.db");
    changed.target = Target::Turso(untouched.clone());
    assert_eq!(
        registry.register("db", changed).await.unwrap_err().code,
        "configuration_conflict"
    );
    assert!(!untouched.exists());
    let mut changed = fixture.config.clone();
    changed.migrations = fixture.directory.path().join("different-migrations");
    assert_eq!(
        registry.register("db", changed).await.unwrap_err().status,
        409
    );
    successful(&registry, "db", registry.reload("db").unwrap()).await;
    assert_eq!(
        get(&registry, "db").await,
        json!({"records":[{"value":"old"}]})
    );
    assert_eq!(
        registry
            .register("db", fixture.config.clone())
            .await
            .unwrap()
            .phase,
        Phase::Ready
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_registration_and_file_aliases_have_one_owner() {
    let registry = Registry::new();
    let fixture = Fixture::new();
    let mut requests = Vec::new();
    for _ in 0..16 {
        let registry = registry.clone();
        let config = fixture.config.clone();
        requests.push(tokio::spawn(async move {
            registry.register("same", config).await
        }));
    }
    for request in requests {
        assert_eq!(request.await.unwrap().unwrap().phase, Phase::Unloaded);
    }
    let hardlink = fixture.directory.path().join("hardlink.db");
    fs::hard_link(fixture.db_path(), &hardlink).unwrap();
    let mut config = fixture.config.clone();
    config.target = Target::Turso(hardlink);
    assert_eq!(
        registry
            .register("hardlink", config.clone())
            .await
            .unwrap_err()
            .code,
        "database_already_registered"
    );
    // Ownership is process-wide, not confined to one Registry object.
    assert_eq!(
        Registry::new()
            .register("other", config)
            .await
            .unwrap_err()
            .code,
        "database_already_registered"
    );

    #[cfg(unix)]
    {
        let alias = fixture.directory.path().join("alias.db");
        std::os::unix::fs::symlink(fixture.db_path(), &alias).unwrap();
        let mut config = fixture.config.clone();
        config.target = Target::Turso(alias);
        assert_eq!(
            registry.register("alias", config).await.unwrap_err().code,
            "database_already_registered"
        );
    }
    let mut config = fixture.config.clone();
    config.target = Target::Turso(fixture.directory.path().join(".").join("data.db"));
    assert_eq!(
        registry.register("dot", config).await.unwrap_err().code,
        "database_already_registered"
    );

    // Two different names racing to create a new file also have exactly one winner.
    let fresh = Fixture::new();
    let a = registry.register("first", fresh.config.clone());
    let b = registry.register("second", fresh.config.clone());
    let (a, b) = tokio::join!(a, b);
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(
        a.err().or_else(|| b.err()).unwrap().code,
        "database_already_registered"
    );
}

#[tokio::test]
async fn failed_registration_can_retry_without_deleting_data() {
    let registry = Registry::new();
    let fixture = Fixture::new();
    let mut config = fixture.config.clone();
    config.target = Target::Turso(fixture.directory.path().join("missing").join("data.db"));
    assert_eq!(
        registry
            .register("db", config.clone())
            .await
            .unwrap_err()
            .code,
        "invalid_database_path"
    );
    assert_eq!(
        registry.status("db").unwrap_err().code,
        "database_not_found"
    );
    fs::create_dir(fixture.directory.path().join("missing")).unwrap();
    registry.register("db", config).await.unwrap();

    let invalid = fixture.directory.path().join("invalid.db");
    fs::write(
        &invalid,
        b"this is not a valid database and must not be truncated",
    )
    .unwrap();
    let mut config = fixture.config.clone();
    config.target = Target::Turso(invalid.clone());
    let bytes = fs::read(&invalid).unwrap();
    assert!(registry.register("invalid", config).await.is_err());
    assert_eq!(fs::read(invalid).unwrap(), bytes);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_creation_through_directory_symlink_has_one_owner() {
    let registry = Registry::new();
    let fixture = Fixture::new();
    let real = fixture.directory.path().join("real");
    let alias = fixture.directory.path().join("alias");
    fs::create_dir(&real).unwrap();
    std::os::unix::fs::symlink(&real, &alias).unwrap();
    let mut a = fixture.config.clone();
    let mut b = fixture.config.clone();
    a.target = Target::Turso(real.join("new.db"));
    b.target = Target::Turso(alias.join("new.db"));
    let (a, b) = tokio::join!(registry.register("real", a), registry.register("alias", b));
    assert_ne!(a.is_ok(), b.is_ok());
    assert_eq!(
        a.err().or_else(|| b.err()).unwrap().code,
        "database_already_registered"
    );
}

#[tokio::test]
async fn failed_reload_preserves_published_sql_and_openapi() {
    let registry = Registry::new();
    let fixture = Fixture::new();
    registry
        .register("db", fixture.config.clone())
        .await
        .unwrap();
    let failed = registry.reload("db").unwrap();
    assert_eq!(
        registry.wait_operation("db", failed).await.unwrap().outcome,
        Outcome::Failed
    );
    assert_eq!(registry.status("db").unwrap().phase, Phase::Unloaded);
    fixture.deploy("SELECT 'old' AS value");
    successful(&registry, "db", registry.reload("db").unwrap()).await;
    let old = registry.openapi("db", "/db/db").unwrap();
    let version = registry.status("db").unwrap().version;
    fs::write(
        fixture.config.interfaces.join("get.sql"),
        "SELECT ${body.value}",
    )
    .unwrap();
    let id = registry.reload("db").unwrap();
    assert_eq!(
        registry.wait_operation("db", id).await.unwrap().outcome,
        Outcome::Failed
    );
    assert_eq!(registry.openapi("db", "/db/db").unwrap(), old);
    assert_eq!(registry.status("db").unwrap().version, version);
    assert_eq!(
        get(&registry, "db").await,
        json!({"records":[{"value":"old"}]})
    );
    fixture.deploy("SELECT 'new' AS value");
    successful(&registry, "db", registry.reload("db").unwrap()).await;
    assert_ne!(registry.status("db").unwrap().version, version);
    assert_eq!(
        get(&registry, "db").await,
        json!({"records":[{"value":"new"}]})
    );
    assert_eq!(
        registry.operation("db", id).unwrap_err().code,
        "operation_not_found"
    );
}

// An occupied blocking worker makes publication/admission races deterministic.
async fn block_worker() -> (std::sync::mpsc::Sender<()>, tokio::task::JoinHandle<()>) {
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let task = tokio::task::spawn_blocking(move || {
        started_tx.send(()).unwrap();
        release_rx.recv_timeout(Duration::from_secs(10)).unwrap();
    });
    started_rx.await.unwrap();
    (release_tx, task)
}

fn serial_worker_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap()
}

#[test]
fn reload_is_atomic_detached_and_management_conflicts_do_not_queue() {
    serial_worker_runtime().block_on(async {
        let registry = Registry::new();
        let fixture = Fixture::new();
        let other = Fixture::new();
        fixture.deploy("SELECT 'old' AS value");
        other.deploy("SELECT 'other' AS value");
        registry
            .register("db", fixture.config.clone())
            .await
            .unwrap();
        registry
            .register("other", other.config.clone())
            .await
            .unwrap();
        successful(&registry, "db", registry.reload("db").unwrap()).await;
        let old_version = registry.status("db").unwrap().version;
        let old_openapi = registry.openapi("db", "/db/db").unwrap();
        let (release, blocker) = block_worker().await;
        let request_registry = registry.clone();
        let old_request = tokio::spawn(async move { get(&request_registry, "db").await });
        active(&registry, "db").await;
        fixture.deploy("SELECT 'new' AS renamed");
        fs::write(
            fixture.config.interfaces.join("get.response.yaml"),
            r#"{"type":"object","properties":{"renamed":{"type":"string"}},"required":["renamed"],"additionalProperties":false}"#,
        ).unwrap();
        let id = registry.reload("db").unwrap();
        assert_eq!(
            registry.status("db").unwrap().current_operation.unwrap().id,
            id
        );
        assert_eq!(registry.status("db").unwrap().version, old_version);
        assert_eq!(registry.openapi("db", "/db/db").unwrap(), old_openapi);
        assert_eq!(
            registry.reload("db").unwrap_err().code,
            "operation_in_progress"
        );
        assert_eq!(
            registry.unregister("db").unwrap_err().code,
            "operation_in_progress"
        );
        // A different database is accepted immediately, even while db is busy.
        let other_id = registry.reload("other").unwrap();
        let waiting_registry = registry.clone();
        let waiter = tokio::spawn(async move { waiting_registry.wait_operation("db", id).await });
        tokio::task::yield_now().await;
        waiter.abort();
        assert!(waiter.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        blocker.await.unwrap();
        successful(&registry, "db", id).await;
        successful(&registry, "other", other_id).await;
        assert_ne!(registry.openapi("db", "/db/db").unwrap(), old_openapi);
        assert_eq!(
            old_request.await.unwrap(),
            json!({"records":[{"value":"old"}]})
        );
        assert_eq!(
            get(&registry, "db").await,
            json!({"records":[{"renamed":"new"}]})
        );
    });
}

#[test]
fn unregister_drains_rejects_admission_retains_result_and_preserves_data() {
    serial_worker_runtime().block_on(async {
        let registry = Registry::new();
        let fixture = Fixture::new();
        fixture.deploy("SELECT value FROM items");
        fs::write(
            fixture.config.interfaces.join("post.sql"),
            "CREATE TABLE IF NOT EXISTS items(value TEXT); INSERT INTO items VALUES('persisted')",
        )
        .unwrap();
        registry
            .register("db", fixture.config.clone())
            .await
            .unwrap();
        successful(&registry, "db", registry.reload("db").unwrap()).await;
        let (release, blocker) = block_worker().await;
        let request_registry = registry.clone();
        let request = tokio::spawn(async move {
            request_registry
                .execute("db", "post", &[], Input::default())
                .await
        });
        active(&registry, "db").await;
        let id = registry.unregister("db").unwrap();
        assert_eq!(registry.status("db").unwrap().phase, Phase::Unregistering);
        assert_eq!(
            registry.operation("db", id).unwrap().outcome,
            Outcome::Running
        );
        assert_eq!(
            registry
                .execute("db", "get", &[], Input::default())
                .await
                .unwrap_err()
                .status,
            503
        );
        assert_eq!(
            registry.reload("db").unwrap_err().code,
            "operation_in_progress"
        );
        assert!(registry.openapi("db", "/db/db").is_ok());
        release.send(()).unwrap();
        blocker.await.unwrap();
        request.await.unwrap().unwrap();
        successful(&registry, "db", id).await;
        let status = registry.status("db").unwrap();
        assert_eq!(status.phase, Phase::Unregistered);
        assert_eq!(status.active_requests, 0);
        assert!(status.version.is_none());
        assert_eq!(
            registry.operation("db", id).unwrap().outcome,
            Outcome::Succeeded
        );
        assert!(fixture.db_path().is_file());
        assert_eq!(registry.openapi("db", "/db/db").unwrap_err().status, 503);
        registry
            .register("db", fixture.config.clone())
            .await
            .unwrap();
        assert_eq!(
            registry.operation("db", id).unwrap_err().code,
            "operation_not_found"
        );
        successful(&registry, "db", registry.reload("db").unwrap()).await;
        assert_eq!(
            get(&registry, "db").await,
            json!({"records":[{"value":"persisted"}]})
        );
        successful(&registry, "db", registry.unregister("db").unwrap()).await;
        // Released file identity can now be acquired under a different name.
        registry
            .register("renamed", fixture.config.clone())
            .await
            .unwrap();
    });
}

#[tokio::test]
async fn resolved_path_parameters_override_supplied_input() {
    let registry = Registry::new();
    let fixture = Fixture::new();
    let route = fixture.config.interfaces.join("[id]");
    fs::create_dir_all(&route).unwrap();
    fs::write(route.join("get.sql"), "SELECT ${path.id:string} AS value").unwrap();
    fs::write(route.join("get.response.yaml"), SCHEMA).unwrap();
    registry
        .register("db", fixture.config.clone())
        .await
        .unwrap();
    successful(&registry, "db", registry.reload("db").unwrap()).await;
    let mut input = Input::default();
    input.path.insert("id".into(), "forged".into());
    let bytes = registry
        .execute("db", "get", &["actual"], input)
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap(),
        json!({"records":[{"value":"actual"}]})
    );
    assert_eq!(registry.status("db").unwrap().active_requests, 0);
    assert_eq!(
        registry
            .execute("db", "post", &["actual"], Input::default())
            .await
            .unwrap_err()
            .status,
        405
    );
    assert_eq!(registry.status("db").unwrap().active_requests, 0);
}

#[tokio::test]
async fn dropped_registration_waiter_does_not_cancel_reserved_registration() {
    let registry = Registry::new();
    let fixture = Fixture::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut postgres = tokio_postgres::Config::new();
    postgres
        .host("127.0.0.1")
        .port(listener.local_addr().unwrap().port())
        .user("private-user")
        .password("private-password");
    let mut config = fixture.config.clone();
    config.target = Target::PostgresUnencrypted(Box::new(postgres));
    config.limits.timeout = Duration::from_millis(100);
    let registering = registry.clone();
    let request = tokio::spawn(async move { registering.register("db", config).await });
    let (socket, _) = listener.accept().await.unwrap();
    assert_eq!(registry.status("db").unwrap().phase, Phase::Registering);
    assert_eq!(
        registry.reload("db").unwrap_err().code,
        "operation_in_progress"
    );
    let status = serde_json::to_string(&registry.status("db").unwrap()).unwrap();
    assert!(!status.contains("private"));
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    tokio::time::timeout(Duration::from_secs(2), async {
        while registry.status("db").is_ok() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(socket);
    registry
        .register("db", fixture.config.clone())
        .await
        .unwrap();
}
