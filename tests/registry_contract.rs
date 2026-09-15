use serde_json::{Value, json};
use sqlrest::{
    params::Input,
    registry::{
        DatabaseConfig, OperationId, Outcome, Phase, PublishRequest, Recovery, Registry,
        RequestLimits,
    },
};
use std::{
    fs,
    path::{Path, PathBuf},
    time::Duration,
};

const SCHEMA: &str = r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#;

fn layout(root: &Path, name: &str) -> PathBuf {
    let db = root.join("databases").join(name);
    fs::create_dir_all(db.join("interfaces")).unwrap();
    fs::create_dir_all(db.join("migrations")).unwrap();
    fs::write(
        db.join("interfaces/get.sql"),
        "SELECT value FROM items ORDER BY value",
    )
    .unwrap();
    fs::write(db.join("interfaces/get.response.yaml"), SCHEMA).unwrap();
    fs::write(
        db.join("migrations/0001_initial.sql"),
        "CREATE TABLE items(value TEXT); INSERT INTO items VALUES('one');",
    )
    .unwrap();
    db
}

fn first() -> PublishRequest {
    PublishRequest {
        database: Some(DatabaseConfig::Turso {}),
        ..Default::default()
    }
}

async fn successful(registry: &Registry, name: &str, id: OperationId) {
    let op = tokio::time::timeout(Duration::from_secs(15), registry.wait_operation(name, id))
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

#[tokio::test]
async fn publish_persists_and_restart_needs_no_runtime_replay() {
    let root = tempfile::tempdir().unwrap();
    let db = layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    let id = registry.publish("app", first()).unwrap();
    successful(&registry, "app", id).await;
    assert_eq!(
        get(&registry, "app").await,
        json!({"records":[{"value":"one"}]})
    );
    let contents = fs::read_to_string(db.join("database.toml")).unwrap();
    assert!(contents.contains("request_timeout_ms = 5000"));
    assert!(contents.contains("max_rows = 1000"));
    assert!(contents.contains("recovery = \"none\""));
    registry.shutdown().await.unwrap();
    assert!(db.join("database.toml").is_file());
    drop(registry);
    // Restart loads current source, even without a previous explicit publish.
    fs::write(db.join("interfaces/get.sql"), "SELECT 'restart' AS value").unwrap();
    let registry = Registry::open(root.path()).await.unwrap();
    assert_eq!(registry.status("app").unwrap().phase, Phase::Ready);
    assert_eq!(
        get(&registry, "app").await,
        json!({"records":[{"value":"restart"}]})
    );
    assert_eq!(registry.operation("app", id).unwrap_err().status, 404);
    let next = registry.publish("app", PublishRequest::default()).unwrap();
    assert_ne!(id, next);
    successful(&registry, "app", next).await;
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn unregister_retains_files_and_requires_explicit_republication() {
    let root = tempfile::tempdir().unwrap();
    let db = layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    successful(&registry, "app", registry.publish("app", first()).unwrap()).await;
    let id = registry.unregister("app").unwrap();
    assert!(id.to_string().starts_with("unregister-"));
    successful(&registry, "app", id).await;
    assert!(!db.join("database.toml").exists());
    assert!(db.join("data.db").is_file());
    assert!(db.join("interfaces/get.sql").is_file());
    registry.shutdown().await.unwrap();
    drop(registry);
    let registry = Registry::open(root.path()).await.unwrap();
    assert_eq!(registry.status("app").unwrap_err().status, 404);
    successful(&registry, "app", registry.publish("app", first()).unwrap()).await;
    assert_eq!(registry.export_migrations("app").await.unwrap().len(), 1);
    assert_eq!(
        get(&registry, "app").await["records"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn limits_and_snapshot_change_together_and_omissions_reset_defaults() {
    let root = tempfile::tempdir().unwrap();
    let db = layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    let request = PublishRequest {
        limits: RequestLimits {
            max_rows: 1,
            request_timeout_ms: 700,
        },
        ..first()
    };
    successful(&registry, "app", registry.publish("app", request).unwrap()).await;
    let old = registry.status("app").unwrap().version;
    let persisted = fs::read(db.join("database.toml")).unwrap();
    fs::write(db.join("interfaces/get.sql"), "SELECT ${invalid}").unwrap();
    let id = registry.publish("app", PublishRequest::default()).unwrap();
    assert_eq!(
        registry.wait_operation("app", id).await.unwrap().outcome,
        Outcome::Failed
    );
    assert_eq!(registry.status("app").unwrap().version, old);
    assert_eq!(registry.status("app").unwrap().limits.unwrap().max_rows, 1);
    assert_eq!(fs::read(db.join("database.toml")).unwrap(), persisted);
    assert_eq!(get(&registry, "app").await["records"][0]["value"], "one");
    fs::write(
        db.join("interfaces/get.sql"),
        "SELECT 'two' AS value UNION ALL SELECT 'three'",
    )
    .unwrap();
    successful(
        &registry,
        "app",
        registry.publish("app", PublishRequest::default()).unwrap(),
    )
    .await;
    assert_eq!(
        registry.status("app").unwrap().limits.unwrap(),
        RequestLimits::default()
    );
    assert_eq!(
        get(&registry, "app").await["records"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn startup_errors_are_isolated_and_missing_data_is_never_created() {
    let root = tempfile::tempdir().unwrap();
    let good = layout(root.path(), "good");
    let missing = layout(root.path(), "missing");
    let registry = Registry::open(root.path()).await.unwrap();
    for name in ["good", "missing"] {
        successful(&registry, name, registry.publish(name, first()).unwrap()).await;
    }
    registry.shutdown().await.unwrap();
    drop(registry);
    fs::remove_file(missing.join("data.db")).unwrap();
    let bad = layout(root.path(), "bad");
    fs::write(bad.join("database.toml"), "not valid = [").unwrap();
    let _unregistered = layout(root.path(), "ignored");
    let registry = Registry::open(root.path()).await.unwrap();
    assert_eq!(get(&registry, "good").await["records"][0]["value"], "one");
    assert_eq!(
        registry.status("missing").unwrap().phase,
        Phase::RecoveryRequired
    );
    assert!(registry.status("bad").unwrap().error.is_some());
    assert_eq!(registry.status("ignored").unwrap_err().status, 404);
    assert!(registry.publish("bad", first()).is_err());
    let id = registry
        .publish("missing", PublishRequest::default())
        .unwrap();
    assert_eq!(
        registry
            .wait_operation("missing", id)
            .await
            .unwrap()
            .outcome,
        Outcome::Failed
    );
    assert!(!missing.join("data.db").exists());
    assert!(good.join("data.db").exists());
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_first_publish_and_committed_migration_remain_blocked_after_restart() {
    let root = tempfile::tempdir().unwrap();
    let db = layout(root.path(), "app");
    fs::write(db.join("interfaces/get.sql"), "SELECT ${invalid}").unwrap();
    let registry = Registry::open(root.path()).await.unwrap();
    let id = registry.publish("app", first()).unwrap();
    assert_eq!(
        registry.wait_operation("app", id).await.unwrap().outcome,
        Outcome::Failed
    );
    assert_eq!(
        registry.status("app").unwrap().recovery,
        Some(Recovery::Reload)
    );
    assert_eq!(registry.export_migrations("app").await.unwrap().len(), 1);
    registry.shutdown().await.unwrap();
    drop(registry);
    fs::write(db.join("interfaces/get.sql"), "SELECT value FROM items").unwrap();
    let registry = Registry::open(root.path()).await.unwrap();
    assert_eq!(
        registry.status("app").unwrap().phase,
        Phase::RecoveryRequired
    );
    assert_eq!(
        registry
            .execute("app", "get", &[], Input::default())
            .await
            .unwrap_err()
            .status,
        503
    );
    successful(
        &registry,
        "app",
        registry.publish("app", PublishRequest::default()).unwrap(),
    )
    .await;
    assert_eq!(get(&registry, "app").await["records"][0]["value"], "one");
    registry.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn restart_rejects_managed_directory_symlinks_and_publish_repairs_isolated_entries() {
    let root = tempfile::tempdir().unwrap();
    let external = tempfile::tempdir().unwrap();
    layout(root.path(), "good");
    for name in ["interfaces", "migrations"] {
        layout(root.path(), name);
    }
    let registry = Registry::open(root.path()).await.unwrap();
    for name in ["good", "interfaces", "migrations"] {
        successful(&registry, name, registry.publish(name, first()).unwrap()).await;
    }
    registry.shutdown().await.unwrap();
    drop(registry);

    for name in ["interfaces", "migrations"] {
        let managed = root.path().join("databases").join(name).join(name);
        let outside = external.path().join(name);
        fs::rename(&managed, &outside).unwrap();
        std::os::unix::fs::symlink(outside, managed).unwrap();
    }
    let registry = Registry::open(root.path()).await.unwrap();
    assert_eq!(get(&registry, "good").await["records"][0]["value"], "one");
    for name in ["interfaces", "migrations"] {
        let status = registry.status(name).unwrap();
        assert_eq!(status.phase, Phase::RecoveryRequired);
        assert!(status.error.is_some());
        assert_eq!(
            registry
                .execute(name, "get", &[], Input::default())
                .await
                .unwrap_err()
                .status,
            503
        );
        let managed = root.path().join("databases").join(name).join(name);
        fs::remove_file(&managed).unwrap();
        fs::rename(external.path().join(name), managed).unwrap();
        successful(
            &registry,
            name,
            registry.publish(name, PublishRequest::default()).unwrap(),
        )
        .await;
        assert_eq!(get(&registry, name).await["records"][0]["value"], "one");
    }
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn broken_candidate_connection_does_not_replace_live_service() {
    let root = tempfile::tempdir().unwrap();
    let db = layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    successful(&registry, "app", registry.publish("app", first()).unwrap()).await;
    let persisted = fs::read(db.join("database.toml")).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let request = PublishRequest {
        database: Some(DatabaseConfig::PostgresUnencrypted {
            connection: format!("host=127.0.0.1 port={port} user=test dbname=test"),
        }),
        ..Default::default()
    };
    let id = registry.publish("app", request).unwrap();
    let op = registry.wait_operation("app", id).await.unwrap();
    assert_eq!(op.outcome, Outcome::Failed);
    assert!(op.error.unwrap().diagnostic().is_some());
    assert_eq!(fs::read(db.join("database.toml")).unwrap(), persisted);
    assert_eq!(get(&registry, "app").await["records"][0]["value"], "one");
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn migration_failure_preserves_prior_limits_and_durable_history() {
    let root = tempfile::tempdir().unwrap();
    let db = layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    successful(&registry, "app", registry.publish("app", first()).unwrap()).await;
    fs::write(
        db.join("migrations/0002_ok.sql"),
        "INSERT INTO items VALUES('two')",
    )
    .unwrap();
    fs::write(
        db.join("migrations/0003_bad.sql"),
        "INSERT INTO nonexistent VALUES(1)",
    )
    .unwrap();
    let request = PublishRequest {
        limits: RequestLimits {
            request_timeout_ms: 12,
            max_rows: 2,
        },
        ..Default::default()
    };
    let id = registry.publish("app", request).unwrap();
    assert_eq!(
        registry.wait_operation("app", id).await.unwrap().outcome,
        Outcome::Failed
    );
    assert_eq!(
        registry.status("app").unwrap().limits.unwrap(),
        RequestLimits::default()
    );
    assert_eq!(
        registry.status("app").unwrap().recovery,
        Some(Recovery::Migration)
    );
    assert_eq!(registry.export_migrations("app").await.unwrap().len(), 2);
    registry.shutdown().await.unwrap();
    drop(registry);
    fs::write(
        db.join("migrations/0003_bad.sql"),
        "INSERT INTO items VALUES('three')",
    )
    .unwrap();
    let registry = Registry::open(root.path()).await.unwrap();
    assert_eq!(
        registry.status("app").unwrap().recovery,
        Some(Recovery::Migration)
    );
    successful(
        &registry,
        "app",
        registry.publish("app", PublishRequest::default()).unwrap(),
    )
    .await;
    assert_eq!(
        get(&registry, "app").await["records"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    registry.shutdown().await.unwrap();
}

#[tokio::test]
async fn workspace_lock_and_management_exclusion_survive_waiter_cancellation() {
    let root = tempfile::tempdir().unwrap();
    layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    assert!(Registry::open(root.path()).await.is_err());
    let id = registry.publish("app", first()).unwrap();
    assert_eq!(
        registry.publish("app", first()).unwrap_err().code,
        "operation_in_progress"
    );
    assert_eq!(
        registry.unregister("app").unwrap_err().code,
        "operation_in_progress"
    );
    let waiter = {
        let registry = registry.clone();
        tokio::spawn(async move { registry.wait_operation("app", id).await })
    };
    waiter.abort();
    successful(&registry, "app", id).await;
    registry.shutdown().await.unwrap();
    assert_eq!(
        registry.publish("app", first()).unwrap_err().code,
        "server_shutting_down"
    );
    drop(registry);
    let _ = waiter.await;
    let reopened = Registry::open(root.path()).await.unwrap();
    reopened.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn fixed_paths_reject_aliases_and_hardlinked_database_ownership() {
    let root = tempfile::tempdir().unwrap();
    let first_db = layout(root.path(), "first");
    let second_db = layout(root.path(), "second");
    let registry = Registry::open(root.path()).await.unwrap();
    successful(
        &registry,
        "first",
        registry.publish("first", first()).unwrap(),
    )
    .await;
    fs::hard_link(first_db.join("data.db"), second_db.join("data.db")).unwrap();
    let id = registry.publish("second", first()).unwrap();
    assert_eq!(
        registry
            .wait_operation("second", id)
            .await
            .unwrap()
            .error
            .unwrap()
            .code,
        "database_already_registered"
    );
    std::os::unix::fs::symlink(&first_db, root.path().join("databases/alias")).unwrap();
    let id = registry.publish("alias", first()).unwrap();
    assert_eq!(
        registry.wait_operation("alias", id).await.unwrap().outcome,
        Outcome::Failed
    );
    registry.shutdown().await.unwrap();
}
