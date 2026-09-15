use serde_json::{Value, json};
use sqlrest::{
    params::Input,
    registry::{DatabaseConfig, OperationId, Outcome, Phase, PublishRequest, Recovery, Registry},
};
use std::{fs, path::Path, time::Duration};

const SCHEMA: &str = r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#;

async fn connect(url: &str) -> tokio_postgres::Client {
    let (client, driver) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    client
}

async fn create_database(suffix: &str) -> String {
    let base = std::env::var("SQLREST_TEST_POSTGRES").expect("disposable PostgreSQL URL required");
    let admin = connect(&base).await;
    let name = format!("registry_{}_{}", std::process::id(), suffix);
    admin
        .batch_execute(&format!("CREATE DATABASE {name}"))
        .await
        .unwrap();
    let mut url = url::Url::parse(&base).unwrap();
    url.set_path(&name);
    url.into()
}

fn layout(root: &Path, name: &str) {
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
        "CREATE TABLE items(value TEXT); INSERT INTO items VALUES('initial')",
    )
    .unwrap();
}

fn request(connection: &str) -> PublishRequest {
    PublishRequest {
        database: Some(DatabaseConfig::PostgresUnencrypted {
            connection: connection.into(),
        }),
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
#[ignore = "requires disposable PostgreSQL"]
async fn connection_replacement_and_restart_use_the_new_database_history() {
    let first = create_database("first").await;
    let second = create_database("second").await;
    let root = tempfile::tempdir().unwrap();
    layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    successful(
        &registry,
        "app",
        registry.publish("app", request(&first)).unwrap(),
    )
    .await;
    connect(&first)
        .await
        .batch_execute("INSERT INTO items VALUES('old data')")
        .await
        .unwrap();
    successful(
        &registry,
        "app",
        registry.publish("app", request(&second)).unwrap(),
    )
    .await;
    assert_eq!(
        get(&registry, "app").await,
        json!({"records":[{"value":"initial"}]})
    );
    assert_eq!(registry.export_migrations("app").await.unwrap().len(), 1);
    assert_eq!(
        connect(&first)
            .await
            .query_one("SELECT count(*) FROM items", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    let status = serde_json::to_string(&registry.status("app").unwrap()).unwrap();
    assert!(!status.contains(&second));
    registry.shutdown().await.unwrap();
    drop(registry);
    let registry = Registry::open(root.path()).await.unwrap();
    assert_eq!(
        get(&registry, "app").await,
        json!({"records":[{"value":"initial"}]})
    );
    registry.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn failed_publish_after_target_switch_never_falls_back_to_old_database() {
    let first = create_database("switch_old").await;
    let second = create_database("switch_new").await;
    let root = tempfile::tempdir().unwrap();
    layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    successful(
        &registry,
        "app",
        registry.publish("app", request(&first)).unwrap(),
    )
    .await;
    let source = root.path().join("databases/app/interfaces/get.sql");
    fs::write(&source, "SELECT ${invalid}").unwrap();
    let id = registry.publish("app", request(&second)).unwrap();
    assert_eq!(
        registry.wait_operation("app", id).await.unwrap().outcome,
        Outcome::Failed
    );
    assert_eq!(
        registry.status("app").unwrap().phase,
        Phase::RecoveryRequired
    );
    assert_eq!(
        registry.status("app").unwrap().recovery,
        Some(Recovery::Reload)
    );
    assert!(
        fs::read_to_string(root.path().join("databases/app/database.toml"))
            .unwrap()
            .contains(&second)
    );
    registry.shutdown().await.unwrap();
    drop(registry);
    let registry = Registry::open(root.path()).await.unwrap();
    fs::write(source, "SELECT value FROM items").unwrap();
    successful(
        &registry,
        "app",
        registry.publish("app", PublishRequest::default()).unwrap(),
    )
    .await;
    assert_eq!(
        get(&registry, "app").await["records"][0]["value"],
        "initial"
    );
    registry.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn migration_deadline_is_one_batch_not_per_file() {
    let connection = create_database("deadline").await;
    let root = tempfile::tempdir().unwrap();
    layout(root.path(), "app");
    let registry = Registry::open(root.path()).await.unwrap();
    successful(
        &registry,
        "app",
        registry.publish("app", request(&connection)).unwrap(),
    )
    .await;
    let migrations = root.path().join("databases/app/migrations");
    for version in [2, 3] {
        fs::write(
            migrations.join(format!("000{version}_sleep.sql")),
            format!("INSERT INTO items VALUES('migration {version}'); SELECT pg_sleep(2);"),
        )
        .unwrap();
    }
    let id = registry
        .publish(
            "app",
            PublishRequest {
                migration_timeout_ms: 3200,
                ..Default::default()
            },
        )
        .unwrap();
    let op = registry.wait_operation("app", id).await.unwrap();
    assert_eq!(op.outcome, Outcome::Failed);
    assert_eq!(op.error.unwrap().code, "execution_timeout");
    assert_eq!(op.publish.unwrap().applied_versions, [2]);
    assert_eq!(registry.export_migrations("app").await.unwrap().len(), 2);
    assert_eq!(
        connect(&connection)
            .await
            .query_one("SELECT count(*) FROM items", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    successful(
        &registry,
        "app",
        registry.publish("app", PublishRequest::default()).unwrap(),
    )
    .await;
    registry.shutdown().await.unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn unregister_waits_for_caller_cancel_and_real_rollback() {
    let connection = create_database("cancel").await;
    let observer = connect(&connection).await;
    let root = tempfile::tempdir().unwrap();
    layout(root.path(), "app");
    let interfaces = root.path().join("databases/app/interfaces");
    fs::write(
        interfaces.join("post.sql"),
        "INSERT INTO items VALUES('must rollback'); SELECT pg_sleep(30); INSERT INTO items VALUES('after sleep')",
    )
    .unwrap();
    let registry = Registry::open(root.path()).await.unwrap();
    let mut config = request(&connection);
    config.limits.request_timeout_ms = 60000;
    successful(&registry, "app", registry.publish("app", config).unwrap()).await;
    let cloned = registry.clone();
    let request =
        tokio::spawn(async move { cloned.execute("app", "post", &[], Input::default()).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let active: bool = observer.query_one(
                "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE datname=current_database() AND pid<>pg_backend_pid() AND state='active' AND query LIKE '%pg_sleep%')", &[]
            ).await.unwrap().get(0);
            if active { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.unwrap();
    let id = registry.unregister("app").unwrap();
    assert_eq!(
        registry.operation("app", id).unwrap().outcome,
        Outcome::Running
    );
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    successful(&registry, "app", id).await;
    assert_eq!(
        observer
            .query_one("SELECT count(*) FROM items", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    assert!(!root.path().join("databases/app/database.toml").exists());
    registry.shutdown().await.unwrap();
}
