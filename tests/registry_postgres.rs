use serde_json::{Value, json};
use sqlrest::{
    execution::Limits,
    params::Input,
    registry::{Configuration, OperationId, Outcome, Registry, Target},
};
use std::{fs, time::Duration};

const SCHEMA: &str = r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#;

struct Fixture {
    _directory: tempfile::TempDir,
    config: Configuration,
}

impl Fixture {
    fn new(postgres: tokio_postgres::Config) -> Self {
        let directory = tempfile::tempdir().unwrap();
        let config = Configuration {
            target: Target::PostgresUnencrypted(Box::new(postgres)),
            interfaces: directory.path().join("interfaces"),
            migrations: directory.path().join("migrations"),
            limits: Limits {
                timeout: Duration::from_secs(30),
                max_rows: 10,
            },
        };
        fs::create_dir(&config.interfaces).unwrap();
        let fixture = Self {
            _directory: directory,
            config,
        };
        fixture.deploy();
        fixture
    }

    fn deploy(&self) {
        fs::write(
            self.config.interfaces.join("get.sql"),
            "SELECT value FROM items ORDER BY value",
        )
        .unwrap();
        fs::write(self.config.interfaces.join("get.response.yaml"), SCHEMA).unwrap();
    }
}

async fn successful(registry: &Registry, name: &str, id: OperationId) {
    let op = tokio::time::timeout(Duration::from_secs(6), registry.wait_operation(name, id))
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

fn base_config() -> tokio_postgres::Config {
    std::env::var("SQLREST_TEST_POSTGRES")
        .expect("Set SQLREST_TEST_POSTGRES to a disposable database")
        .parse()
        .unwrap()
}

async fn connection(config: &tokio_postgres::Config) -> tokio_postgres::Client {
    let (client, connection) = config.connect(tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; creates isolated databases"]
async fn multiple_databases_on_one_endpoint_and_reregistration() {
    let registry = Registry::new();
    let base = base_config();
    let admin = connection(&base).await;
    let mut fixtures = Vec::new();
    for index in 0..2 {
        let database = format!("sqlrest_registry_{}_{}", std::process::id(), index);
        admin
            .batch_execute(&format!("CREATE DATABASE {database}"))
            .await
            .unwrap();
        let mut config = base.clone();
        config.dbname(&database);
        let fixture = Fixture::new(config);
        fs::write(
            fixture.config.interfaces.join("post.sql"),
            "CREATE TABLE IF NOT EXISTS items(value TEXT); INSERT INTO items VALUES('persisted')",
        )
        .unwrap();
        registry
            .register(&database, fixture.config.clone())
            .await
            .unwrap();
        successful(&registry, &database, registry.reload(&database).unwrap()).await;
        registry
            .execute(&database, "post", &[], Input::default())
            .await
            .unwrap();
        fixtures.push((database, fixture));
    }
    let (first, fixture) = &fixtures[0];
    registry
        .execute(first, "post", &[], Input::default())
        .await
        .unwrap();
    assert_eq!(
        get(&registry, first).await["records"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        get(&registry, &fixtures[1].0).await["records"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let old = registry.openapi(first, "/db/first").unwrap();
    fs::write(
        fixture.config.interfaces.join("get.sql"),
        "SELECT ${invalid}",
    )
    .unwrap();
    let failed = registry.reload(first).unwrap();
    assert_eq!(
        registry
            .wait_operation(first, failed)
            .await
            .unwrap()
            .outcome,
        Outcome::Failed
    );
    assert_eq!(registry.openapi(first, "/db/first").unwrap(), old);
    assert_eq!(
        get(&registry, first).await["records"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    fixture.deploy();
    successful(&registry, first, registry.unregister(first).unwrap()).await;
    registry
        .register(first, fixture.config.clone())
        .await
        .unwrap();
    successful(&registry, first, registry.reload(first).unwrap()).await;
    assert_eq!(
        get(&registry, first).await["records"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    for (name, _) in &fixtures {
        successful(&registry, name, registry.unregister(name).unwrap()).await;
    }
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn unregister_waits_for_caller_cancel_and_real_rollback() {
    let registry = Registry::new();
    let mut config = base_config();
    let observer = connection(&config).await;
    let schema = format!("sqlrest_registry_cancel_{}", std::process::id());
    observer
        .batch_execute(&format!(
            "CREATE SCHEMA {schema}; CREATE TABLE {schema}.items(value TEXT)"
        ))
        .await
        .unwrap();
    config
        .options(format!("-c search_path={schema}"))
        .application_name(&schema);
    let fixture = Fixture::new(config);
    fs::write(fixture.config.interfaces.join("post.sql"),
        "INSERT INTO items VALUES('must rollback'); SELECT sum(x)::bigint AS total FROM generate_series(1,1000000000) x").unwrap();
    fs::write(fixture.config.interfaces.join("post.response.yaml"),
        r#"{"type":"object","properties":{"total":{"type":"integer"}},"required":["total"],"additionalProperties":false}"#).unwrap();
    registry
        .register("db", fixture.config.clone())
        .await
        .unwrap();
    successful(&registry, "db", registry.reload("db").unwrap()).await;
    let request_registry = registry.clone();
    let request = tokio::spawn(async move {
        request_registry
            .execute("db", "post", &[], Input::default())
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let running: bool = observer.query_one(
                "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE application_name=$1 AND state='active' AND query LIKE '%generate_series%')", &[&schema]
            ).await.unwrap().get(0);
            if running { break; }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }).await.expect("must observe actual SQL execution");
    let id = registry.unregister("db").unwrap();
    assert_eq!(registry.status("db").unwrap().active_requests, 1);
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
    request.abort();
    assert!(request.await.unwrap_err().is_cancelled());
    successful(&registry, "db", id).await;
    let rows: i64 = observer
        .query_one(&format!("SELECT count(*) FROM {schema}.items"), &[])
        .await
        .unwrap()
        .get(0);
    assert_eq!(rows, 0);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let sessions: i64 = observer
                .query_one(
                    "SELECT count(*) FROM pg_stat_activity WHERE application_name=$1",
                    &[&schema],
                )
                .await
                .unwrap()
                .get(0);
            if sessions == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("unregister must release the database connection");
    registry
        .register("db", fixture.config.clone())
        .await
        .unwrap();
    successful(&registry, "db", registry.reload("db").unwrap()).await;
    assert_eq!(get(&registry, "db").await, json!({"records":[]}));
    successful(&registry, "db", registry.unregister("db").unwrap()).await;
}
