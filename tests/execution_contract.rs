use serde_json::{Value, json};
use sqlrest::{
    execution::{Executor, Limits},
    loader::{Endpoint, Snapshot},
    params::Input,
    sql::Backend,
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio_postgres::NoTls;
use turso_sdk_kit::rsapi::TursoConnection;

const RECORD: &str = r#"{"type":"object","required":["id","title"],"properties":{"id":{"type":"integer"},"title":{"type":"string"}},"additionalProperties":false}"#;
const ID: &str = r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}},"additionalProperties":false}"#;
static NEXT: AtomicU64 = AtomicU64::new(1);

enum Observer {
    Turso(Arc<TursoConnection>),
    Postgres(tokio_postgres::Client),
}

struct Harness {
    executor: Executor,
    backend: Backend,
    observer: Observer,
    _directory: Option<tempfile::TempDir>,
}

impl Harness {
    async fn new(backend: Backend) -> Self {
        let harness = match backend {
            Backend::Turso => {
                let directory = tempfile::tempdir().unwrap();
                let database =
                    sqlrest::turso_driver::open(&directory.path().join("test.db")).unwrap();
                Self {
                    executor: Executor::turso(database.clone()),
                    backend,
                    observer: Observer::Turso(database.connect().unwrap()),
                    _directory: Some(directory),
                }
            }
            Backend::Postgres => {
                let url = std::env::var("SQLREST_TEST_POSTGRES")
                    .expect("Set SQLREST_TEST_POSTGRES to a disposable database");
                let mut config: tokio_postgres::Config = url.parse().unwrap();
                let (client, connection) = config.connect(NoTls).await.unwrap();
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                let schema = format!(
                    "sqlrest_execution_{}_{}",
                    std::process::id(),
                    NEXT.fetch_add(1, Ordering::Relaxed)
                );
                client
                    .batch_execute(&format!("CREATE SCHEMA {schema}"))
                    .await
                    .unwrap();
                config.options(format!("-c search_path={schema}"));
                config.application_name(&schema);
                let (observer, connection) = config.connect(NoTls).await.unwrap();
                tokio::spawn(async move {
                    let _ = connection.await;
                });
                Self {
                    executor: Executor::postgres_unencrypted(config),
                    backend,
                    observer: Observer::Postgres(observer),
                    _directory: None,
                }
            }
        };
        harness
            .raw("CREATE TABLE items(id BIGINT PRIMARY KEY, title TEXT NOT NULL)")
            .await;
        harness
            .raw("INSERT INTO items VALUES(1,'one'),(2,'two')")
            .await;
        harness
    }

    async fn raw(&self, sql: &str) {
        match &self.observer {
            Observer::Turso(connection) => sqlrest::turso_driver::execute(
                connection,
                sql,
                Instant::now() + Duration::from_secs(5),
            )
            .unwrap(),
            Observer::Postgres(client) => client.batch_execute(sql).await.unwrap(),
        }
    }

    async fn scalar(&self, sql: &str) -> i64 {
        match &self.observer {
            Observer::Turso(connection) => {
                let mut statement = connection.prepare_single(sql).unwrap();
                statement.step(None).unwrap();
                statement.row_value(0).unwrap().to_string().parse().unwrap()
            }
            Observer::Postgres(client) => client.query_one(sql, &[]).await.unwrap().get(0),
        }
    }

    fn endpoint(&self, method: &str, sql: &str, schema: Option<&str>) -> Arc<Endpoint> {
        let mut files = BTreeMap::from([(format!("{method}.sql"), sql.into())]);
        if let Some(schema) = schema {
            files.insert(format!("{method}.response.yaml"), schema.into());
        }
        Snapshot::from_files(files, self.backend)
            .unwrap()
            .endpoints()[0]
            .clone()
    }

    async fn run(
        &self,
        method: &str,
        sql: &str,
        schema: Option<&str>,
        body: Value,
        max_rows: usize,
        timeout_ms: u64,
    ) -> Result<Value, sqlrest::SqlrestError> {
        let endpoint = self.endpoint(method, sql, schema);
        let input = Input::from_http("", &serde_json::to_vec(&body).unwrap()).unwrap();
        let bytes = self
            .executor
            .execute(
                endpoint,
                input,
                Limits {
                    timeout: Duration::from_millis(timeout_ms),
                    max_rows,
                },
            )
            .await?;
        Ok(serde_json::from_slice(&bytes).unwrap())
    }

    fn cpu_sql(&self) -> &'static str {
        match self.backend {
            Backend::Turso => {
                "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) AS id FROM n"
            }
            Backend::Postgres => "SELECT sum(x)::bigint AS id FROM generate_series(1,1000000000) x",
        }
    }

    async fn writer_running(&self) -> bool {
        match &self.observer {
            Observer::Turso(connection) => {
                connection.set_busy_timeout(Duration::ZERO);
                let result = connection.prepare_single("BEGIN IMMEDIATE").unwrap().step(None);
                match result {
                    Err(turso_sdk_kit::rsapi::TursoError::Busy(_)) => true,
                    Ok(_) => { self.raw("ROLLBACK").await; false },
                    other => panic!("unexpected lock observation: {other:?}"),
                }
            },
            Observer::Postgres(client) => {
                client.query_one("SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND application_name=current_setting('application_name') AND state='active' AND query LIKE '%generate_series%')",&[]).await.unwrap().get(0)
            }
        }
    }
}

async fn contracts(backend: Backend) {
    let h = Harness::new(backend).await;
    let rows = h
        .run(
            "get",
            "SELECT id,title FROM items ORDER BY id",
            Some(RECORD),
            json!({}),
            2,
            2000,
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        json!({"records":[{"id":1,"title":"one"},{"id":2,"title":"two"}]})
    );
    let empty = h
        .run(
            "get",
            "SELECT id,title FROM items WHERE false",
            Some(RECORD),
            json!({}),
            0,
            2000,
        )
        .await
        .unwrap();
    assert_eq!(empty, json!({"records":[]}));
    let error = h
        .run(
            "get",
            "SELECT id AS wrong,title FROM items WHERE false",
            Some(RECORD),
            json!({}),
            2,
            2000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "response_contract_mismatch");
    let error = h
        .run(
            "get",
            "SELECT id,id AS id FROM items WHERE false",
            Some(ID),
            json!({}),
            2,
            2000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "response_contract_mismatch");
    let schema = json!({"type":"object","properties":{"id":{"type":"integer"},"title":{"type":"string"},"flag":{"type":"boolean"},"amount":{"type":"number"},"note":{"type":["string","null"]},"ids":{"type":"array","items":{"type":"integer"}}}});
    let echo = h.run("post","SELECT ${body.id:int64} AS id, ${body.title:string} AS title, ${body.flag:boolean} AS flag, ${body.amount:float64} AS amount, ${body.note:nullable<string>} AS note, ${body.ids:array<int64>} AS ids",Some(&schema.to_string()),json!({"id":i64::MAX,"title":"test","flag":true,"amount":2,"note":null,"ids":[1,2]}),1,2000).await.unwrap();
    assert_eq!(echo["records"][0]["id"], json!(i64::MAX));
    assert_eq!(echo["records"][0]["flag"], true);
    assert_eq!(echo["records"][0]["amount"].as_f64(), Some(2.0));
    assert!(echo["records"][0]["note"].is_null());
    assert_eq!(echo["records"][0]["ids"], json!([1, 2]));
    if backend == Backend::Turso {
        let record = r#"{"type":"object","properties":{"kind":{"type":"string"}}}"#;
        let value = h
            .run(
                "post",
                "SELECT typeof(${body.amount:float64}) AS kind",
                Some(record),
                json!({"amount":2}),
                1,
                2000,
            )
            .await
            .unwrap();
        assert_eq!(value["records"][0]["kind"], "real");
    } else {
        let error = h
            .run(
                "get",
                "SELECT 1.2::numeric AS id WHERE false",
                Some(ID),
                json!({}),
                0,
                2000,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "response_contract_mismatch");
    }
}

async fn rollback(backend: Backend) {
    let h = Harness::new(backend).await;
    let error = h
        .run(
            "post",
            "INSERT INTO items VALUES(9,'nine'); SELECT ${body.missing:int64} AS id",
            Some(ID),
            json!({}),
            20,
            2000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "parameter_missing");
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id=9").await, 0);
    let nested = r#"{"type":"object","properties":{"details":{"type":"object","required":["flag"],"properties":{"flag":{"type":"boolean"}}}}}"#;
    let value = h
        .run(
            "get",
            r#"SELECT '{"flag":false}' AS details"#,
            Some(nested),
            json!({}),
            1,
            2000,
        )
        .await
        .unwrap();
    assert_eq!(value["records"][0]["details"], json!({"flag":false}));
    let error = h
        .run(
            "post",
            r#"INSERT INTO items VALUES(9,'nine'); SELECT '{"flag":0}' AS details"#,
            Some(nested),
            json!({}),
            1,
            2000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "response_contract_mismatch");
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id=9").await, 0);
    let bad_schema =
        r#"{"type":"object","properties":{"id":{"type":"integer"},"title":{"type":"boolean"}}}"#;
    let error = h
        .run(
            "post",
            "INSERT INTO items VALUES(9,'nine'); SELECT id,title FROM items ORDER BY id",
            Some(bad_schema),
            json!({}),
            20,
            2000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "response_contract_mismatch");
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id=9").await, 0);
    let error = h
        .run(
            "post",
            "INSERT INTO items VALUES(9,'nine'),(10,'ten') RETURNING id,title",
            Some(RECORD),
            json!({}),
            1,
            2000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "row_limit_exceeded");
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id>=9").await, 0);
    // Intermediate SELECT/RETURNING rows are consumed but neither returned nor
    // counted against the final-result limit.
    let success = h.run("post","INSERT INTO items VALUES(9,'nine') RETURNING id,title; SELECT id,title FROM items; INSERT INTO items VALUES(10,'ten')",None,json!({}),0,2000).await.unwrap();
    assert_eq!(success, json!({"records":[]}));
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id>=9").await, 2);
    let error = h
        .run(
            "post",
            "INSERT INTO items VALUES(9,'duplicate')",
            None,
            json!({}),
            1,
            2000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "constraint_violation");
    assert_eq!(error.status, 409);
    assert!(!error.message.contains("items"));
    for method in ["get", "head"] {
        let error = h
            .run(method, "DELETE FROM items", None, json!({}), 0, 2000)
            .await
            .unwrap_err();
        assert_eq!(error.code, "read_only_violation");
    }
    assert_eq!(h.scalar("SELECT count(*) FROM items").await, 4);
    h.raw("CREATE TABLE parents(id BIGINT PRIMARY KEY)").await;
    h.raw("CREATE TABLE children(parent_id BIGINT REFERENCES parents(id))")
        .await;
    let error = h
        .run(
            "post",
            "INSERT INTO items VALUES(20,'before constraint'); INSERT INTO children VALUES(999)",
            None,
            json!({}),
            0,
            2000,
        )
        .await
        .unwrap_err();
    assert_eq!(error.code, "constraint_violation");
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id=20").await, 0);
}

async fn timeout_and_cancellation(backend: Backend) {
    let h = Harness::new(backend).await;
    let sql = format!("INSERT INTO items VALUES(9,'nine'); {}", h.cpu_sql());
    let start = Instant::now();
    let executor = h.executor.clone();
    let endpoint = h.endpoint("post", &sql, Some(ID));
    let task = tokio::spawn(async move {
        executor
            .execute(
                endpoint,
                Input::default(),
                Limits {
                    timeout: Duration::from_secs(1),
                    max_rows: 10,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_millis(900), async {
        while !h.writer_running().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("must observe executing SQL before its deadline");
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.code, "execution_timeout", "{error}");
    assert!(start.elapsed() < Duration::from_secs(3));
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id=9").await, 0);
    // Confirm the write transaction is actually running before dropping caller.
    let executor = h.executor.clone();
    let endpoint = h.endpoint("post", &sql, Some(ID));
    let task = tokio::spawn(async move {
        executor
            .execute(
                endpoint,
                Input::default(),
                Limits {
                    timeout: Duration::from_secs(10),
                    max_rows: 10,
                },
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !h.writer_running().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let stopped = tokio::time::timeout(Duration::from_secs(3), async {
        while h.writer_running().await {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    if stopped.is_err()
        && let Observer::Postgres(client) = &h.observer
    {
        for row in client.query("SELECT pid,state,wait_event_type,wait_event,query FROM pg_stat_activity WHERE application_name=current_setting('application_name')",&[]).await.unwrap() {
            eprintln!("activity pid={} state={:?} wait={:?}/{:?} query={:?}",row.get::<_,i32>(0),row.get::<_,Option<String>>(1),row.get::<_,Option<String>>(2),row.get::<_,Option<String>>(3),row.get::<_,Option<String>>(4));
        }
    }
    stopped.unwrap();
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id=9").await, 0);
    h.run(
        "post",
        "INSERT INTO items VALUES(11,'after cancellation')",
        None,
        json!({}),
        0,
        2000,
    )
    .await
    .unwrap();
}

async fn lock_timeout(backend: Backend) {
    let h = Harness::new(backend).await;
    h.raw("BEGIN").await;
    h.raw("UPDATE items SET title='locked' WHERE id=1").await;
    let start = Instant::now();
    let executor = h.executor.clone();
    let endpoint = h.endpoint("post", "UPDATE items SET title='waiter' WHERE id=1", None);
    let task = tokio::spawn(async move {
        executor
            .execute(
                endpoint,
                Input::default(),
                Limits {
                    timeout: Duration::from_secs(1),
                    max_rows: 0,
                },
            )
            .await
    });
    if let Observer::Postgres(client) = &h.observer {
        tokio::time::timeout(Duration::from_millis(900), async {
            loop {
                client.batch_execute("SELECT pg_stat_clear_snapshot()").await.unwrap();
                let waiting: bool = client.query_one(
                    "SELECT EXISTS(SELECT FROM pg_stat_activity WHERE pid<>pg_backend_pid() AND application_name=current_setting('application_name') AND wait_event_type='Lock')", &[]
                ).await.unwrap().get(0);
                if waiting { break; }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }).await.expect("must observe the PostgreSQL lock wait before its deadline");
    }
    let error = task.await.unwrap().unwrap_err();
    assert_eq!(error.code, "execution_timeout", "{error}");
    assert!(start.elapsed() < Duration::from_secs(3));
    h.raw("ROLLBACK").await;
    h.run(
        "post",
        "UPDATE items SET title='after' WHERE id=1",
        None,
        json!({}),
        0,
        2000,
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_uses_one_deadline_for_all_statements() {
    let h = Harness::new(Backend::Postgres).await;
    let error = h.run("post",
        "INSERT INTO items VALUES(9,'nine'); SELECT pg_sleep(0.08); SELECT pg_sleep(0.08); SELECT pg_sleep(0.08); SELECT id FROM items",
        Some(ID),json!({}),10,180).await.unwrap_err();
    assert_eq!(error.code, "execution_timeout");
    assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id=9").await, 0);
}

#[test]
fn turso_queue_wait_is_in_the_request_budget() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .max_blocking_threads(1)
        .build()
        .unwrap();
    runtime.block_on(async {
        let h = Harness::new(Backend::Turso).await;
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocker = tokio::task::spawn_blocking(move || {
            started_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
        started_rx.await.unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(300),
            h.run(
                "post",
                "INSERT INTO items VALUES(9,'nine')",
                None,
                json!({}),
                0,
                30,
            ),
        )
        .await;
        // Always release before asserting, including on failure.
        release_tx.send(()).unwrap();
        blocker.await.unwrap();
        assert_eq!(result.unwrap().unwrap_err().code, "execution_timeout");
        assert_eq!(h.scalar("SELECT count(*) FROM items WHERE id=9").await, 0);
    });
}

#[tokio::test]
async fn turso_types_and_metadata() {
    contracts(Backend::Turso).await;
}

#[tokio::test]
async fn turso_atomicity_and_readonly() {
    rollback(Backend::Turso).await;
}

#[tokio::test]
async fn turso_deadline_and_cancellation() {
    timeout_and_cancellation(Backend::Turso).await;
}

#[tokio::test]
async fn turso_lock_wait() {
    lock_timeout(Backend::Turso).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_types_and_metadata() {
    contracts(Backend::Postgres).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_atomicity_and_readonly() {
    rollback(Backend::Postgres).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_deadline_and_cancellation() {
    timeout_and_cancellation(Backend::Postgres).await;
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL"]
async fn postgres_lock_wait() {
    lock_timeout(Backend::Postgres).await;
}
