use tokio_postgres::{Client, NoTls, types::Type};

async fn connect() -> Client {
    let url = std::env::var("SQLREST_TEST_POSTGRES")
        .expect("Set SQLREST_TEST_POSTGRES to a disposable PostgreSQL database");
    let (client, connection) = tokio_postgres::connect(&url, NoTls).await.unwrap();
    tokio::spawn(async move {
        connection.await.unwrap();
    });
    client
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run explicitly with SQLREST_TEST_POSTGRES"]
async fn binding_metadata_and_transactions() {
    let mut client = connect().await;
    let statement = client.prepare_typed("SELECT $1 AS lo, $2 AS hi, $3 AS flag, $4 AS value, $5 AS text, $6 AS absent, $7 AS ids",
        &[Type::INT8, Type::INT8, Type::BOOL, Type::FLOAT8, Type::TEXT, Type::TEXT, Type::JSONB]).await.unwrap();
    let ids = serde_json::json!([1, 2]);
    let row = client
        .query_one(
            &statement,
            &[
                &i64::MIN,
                &i64::MAX,
                &true,
                &1.25f64,
                &"literal",
                &None::<String>,
                &ids,
            ],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), i64::MIN);
    assert_eq!(row.get::<_, i64>(1), i64::MAX);
    assert!(row.get::<_, bool>(2));
    assert_eq!(row.get::<_, f64>(3), 1.25);
    assert_eq!(row.get::<_, String>(4), "literal");
    assert_eq!(row.get::<_, Option<String>>(5), None);
    assert_eq!(row.get::<_, serde_json::Value>(6), ids);
    assert_eq!(
        client
            .query("SELECT value FROM jsonb_array_elements($1)", &[&ids])
            .await
            .unwrap()
            .len(),
        2
    );
    let empty = client
        .prepare("SELECT 1::bigint AS id, true AS completed WHERE false")
        .await
        .unwrap();
    assert_eq!(empty.columns()[0].name(), "id");
    assert_eq!(*empty.columns()[1].type_(), Type::BOOL);
    assert!(client.query(&empty, &[]).await.unwrap().is_empty());
    client
        .batch_execute("CREATE TEMP TABLE items(id bigint PRIMARY KEY)")
        .await
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    assert_eq!(
        transaction
            .query("INSERT INTO items VALUES(1) RETURNING id", &[])
            .await
            .unwrap()
            .len(),
        1
    );
    transaction.rollback().await.unwrap();
    assert!(
        client
            .query("SELECT * FROM items", &[])
            .await
            .unwrap()
            .is_empty()
    );
    let transaction = client.transaction().await.unwrap();
    transaction
        .query("INSERT INTO items VALUES(2) RETURNING id", &[])
        .await
        .unwrap();
    transaction
        .query("SELECT id FROM items", &[])
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT id FROM items", &[])
            .await
            .unwrap()
            .get::<_, i64>(0),
        2
    );
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run explicitly with SQLREST_TEST_POSTGRES"]
async fn readonly_cpu_timeout_and_cleanup() {
    let mut client = connect().await;
    // Permanent test-local table: PostgreSQL deliberately permits writes to TEMP
    // tables in read-only transactions, so TEMP would not test the contract.
    client
        .batch_execute("CREATE TABLE sqlrest_readonly_probe(id bigint)")
        .await
        .unwrap();
    let transaction = client
        .build_transaction()
        .read_only(true)
        .start()
        .await
        .unwrap();
    let error = transaction
        .execute("INSERT INTO sqlrest_readonly_probe VALUES(1)", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "25006");
    transaction.rollback().await.unwrap();
    let transaction = client.transaction().await.unwrap();
    transaction
        .execute("INSERT INTO sqlrest_readonly_probe VALUES(2)", &[])
        .await
        .unwrap();
    transaction
        .batch_execute("SET LOCAL statement_timeout = '30ms'")
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let error = transaction
        .query("SELECT sum(x) FROM generate_series(1,1000000000) AS x", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "57014");
    assert!(start.elapsed() < std::time::Duration::from_secs(2));
    transaction.rollback().await.unwrap();
    assert!(
        client
            .query("SELECT * FROM sqlrest_readonly_probe", &[])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        client
            .query_one("SHOW statement_timeout", &[])
            .await
            .unwrap()
            .get::<_, String>(0),
        "0"
    );
    client
        .batch_execute("DROP TABLE sqlrest_readonly_probe")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run explicitly with SQLREST_TEST_POSTGRES"]
async fn lock_timeout_and_cleanup() {
    let owner = connect().await;
    let waiter = connect().await;
    owner
        .batch_execute("CREATE TABLE sqlrest_lock_probe(id bigint)")
        .await
        .unwrap();
    owner
        .batch_execute("BEGIN; LOCK TABLE sqlrest_lock_probe IN ACCESS EXCLUSIVE MODE")
        .await
        .unwrap();
    waiter
        .batch_execute("BEGIN; SET LOCAL statement_timeout='30ms'")
        .await
        .unwrap();
    let error = waiter
        .query("SELECT * FROM sqlrest_lock_probe", &[])
        .await
        .unwrap_err();
    assert_eq!(error.code().unwrap().code(), "57014");
    waiter.batch_execute("ROLLBACK").await.unwrap();
    owner.batch_execute("COMMIT").await.unwrap();
    assert!(
        waiter
            .query("SELECT * FROM sqlrest_lock_probe", &[])
            .await
            .unwrap()
            .is_empty()
    );
    owner
        .batch_execute("DROP TABLE sqlrest_lock_probe")
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run explicitly with SQLREST_TEST_POSTGRES"]
async fn native_types_and_explicit_encoding() {
    let client = connect().await;
    let statement = client.prepare("SELECT 1::smallint, 2::integer, 3::bigint, 1.5::real, 2.5::double precision, 1.23::numeric, DATE '2026-09-10', TIMESTAMP '2026-09-10 12:00:00', '00000000-0000-0000-0000-000000000001'::uuid, decode('ff','hex'), '{}'::json, '{}'::jsonb").await.unwrap();
    assert_eq!(
        statement
            .columns()
            .iter()
            .map(|c| c.type_().clone())
            .collect::<Vec<_>>(),
        vec![
            Type::INT2,
            Type::INT4,
            Type::INT8,
            Type::FLOAT4,
            Type::FLOAT8,
            Type::NUMERIC,
            Type::DATE,
            Type::TIMESTAMP,
            Type::UUID,
            Type::BYTEA,
            Type::JSON,
            Type::JSONB
        ]
    );
    let row = client.query_one(&statement, &[]).await.unwrap();
    assert_eq!(row.get::<_, i16>(0), 1);
    assert_eq!(row.get::<_, i32>(1), 2);
    assert_eq!(row.get::<_, i64>(2), 3);
    assert_eq!(row.get::<_, f32>(3), 1.5);
    assert_eq!(row.get::<_, f64>(4), 2.5);
    for column in 5..10 {
        assert!(row.try_get::<_, String>(column).is_err());
    }
    for column in 10..12 {
        assert_eq!(
            row.get::<_, serde_json::Value>(column),
            serde_json::json!({})
        );
    }
    let row = client.query_one("SELECT (1.23::numeric)::text, (DATE '2026-09-10')::text, encode(decode('ff','hex'),'hex')", &[]).await.unwrap();
    assert_eq!(row.get::<_, String>(0), "1.23");
    assert_eq!(row.get::<_, String>(1), "2026-09-10");
    assert_eq!(row.get::<_, String>(2), "ff");
}

#[tokio::test]
#[ignore = "requires disposable PostgreSQL; run explicitly with SQLREST_TEST_POSTGRES"]
async fn explicit_cancellation_and_reuse() {
    let client = connect().await;
    let observer = connect().await;
    let pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .await
        .unwrap()
        .get(0);
    client
        .batch_execute("CREATE TEMP TABLE cancelled(id int)")
        .await
        .unwrap();
    client
        .batch_execute("BEGIN; INSERT INTO cancelled VALUES(1)")
        .await
        .unwrap();
    let cancel = client.cancel_token();
    let query = client.query("SELECT pg_sleep(10)", &[]);
    let cancellation = async {
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            loop {
                let active: bool = observer.query_one("SELECT state='active' AND query LIKE '%pg_sleep%' FROM pg_stat_activity WHERE pid=$1", &[&pid]).await.unwrap().get(0);
                if active { break; }
                tokio::task::yield_now().await;
            }
        }).await.unwrap();
        cancel.cancel_query(NoTls).await.unwrap();
    };
    let (result, ()) = tokio::join!(query, cancellation);
    assert_eq!(result.unwrap_err().code().unwrap().code(), "57014");
    client.batch_execute("ROLLBACK").await.unwrap();
    assert!(
        client
            .query("SELECT * FROM cancelled", &[])
            .await
            .unwrap()
            .is_empty()
    );
    client.query_one("SELECT 1", &[]).await.unwrap();
}
