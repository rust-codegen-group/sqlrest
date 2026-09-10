use std::time::Duration;

#[tokio::test]
async fn boolean_declaration_constraints_and_strict_tables() {
    let directory = tempfile::tempdir().unwrap();
    let database = turso::Builder::new_local(directory.path().join("boolean.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    connection
        .execute("CREATE TABLE loose(completed BOOLEAN)", ())
        .await
        .unwrap();
    connection
        .execute(
            "INSERT INTO loose VALUES (TRUE), (FALSE), (2), ('text'), (NULL)",
            (),
        )
        .await
        .unwrap();
    let mut rows = connection
        .query(
            "SELECT completed, typeof(completed) FROM loose ORDER BY rowid",
            (),
        )
        .await
        .unwrap();
    for (expected, storage) in [
        (Some(1), "integer"),
        (Some(0), "integer"),
        (Some(2), "integer"),
        (None, "text"),
        (None, "null"),
    ] {
        let row = rows.next().await.unwrap().unwrap();
        assert_eq!(row.get::<String>(1).unwrap(), storage);
        if let Some(expected) = expected {
            assert_eq!(row.get::<i64>(0).unwrap(), expected);
        }
    }
    assert!(rows.next().await.unwrap().is_none());
    drop(rows);
    connection
        .execute(
            "CREATE TABLE checked(completed BOOLEAN NOT NULL DEFAULT 0 CHECK(completed IN (0,1)))",
            (),
        )
        .await
        .unwrap();
    for sql in [
        "INSERT INTO checked DEFAULT VALUES",
        "INSERT INTO checked VALUES(TRUE)",
        "INSERT INTO checked VALUES(FALSE)",
    ] {
        connection.execute(sql, ()).await.unwrap();
    }
    for literal in ["2", "-1", "'text'", "NULL"] {
        assert!(
            connection
                .execute(format!("INSERT INTO checked VALUES({literal})"), ())
                .await
                .is_err(),
            "must reject {literal}"
        );
    }
    let error = connection
        .execute("CREATE TABLE strict_boolean(completed BOOLEAN) STRICT", ())
        .await
        .unwrap_err();
    println!("STRICT BOOLEAN rejected: {error}");
    connection.execute("CREATE TABLE strict_integer(completed INTEGER NOT NULL DEFAULT 0 CHECK(completed IN (0,1))) STRICT", ()).await.unwrap();
    connection
        .execute("INSERT INTO strict_integer VALUES(TRUE),(FALSE)", ())
        .await
        .unwrap();
    for literal in ["2", "'text'", "NULL"] {
        assert!(
            connection
                .execute(format!("INSERT INTO strict_integer VALUES({literal})"), ())
                .await
                .is_err()
        );
    }
}

#[test]
fn sdk_bindings_metadata_and_lock_cleanup() {
    use sqlrest::turso_driver::{execute, open};
    use std::time::Instant;
    use turso_core::Value;
    use turso_sdk_kit::rsapi::TursoStatusCode;
    let directory = tempfile::tempdir().unwrap();
    let database = open(&directory.path().join("bindings.db")).unwrap();
    let owner = database.connect().unwrap();
    let waiter = database.connect().unwrap();
    let deadline = || Instant::now() + Duration::from_secs(2);
    let mut statement = owner
        .prepare_single(
            "SELECT $1 AS lo, $2 AS hi, $3 AS flag, $4 AS real, $5 AS text, $6 AS absent",
        )
        .unwrap();
    for (i, value) in [
        Value::from_i64(i64::MIN),
        Value::from_i64(i64::MAX),
        Value::from_i64(1),
        Value::from_f64(1.25),
        Value::Text("literal".into()),
        Value::Null,
    ]
    .into_iter()
    .enumerate()
    {
        statement.bind_positional(i + 1, value).unwrap();
    }
    assert_eq!(statement.column_count(), 6);
    assert_eq!(statement.column_name(0).unwrap(), "lo");
    assert!(matches!(
        statement.step(None).unwrap(),
        TursoStatusCode::Row
    ));
    assert_eq!(
        statement.row_value(0).unwrap().to_string(),
        i64::MIN.to_string()
    );
    assert_eq!(
        statement.row_value(1).unwrap().to_string(),
        i64::MAX.to_string()
    );
    assert_eq!(statement.row_value(2).unwrap().to_string(), "1");
    assert_eq!(statement.row_value(3).unwrap().to_string(), "1.25");
    assert!(matches!(statement.row_value(5).unwrap(), Value::Null));
    assert!(matches!(
        statement.step(None).unwrap(),
        TursoStatusCode::Done
    ));
    drop(statement);
    let mut statement = owner
        .prepare_single("SELECT value FROM json_each($1)")
        .unwrap();
    statement
        .bind_positional(1, Value::Text("[1,2]".into()))
        .unwrap();
    for expected in ["1", "2"] {
        assert!(matches!(
            statement.step(None).unwrap(),
            TursoStatusCode::Row
        ));
        assert_eq!(statement.row_value(0).unwrap().to_string(), expected);
    }
    assert!(matches!(
        statement.step(None).unwrap(),
        TursoStatusCode::Done
    ));
    drop(statement);
    let mut empty = owner.prepare_single("SELECT 1 AS id WHERE false").unwrap();
    assert_eq!(empty.column_name(0).unwrap(), "id");
    assert!(matches!(empty.step(None).unwrap(), TursoStatusCode::Done));
    drop(empty);
    execute(&owner, "CREATE TABLE items(id INTEGER)", deadline()).unwrap();
    execute(&waiter, "PRAGMA query_only=1", deadline()).unwrap();
    execute(&waiter, "BEGIN", deadline()).unwrap();
    assert!(execute(&waiter, "INSERT INTO items VALUES(99)", deadline()).is_err());
    execute(&waiter, "ROLLBACK", deadline()).unwrap();
    execute(&waiter, "PRAGMA query_only=0", deadline()).unwrap();
    execute(&owner, "BEGIN IMMEDIATE", deadline()).unwrap();
    execute(&owner, "INSERT INTO items VALUES(1)", deadline()).unwrap();
    waiter.set_busy_timeout(Duration::from_secs(5));
    let start = Instant::now();
    let error = execute(
        &waiter,
        "INSERT INTO items VALUES(2)",
        start + Duration::from_millis(30),
    )
    .unwrap_err();
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "lock wait exceeded deadline: {error}"
    );
    assert!(waiter.get_auto_commit());
    execute(&owner, "COMMIT", deadline()).unwrap();
    execute(&waiter, "INSERT INTO items VALUES(3)", deadline()).unwrap();
    let mut rows = waiter
        .prepare_single("SELECT id FROM items ORDER BY id")
        .unwrap();
    for expected in ["1", "3"] {
        assert!(matches!(rows.step(None).unwrap(), TursoStatusCode::Row));
        assert_eq!(rows.row_value(0).unwrap().to_string(), expected);
    }
    assert!(matches!(rows.step(None).unwrap(), TursoStatusCode::Done));
}

#[test]
fn sdk_deadline_interrupts_and_rolls_back() {
    use sqlrest::turso_driver::{execute, open};
    use std::time::Instant;
    let directory = tempfile::tempdir().unwrap();
    let database = open(&directory.path().join("sdk.db")).unwrap();
    let connection = database.connect().unwrap();
    let deadline = || Instant::now() + Duration::from_secs(2);
    execute(&connection, "CREATE TABLE items(id INTEGER)", deadline()).unwrap();
    execute(&connection, "BEGIN", deadline()).unwrap();
    execute(&connection, "INSERT INTO items VALUES(1)", deadline()).unwrap();
    let start = Instant::now();
    let error = execute(&connection,
        "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n",
        start + Duration::from_millis(20)).unwrap_err();
    assert_eq!(error.code, "execution_timeout");
    assert!(start.elapsed() < Duration::from_secs(2));
    // Interruption may itself abort the transaction. Both outcomes are clean
    // only after autocommit has been observed or explicit rollback succeeded.
    if !connection.get_auto_commit() {
        execute(&connection, "ROLLBACK", deadline()).unwrap();
    }
    assert!(connection.get_auto_commit());
    let mut statement = connection
        .prepare_single("SELECT count(*) FROM items")
        .unwrap();
    statement.step(None).unwrap();
    assert_eq!(statement.row_value(0).unwrap().to_string(), "0");
}

#[tokio::test]
async fn readonly_and_returning_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let database = turso::Builder::new_local(directory.path().join("test.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    connection
        .execute("CREATE TABLE items(id INTEGER PRIMARY KEY, title TEXT)", ())
        .await
        .unwrap();
    connection.execute("PRAGMA query_only=1", ()).await.unwrap();
    connection.execute("BEGIN", ()).await.unwrap();
    assert!(
        connection
            .execute("INSERT INTO items VALUES (1, 'no')", ())
            .await
            .is_err()
    );
    connection.execute("ROLLBACK", ()).await.unwrap();
    connection.execute("PRAGMA query_only=0", ()).await.unwrap();
    connection.execute("BEGIN", ()).await.unwrap();
    let mut statement = connection
        .prepare("INSERT INTO items VALUES (1, 'yes') RETURNING id, title")
        .await
        .unwrap();
    assert_eq!(statement.column_names(), vec!["id", "title"]);
    let mut rows = statement.query(()).await.unwrap();
    assert!(rows.next().await.unwrap().is_some());
    assert!(rows.next().await.unwrap().is_none());
    drop(rows);
    drop(statement);
    connection.execute("ROLLBACK", ()).await.unwrap();
    let mut rows = connection
        .query("SELECT count(*) FROM items", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "upstream high-level binding does not yield during CPU-bound queries; use SDK deadline test"]
async fn timeout_cancels_work_and_allows_rollback() {
    let directory = tempfile::tempdir().unwrap();
    let database = turso::Builder::new_local(directory.path().join("test.db").to_str().unwrap())
        .build()
        .await
        .unwrap();
    let connection = database.connect().unwrap();
    connection
        .execute("CREATE TABLE items(id INTEGER)", ())
        .await
        .unwrap();
    connection.execute("BEGIN", ()).await.unwrap();
    connection
        .execute("INSERT INTO items VALUES (1)", ())
        .await
        .unwrap();
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(Duration::from_millis(20), async {
        let mut rows = connection.query(
            "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000000) SELECT sum(x) FROM n", ()
        ).await.unwrap();
        rows.next().await.unwrap()
    }).await;
    assert!(result.is_err(), "query must time out");
    assert!(
        start.elapsed() < Duration::from_secs(2),
        "driver must yield to deadline"
    );
    connection.execute("ROLLBACK", ()).await.unwrap();
    let mut rows = connection
        .query("SELECT count(*) FROM items", ())
        .await
        .unwrap();
    assert_eq!(
        rows.next().await.unwrap().unwrap().get::<i64>(0).unwrap(),
        0
    );
}
