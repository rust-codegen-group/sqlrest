use aioduct::TokioClient as Client;
use axum::http::{Method, StatusCode};
use serde_json::{Value, json};
use sqlrest::{
    http::Server,
    registry::{OperationId, Outcome, Phase, PublishStep, Registry},
};
use std::{fs, net::SocketAddr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    sync::oneshot,
};

const RECORD: &str = r#"{"type":"object","properties":{"id":{"type":"integer"},"value":{"type":"string"}},"required":["id","value"],"additionalProperties":false}"#;
const VALUE: &str = r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#;
const INITIAL: &str = "CREATE TABLE items(id BIGINT PRIMARY KEY, value TEXT NOT NULL); INSERT INTO items VALUES(1,'one');";

struct Harness {
    directory: tempfile::TempDir,
    registry: Registry,
    client: Client,
    data: String,
    management: String,
    data_address: SocketAddr,
    management_address: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<Result<(), sqlrest::SqlrestError>>>,
}

impl Harness {
    async fn new() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let interfaces = directory.path().join("databases/db/interfaces");
        fs::create_dir_all(&interfaces).unwrap();
        let migrations = directory.path().join("databases/db/migrations");
        fs::create_dir(&migrations).unwrap();
        fs::write(migrations.join("0001_initial.sql"), INITIAL).unwrap();
        fs::write(
            interfaces.join("get.sql"),
            "SELECT id,value FROM items ORDER BY id",
        )
        .unwrap();
        fs::write(interfaces.join("get.response.yaml"), RECORD).unwrap();
        fs::write(
            interfaces.join("post.sql"),
            "INSERT INTO items VALUES(${body.id:int64},${body.value:string}) RETURNING id,value",
        )
        .unwrap();
        fs::write(interfaces.join("post.response.yaml"), RECORD).unwrap();
        fs::write(
            interfaces.join("head.sql"),
            "SELECT id,value FROM items ORDER BY id",
        )
        .unwrap();
        fs::write(interfaces.join("head.response.yaml"), RECORD).unwrap();
        for (route, sql) in [
            ("echo/[id]", "SELECT ${path.id:string} AS value"),
            ("onlyget", "SELECT 'get' AS value"),
            ("broken", "SELECT private_secret_function() AS value"),
            (
                "slow",
                "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<1000000000) SELECT CAST(sum(x) AS TEXT) AS value FROM n",
            ),
        ] {
            let root = interfaces.join(route);
            fs::create_dir_all(&root).unwrap();
            fs::write(root.join("get.sql"), sql).unwrap();
            fs::write(root.join("get.response.yaml"), VALUE).unwrap();
        }
        let registry = Registry::open(directory.path()).await.unwrap();
        let server = Server::bind(
            registry.clone(),
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let data_address = server.data_address();
        let management_address = server.management_address();
        let (stop, receiver) = oneshot::channel();
        let task = tokio::spawn(server.serve(async {
            let _ = receiver.await;
        }));
        Self {
            directory,
            registry,
            client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .unwrap(),
            data: format!("http://{data_address}"),
            management: format!("http://{management_address}"),
            data_address,
            management_address,
            stop: Some(stop),
            task: Some(task),
        }
    }

    fn config(&self, timeout: u64) -> Value {
        json!({
            "database":{"kind":"turso"},
            "limits":{"request_timeout_ms":timeout,"max_rows":10}
        })
    }

    async fn register(&self, timeout: u64) -> Value {
        let response = self
            .client
            .post(&format!("{}/databases/db/publish", self.management))
            .unwrap()
            .json(&self.config(timeout))
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body: Value = response.json().await.unwrap();
        self.wait(body["operation_id"].as_str().unwrap().to_owned())
            .await
    }

    async fn operation(&self, method: Method, suffix: &str) -> Value {
        let response = self
            .client
            .request(method, &format!("{}/databases/db{suffix}", self.management))
            .unwrap()
            .json(&json!({}))
            .unwrap()
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        let body: Value = response.json().await.unwrap();
        self.wait(body["operation_id"].as_str().unwrap().to_owned())
            .await
    }

    async fn wait(&self, id: String) -> Value {
        tokio::time::timeout(Duration::from_secs(8), async {
            loop {
                let response = self
                    .client
                    .get(&format!("{}/databases/db/operations/{id}", self.management))
                    .unwrap()
                    .send()
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::OK);
                let body: Value = response.json().await.unwrap();
                if body["outcome"] != "running" {
                    return body;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap()
    }

    async fn initialize(&self, timeout: u64) {
        assert_eq!(self.register(timeout).await["outcome"], "succeeded");
    }

    async fn close(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        tokio::time::timeout(Duration::from_secs(8), self.task.take().unwrap())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}

#[tokio::test]
#[ignore = "requires SQLREST_TEST_POSTGRES disposable PostgreSQL"]
async fn postgres_http_lifecycle() {
    let connection = std::env::var("SQLREST_TEST_POSTGRES").unwrap();
    let (admin, driver) = tokio_postgres::connect(&connection, tokio_postgres::NoTls)
        .await
        .unwrap();
    tokio::spawn(driver);
    let database = format!("http_{}", std::process::id());
    admin
        .batch_execute(&format!("CREATE DATABASE {database}"))
        .await
        .unwrap();
    let mut h = Harness::new().await;
    let mut config = h.config(5000);
    let mut url = url::Url::parse(&connection).unwrap();
    url.set_path(&database);
    config["database"] = json!({"kind":"postgres_unencrypted","connection":url.as_str()});
    let response = h
        .client
        .post(&format!("{}/databases/db/publish", h.management))
        .unwrap()
        .json(&config)
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let id = response.json::<Value>().await.unwrap()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(h.wait(id).await["outcome"], "succeeded");
    let response = h
        .client
        .post(&format!("{}/db/db", h.data))
        .unwrap()
        .json(&json!({"id":2,"value":"two"}))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap(),
        json!({"records":[{"id":2,"value":"two"}]})
    );
    let response = h
        .client
        .get(&format!("{}/db/db", h.data))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.json::<Value>().await.unwrap()["records"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        h.client
            .get(&format!("{}/databases/db/migrations", h.management))
            .unwrap()
            .send()
            .await
            .unwrap()
            .json::<Value>()
            .await
            .unwrap()["migrations"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    h.close().await;
    admin
        .batch_execute(&format!("DROP DATABASE {database}"))
        .await
        .unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn binary_wildcard_bind_and_sigterm() {
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    let binary = env!("CARGO_BIN_EXE_sqlrest");
    assert!(
        tokio::process::Command::new(binary)
            .arg("--help")
            .output()
            .await
            .unwrap()
            .status
            .success()
    );
    assert!(
        !tokio::process::Command::new(binary)
            .output()
            .await
            .unwrap()
            .status
            .success()
    );
    let mut h = Harness::new().await;
    h.close().await;
    let spare = tempfile::tempdir().unwrap();
    let retired = std::mem::replace(&mut h.registry, Registry::open(spare.path()).await.unwrap());
    drop(retired);
    let mut child = tokio::process::Command::new(binary)
        .arg("--workspace")
        .arg(h.directory.path())
        .args([
            "--data-listen",
            "0.0.0.0:0",
            "--management-listen",
            "127.0.0.1:0",
        ])
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut lines = BufReader::new(child.stderr.take().unwrap()).lines();
    let line = tokio::time::timeout(Duration::from_secs(10), lines.next_line())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let (data, management) = line
        .strip_prefix("data=")
        .unwrap()
        .split_once(" management=")
        .unwrap();
    assert!(data.starts_with("0.0.0.0:"));
    h.data = format!("http://{}", data.replace("0.0.0.0", "127.0.0.1"));
    h.management = format!("http://{management}");
    h.initialize(5000).await;
    assert_eq!(
        h.client
            .get(&format!("{}/db/db", h.data))
            .unwrap()
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert!(
        tokio::process::Command::new("kill")
            .args(["-TERM", &child.id().unwrap().to_string()])
            .status()
            .await
            .unwrap()
            .success()
    );
    assert!(
        tokio::time::timeout(Duration::from_secs(10), child.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
}

#[tokio::test]
async fn real_http_lifecycle_matches_embedded_registry() {
    let mut h = Harness::new().await;
    let status = h.register(5000).await;
    assert_eq!(status["outcome"], "succeeded");
    assert_eq!(
        h.client
            .get(&format!("{}/db/db", h.data))
            .unwrap()
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK
    );
    assert_eq!(h.register(5000).await["outcome"], "succeeded");
    let mut different = h.config(5000);
    different["limits"]["max_rows"] = json!(20);
    let response = h
        .client
        .post(&format!("{}/databases/db/publish", h.management))
        .unwrap()
        .json(&different)
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let id = response.json::<Value>().await.unwrap()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(h.wait(id).await["outcome"], "succeeded");
    assert_eq!(
        h.registry.status("db").unwrap().limits.unwrap().max_rows,
        20
    );
    assert_eq!(
        h.operation(Method::POST, "/publish").await["outcome"],
        "succeeded"
    );
    let response = h
        .client
        .get(&format!("{}/db/db/", h.data))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let http: Value = response.json().await.unwrap();
    let embedded: Value = serde_json::from_slice(
        &h.registry
            .execute("db", "get", &[], Default::default())
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(http, embedded);
    let post = h
        .client
        .post(&format!("{}/db/db", h.data))
        .unwrap()
        .json(&json!({"id":i64::MAX,"value":"large id"}))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::OK);
    assert_eq!(
        post.json::<Value>().await.unwrap()["records"][0]["id"],
        json!(i64::MAX)
    );
    let document: Value = h
        .client
        .get(&format!(
            "{}/databases/db/openapi?server_url=%2Fapi",
            h.management
        ))
        .unwrap()
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(document, h.registry.openapi("db", "/api").unwrap());
    let exported: Value = h
        .client
        .get(&format!("{}/databases/db/migrations", h.management))
        .unwrap()
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(exported["migrations"][0]["source"], INITIAL);
    let unregistered = h.operation(Method::DELETE, "").await;
    assert_eq!(unregistered["outcome"], "succeeded");
    assert_eq!(
        h.wait(unregistered["id"].as_str().unwrap().to_owned())
            .await["outcome"],
        "succeeded"
    );
    h.register(5000).await;
    assert_eq!(
        h.operation(Method::POST, "/publish").await["outcome"],
        "succeeded"
    );
    let response: Value = h
        .client
        .get(&format!("{}/db/db", h.data))
        .unwrap()
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(response["records"].as_array().unwrap().len(), 2);
    h.close().await;
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Unregistered);
}

#[tokio::test]
async fn strict_http_inputs_paths_methods_and_separate_listeners() {
    let mut h = Harness::new().await;
    h.initialize(5000).await;
    for (url, status) in [
        (format!("{}/databases/db", h.data), StatusCode::NOT_FOUND),
        (format!("{}/db/db", h.management), StatusCode::NOT_FOUND),
        (format!("{}/db/db?a=1&a=2", h.data), StatusCode::BAD_REQUEST),
        (
            format!("{}/databases/db?unknown=1", h.management),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("{}/databases/db/operations/not-an-id", h.management),
            StatusCode::BAD_REQUEST,
        ),
        (
            format!("{}/db/db/echo/a%2Fb", h.data),
            StatusCode::BAD_REQUEST,
        ),
        (format!("{}/db/db/onlyget/", h.data), StatusCode::NOT_FOUND),
    ] {
        let response = h.client.get(&url).unwrap().send().await.unwrap();
        assert_eq!(response.status(), status);
        assert!(response.json::<Value>().await.unwrap()["error"]["code"].is_string());
    }
    for (value, expected) in [
        ("%E4%BD%A0%E5%A5%BD", "你好"),
        ("%252F", "%2F"),
        ("a+b", "a+b"),
    ] {
        let value: Value = h
            .client
            .get(&format!("{}/db/db/echo/{value}", h.data))
            .unwrap()
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(value["records"][0]["value"], expected);
    }
    let head = h
        .client
        .head(&format!("{}/db/db", h.data))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(head.status(), StatusCode::OK);
    assert!(head.bytes().await.unwrap().is_empty());
    assert_eq!(
        h.client
            .head(&format!("{}/db/db/onlyget", h.data))
            .unwrap()
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::METHOD_NOT_ALLOWED
    );
    let options = h
        .client
        .request(Method::OPTIONS, &format!("{}/db/db", h.data))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(options.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(options.headers()["allow"], "GET, HEAD, POST");
    assert!(
        !options
            .headers()
            .contains_key("access-control-allow-origin")
    );
    for body in [
        r#"{"id":2,"id":3,"value":"x"}"#,
        r#"{"id":"2","value":"x"}"#,
        r#"{"id":2,"value":"x","extra":{"x":1,"x":2}}"#,
    ] {
        let response = h
            .client
            .post(&format!("{}/db/db", h.data))
            .unwrap()
            .header_str("content-type", "application/json")
            .unwrap()
            .body(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }
    assert_eq!(
        h.client
            .post(&format!("{}/db/db", h.data))
            .unwrap()
            .header_str("content-type", "text/plain")
            .unwrap()
            .body("{}")
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNSUPPORTED_MEDIA_TYPE
    );
    let failure = h
        .client
        .get(&format!("{}/db/db/broken", h.data))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(failure.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let text = failure.text().await.unwrap();
    assert!(!text.contains("private_secret"));
    assert!(!text.contains("SELECT"));
    let mut invalid = h.config(5000);
    invalid["unexpected"] = json!("secret-value");
    let failure = h
        .client
        .post(&format!("{}/databases/other/publish", h.management))
        .unwrap()
        .json(&invalid)
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(failure.status(), StatusCode::BAD_REQUEST);
    assert!(!failure.text().await.unwrap().contains("secret-value"));
    let duplicate = serde_json::to_string(&h.config(5000)).unwrap().replacen(
        "{",
        "{\"interfaces\":\"secret-value\",",
        1,
    );
    assert_eq!(
        h.client
            .post(&format!("{}/databases/other/publish", h.management))
            .unwrap()
            .header_str("content-type", "application/json")
            .unwrap()
            .body(duplicate)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::BAD_REQUEST
    );
    // Larger than Axum's default JSON extractor body cap, with no extra limit.
    let large = "x".repeat(2_200_000);
    let response = h
        .client
        .post(&format!("{}/db/db", h.data))
        .unwrap()
        .json(&json!({"id":2,"value":large}))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.json::<Value>().await.unwrap()["records"][0]["value"]
            .as_str()
            .unwrap()
            .len(),
        2_200_000
    );
    h.close().await;
}

#[tokio::test]
async fn http_migration_pause_repair_and_lost_accepted_response() {
    let mut h = Harness::new().await;
    h.initialize(5000).await;
    fs::write(
        h.directory
            .path()
            .join("databases/db/migrations/0002_change.sql"),
        "INSERT INTO missing VALUES(1)",
    )
    .unwrap();
    let failed = h.operation(Method::POST, "/publish").await;
    assert_eq!(failed["outcome"], "failed");
    assert_eq!(
        h.client
            .get(&format!("{}/db/db", h.data))
            .unwrap()
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    let reload = h
        .client
        .post(&format!("{}/databases/db/reload", h.management))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(reload.status(), StatusCode::NOT_FOUND);
    assert_eq!(
        reload.json::<Value>().await.unwrap()["error"]["code"],
        "route_not_found"
    );
    fs::write(
        h.directory
            .path()
            .join("databases/db/migrations/0002_change.sql"),
        "INSERT INTO items VALUES(2,'two')",
    )
    .unwrap();
    let repaired = h.operation(Method::POST, "/publish").await;
    assert_eq!(repaired["outcome"], "succeeded");
    assert_eq!(repaired["publish"]["step"], "complete");
    // Do not consume the publish response. Recover its ID from status.
    let mut socket = TcpStream::connect(h.management_address).await.unwrap();
    socket.write_all(b"POST /databases/db/publish HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}").await.unwrap();
    let id = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status: Value = h
                .client
                .get(&format!("{}/databases/db", h.management))
                .unwrap()
                .send()
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            let op = if status["current_operation"].is_object() {
                &status["current_operation"]
            } else {
                &status["last_operation"]
            };
            if op["id"] != repaired["id"] {
                break op["id"].as_str().unwrap().to_owned();
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    drop(socket);
    assert_eq!(h.wait(id).await["outcome"], "succeeded");
    h.close().await;
}

#[tokio::test]
async fn admitted_upload_keeps_old_snapshot_and_counts_toward_drain() {
    let mut h = Harness::new().await;
    h.initialize(5000).await;
    let mut upload = TcpStream::connect(h.data_address).await.unwrap();
    let body = r#"{"id":2,"value":"upload"}"#;
    upload.write_all(format!("POST /db/db HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.registry.status("db").unwrap().active_requests != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fs::write(
        h.directory.path().join("databases/db/interfaces/post.sql"),
        "INSERT INTO items VALUES(${body.id:int64}, 'new snapshot') RETURNING id,value",
    )
    .unwrap();
    assert_eq!(
        h.operation(Method::POST, "/publish").await["outcome"],
        "succeeded"
    );
    let response = h
        .client
        .delete(&format!("{}/databases/db", h.management))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let id = response.json::<Value>().await.unwrap()["operation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Unregistering);
    assert_eq!(
        h.client
            .get(&format!("{}/db/db", h.data))
            .unwrap()
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
    upload.write_all(body.as_bytes()).await.unwrap();
    let mut response = String::new();
    upload.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("\"value\":\"upload\""));
    assert!(!response.contains("new snapshot"));
    assert_eq!(h.wait(id).await["outcome"], "succeeded");
    h.close().await;
}

#[tokio::test]
async fn upload_budget_and_shutdown_abort_unaccepted_bodies() {
    let mut h = Harness::new().await;
    h.initialize(100).await;
    let mut upload = TcpStream::connect(h.data_address).await.unwrap();
    upload.write_all(b"POST /db/db HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\nConnection: close\r\n\r\n{").await.unwrap();
    let mut response = String::new();
    tokio::time::timeout(Duration::from_secs(2), upload.read_to_string(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.starts_with("HTTP/1.1 504"));
    assert!(response.contains("execution_timeout"));
    let mut management = TcpStream::connect(h.management_address).await.unwrap();
    management.write_all(b"POST /databases/slow/publish HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: 100\r\n\r\n{").await.unwrap();
    let _idle = TcpStream::connect(h.management_address).await.unwrap();
    let mut incomplete_headers = TcpStream::connect(h.data_address).await.unwrap();
    incomplete_headers
        .write_all(b"GET /db/db HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    h.close().await;
    assert!(h.registry.is_shutting_down());
    assert_eq!(
        h.registry
            .publish("db", Default::default())
            .unwrap_err()
            .code,
        "server_shutting_down"
    );
}

#[tokio::test]
async fn shutdown_finishes_accepted_migration_and_releases_file_identity() {
    let mut h = Harness::new().await;
    h.initialize(1000).await;
    let client = h.client.clone();
    let slow_url = format!("{}/db/db/slow", h.data);
    let request = tokio::spawn(async move { client.get(&slow_url).unwrap().send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(2), async {
        while h.registry.status("db").unwrap().active_requests == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fs::write(
        h.directory
            .path()
            .join("databases/db/migrations/0002_shutdown.sql"),
        "INSERT INTO items VALUES(2,'after drain')",
    )
    .unwrap();
    let response = h
        .client
        .post(&format!("{}/databases/db/publish", h.management))
        .unwrap()
        .json(&json!({}))
        .unwrap()
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::ACCEPTED);
    let id: OperationId =
        serde_json::from_value(response.json::<Value>().await.unwrap()["operation_id"].clone())
            .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while h
            .registry
            .status("db")
            .unwrap()
            .current_operation
            .as_ref()
            .unwrap()
            .publish
            .as_ref()
            .unwrap()
            .step
            != PublishStep::Draining
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    h.close().await;
    assert_eq!(request.await.unwrap().status(), StatusCode::GATEWAY_TIMEOUT);
    assert_eq!(
        h.registry.operation("db", id).unwrap().outcome,
        Outcome::Succeeded
    );
    assert_eq!(h.registry.status("db").unwrap().phase, Phase::Unregistered);
    // Release all workspace owners, then recover without registration replay.
    let spare = tempfile::tempdir().unwrap();
    let retired = std::mem::replace(&mut h.registry, Registry::open(spare.path()).await.unwrap());
    drop(retired);
    let registry = Registry::open(h.directory.path()).await.unwrap();
    assert_eq!(registry.export_migrations("db").await.unwrap().len(), 2);
    registry.shutdown().await.unwrap();
}
