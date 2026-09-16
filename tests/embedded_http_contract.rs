use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    http::{Request, StatusCode},
};
use serde_json::{Value, json};
use sqlrest::{
    http::DataService,
    registry::{Outcome, PublishRequest, Registry},
};
use std::{fs, time::Duration};
use tower::ServiceExt;

async fn service(timeout_ms: u64) -> (tempfile::TempDir, Registry, DataService) {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path().join("databases/test");
    fs::create_dir_all(root.join("interfaces/echo/[id]")).unwrap();
    fs::create_dir_all(root.join("interfaces/broken")).unwrap();
    fs::create_dir_all(root.join("migrations")).unwrap();
    fs::write(
        root.join("interfaces/echo/[id]/post.sql"),
        "SELECT ${path.id:string} AS id, ${body.value:string} AS value",
    )
    .unwrap();
    fs::write(
        root.join("interfaces/echo/[id]/post.response.yaml"),
        r#"{"type":"object","properties":{"id":{"type":"string"},"value":{"type":"string"}},"required":["id","value"],"additionalProperties":false}"#,
    )
    .unwrap();
    fs::write(
        root.join("interfaces/broken/get.sql"),
        "SELECT private_missing_function() AS value",
    )
    .unwrap();
    fs::write(
        root.join("interfaces/broken/get.response.yaml"),
        r#"{"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}"#,
    )
    .unwrap();
    let registry = Registry::open(directory.path()).await.unwrap();
    let config: PublishRequest = serde_json::from_value(json!({
        "database": {"kind": "turso"},
        "limits": {"request_timeout_ms": timeout_ms}
    }))
    .unwrap();
    let operation = registry.publish("test", config).unwrap();
    let result = registry.wait_operation("test", operation).await.unwrap();
    assert_eq!(result.outcome, Outcome::Succeeded);
    let service = DataService::new(registry.clone());
    (directory, registry, service)
}

fn post(uri: &str, body: Body) -> Request<Body> {
    Request::post(uri)
        .header("content-type", "application/json")
        .body(body)
        .unwrap()
}

async fn json_body(response: axum::response::Response) -> Value {
    serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap()
}

#[tokio::test]
async fn data_service_mounts_without_listeners_and_preserves_http_contract() {
    let (_directory, _registry, service) = service(5000).await;
    drop(service.clone());
    let app = Router::new().nest_service("/corva", service.router());
    let response = app
        .clone()
        .oneshot(post(
            "/corva/db/test/echo/a%252Fb",
            Body::from(r#"{"value":"hello"}"#),
        ))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await,
        json!({"records":[{"id":"a%2Fb","value":"hello"}]})
    );

    for (uri, body, expected) in [
        (
            "/db/test/echo/a%2Fb",
            r#"{"value":"hello"}"#,
            "invalid_path",
        ),
        (
            "/db/test/echo/a",
            r#"{"value":1}"#,
            "parameter_type_mismatch",
        ),
    ] {
        let response = service.handle(post(uri, Body::from(body))).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json_body(response).await["error"]["code"], expected);
    }

    let response = app
        .clone()
        .oneshot(
            Request::head("/corva/db/test/broken")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(response.headers()["allow"], "GET");
    assert!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .is_empty()
    );

    let response = service
        .handle(Request::get("/db/test/broken").body(Body::empty()).unwrap())
        .await;
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    assert!(!String::from_utf8_lossy(&bytes).contains("private_missing_function"));

    let response = app
        .oneshot(
            Request::get("/corva/databases")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn mounted_data_service_respects_host_route_authorization() {
    async fn authorize(
        request: axum::extract::Request,
        next: axum::middleware::Next,
    ) -> axum::response::Response {
        use axum::response::IntoResponse;

        if request
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            != Some("Bearer test-token")
        {
            return StatusCode::UNAUTHORIZED.into_response();
        }
        next.run(request).await
    }

    let (_directory, _registry, service) = service(5000).await;
    let app = Router::new()
        .route("/health", axum::routing::get(|| async { "ok" }))
        .nest_service("/corva", service.router())
        .route_layer(axum::middleware::from_fn(authorize));

    // Include an ordinary host route so replacing nest_service with nest would
    // silently bypass auth on SQLRest, rather than merely panic in route_layer.
    for (method, uri) in [
        ("GET", "/health"),
        ("POST", "/corva/db/test/echo/a"),
        ("HEAD", "/corva/db/test/echo/a"),
        ("OPTIONS", "/corva/db/test/echo/a"),
        ("GET", "/corva/unknown"),
        ("GET", "/corva"),
        ("GET", "/corva/"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "{method} {uri}"
        );
    }

    let mut request = post(
        "/corva/db/test/echo/a%252Fb",
        Body::from(r#"{"value":"authorized"}"#),
    );
    request
        .headers_mut()
        .insert("authorization", "Bearer test-token".parse().unwrap());
    let response = app.oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        json_body(response).await,
        json!({"records":[{"id":"a%2Fb","value":"authorized"}]})
    );
    service.shutdown().await.unwrap();
}

fn pending_body() -> Body {
    Body::from_stream(futures_util::stream::pending::<Result<Bytes, std::io::Error>>())
}

#[tokio::test]
async fn embedded_upload_uses_request_deadline() {
    let (_directory, _registry, service) = service(30).await;
    let response = tokio::time::timeout(
        Duration::from_secs(2),
        service.handle(post("/db/test/echo/a", pending_body())),
    )
    .await
    .unwrap();
    assert_eq!(
        json_body(response).await["error"]["code"],
        "execution_timeout"
    );
    service.shutdown().await.unwrap();
}

#[tokio::test]
async fn embedded_shutdown_cancels_upload_and_closes_shared_registry() {
    let (_directory, registry, service) = service(60000).await;
    let request_service = service.clone();
    let task = tokio::spawn(async move {
        request_service
            .handle(post("/db/test/echo/a", pending_body()))
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        while registry.status("test").unwrap().active_requests == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(2), service.shutdown())
        .await
        .unwrap()
        .unwrap();
    let response = task.await.unwrap();
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let response = service
        .handle(post("/db/test/echo/a", Body::from(r#"{"value":"x"}"#)))
        .await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
}
