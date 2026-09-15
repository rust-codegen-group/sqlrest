//! Independent data and management listeners over the shared Registry.
use crate::{
    SqlrestError,
    params::Input,
    registry::{self, PublishRequest, Registry},
};
use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::{Method, StatusCode, header},
    response::Response,
};
use serde::Serialize;
use std::{future::Future, net::SocketAddr};
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub struct Server {
    registry: Registry,
    data: TcpListener,
    management: TcpListener,
    data_address: SocketAddr,
    management_address: SocketAddr,
}

impl Server {
    /// Bind both listeners before starting either. No address policy is imposed.
    pub async fn bind(
        registry: Registry,
        data: SocketAddr,
        management: SocketAddr,
    ) -> Result<Self, SqlrestError> {
        let data = TcpListener::bind(data)
            .await
            .map_err(|_| SqlrestError::new(500, "listen_failed", "Cannot bind data listener"))?;
        let management = TcpListener::bind(management).await.map_err(|_| {
            SqlrestError::new(500, "listen_failed", "Cannot bind management listener")
        })?;
        Self::from_listeners(registry, data, management)
    }

    pub fn from_listeners(
        registry: Registry,
        data: TcpListener,
        management: TcpListener,
    ) -> Result<Self, SqlrestError> {
        let data_address = data.local_addr().map_err(|_| server_failed())?;
        let management_address = management.local_addr().map_err(|_| server_failed())?;
        Ok(Self {
            registry,
            data,
            management,
            data_address,
            management_address,
        })
    }

    pub fn data_address(&self) -> SocketAddr {
        self.data_address
    }

    pub fn management_address(&self) -> SocketAddr {
        self.management_address
    }

    /// Awaiting completion confirms that accepted operations and requests drained.
    pub async fn serve(self, shutdown: impl Future<Output = ()>) -> Result<(), SqlrestError> {
        let stop = CancellationToken::new();
        let lifetime = ServerLifetime {
            registry: self.registry.clone(),
            stop: stop.clone(),
        };
        let state = ServiceState {
            registry: self.registry.clone(),
            stop: stop.clone(),
        };
        // A fallback handler keeps explicit HEAD/OPTIONS and our JSON errors;
        // it does not install implicit GET-to-HEAD or CORS behavior.
        let data = Router::new()
            .fallback(data_handler)
            .with_state(state.clone());
        let management = Router::new().fallback(management_handler).with_state(state);
        let mut servers = JoinSet::new();
        let data_stop = stop.clone();
        servers.spawn(async move {
            axum::serve(self.data, data)
                .with_graceful_shutdown(data_stop.cancelled_owned())
                .await
        });
        let management_stop = stop.clone();
        servers.spawn(async move {
            axum::serve(self.management, management)
                .with_graceful_shutdown(management_stop.cancelled_owned())
                .await
        });
        let mut failure = None;
        tokio::select! {
            _ = shutdown => {},
            _ = servers.join_next() => { failure = Some(server_failed()); },
        }
        // Linearize closed admission before stopping accepts or draining HTTP.
        self.registry.start_shutdown()?;
        stop.cancel();
        let (http_result, core_result) = tokio::join!(
            async {
                let mut failed = false;
                while let Some(result) = servers.join_next().await {
                    if !matches!(result, Ok(Ok(()))) {
                        failed = true;
                    }
                }
                if failed { Err(server_failed()) } else { Ok(()) }
            },
            self.registry.shutdown(),
        );
        drop(lifetime);
        core_result?;
        http_result?;
        failure.map_or(Ok(()), Err)
    }
}

struct ServerLifetime {
    registry: Registry,
    stop: CancellationToken,
}

impl Drop for ServerLifetime {
    fn drop(&mut self) {
        let _ = self.registry.start_shutdown();
        self.stop.cancel();
    }
}

#[derive(Clone)]
struct ServiceState {
    registry: Registry,
    stop: CancellationToken,
}

async fn data_handler(State(state): State<ServiceState>, request: Request) -> Response {
    let method = request.method().clone();
    let response = data_request(&state, request)
        .await
        .unwrap_or_else(error_response);
    head_response(&method, response)
}

async fn data_request(state: &ServiceState, request: Request) -> Result<Response, SqlrestError> {
    let segments = path(request.uri().path())?;
    let (name, route) = match segments.as_slice() {
        [prefix, name, route @ ..] if prefix == "db" && valid_name(name) => {
            (name.clone(), route.to_vec())
        }
        _ => return Err(not_found()),
    };
    let method = request.method().as_str().to_owned();
    let route: Vec<_> = route.iter().map(String::as_str).collect();
    let admitted = state.registry.admit(&name, &method, &route)?;
    let deadline = tokio::time::Instant::now() + admitted.limits.timeout;
    let query = request.uri().query().unwrap_or("").to_owned();
    let (parts, body) = request.into_parts();
    let bytes = tokio::select! {
        biased;
        _ = state.stop.cancelled() => return Err(registry::shutting_down()),
        result = tokio::time::timeout_at(deadline, read_body(body)) => {
            result.map_err(|_| crate::turso_driver::timeout())??
        }
    };
    if !bytes.is_empty() {
        require_json(&parts.headers)?;
    }
    let parsing = tokio::task::spawn_blocking(move || Input::from_http(&query, &bytes));
    let input = tokio::select! {
        biased;
        _ = state.stop.cancelled() => return Err(registry::shutting_down()),
        result = tokio::time::timeout_at(deadline, parsing) => {
            result
                .map_err(|_| crate::turso_driver::timeout())?
                .map_err(|_| server_failed())??
        }
    };
    let remaining = deadline
        .checked_duration_since(tokio::time::Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(crate::turso_driver::timeout)?;
    // The executor owns its timeout and cleanup; don't return early by dropping
    // it under a second outer SQL timeout.
    let bytes = admitted.execute(input, Some(remaining)).await?;
    Ok(bytes_response(StatusCode::OK, bytes))
}

async fn management_handler(State(state): State<ServiceState>, request: Request) -> Response {
    let method = request.method().clone();
    let response = management_request(&state, request)
        .await
        .unwrap_or_else(error_response);
    head_response(&method, response)
}

async fn management_request(
    state: &ServiceState,
    request: Request,
) -> Result<Response, SqlrestError> {
    let segments = path(request.uri().path())?;
    if segments == ["databases"] {
        if request.method() != Method::GET {
            return Err(method_not_allowed("GET"));
        }
        if request.uri().query().is_some_and(|query| !query.is_empty()) {
            return Err(SqlrestError::new(
                400,
                "invalid_query",
                "Unsupported management query parameter",
            ));
        }
        return json_response(StatusCode::OK, &state.registry.statuses());
    }
    let (name, tail) = match segments.as_slice() {
        [prefix, name, tail @ ..] if prefix == "databases" && valid_name(name) => {
            (name.clone(), tail)
        }
        _ => return Err(not_found()),
    };
    let query = Input::from_http(request.uri().query().unwrap_or(""), b"")?.query;
    if !query.is_empty()
        && !(tail == ["openapi"] && query.len() == 1 && query.contains_key("server_url"))
    {
        return Err(SqlrestError::new(
            400,
            "invalid_query",
            "Unsupported management query parameter",
        ));
    }
    match (request.method().as_str(), tail) {
        ("POST", [operation]) if operation == "publish" => {
            require_json(request.headers())?;
            let bytes = tokio::select! {
                biased;
                _ = state.stop.cancelled() => return Err(registry::shutting_down()),
                bytes = read_body(request.into_body()) => bytes?,
            };
            let parsing = tokio::task::spawn_blocking(move || {
                serde_json::from_slice::<PublishRequest>(&bytes)
                    .map_err(|_| invalid_configuration())
            });
            let config = tokio::select! {
                biased;
                _ = state.stop.cancelled() => return Err(registry::shutting_down()),
                result = parsing => result.map_err(|_| server_failed())??,
            };
            accepted(state.registry.publish(&name, config)?)
        }
        ("GET", []) => json_response(StatusCode::OK, &state.registry.status(&name)?),
        ("DELETE", []) => accepted(state.registry.unregister(&name)?),
        ("GET", [resource]) if resource == "openapi" => {
            let registry = state.registry.clone();
            let server_url = query
                .get("server_url")
                .cloned()
                .unwrap_or_else(|| format!("/db/{name}"));
            tokio::task::spawn_blocking(move || {
                json_response(StatusCode::OK, &registry.openapi(&name, &server_url)?)
            })
            .await
            .map_err(|_| server_failed())?
        }
        ("GET", [resource]) if resource == "migrations" => {
            let records = state.registry.export_migrations(&name).await?;
            tokio::task::spawn_blocking(move || {
                json_response(StatusCode::OK, &serde_json::json!({"migrations":records}))
            })
            .await
            .map_err(|_| server_failed())?
        }
        ("GET", [resource, id]) if resource == "operations" => json_response(
            StatusCode::OK,
            &state.registry.operation(&name, id.parse()?)?,
        ),
        (_, []) => Err(method_not_allowed("DELETE, GET")),
        (_, [resource]) if matches!(resource.as_str(), "publish" | "openapi" | "migrations") => {
            Err(method_not_allowed(if resource == "publish" {
                "POST"
            } else {
                "GET"
            }))
        }
        (_, [resource, _]) if resource == "operations" => Err(method_not_allowed("GET")),
        _ => Err(not_found()),
    }
}

fn accepted(id: crate::registry::OperationId) -> Result<Response, SqlrestError> {
    json_response(
        StatusCode::ACCEPTED,
        &serde_json::json!({"operation_id":id}),
    )
}

async fn read_body(body: Body) -> Result<Bytes, SqlrestError> {
    // No implicit Axum JSON extractor/body-size cap: the agreed limit is not bytes.
    to_bytes(body, usize::MAX)
        .await
        .map_err(|_| SqlrestError::new(400, "invalid_body", "Cannot read request body"))
}

fn require_json(headers: &axum::http::HeaderMap) -> Result<(), SqlrestError> {
    if headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.split(';').next())
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("application/json"))
    {
        Ok(())
    } else {
        Err(SqlrestError::new(
            415,
            "unsupported_media_type",
            "Expected application/json",
        ))
    }
}

fn path(path: &str) -> Result<Vec<String>, SqlrestError> {
    let raw = path.strip_prefix('/').ok_or_else(not_found)?;
    let mut segments = Vec::new();
    for segment in raw.split('/') {
        let bytes = segment.as_bytes();
        if bytes.iter().enumerate().any(|(i, b)| {
            *b == b'%'
                && (i + 2 >= bytes.len()
                    || !bytes[i + 1].is_ascii_hexdigit()
                    || !bytes[i + 2].is_ascii_hexdigit())
        }) {
            return Err(invalid_path());
        }
        let decoded = percent_encoding::percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| invalid_path())?;
        if decoded.contains('/') {
            return Err(invalid_path());
        }
        segments.push(decoded.into_owned());
    }
    // Both spellings of a database's root are accepted; non-root trailing slash
    // and repeated slashes are not normalized into another route.
    if segments.len() == 3 && segments[2].is_empty() {
        segments.pop();
    }
    Ok(segments)
}

fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
}

fn json_response(status: StatusCode, value: &impl Serialize) -> Result<Response, SqlrestError> {
    let bytes = serde_json::to_vec(value).map_err(|_| server_failed())?;
    Ok(bytes_response(status, bytes))
}

fn bytes_response(status: StatusCode, bytes: Vec<u8>) -> Response {
    let length = bytes.len();
    let mut response = Response::new(Body::from(bytes));
    *response.status_mut() = status;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        header::HeaderValue::from_static("application/json"),
    );
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, header::HeaderValue::from(length));
    response
}

fn error_response(error: SqlrestError) -> Response {
    let status = StatusCode::from_u16(error.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let allowed = error
        .allowed_methods
        .as_ref()
        .and_then(|s| header::HeaderValue::from_str(s).ok());
    let mut response = json_response(status, &serde_json::json!({"error":error}))
        .expect("error JSON is serializable");
    if let Some(allowed) = allowed {
        response.headers_mut().insert(header::ALLOW, allowed);
    }
    response
}

fn head_response(method: &Method, mut response: Response) -> Response {
    if *method == Method::HEAD {
        *response.body_mut() = Body::empty();
    }
    response
}

fn invalid_configuration() -> SqlrestError {
    SqlrestError::new(
        400,
        "invalid_configuration",
        "Invalid publish configuration",
    )
}

fn invalid_path() -> SqlrestError {
    SqlrestError::new(400, "invalid_path", "Invalid request path encoding")
}

fn not_found() -> SqlrestError {
    SqlrestError::new(404, "route_not_found", "No matching route")
}

fn method_not_allowed(allowed: &str) -> SqlrestError {
    SqlrestError::new(
        405,
        "method_not_allowed",
        "Method not defined for this route",
    )
    .with_allowed_methods(allowed.into())
}

fn server_failed() -> SqlrestError {
    SqlrestError::new(500, "server_failed", "HTTP server operation failed")
}
