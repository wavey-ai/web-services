use super::balancer::LoadBalancingMode;
use super::state::ProxyState;
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use serde::{Deserialize, Serialize};
use tracing::debug;
use crate::{HandlerResponse, HandlerResult};

#[derive(Debug, Deserialize)]
struct AddBackendRequest {
    url: url::Url,
    max_connections: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct SetBalancerRequest {
    mode: String,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

#[derive(Debug, Serialize)]
struct BackendsResponse {
    mode: String,
    http: Vec<super::backend::BackendView>,
    ws: Vec<super::backend::BackendView>,
}

#[derive(Debug, Serialize)]
struct BalancerResponse {
    mode: String,
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
}

pub async fn handle_api(
    state: &ProxyState,
    req: Request<()>,
    body: Bytes,
) -> HandlerResult<HandlerResponse> {
    let path = req.uri().path();
    let method = req.method();

    match (method, path) {
        (&Method::GET, "/api/health") => Ok(json_response(
            &HealthResponse { status: "ok" },
            StatusCode::OK,
        )),
        (&Method::GET, "/api/backends") => {
            let (http, ws) = state.list_backends().await;
            let mode = state.mode().await;
            Ok(json_response(
                &BackendsResponse {
                    mode: mode.as_str().to_string(),
                    http,
                    ws,
                },
                StatusCode::OK,
            ))
        }
        (&Method::POST, "/api/backends") => {
            let request: AddBackendRequest = match serde_json::from_slice(&body) {
                Ok(value) => value,
                Err(err) => {
                    return Ok(json_error(
                        StatusCode::BAD_REQUEST,
                        format!("invalid json: {err}"),
                    ));
                }
            };
            debug!(
                backend_url = %request.url,
                max_connections = ?request.max_connections,
                "api add backend"
            );
            match state
                .add_backend(request.url, request.max_connections)
                .await
            {
                Ok(backend) => Ok(json_response(&backend, StatusCode::CREATED)),
                Err(err) => Ok(json_error(StatusCode::BAD_REQUEST, err)),
            }
        }
        (&Method::DELETE, path) if path.starts_with("/api/backends/") => {
            let id = path.trim_start_matches("/api/backends/");
            if id.is_empty() {
                return Ok(json_error(
                    StatusCode::BAD_REQUEST,
                    "backend id is required".to_string(),
                ));
            }
            debug!(backend_id = %id, "api remove backend");
            match state.remove_backend(id).await {
                Some(backend) => Ok(json_response(&backend, StatusCode::OK)),
                None => Ok(json_error(
                    StatusCode::NOT_FOUND,
                    "backend not found".to_string(),
                )),
            }
        }
        (&Method::GET, "/api/balancer") => {
            let mode = state.mode().await;
            Ok(json_response(
                &BalancerResponse {
                    mode: mode.as_str().to_string(),
                },
                StatusCode::OK,
            ))
        }
        (&Method::PUT, "/api/balancer") => {
            let request: SetBalancerRequest = match serde_json::from_slice(&body) {
                Ok(value) => value,
                Err(err) => {
                    return Ok(json_error(
                        StatusCode::BAD_REQUEST,
                        format!("invalid json: {err}"),
                    ));
                }
            };
            let mode = match request.mode.parse::<LoadBalancingMode>() {
                Ok(mode) => mode,
                Err(err) => {
                    return Ok(json_error(StatusCode::BAD_REQUEST, err));
                }
            };
            debug!(mode = %mode.as_str(), "api set load balancer mode");
            state.set_mode(mode).await;
            Ok(json_response(
                &BalancerResponse {
                    mode: mode.as_str().to_string(),
                },
                StatusCode::OK,
            ))
        }
        _ => Ok(json_error(
            StatusCode::NOT_FOUND,
            "unknown api endpoint".to_string(),
        )),
    }
}

fn json_response<T: Serialize>(value: &T, status: StatusCode) -> HandlerResponse {
    let body = serde_json::to_vec(value).unwrap_or_else(|_| b"{}".to_vec());
    HandlerResponse {
        status,
        body: Some(Bytes::from(body)),
        content_type: Some("application/json".to_string()),
        headers: vec![],
        etag: None,
    }
}

fn json_error(status: StatusCode, message: String) -> HandlerResponse {
    json_response(
        &ErrorResponse {
            error: message,
        },
        status,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::balancer::LoadBalancingMode;
    use bytes::Bytes;
    use http::StatusCode;
    use serde_json::{json, Value};
    use std::sync::Once;

    fn init_crypto() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    async fn call_api(
        state: &ProxyState,
        method: Method,
        path: &str,
        body: Bytes,
    ) -> HandlerResponse {
        let req = Request::builder()
            .method(method)
            .uri(path)
            .body(())
            .unwrap();
        handle_api(state, req, body).await.unwrap()
    }

    fn response_json(response: &HandlerResponse) -> Value {
        let body = response.body.as_ref().expect("response body");
        serde_json::from_slice(body).expect("valid json response")
    }

    #[tokio::test]
    async fn health_ok() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();
        let response = call_api(&state, Method::GET, "/api/health", Bytes::new()).await;

        assert_eq!(response.status, StatusCode::OK);
        let payload = response_json(&response);
        assert_eq!(payload["status"], "ok");
    }

    #[tokio::test]
    async fn add_list_delete_backend() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();

        let body = Bytes::from(serde_json::to_vec(&json!({
            "url": "https://example.com",
            "max_connections": null
        }))
        .unwrap());
        let response = call_api(&state, Method::POST, "/api/backends", body).await;
        assert_eq!(response.status, StatusCode::CREATED);
        let created = response_json(&response);
        let id = created["id"].as_str().unwrap().to_string();

        let response = call_api(&state, Method::GET, "/api/backends", Bytes::new()).await;
        assert_eq!(response.status, StatusCode::OK);
        let list = response_json(&response);
        assert_eq!(list["mode"], "leastconn");
        assert_eq!(list["http"].as_array().unwrap().len(), 1);
        assert!(list["ws"].as_array().unwrap().is_empty());

        let response = call_api(
            &state,
            Method::DELETE,
            &format!("/api/backends/{id}"),
            Bytes::new(),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK);

        let response = call_api(
            &state,
            Method::DELETE,
            &format!("/api/backends/{id}"),
            Bytes::new(),
        )
        .await;
        assert_eq!(response.status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn add_backend_rejects_invalid_json() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();

        let response =
            call_api(&state, Method::POST, "/api/backends", Bytes::from_static(b"{oops"))
                .await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        let payload = response_json(&response);
        assert!(payload["error"]
            .as_str()
            .unwrap()
            .contains("invalid json"));
    }

    #[tokio::test]
    async fn add_backend_rejects_invalid_scheme() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();

        let body = Bytes::from(serde_json::to_vec(&json!({
            "url": "ftp://example.com"
        }))
        .unwrap());
        let response = call_api(&state, Method::POST, "/api/backends", body).await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        let payload = response_json(&response);
        assert!(payload["error"]
            .as_str()
            .unwrap()
            .contains("unsupported backend scheme"));
    }

    #[tokio::test]
    async fn set_balancer_and_get_mode() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();

        let body = Bytes::from(serde_json::to_vec(&json!({
            "mode": "queue"
        }))
        .unwrap());
        let response = call_api(&state, Method::PUT, "/api/balancer", body).await;
        assert_eq!(response.status, StatusCode::OK);

        let response = call_api(&state, Method::GET, "/api/balancer", Bytes::new()).await;
        assert_eq!(response.status, StatusCode::OK);
        let payload = response_json(&response);
        assert_eq!(payload["mode"], "queue");
    }

    #[tokio::test]
    async fn set_balancer_rejects_invalid_mode() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();

        let body = Bytes::from(serde_json::to_vec(&json!({
            "mode": "unknown"
        }))
        .unwrap());
        let response = call_api(&state, Method::PUT, "/api/balancer", body).await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        let payload = response_json(&response);
        assert!(payload["error"]
            .as_str()
            .unwrap()
            .contains("unsupported load balancer"));
    }

    #[tokio::test]
    async fn delete_backend_requires_id() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();

        let response = call_api(&state, Method::DELETE, "/api/backends/", Bytes::new()).await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
        let payload = response_json(&response);
        assert!(payload["error"]
            .as_str()
            .unwrap()
            .contains("backend id is required"));
    }

    #[tokio::test]
    async fn unknown_endpoint_returns_not_found() {
        init_crypto();
        let state = ProxyState::new(LoadBalancingMode::LeastConn).unwrap();

        let response = call_api(&state, Method::GET, "/api/unknown", Bytes::new()).await;
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        let payload = response_json(&response);
        assert!(payload["error"]
            .as_str()
            .unwrap()
            .contains("unknown api endpoint"));
    }
}
