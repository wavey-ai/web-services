use crate::{
    config::ServerConfig,
    error::{H3Error, ServerError, ServerResult},
    h3::{add_cors_preflight_headers, add_cors_response_headers},
    http_range::apply_byte_range,
    traits::{
        response_header_name, response_header_value, BodyStream, HandlerResponse, Router,
        StreamWriter,
    },
};
use base64::{engine::general_purpose::STANDARD as base64_engine, Engine as _};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http::{
    header::{HOST, RANGE},
    HeaderName, HeaderValue, Method, Request, Response, Uri,
};
use std::{
    fs,
    net::{Ipv4Addr, SocketAddr},
    path::{Path, PathBuf},
    sync::Arc,
};
use tempfile::TempDir;
use tokio::{net::UdpSocket, sync::watch};
use tokio_quiche::{
    http3::{
        driver::{
            H3Event, InboundFrame, IncomingH3Headers, OutboundFrame, OutboundFrameSender,
            ServerEventStream, ServerH3Event,
        },
        settings::Http3Settings,
    },
    metrics::DefaultMetrics,
    quiche::h3::{Header, NameValue},
    settings::{CertificateKind, Hooks, QuicSettings, TlsCertificatePaths},
    ConnectionParams, ServerH3Driver,
};

const STREAM_WINDOW_BYTES: u64 = 16 * 1024 * 1024;
const MAX_CONCURRENT_STREAMS: u64 = 256;
const MAX_UDP_PAYLOAD_BYTES: usize = 1_400;
const QPACK_TABLE_BYTES: u64 = 4 * 1024;
const QPACK_BLOCKED_STREAMS: u64 = 16;

pub struct TokioQuicheHttp3Server {
    config: ServerConfig,
    router: Arc<dyn Router>,
}

impl TokioQuicheHttp3Server {
    pub fn new(config: ServerConfig, router: Arc<dyn Router>) -> Self {
        Self { config, router }
    }

    pub async fn start(&self, mut shutdown_rx: watch::Receiver<()>) -> ServerResult<()> {
        if self.config.enable_webtransport {
            return Err(ServerError::Config(
                "tokio-quiche H3 backend does not yet provide web-service WebTransport sessions"
                    .into(),
            ));
        }

        let tls_files = MaterializedTls::new(&self.config)?;
        let addr = SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), self.config.port);
        let socket = UdpSocket::bind(addr).await?;

        let quic = quic_settings();

        let cert_path = tls_files.path_string(&tls_files.cert_path)?;
        let key_path = tls_files.path_string(&tls_files.key_path)?;
        let params = ConnectionParams::new_server(
            quic,
            TlsCertificatePaths {
                cert: &cert_path,
                private_key: &key_path,
                kind: CertificateKind::X509,
            },
            Hooks::default(),
        );
        let mut listeners =
            tokio_quiche::listen([socket], params, DefaultMetrics).map_err(ServerError::Io)?;
        let accepted = listeners
            .first_mut()
            .ok_or_else(|| ServerError::Config("tokio-quiche created no UDP listener".into()))?;

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => break,
                connection = accepted.next() => {
                    let Some(connection) = connection else { break };
                    match connection {
                        Ok(connection) => {
                            let h3_settings = Http3Settings {
                                qpack_max_table_capacity: Some(QPACK_TABLE_BYTES),
                                qpack_blocked_streams: Some(QPACK_BLOCKED_STREAMS),
                                ..Http3Settings::default()
                            };
                            let (driver, mut controller) = ServerH3Driver::new(h3_settings);
                            connection.start(driver);
                            let router = Arc::clone(&self.router);
                            tokio::spawn(async move {
                                if let Err(error) = serve_connection(
                                    controller.event_receiver_mut(),
                                    router,
                                ).await {
                                    tracing::debug!(backend = "tokio-quiche", %error, "H3 connection ended");
                                }
                            });
                        }
                        Err(error) => {
                            tracing::warn!(backend = "tokio-quiche", %error, "rejected QUIC connection");
                        }
                    }
                }
            }
        }

        drop(tls_files);
        Ok(())
    }
}

fn quic_settings() -> QuicSettings {
    let mut quic = QuicSettings::default();
    quic.disable_client_ip_validation = true;
    quic.enable_dgram = false;
    quic.initial_max_data = STREAM_WINDOW_BYTES;
    quic.initial_max_stream_data_bidi_local = STREAM_WINDOW_BYTES;
    quic.initial_max_stream_data_bidi_remote = STREAM_WINDOW_BYTES;
    quic.initial_max_stream_data_uni = STREAM_WINDOW_BYTES;
    quic.initial_max_streams_bidi = MAX_CONCURRENT_STREAMS;
    quic.initial_max_streams_uni = MAX_CONCURRENT_STREAMS;
    quic.max_connection_window = STREAM_WINDOW_BYTES;
    quic.max_stream_window = STREAM_WINDOW_BYTES;
    quic.max_recv_udp_payload_size = MAX_UDP_PAYLOAD_BYTES;
    quic.max_send_udp_payload_size = MAX_UDP_PAYLOAD_BYTES;
    quic.discover_path_mtu = true;
    quic
}

struct MaterializedTls {
    _directory: TempDir,
    cert_path: PathBuf,
    key_path: PathBuf,
}

impl MaterializedTls {
    fn new(config: &ServerConfig) -> ServerResult<Self> {
        let directory = tempfile::Builder::new()
            .prefix("web-service-tokio-quiche-tls-")
            .tempdir()
            .map_err(ServerError::Io)?;
        let cert_path = directory.path().join("fullchain.pem");
        let key_path = directory.path().join("privkey.pem");
        let cert = base64_engine
            .decode(&config.cert_pem_base64)
            .map_err(|error| ServerError::Tls(error.to_string()))?;
        let key = base64_engine
            .decode(&config.privkey_pem_base64)
            .map_err(|error| ServerError::Tls(error.to_string()))?;
        fs::write(&cert_path, cert)?;
        fs::write(&key_path, key)?;
        set_private_permissions(&key_path)?;
        Ok(Self {
            _directory: directory,
            cert_path,
            key_path,
        })
    }

    fn path_string(&self, path: &Path) -> ServerResult<String> {
        path.to_str()
            .map(str::to_owned)
            .ok_or_else(|| ServerError::Tls("temporary TLS path is not valid UTF-8".into()))
    }
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> ServerResult<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> ServerResult<()> {
    Ok(())
}

async fn serve_connection(
    events: &mut ServerEventStream,
    router: Arc<dyn Router>,
) -> Result<(), H3Error> {
    while let Some(event) = events.recv().await {
        match event {
            ServerH3Event::Headers {
                incoming_headers, ..
            } => {
                let router = Arc::clone(&router);
                tokio::spawn(async move {
                    if let Err(error) = handle_request(incoming_headers, router).await {
                        tracing::warn!(backend = "tokio-quiche", %error, "H3 request failed");
                    }
                });
            }
            ServerH3Event::Core(H3Event::ConnectionError(error)) => {
                return Err(H3Error::Transport(error.to_string()));
            }
            ServerH3Event::Core(H3Event::ConnectionShutdown(error)) => {
                return match error {
                    Some(error) => Err(H3Error::Transport(error.to_string())),
                    None => Ok(()),
                };
            }
            ServerH3Event::Core(_) => {}
        }
    }
    Ok(())
}

async fn handle_request(
    incoming: IncomingH3Headers,
    router: Arc<dyn Router>,
) -> Result<(), H3Error> {
    let IncomingH3Headers {
        headers,
        send,
        recv,
        ..
    } = incoming;
    let request = request_from_h3_headers(headers)?;
    let is_preflight = request.method() == Method::OPTIONS;
    let has_body_stream_handler = router.has_body_stream_handler(request.uri().path());
    let is_streaming = !has_body_stream_handler && router.is_streaming(request.uri().path());
    let has_body_handler =
        !has_body_stream_handler && !is_streaming && router.has_body_handler(request.uri().path());

    if has_body_stream_handler {
        let body = body_stream(recv);
        let writer = TokioQuicheStreamWriter::new(send);
        return router
            .route_body_stream(request, body, Box::new(writer))
            .await
            .map_err(H3Error::Router);
    }

    if is_streaming {
        let writer = TokioQuicheStreamWriter::new(send);
        return router
            .route_stream(request, Box::new(writer))
            .await
            .map_err(H3Error::Router);
    }

    let range_header = request.headers().get(RANGE).cloned();
    let response = if has_body_handler {
        router
            .route_body(request, body_stream(recv))
            .await
            .map_err(H3Error::Router)?
    } else {
        router.route(request).await.map_err(H3Error::Router)?
    };
    send_handler_response(
        apply_byte_range(range_header.as_ref(), response),
        send,
        is_preflight,
    )
    .await
}

fn body_stream(recv: tokio_quiche::http3::driver::InboundFrameStream) -> BodyStream {
    Box::pin(futures_util::stream::unfold(
        (recv, false),
        |(mut recv, finished)| async move {
            if finished {
                return None;
            }
            loop {
                match recv.recv().await {
                    Some(InboundFrame::Body(data, fin)) if !data.is_empty() => {
                        return Some((Ok(data.freeze()), (recv, fin)));
                    }
                    Some(InboundFrame::Body(_, true)) | None => return None,
                    Some(InboundFrame::Body(_, false)) | Some(InboundFrame::Datagram(_)) => {
                        continue
                    }
                }
            }
        },
    ))
}

fn request_from_h3_headers(headers: Vec<Header>) -> Result<Request<()>, H3Error> {
    let mut method = None;
    let mut uri = None;
    let mut authority = None;
    let mut regular_headers = Vec::new();

    for header in headers {
        match header.name() {
            b":method" => {
                method = Some(
                    Method::from_bytes(header.value())
                        .map_err(|error| H3Error::Transport(error.to_string()))?,
                );
            }
            b":path" => {
                uri = Some(
                    Uri::try_from(header.value())
                        .map_err(|error| H3Error::Transport(error.to_string()))?,
                );
            }
            b":authority" => {
                authority = Some(
                    HeaderValue::from_bytes(header.value())
                        .map_err(|error| H3Error::Transport(error.to_string()))?,
                );
            }
            name if name.starts_with(b":") => {}
            name => regular_headers.push((
                HeaderName::from_bytes(name)
                    .map_err(|error| H3Error::Transport(error.to_string()))?,
                HeaderValue::from_bytes(header.value())
                    .map_err(|error| H3Error::Transport(error.to_string()))?,
            )),
        }
    }

    let mut request = Request::builder()
        .method(method.ok_or_else(|| H3Error::Transport("missing :method".into()))?)
        .uri(uri.ok_or_else(|| H3Error::Transport("missing :path".into()))?)
        .body(())
        .map_err(H3Error::Header)?;
    if let Some(authority) = authority {
        request.headers_mut().insert(HOST, authority);
    }
    for (name, value) in regular_headers {
        request.headers_mut().append(name, value);
    }
    Ok(request)
}

async fn send_handler_response(
    response: HandlerResponse,
    mut sender: OutboundFrameSender,
    is_preflight: bool,
) -> Result<(), H3Error> {
    let (headers, body) = response_parts(response, is_preflight)?;
    sender
        .send(OutboundFrame::Headers(headers, None))
        .await
        .map_err(channel_error)?;
    sender
        .send(OutboundFrame::Body(body.unwrap_or_default(), true))
        .await
        .map_err(channel_error)
}

fn response_parts(
    response: HandlerResponse,
    is_preflight: bool,
) -> Result<(Vec<Header>, Option<Bytes>), H3Error> {
    let HandlerResponse {
        status,
        body,
        content_type,
        headers,
        etag,
    } = response;
    let mut builder = Response::builder().status(status);
    if let Some(content_type) = content_type {
        builder = builder.header(
            "content-type",
            response_header_value(content_type).map_err(|error| H3Error::Header(error.into()))?,
        );
    }
    if let Some(etag) = etag {
        builder = builder.header("etag", etag.to_string());
    }
    for (name, value) in headers {
        builder = builder.header(
            response_header_name(name).map_err(|error| H3Error::Header(error.into()))?,
            response_header_value(value).map_err(|error| H3Error::Header(error.into()))?,
        );
    }
    let mut response = builder.body(()).map_err(H3Error::Header)?;
    if is_preflight {
        add_cors_preflight_headers(&mut response);
    } else {
        add_cors_response_headers(&mut response);
    }
    Ok((response_headers(&response), body))
}

fn response_headers(response: &Response<()>) -> Vec<Header> {
    let mut headers = vec![Header::new(
        b":status",
        response.status().as_str().as_bytes(),
    )];
    headers.extend(
        response
            .headers()
            .iter()
            .map(|(name, value)| Header::new(name.as_str().as_bytes(), value.as_bytes())),
    );
    headers
}

fn channel_error<T>(error: T) -> H3Error
where
    T: std::fmt::Display,
{
    H3Error::Transport(error.to_string())
}

struct TokioQuicheStreamWriter {
    sender: tokio::sync::Mutex<OutboundFrameSender>,
}

impl TokioQuicheStreamWriter {
    fn new(sender: OutboundFrameSender) -> Self {
        Self {
            sender: tokio::sync::Mutex::new(sender),
        }
    }
}

#[async_trait::async_trait]
impl StreamWriter for TokioQuicheStreamWriter {
    async fn send_response(&mut self, mut response: Response<()>) -> Result<(), ServerError> {
        add_cors_response_headers(&mut response);
        self.sender
            .lock()
            .await
            .send(OutboundFrame::Headers(response_headers(&response), None))
            .await
            .map_err(|error| transport_server_error(error.to_string()))
    }

    async fn send_data(&mut self, data: Bytes) -> Result<(), ServerError> {
        self.sender
            .lock()
            .await
            .send(OutboundFrame::Body(data, false))
            .await
            .map_err(|error| transport_server_error(error.to_string()))
    }

    async fn finish(&mut self) -> Result<(), ServerError> {
        self.sender
            .lock()
            .await
            .send(OutboundFrame::Body(Bytes::new(), true))
            .await
            .map_err(|error| transport_server_error(error.to_string()))
    }
}

fn transport_server_error(message: String) -> ServerError {
    ServerError::Handler(Box::new(std::io::Error::other(message)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::StatusCode;

    #[test]
    fn request_conversion_preserves_method_path_authority_and_headers() {
        let request = request_from_h3_headers(vec![
            Header::new(b":method", b"GET"),
            Header::new(b":scheme", b"https"),
            Header::new(b":authority", b"example.test"),
            Header::new(b":path", b"/part.mp4?sequence=7"),
            Header::new(b"range", b"bytes=0-99"),
        ])
        .expect("valid H3 request");
        assert_eq!(request.method(), Method::GET);
        assert_eq!(request.uri(), "/part.mp4?sequence=7");
        assert_eq!(request.headers()[HOST], "example.test");
        assert_eq!(request.headers()["range"], "bytes=0-99");
    }

    #[test]
    fn response_conversion_preserves_payload_and_cors() {
        let payload = Bytes::from_static(b"pcm");
        let (headers, body) = response_parts(
            HandlerResponse {
                status: StatusCode::OK,
                body: Some(payload.clone()),
                content_type: Some("audio/L24".into()),
                ..HandlerResponse::default()
            },
            false,
        )
        .expect("valid response");
        assert_eq!(body, Some(payload));
        assert!(headers
            .iter()
            .any(|header| { header.name() == b"content-type" && header.value() == b"audio/L24" }));
        assert!(headers.iter().any(|header| {
            header.name() == b"access-control-allow-origin" && header.value() == b"*"
        }));
        assert!(!headers
            .iter()
            .any(|header| header.name() == b"access-control-allow-methods"));
        assert!(!headers
            .iter()
            .any(|header| header.name() == b"access-control-allow-headers"));
    }

    #[test]
    fn preflight_response_includes_cors_permissions() {
        let (headers, _) =
            response_parts(HandlerResponse::default(), true).expect("valid response");
        assert!(headers.iter().any(|header| {
            header.name() == b"access-control-allow-origin" && header.value() == b"*"
        }));
        assert!(headers
            .iter()
            .any(|header| header.name() == b"access-control-allow-methods"));
        assert!(headers
            .iter()
            .any(|header| header.name() == b"access-control-allow-headers"));
    }

    #[test]
    fn transport_settings_allow_pmtu_above_the_conservative_default() {
        let settings = quic_settings();
        assert_eq!(settings.max_recv_udp_payload_size, 1_400);
        assert_eq!(settings.max_send_udp_payload_size, 1_400);
        assert!(settings.discover_path_mtu);
    }
}
