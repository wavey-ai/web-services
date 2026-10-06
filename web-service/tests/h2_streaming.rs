//! Streaming responses on the HTTP/1.1+HTTP/2 listener, over TLS against a real server.
//!
//! The streaming handler runs inside the response body. These tests cover the order of head
//! and body, backpressure on a client that stops reading, the reset of a body that a handler
//! leaves unfinished, the answer to a handler that stops before its head, cancellation when the
//! client goes away, panic containment, the request permit, shutdown, and a request body
//! streamed in while the response streams out. Each test runs over HTTP/1.1 and HTTP/2.

use std::convert::Infallible;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use bytes::Bytes;
use futures_util::StreamExt;
use http::{Request, Response, StatusCode};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Empty, StreamBody};
use hyper::body::{Frame, Incoming};
use hyper_util::rt::{TokioExecutor, TokioIo};
use portpicker::pick_unused_port;
use tokio::sync::{mpsc, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};
use tokio_rustls::rustls::{self, pki_types::ServerName, ClientConfig, RootCertStore};
use web_service::{
    BodyStream, H2H3Server, H2H3ServerBuilder, HandlerResponse, HandlerResult, Router, Server,
    ServerBuilder, ServerError, ServerHandle, StreamWriter, WebSocketHandler, WebTransportHandler,
};

/// Large enough that socket buffers and HTTP/2 windows hold only a few chunks.
const FLOOD_CHUNK: usize = 1024 * 1024;
/// More chunks than the 32 the listener once buffered per stream.
const FLOOD_CHUNKS: usize = 40;
const ORDERED_CHUNKS: usize = 8;
const ECHO_CHUNK: usize = 256 * 1024;
const ECHO_CHUNKS: usize = 64;
/// The HTTP/2 client's flow-control windows: the protocol's initial default.
const CLIENT_WINDOW: u32 = 65_535;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Proto {
    Http1,
    Http2,
}

struct State {
    started: Notify,
    go: Notify,
    release: Semaphore,
    produced: AtomicUsize,
    dropped: AtomicBool,
    dropped_notify: Notify,
    completed: AtomicBool,
}

impl Default for State {
    fn default() -> Self {
        Self {
            started: Notify::new(),
            go: Notify::new(),
            release: Semaphore::new(0),
            produced: AtomicUsize::new(0),
            dropped: AtomicBool::new(false),
            dropped_notify: Notify::new(),
            completed: AtomicBool::new(false),
        }
    }
}

struct DropGuard(Arc<State>);

impl Drop for DropGuard {
    fn drop(&mut self) {
        self.0.dropped.store(true, Ordering::SeqCst);
        self.0.dropped_notify.notify_one();
    }
}

fn ordered_chunk(index: usize) -> Bytes {
    Bytes::from(format!("chunk {index} of {ORDERED_CHUNKS};"))
}

fn ordered_body() -> Vec<u8> {
    (0..ORDERED_CHUNKS)
        .flat_map(|index| ordered_chunk(index).to_vec())
        .collect()
}

const BURST_CHUNKS: usize = 64;

/// Above the size at which an HTTP/2 receiver counts a DATA frame against its flood budget.
const BURST_CHUNK: usize = 4096;

fn burst_chunk(index: usize) -> Bytes {
    Bytes::from(vec![index as u8; BURST_CHUNK])
}

fn burst_body() -> Vec<u8> {
    (0..BURST_CHUNKS)
        .flat_map(|index| burst_chunk(index).to_vec())
        .collect()
}

fn flood_chunk(index: usize) -> Bytes {
    Bytes::from(vec![index as u8; FLOOD_CHUNK])
}

fn config_error(message: &str) -> ServerError {
    ServerError::Config(message.into())
}

struct TestRouter {
    state: Arc<State>,
    /// Answer the server's own errors in a JSON shape.
    shaped: bool,
}

impl TestRouter {
    async fn stream(&self, path: &str, writer: &mut Box<dyn StreamWriter>) -> HandlerResult<()> {
        let state = &self.state;
        match path {
            "/ordered" => {
                writer
                    .send_response(
                        Response::builder()
                            .header("x-test", "ordered")
                            .body(())
                            .unwrap(),
                    )
                    .await?;
                state.go.notified().await;
                for index in 0..ORDERED_CHUNKS {
                    writer.send_data(ordered_chunk(index)).await?;
                    tokio::task::yield_now().await;
                }
                writer.finish().await
            }
            "/burst" => {
                writer.send_response(Response::new(())).await?;
                for index in 0..BURST_CHUNKS {
                    writer.send_data(burst_chunk(index)).await?;
                    if index % 8 == 0 {
                        tokio::task::yield_now().await;
                    }
                }
                writer.finish().await
            }
            "/flood" => {
                writer.send_response(Response::new(())).await?;
                for index in 0..FLOOD_CHUNKS {
                    writer.send_data(flood_chunk(index)).await?;
                    state.produced.fetch_add(1, Ordering::SeqCst);
                }
                writer.finish().await
            }
            "/complete" => {
                writer.send_response(Response::new(())).await?;
                writer.send_data(ordered_chunk(0)).await?;
                writer.finish().await
            }
            "/abort-error" => {
                writer.send_response(Response::new(())).await?;
                writer.send_data(ordered_chunk(0)).await?;
                Err(config_error("handler failed mid-body"))
            }
            "/abort-return" => {
                writer.send_response(Response::new(())).await?;
                writer.send_data(ordered_chunk(0)).await?;
                Ok(())
            }
            "/panic-mid" => {
                let _guard = DropGuard(Arc::clone(state));
                writer.send_response(Response::new(())).await?;
                writer.send_data(ordered_chunk(0)).await?;
                state.started.notify_one();
                tokio::task::yield_now().await;
                panic!("test handler panic mid-body");
            }
            "/fail-before-head" => Err(config_error("handler failed before the head")),
            "/silent" => writer.finish().await,
            "/panic-before-head" => panic!("test handler panic before the head"),
            "/hang" => {
                let _guard = DropGuard(Arc::clone(state));
                writer.send_response(Response::new(())).await?;
                writer.send_data(ordered_chunk(0)).await?;
                state.started.notify_one();
                std::future::pending::<()>().await;
                Ok(())
            }
            "/hang-before-head" => {
                let _guard = DropGuard(Arc::clone(state));
                state.started.notify_one();
                std::future::pending::<()>().await;
                Ok(())
            }
            "/hang-in-send" => {
                let _guard = DropGuard(Arc::clone(state));
                writer.send_response(Response::new(())).await?;
                state.started.notify_one();
                for index in 0.. {
                    writer.send_data(flood_chunk(index)).await?;
                    state.produced.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            }
            "/hold" => {
                let _guard = DropGuard(Arc::clone(state));
                writer.send_response(Response::new(())).await?;
                state.started.notify_one();
                state
                    .release
                    .acquire()
                    .await
                    .map_err(|_| config_error("test release closed"))?
                    .forget();
                writer.send_data(ordered_chunk(0)).await?;
                writer.finish().await?;
                state.completed.store(true, Ordering::SeqCst);
                Ok(())
            }
            _ => Err(config_error("no such streaming route")),
        }
    }
}

#[async_trait]
impl Router for TestRouter {
    async fn route(&self, _req: Request<()>) -> HandlerResult<HandlerResponse> {
        Ok(HandlerResponse {
            body: Some(Bytes::from_static(b"ok")),
            ..Default::default()
        })
    }

    fn is_streaming(&self, path: &str) -> bool {
        path != "/fast"
    }

    async fn route_stream(
        &self,
        req: Request<()>,
        mut writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        self.stream(req.uri().path(), &mut writer).await
    }

    fn has_body_stream_handler(&self, path: &str) -> bool {
        matches!(path, "/echo" | "/count")
    }

    async fn route_body_stream(
        &self,
        req: Request<()>,
        mut body: BodyStream,
        mut writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        if req.uri().path() == "/count" {
            let mut read = 0usize;
            while let Some(chunk) = body.next().await {
                read += chunk?.len();
            }
            writer.send_response(Response::new(())).await?;
            writer.send_data(Bytes::from(read.to_string())).await?;
            return writer.finish().await;
        }
        writer.send_response(Response::new(())).await?;
        while let Some(chunk) = body.next().await {
            writer.send_data(chunk?).await?;
        }
        writer.finish().await
    }

    fn webtransport_handler(&self) -> Option<&dyn WebTransportHandler> {
        None
    }

    fn websocket_handler(&self, _path: &str) -> Option<&dyn WebSocketHandler> {
        None
    }

    fn server_response(&self, status: StatusCode) -> HandlerResponse {
        if !self.shaped {
            return HandlerResponse {
                status,
                body: Some(Bytes::from_static(b"internal server error")),
                content_type: Some("text/plain".into()),
                ..Default::default()
            };
        }
        HandlerResponse {
            status,
            body: Some(Bytes::from(format!(
                r#"{{"detail":"{}"}}"#,
                status.as_u16()
            ))),
            content_type: Some("application/json".into()),
            ..Default::default()
        }
    }
}

fn ensure_rustls_provider() {
    static INSTALL: OnceLock<()> = OnceLock::new();
    INSTALL.get_or_init(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

struct TestServer {
    handle: ServerHandle,
    port: u16,
    roots: Arc<RootCertStore>,
    state: Arc<State>,
}

async fn start(configure: impl Fn(H2H3ServerBuilder) -> H2H3ServerBuilder) -> TestServer {
    start_router(false, configure).await
}

async fn start_router(
    shaped: bool,
    configure: impl Fn(H2H3ServerBuilder) -> H2H3ServerBuilder,
) -> TestServer {
    ensure_rustls_provider();
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(cert.der().clone()).unwrap();
    let state = Arc::new(State::default());
    let mut attempts = 0;
    // Another test process can take a free port before this server binds it.
    let (mut handle, port) = loop {
        let port = pick_unused_port().expect("unused test port");
        let builder = H2H3Server::builder()
            .with_tls(
                STANDARD.encode(cert.pem()),
                STANDARD.encode(key_pair.serialize_pem()),
            )
            .with_bind_address(IpAddr::V4(Ipv4Addr::LOCALHOST))
            .with_port(port)
            .enable_h2(true)
            .enable_h3(false)
            .enable_websocket(false)
            .enable_webtransport(false)
            .with_router(Box::new(TestRouter {
                state: Arc::clone(&state),
                shaped,
            }));
        match configure(builder).build().unwrap().start().await {
            Ok(handle) => break (handle, port),
            Err(error) if attempts < 5 && error.to_string().contains("Address already in use") => {
                attempts += 1;
            }
            Err(error) => panic!("server start failed: {error}"),
        }
    };
    (&mut handle.ready_rx).await.expect("server readiness");
    TestServer {
        handle,
        port,
        roots: Arc::new(roots),
        state,
    }
}

impl TestServer {
    async fn connect(&self, proto: Proto) -> Client {
        let tcp =
            tokio::net::TcpStream::connect(SocketAddr::from((Ipv4Addr::LOCALHOST, self.port)))
                .await
                .unwrap();
        let mut config = ClientConfig::builder()
            .with_root_certificates(Arc::clone(&self.roots))
            .with_no_client_auth();
        config.alpn_protocols = vec![match proto {
            Proto::Http1 => b"http/1.1".to_vec(),
            Proto::Http2 => b"h2".to_vec(),
        }];
        let tls = tokio_rustls::TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        let io = TokioIo::new(tls);
        match proto {
            Proto::Http1 => {
                let (sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
                Client {
                    sender: Sender::Http1(sender),
                    connection: tokio::spawn(async move {
                        let _ = connection.await;
                    }),
                }
            }
            Proto::Http2 => {
                let (sender, connection) =
                    hyper::client::conn::http2::Builder::new(TokioExecutor::new())
                        .initial_stream_window_size(CLIENT_WINDOW)
                        .initial_connection_window_size(CLIENT_WINDOW)
                        .handshake(io)
                        .await
                        .unwrap();
                Client {
                    sender: Sender::Http2(sender),
                    connection: tokio::spawn(async move {
                        let _ = connection.await;
                    }),
                }
            }
        }
    }

    async fn stop(self) {
        let _ = self.handle.shutdown_tx.send(());
        timeout(Duration::from_secs(5), self.handle.finished_rx)
            .await
            .expect("server shutdown timed out")
            .unwrap();
    }

    async fn started(&self) {
        timeout(Duration::from_secs(5), self.state.started.notified())
            .await
            .expect("the streaming handler did not start");
    }

    async fn handler_dropped(&self) {
        timeout(Duration::from_secs(2), async {
            while !self.state.dropped.load(Ordering::SeqCst) {
                self.state.dropped_notify.notified().await;
            }
        })
        .await
        .expect("the streaming handler was not dropped");
    }

    /// `/fast` on a new HTTP/1.1 connection.
    async fn fast_status(&self) -> StatusCode {
        let mut client = self.connect(Proto::Http1).await;
        client.get("/fast").await.unwrap().status()
    }
}

type ClientBody = UnsyncBoxBody<Bytes, Infallible>;

enum Sender {
    Http1(hyper::client::conn::http1::SendRequest<ClientBody>),
    Http2(hyper::client::conn::http2::SendRequest<ClientBody>),
}

/// One client connection. Dropping it closes the connection.
struct Client {
    sender: Sender,
    connection: JoinHandle<()>,
}

impl Drop for Client {
    fn drop(&mut self) {
        self.connection.abort();
    }
}

impl Client {
    async fn send(&mut self, req: Request<ClientBody>) -> Result<Response<Incoming>, hyper::Error> {
        match &mut self.sender {
            Sender::Http1(sender) => {
                sender.ready().await?;
                sender.send_request(req).await
            }
            Sender::Http2(sender) => {
                sender.ready().await?;
                sender.send_request(req).await
            }
        }
    }

    fn http2_sender(&self) -> hyper::client::conn::http2::SendRequest<ClientBody> {
        match &self.sender {
            Sender::Http2(sender) => sender.clone(),
            Sender::Http1(_) => panic!("not an HTTP/2 connection"),
        }
    }

    async fn get(&mut self, path: &str) -> Result<Response<Incoming>, hyper::Error> {
        let req = Request::get(format!("https://localhost{path}"))
            .body(Empty::new().boxed_unsync())
            .unwrap();
        timeout(Duration::from_secs(5), self.send(req))
            .await
            .expect("no response head")
    }

    /// Whether the connection still serves a request.
    async fn serves_another_request(&mut self) -> bool {
        match self.get("/fast").await {
            Ok(response) => {
                response.status() == StatusCode::OK
                    && read_body(response).await.ok().as_deref() == Some(&b"ok"[..])
            }
            Err(_) => false,
        }
    }

    async fn closed(&mut self) {
        timeout(Duration::from_secs(5), &mut self.connection)
            .await
            .expect("the connection stayed open")
            .ok();
    }
}

async fn read_body(response: Response<Incoming>) -> Result<Bytes, hyper::Error> {
    timeout(Duration::from_secs(10), response.into_body().collect())
        .await
        .expect("the body neither ended nor failed")
        .map(|body| body.to_bytes())
}

/// A response must fail, not end cleanly. The server sends the head, a chunk and the abort
/// back to back, so the client can see the failure before it yields the head. On HTTP/2 the
/// failure must be the server's `RST_STREAM` with `INTERNAL_ERROR`.
async fn assert_aborted(
    proto: Proto,
    response: Result<Response<Incoming>, hyper::Error>,
    what: &str,
) {
    let error = match response {
        Ok(response) => {
            assert_eq!(response.status(), StatusCode::OK, "{what}");
            match read_body(response).await {
                Ok(body) => panic!("{what}: the body ended cleanly after {} bytes", body.len()),
                Err(error) => error,
            }
        }
        Err(error) => error,
    };
    if proto == Proto::Http2 {
        let error = format!("{error:?}");
        assert!(
            error.contains("INTERNAL_ERROR"),
            "{what}: expected an HTTP/2 stream reset, got {error}"
        );
    }
}

// 1. Order of head and body.

async fn the_head_arrives_before_the_body_and_chunks_arrive_in_order(proto: Proto) {
    let server = start(|builder| builder).await;
    let mut client = server.connect(proto).await;
    // The handler sends its head, then waits: the head must reach the client on its own.
    let response = client.get("/ordered").await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["x-test"], "ordered");
    assert_eq!(response.headers()["access-control-allow-origin"], "*");
    assert!(response.headers().contains_key("x-request-id"));
    server.state.go.notify_one();

    let mut body = response.into_body();
    let mut received = Vec::new();
    while let Some(frame) = timeout(Duration::from_secs(5), body.frame())
        .await
        .expect("the body stalled")
    {
        if let Ok(data) = frame.unwrap().into_data() {
            received.extend_from_slice(&data);
        }
    }
    assert_eq!(received, ordered_body());
    assert!(client.serves_another_request().await);
    server.stop().await;
}

async fn many_concurrent_streams_complete(proto: Proto) {
    const STREAMS: usize = 64;
    let server = start(|builder| builder).await;
    // HTTP/2 multiplexes every stream on one connection; HTTP/1.1 takes one each.
    let shared = server.connect(Proto::Http2).await;
    let mut requests = Vec::new();
    for _ in 0..STREAMS {
        let req = Request::get("https://localhost/burst")
            .body(Empty::new().boxed_unsync())
            .unwrap();
        let mut client = match proto {
            Proto::Http1 => Some(server.connect(proto).await),
            Proto::Http2 => None,
        };
        let mut sender = shared.http2_sender();
        requests.push(tokio::spawn(async move {
            let response = match client.as_mut() {
                Some(client) => client.send(req).await.unwrap(),
                None => {
                    sender.ready().await.unwrap();
                    sender.send_request(req).await.unwrap()
                }
            };
            read_body(response).await.unwrap()
        }));
    }
    for request in requests {
        let body = timeout(Duration::from_secs(30), request)
            .await
            .expect("concurrent streams stalled")
            .unwrap();
        assert_eq!(body, burst_body());
    }
    server.stop().await;
}

// 2. Backpressure.

/// The most chunks a handler may hand over while its client reads nothing. On HTTP/2 the
/// client's 64 KiB windows admit part of one chunk; hyper holds that chunk in its send buffer
/// and the next while it waits for capacity. On HTTP/1.1 hyper's write buffer takes one chunk
/// and the socket buffers take part of the next; up to 4 MiB of socket buffers is allowed for.
/// The handler may also have one chunk in the writer's slot.
fn stalled_bound(proto: Proto) -> usize {
    match proto {
        Proto::Http1 => 2 + 4,
        Proto::Http2 => 3,
    }
}

async fn a_stalled_client_holds_the_handler_back(proto: Proto) {
    let server = start(|builder| builder).await;
    let mut client = server.connect(proto).await;
    let response = client.get("/flood").await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    sleep(Duration::from_millis(500)).await;
    let first = server.state.produced.load(Ordering::SeqCst);
    sleep(Duration::from_millis(300)).await;
    let second = server.state.produced.load(Ordering::SeqCst);
    assert_eq!(
        first, second,
        "the handler kept producing for a stalled client"
    );
    assert!(
        second <= stalled_bound(proto),
        "the handler produced {second} chunks of {FLOOD_CHUNK} bytes for a client that read nothing"
    );

    let body = read_body(response).await.unwrap();
    assert_eq!(body.len(), FLOOD_CHUNK * FLOOD_CHUNKS);
    for (index, chunk) in body.chunks(FLOOD_CHUNK).enumerate() {
        assert!(
            chunk.iter().all(|byte| *byte == index as u8),
            "chunk {index}"
        );
    }
    assert_eq!(server.state.produced.load(Ordering::SeqCst), FLOOD_CHUNKS);
    server.stop().await;
}

// 3. A handler that stops without `finish` after the head.

async fn a_body_left_unfinished_is_aborted(proto: Proto) {
    let server = start(|builder| builder).await;
    for path in ["/abort-error", "/abort-return"] {
        let mut client = server.connect(proto).await;
        assert_aborted(proto, client.get(path).await, path).await;
        match proto {
            // A reset stream leaves the connection serving.
            Proto::Http2 => assert!(client.serves_another_request().await, "{path}"),
            // HTTP/1.1 can only abort the connection.
            Proto::Http1 => client.closed().await,
        }
    }

    let mut client = server.connect(proto).await;
    let response = client.get("/complete").await.unwrap();
    assert_eq!(read_body(response).await.unwrap(), ordered_chunk(0));
    server.stop().await;
}

// 4. A handler that stops before its head.

async fn a_handler_that_stops_before_its_head_is_answered_500(proto: Proto) {
    for shaped in [false, true] {
        let server = start_router(shaped, |builder| builder).await;
        let mut client = server.connect(proto).await;
        for path in ["/fail-before-head", "/silent", "/panic-before-head"] {
            let response = client.get(path).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "{path}"
            );
            assert!(response.headers().contains_key("x-request-id"), "{path}");
            let expected: &[u8] = if shaped {
                br#"{"detail":"500"}"#
            } else {
                b"internal server error"
            };
            assert_eq!(read_body(response).await.unwrap(), expected, "{path}");
            // The same connection serves the next request, on HTTP/1.1 too.
            assert!(client.serves_another_request().await, "{path}");
        }
        server.stop().await;
    }
}

// 5. Client disconnect.

async fn a_client_that_goes_away_cancels_the_handler(proto: Proto) {
    for path in ["/hang", "/hang-in-send", "/hang-before-head"] {
        let server = start(|builder| {
            builder
                .with_max_in_flight_requests(1)
                .with_request_queue_timeout_ms(0)
        })
        .await;
        let mut client = server.connect(proto).await;
        if path == "/hang-before-head" {
            let req = Request::get(format!("https://localhost{path}"))
                .body(Empty::new().boxed_unsync())
                .unwrap();
            let pending = client.send(req);
            tokio::pin!(pending);
            tokio::select! {
                _ = server.started() => {}
                _ = &mut pending => panic!("the handler answered"),
            }
            // Dropping the request future cancels the stream.
        } else {
            let response = client.get(path).await.unwrap();
            server.started().await;
            if path == "/hang-in-send" {
                sleep(Duration::from_millis(200)).await;
            }
            drop(response);
        }
        if proto == Proto::Http1 {
            // HTTP/1.1 has no stream cancel: the client closes the connection.
            drop(client);
            server.handler_dropped().await;
        } else {
            server.handler_dropped().await;
            assert!(client.serves_another_request().await, "{path}");
        }
        // The request permit went with the handler.
        assert_eq!(server.fast_status().await, StatusCode::OK, "{path}");
        server.stop().await;
    }
}

// 6. Handler panic.

async fn a_handler_panic_fails_its_stream_and_the_server_keeps_serving(proto: Proto) {
    let server = start(|builder| builder).await;
    let mut client = server.connect(proto).await;
    assert_aborted(proto, client.get("/panic-mid").await, "/panic-mid").await;
    server.handler_dropped().await;

    if proto == Proto::Http2 {
        assert!(client.serves_another_request().await);
    } else {
        client.closed().await;
        client = server.connect(proto).await;
    }
    let response = client.get("/ordered").await.unwrap();
    server.state.go.notify_one();
    assert_eq!(read_body(response).await.unwrap(), ordered_body());

    // A panic before the head is answered, and the connection serves on.
    let response = client.get("/panic-before-head").await.unwrap();
    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    read_body(response).await.unwrap();
    assert!(client.serves_another_request().await);
    server.stop().await;
}

// 7. The request permit.

async fn the_request_permit_is_held_until_the_stream_ends(proto: Proto) {
    let server = start(|builder| {
        builder
            .with_max_in_flight_requests(1)
            .with_request_queue_timeout_ms(0)
    })
    .await;

    // A stream whose handler waits after its head.
    let mut client = server.connect(proto).await;
    let response = client.get("/hold").await.unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    server.started().await;
    assert_eq!(server.fast_status().await, StatusCode::SERVICE_UNAVAILABLE);
    server.state.release.add_permits(1);
    assert_eq!(read_body(response).await.unwrap(), ordered_chunk(0));
    assert!(server.state.completed.load(Ordering::SeqCst));
    assert_eq!(server.fast_status().await, StatusCode::OK);

    // A stream held back by a client that reads nothing.
    let response = client.get("/flood").await.unwrap();
    sleep(Duration::from_millis(200)).await;
    assert_eq!(server.fast_status().await, StatusCode::SERVICE_UNAVAILABLE);
    let body = read_body(response).await.unwrap();
    assert_eq!(body.len(), FLOOD_CHUNK * FLOOD_CHUNKS);
    assert_eq!(server.fast_status().await, StatusCode::OK);
    server.stop().await;
}

// 8. Shutdown.

async fn shutdown_without_a_drain_aborts_streams_in_flight(proto: Proto) {
    for path in ["/hang", "/hang-in-send"] {
        let server = start(|builder| builder).await;
        let mut client = server.connect(proto).await;
        let response = client.get(path).await.unwrap();
        server.started().await;
        sleep(Duration::from_millis(100)).await;

        let state = Arc::clone(&server.state);
        server.stop().await;
        assert!(
            state.dropped.load(Ordering::SeqCst),
            "{path}: the server stopped with the handler still alive"
        );
        assert!(read_body(response).await.is_err(), "{path}");
    }
}

async fn shutdown_drain_lets_a_stream_in_flight_finish(proto: Proto) {
    let server = start(|builder| builder.with_shutdown_drain_ms(5_000)).await;
    let mut client = server.connect(proto).await;
    let response = client.get("/hold").await.unwrap();
    server.started().await;

    let TestServer {
        mut handle, state, ..
    } = server;
    let _ = handle.shutdown_tx.send(());
    sleep(Duration::from_millis(200)).await;
    assert!(
        handle.finished_rx.try_recv().is_err(),
        "the server stopped with a stream in flight"
    );
    state.release.add_permits(1);
    assert_eq!(read_body(response).await.unwrap(), ordered_chunk(0));
    timeout(Duration::from_secs(5), handle.finished_rx)
        .await
        .expect("the server did not stop after its streams drained")
        .unwrap();
    assert!(state.completed.load(Ordering::SeqCst));
}

async fn a_stream_still_running_when_the_drain_ends_is_aborted(proto: Proto) {
    let server = start(|builder| builder.with_shutdown_drain_ms(300)).await;
    let mut client = server.connect(proto).await;
    let response = client.get("/hang").await.unwrap();
    server.started().await;

    let state = Arc::clone(&server.state);
    let _ = server.handle.shutdown_tx.send(());
    sleep(Duration::from_millis(150)).await;
    assert!(
        !state.dropped.load(Ordering::SeqCst),
        "the handler was dropped inside the drain window"
    );
    timeout(Duration::from_secs(5), server.handle.finished_rx)
        .await
        .expect("the server did not stop after the drain window")
        .unwrap();
    assert!(
        state.dropped.load(Ordering::SeqCst),
        "the server stopped with the handler still alive"
    );
    assert!(read_body(response).await.is_err());
}

// 9. A request body streamed in while the response streams out.

async fn a_body_stream_echoes_without_deadlock(proto: Proto) {
    let server = start(|builder| builder).await;
    let mut client = server.connect(proto).await;
    let (body_tx, body_rx) = mpsc::channel::<Bytes>(1);
    let request_body =
        StreamBody::new(futures_util::stream::unfold(body_rx, |mut rx| async move {
            rx.recv()
                .await
                .map(|chunk| (Ok::<_, Infallible>(Frame::data(chunk)), rx))
        }))
        .boxed_unsync();
    let req = Request::post("https://localhost/echo")
        .body(request_body)
        .unwrap();

    let sending = tokio::spawn(async move {
        for index in 0..ECHO_CHUNKS {
            let chunk = Bytes::from(vec![index as u8; ECHO_CHUNK]);
            if body_tx.send(chunk).await.is_err() {
                return index;
            }
        }
        ECHO_CHUNKS
    });
    let response = timeout(Duration::from_secs(5), client.send(req))
        .await
        .expect("no response head while the request body streams")
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = timeout(Duration::from_secs(30), response.into_body().collect())
        .await
        .expect("the echo deadlocked")
        .unwrap()
        .to_bytes();
    assert_eq!(sending.await.unwrap(), ECHO_CHUNKS);
    assert_eq!(body.len(), ECHO_CHUNK * ECHO_CHUNKS);
    for (index, chunk) in body.chunks(ECHO_CHUNK).enumerate() {
        assert!(
            chunk.iter().all(|byte| *byte == index as u8),
            "chunk {index}"
        );
    }

    // A handler that reads the whole request body before its head.
    let req = Request::post("https://localhost/count")
        .body(
            http_body_util::Full::new(Bytes::from(vec![7u8; 4 * 1024 * 1024]))
                .map_err(|never| match never {})
                .boxed_unsync(),
        )
        .unwrap();
    let response = timeout(Duration::from_secs(10), client.send(req))
        .await
        .expect("no response head")
        .unwrap();
    assert_eq!(
        read_body(response).await.unwrap(),
        (4 * 1024 * 1024).to_string()
    );
    server.stop().await;
}

macro_rules! over_both_protocols {
    ($($test:ident),* $(,)?) => {
        mod http1 {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $test() {
                    super::$test(super::Proto::Http1).await;
                }
            )*
        }
        mod http2 {
            $(
                #[tokio::test(flavor = "multi_thread")]
                async fn $test() {
                    super::$test(super::Proto::Http2).await;
                }
            )*
        }
    };
}

over_both_protocols!(
    the_head_arrives_before_the_body_and_chunks_arrive_in_order,
    many_concurrent_streams_complete,
    a_stalled_client_holds_the_handler_back,
    a_body_left_unfinished_is_aborted,
    a_handler_that_stops_before_its_head_is_answered_500,
    a_client_that_goes_away_cancels_the_handler,
    a_handler_panic_fails_its_stream_and_the_server_keeps_serving,
    the_request_permit_is_held_until_the_stream_ends,
    shutdown_without_a_drain_aborts_streams_in_flight,
    shutdown_drain_lets_a_stream_in_flight_finish,
    a_stream_still_running_when_the_drain_ends_is_aborted,
    a_body_stream_echoes_without_deadlock,
);
