use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_util::{SinkExt, StreamExt};
use h3_quinn::Connection as QuinnConnection;
use h3_webtransport::server::WebTransportSession;
use http::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use playlists::chunk_cache::ChunkCache;
use std::sync::Arc;
use tokio::time::{Duration, Instant, sleep};
use tokio_tungstenite::{WebSocketStream, tungstenite::Message};
use tracing::{error, info};
use web_service::{
    HandlerResult, ServerError, StreamWriter, StreamingHandler, WebSocketHandler,
    WebTransportHandler,
};
use xmpegts::define::epsi_stream_type;
use xmpegts::ts::TsMuxer;

/// Tail streaming handler
pub struct TailStreamHandler {
    chunk_cache: Arc<ChunkCache>,
}

impl TailStreamHandler {
    pub fn new(chunk_cache: Arc<ChunkCache>) -> Self {
        Self { chunk_cache }
    }
}

#[async_trait]
impl StreamingHandler for TailStreamHandler {
    async fn handle_stream(
        &self,
        _req: Request<()>,
        parts: Vec<&str>,
        mut writer: Box<dyn StreamWriter>,
    ) -> HandlerResult<()> {
        if parts.len() != 2 || parts[1] != "tail" {
            return Err(ServerError::Config("Invalid tail request path".into()));
        }
        let stream_id = parts[0]
            .parse::<u64>()
            .map_err(|_| ServerError::Config("Invalid stream ID".into()))?;
        let idx = self
            .chunk_cache
            .get_stream_idx(stream_id)
            .await
            .ok_or(ServerError::Config("Stream not found".into()))?;
        let mut last = self
            .chunk_cache
            .last(idx)
            .ok_or(ServerError::Config("No data available for stream".into()))?;
        info!("{} [{}] last sequence is {}", stream_id, idx, last);

        let response = Response::builder()
            .status(StatusCode::OK)
            .header("Stream-Id", stream_id.to_string())
            .header("content-type", "video/mp4")
            .header("cache-control", "no-cache")
            .body(())
            .unwrap();
        writer.send_response(response).await?;

        loop {
            if let Some((data, _)) = self.get_part_with_timeout(idx, last).await {
                writer.send_data(data).await.map_err(|e| {
                    error!("Error sending on stream: {}", e);
                    ServerError::Handler(Box::new(e))
                })?;
                last += 1;
            } else {
                sleep(Duration::from_millis(5)).await;
            }
        }
    }

    fn is_streaming(&self, path: &str) -> bool {
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        parts.len() == 2 && parts[1] == "tail"
    }
}

impl TailStreamHandler {
    async fn get_part_with_timeout(&self, idx: usize, id: usize) -> Option<(Bytes, u64)> {
        let timeout = Duration::from_secs(3);
        let start = Instant::now();
        let interval = Duration::from_millis(1);

        while start.elapsed() < timeout {
            if let Some(d) = self.chunk_cache.get(idx, id).await {
                return Some(d.clone());
            }
            sleep(interval).await;
        }
        None
    }
}

/// WebTransport streaming handler for HLS
pub struct HlsWebTransportHandler {
    chunk_cache: Arc<ChunkCache>,
}

impl HlsWebTransportHandler {
    pub fn new(chunk_cache: Arc<ChunkCache>) -> Self {
        Self { chunk_cache }
    }
}

#[async_trait]
impl WebTransportHandler for HlsWebTransportHandler {
    async fn handle_session(
        &self,
        session: WebTransportSession<QuinnConnection, Bytes>,
    ) -> HandlerResult<()> {
        handle_transport_session(session, Arc::clone(&self.chunk_cache)).await
    }
}

async fn handle_transport_session(
    session: WebTransportSession<QuinnConnection, Bytes>,
    chunk_cache: Arc<ChunkCache>,
) -> HandlerResult<()> {
    let mut muxer = TsMuxer::new();

    // 1) Add the AAC stream to the muxer, logging + returning on error
    let pid = match muxer.add_stream(epsi_stream_type::PSI_STREAM_AAC, BytesMut::new()) {
        Ok(pid) => pid,
        Err(e) => {
            error!("Failed to add stream to TS muxer: {:?}", e);
            let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
            return Err(ServerError::Handler(Box::new(io_err)));
        }
    };

    let mut reader = session.datagram_reader();
    let mut sender = session.datagram_sender();

    loop {
        let datagram = match reader.read_datagram().await {
            Ok(d) => d,
            Err(e) => {
                error!("Failed to read datagram: {}", e);
                break;
            }
        };

        let mut buf = datagram.into_payload();
        if buf.remaining() < 8 {
            continue;
        }
        let stream_id_u64 = buf.get_u64();
        let stream_idx = stream_id_u64 as usize;
        let mut last = chunk_cache.last(stream_idx).unwrap_or(0);

        while let Some((data, _seq)) = chunk_cache.get(stream_idx, last).await {
            // 2) Mux to TS, logging + returning on error
            if let Err(e) = muxer.write(pid, 0, 0, 0, BytesMut::from(data)) {
                error!("TS muxer write error: {:?}", e);
                let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                return Err(ServerError::Handler(Box::new(io_err)));
            }

            let encoded = muxer.get_data();
            for chunk in encoded.chunks(188 * 6) {
                let mut out = BytesMut::with_capacity(8 + chunk.len());
                out.put_u64(stream_id_u64);
                out.put_slice(chunk);

                // 3) Send datagram, logging + returning on error
                if let Err(e) = sender.send_datagram(out.freeze()) {
                    error!("Failed to send datagram: {:?}", e);
                    let io_err = std::io::Error::new(std::io::ErrorKind::Other, e.to_string());
                    return Err(ServerError::Handler(Box::new(io_err)));
                }
            }

            last += 1;
        }

        sleep(Duration::from_millis(1)).await;
    }

    info!("WebTransport session finished");
    Ok(())
}

/// WebSocket tail handler (binary frames carrying fMP4 parts)
pub struct TailWebSocketHandler {
    chunk_cache: Arc<ChunkCache>,
}

impl TailWebSocketHandler {
    pub fn new(chunk_cache: Arc<ChunkCache>) -> Self {
        Self { chunk_cache }
    }
}

#[async_trait]
impl WebSocketHandler for TailWebSocketHandler {
    async fn handle_websocket(
        &self,
        req: Request<()>,
        mut stream: WebSocketStream<TokioIo<hyper::upgrade::Upgraded>>,
    ) -> HandlerResult<()> {
        let parts: Vec<&str> = req
            .uri()
            .path()
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();
        if parts.len() != 2 || parts[1] != "ws" {
            return Err(ServerError::Config("Invalid WebSocket path".into()));
        }

        let stream_id = parts[0]
            .parse::<u64>()
            .map_err(|_| ServerError::Config("Invalid stream ID".into()))?;

        let idx = self
            .chunk_cache
            .get_stream_idx(stream_id)
            .await
            .ok_or(ServerError::Config("Stream not found".into()))?;
        let mut last = self.chunk_cache.last(idx).unwrap_or(0);

        loop {
            match tokio::time::timeout(Duration::from_millis(1), stream.next()).await {
                Ok(Some(Ok(Message::Close(_)))) => break,
                Ok(Some(Ok(Message::Ping(payload)))) => {
                    let _ = stream.send(Message::Pong(payload)).await;
                }
                Ok(Some(Ok(_))) => {}
                Ok(Some(Err(e))) => return Err(ServerError::Handler(Box::new(e))),
                Ok(None) => break,
                Err(_) => {}
            }

            if let Some((data, _)) = self.chunk_cache.get(idx, last).await {
                if let Err(e) = stream.send(Message::Binary(data)).await {
                    return Err(ServerError::Handler(Box::new(e)));
                }
                last += 1;
            } else {
                sleep(Duration::from_millis(5)).await;
            }
        }

        Ok(())
    }

    fn can_handle(&self, path: &str) -> bool {
        let parts: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        parts.len() == 2 && parts[1] == "ws"
    }
}
