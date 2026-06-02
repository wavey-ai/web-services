use crate::{
    config::ServerConfig,
    error::{ServerError, ServerResult},
    traits::{RawStream, RawTcpHandler},
};
use bytes::Bytes;
use std::io;
use std::{net::SocketAddr, sync::Arc};
use tls_helpers::tls_acceptor_from_base64;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tracing::{error, info};

type DynStream = Box<dyn RawStream>;

pub async fn read_length_prefixed_frame<R>(
    stream: &mut R,
    max_frame_bytes: usize,
) -> ServerResult<Option<Bytes>>
where
    R: AsyncRead + Unpin + ?Sized,
{
    let mut len_buf = [0u8; 4];
    let mut read = 0usize;
    while read < len_buf.len() {
        match stream.read(&mut len_buf[read..]).await {
            Ok(0) if read == 0 => return Ok(None),
            Ok(0) => {
                return Err(ServerError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated raw TCP frame length",
                )));
            }
            Ok(n) => read += n,
            Err(error) => return Err(ServerError::Io(error)),
        }
    }

    let frame_len = u32::from_be_bytes(len_buf) as usize;
    if frame_len == 0 {
        return Err(ServerError::Config(
            "raw TCP frames must not be empty".into(),
        ));
    }
    if frame_len > max_frame_bytes {
        return Err(ServerError::Config(format!(
            "raw TCP frame too large: {frame_len} bytes exceeds {max_frame_bytes}"
        )));
    }

    let mut frame = vec![0u8; frame_len];
    let mut read = 0usize;
    while read < frame.len() {
        match stream.read(&mut frame[read..]).await {
            Ok(0) => {
                return Err(ServerError::Io(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated raw TCP frame body",
                )));
            }
            Ok(n) => read += n,
            Err(error) => return Err(ServerError::Io(error)),
        }
    }

    Ok(Some(Bytes::from(frame)))
}

pub async fn write_length_prefixed_frame<W>(stream: &mut W, payload: &[u8]) -> ServerResult<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    let response_len = u32::try_from(payload.len())
        .map_err(|_| ServerError::Config("raw TCP response too large for u32 frame".into()))?;
    stream
        .write_all(&response_len.to_be_bytes())
        .await
        .map_err(ServerError::Io)?;
    stream.write_all(payload).await.map_err(ServerError::Io)?;
    stream.flush().await.map_err(ServerError::Io)
}

pub struct RawTcpServer {
    config: ServerConfig,
    handler: Arc<dyn RawTcpHandler>,
}

impl RawTcpServer {
    pub fn new(config: ServerConfig, handler: Arc<dyn RawTcpHandler>) -> Self {
        Self { config, handler }
    }

    pub async fn start(&self, mut shutdown_rx: watch::Receiver<()>) -> ServerResult<()> {
        let addr = SocketAddr::from(([0, 0, 0, 0], self.config.raw_tcp_port));
        let listener = TcpListener::bind(addr)
            .await
            .map_err(crate::error::ServerError::Io)?;
        info!(
            "Raw TCP server listening at {} (tls={})",
            addr, self.config.raw_tcp_tls
        );

        let tls_acceptor = if self.config.raw_tcp_tls {
            Some(
                tls_acceptor_from_base64(
                    &self.config.cert_pem_base64,
                    &self.config.privkey_pem_base64,
                    true,
                    false,
                )
                .map_err(|e| crate::error::ServerError::Tls(e.to_string()))?,
            )
        } else {
            None
        };

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    info!("Raw TCP server shutting down");
                    break;
                }
                accept_res = listener.accept() => {
                    match accept_res {
                        Ok((stream, _peer)) => {
                            let handler = Arc::clone(&self.handler);
                            let tls_acceptor = tls_acceptor.clone();
                            let is_tls = self.config.raw_tcp_tls;
                            tokio::spawn(async move {
                                let boxed_stream: DynStream = if let Some(acceptor) = tls_acceptor {
                                    match acceptor.accept(stream).await {
                                        Ok(tls_stream) => Box::new(tls_stream),
                                        Err(e) => {
                                            error!("TLS accept failed: {}", e);
                                            return;
                                        }
                                    }
                                } else {
                                    Box::new(stream)
                                };

                                if let Err(e) = handler.handle_stream(boxed_stream, is_tls).await {
                                    error!("Raw TCP handler error: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            error!("Raw TCP accept failed: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{read_length_prefixed_frame, write_length_prefixed_frame};
    use crate::ServerError;
    use tokio::io::AsyncWriteExt;

    fn frame(payload: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(4 + payload.len());
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(payload);
        bytes
    }

    #[tokio::test]
    async fn reads_length_prefixed_frame_and_clean_eof() {
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&frame(b"mesh")).await.unwrap();
        drop(client);

        assert_eq!(
            read_length_prefixed_frame(&mut server, 16).await.unwrap(),
            Some(bytes::Bytes::from_static(b"mesh"))
        );
        assert_eq!(
            read_length_prefixed_frame(&mut server, 16).await.unwrap(),
            None
        );
    }

    #[tokio::test]
    async fn rejects_truncated_length_prefix() {
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&[0, 0]).await.unwrap();
        drop(client);

        let error = read_length_prefixed_frame(&mut server, 16)
            .await
            .unwrap_err();
        assert!(matches!(error, ServerError::Io(_)));
    }

    #[tokio::test]
    async fn rejects_zero_length_frame() {
        let (mut client, mut server) = tokio::io::duplex(64);
        client.write_all(&0u32.to_be_bytes()).await.unwrap();
        drop(client);

        let error = read_length_prefixed_frame(&mut server, 16)
            .await
            .unwrap_err();
        assert!(matches!(error, ServerError::Config(_)));
    }

    #[tokio::test]
    async fn writes_length_prefixed_frame() {
        let (mut client, mut server) = tokio::io::duplex(64);
        let writer = tokio::spawn(async move {
            write_length_prefixed_frame(&mut client, b"response")
                .await
                .unwrap();
        });

        assert_eq!(
            read_length_prefixed_frame(&mut server, 16).await.unwrap(),
            Some(bytes::Bytes::from_static(b"response"))
        );
        writer.await.unwrap();
    }
}
