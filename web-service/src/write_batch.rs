//! Write batching below TLS for HTTP/2 connections.
//!
//! With `TCP_NODELAY` on, every socket write leaves at once as its own segment. hyper's HTTP/2
//! connection flushes after each DATA frame that carries a payload (h2 chains the payload
//! instead of copying it, and an encoder that holds a chained frame must flush before it takes
//! the next one), and tokio-rustls writes each flush's TLS records to the socket at once. A
//! stream of four 16 KiB chunks thus costs four socket writes.
//!
//! [`WriteBatch`] sits between the TLS stream and the socket. It collects TLS records in a
//! bounded buffer and writes them out in one call: when the buffer is full, before the
//! connection reads, and when the connection shuts down or is dropped. A flush returns before
//! its bytes reach the socket and wakes the task once its current poll has returned; the next
//! call into the socket then writes the batch. hyper's HTTP/2 connection reads on every poll,
//! so the bytes go out within one more poll of the connection task and never wait for the peer.
//!
//! The listener batches HTTP/2 only. hyper writes an HTTP/1.1 message with one flush, and a
//! WebSocket session on an upgraded connection may not touch the socket again after a flush.

use std::{
    future::Future,
    io::{self, IoSlice},
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll, Wake, Waker},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

/// The most bytes a connection holds back. A loopback or TSO segment carries up to 64 KiB, so a
/// larger write saves system calls but not segments; rustls also holds at most 64 KiB of
/// records per write. Writes of this size or more go to the socket directly.
pub(crate) const BATCH_BYTES: usize = 64 * 1024;

/// A socket that takes a best-effort write without a task context, for bytes left in the
/// buffer when the connection is dropped.
pub(crate) trait TryWrite {
    fn try_write_now(&self, buf: &[u8]) -> io::Result<usize>;
}

impl TryWrite for TcpStream {
    /// `TcpStream::try_write` only writes once the runtime has seen the socket writable, which
    /// a socket that has not written yet may not have, so this writes on a duplicate of the
    /// descriptor. It shares the socket's non-blocking mode.
    #[cfg(unix)]
    fn try_write_now(&self, buf: &[u8]) -> io::Result<usize> {
        use std::io::Write;
        use std::os::fd::AsFd;
        let socket = std::net::TcpStream::from(self.as_fd().try_clone_to_owned()?);
        (&socket).write(buf)
    }

    #[cfg(not(unix))]
    fn try_write_now(&self, buf: &[u8]) -> io::Result<usize> {
        self.try_write(buf)
    }
}

/// Wakes the task once the poll that buffered bytes has returned, unless the bytes have been
/// written by then.
struct DrainWake {
    due: AtomicBool,
    written: AtomicBool,
    task: Waker,
}

impl Wake for DrainWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.due.store(true, Ordering::Release);
        if !self.written.load(Ordering::Acquire) {
            self.task.wake_by_ref();
        }
    }
}

pub(crate) struct WriteBatch<T: TryWrite> {
    io: T,
    buf: Vec<u8>,
    /// Bytes at the front of `buf` already written to the socket.
    sent: usize,
    /// Off during the TLS handshake, which flushes and then waits for the peer's reply.
    batching: bool,
    drain: Option<Arc<DrainWake>>,
}

impl<T> WriteBatch<T>
where
    T: AsyncRead + AsyncWrite + Unpin + TryWrite,
{
    /// Writes pass straight through until [`WriteBatch::start_batching`].
    pub(crate) fn new(io: T) -> Self {
        Self {
            io,
            buf: Vec::new(),
            sent: 0,
            batching: false,
            drain: None,
        }
    }

    pub(crate) fn start_batching(&mut self) {
        self.batching = true;
    }

    fn pending(&self) -> usize {
        self.buf.len() - self.sent
    }

    fn drain_due(&self) -> bool {
        self.drain
            .as_ref()
            .is_some_and(|drain| drain.due.load(Ordering::Acquire))
    }

    /// Arrange for the bytes to be written once the current poll of the task has returned.
    fn schedule_drain(&mut self, cx: &Context<'_>) {
        if self.drain.is_some() {
            return;
        }
        let drain = Arc::new(DrainWake {
            due: AtomicBool::new(false),
            written: AtomicBool::new(false),
            task: cx.waker().clone(),
        });
        let waker = Waker::from(Arc::clone(&drain));
        // The runtime defers the wake-up of a yield until the current poll has returned.
        let _ = std::pin::pin!(tokio::task::yield_now())
            .as_mut()
            .poll(&mut Context::from_waker(&waker));
        self.drain = Some(drain);
    }

    /// Write the buffered bytes, in one call when the socket takes them all.
    fn poll_drain(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.pending() > 0 {
            let n =
                std::task::ready!(Pin::new(&mut self.io).poll_write(cx, &self.buf[self.sent..]))?;
            if n == 0 {
                return Poll::Ready(Err(io::ErrorKind::WriteZero.into()));
            }
            self.sent += n;
        }
        self.buf.clear();
        self.sent = 0;
        if let Some(drain) = self.drain.take() {
            drain.written.store(true, Ordering::Release);
        }
        Poll::Ready(Ok(()))
    }

    /// Make room for `wanted` bytes. `Ready(false)` when the buffer is full and the socket
    /// takes nothing now; its waker is then registered.
    fn poll_room(&mut self, cx: &mut Context<'_>, wanted: usize) -> Poll<io::Result<bool>> {
        if self.drain_due() || self.buf.len() + wanted > BATCH_BYTES {
            if let Poll::Ready(Err(error)) = self.poll_drain(cx) {
                return Poll::Ready(Err(error));
            }
        }
        if self.sent > 0 && self.buf.len() + wanted > BATCH_BYTES {
            self.buf.drain(..self.sent);
            self.sent = 0;
        }
        Poll::Ready(Ok(self.buf.len() < BATCH_BYTES))
    }

    fn append(&mut self, data: &[u8]) -> usize {
        if self.buf.capacity() == 0 {
            self.buf.reserve_exact(BATCH_BYTES);
        }
        let n = data.len().min(BATCH_BYTES - self.buf.len());
        self.buf.extend_from_slice(&data[..n]);
        n
    }
}

impl<T> AsyncRead for WriteBatch<T>
where
    T: AsyncRead + AsyncWrite + Unpin + TryWrite,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        // The peer may be waiting for these bytes before it sends anything.
        if this.pending() > 0 {
            if let Poll::Ready(Err(error)) = this.poll_drain(cx) {
                return Poll::Ready(Err(error));
            }
        }
        Pin::new(&mut this.io).poll_read(cx, buf)
    }
}

impl<T> AsyncWrite for WriteBatch<T>
where
    T: AsyncRead + AsyncWrite + Unpin + TryWrite,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if !this.batching || (this.pending() == 0 && data.len() >= BATCH_BYTES) {
            std::task::ready!(this.poll_drain(cx))?;
            return Pin::new(&mut this.io).poll_write(cx, data);
        }
        if !std::task::ready!(this.poll_room(cx, data.len()))? {
            return Poll::Pending;
        }
        Poll::Ready(Ok(this.append(data)))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let total: usize = bufs.iter().map(|buf| buf.len()).sum();
        if !this.batching || (this.pending() == 0 && total >= BATCH_BYTES) {
            std::task::ready!(this.poll_drain(cx))?;
            return Pin::new(&mut this.io).poll_write_vectored(cx, bufs);
        }
        if !std::task::ready!(this.poll_room(cx, total))? {
            return Poll::Pending;
        }
        let mut written = 0;
        for buf in bufs {
            let n = this.append(buf);
            written += n;
            if n < buf.len() {
                break;
            }
        }
        Poll::Ready(Ok(written))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if this.pending() > 0 {
            if this.batching && !this.drain_due() {
                this.schedule_drain(cx);
                return Poll::Ready(Ok(()));
            }
            std::task::ready!(this.poll_drain(cx))?;
        }
        Pin::new(&mut this.io).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        std::task::ready!(this.poll_drain(cx))?;
        Pin::new(&mut this.io).poll_shutdown(cx)
    }
}

impl<T: TryWrite> Drop for WriteBatch<T> {
    fn drop(&mut self) {
        // A connection dropped without a shutdown (aborted when a drain window ends) still
        // sends what the socket takes now.
        while self.sent < self.buf.len() {
            match self.io.try_write_now(&self.buf[self.sent..]) {
                Ok(0) | Err(_) => break,
                Ok(n) => self.sent += n,
            }
        }
        if let Some(drain) = self.drain.take() {
            drain.written.store(true, Ordering::Release);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use http::{Request, Response};
    use http_body_util::{BodyExt, Empty, StreamBody};
    use hyper::body::{Frame, Incoming};
    use hyper_util::rt::{TokioExecutor, TokioIo};
    use std::convert::Infallible;
    use std::sync::atomic::AtomicUsize;
    use tokio::net::TcpListener;
    use tokio_rustls::rustls::{
        self,
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
    };

    const CHUNK: usize = 16 * 1024;
    const CHUNKS: usize = 4;

    /// Counts the writes that reach the socket.
    struct Counted {
        io: TcpStream,
        writes: Arc<AtomicUsize>,
    }

    impl TryWrite for Counted {
        fn try_write_now(&self, buf: &[u8]) -> io::Result<usize> {
            self.io.try_write_now(buf)
        }
    }

    impl Counted {
        fn count(&self, polled: &Poll<io::Result<usize>>) {
            if matches!(polled, Poll::Ready(Ok(n)) if *n > 0) {
                self.writes.fetch_add(1, Ordering::SeqCst);
            }
        }
    }

    impl AsyncRead for Counted {
        fn poll_read(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            Pin::new(&mut self.io).poll_read(cx, buf)
        }
    }

    impl AsyncWrite for Counted {
        fn poll_write(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            let polled = Pin::new(&mut self.io).poll_write(cx, buf);
            self.count(&polled);
            polled
        }

        fn poll_write_vectored(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let polled = Pin::new(&mut self.io).poll_write_vectored(cx, bufs);
            self.count(&polled);
            polled
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.io).poll_flush(cx)
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Pin::new(&mut self.io).poll_shutdown(cx)
        }
    }

    fn tls_configs(alpn: &[u8]) -> (rustls::ServerConfig, rustls::ClientConfig) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let rcgen::CertifiedKey { cert, key_pair } =
            rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let mut server = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(
                vec![cert.der().clone()],
                PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key_pair.serialize_der())),
            )
            .unwrap();
        server.alpn_protocols = vec![alpn.to_vec()];
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert.der().clone()).unwrap();
        let mut client = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        client.alpn_protocols = vec![alpn.to_vec()];
        (server, client)
    }

    fn chunked_body(
        chunks: usize,
        size: usize,
    ) -> http_body_util::combinators::UnsyncBoxBody<Bytes, Infallible> {
        StreamBody::new(futures_util::stream::iter((0..chunks).map(move |index| {
            Ok(Frame::data(Bytes::from(vec![index as u8; size])))
        })))
        .boxed_unsync()
    }

    /// Serve `streams` requests of four 16 KiB chunks on one TLS connection and return the
    /// socket writes the server made after its handshake.
    async fn server_writes(http2: bool, batching: bool, streams: usize) -> usize {
        server_writes_of(http2, batching, streams, CHUNKS, CHUNK).await
    }

    async fn server_writes_of(
        http2: bool,
        batching: bool,
        streams: usize,
        chunks: usize,
        size: usize,
    ) -> usize {
        let (server_config, client_config) = tls_configs(if http2 { b"h2" } else { b"http/1.1" });
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let writes = Arc::new(AtomicUsize::new(0));
        let server = tokio::spawn({
            let writes = Arc::clone(&writes);
            async move {
                let (tcp, _) = listener.accept().await.unwrap();
                tcp.set_nodelay(true).unwrap();
                let io = WriteBatch::new(Counted {
                    io: tcp,
                    writes: Arc::clone(&writes),
                });
                let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
                let mut tls = acceptor.accept(io).await.unwrap();
                if batching {
                    tls.get_mut().0.start_batching();
                }
                writes.store(0, Ordering::SeqCst);
                let service =
                    hyper::service::service_fn(move |_req: Request<Incoming>| async move {
                        Ok::<_, Infallible>(Response::new(chunked_body(chunks, size)))
                    });
                if http2 {
                    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                        .serve_connection(TokioIo::new(tls), service)
                        .await;
                } else {
                    let _ = hyper::server::conn::http1::Builder::new()
                        .serve_connection(TokioIo::new(tls), service)
                        .await;
                }
            }
        });

        let tcp = TcpStream::connect(addr).await.unwrap();
        let tls = tokio_rustls::TlsConnector::from(Arc::new(client_config))
            .connect(ServerName::try_from("localhost").unwrap(), tcp)
            .await
            .unwrap();
        let io = TokioIo::new(tls);
        let mut bodies = Vec::new();
        if http2 {
            let (sender, connection) = hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
                TokioExecutor::new(),
                io,
            )
            .await
            .unwrap();
            tokio::spawn(connection);
            // Let the server settle its SETTINGS exchange before counting starts.
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            writes.store(0, Ordering::SeqCst);
            let mut requests = Vec::new();
            for _ in 0..streams {
                let mut sender = sender.clone();
                requests.push(tokio::spawn(async move {
                    sender.ready().await.unwrap();
                    let response = sender
                        .send_request(
                            Request::get("https://localhost/")
                                .body(Empty::new())
                                .unwrap(),
                        )
                        .await
                        .unwrap();
                    response.into_body().collect().await.unwrap().to_bytes()
                }));
            }
            for request in requests {
                bodies.push(request.await.unwrap());
            }
        } else {
            let (mut sender, connection) = hyper::client::conn::http1::handshake(io).await.unwrap();
            tokio::spawn(connection);
            for _ in 0..streams {
                sender.ready().await.unwrap();
                let response = sender
                    .send_request(
                        Request::get("https://localhost/")
                            .body(Empty::<Bytes>::new())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                bodies.push(response.into_body().collect().await.unwrap().to_bytes());
            }
        }
        for body in &bodies {
            assert_eq!(body.len(), chunks * size);
        }
        let counted = writes.load(Ordering::SeqCst);
        server.abort();
        counted
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http2_frames_written_in_one_poll_share_a_socket_write() {
        for streams in [1, 16, 64] {
            let unbatched = server_writes(true, false, streams).await;
            let batched = server_writes(true, true, streams).await;
            // Unbatched, each 16 KiB DATA frame is its own write. Batched, a stream's 64 KiB
            // (just over one batch with its framing) takes one or two.
            assert!(
                unbatched >= CHUNKS * streams,
                "{streams} streams: {unbatched} unbatched writes"
            );
            assert!(
                batched <= 2 * streams && batched * 2 <= unbatched,
                "{streams} streams: {batched} batched writes against {unbatched} unbatched"
            );
        }
        // A small response: HEADERS and DATA went out as two writes, and now as one.
        assert_eq!(server_writes_of(true, false, 1, 1, 1024).await, 2);
        assert_eq!(server_writes_of(true, true, 1, 1, 1024).await, 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn http1_writes_one_flush_per_response() {
        // hyper buffers an HTTP/1.1 response and flushes it once; rustls takes up to 64 KiB of
        // it per write, so a 64 KiB body and its head take two.
        for streams in [1, 16] {
            assert!(server_writes(false, false, streams).await <= 2 * streams);
        }
    }

    async fn connected_pair() -> (WriteBatch<TcpStream>, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let client = TcpStream::connect(listener.local_addr().unwrap());
        let (accepted, client) = tokio::join!(listener.accept(), client);
        let server = accepted.unwrap().0;
        server.set_nodelay(true).unwrap();
        let mut batch = WriteBatch::new(server);
        batch.start_batching();
        (batch, client.unwrap())
    }

    async fn read_all(mut client: TcpStream) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let mut received = Vec::new();
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client.read_to_end(&mut received),
        )
        .await
        .expect("the bytes did not arrive")
        .unwrap();
        received
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_handshake_completes_with_batching_on_from_the_start() {
        let (server_config, client_config) = tls_configs(b"h2");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut io = WriteBatch::new(tcp);
            io.start_batching();
            let tls = tokio_rustls::TlsAcceptor::from(Arc::new(server_config))
                .accept(io)
                .await
                .unwrap();
            let service = hyper::service::service_fn(|_req: Request<Incoming>| async {
                Ok::<_, Infallible>(Response::new(chunked_body(CHUNKS, CHUNK)))
            });
            let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(tls), service)
                .await;
        });
        let tcp = TcpStream::connect(addr).await.unwrap();
        let tls = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio_rustls::TlsConnector::from(Arc::new(client_config))
                .connect(ServerName::try_from("localhost").unwrap(), tcp),
        )
        .await
        .expect("the handshake stalled")
        .unwrap();
        let (mut sender, connection) = hyper::client::conn::http2::handshake::<_, _, Empty<Bytes>>(
            TokioExecutor::new(),
            TokioIo::new(tls),
        )
        .await
        .unwrap();
        tokio::spawn(connection);
        sender.ready().await.unwrap();
        let response = sender
            .send_request(
                Request::get("https://localhost/")
                    .body(Empty::new())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(body.len(), CHUNK * CHUNKS);
        server.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_buffer_holds_the_writer_back() {
        use tokio::io::AsyncWriteExt;
        let (mut batch, client) = connected_pair().await;
        let chunk = vec![7u8; 16 * 1024];
        // The client reads nothing: the kernel buffers fill, then the batch, then writes wait.
        let mut accepted = 0usize;
        loop {
            let wrote = futures_util::future::poll_fn(|cx| {
                Poll::Ready(Pin::new(&mut batch).poll_write(cx, &chunk))
            })
            .await;
            match wrote {
                Poll::Ready(Ok(n)) => accepted += n,
                Poll::Ready(Err(error)) => panic!("{error}"),
                Poll::Pending => break,
            }
            assert!(accepted < 64 * 1024 * 1024, "the writes never waited");
        }
        assert!(batch.pending() <= BATCH_BYTES);
        assert!(batch.buf.capacity() <= BATCH_BYTES);

        let reader = tokio::spawn(read_all(client));
        let total = accepted + 8 * 1024 * 1024;
        let mut written = accepted;
        while written < total {
            batch.write_all(&chunk).await.unwrap();
            written += chunk.len();
        }
        batch.flush().await.unwrap();
        batch.shutdown().await.unwrap();
        drop(batch);
        let received = reader.await.unwrap();
        assert_eq!(received.len(), written);
        assert!(received.iter().all(|byte| *byte == 7));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn shutdown_and_drop_send_buffered_bytes() {
        use tokio::io::AsyncWriteExt;
        let (mut batch, client) = connected_pair().await;
        batch.write_all(b"before shutdown").await.unwrap();
        batch.flush().await.unwrap();
        batch.shutdown().await.unwrap();
        assert_eq!(read_all(client).await, b"before shutdown");

        let (mut batch, client) = connected_pair().await;
        batch.write_all(b"before drop").await.unwrap();
        batch.flush().await.unwrap();
        assert_eq!(batch.pending(), b"before drop".len());
        drop(batch);
        assert_eq!(read_all(client).await, b"before drop");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_flushed_batch_goes_out_once_the_poll_returns() {
        use tokio::io::AsyncWriteExt;
        let (mut batch, mut client) = connected_pair().await;
        let writer = tokio::spawn(async move {
            batch.write_all(b"flushed").await.unwrap();
            batch.flush().await.unwrap();
            // The task now waits without reading or writing; the first poll after the flush
            // has returned finds the batch due, as hyper's next flush would.
            let mut polls = 0;
            futures_util::future::poll_fn(|cx| {
                polls += 1;
                if batch.pending() > 0 {
                    if polls > 1 {
                        let _ = Pin::new(&mut batch).poll_flush(cx);
                    }
                    if batch.pending() > 0 {
                        return Poll::Pending;
                    }
                }
                Poll::Ready(())
            })
            .await;
            batch
        });
        let mut received = [0u8; 7];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            tokio::io::AsyncReadExt::read_exact(&mut client, &mut received),
        )
        .await
        .expect("the flushed bytes did not go out")
        .unwrap();
        assert_eq!(&received, b"flushed");
        let _batch = writer.await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_sends_the_batch_first() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (mut batch, mut client) = connected_pair().await;
        batch.write_all(b"ping").await.unwrap();
        batch.flush().await.unwrap();
        let echo = tokio::spawn(async move {
            let mut ping = [0u8; 4];
            client.read_exact(&mut ping).await.unwrap();
            client.write_all(b"pong").await.unwrap();
            client
        });
        let mut pong = [0u8; 4];
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            batch.read_exact(&mut pong),
        )
        .await
        .expect("the read waited on unsent bytes")
        .unwrap();
        assert_eq!(&pong, b"pong");
        let _client = echo.await.unwrap();
    }
}
