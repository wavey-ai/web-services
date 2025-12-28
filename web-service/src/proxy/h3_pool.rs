use bytes::Bytes;
use futures_util::future;
use h3::client::SendRequest;
use h3_quinn::OpenStreams;
use quinn::Endpoint;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{Mutex, Notify};
use tracing::debug;

pub(crate) type H3SendRequest = SendRequest<OpenStreams, Bytes>;

#[derive(Clone, Hash, Eq, PartialEq)]
struct H3BackendKey {
    host: String,
    port: u16,
}

impl H3BackendKey {
    fn new(host: &str, port: u16) -> Self {
        Self {
            host: host.to_ascii_lowercase(),
            port,
        }
    }
}

struct H3Connection {
    send_request: H3SendRequest,
}

enum H3PoolEntry {
    Ready(H3Connection),
    Connecting(Arc<Notify>),
}

pub(crate) struct H3ConnectionPool {
    endpoint: Endpoint,
    entries: Mutex<HashMap<H3BackendKey, H3PoolEntry>>,
}

impl H3ConnectionPool {
    pub(crate) fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            entries: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) async fn get_or_connect(
        &self,
        host: &str,
        port: u16,
    ) -> Result<H3SendRequest, String> {
        let key = H3BackendKey::new(host, port);
        loop {
            enum Action {
                Ready(H3SendRequest),
                Wait(Arc<Notify>),
                Connect(Arc<Notify>),
            }

            let action = {
                let mut entries = self.entries.lock().await;
                match entries.get(&key) {
                    Some(H3PoolEntry::Ready(conn)) => Action::Ready(conn.send_request.clone()),
                    Some(H3PoolEntry::Connecting(notify)) => Action::Wait(Arc::clone(notify)),
                    None => {
                        let notify = Arc::new(Notify::new());
                        entries.insert(key.clone(), H3PoolEntry::Connecting(Arc::clone(&notify)));
                        Action::Connect(notify)
                    }
                }
            };

            match action {
                Action::Ready(send_request) => return Ok(send_request),
                Action::Wait(notify) => {
                    notify.notified().await;
                }
                Action::Connect(notify) => {
                    let connection = match self.connect(host, port).await {
                        Ok(connection) => connection,
                        Err(err) => {
                            let mut entries = self.entries.lock().await;
                            entries.remove(&key);
                            notify.notify_waiters();
                            return Err(err);
                        }
                    };
                    let send_request = connection.send_request.clone();
                    let mut entries = self.entries.lock().await;
                    entries.insert(key, H3PoolEntry::Ready(connection));
                    notify.notify_waiters();
                    return Ok(send_request);
                }
            }
        }
    }

    pub(crate) async fn invalidate(&self, host: &str, port: u16) {
        let key = H3BackendKey::new(host, port);
        let mut entries = self.entries.lock().await;
        if let Some(entry) = entries.remove(&key) {
            if let H3PoolEntry::Connecting(notify) = entry {
                notify.notify_waiters();
            }
            debug!(host, port, "http3 upstream connection invalidated");
        }
    }

    async fn connect(&self, host: &str, port: u16) -> Result<H3Connection, String> {
        let addrs = tokio::net::lookup_host((host, port))
            .await
            .map_err(|err| format!("upstream resolve error: {err}"))?
            .collect::<Vec<SocketAddr>>();
        let wants_v4 = self
            .endpoint
            .local_addr()
            .ok()
            .map(|addr| addr.is_ipv4())
            .unwrap_or(true);
        let addr = if wants_v4 {
            addrs.iter().find(|addr| addr.is_ipv4()).copied()
        } else {
            addrs.iter().find(|addr| addr.is_ipv6()).copied()
        }
        .or_else(|| addrs.first().copied())
        .ok_or_else(|| "upstream resolve error: no compatible addresses found".to_string())?;

        let connecting = self
            .endpoint
            .connect(addr, host)
            .map_err(|err| format!("upstream connect error: {err}"))?;
        let conn = connecting
            .await
            .map_err(|err| format!("upstream connect error: {err}"))?;

        let quinn_conn = h3_quinn::Connection::new(conn);
        let (mut driver, send_request) = h3::client::new(quinn_conn)
            .await
            .map_err(|err| format!("upstream error: {err}"))?;

        tokio::spawn(async move {
            let _ = future::poll_fn(|cx| driver.poll_close(cx)).await;
        });

        Ok(H3Connection { send_request })
    }
}
