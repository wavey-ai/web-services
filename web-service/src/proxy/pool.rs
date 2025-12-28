use super::backend::{Backend, BackendView};
use super::balancer::LoadBalancingMode;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{Mutex, Notify};
use tokio::time::Instant;
use tracing::{debug, debug_span, Instrument};

pub struct BackendPool {
    inner: Mutex<PoolState>,
    notify: Notify,
    max_queue: Option<usize>,
    waiters: AtomicUsize,
}

struct PoolState {
    backends: Vec<BackendState>,
    rr_cursor: usize,
}

struct BackendState {
    backend: Backend,
    active: usize,
}

pub struct BackendLease {
    pool: Arc<BackendPool>,
    backend: Backend,
    backend_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    NoAvailable,
    QueueFull,
}

struct QueueWaiter {
    pool: Arc<BackendPool>,
}

impl BackendPool {
    pub fn new(max_queue: Option<usize>) -> Self {
        Self {
            inner: Mutex::new(PoolState {
                backends: Vec::new(),
                rr_cursor: 0,
            }),
            notify: Notify::new(),
            max_queue,
            waiters: AtomicUsize::new(0),
        }
    }

    pub async fn add_backend(&self, backend: Backend) -> BackendView {
        let view = {
            let mut state = self.inner.lock().await;
            state.backends.push(BackendState { backend, active: 0 });
            state
                .backends
                .last()
                .expect("backend just inserted")
                .view()
        };
        debug!(
            backend_id = %view.id,
            backend_url = %view.url,
            "backend added"
        );
        self.notify.notify_waiters();
        view
    }

    pub async fn remove_backend(&self, id: &str) -> Option<BackendView> {
        let removed = {
            let mut state = self.inner.lock().await;
            let pos = state.backends.iter().position(|b| b.backend.id == id)?;
            let removed = state.backends.remove(pos);
            if state.rr_cursor >= state.backends.len() {
                state.rr_cursor = 0;
            }
            removed
        };
        let view = removed.view();
        debug!(
            backend_id = %view.id,
            backend_url = %view.url,
            "backend removed"
        );
        self.notify.notify_waiters();
        Some(view)
    }

    pub async fn list_backends(&self) -> Vec<BackendView> {
        let state = self.inner.lock().await;
        state.backends.iter().map(BackendState::view).collect()
    }

    pub async fn acquire(
        self: &Arc<Self>,
        mode: LoadBalancingMode,
    ) -> Result<BackendLease, AcquireError> {
        match mode {
            LoadBalancingMode::LeastConn => self.try_acquire(mode).await,
            LoadBalancingMode::Queue => self.acquire_queue().await,
        }
    }

    async fn release(&self, id: &str) {
        let mut state = self.inner.lock().await;
        if let Some(backend) = state.backends.iter_mut().find(|b| b.backend.id == id) {
            backend.active = backend.active.saturating_sub(1);
            debug!(
                backend_id = %backend.backend.id,
                active_connections = backend.active,
                "backend released"
            );
        }
        drop(state);
        self.notify.notify_waiters();
    }

    async fn try_acquire(
        self: &Arc<Self>,
        mode: LoadBalancingMode,
    ) -> Result<BackendLease, AcquireError> {
        let mut state = self.inner.lock().await;
        if state.backends.is_empty() {
            return Err(AcquireError::NoAvailable);
        }
        let idx = match mode {
            LoadBalancingMode::LeastConn => select_least_conn(&state.backends),
            LoadBalancingMode::Queue => select_queue(&mut state),
        }
        .ok_or(AcquireError::NoAvailable)?;
        Ok(allocate_backend(self, &mut state, idx))
    }

    async fn acquire_queue(self: &Arc<Self>) -> Result<BackendLease, AcquireError> {
        let mut wait_start: Option<Instant> = None;
        let mut waiter: Option<QueueWaiter> = None;
        loop {
            let notified = {
                let mut state = self.inner.lock().await;
                if state.backends.is_empty() {
                    debug!("queue acquire: no backends configured");
                    return Err(AcquireError::NoAvailable);
                }
                if let Some(idx) = select_queue(&mut state) {
                    if let Some(start) = wait_start.take() {
                        debug!(
                            queue_wait_ms = start.elapsed().as_millis(),
                            "queue wait complete"
                        );
                    }
                    return Ok(allocate_backend(self, &mut state, idx));
                }
                if wait_start.is_none() {
                    wait_start = Some(Instant::now());
                }
                if waiter.is_none() {
                    waiter = Some(self.reserve_queue_slot()?);
                }
                debug!("queue acquire: waiting for available backend");
                self.notify.notified()
            };
            notified
                .instrument(debug_span!("queue_wait"))
                .await;
        }
    }

    fn reserve_queue_slot(self: &Arc<Self>) -> Result<QueueWaiter, AcquireError> {
        if let Some(max) = self.max_queue {
            let current = self.waiters.fetch_add(1, Ordering::SeqCst) + 1;
            if current > max {
                self.waiters.fetch_sub(1, Ordering::SeqCst);
                debug!(
                    max_queue = max,
                    queue_waiters = current.saturating_sub(1),
                    "queue full"
                );
                return Err(AcquireError::QueueFull);
            }
        } else {
            self.waiters.fetch_add(1, Ordering::SeqCst);
        }
        Ok(QueueWaiter {
            pool: Arc::clone(self),
        })
    }
}

impl BackendLease {
    pub fn backend(&self) -> &Backend {
        &self.backend
    }
}

impl Drop for BackendLease {
    fn drop(&mut self) {
        let pool = Arc::clone(&self.pool);
        let backend_id = self.backend_id.clone();
        tokio::spawn(async move {
            pool.release(&backend_id).await;
        });
    }
}

impl Drop for QueueWaiter {
    fn drop(&mut self) {
        self.pool.waiters.fetch_sub(1, Ordering::SeqCst);
    }
}

impl BackendState {
    fn view(&self) -> BackendView {
        BackendView {
            id: self.backend.id.clone(),
            url: self.backend.url.clone(),
            scheme: self.backend.scheme,
            max_connections: self.backend.max_connections,
            active_connections: self.active,
        }
    }

    fn is_available(&self) -> bool {
        match self.backend.max_connections {
            Some(max) => self.active < max,
            None => true,
        }
    }
}

fn select_least_conn(backends: &[BackendState]) -> Option<usize> {
    let mut selected: Option<usize> = None;
    for (idx, backend) in backends.iter().enumerate() {
        if !backend.is_available() {
            continue;
        }
        match selected {
            None => selected = Some(idx),
            Some(best) => {
                if backend.active < backends[best].active {
                    selected = Some(idx);
                }
            }
        }
    }
    selected
}

fn select_queue(state: &mut PoolState) -> Option<usize> {
    let total = state.backends.len();
    if total == 0 {
        return None;
    }
    for offset in 0..total {
        let idx = (state.rr_cursor + offset) % total;
        if state.backends[idx].is_available() {
            state.rr_cursor = (idx + 1) % total;
            return Some(idx);
        }
    }
    None
}

fn allocate_backend(
    pool: &Arc<BackendPool>,
    state: &mut PoolState,
    idx: usize,
) -> BackendLease {
    let backend_state = state
        .backends
        .get_mut(idx)
        .expect("backend index exists");
    backend_state.active = backend_state.active.saturating_add(1);
    let backend = backend_state.backend.clone();
    let backend_id = backend.id.clone();
    debug!(
        backend_id = %backend.id,
        backend_url = %backend.url,
        active_connections = backend_state.active,
        "backend leased"
    );

    BackendLease {
        pool: Arc::clone(pool),
        backend,
        backend_id,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::backend::BackendScheme;
    use std::sync::Arc;
    use tokio::time::{timeout, Duration};
    use url::Url;

    fn backend(id: &str, max_connections: Option<usize>) -> Backend {
        Backend {
            id: id.to_string(),
            url: Url::parse(&format!("https://{id}.example.com")).unwrap(),
            scheme: BackendScheme::Https,
            max_connections,
        }
    }

    #[test]
    fn select_least_conn_skips_unavailable() {
        let backends = vec![
            BackendState {
                backend: backend("full", Some(1)),
                active: 1,
            },
            BackendState {
                backend: backend("idle", None),
                active: 3,
            },
        ];
        assert_eq!(select_least_conn(&backends), Some(1));

        let backends = vec![BackendState {
            backend: backend("only", Some(1)),
            active: 1,
        }];
        assert_eq!(select_least_conn(&backends), None);
    }

    #[test]
    fn select_queue_round_robin_advances_cursor() {
        let mut state = PoolState {
            backends: vec![
                BackendState {
                    backend: backend("a", None),
                    active: 0,
                },
                BackendState {
                    backend: backend("b", None),
                    active: 0,
                },
                BackendState {
                    backend: backend("c", None),
                    active: 0,
                },
            ],
            rr_cursor: 0,
        };

        assert_eq!(select_queue(&mut state), Some(0));
        assert_eq!(state.rr_cursor, 1);
        assert_eq!(select_queue(&mut state), Some(1));
        assert_eq!(state.rr_cursor, 2);
        assert_eq!(select_queue(&mut state), Some(2));
        assert_eq!(state.rr_cursor, 0);
    }

    #[tokio::test]
    async fn acquire_queue_waits_until_release() {
        let pool = Arc::new(BackendPool::new(None));
        pool.add_backend(backend("solo", Some(1))).await;

        let lease = pool.acquire(LoadBalancingMode::Queue).await.unwrap();

        let pool_clone = Arc::clone(&pool);
        let pending = tokio::spawn(async move {
            timeout(Duration::from_millis(500), pool_clone.acquire(LoadBalancingMode::Queue)).await
        });

        tokio::time::sleep(Duration::from_millis(50)).await;
        drop(lease);

        let acquired = pending.await.unwrap().unwrap().unwrap();
        assert_eq!(acquired.backend().id, "solo");
    }

    #[tokio::test]
    async fn acquire_queue_respects_max_queue() {
        let pool = Arc::new(BackendPool::new(Some(0)));
        pool.add_backend(backend("solo", Some(1))).await;

        let lease = pool.acquire(LoadBalancingMode::Queue).await.unwrap();
        let rejected = pool.acquire(LoadBalancingMode::Queue).await;
        assert!(matches!(rejected, Err(AcquireError::QueueFull)));

        drop(lease);
    }

    #[tokio::test]
    async fn remove_backend_resets_rr_cursor_when_out_of_range() {
        let pool = Arc::new(BackendPool::new(None));
        pool.add_backend(backend("a", None)).await;
        pool.add_backend(backend("b", None)).await;

        let lease = pool.acquire(LoadBalancingMode::Queue).await.unwrap();
        drop(lease);

        let removed = pool.remove_backend("b").await.unwrap();
        assert_eq!(removed.id, "b");

        let state = pool.inner.lock().await;
        assert_eq!(state.rr_cursor, 0);
    }
}
