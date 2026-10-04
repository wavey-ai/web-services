use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::Notify;
use tokio::time::{timeout_at, Duration, Instant};
use tracing::{debug, warn};

/// Exact nonblocking limit for short-lived request work.
///
/// Alignment keeps the hot count away from the adjacent `Arc` reference counts.
#[repr(align(64))]
#[derive(Debug)]
pub(crate) struct RequestLimiter {
    active: AtomicUsize,
    maximum: usize,
    /// Signalled whenever a permit is returned, for callers waiting for one.
    released: Notify,
}

impl RequestLimiter {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            maximum: maximum.max(1),
            released: Notify::new(),
        }
    }

    pub(crate) fn try_acquire_owned(self: Arc<Self>) -> Result<RequestPermit, Arc<Self>> {
        let previous = self.active.fetch_add(1, Ordering::Relaxed);
        if previous < self.maximum {
            Ok(RequestPermit {
                limiter: Some(self),
            })
        } else {
            self.active.fetch_sub(1, Ordering::Relaxed);
            Err(self)
        }
    }

    /// Wait up to `wait` for a permit. The wake-up is armed before each check, so a permit
    /// returned between the check and the wait is never missed.
    pub(crate) async fn acquire_within(
        self: Arc<Self>,
        wait: Duration,
    ) -> Result<RequestPermit, Arc<Self>> {
        let deadline = Instant::now() + wait;
        loop {
            let released = self.released.notified();
            tokio::pin!(released);
            released.as_mut().enable();
            let limiter = match Arc::clone(&self).try_acquire_owned() {
                Ok(permit) => return Ok(permit),
                Err(limiter) => limiter,
            };
            if timeout_at(deadline, released).await.is_err() {
                return Err(limiter);
            }
        }
    }

    #[cfg(test)]
    fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
}

/// How a request on the HTTP/1.1+HTTP/2 listener gets a slot from the shared [`RequestLimiter`]:
/// at once, after waiting up to the queue timeout, or not at all (the caller answers 503).
/// Exempt paths (health checks) take no slot, so a busy server is not taken for a dead one.
#[derive(Clone)]
pub(crate) struct Admission {
    limiter: Arc<RequestLimiter>,
    queue_timeout: Duration,
    exempt_paths: Arc<[String]>,
}

impl Admission {
    pub(crate) fn new(
        limiter: Arc<RequestLimiter>,
        queue_timeout: Duration,
        exempt_paths: &[String],
    ) -> Self {
        Self {
            limiter,
            queue_timeout,
            exempt_paths: exempt_paths.into(),
        }
    }

    pub(crate) async fn admit(&self, path: &str) -> Option<RequestPermit> {
        if self.exempt_paths.iter().any(|exempt| exempt == path) {
            return Some(RequestPermit::unlimited());
        }
        let limiter = match Arc::clone(&self.limiter).try_acquire_owned() {
            Ok(permit) => return Some(permit),
            Err(limiter) => limiter,
        };
        let limit = limiter.maximum;
        if self.queue_timeout.is_zero() {
            debug!(limit, "request limit reached; request refused");
            return None;
        }
        debug!(limit, "request limit reached; waiting for a slot");
        let started = Instant::now();
        match limiter.acquire_within(self.queue_timeout).await {
            Ok(permit) => Some(permit),
            Err(_) => {
                warn!(
                    limit,
                    waited_ms = started.elapsed().as_millis() as u64,
                    "server busy; request refused"
                );
                None
            }
        }
    }
}

pub(crate) struct RequestPermit {
    /// `None` for a request on a path that skips the limit.
    limiter: Option<Arc<RequestLimiter>>,
}

impl RequestPermit {
    /// A permit that holds no slot, for paths that skip the limit (health checks).
    pub(crate) fn unlimited() -> Self {
        Self { limiter: None }
    }
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        let Some(limiter) = self.limiter.take() else {
            return;
        };
        let previous = limiter.active.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "request permit count underflowed");
        limiter.released.notify_one();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn limits_and_releases_request_work() {
        let limiter = Arc::new(RequestLimiter::new(2));
        let first = Arc::clone(&limiter).try_acquire_owned().unwrap();
        let second = Arc::clone(&limiter).try_acquire_owned().unwrap();

        assert!(Arc::clone(&limiter).try_acquire_owned().is_err());
        assert_eq!(limiter.active(), 2);

        drop(first);
        let replacement = Arc::clone(&limiter).try_acquire_owned().unwrap();
        assert_eq!(limiter.active(), 2);

        drop((second, replacement));
        assert_eq!(limiter.active(), 0);
    }

    #[tokio::test]
    async fn a_waiting_request_takes_the_next_released_permit_or_times_out() {
        let limiter = Arc::new(RequestLimiter::new(1));
        let held = Arc::clone(&limiter).try_acquire_owned().unwrap();
        assert!(Arc::clone(&limiter)
            .acquire_within(Duration::from_millis(20))
            .await
            .is_err());

        let waiter = tokio::spawn({
            let limiter = Arc::clone(&limiter);
            async move { limiter.acquire_within(Duration::from_secs(5)).await.is_ok() }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
        assert!(waiter.await.unwrap());
        assert_eq!(limiter.active(), 0);
        drop(RequestPermit::unlimited());
        assert_eq!(limiter.active(), 0);
    }

    #[tokio::test]
    async fn exempt_paths_skip_a_full_limit_and_others_wait_their_turn() {
        let limiter = Arc::new(RequestLimiter::new(1));
        let admission = Admission::new(
            Arc::clone(&limiter),
            Duration::from_millis(20),
            &["/health".to_owned()],
        );
        let held = admission.admit("/work").await.unwrap();
        assert!(admission.admit("/work").await.is_none());
        assert!(admission.admit("/health").await.is_some());
        assert!(admission.admit("/health/deep").await.is_none());
        drop(held);
        assert!(admission.admit("/work").await.is_some());

        let refuse_at_once = Admission::new(Arc::clone(&limiter), Duration::ZERO, &[]);
        let _held = refuse_at_once.admit("/work").await.unwrap();
        let started = Instant::now();
        assert!(refuse_at_once.admit("/work").await.is_none());
        assert!(started.elapsed() < Duration::from_millis(20));
    }

    #[test]
    fn normalizes_a_zero_limit() {
        let limiter = Arc::new(RequestLimiter::new(0));
        let permit = Arc::clone(&limiter).try_acquire_owned().unwrap();
        assert!(Arc::clone(&limiter).try_acquire_owned().is_err());
        drop(permit);
    }
}
