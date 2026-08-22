use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

/// Exact nonblocking limit for short-lived request work.
///
/// Alignment keeps the hot count away from the adjacent `Arc` reference counts.
#[repr(align(64))]
#[derive(Debug)]
pub(crate) struct RequestLimiter {
    active: AtomicUsize,
    maximum: usize,
}

impl RequestLimiter {
    pub(crate) fn new(maximum: usize) -> Self {
        Self {
            active: AtomicUsize::new(0),
            maximum: maximum.max(1),
        }
    }

    pub(crate) fn try_acquire_owned(self: Arc<Self>) -> Result<RequestPermit, Arc<Self>> {
        let previous = self.active.fetch_add(1, Ordering::Relaxed);
        if previous < self.maximum {
            Ok(RequestPermit { limiter: self })
        } else {
            self.active.fetch_sub(1, Ordering::Relaxed);
            Err(self)
        }
    }

    #[cfg(test)]
    fn active(&self) -> usize {
        self.active.load(Ordering::Relaxed)
    }
}

pub(crate) struct RequestPermit {
    limiter: Arc<RequestLimiter>,
}

impl Drop for RequestPermit {
    fn drop(&mut self) {
        let previous = self.limiter.active.fetch_sub(1, Ordering::Relaxed);
        debug_assert!(previous > 0, "request permit count underflowed");
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

    #[test]
    fn normalizes_a_zero_limit() {
        let limiter = Arc::new(RequestLimiter::new(0));
        let permit = Arc::clone(&limiter).try_acquire_owned().unwrap();
        assert!(Arc::clone(&limiter).try_acquire_owned().is_err());
        drop(permit);
    }
}
