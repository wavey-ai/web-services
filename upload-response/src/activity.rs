use std::sync::Arc;

use tokio::sync::watch;
use tokio::time::{sleep_until, Instant};

use crate::{ActiveStreamInfo, UploadResponseService};

/// Active stream snapshots in arrival order. Each watcher has an independent cursor.
pub struct ActiveStreamWatcher {
    service: Arc<UploadResponseService>,
    updates: watch::Receiver<u64>,
    initial: bool,
    next_expiry: Option<Instant>,
}

impl ActiveStreamWatcher {
    pub(crate) fn new(service: Arc<UploadResponseService>, updates: watch::Receiver<u64>) -> Self {
        Self {
            service,
            updates,
            initial: true,
            next_expiry: None,
        }
    }

    /// Return the initial snapshot, then wait for activity or a claim to expire.
    pub async fn next(&mut self) -> Vec<ActiveStreamInfo> {
        if !self.initial {
            let expiry = async {
                match self.next_expiry {
                    Some(deadline) => sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                _ = self.updates.changed() => {},
                _ = expiry => {},
            }
        }
        // Keep a snapshot pending if the caller cancels during a lock wait.
        self.initial = true;
        self.updates.borrow_and_update();
        let now = Instant::now();
        self.next_expiry = None;
        for slot in self.service.active_stream_slots() {
            let claim = self.service.response_claims[slot.stream_idx].lock().await;
            if let Some(claim) = claim.as_ref().filter(|claim| claim.expires_at > now) {
                self.next_expiry = Some(
                    self.next_expiry
                        .map_or(claim.expires_at, |expiry| expiry.min(claim.expires_at)),
                );
            }
        }
        let streams = self.service.active_streams().await;
        self.initial = false;
        streams
    }
}
