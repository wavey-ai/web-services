pub mod api;
pub mod backend;
pub mod balancer;
pub mod config;
pub mod context;
mod h3_pool;
pub mod ingress;
mod id;
pub mod pool;
pub mod proxy;
pub mod queue;
pub mod router;
pub mod state;
pub mod upstream;
pub mod worker;

pub const DEFAULT_MAX_BACKENDS: usize = 4;
pub const DEFAULT_MAX_QUEUE_PER_BACKEND: usize = 1;
pub const DEFAULT_QUEUE_REQUEST_KB: usize = 5 * 1024;
pub const DEFAULT_QUEUE_SLOT_KB: usize = 200;
pub const HTTP_PACK_FRAME_OVERHEAD_BYTES: usize = 32;
pub const DEFAULT_QUEUE_SLOT_BYTES: usize = DEFAULT_QUEUE_SLOT_KB * 1024;
pub const DEFAULT_HTTP_PACK_BODY_CHUNK_BYTES: usize =
    DEFAULT_QUEUE_SLOT_BYTES - HTTP_PACK_FRAME_OVERHEAD_BYTES;

pub use backend::{Backend, BackendScheme, BackendView};
pub use balancer::LoadBalancingMode;
pub use config::ProxyConfig;
pub use context::REQUEST_ID_HEADER;
pub use ingress::ProxyIngress;
pub use router::ProxyRouter;
pub use state::ProxyState;
pub use upstream::UpstreamProtocol;
pub use queue::ProxyQueue;
pub use worker::{WorkerPool, ProxyWorker};
