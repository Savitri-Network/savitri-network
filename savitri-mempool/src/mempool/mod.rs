// Mempool module with class-aware architecture
pub mod admission;
pub mod core;
pub mod dual_token_integration;
pub mod handle;
pub mod integration;
pub mod metrics;
pub mod nonce_limits; // Centralized nonce-gap constants (audit §2.2)
pub mod prevalidation;
pub mod queued_pool; // Queued pool for future nonce transactions
pub mod replay_prevention; // Comprehensive replay prevention system
pub mod types; // Class-aware tx types (TxClass, MempoolTx, PrevalidatedTx, …)
               // Legacy mempool implementation has been moved to legacy_not_used/mempool_legacy.rs
               // It is no longer used in production - MempoolPipeline is the primary implementation
               // pub mod legacy; // MOVED TO legacy_not_used/mempool_legacy.rs
pub mod hybrid;
pub mod sharded; // Experimental - for future high-concurrency edge nodes // Hybrid architecture: Sharded ingress + Monolithic production
                 // pub mod tests; // Advanced integration tests for atomic nonce resolution (temporarily disabled)
// `simple_tests` was removed: it referenced an old `core::tx` module that
// no longer exists, and the workspace split made all of its imports
// unresolvable. See git history.

// Legacy mempool implementation has been moved to legacy_not_used/
// The new MempoolPipeline (from integration.rs) is now the primary implementation
// pub use legacy::*; // REMOVED - legacy mempool moved to legacy_not_used/

// Re-export new architecture types (but not Mempool to avoid conflict)
pub use crate::mempool::admission::{AdmissionConfig, AdmissionControl, AdmissionResult};
pub use crate::mempool::core::{
    Mempool, MempoolConfig as MempoolConfigCore,
    MempoolWithBackgroundPurge as MempoolWithBackgroundPurgeCore,
};
pub use crate::mempool::handle::MempoolHandle;
pub use crate::mempool::integration::{
    bytes_to_raw_tx, get_tx_bytes_from_handles, MempoolPipeline,
};
pub use crate::mempool::metrics::*;
pub use crate::mempool::prevalidation::{PrevalidationResult, Prevalidator, TxStorage};
pub use crate::mempool::queued_pool::{
    QueuedPool, QueuedPoolConfig, QueuedPoolError, QueuedPoolStats,
};
pub use crate::mempool::replay_prevention::{
    ReplayPrevention, ReplayPreventionConfig, ReplayPreventionError, ReplayPreventionStats,
};
pub use crate::mempool::types::{MempoolTx, PrevalidatedTx, RawTx, SenderId, TxClass, TxHandle};

// Error types for mempool operations
#[derive(Default, Clone, Debug)]
pub struct PurgeMetrics {
    pub total_purged: u64,
    pub batches_purged: u64,
    pub transactions_purged: u64,
    pub purge_count: u64,
    pub lazy_skip_count: u64,
    pub total_purge_time_us: u64,
    pub last_purge_time_us: u64,
    pub purged_tx_count: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum TxIngestError {
    #[error("Transaction is empty")]
    Empty,
    #[error("Transaction too large: {0} bytes")]
    TooLarge(usize),
    #[error("Decode error: {0}")]
    Decode(String),
    #[error("Rejected: {0}")]
    Rejected(String),
    #[error("Duplicate transaction")]
    Duplicate,
    #[error("Bad signature: {0}")]
    BadSignature(String),
}

// Experimental sharded mempool (for future edge nodes with high concurrency)
// ⚠️ EXPERIMENTAL - Not used in production. Mempool (monolithic) is the primary implementation.
// See docs/architecture/sharded_mempool.md for details.
// Available for testing, benchmarking, and future high-concurrency edge nodes.
pub use crate::mempool::sharded::{MempoolInterface, MempoolShard, ShardBatch, ShardedMempool};

// Hybrid mempool architecture: Sharded ingress + Monolithic production
// See docs/architecture/sharded_mempool_hybrid_design.md for architecture details
pub use crate::mempool::hybrid::{HybridMempoolPipeline, MempoolTransfer, TransferMetrics};
