#![allow(dead_code, unused_variables, unused_imports)]

//! Savitri Lightnode Library
//!

// Note: Real P2P implementations are in src/p2p/ directory

#[cfg(feature = "desktop")]
pub mod availability;
#[cfg(feature = "desktop")]
pub mod config;
#[cfg(feature = "desktop")]
pub mod integrity;
#[cfg(feature = "desktop")]
pub mod latency_service;
// V0.2 Phase 1 (Score Canonicity, issue #31)
#[cfg(feature = "desktop")]
pub mod latency_canon_publisher;
#[cfg(feature = "desktop")]
pub mod latency_canon_state;
#[cfg(feature = "desktop")]
pub mod logging;
#[cfg(feature = "desktop")]
pub mod p2p;
#[cfg(feature = "desktop")]
pub mod proposer;
// peer_registry works for both desktop and mobile — needs reqwest only
pub mod peer_registry;
#[cfg(feature = "desktop")]
pub mod peer_server;

// Core modules for light node
pub mod core;
pub mod fee;
pub mod sharding;
pub mod storage;

// Additional modules needed
pub mod compatibility;
/// Tier 8 (DIAG consolidation) — typed Prometheus metric sinks. Provides
/// `ConsensusObsMetrics` and `RpcConsumerMetrics` ZST structs that
/// callsites use to emit `metrics::counter!/histogram!` without scattering
/// `static AtomicU64` blocks across the codebase.
#[cfg(feature = "desktop")]
pub mod observability;
pub mod tx;
/// Shard-aware TX dispatcher (P1). Depends on `savitri-rpc` for the TxRouter
/// trait, so it's only compiled when the `rpc` feature is active.
#[cfg(feature = "rpc")]
pub mod tx_router;

// Re-export commonly used types
pub use core::tx::{
    CallTransaction, MempoolTx, Transaction, TxHandle, TxPoolEntry, ValidationResult,
};
pub use fee::fee::{DualTokenEngine, FeeConfig, FeeEngine, FeeEstimate, FeeMarket};
pub use sharding::sharding::{
    CrossShardCoordinator, ShardConfig, ShardId, ShardManager, ShardStats,
};
pub use storage::{Account, Storage, StorageConfig};
pub use tx::{
    build_and_sign_transaction_ext, verify_transaction_signature_ext, Block, TransactionExt,
};
#[cfg(feature = "desktop")]
pub mod adaptive_latency;
#[cfg(feature = "desktop")]
pub mod resource;
#[cfg(feature = "desktop")]
pub mod signer;
#[cfg(feature = "desktop")]
pub mod telemetry;

// Re-export principali tipi e funzioni (solo con feature full)
#[cfg(feature = "desktop")]
pub use p2p::certificate::{validate_certificate, CertificatePendingBlocks};
#[cfg(feature = "desktop")]
pub use proposer::{BlockProposal, ProposalTransaction, ProposerConfig, ProposerService};
#[cfg(feature = "desktop")]
pub use signer::load_or_generate_ed25519;
#[cfg(feature = "desktop")]
pub use tx::run_tx_generator;

#[cfg(feature = "desktop")]
use anyhow::Result;
#[cfg(feature = "desktop")]
use std::sync::Arc;
#[cfg(feature = "desktop")]
use tokio::sync::mpsc;

/// Main lightnode structure (solo con feature full)
#[cfg(feature = "desktop")]
pub struct Lightnode {
    proposer: ProposerService,
    certificate_manager: Arc<tokio::sync::Mutex<CertificatePendingBlocks>>,
}

#[cfg(feature = "desktop")]
impl Lightnode {
    /// Create a new lightnode instance
    pub fn new(
        keypair: savitri_core::crypto::Keypair,
        config: ProposerConfig,
        proposal_tx: mpsc::Sender<BlockProposal>,
    ) -> Self {
        let proposer = ProposerService::new(keypair, config, proposal_tx);
        let certificate_manager =
            Arc::new(tokio::sync::Mutex::new(CertificatePendingBlocks::new()));

        Self {
            proposer,
            certificate_manager,
        }
    }

    /// Get proposer service reference
    pub fn proposer(&self) -> &ProposerService {
        &self.proposer
    }

    /// Get certificate manager reference
    pub fn certificate_manager(&self) -> Arc<tokio::sync::Mutex<CertificatePendingBlocks>> {
        self.certificate_manager.clone()
    }
}

// Note: autonomous_tests module removed — file was deleted in prior cleanup.
// Tests live inline in their respective modules (dag.rs, etc.).
