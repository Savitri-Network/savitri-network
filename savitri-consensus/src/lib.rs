//! Savitri Consensus Library
//!
//! Unified consensus interface for Savitri blockchain nodes.

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

#[cfg(feature = "zkp")]
use savitri_zkp::{create_verifier, ZkpBackend, ZkpConfig};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::RwLock;

pub mod crypto; // Add crypto module
mod dag;
pub mod error; // Add error module
pub mod primitives;
pub mod protocols;
pub mod scoring; // Runtime observation store feeding real PoU inputs
mod serialization;
pub mod slashing; // Misbehavior tracking, jailing, and permanent removal
pub mod traits;
pub mod types;
pub mod validation; // Make validation public // Canonical consensus primitives (epoch, hashing, quorum) — Tier 1 refactor

// Re-export DAG types for convenience
pub use dag::{BranchInfo, Conflict, ConflictDetector, DAGConfig, DAGManager};
pub use validation::*;
// Re-export protocol types for convenience
pub use protocols::group_aware::{
    GroupAwareConsensus as ProtocolGroupAwareConsensus, GroupProposerSelector,
};

// ============================================================================
// RE-EXPORTS
// ============================================================================

// Re-export error types from the error module
pub use error::{ConsensusError, Result};

// ============================================================================
// CORE TYPES
// ============================================================================

/// PoU score type (0-1000)
pub type PouScore = u16;

/// Maximum PoU score
pub const POU_SCORE_MAX: PouScore = 1000;

/// Validation result
#[derive(Debug, Clone, PartialEq)]
pub enum ValidationResult {
    /// Valid
    Valid,
    /// Invalid with reason
    Invalid(String),
    /// Pending
    Pending,
}

impl ValidationResult {
    /// Check if valid
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}

/// Block header with multi-parent DAG support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeader {
    /// Version
    pub version: u32,
    /// Height
    pub height: u64,
    /// Timestamp
    pub timestamp: u64,

    // MODIFICATO: Supporto multi-genitori con backward compatibility
    /// Primary parent hash (mantained for backward compatibility)
    pub parent_hash: Vec<u8>,
    /// Additional parent hashes for DAG structure (empty for legacy blocks)
    #[serde(with = "serialization::optional_parent_hashes")]
    pub parent_hashes: Vec<Vec<u8>>,

    /// State root
    pub state_root: Vec<u8>,
    /// Transaction root
    pub tx_root: Vec<u8>,
    /// Proposer public key
    pub proposer: Vec<u8>,
    /// Slot number
    pub slot: u64,
    /// Epoch number
    pub epoch: u64,
    /// Transaction count
    pub tx_count: u32,
    /// ZKP proof for block integrity (optional)
    #[cfg(feature = "zkp")]
    pub zkp_proof: Option<Vec<u8>>,
    /// Placeholder for ZKP when feature is disabled
    #[cfg(not(feature = "zkp"))]
    pub _zkp_placeholder: Option<Vec<u8>>,
}

impl Default for BlockHeader {
    fn default() -> Self {
        Self {
            version: 1,
            height: 0,
            timestamp: 0,
            parent_hash: vec![0u8; 64],
            parent_hashes: Vec::new(),
            state_root: vec![0u8; 64],
            tx_root: vec![0u8; 64],
            proposer: vec![0u8; 32],
            slot: 0,
            epoch: 0,
            tx_count: 0,
            #[cfg(feature = "zkp")]
            zkp_proof: None,
            #[cfg(not(feature = "zkp"))]
            _zkp_placeholder: None,
        }
    }
}

impl BlockHeader {
    /// Maximum number of parent hashes supported
    pub const MAX_PARENT_HASHES: usize = 50;

    /// Get parent hash as array
    pub fn parent_hash_array(&self) -> [u8; 64] {
        let mut hash = [0u8; 64];
        let len = std::cmp::min(self.parent_hash.len(), 64);
        hash[..len].copy_from_slice(&self.parent_hash[..len]);
        hash
    }

    /// Get state root as array
    pub fn state_root_array(&self) -> [u8; 64] {
        let mut root = [0u8; 64];
        let len = std::cmp::min(self.state_root.len(), 64);
        root[..len].copy_from_slice(&self.state_root[..len]);
        root
    }

    /// Get transaction root as array
    pub fn tx_root_array(&self) -> [u8; 64] {
        let mut root = [0u8; 64];
        let len = std::cmp::min(self.tx_root.len(), 64);
        root[..len].copy_from_slice(&self.tx_root[..len]);
        root
    }

    /// Get proposer as array
    pub fn proposer_array(&self) -> [u8; 32] {
        let mut proposer = [0u8; 32];
        let len = std::cmp::min(self.proposer.len(), 32);
        proposer[..len].copy_from_slice(&self.proposer[..len]);
        proposer
    }

    /// Get all parent hashes (primary + additional)
    pub fn get_all_parents(&self) -> Vec<[u8; 64]> {
        let mut all_parents = Vec::with_capacity(1 + self.parent_hashes.len());
        all_parents.push(self.parent_hash_array());
        for hash_vec in &self.parent_hashes {
            if hash_vec.len() == 64 {
                let mut hash = [0u8; 64];
                hash.copy_from_slice(hash_vec);
                all_parents.push(hash);
            }
        }
        all_parents
    }

    /// Get total number of parent hashes
    pub fn parent_count(&self) -> usize {
        1 + self.parent_hashes.len()
    }

    /// Check if this is a multi-parent block
    pub fn is_multi_parent(&self) -> bool {
        !self.parent_hashes.is_empty()
    }

    /// Validate parent hashes constraints
    pub fn validate_parents(&self) -> Result<()> {
        if self.parent_hashes.len() > Self::MAX_PARENT_HASHES - 1 {
            return Err(ConsensusError::ValidationFailed(format!(
                "Too many parent hashes: {} > {}",
                self.parent_hashes.len(),
                Self::MAX_PARENT_HASHES - 1
            )));
        }

        // Check primary hash length
        if self.parent_hash.len() != 64 {
            return Err(ConsensusError::ValidationFailed(
                "Invalid primary parent hash length".to_string(),
            ));
        }

        // Check additional hash lengths
        for hash in &self.parent_hashes {
            if hash.len() != 64 {
                return Err(ConsensusError::ValidationFailed(
                    "Invalid parent hash length".to_string(),
                ));
            }
        }

        // Check for duplicates
        let mut all_parents = self.get_all_parents();
        all_parents.sort();
        all_parents.dedup();
        if all_parents.len() != self.parent_count() {
            return Err(ConsensusError::ValidationFailed(
                "Duplicate parent hashes detected".to_string(),
            ));
        }

        Ok(())
    }

    /// Create legacy block header (backward compatibility)
    pub fn legacy(
        version: u32,
        height: u64,
        timestamp: u64,
        parent_hash: [u8; 64],
        state_root: [u8; 64],
        tx_root: [u8; 64],
        proposer: [u8; 32],
        slot: u64,
        epoch: u64,
        tx_count: u32,
    ) -> Self {
        Self {
            version,
            height,
            timestamp,
            parent_hash: parent_hash.to_vec(),
            parent_hashes: Vec::new(),
            state_root: state_root.to_vec(),
            tx_root: tx_root.to_vec(),
            proposer: proposer.to_vec(),
            slot,
            epoch,
            tx_count,
            #[cfg(feature = "zkp")]
            zkp_proof: None,
            #[cfg(not(feature = "zkp"))]
            _zkp_placeholder: None,
        }
    }

    /// Create multi-parent block header
    pub fn multi_parent(
        version: u32,
        height: u64,
        timestamp: u64,
        parent_hash: [u8; 64],
        parent_hashes: Vec<[u8; 64]>,
        state_root: [u8; 64],
        tx_root: [u8; 64],
        proposer: [u8; 32],
        slot: u64,
        epoch: u64,
        tx_count: u32,
    ) -> Result<Self> {
        let header = Self {
            version,
            height,
            timestamp,
            parent_hash: parent_hash.to_vec(),
            parent_hashes: parent_hashes.into_iter().map(|h| h.to_vec()).collect(),
            state_root: state_root.to_vec(),
            tx_root: tx_root.to_vec(),
            proposer: proposer.to_vec(),
            slot,
            epoch,
            tx_count,
            #[cfg(feature = "zkp")]
            zkp_proof: None,
            #[cfg(not(feature = "zkp"))]
            _zkp_placeholder: None,
        };

        header.validate_parents()?;
        Ok(header)
    }
}

/// Transaction
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Transaction {
    /// Hash
    pub hash: Vec<u8>,
    /// From address
    pub from: Vec<u8>,
    /// To address
    pub to: Vec<u8>,
    /// Amount
    pub amount: u64,
    /// Nonce
    pub nonce: u64,
    /// Fee
    pub fee: u64,
    /// Data
    pub data: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: Vec<u8>,
}

impl Default for Transaction {
    fn default() -> Self {
        Self {
            hash: vec![0u8; 64],
            from: vec![0u8; 32],
            to: vec![0u8; 32],
            amount: 0,
            nonce: 0,
            fee: 0,
            data: Vec::new(),
            timestamp: 0,
            signature: vec![0u8; 64],
        }
    }
}

/// Block
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Block {
    /// Header
    pub header: crate::types::block::BlockHeader,
    /// Transactions
    pub transactions: Vec<Transaction>,
}

impl Block {
    /// Calculate block hash
    pub fn hash(&self) -> [u8; 64] {
        let data = match bincode::serialize(&self.header) {
            Ok(d) => d,
            Err(_) => return [0u8; 64],
        };
        let hash = blake3::hash(&data);
        let mut result = [0u8; 64];
        result[..32].copy_from_slice(hash.as_bytes());
        result
    }

    /// Check if block is valid
    pub fn is_valid(&self) -> bool {
        self.header.height > 0 || self.header.timestamp > 0
    }
}

// ── SECURITY (C-08): VRF-like proposer selection helper ──────────────────
//
// Deterministic but unpredictable weighted selection using blake3.
// Used by all consensus implementations in this crate.
fn vrf_weighted_select_lib(
    slot: u64,
    last_block_hash: &[u8; 64],
    proposers: &[ProposerInfo],
) -> usize {
    if proposers.is_empty() {
        return 0;
    }
    let mut input = Vec::with_capacity(14 + 8 + 64);
    input.extend_from_slice(b"PROPOSER-VRF");
    input.extend_from_slice(&slot.to_le_bytes());
    input.extend_from_slice(last_block_hash);
    let hash = blake3::hash(&input);
    let hb = hash.as_bytes();
    let rand_val = u64::from_le_bytes([hb[0], hb[1], hb[2], hb[3], hb[4], hb[5], hb[6], hb[7]]);

    let total_weight: u64 = proposers.iter().map(|p| (p.score as u64).max(1)).sum();
    let target = rand_val % total_weight;
    let mut cumulative: u64 = 0;
    for (i, p) in proposers.iter().enumerate() {
        cumulative += (p.score as u64).max(1);
        if target < cumulative {
            return i;
        }
    }
    proposers.len() - 1
}

/// Proposer information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProposerInfo {
    /// Node ID
    pub node_id: String,
    /// Peer ID
    pub peer_id: String,
    /// Public key
    pub public_key: [u8; 32],
    /// Score
    pub score: u32,
    /// Group ID
    pub group_id: Option<String>,
    /// Region
    pub region: String,
    /// Capabilities
    pub capabilities: Vec<String>,
}

impl Default for ProposerInfo {
    fn default() -> Self {
        Self {
            node_id: String::new(),
            peer_id: String::new(),
            public_key: [0u8; 32],
            score: 0,
            group_id: None,
            region: String::new(),
            capabilities: Vec::new(),
        }
    }
}

pub use types::validation::{GroupInfo, GroupStatus};

/// Consensus state
#[derive(Debug, Clone)]
pub struct ConsensusState {
    /// Current epoch
    pub current_epoch: u64,
    /// Current slot
    pub current_slot: u64,
    /// Current height
    pub current_height: u64,
    /// Last block hash
    pub last_block_hash: [u8; 64],
    /// Last updated
    pub last_updated: u64,
}

impl Default for ConsensusState {
    fn default() -> Self {
        Self {
            current_epoch: 0,
            current_slot: 0,
            current_height: 0,
            last_block_hash: [0u8; 64],
            last_updated: 0,
        }
    }
}

/// Consensus statistics
#[derive(Debug, Clone, Default)]
pub struct ConsensusStats {
    pub total_validations: u64,
    pub successful_validations: u64,
    pub failed_validations: u64,
    pub average_validation_time_ms: f64,
    /// NUOVO: Multi-group selections
    pub multi_group_selections: u64,
    /// NUOVO: Total multi-group proposers selected
    pub total_multi_group_proposers: u64,
    /// NUOVO: Average multi-group selection time in milliseconds
    pub average_multi_group_selection_time_ms: f64,
    /// NUOVO: Maximum simultaneous groups used
    pub max_simultaneous_groups_used: usize,
    /// NUOVO: DAG parallelism enabled flag
    pub dag_parallelism_enabled: bool,
}

/// Block proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockProposal {
    /// Round ID
    pub round_id: u64,
    /// Height
    pub height: u64,
    /// Timestamp
    pub timestamp: u64,
    /// Proposer public key
    pub proposer_pubkey: Vec<u8>,
    /// Proposer PoU score
    pub proposer_pou_score: u32,
    /// Parent hash
    pub parent_hash: Vec<u8>,
    /// State root
    pub state_root: Vec<u8>,
    /// Transaction root
    pub tx_root: Vec<u8>,
    /// Transactions
    pub transactions: Vec<Transaction>,
    /// Signature
    pub signature: Vec<u8>,
    /// ZKP proof for block integrity (optional)
    #[cfg(feature = "zkp")]
    pub zkp_proof: Option<Vec<u8>>,
}

impl Default for BlockProposal {
    fn default() -> Self {
        Self {
            round_id: 0,
            height: 0,
            timestamp: 0,
            proposer_pubkey: vec![0u8; 32],
            proposer_pou_score: 0,
            parent_hash: vec![0u8; 64],
            state_root: vec![0u8; 64],
            tx_root: vec![0u8; 64],
            transactions: Vec::new(),
            signature: vec![0u8; 64],
            #[cfg(feature = "zkp")]
            zkp_proof: None,
        }
    }
}

impl BlockProposal {
    /// Check if proposal is valid
    pub fn is_valid(&self) -> bool {
        self.height > 0 || self.timestamp > 0
    }
}

/// PoU score result
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PouScoreResult {
    /// Score
    pub score: PouScore,
    /// Node ID
    pub node_id: String,
    /// Peer ID
    pub peer_id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Epoch
    pub epoch: u64,
}

// ============================================================================
// CONFIGURATION TYPES
// ============================================================================

/// Group-aware consensus configuration
#[derive(Debug, Clone)]
pub struct GroupAwareConfig {
    /// Minimum group size
    pub min_group_size: usize,
    /// Maximum group size
    pub max_group_size: usize,
    /// Group cache TTL
    pub group_cache_ttl_secs: u64,
    /// Enable fallback
    pub enable_fallback: bool,
    /// Validation timeout
    pub validation_timeout_ms: u64,
    /// Minimum health score in permille (0–1000). AUDIT-003.
    pub min_health_score: u32,
    /// Minimum uptime in permille (0–1000). AUDIT-003.
    pub min_uptime_percentage: u32,
    pub enable_geographic_validation: bool,
    pub enable_performance_validation: bool,
    pub enable_signature_validation: bool,
    /// Cache TTL
    pub cache_ttl_secs: u64,
    /// Rate limit per minute
    pub rate_limit_per_minute: u32,
    /// ZKP configuration
    #[cfg(feature = "zkp")]
    pub zkp_config: Option<ZkpConfig>,

    // NUOVI: Parametri DAG (con default safe)
    /// Maximum simultaneous groups for DAG parallelism
    pub max_simultaneous_groups: usize,
    /// Enable DAG parallelism feature flag
    pub enable_dag_parallelism: bool,
    /// Maximum parent hashes per block in DAG
    pub max_parent_hashes: usize,
    /// Enable conflict detection for DAG merges
    pub conflict_detection_enabled: bool,
    /// Block interval for DAG merge operations
    pub merge_interval_blocks: u64,
}

impl GroupAwareConfig {
    /// Create new configuration with DAG parameters
    pub fn with_dag_support(
        min_group_size: usize,
        max_group_size: usize,
        max_simultaneous_groups: usize,
        max_parent_hashes: usize,
    ) -> Result<Self> {
        let mut config = Self::default();
        config.min_group_size = min_group_size;
        config.max_group_size = max_group_size;
        config.max_simultaneous_groups = max_simultaneous_groups;
        config.max_parent_hashes = max_parent_hashes;
        config.enable_dag_parallelism = true;

        config.validate_dag_params()?;
        Ok(config)
    }

    /// Validate DAG parameters
    pub fn validate_dag_params(&self) -> Result<()> {
        // Validate group sizes
        if self.min_group_size == 0 || self.max_group_size == 0 {
            return Err(ConsensusError::ValidationFailed(
                "Group sizes must be greater than 0".to_string(),
            ));
        }

        if self.min_group_size > self.max_group_size {
            return Err(ConsensusError::ValidationFailed(
                "min_group_size cannot be greater than max_group_size".to_string(),
            ));
        }

        // Validate DAG parameters
        if self.enable_dag_parallelism {
            if self.max_simultaneous_groups == 0 {
                return Err(ConsensusError::ValidationFailed(
                    "max_simultaneous_groups must be greater than 0 when DAG parallelism is enabled".to_string()
                ));
            }

            if self.max_parent_hashes == 0 || self.max_parent_hashes > 50 {
                return Err(ConsensusError::ValidationFailed(
                    "max_parent_hashes must be between 1 and 50".to_string(),
                ));
            }

            if self.merge_interval_blocks == 0 {
                return Err(ConsensusError::ValidationFailed(
                    "merge_interval_blocks must be greater than 0".to_string(),
                ));
            }

            // Validate reasonable limits
            if self.max_simultaneous_groups > 1000 {
                return Err(ConsensusError::ValidationFailed(
                    "max_simultaneous_groups should not exceed 1000 for performance reasons"
                        .to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Check if DAG parallelism is enabled and properly configured
    pub fn is_dag_enabled(&self) -> bool {
        self.enable_dag_parallelism
            && self.max_simultaneous_groups > 0
            && self.max_parent_hashes > 0
    }

    /// Get effective maximum groups considering DAG settings
    pub fn effective_max_groups(&self) -> usize {
        if self.is_dag_enabled() {
            self.max_simultaneous_groups.max(self.max_group_size)
        } else {
            self.max_group_size
        }
    }

    /// Toggle DAG parallelism feature flag
    pub fn toggle_dag_parallelism(&mut self, enabled: bool) -> Result<()> {
        self.enable_dag_parallelism = enabled;
        if enabled {
            self.validate_dag_params()?;
        }
        Ok(())
    }

    /// Create production-safe configuration
    pub fn production() -> Self {
        let mut config = Self::default();
        // Ensure production-safe values
        config.max_simultaneous_groups = 50;
        config.enable_dag_parallelism = false; // Disabled by default for safety
        config.max_parent_hashes = 10;
        config.conflict_detection_enabled = true;
        config.merge_interval_blocks = 15;
        config
    }

    /// Create development configuration with DAG enabled
    pub fn development() -> Self {
        let mut config = Self::default();
        config.enable_dag_parallelism = true;
        config.max_simultaneous_groups = 20;
        config.max_parent_hashes = 5;
        config.conflict_detection_enabled = true;
        config.merge_interval_blocks = 5;
        config
    }
}

impl Default for GroupAwareConfig {
    fn default() -> Self {
        Self {
            // Valori esistenti mantenuti
            min_group_size: 4,
            max_group_size: 8,
            group_cache_ttl_secs: 300,
            enable_fallback: true,
            validation_timeout_ms: 1000,
            min_health_score: 700,      // 700 permille = 70%
            min_uptime_percentage: 800, // 800 permille = 80%
            enable_geographic_validation: true,
            enable_performance_validation: true,
            enable_signature_validation: true,
            cache_ttl_secs: 60,
            rate_limit_per_minute: 10,
            #[cfg(feature = "zkp")]
            zkp_config: Some(ZkpConfig::production()),

            max_simultaneous_groups: 50,
            enable_dag_parallelism: false, // Feature flag disabilitato di default
            max_parent_hashes: 10,
            conflict_detection_enabled: true,
            merge_interval_blocks: 15,
        }
    }
}

/// PoU-based consensus configuration
#[derive(Debug, Clone)]
pub struct PouConfig {
    /// Score update interval
    pub score_update_interval_secs: u64,
    /// Minimum proposer score
    pub min_proposer_score: PouScore,
    /// Enable latency proof
    pub enable_latency_proof: bool,
    /// Enable availability proof
    pub enable_availability_proof: bool,
    /// Maximum proposal size
    pub max_proposal_size: usize,
    /// Proposal timeout
    pub proposal_timeout_ms: u64,
    pub enable_signature_validation: bool,
    pub enable_state_validation: bool,
    /// Score decay rate (permille, 0-1000; e.g. 10 = 1.0% decay)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub score_decay_rate: u32,
    /// Minimum uptime percentage (permille, 0-1000; e.g. 800 = 80%)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub min_uptime_percentage: u32,
    /// Geographic weight (permille, 0-1000)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub geographic_weight: u32,
    /// Performance weight (permille, 0-1000)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub performance_weight: u32,
}

impl Default for PouConfig {
    fn default() -> Self {
        Self {
            score_update_interval_secs: 60,
            min_proposer_score: 500,
            enable_latency_proof: true,
            enable_availability_proof: true,
            max_proposal_size: 1024 * 1024,
            proposal_timeout_ms: 5000,
            enable_signature_validation: true,
            enable_state_validation: true,
            score_decay_rate: 10,       // 10 permille = 1.0%
            min_uptime_percentage: 800, // 800 permille = 80%
            geographic_weight: 100,     // 100 permille = 10%
            performance_weight: 200,    // 200 permille = 20%
        }
    }
}

/// Storage configuration
#[derive(Debug, Clone, Default)]
pub struct StorageConfig {
    /// Path
    pub path: String,
}

// ============================================================================
// STORAGE
// ============================================================================

/// Memory storage for consensus data
pub struct MemoryStorage {
    blocks: Arc<RwLock<HashMap<Vec<u8>, Block>>>,
    height_index: Arc<RwLock<HashMap<u64, Vec<u8>>>>,
    groups: Arc<RwLock<HashMap<String, GroupInfo>>>,
    scores: Arc<RwLock<HashMap<String, PouScoreResult>>>,
    state: Arc<RwLock<Option<ConsensusState>>>,
}

impl MemoryStorage {
    /// Create new memory storage
    pub fn new(_config: StorageConfig) -> Self {
        Self {
            blocks: Arc::new(RwLock::new(HashMap::new())),
            height_index: Arc::new(RwLock::new(HashMap::new())),
            groups: Arc::new(RwLock::new(HashMap::new())),
            scores: Arc::new(RwLock::new(HashMap::new())),
            state: Arc::new(RwLock::new(None)),
        }
    }

    /// Store a group
    pub async fn store_group(&self, group: &GroupInfo) -> Result<()> {
        self.groups
            .write()
            .await
            .insert(group.group_id.clone(), group.clone());
        Ok(())
    }

    /// Get a group
    pub async fn get_group(&self, group_id: &str) -> Result<Option<GroupInfo>> {
        Ok(self.groups.read().await.get(group_id).cloned())
    }

    /// Get active groups
    pub async fn get_active_groups(&self) -> Result<Vec<GroupInfo>> {
        Ok(self
            .groups
            .read()
            .await
            .values()
            .filter(|g| g.status == GroupStatus::Active)
            .cloned()
            .collect())
    }

    /// Store a score
    pub async fn store_score(&self, node_id: &str, score: &PouScoreResult) -> Result<()> {
        self.scores
            .write()
            .await
            .insert(node_id.to_string(), score.clone());
        Ok(())
    }

    /// Get a score
    pub async fn get_score(&self, node_id: &str) -> Result<Option<PouScoreResult>> {
        Ok(self.scores.read().await.get(node_id).cloned())
    }
}

// ============================================================================
// GROUP-AWARE CONSENSUS (MASTERNODE)
// ============================================================================

/// Group-aware consensus engine for masternodes
pub struct GroupAwareConsensus {
    config: GroupAwareConfig,
    storage: Arc<dyn traits::storage::Storage>,
    state: Arc<RwLock<ConsensusState>>,
    stats: Arc<RwLock<ConsensusStats>>,
    #[cfg(feature = "zkp")]
    zkp_verifier: Option<Box<dyn savitri_zkp::ZkVerifier>>,
}

impl GroupAwareConsensus {
    /// Create a new group-aware consensus engine.
    /// Accepts any storage implementing `traits::storage::Storage` (e.g. MemoryStorage or ConsensusStorageAdapter).
    pub fn new(
        config: GroupAwareConfig,
        storage: Arc<dyn traits::storage::Storage>,
    ) -> Result<Self> {
        #[cfg(feature = "zkp")]
        let zkp_verifier = config
            .zkp_config
            .as_ref()
            .map(|cfg| create_verifier(cfg.clone()));

        Ok(Self {
            config,
            storage,
            state: Arc::new(RwLock::new(ConsensusState::default())),
            stats: Arc::new(RwLock::new(ConsensusStats::default())),
            #[cfg(feature = "zkp")]
            zkp_verifier,
        })
    }

    /// Validate a block proposal
    pub async fn validate_proposal(&self, proposal: &BlockProposal) -> Result<ValidationResult> {
        if !proposal.is_valid() {
            return Ok(ValidationResult::Invalid("Invalid proposal".to_string()));
        }

        if proposal.proposer_pou_score < self.config.min_health_score {
            return Ok(ValidationResult::Invalid("Score too low".to_string()));
        }

        #[cfg(feature = "zkp")]
        if let (Some(verifier), Some(proof_data)) = (&self.zkp_verifier, &proposal.zkp_proof) {
            self.validate_zkp_proof(verifier.as_ref(), proposal, proof_data)
                .await?;
        }

        Ok(ValidationResult::Valid)
    }

    /// Validate ZKP proof for block proposal
    #[cfg(feature = "zkp")]
    async fn validate_zkp_proof(
        &self,
        verifier: &dyn savitri_zkp::ZkVerifier,
        proposal: &BlockProposal,
        proof_data: &[u8],
    ) -> Result<()> {
        use savitri_zkp::{Statement, ZkProof};

        // Create statement from block header data
        let statement = Statement {
            a: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.parent_hash[..32]);
                arr
            },
            b: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.state_root[..32]);
                arr
            },
            c: {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&proposal.tx_root[..32]);
                arr
            },
            d: [0u8; 32], // Reserved for future use
            e: proposal.height,
            f: proposal.timestamp,
        };

        // Reconstruct ZKP proof from block data
        let proof = ZkProof {
            proof: proof_data.to_vec(),
            public_inputs: {
                let mut inputs = Vec::new();
                inputs.extend_from_slice(&proposal.parent_hash[..32]);
                inputs.extend_from_slice(&proposal.state_root[..32]);
                inputs.extend_from_slice(&proposal.tx_root[..32]);
                inputs.extend_from_slice(&[0u8; 32]);
                inputs.extend_from_slice(&proposal.height.to_le_bytes());
                inputs.extend_from_slice(&proposal.timestamp.to_le_bytes());
                inputs
            },
            verification_key: proposal.proposer_pubkey.to_vec(),
        };

        // Verify the proof
        let is_valid = verifier
            .verify(&statement, &proof)
            .map_err(|e| ConsensusError::ZkpError(e.to_string()))?;

        if !is_valid {
            return Err(ConsensusError::ZkpError(
                "ZKP proof verification failed".to_string(),
            ));
        }

        Ok(())
    }

    /// Get proposer for a given slot
    ///
    /// SECURITY (C-08): Uses blake3-based VRF-like selection instead of
    /// deterministic `slot % len` to prevent targeted DDoS on predictable proposers.
    pub async fn get_proposer(&self, slot: u64) -> Result<Option<ProposerInfo>> {
        let groups = self.storage.get_active_groups().await?;

        if groups.is_empty() {
            return Ok(None);
        }

        let state = self.state.read().await;
        // VRF-like group selection
        let mut input = Vec::with_capacity(14 + 8 + 64);
        input.extend_from_slice(b"GROUP-VRF   ");
        input.extend_from_slice(&slot.to_le_bytes());
        input.extend_from_slice(&state.last_block_hash);
        let hash = blake3::hash(&input);
        let hb = hash.as_bytes();
        let rand_val = u64::from_le_bytes([hb[0], hb[1], hb[2], hb[3], hb[4], hb[5], hb[6], hb[7]]);
        let group_index = (rand_val as usize) % groups.len();
        let selected_group = &groups[group_index];

        if let Some(proposer_id) = &selected_group.proposer {
            Ok(Some(ProposerInfo {
                node_id: proposer_id.clone(),
                peer_id: proposer_id.clone(),
                public_key: [0u8; 32],
                score: 800,
                group_id: Some(selected_group.group_id.clone()),
                region: "global".to_string(),
                capabilities: vec!["group-proposer".to_string()],
            }))
        } else {
            Ok(None)
        }
    }

    /// Update groups
    pub async fn update_groups(&self, groups: Vec<GroupInfo>) -> Result<()> {
        for group in groups {
            self.storage.store_group(&group).await?;
        }
        Ok(())
    }

    /// Get active groups
    pub async fn get_active_groups(&self) -> Result<Vec<GroupInfo>> {
        self.storage.get_active_groups().await
    }

    /// Validate group membership
    pub async fn validate_group_membership(
        &self,
        proposer_id: &str,
        group_id: &str,
    ) -> Result<bool> {
        if let Some(group) = self.storage.get_group(group_id).await? {
            Ok(group.members.contains(&proposer_id.to_string())
                && group.status == GroupStatus::Active)
        } else {
            Ok(false)
        }
    }

    /// NUOVO: Seleziona multiple proposte da gruppi diversi
    pub async fn select_multiple_proposers(
        &self,
        groups: &[GroupInfo],
        slot: u64,
    ) -> Result<Vec<ProposerInfo>> {
        if !self.config.enable_dag_parallelism {
            return Ok(vec![]); // Fallback a comportamento singolo
        }

        let active_groups: Vec<_> = groups
            .iter()
            .filter(|g| g.status == GroupStatus::Active)
            .filter(|g| g.members.len() >= self.config.min_group_size)
            .filter(|g| g.members.len() <= self.config.max_group_size)
            .filter(|g| g.health_score >= self.config.min_health_score)
            .collect();

        let num_groups = std::cmp::min(active_groups.len(), self.config.max_simultaneous_groups);
        let mut proposers = Vec::new();

        for (i, group) in active_groups.iter().take(num_groups).enumerate() {
            // Simple proposer selection - take first member as proposer
            if let Some(member_id) = group.members.first() {
                proposers.push(ProposerInfo {
                    node_id: member_id.clone(),
                    peer_id: member_id.clone(),
                    public_key: [0u8; 32],
                    score: 800,
                    group_id: Some(group.group_id.clone()),
                    region: "global".to_string(),
                    capabilities: vec!["group-proposer".to_string()],
                });
            }
        }

        Ok(proposers)
    }

    /// Get consensus statistics
    pub fn stats(&self) -> ConsensusStats {
        ConsensusStats::default()
    }

    /// Get consensus state
    pub async fn state(&self) -> ConsensusState {
        self.state.read().await.clone()
    }
}

// ============================================================================
// POU-BASED CONSENSUS (LIGHTNODE)
// ============================================================================

/// PoU-based consensus engine for lightnodes
pub struct PouBasedConsensus {
    config: PouConfig,
    storage: Arc<MemoryStorage>,
    state: Arc<RwLock<ConsensusState>>,
    stats: Arc<RwLock<ConsensusStats>>,
}

impl PouBasedConsensus {
    /// Create a new PoU-based consensus engine
    pub fn new(config: PouConfig, storage: Arc<MemoryStorage>) -> Result<Self> {
        Ok(Self {
            config,
            storage,
            state: Arc::new(RwLock::new(ConsensusState::default())),
            stats: Arc::new(RwLock::new(ConsensusStats::default())),
        })
    }

    /// Validate a block proposal
    pub async fn validate_proposal(&self, proposal: &BlockProposal) -> Result<ValidationResult> {
        if !proposal.is_valid() {
            return Ok(ValidationResult::Invalid("Invalid proposal".to_string()));
        }

        if proposal.proposer_pou_score < self.config.min_proposer_score as u32 {
            return Ok(ValidationResult::Invalid("Score too low".to_string()));
        }

        Ok(ValidationResult::Valid)
    }

    /// Create a block proposal
    pub async fn create_proposal(&self, slot: u64) -> Result<Option<BlockProposal>> {
        let state = self.state.read().await;

        let proposal = BlockProposal {
            round_id: slot,
            height: state.current_height + 1,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            proposer_pubkey: vec![0u8; 32],
            proposer_pou_score: 800,
            parent_hash: state.last_block_hash.to_vec(),
            state_root: vec![0u8; 64],
            tx_root: vec![0u8; 64],
            transactions: Vec::new(),
            signature: vec![0u8; 64],
            #[cfg(feature = "zkp")]
            zkp_proof: None,
        };

        Ok(Some(proposal))
    }

    /// Get proposer for a given slot
    ///
    /// SECURITY (C-08): Uses blake3-based VRF-like selection instead of
    /// deterministic `slot % len` to prevent targeted DDoS on predictable proposers.
    pub async fn get_proposer(&self, slot: u64) -> Result<Option<ProposerInfo>> {
        let top_proposers = self.get_top_proposers(10).await?;

        if top_proposers.is_empty() {
            return Ok(None);
        }

        let state = self.state.read().await;
        let idx = vrf_weighted_select_lib(slot, &state.last_block_hash, &top_proposers);
        Ok(Some(top_proposers[idx].clone()))
    }

    /// Get top proposers by score
    pub async fn get_top_proposers(&self, _count: usize) -> Result<Vec<ProposerInfo>> {
        Ok(Vec::new())
    }

    /// Get node score
    pub async fn get_node_score(&self, node_id: &str) -> Result<Option<PouScoreResult>> {
        self.storage.get_score(node_id).await
    }

    /// Update node score
    pub async fn update_node_score(&self, node_id: &str, score: PouScoreResult) -> Result<()> {
        self.storage.store_score(node_id, &score).await
    }

    /// Calculate score for a node
    pub async fn calculate_score(&self, node_id: &str) -> Result<PouScoreResult> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(PouScoreResult {
            score: 500,
            node_id: node_id.to_string(),
            peer_id: node_id.to_string(),
            timestamp: current_time,
            epoch: current_time / 3600,
        })
    }

    /// Check if node is eligible to be proposer
    pub async fn is_eligible_proposer(
        &self,
        _node_id: &str,
        current_score: PouScore,
    ) -> Result<bool> {
        Ok(current_score >= self.config.min_proposer_score)
    }

    /// Get consensus statistics
    pub fn stats(&self) -> ConsensusStats {
        ConsensusStats::default()
    }

    /// Get consensus state
    pub async fn state(&self) -> ConsensusState {
        self.state.read().await.clone()
    }
}

// ============================================================================
// CONSENSUS VERSION
// ============================================================================

/// Consensus version information
#[derive(Debug, Clone, PartialEq)]
pub struct ConsensusVersion {
    /// Major version
    pub major: u32,
    /// Minor version
    pub minor: u32,
    /// Patch version
    pub patch: u32,
    /// Protocol
    pub protocol: String,
}

impl ConsensusVersion {
    /// Create new version
    pub fn new(major: u32, minor: u32, patch: u32, protocol: String) -> Self {
        Self {
            major,
            minor,
            patch,
            protocol,
        }
    }

    /// Check compatibility
    pub fn is_compatible(&self, other: &ConsensusVersion) -> bool {
        self.major == other.major && self.protocol == other.protocol
    }
}

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

// ============================================================================
// TESTS
// ============================================================================
