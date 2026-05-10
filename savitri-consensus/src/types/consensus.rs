//! Consensus-specific types and structures
//!
//! This module defines the consensus-specific data structures used across
//! all consensus implementations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Consensus state information
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConsensusState {
    /// Current epoch
    pub current_epoch: u64,
    /// Current slot
    pub current_slot: u64,
    /// Current block height
    pub current_height: u64,
    /// Last block hash
    pub last_block_hash: crate::types::block::Hash64,
    /// Current proposer
    pub current_proposer: Option<String>,
    pub active_validators: HashMap<String, ValidatorInfo>,
    /// Consensus configuration
    pub config: ConsensusConfig,
    /// Consensus metrics
    pub metrics: ConsensusMetrics,
    /// Last updated timestamp
    pub last_updated: u64,
}

/// Consensus configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusConfig {
    /// Consensus type
    pub consensus_type: ConsensusType,
    /// Slot duration in milliseconds
    pub slot_duration_ms: u64,
    /// Epoch length in slots
    pub epoch_length: u64,
    /// Block size limit in bytes
    pub max_block_size: u64,
    /// Maximum transactions per block
    pub max_tx_per_block: u32,
    pub min_validators: u32,
    pub max_validators: u32,
    /// Proposer selection algorithm
    pub proposer_selection: ProposerSelection,
    /// Validation requirements
    pub validation_requirements: ValidationRequirements,
}

impl Default for ConsensusConfig {
    fn default() -> Self {
        Self {
            consensus_type: ConsensusType::default(),
            slot_duration_ms: 1000,
            epoch_length: 100,
            max_block_size: 1024 * 1024,
            max_tx_per_block: 10000,
            min_validators: 3,
            max_validators: 100,
            proposer_selection: ProposerSelection::Hybrid,
            validation_requirements: ValidationRequirements::new(),
        }
    }
}

/// Consensus type enumeration (Serialize in block.rs to avoid conflict; Deserialize here for ConsensusConfig/ConsensusState)
#[derive(Debug, Clone, PartialEq, Default, Deserialize)]
pub enum ConsensusType {
    /// Group-aware consensus (masternode)
    GroupAware,
    /// PoU-based consensus (lightnode)
    PouBased,
    /// Hybrid consensus
    Hybrid,
    /// Unknown consensus type
    #[default]
    Unknown,
}

/// Proposer selection algorithm
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub enum ProposerSelection {
    /// Round-robin selection
    #[default]
    RoundRobin,
    /// Random selection
    Random,
    /// Score-based selection (PoU)
    ScoreBased,
    /// Group-based selection
    GroupBased,
    /// Hybrid selection
    Hybrid,
}

/// Validation requirements
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationRequirements {
    pub min_signatures: u32,
    /// Supermajority threshold (2/3 by default)
    pub supermajority_threshold: f64,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    pub enable_parallel_validation: bool,

    pub enable_dag_validation: bool, // Default: false
    pub max_dag_branches: usize,     // Default: 50
    /// Timeout for DAG conflict resolution
    pub conflict_resolution_timeout_ms: u64, // Default: 500
}

impl ValidationRequirements {
    pub fn new() -> Self {
        Self {
            min_signatures: 3,
            supermajority_threshold: 0.67,
            timeout_ms: 5000,
            enable_parallel_validation: false,
            enable_dag_validation: false,
            max_dag_branches: 50,
            conflict_resolution_timeout_ms: 500,
        }
    }

    pub fn with_dag_support(
        min_signatures: u32,
        max_dag_branches: usize,
        timeout_ms: u64,
        conflict_resolution_timeout_ms: u64,
    ) -> Result<Self, String> {
        if min_signatures == 0 {
            return Err("Minimum signatures must be > 0".to_string());
        }
        if max_dag_branches == 0 {
            return Err("Maximum DAG branches must be > 0".to_string());
        }
        if timeout_ms == 0 {
            return Err("Timeout must be > 0".to_string());
        }
        if conflict_resolution_timeout_ms == 0 {
            return Err("Conflict resolution timeout must be > 0".to_string());
        }

        Ok(Self {
            min_signatures,
            supermajority_threshold: 0.67,
            timeout_ms,
            enable_parallel_validation: true,
            enable_dag_validation: true,
            max_dag_branches,
            conflict_resolution_timeout_ms,
        })
    }
}

/// Validator information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidatorInfo {
    /// Validator ID
    pub validator_id: String,
    /// Validator public key
    pub public_key: [u8; 32],
    /// Validator stake amount
    pub stake: u64,
    /// Validator score
    pub score: u32,
    /// Validator status
    pub status: ValidatorStatus,
    /// Last active timestamp
    pub last_active: u64,
    /// Validator metadata
    pub metadata: ValidatorMetadata,
}

/// Validator status
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum ValidatorStatus {
    /// Validator is active
    #[default]
    Active,
    /// Validator is inactive
    Inactive,
    /// Validator is slashed
    Slashed,
    /// Validator is jailed
    Jailed,
    /// Validator is pending
    Pending,
}

/// Validator metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ValidatorMetadata {
    /// Geographic region
    pub region: String,
    /// Node type
    pub node_type: NodeType,
    /// Capabilities
    pub capabilities: Vec<String>,
    /// Version information
    pub version: String,
    /// Network address
    pub address: String,
}

/// Node type
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum NodeType {
    /// Masternode
    Masternode,
    /// Lightnode
    Lightnode,
    /// Guardian node
    Guardian,
    /// Unknown node type
    #[default]
    Unknown,
}

/// Consensus metrics - ESTESO per DAG
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ConsensusMetrics {
    /// Total proposals processed
    pub total_proposals: u64,
    /// Successful proposals
    pub successful_proposals: u64,
    /// Failed proposals
    pub failed_proposals: u64,
    pub total_validations: u64,
    pub successful_validations: u64,
    pub failed_validations: u64,
    /// Average proposal time in milliseconds
    pub average_proposal_time_ms: f64,
    pub average_validation_time_ms: f64,
    /// Network latency in milliseconds
    pub network_latency_ms: f64,
    /// Consensus round duration in milliseconds
    pub round_duration_ms: u64,
    /// Last proposal timestamp
    pub last_proposal_timestamp: u64,
    pub last_validation_timestamp: u64,

    // NUOVI: Metriche DAG
    pub dag_parallel_validations: u64,
    /// Total conflicts detected in DAG structure
    pub dag_conflicts_detected: u64,
    /// Number of currently active DAG branches
    pub dag_branches_active: u64,
    /// Total merge operations performed
    pub dag_merge_operations: u64,
    /// Average number of parents per block in DAG
    pub dag_average_parent_count: f64,
    /// Maximum number of parents seen in any block
    pub dag_max_parent_count: usize,
}

impl ConsensusMetrics {
    ///
    ///
    /// # Arguments
    ///
    /// # Examples
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    /// let mut metrics = ConsensusMetrics::default();
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    pub fn record_dag_parallel_validation(&mut self, duration_ms: f64, parent_count: usize) {
        self.dag_parallel_validations += 1;
        self.total_validations += 1;
        self.successful_validations += 1;

        self.average_validation_time_ms = if self.total_validations == 1 {
            duration_ms
        } else {
            (self.average_validation_time_ms * (self.total_validations - 1) as f64 + duration_ms)
                / self.total_validations as f64
        };

        // Update parent count metrics
        self.dag_average_parent_count = if self.dag_parallel_validations == 1 {
            parent_count as f64
        } else {
            (self.dag_average_parent_count * (self.dag_parallel_validations - 1) as f64
                + parent_count as f64)
                / self.dag_parallel_validations as f64
        };

        self.dag_max_parent_count = self.dag_max_parent_count.max(parent_count);

        self.last_validation_timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// NUOVO: Registra conflitto DAG
    ///
    /// Registra il rilevamento di un conflitto in the struttura DAG.
    ///
    /// # Arguments
    /// * `conflict_type` - Tipo di conflitto rilevato (es. "double_spend", "orphan", "state_conflict")
    ///
    /// # Examples
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    /// let mut metrics = ConsensusMetrics::default();
    /// metrics.record_dag_conflict("double_spend");
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    pub fn record_dag_conflict(&mut self, conflict_type: &str) {
        self.dag_conflicts_detected += 1;

        // Log conflict for debugging (in production, use proper logging)
        tracing::debug!("DAG conflict detected: {}", conflict_type);
    }

    ///
    ///
    /// # Arguments
    ///
    /// # Examples
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    /// let mut metrics = ConsensusMetrics::default();
    /// metrics.update_active_branches(5);
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    pub fn update_active_branches(&mut self, count: u64) {
        self.dag_branches_active = count;
    }

    ///
    ///
    /// # Arguments
    ///
    /// # Examples
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    /// let mut metrics = ConsensusMetrics::default();
    /// metrics.record_dag_merge(12.5);
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    pub fn record_dag_merge(&mut self, duration_ms: f64) {
        self.dag_merge_operations += 1;

        // Update round duration if merge is part of consensus round
        self.round_duration_ms = self.round_duration_ms.max(duration_ms as u64);
    }

    ///
    ///
    /// # Returns
    ///
    /// # Examples
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    /// let mut metrics = ConsensusMetrics::default();
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    pub fn dag_throughput(&self) -> f64 {
        if self.average_validation_time_ms > 0.0 {
            1000.0 / self.average_validation_time_ms
        } else {
            0.0
        }
    }

    /// NUOVO: Compute conflict rate
    ///
    ///
    /// # Returns
    /// * `f64` - Conflict rate (0.0 - 1.0)
    ///
    /// # Examples
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    /// let mut metrics = ConsensusMetrics::default();
    /// metrics.record_dag_conflict("double_spend");
    /// let rate = metrics.dag_conflict_rate(); // 1.0 (100%)
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    pub fn dag_conflict_rate(&self) -> f64 {
        if self.dag_parallel_validations > 0 {
            self.dag_conflicts_detected as f64 / self.dag_parallel_validations as f64
        } else {
            0.0
        }
    }

    /// NUOVO: Reset metriche DAG
    ///
    /// Resetta solo le metriche DAG mantenendo intatte le metriche di consenso esistenti.
    /// Utile per test o per riavviare il monitoraggio DAG.
    ///
    /// # Examples
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    /// let mut metrics = ConsensusMetrics::default();
    /// metrics.record_dag_conflict("test");
    /// metrics.reset_dag_metrics(); // Resetta solo le metriche DAG
    /// ```
    /// # use savitri_consensus::types::ConsensusMetrics;
    pub fn reset_dag_metrics(&mut self) {
        self.dag_parallel_validations = 0;
        self.dag_conflicts_detected = 0;
        self.dag_branches_active = 0;
        self.dag_merge_operations = 0;
        self.dag_average_parent_count = 0.0;
        self.dag_max_parent_count = 0;
    }

    /// NUOVO: Compute efficienza complessiva DAG
    ///
    /// Compute un punteggio di efficienza complessiva basato su throughput, conflict rate e branch count.
    ///
    /// # Returns
    /// * `f64` - Efficiency score (0.0 - 1.0, dove 1.0 è ottimale)
    pub fn dag_efficiency_score(&self) -> f64 {
        let throughput_score = (self.dag_throughput() / 1000.0).min(1.0); // Normalize to 0-1
        let conflict_penalty = self.dag_conflict_rate() * 0.5; // Conflicts reduce efficiency
        let branch_factor = (self.dag_branches_active as f64 / 50.0).min(1.0); // Normalize to max 50 branches

        (throughput_score + branch_factor - conflict_penalty)
            .max(0.0)
            .min(1.0)
    }

    /// NUOVO: Check soglie di performance DAG
    ///
    ///
    /// # Returns
    pub fn dag_within_thresholds(&self) -> bool {
        self.dag_conflict_rate() < 0.1 && // < 10% conflict rate
        self.dag_throughput() > 10.0 && // > 10 validations/sec
        self.dag_average_parent_count <= 5.0 && // Average parents <= 5
        self.dag_max_parent_count <= 10 // Max parents <= 10
    }
}

/// Consensus message types
#[derive(Debug, Clone)]
pub enum ConsensusMessage {
    /// Proposal message
    Proposal(ProposalMessage),
    /// Vote message
    Vote(VoteMessage),
    /// Commit message
    Commit(CommitMessage),
    /// Prevote message
    Prevote(PrevoteMessage),
    /// Precommit message
    Precommit(PrecommitMessage),
    /// Heartbeat message
    Heartbeat(HeartbeatMessage),
    /// Sync message
    Sync(SyncMessage),
    /// Sync request
    SyncRequest(SyncRequestMessage),
    /// Sync response
    SyncResponse(SyncResponseMessage),
    /// Custom message
    Custom(CustomMessage),
}

/// Proposal message
#[derive(Debug, Clone, Default)]
pub struct ProposalMessage {
    /// Proposal ID
    pub proposal_id: String,
    /// Block proposal data
    pub proposal_data: Vec<u8>,
    /// Proposer ID
    pub proposer_id: String,
    /// Round number
    pub round: u64,
    /// Slot number
    pub slot: u64,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: crate::types::block::Hash64,
}

/// Vote message
#[derive(Debug, Clone, Default)]
pub struct VoteMessage {
    /// Vote type
    pub vote_type: VoteType,
    /// Proposal ID
    pub proposal_id: String,
    /// Voter ID
    pub voter_id: String,
    /// Round number
    pub round: u64,
    /// Slot number
    pub slot: u64,
    /// Vote value
    pub vote: bool,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: crate::types::block::Hash64,
}

/// Vote type
#[derive(Debug, Clone, PartialEq)]
pub enum VoteType {
    /// Prevote vote
    Prevote,
    /// Precommit vote
    Precommit,
    /// Commit vote
    Commit,
}

/// Commit message
#[derive(Debug, Clone, Default)]
pub struct CommitMessage {
    /// Block hash
    pub block_hash: crate::types::block::Hash64,
    /// Block height
    pub height: u64,
    /// Round number
    pub round: u64,
    /// Commit signatures
    pub signatures: Vec<crate::types::block::Hash64>,
    /// Timestamp
    pub timestamp: u64,
}

/// Prevote message
#[derive(Debug, Clone, Default)]
pub struct PrevoteMessage {
    /// Block hash
    pub block_hash: crate::types::block::Hash64,
    /// Block height
    pub height: u64,
    /// Round number
    pub round: u64,
    /// Validator ID
    pub validator_id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: crate::types::block::Hash64,
}

/// Precommit message
#[derive(Debug, Clone, Default)]
pub struct PrecommitMessage {
    /// Block hash
    pub block_hash: crate::types::block::Hash64,
    /// Block height
    pub height: u64,
    /// Round number
    pub round: u64,
    /// Validator ID
    pub validator_id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: crate::types::block::Hash64,
}

/// Heartbeat message
#[derive(Debug, Clone, Default)]
pub struct HeartbeatMessage {
    /// Validator ID
    pub validator_id: String,
    /// Current height
    pub height: u64,
    /// Current round
    pub round: u64,
    /// Current slot
    pub slot: u64,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: crate::types::block::Hash64,
}

/// Sync message
#[derive(Debug, Clone, Default)]
pub struct SyncMessage {
    /// Sync type
    pub sync_type: SyncType,
    /// Data
    pub data: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
}

/// Sync type
#[derive(Debug, Clone, PartialEq)]
pub enum SyncType {
    /// State sync
    State,
    /// Block sync
    Block,
    /// Header sync
    Header,
}

impl Default for SyncType {
    fn default() -> Self {
        SyncType::State
    }
}

/// Sync request message
#[derive(Debug, Clone, Default)]
pub struct SyncRequestMessage {
    /// Requester ID
    pub requester_id: String,
    /// Start height
    pub start_height: u64,
    /// End height
    pub end_height: u64,
    /// Request type
    pub request_type: SyncRequestType,
    /// Timestamp
    pub timestamp: u64,
}

/// Sync request type
#[derive(Debug, Clone, PartialEq)]
pub enum SyncRequestType {
    /// Full sync
    Full,
    /// Header sync only
    Headers,
    /// Block sync only
    Blocks,
    /// State sync only
    State,
}

/// Sync response message
#[derive(Debug, Clone, Default)]
pub struct SyncResponseMessage {
    /// Responder ID
    pub responder_id: String,
    /// Request ID
    pub request_id: String,
    /// Sync data
    pub data: Vec<u8>,
    /// Response type
    pub response_type: SyncResponseType,
    /// Timestamp
    pub timestamp: u64,
}

/// Sync response type
#[derive(Debug, Clone, PartialEq)]
pub enum SyncResponseType {
    /// Full response
    Full,
    /// Headers response
    Headers,
    /// Blocks response
    Blocks,
    /// State response
    State,
    /// Error response
    Error(String),
}

/// Custom message
#[derive(Debug, Clone, Default)]
pub struct CustomMessage {
    /// Message type
    pub message_type: String,
    /// Message data
    pub data: Vec<u8>,
    /// Sender ID
    pub sender_id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: crate::types::block::Hash64,
}

/// Consensus response types
#[derive(Debug, Clone)]
pub enum ConsensusResponse {
    /// Success response
    Success(SuccessResponse),
    /// Error response
    Error(ErrorResponse),
    /// Pending response
    Pending(PendingResponse),
    /// Custom response
    Custom(CustomResponse),
}

/// Success response
#[derive(Debug, Clone, Default)]
pub struct SuccessResponse {
    /// Response message
    pub message: String,
    /// Response data
    pub data: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
}

/// Error response
#[derive(Debug, Clone, Default)]
pub struct ErrorResponse {
    /// Error code
    pub error_code: u32,
    /// Error message
    pub error_message: String,
    /// Error details
    pub error_details: Vec<String>,
    /// Timestamp
    pub timestamp: u64,
}

/// Pending response
#[derive(Debug, Clone, Default)]
pub struct PendingResponse {
    /// Pending message
    pub message: String,
    /// Estimated completion time
    pub estimated_completion: u64,
    /// Timestamp
    pub timestamp: u64,
}

/// Custom response
#[derive(Debug, Clone, Default)]
pub struct CustomResponse {
    /// Response type
    pub response_type: String,
    /// Response data
    pub data: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
}

/// Consensus statistics
#[derive(Debug, Clone, Default)]
pub struct ConsensusStats {
    /// Total proposer selections
    pub total_proposer_selections: u64,
    /// Group-based selections
    pub group_based_selections: u64,
    /// Fallback selections
    pub fallback_selections: u64,
    pub successful_validations: u64,
    pub failed_validations: u64,
    pub total_validations: u64,
    pub average_validation_time_ms: f64,
    pub last_validation_time: u64,
    /// Uptime percentage
    pub uptime_percentage: f64,
    /// Memory usage in MB
    pub memory_usage_mb: u64,
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

/// Consensus health metrics
#[derive(Debug, Clone, Default)]
pub struct ConsensusHealthMetrics {
    /// Overall health status
    pub is_healthy: bool,
    pub total_validations: u64,
    pub successful_validations: u64,
    pub failed_validations: u64,
    pub average_validation_time_ms: f64,
    pub last_validation_time: u64,
    /// Uptime percentage
    pub uptime_percentage: f64,
    /// Memory usage in MB
    pub memory_usage_mb: u64,
}

/// Consensus version information
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ConsensusVersion {
    /// Major version
    pub major: u32,
    /// Minor version
    pub minor: u32,
    /// Patch version
    pub patch: u32,
    /// Protocol version
    pub protocol: String,
}

impl ConsensusVersion {
    /// Create a new consensus version
    pub fn new(major: u32, minor: u32, patch: u32, protocol: String) -> Self {
        Self {
            major,
            minor,
            patch,
            protocol,
        }
    }

    /// Check if two versions are compatible
    pub fn is_compatible(&self, other: &ConsensusVersion) -> bool {
        // Same major version required
        if self.major != other.major {
            return false;
        }

        // Same protocol required
        if self.protocol != other.protocol {
            return false;
        }

        // Minor version backward compatibility
        if self.minor < other.minor {
            return false;
        }

        true
    }

    /// Get version string
    pub fn version_string(&self) -> String {
        format!(
            "{}.{}.{}-{}",
            self.major, self.minor, self.patch, self.protocol
        )
    }
}

impl Default for ValidationRequirements {
    fn default() -> Self {
        Self {
            min_signatures: 3,
            supermajority_threshold: 0.67,
            timeout_ms: 5000,
            enable_parallel_validation: false,

            // NUOVI: Valori default per DAG
            enable_dag_validation: false,
            max_dag_branches: 50,
            conflict_resolution_timeout_ms: 500,
        }
    }
}

impl Default for VoteType {
    fn default() -> Self {
        Self::Prevote
    }
}

impl Default for SyncRequestType {
    fn default() -> Self {
        Self::Full
    }
}

impl Default for SyncResponseType {
    fn default() -> Self {
        Self::Full
    }
}
