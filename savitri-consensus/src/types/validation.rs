//! Validation types and structures
//!
//! all consensus implementations.

use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

/// Validation result status
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationResult {
    /// Validation passed
    Valid,
    /// Validation failed with specific error
    Invalid(ValidationError),
    /// Validation pending (async check in progress)
    Pending,
    /// Validation skipped (disabled or not applicable)
    Skipped,
    /// Validation expired (timeout)
    Expired,
}

/// Validation error types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ValidationError {
    /// Block not found
    BlockNotFound,
    /// Invalid block structure
    InvalidBlock,
    /// Invalid transaction
    InvalidTransaction,
    /// Invalid state transition
    InvalidStateTransition,
    /// Invalid proposal
    InvalidProposal,
    /// Invalid proposer
    InvalidProposer,
    /// Invalid signature
    InvalidSignature,
    /// Invalid timestamp
    InvalidTimestamp,
    /// Invalid parent hash
    InvalidParentHash,
    /// Invalid state root
    InvalidStateRoot,
    /// Invalid transaction root
    InvalidTxRoot,
    /// Invalid consensus data
    InvalidConsensusData,
    /// Invalid structure
    InvalidStructure(String),
    /// Group not found
    GroupNotFound,
    /// Group inactive
    GroupInactive,
    /// Insufficient members
    InsufficientMembers,
    /// Proposer not in group
    ProposerNotInGroup,
    /// Health check failed
    HealthCheckFailed,
    /// Performance below threshold
    PerformanceBelowThreshold,
    /// Insufficient PoU score
    InsufficientPouScore,
    /// Signature verification failed
    SignatureVerificationFailed,
    /// Epoch mismatch
    EpochMismatch,
    /// Geographic constraint violation
    GeographicConstraintViolation,
    /// Rate limit exceeded
    RateLimitExceeded,
    /// Validation timeout
    ValidationTimeout,
    /// Network error
    NetworkError(String),
    /// Storage error
    StorageError(String),
    /// Serialization error
    SerializationError(String),
    /// Custom error
    Custom(String),
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValidationError::BlockNotFound => write!(f, "Block not found"),
            ValidationError::InvalidBlock => write!(f, "Invalid block structure"),
            ValidationError::InvalidTransaction => write!(f, "Invalid transaction"),
            ValidationError::InvalidStateTransition => write!(f, "Invalid state transition"),
            ValidationError::InvalidProposal => write!(f, "Invalid proposal"),
            ValidationError::InvalidProposer => write!(f, "Invalid proposer"),
            ValidationError::InvalidSignature => write!(f, "Invalid signature"),
            ValidationError::InvalidTimestamp => write!(f, "Invalid timestamp"),
            ValidationError::InvalidParentHash => write!(f, "Invalid parent hash"),
            ValidationError::InvalidStateRoot => write!(f, "Invalid state root"),
            ValidationError::InvalidTxRoot => write!(f, "Invalid transaction root"),
            ValidationError::InvalidConsensusData => write!(f, "Invalid consensus data"),
            ValidationError::InvalidStructure(msg) => write!(f, "Invalid structure: {}", msg),
            ValidationError::GroupNotFound => write!(f, "Group not found"),
            ValidationError::ProposerNotInGroup => write!(f, "Proposer not in group"),
            ValidationError::GroupInactive => write!(f, "Group inactive"),
            ValidationError::HealthCheckFailed => write!(f, "Health check failed"),
            ValidationError::ValidationTimeout => write!(f, "Validation timeout"),
            ValidationError::InsufficientPouScore => write!(f, "Insufficient PoU score"),
            ValidationError::GeographicConstraintViolation => {
                write!(f, "Geographic constraint violation")
            }
            ValidationError::InsufficientMembers => write!(f, "Insufficient members"),
            ValidationError::PerformanceBelowThreshold => write!(f, "Performance below threshold"),
            ValidationError::SignatureVerificationFailed => {
                write!(f, "Signature verification failed")
            }
            ValidationError::EpochMismatch => write!(f, "Epoch mismatch"),
            ValidationError::RateLimitExceeded => write!(f, "Rate limit exceeded"),
            ValidationError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            ValidationError::StorageError(msg) => write!(f, "Storage error: {}", msg),
            ValidationError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            ValidationError::Custom(msg) => write!(f, "Custom error: {}", msg),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct ValidationContext {
    /// Current epoch
    pub current_epoch: u64,
    /// Current slot
    pub current_slot: u64,
    /// Current block height
    pub current_height: u64,
    /// Validator ID
    pub validator_id: String,
    /// Validation timestamp
    pub validation_timestamp: u64,
    /// Strict mode enabled
    pub strict_mode: bool,
    pub max_validation_time_ms: u64,
    /// Additional context data
    pub extra_data: ValidationContextData,
}

#[derive(Debug, Clone, Default)]
pub struct ValidationContextData {
    pub active_groups: std::collections::HashMap<String, GroupInfo>,
    /// Known proposers
    pub known_proposers: std::collections::HashMap<String, ValidationProposerInfo>,
    /// Blacklisted nodes
    pub blacklisted_nodes: std::collections::HashSet<String>,
    /// Minimum required scores
    pub min_scores: ScoreThresholds,
    /// Geographic constraints
    pub geographic_constraints: GeographicConstraints,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GroupInfo {
    /// Group ID
    pub group_id: String,
    /// Group members
    pub members: Vec<String>,
    /// Current proposer
    pub proposer: Option<String>,
    /// Group health score in permille (0–1000). AUDIT-003.
    pub health_score: u32,
    /// Group epoch
    pub epoch: u64,
    /// Group status
    pub status: GroupStatus,
}

/// Group status enumeration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum GroupStatus {
    /// Group is forming
    #[default]
    Forming,
    /// Group is active
    Active,
    /// Group is inactive
    Inactive,
    /// Group is dissolving
    Dissolving,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ValidationProposerInfo {
    /// Proposer node ID
    pub node_id: String,
    /// Proposer peer ID
    pub peer_id: String,
    /// Proposer's public key
    pub proposer_pubkey: [u8; 32],
    /// Proposer's score
    pub score: u32,
    /// Proposer's group ID
    pub group_id: Option<String>,
    /// Proposer's region
    pub region: String,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Uptime percentage
    pub uptime_percentage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ScoreThresholds {
    /// Minimum PoU score
    pub min_pou_score: u32,
    /// Minimum health score
    pub min_health_score: f64,
    /// Minimum uptime percentage
    pub min_uptime_percentage: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GeographicConstraints {
    pub enabled: bool,
    /// Allowed regions
    pub allowed_regions: Vec<String>,
    /// Maximum distance in kilometers
    pub max_distance_km: Option<f64>,
}

/// Validation metrics for monitoring
#[derive(Debug, Clone, Default)]
pub struct ValidationMetrics {
    pub total_validations: u64,
    pub successful_validations: u64,
    pub failed_validations: u64,
    pub skipped_validations: u64,
    pub average_validation_time_ms: f64,
    /// Cache hits
    pub cache_hits: u64,
    /// Cache misses
    pub cache_misses: u64,
    pub last_validation_timestamp: u64,
}

/// Validation configuration
#[derive(Debug, Clone)]
pub struct ValidationConfig {
    pub strict_mode: bool,
    pub max_validation_time_ms: u64,
    /// Enable caching
    pub enable_caching: bool,
    /// Cache TTL in seconds
    pub cache_ttl_secs: u64,
    pub enable_parallel_validation: bool,
    pub max_parallel_validations: usize,
    /// Enable timeout
    pub enable_timeout: bool,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
}

impl Default for ValidationConfig {
    fn default() -> Self {
        Self {
            strict_mode: false,
            max_validation_time_ms: 5000,
            enable_caching: true,
            cache_ttl_secs: 300,
            enable_parallel_validation: true,
            max_parallel_validations: 10,
            enable_timeout: true,
            timeout_ms: 5000,
        }
    }
}

impl ValidationResult {
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }

    pub fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid(_))
    }

    pub fn is_pending(&self) -> bool {
        matches!(self, Self::Pending)
    }

    pub fn is_skipped(&self) -> bool {
        matches!(self, Self::Skipped)
    }

    pub fn is_expired(&self) -> bool {
        matches!(self, Self::Expired)
    }

    pub fn error(&self) -> Option<&ValidationError> {
        match self {
            Self::Invalid(error) => Some(error),
            _ => None,
        }
    }
}

impl ValidationError {
    pub fn error_code(&self) -> u32 {
        match self {
            Self::BlockNotFound => 1001,
            Self::InvalidBlock => 1002,
            Self::InvalidTransaction => 1003,
            Self::InvalidStateTransition => 1003,
            Self::InvalidProposal => 1004,
            Self::InvalidProposer => 1005,
            Self::InvalidSignature => 1005,
            Self::InvalidTimestamp => 1006,
            Self::InvalidParentHash => 1007,
            Self::InvalidStateRoot => 1008,
            Self::InvalidTxRoot => 1009,
            Self::InvalidConsensusData => 1010,
            Self::InvalidStructure(_) => 1011,
            Self::GroupNotFound => 2001,
            Self::GroupInactive => 2002,
            Self::InsufficientMembers => 2003,
            Self::ProposerNotInGroup => 2004,
            Self::HealthCheckFailed => 2005,
            Self::PerformanceBelowThreshold => 2006,
            Self::InsufficientPouScore => 2007,
            Self::SignatureVerificationFailed => 2008,
            Self::EpochMismatch => 2009,
            Self::GeographicConstraintViolation => 2010,
            Self::RateLimitExceeded => 2011,
            Self::ValidationTimeout => 2012,
            Self::NetworkError(_) => 3001,
            Self::StorageError(_) => 3002,
            Self::SerializationError(_) => 3003,
            Self::Custom(_) => 9999,
        }
    }

    /// Get error message
    pub fn error_message(&self) -> String {
        match self {
            Self::NetworkError(msg) => format!("Network error: {}", msg),
            Self::StorageError(msg) => format!("Storage error: {}", msg),
            Self::SerializationError(msg) => format!("Serialization error: {}", msg),
            Self::Custom(msg) => format!("Custom error: {}", msg),
            Self::InvalidStructure(msg) => format!("Invalid structure: {}", msg),
            _ => format!("{:?}", self),
        }
    }
}

impl ValidationContext {
    pub fn new(
        current_epoch: u64,
        current_slot: u64,
        current_height: u64,
        validator_id: String,
    ) -> Self {
        Self {
            current_epoch,
            current_slot,
            current_height,
            validator_id,
            validation_timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            strict_mode: false,
            max_validation_time_ms: 5000,
            extra_data: ValidationContextData::default(),
        }
    }

    pub fn is_strict(&self) -> bool {
        self.strict_mode
    }

    pub fn is_timeout(&self) -> bool {
        let elapsed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.validation_timestamp);
        elapsed > self.max_validation_time_ms / 1000
    }

    /// Get group information for a proposer
    pub fn get_group_info(&self, proposer_id: &str) -> Option<&GroupInfo> {
        self.extra_data.active_groups.values().find(|group| {
            group.members.contains(&proposer_id.to_string())
                || group.proposer.as_ref() == Some(&proposer_id.to_string())
        })
    }

    /// Check if a proposer is blacklisted
    pub fn is_blacklisted(&self, proposer_id: &str) -> bool {
        self.extra_data.blacklisted_nodes.contains(proposer_id)
    }
}

impl ValidationMetrics {
    pub fn record_success(&mut self, duration_ms: f64) {
        self.total_validations += 1;
        self.successful_validations += 1;
        self.update_average_time(duration_ms);
        self.last_validation_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    pub fn record_failure(&mut self, duration_ms: f64) {
        self.total_validations += 1;
        self.failed_validations += 1;
        self.update_average_time(duration_ms);
        self.last_validation_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    pub fn record_skip(&mut self) {
        self.total_validations += 1;
        self.skipped_validations += 1;
    }

    /// Record a cache hit
    pub fn record_cache_hit(&mut self) {
        self.cache_hits += 1;
    }

    /// Record a cache miss
    pub fn record_cache_miss(&mut self) {
        self.cache_misses += 1;
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        if self.total_validations == 0 {
            0.0
        } else {
            self.successful_validations as f64 / self.total_validations as f64
        }
    }

    /// Get cache hit rate
    pub fn cache_hit_rate(&self) -> f64 {
        let total_cache_requests = self.cache_hits + self.cache_misses;
        if total_cache_requests == 0 {
            0.0
        } else {
            self.cache_hits as f64 / total_cache_requests as f64
        }
    }

    fn update_average_time(&mut self, duration_ms: f64) {
        if self.total_validations == 1 {
            self.average_validation_time_ms = duration_ms;
        } else {
            let total_time = self.average_validation_time_ms * (self.total_validations - 1) as f64;
            self.average_validation_time_ms =
                (total_time + duration_ms) / self.total_validations as f64;
        }
    }
}

pub trait Validation: Send + Sync {
    fn result(&self) -> &ValidationResult;

    fn timestamp(&self) -> u64;

    fn validator_id(&self) -> &str;

    fn is_valid(&self) -> bool {
        self.result().is_valid()
    }

    fn score(&self) -> u32;

    fn details(&self) -> Vec<String>;
}

/// Blanket implementation for ValidationResult
impl Validation for ValidationResult {
    fn result(&self) -> &ValidationResult {
        self
    }

    fn timestamp(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    fn validator_id(&self) -> &str {
        "unknown"
    }

    fn score(&self) -> u32 {
        match self {
            Self::Valid => 10000,
            Self::Invalid(_) => 0,
            Self::Pending => 5000,
            Self::Skipped => 7500,
            Self::Expired => 0,
        }
    }

    fn details(&self) -> Vec<String> {
        match self {
            Self::Valid => vec!["Validation passed".to_string()],
            Self::Invalid(error) => vec![error.error_message()],
            Self::Pending => vec!["Validation pending".to_string()],
            Self::Skipped => vec!["Validation skipped".to_string()],
            Self::Expired => vec!["Validation expired".to_string()],
        }
    }
}

// ============================================================================
// VALIDATION REQUIREMENTS TESTS
// ============================================================================

#[derive(Debug, Clone, Default)]
pub struct DagMetrics {
    pub block_count: u64,
    pub conflict_count: u64,
    pub branch_count: u64,
    pub depth: u64,
    pub width: u64,
}

impl DagMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.block_count == 0 {
            return Err("Block count cannot be zero".to_string());
        }
        if self.depth == 0 {
            return Err("Depth cannot be zero".to_string());
        }
        Ok(())
    }
}
