//! DAG Types and Structures
//!
//! This module defines the core types used for DAG management, designed to work
//! seamlessly with existing consensus structures.

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// DAG configuration parameters
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAGConfig {
    /// Maximum number of simultaneous branches
    pub max_branches: usize,
    /// Conflict detection configuration
    pub conflict_config: ConflictConfig,
    /// Branch timeout in seconds
    pub branch_timeout_secs: u64,
    /// Enable automatic conflict resolution
    pub enable_auto_resolution: bool,
}

impl Default for DAGConfig {
    fn default() -> Self {
        Self {
            max_branches: 50,
            conflict_config: ConflictConfig::default(),
            branch_timeout_secs: 3600, // 1 hour
            enable_auto_resolution: false,
        }
    }
}

/// Conflict detection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictConfig {
    /// Enable transaction conflict detection
    pub enable_transaction_conflicts: bool,
    /// Enable state conflict detection
    pub enable_state_conflicts: bool,
    /// Maximum conflicts to track
    pub max_tracked_conflicts: usize,
    /// Conflict resolution timeout in milliseconds
    pub resolution_timeout_ms: u64,
}

impl Default for ConflictConfig {
    fn default() -> Self {
        Self {
            enable_transaction_conflicts: true,
            enable_state_conflicts: true,
            max_tracked_conflicts: 1000,
            resolution_timeout_ms: 5000,
        }
    }
}

/// Branch information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BranchInfo {
    /// Unique branch identifier
    pub id: String,
    /// Parent hashes that define this branch
    pub parent_hashes: Vec<Vec<u8>>,
    /// Creation timestamp
    pub created_at: SystemTime,
    /// Last activity timestamp
    pub last_activity: SystemTime,
    /// Current branch status
    pub status: BranchStatus,
    /// Number of blocks in this branch
    pub block_count: u64,
    /// Branch depth in the DAG
    pub depth: u64,
}

/// Branch status enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum BranchStatus {
    /// Branch is active and accepting blocks
    Active,
    /// Branch is being merged
    Merging,
    /// Branch has been merged and is inactive
    Merged,
    /// Branch is abandoned due to conflicts
    Abandoned,
    /// Branch is paused temporarily
    Paused,
}

impl Default for BranchStatus {
    fn default() -> Self {
        Self::Active
    }
}

/// Conflict information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Conflict {
    /// Unique conflict identifier
    pub id: String,
    /// Type of conflict
    pub conflict_type: ConflictType,
    /// Branches involved in the conflict
    pub branches: Vec<String>,
    /// Blocks involved in the conflict
    pub blocks: Vec<Vec<u8>>,
    /// Conflict detection timestamp
    pub detected_at: SystemTime,
    /// Conflict severity level
    pub severity: ConflictSeverity,
    /// Optional resolution strategy
    pub resolution: Option<ConflictResolution>,
}

/// Conflict type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ConflictType {
    /// Transaction conflicts (double spend, etc.)
    Transaction,
    /// State conflicts (different state roots)
    State,
    /// Parent hash conflicts
    ParentHash,
    /// Timestamp conflicts
    Timestamp,
    /// Unknown conflict type
    Unknown,
}

/// Conflict severity levels
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, Hash)]
pub enum ConflictSeverity {
    /// Low severity (minor conflicts)
    Low,
    /// Medium severity (requires attention)
    Medium,
    /// High severity (critical conflicts)
    High,
    /// Critical severity (requires immediate action)
    Critical,
}

/// Conflict resolution strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConflictResolution {
    /// Resolution strategy used
    pub strategy: ResolutionStrategy,
    /// Timestamp when resolution was applied
    pub resolved_at: SystemTime,
    /// Branch that was chosen as the winner
    pub winning_branch: String,
    /// Branches that were abandoned
    pub abandoned_branches: Vec<String>,
}

/// Resolution strategies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResolutionStrategy {
    /// Choose the branch with more proof of work
    LongestChain,
    /// Choose the branch with higher score
    HighestScore,
    /// Choose the branch created first
    FirstSeen,
    /// Manual resolution required
    Manual,
    /// Random selection (for testing)
    Random,
}

/// DAG statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DAGStats {
    /// Total number of branches
    pub total_branches: usize,
    /// Number of active branches
    pub active_branches: usize,
    /// Number of merged branches
    pub merged_branches: usize,
    /// Number of abandoned branches
    pub abandoned_branches: usize,
    /// Total number of conflicts detected
    pub total_conflicts: usize,
    /// Number of resolved conflicts
    pub resolved_conflicts: usize,
    /// Average branch depth
    pub avg_branch_depth: f64,
    /// Maximum branch depth
    pub max_branch_depth: u64,
    /// Last update timestamp
    pub last_updated: SystemTime,
}

impl Default for DAGStats {
    fn default() -> Self {
        Self {
            total_branches: 0,
            active_branches: 0,
            merged_branches: 0,
            abandoned_branches: 0,
            total_conflicts: 0,
            resolved_conflicts: 0,
            avg_branch_depth: 0.0,
            max_branch_depth: 0,
            last_updated: SystemTime::now(),
        }
    }
}

/// Result type for DAG operations
pub type DAGResult<T> = Result<T, DAGError>;

/// DAG-specific errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum DAGError {
    #[error("Branch not found: {0}")]
    BranchNotFound(String),

    #[error("Conflict detected: {0}")]
    ConflictDetected(String),

    #[error("Maximum branches exceeded: {0}")]
    MaxBranchesExceeded(usize),

    #[error("Invalid branch configuration: {0}")]
    InvalidBranchConfig(String),

    #[error("Conflict resolution failed: {0}")]
    ResolutionFailed(String),

    #[error("DAG validation error: {0}")]
    ValidationError(String),

    #[error("Internal DAG error: {0}")]
    InternalError(String),
}

/// Utility functions for DAG operations
pub mod utils {
    use super::*;

    /// Generate a unique branch ID
    pub fn generate_branch_id() -> String {
        format!("branch_{}", uuid::Uuid::new_v4())
    }

    /// Generate a unique conflict ID
    pub fn generate_conflict_id() -> String {
        format!("conflict_{}", uuid::Uuid::new_v4())
    }

    /// Calculate hash for a set of parent hashes
    pub fn calculate_branch_hash(parent_hashes: &[Vec<u8>]) -> [u8; 64] {
        use blake3;

        if parent_hashes.is_empty() {
            return [0u8; 64];
        }

        let mut hasher = blake3::Hasher::new();
        for hash in parent_hashes {
            hasher.update(hash);
        }

        let hash = hasher.finalize();
        let mut result = [0u8; 64];
        result[..32].copy_from_slice(hash.as_bytes());
        result
    }

    /// Validate parent hashes format
    pub fn validate_parent_hashes(hashes: &[Vec<u8>]) -> DAGResult<()> {
        if hashes.is_empty() {
            return Err(DAGError::InvalidBranchConfig(
                "At least one parent hash required".to_string(),
            ));
        }

        if hashes.len() > 50 {
            return Err(DAGError::InvalidBranchConfig(
                "Too many parent hashes (max 50)".to_string(),
            ));
        }

        // Check hash length
        for hash in hashes {
            if hash.len() != 64 {
                return Err(DAGError::InvalidBranchConfig(
                    "Parent hash must be 64 bytes".to_string(),
                ));
            }
        }

        // Check for duplicates
        let mut seen = std::collections::HashSet::new();
        for hash in hashes {
            let hash_vec = hash.clone();
            if !seen.insert(hash_vec) {
                return Err(DAGError::InvalidBranchConfig(
                    "Duplicate parent hashes detected".to_string(),
                ));
            }
        }

        Ok(())
    }
}
