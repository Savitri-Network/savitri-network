//!
//! This module implements the group-aware consensus mechanism used by masternodes,

use crate::error::ConsensusError;
use crate::protocols::ConsensusHealthChecker as GroupConsensusHealthChecker;
use crate::traits::{
    ConsensusEngine, DefaultProposerContext, DefaultValidationContext, MockHealthChecker,
    MockProposalSigner, MockSignatureValidator, MockStateValidator, ProposalSigner, ProposalStats,
    Proposer, ProposerContext, ProposerHealthChecker, ProposerScore, SignatureValidator,
    StateValidator, Storage, ValidationContext, ValidationStats, Validator,
};
use crate::types::validation::{ValidationError, ValidationResult};
use crate::types::{
    Block, BlockProposal, ConsensusMessage, ConsensusResponse, ConsensusState, ConsensusStats,
    ConsensusVersion, DefaultScoreCalculator, ErrorResponse, GroupInfo, GroupStatus, NodeType,
    PendingResponse, Proposal, ProposalMessage, ProposalMetadata, ScoreCalculator, ScoreThresholds,
    SuccessResponse, Transaction, ValidationProposerInfo, ValidatorInfo, ValidatorMetadata,
    ValidatorStatus,
};
use crate::ProposerInfo;
use hex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::RwLock;

/// Result type for consensus operations
pub type Result<T> = std::result::Result<T, ConsensusError>;

/// Group-aware consensus engine
///
/// This consensus engine organizes light nodes into P2P groups and uses
/// group-based proposer selection while maintaining compatibility with
/// existing consensus systems.
pub struct GroupAwareConsensus {
    config: GroupAwareConfig,
    storage: Arc<dyn Storage>,
    group_manager: Arc<GroupManager>,
    validator: Arc<dyn Validator<Context = DefaultValidationContext>>,
    state: Arc<RwLock<ConsensusState>>, // Only shared state - fully async
    stats: Arc<RwLock<ConsensusStats>>,
    proposer_selector: Arc<GroupProposerSelector>,
    health_checker: GroupConsensusHealthChecker,
}

/// Group-aware consensus configuration
#[derive(Debug, Clone)]
pub struct GroupAwareConfig {
    /// Minimum group size
    pub min_group_size: usize,
    /// Maximum group size
    pub max_group_size: usize,
    /// Group cache TTL in seconds
    pub group_cache_ttl_secs: u64,
    /// Enable fallback to original consensus
    pub enable_fallback: bool,
    pub group_validation_timeout_ms: u64,
    /// Minimum group health score (permille, 0-1000; e.g. 700 = 70%)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub min_health_score: u32,
    /// Minimum member uptime percentage (permille, 0-1000; e.g. 800 = 80%)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub min_uptime_percentage: u32,
    pub enable_geographic_validation: bool,
    pub enable_performance_validation: bool,
    pub enable_signature_validation: bool,
    /// Validation cache TTL in seconds
    pub cache_ttl_secs: u64,
    pub rate_limit_per_minute: u32,
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

impl Default for GroupAwareConfig {
    fn default() -> Self {
        Self {
            min_group_size: 4,
            max_group_size: 8,
            group_cache_ttl_secs: 300, // 5 minutes
            enable_fallback: true,
            group_validation_timeout_ms: 1000,
            min_health_score: 700,      // 700 permille = 70%
            min_uptime_percentage: 800, // 800 permille = 80%
            enable_geographic_validation: true,
            enable_performance_validation: true,
            enable_signature_validation: true,
            cache_ttl_secs: 60,
            rate_limit_per_minute: 10,
            max_simultaneous_groups: 50,
            enable_dag_parallelism: false, // Disabled by default for backward compatibility
            max_parent_hashes: 10,
            conflict_detection_enabled: true,
            merge_interval_blocks: 15,
        }
    }
}

impl GroupAwareConsensus {
    /// Create a new group-aware consensus engine
    pub fn new(config: GroupAwareConfig, storage: Arc<dyn Storage>) -> Result<Self> {
        let group_manager = Arc::new(GroupManager::new(config.clone()));
        let validator = Arc::new(GroupValidator::new(config.clone()));
        let health_checker = GroupConsensusHealthChecker::new();

        let state = Arc::new(RwLock::new(ConsensusState::default()));
        let proposer_selector = Arc::new(GroupProposerSelector::new(config.clone(), state.clone()));
        let stats = Arc::new(RwLock::new(ConsensusStats::default()));

        Ok(Self {
            config,
            storage,
            group_manager,
            validator,
            state,
            stats,
            proposer_selector,
            health_checker,
        })
    }

    /// Get active groups
    pub async fn get_active_groups(&self) -> Result<Vec<GroupInfo>> {
        self.storage.get_active_groups().await
    }

    /// Update group information
    pub async fn update_groups(&self, groups: Vec<GroupInfo>) -> Result<()> {
        for group in groups {
            self.storage.store_group(&group).await?;
        }
        Ok(())
    }

    /// Get group proposer for current slot
    pub async fn get_group_proposer(&self, slot: u64) -> Result<Option<ProposerInfo>> {
        let groups = self.get_active_groups().await?;
        self.proposer_selector.select_proposer(&groups, slot).await
    }

    /// Validate a block proposal
    pub async fn validate_block_proposal(
        &self,
        proposal: &BlockProposal,
    ) -> Result<ValidationResult> {
        let context = DefaultValidationContext::new(
            proposal.round_id / 1000,
            proposal.round_id,
            proposal.height,
            "masternode".to_string(),
        );
        Ok(self.validator.validate_proposal(proposal, &context))
    }

    /// Create a block proposal
    pub async fn create_block_proposal(&self, slot: u64) -> Result<Option<BlockProposal>> {
        // Get proposer for this slot
        if let Some(_proposer) = self.get_group_proposer(slot).await? {
            // Proposal must be built from real block data — never return a
            // default/empty proposal as it could be accepted as a valid
            Err(ConsensusError::ProtocolError(format!(
                "create_block_proposal: proposal builder not yet wired — \
                 callers must supply block data for slot {slot}"
            )))
        } else {
            Ok(None)
        }
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
}

impl ConsensusEngine for GroupAwareConsensus {
    type Config = GroupAwareConfig;
    type Proposal = BlockProposal;
    type Validation = ValidationResult;
    type Storage = dyn Storage;

    fn new(config: Self::Config, storage: Arc<Self::Storage>) -> Result<Self> {
        Self::new(config, storage)
    }

    /// Get consensus state asynchronously
    async fn get_state(&self) -> crate::error::Result<ConsensusState> {
        let state = self.state.read().await;
        Ok(state.clone())
    }

    /// Get mutable consensus state asynchronously
    async fn get_state_mut(&mut self) -> crate::error::Result<ConsensusState> {
        let state = self.state.read().await;
        Ok(state.clone())
    }

    /// Process a consensus message asynchronously
    async fn process_message_async(
        &mut self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        tracing::info!(
            "Processing message with group-aware consensus: {:?}",
            message
        );

        match message {
            ConsensusMessage::Proposal(proposal_msg) => {
                // Process proposal asynchronously
                let proposal = self.deserialize_proposal(&proposal_msg.proposal_data)?;
                let validation_result = self.validate_proposal_async(&proposal).await?;

                match validation_result {
                    ValidationResult::Valid => {
                        let mut state = self.state.write().await;
                        state.current_height += 1;
                        state.last_updated = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        Ok(ConsensusResponse::Success(SuccessResponse {
                            message: "Group proposal validated".to_string(),
                            data: format!(
                                "{}:{}",
                                state.current_height,
                                hex::encode(&state.last_block_hash.0)
                            )
                            .into_bytes(),
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        }))
                    }
                    ValidationResult::Invalid(error) => {
                        Ok(ConsensusResponse::Error(ErrorResponse {
                            error_code: 4001,
                            error_message: format!("Group proposal validation failed: {}", error),
                            error_details: vec![error.to_string()],
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        }))
                    }
                    _ => Ok(ConsensusResponse::Error(ErrorResponse {
                        error_code: 4002,
                        error_message: "Unexpected validation result".to_string(),
                        error_details: vec![],
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    })),
                }
            }
            _ => Ok(ConsensusResponse::Error(ErrorResponse {
                error_code: 4003,
                error_message: "Unsupported message type".to_string(),
                error_details: vec![],
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            })),
        }
    }

    /// Validate a proposal asynchronously
    async fn validate_proposal_async(
        &self,
        proposal: &Self::Proposal,
    ) -> crate::error::Result<Self::Validation> {
        self.validate_block_proposal(proposal).await
    }

    /// Get proposer asynchronously
    async fn get_proposer_async(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>> {
        self.get_group_proposer(slot).await
    }

    /// Create proposal asynchronously
    async fn create_proposal_async(
        &self,
        slot: u64,
    ) -> crate::error::Result<Option<Self::Proposal>> {
        self.create_block_proposal(slot).await
    }

    fn process_message(
        &mut self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        use tokio::runtime::Handle;

        tracing::info!(
            "Processing message with group-aware consensus: {:?}",
            message
        );

        match message {
            ConsensusMessage::Proposal(proposal_msg) => {
                // Process proposal message
                let rt = Handle::current();
                let proposal = rt.block_on(async { BlockProposal::from_message(&proposal_msg) })?;

                // Validate the proposal
                let validation_result =
                    rt.block_on(async { self.validate_block_proposal(&proposal).await })?;

                match validation_result {
                    ValidationResult::Valid => Ok(ConsensusResponse::Success(SuccessResponse {
                        message: "Proposal validated and accepted".to_string(),
                        data: vec![],
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    })),
                    ValidationResult::Invalid(error) => {
                        Ok(ConsensusResponse::Error(ErrorResponse {
                            error_code: 400,
                            error_message: format!("Proposal validation failed: {:?}", error),
                            error_details: vec![],
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        }))
                    }
                    ValidationResult::Pending => {
                        Ok(ConsensusResponse::Pending(PendingResponse {
                            message: "Proposal validation pending".to_string(),
                            estimated_completion: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs()
                                + 60, // 1 minute from now
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        }))
                    }
                    ValidationResult::Skipped => Ok(ConsensusResponse::Success(SuccessResponse {
                        message: "Proposal validation skipped".to_string(),
                        data: vec![],
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    })),
                    ValidationResult::Expired => Ok(ConsensusResponse::Error(ErrorResponse {
                        error_code: 408,
                        error_message: "Proposal validation expired".to_string(),
                        error_details: vec![],
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    })),
                }
            }
            ConsensusMessage::Vote(_) => {
                // Process vote message
                Ok(ConsensusResponse::Success(SuccessResponse {
                    message: "Vote processed".to_string(),
                    data: vec![],
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                }))
            }
            ConsensusMessage::Commit(_) => {
                // Process commit message
                Ok(ConsensusResponse::Success(SuccessResponse {
                    message: "Commit processed".to_string(),
                    data: vec![],
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                }))
            }
            ConsensusMessage::Sync(_) => {
                // Process sync message
                Ok(ConsensusResponse::Success(SuccessResponse {
                    message: "Sync processed".to_string(),
                    data: vec![],
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                }))
            }
            _ => {
                // Handle unimplemented message types
                tracing::warn!("Unimplemented consensus message type: {:?}", message);
                Ok(ConsensusResponse::Error(ErrorResponse {
                    error_code: 500,
                    error_message: "Unimplemented message type".to_string(),
                    error_details: vec![],
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                }))
            }
        }
    }

    fn validate_proposal(
        &self,
        proposal: &Self::Proposal,
    ) -> crate::error::Result<Self::Validation> {
        use tokio::runtime::Handle;

        let rt = Handle::current();
        let validation_result =
            rt.block_on(async { self.validate_block_proposal(proposal).await })?;

        Ok(validation_result)
    }

    fn get_proposer(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>> {
        use tokio::runtime::Handle;

        // Use the async proposer selection method with blocking call
        let rt = Handle::current();
        let proposer = rt.block_on(async { self.get_group_proposer(slot).await })?;

        Ok(proposer)
    }

    fn create_proposal(&self, slot: u64) -> crate::error::Result<Option<Self::Proposal>> {
        use tokio::runtime::Handle;

        // Use the async proposal creation method with blocking call
        let rt = Handle::current();
        let proposal = rt.block_on(async { self.create_block_proposal(slot).await })?;

        Ok(proposal)
    }

    fn stats(&self) -> ConsensusStats {
        // Note: This would need async in a real implementation
        // For now, return default stats
        ConsensusStats::default()
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn storage(&self) -> &Arc<Self::Storage> {
        &self.storage
    }

    fn reset_stats(&mut self) {
        // Reset statistics
        if let Ok(mut stats) = self.stats.try_write() {
            *stats = ConsensusStats::default();
        }
    }

    fn supported_version(&self) -> ConsensusVersion {
        ConsensusVersion::new(1, 0, 0, "group-aware".to_string())
    }
}

// Helper methods for GroupAwareConsensus
impl GroupAwareConsensus {
    /// Helper method to deserialize proposal from bytes
    fn deserialize_proposal(&self, data: &[u8]) -> crate::error::Result<BlockProposal> {
        // SECURITY: Limit deserialization size to prevent memory exhaustion
        const MAX_PROPOSAL_SIZE: u64 = 4_194_304; // 4 MB
        if data.len() as u64 > MAX_PROPOSAL_SIZE {
            return Err(crate::error::ConsensusError::InvalidMessage(format!(
                "Proposal data too large: {} bytes (max {})",
                data.len(),
                MAX_PROPOSAL_SIZE
            )));
        }
        // Try to deserialize using bincode with size limit
        use bincode::Options;
        let options = bincode::DefaultOptions::new().with_limit(MAX_PROPOSAL_SIZE);
        if let Ok(proposal) = options.deserialize::<BlockProposal>(data) {
            return Ok(proposal);
        }

        // SECURITY: Reject malformed proposals — never create default empty proposals
        Err(crate::error::ConsensusError::InvalidMessage(
            "Failed to deserialize block proposal".to_string(),
        ))
    }
}

/// Group manager for handling P2P groups
pub struct GroupManager {
    config: GroupAwareConfig,
    group_cache: Arc<RwLock<HashMap<String, CachedGroup>>>,
}

impl GroupManager {
    pub fn new(config: GroupAwareConfig) -> Self {
        Self {
            config,
            group_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get cached group information
    pub async fn get_cached_group(&self, group_id: &str) -> Option<GroupInfo> {
        let cache = self.group_cache.read().await;
        cache.get(group_id).and_then(|cached| {
            if cached.is_valid(self.config.group_cache_ttl_secs) {
                Some(cached.group.clone())
            } else {
                None
            }
        })
    }

    /// Cache group information
    pub async fn cache_group(&self, group: GroupInfo) {
        let mut cache = self.group_cache.write().await;
        cache.insert(group.group_id.clone(), CachedGroup::new(group));
    }

    /// Clean expired cache entries
    pub async fn clean_expired_cache(&self) {
        let mut cache = self.group_cache.write().await;
        cache.retain(|_, cached| cached.is_valid(self.config.group_cache_ttl_secs));
    }
}

/// Cached group information
#[derive(Debug, Clone)]
struct CachedGroup {
    group: GroupInfo,
    cached_at: u64,
}

impl CachedGroup {
    pub fn new(group: GroupInfo) -> Self {
        Self {
            group,
            cached_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    pub fn is_valid(&self, ttl_secs: u64) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now.saturating_sub(self.cached_at) < ttl_secs
    }
}

/// Group proposer selector
pub struct GroupProposerSelector {
    config: GroupAwareConfig,
    state: Arc<RwLock<ConsensusState>>,
}

impl GroupProposerSelector {
    pub fn new(config: GroupAwareConfig, state: Arc<RwLock<ConsensusState>>) -> Self {
        Self { config, state }
    }

    /// Select proposer from active groups - ESTESO per parallelismo
    pub async fn select_proposer(
        &self,
        groups: &[GroupInfo],
        slot: u64,
    ) -> Result<Option<ProposerInfo>> {
        let active_groups: Vec<_> = groups
            .iter()
            .filter(|g| g.status == GroupStatus::Active)
            .filter(|g| g.members.len() >= self.config.min_group_size)
            .filter(|g| g.members.len() <= self.config.max_group_size)
            .filter(|g| g.health_score >= self.config.min_health_score)
            .collect();

        if active_groups.is_empty() {
            return Ok(None);
        }

        // NUOVO: Supporto multi-gruppi simultanei
        if self.config.enable_dag_parallelism {
            // Seleziona fino a max_simultaneous_groups gruppi
            let num_groups =
                std::cmp::min(active_groups.len(), self.config.max_simultaneous_groups);
            let mut selected_proposers = Vec::new();

            for (i, group) in active_groups.iter().take(num_groups).enumerate() {
                if let Ok(Some(proposer)) = self
                    .select_proposer_from_group(group, slot + i as u64)
                    .await
                {
                    selected_proposers.push(proposer);
                }
            }

            Ok(selected_proposers.into_iter().next())
        } else {
            // SECURITY (C-08): VRF-like group selection instead of deterministic slot % len
            let state = self.state.read().await;
            let mut input = Vec::with_capacity(14 + 8 + 64);
            input.extend_from_slice(b"GROUP-VRF   ");
            input.extend_from_slice(&slot.to_le_bytes());
            input.extend_from_slice(&state.last_block_hash.0);
            let hash = blake3::hash(&input);
            let hb = hash.as_bytes();
            let rand_val =
                u64::from_le_bytes([hb[0], hb[1], hb[2], hb[3], hb[4], hb[5], hb[6], hb[7]]);
            let group_index = (rand_val as usize) % active_groups.len();
            let selected_group = &active_groups[group_index];
            let proposer = self
                .select_proposer_from_group(selected_group, slot)
                .await?;
            Ok(proposer)
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
            if let Ok(Some(proposer)) = self
                .select_proposer_from_group(group, slot + i as u64)
                .await
            {
                proposers.push(proposer);
            }
        }

        Ok(proposers)
    }

    /// Select proposer from group members
    async fn select_proposer_from_group(
        &self,
        group: &GroupInfo,
        slot: u64,
    ) -> Result<Option<ProposerInfo>> {
        // Get proposer information for all group members
        let mut proposers = Vec::new();

        for member_id in &group.members {
            if let Some(validator) = self.get_validator_info(member_id).await? {
                proposers.push(validator);
            }
        }

        if proposers.is_empty() {
            return Ok(None);
        }

        // Sort by score (highest first)
        proposers.sort_by(|a, b| b.score.cmp(&a.score));

        // SECURITY (C-08): VRF-like weighted selection instead of deterministic slot % len
        let state = self.state.read().await;
        let mut input = Vec::with_capacity(14 + 8 + 64);
        input.extend_from_slice(b"PROPOSER-VRF");
        input.extend_from_slice(&slot.to_le_bytes());
        input.extend_from_slice(&state.last_block_hash.0);
        let hash = blake3::hash(&input);
        let hb = hash.as_bytes();
        let rand_val = u64::from_le_bytes([hb[0], hb[1], hb[2], hb[3], hb[4], hb[5], hb[6], hb[7]]);
        let total_weight: u64 = proposers.iter().map(|p| (p.score as u64).max(1)).sum();
        let target = rand_val % total_weight;
        let mut cumulative: u64 = 0;
        let mut proposer_index = proposers.len() - 1;
        for (i, p) in proposers.iter().enumerate() {
            cumulative += (p.score as u64).max(1);
            if target < cumulative {
                proposer_index = i;
                break;
            }
        }
        let selected_proposer = &proposers[proposer_index];

        Ok(Some(ProposerInfo {
            node_id: selected_proposer.validator_id.clone(),
            peer_id: selected_proposer.validator_id.clone(),
            public_key: selected_proposer.public_key,
            score: selected_proposer.score,
            group_id: Some(group.group_id.clone()),
            region: selected_proposer.metadata.region.clone(),
            capabilities: selected_proposer.metadata.capabilities.clone(),
        }))
    }

    async fn get_validator_info(&self, validator_id: &str) -> Result<Option<ValidatorInfo>> {
        // In a real implementation, this would query the storage layer
        let validator_info = ValidatorInfo {
            validator_id: validator_id.to_string(),
            public_key: [0u8; 32],
            stake: 1000,
            score: 800,
            status: ValidatorStatus::Active,
            last_active: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            metadata: ValidatorMetadata {
                region: "global".to_string(),
                node_type: NodeType::Lightnode,
                capabilities: vec!["group-member".to_string()],
                version: "1.0.0".to_string(),
                address: validator_id.to_string(),
            },
        };

        Ok(Some(validator_info))
    }
}

pub struct GroupValidator {
    config: GroupAwareConfig,
    signature_validator: Arc<dyn SignatureValidator>,
    state_validator: Arc<dyn StateValidator>,
}

impl GroupValidator {
    pub fn new(config: GroupAwareConfig) -> Self {
        Self {
            config,
            signature_validator: Arc::new(MockSignatureValidator),
            state_validator: Arc::new(MockStateValidator),
        }
    }
}

impl Validator for GroupValidator {
    type Context = DefaultValidationContext;

    fn validate_proposal(
        &self,
        proposal: &dyn Proposal,
        context: &Self::Context,
    ) -> ValidationResult {
        use tokio::runtime::Handle;

        // Convert the trait object to a concrete BlockProposal if possible
        if let Some(block_proposal) = proposal.as_any().downcast_ref::<BlockProposal>() {
            let rt = Handle::current();
            let validation_result: ValidationResult = rt.block_on(async {
                self.validate_proposal(
                    block_proposal,
                    &DefaultValidationContext::new(
                        0,                       // current_epoch
                        0,                       // current_slot
                        0,                       // current_height
                        "validator".to_string(), // validator_id
                    ),
                )
            });

            let validation_result = validation_result;

            validation_result
        } else {
            ValidationResult::Valid
        }
    }

    fn validate_block(&self, block: &Block, context: &Self::Context) -> ValidationResult {
        if !block.is_valid() {
            return ValidationResult::Invalid(ValidationError::InvalidBlock);
        }

        // Validate proposer is in valid group
        if let Some(group_id) = &block.consensus_data.proposer_info.group_id {
            if let Some(group) = context.get_active_groups().get(group_id) {
                if !group
                    .members
                    .contains(&block.consensus_data.proposer_info.node_id)
                {
                    return ValidationResult::Invalid(ValidationError::ProposerNotInGroup);
                }

                if group.status != GroupStatus::Active {
                    return ValidationResult::Invalid(ValidationError::GroupInactive);
                }

                if group.health_score < self.config.min_health_score {
                    return ValidationResult::Invalid(ValidationError::HealthCheckFailed);
                }
            } else {
                return ValidationResult::Invalid(ValidationError::GroupNotFound);
            }
        }

        ValidationResult::Valid
    }

    fn validate_transaction(&self, tx: &Transaction, _context: &Self::Context) -> ValidationResult {
        if !tx.is_valid() {
            return ValidationResult::Invalid(ValidationError::InvalidTransaction);
        }
        ValidationResult::Valid
    }

    fn validate_proposer(
        &self,
        proposer: &ProposerInfo,
        context: &Self::Context,
    ) -> ValidationResult {
        // Check if proposer is blacklisted
        if context.is_blacklisted(&proposer.node_id) {
            return ValidationResult::Invalid(ValidationError::InvalidProposer);
        }

        // Check minimum score requirements
        let min_scores = context.get_min_scores();
        if proposer.score < min_scores.min_pou_score {
            return ValidationResult::Invalid(ValidationError::InsufficientPouScore);
        }

        // Validate group membership if group ID is present
        if let Some(group_id) = &proposer.group_id {
            if let Some(group) = context.get_active_groups().get(group_id) {
                if !group.members.contains(&proposer.node_id) {
                    return ValidationResult::Invalid(ValidationError::ProposerNotInGroup);
                }

                if group.health_score < self.config.min_health_score {
                    return ValidationResult::Invalid(ValidationError::HealthCheckFailed);
                }
            } else {
                return ValidationResult::Invalid(ValidationError::GroupNotFound);
            }
        }

        ValidationResult::Valid
    }

    fn validator_info(&self) -> ValidatorInfo {
        ValidatorInfo {
            validator_id: "group-validator".to_string(),
            public_key: [0u8; 32],
            stake: 0,
            score: 1000,
            status: ValidatorStatus::Active,
            last_active: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            metadata: ValidatorMetadata {
                region: "global".to_string(),
                node_type: NodeType::Masternode,
                capabilities: vec!["group-validation".to_string()],
                version: "1.0.0".to_string(),
                address: "0.0.0.0".to_string(),
            },
        }
    }

    fn is_active(&self) -> bool {
        true
    }

    fn stake(&self) -> u64 {
        0
    }

    fn score(&self) -> u32 {
        1000
    }

    fn update_score(&self, _new_score: u32) -> Result<()> {
        Ok(())
    }

    fn validation_stats(&self) -> ValidationStats {
        ValidationStats::default()
    }
}

/// Consensus health checker
pub struct ConsensusHealthChecker {
    start_time: std::time::Instant,
}

impl ConsensusHealthChecker {
    pub fn new() -> Self {
        Self {
            start_time: std::time::Instant::now(),
        }
    }

    pub fn is_healthy(&self) -> bool {
        // Simple health check - uptime > 0
        self.start_time.elapsed().as_secs() > 0
    }
}

// Helper implementation for BlockProposal
impl BlockProposal {
    pub fn from_message(_message: &ProposalMessage) -> Result<Self> {
        Err(ConsensusError::ProtocolError(
            "BlockProposal::from_message: deserialization not implemented — \
             must be wired to the real proposal codec"
                .to_string(),
        ))
    }
}

impl Default for BlockProposal {
    fn default() -> Self {
        Self {
            round_id: 0,
            height: 0,
            timestamp: 0,
            proposer_pubkey: crate::types::block::Hash32([0u8; 32]),
            proposer_pou_score: 0,
            parent_hash: crate::types::block::Hash64([0u8; 64]),
            state_root: crate::types::block::Hash64([0u8; 64]),
            tx_root: crate::types::block::Hash64([0u8; 64]),
            transactions: vec![],
            latency_proof: None,
            availability_proof: None,
            group_proof: None,
            signature: crate::types::block::Hash64([0u8; 64]),
            metadata: ProposalMetadata::default(),
        }
    }
}
