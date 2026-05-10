//! Hybrid consensus implementation
//!
//! This module implements a hybrid consensus mechanism that combines
//! group-aware and PoU-based consensus for enhanced reliability and performance.

use crate::error::ConsensusError;
use crate::protocols::{GroupAwareConsensus, PouBasedConsensus};
use crate::traits::ValidationStats;
use crate::traits::*;
use crate::types::*;
use crate::types::{
    BlockProposal, ConsensusMessage, ConsensusResponse, ConsensusState, ConsensusStats,
    ConsensusVersion, ErrorResponse, PendingResponse, Proposal, SuccessResponse,
};
use crate::ProposerInfo;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Hybrid consensus engine combining multiple consensus mechanisms
pub struct HybridConsensus {
    config: HybridConfig,
    group_consensus: Arc<GroupAwareConsensus>,
    pou_consensus: Arc<PouBasedConsensus>,
    fallback_strategy: Arc<FallbackStrategy>,
    state: Arc<RwLock<HybridState>>,
    local_state: ConsensusState, // Local state for synchronous access
    stats: Arc<RwLock<HybridStats>>,
    health_checker: Arc<HybridHealthChecker>,
}

/// Hybrid consensus configuration
#[derive(Debug, Clone)]
pub struct HybridConfig {
    /// Primary consensus type
    pub primary_consensus: ConsensusType,
    /// Secondary consensus type (fallback)
    pub secondary_consensus: ConsensusType,
    /// Enable automatic fallback
    pub enable_fallback: bool,
    /// Fallback threshold in permille (failure rate before switching).
    /// AUDIT-003: Replaced f64 with integer permille.
    pub fallback_threshold_permille: u32,
    /// Health check interval in seconds
    pub health_check_interval_secs: u64,
    /// Consensus switch cooldown in seconds
    pub switch_cooldown_secs: u64,
    /// Enable consensus blending
    pub enable_blending: bool,
    /// Blend weights in permille (primary, secondary). Must sum to 1000.
    /// AUDIT-003: Replaced f64 with integer permille.
    pub blend_weights_permille: (u32, u32),
    /// Minimum agreement threshold in permille for consensus.
    /// AUDIT-003: Replaced f64 with integer permille.
    pub agreement_threshold_permille: u32,
}

impl Default for HybridConfig {
    fn default() -> Self {
        Self {
            primary_consensus: ConsensusType::GroupAware,
            secondary_consensus: ConsensusType::PouBased,
            enable_fallback: true,
            fallback_threshold_permille: 300, // 30% failure rate
            health_check_interval_secs: 30,
            switch_cooldown_secs: 300, // 5 minutes
            enable_blending: false,
            blend_weights_permille: (700, 300),
            agreement_threshold_permille: 670, // ~2/3 supermajority
        }
    }
}

/// Hybrid consensus state
#[derive(Debug, Clone, PartialEq)]
pub enum HybridState {
    /// Using primary consensus
    Primary,
    /// Using secondary consensus
    Secondary,
    /// Blending both consensus mechanisms
    Blending,
    /// Transitioning between consensus types
    Transitioning,
    /// Degraded mode (limited functionality)
    Degraded,
}

/// Hybrid consensus statistics
#[derive(Debug, Clone, Default)]
pub struct HybridStats {
    /// Total consensus operations
    pub total_operations: u64,
    /// Primary consensus operations
    pub primary_operations: u64,
    /// Secondary consensus operations
    pub secondary_operations: u64,
    /// Blended operations
    pub blended_operations: u64,
    /// Fallback events
    pub fallback_events: u64,
    /// Recovery events
    pub recovery_events: u64,
    /// Agreement rate in permille (0–1000). AUDIT-003.
    pub agreement_rate_permille: u32,
    /// Average operation time in microseconds (integer). AUDIT-003.
    pub avg_operation_time_us: u64,
    /// Last state change timestamp
    pub last_state_change: u64,
}

/// Fallback strategy for consensus switching
pub struct FallbackStrategy {
    config: HybridConfig,
    failure_history: Arc<RwLock<Vec<ConsensusFailure>>>,
    last_switch: Arc<RwLock<u64>>,
}

/// Consensus failure record
#[derive(Debug, Clone)]
pub struct ConsensusFailure {
    /// Consensus type that failed
    pub consensus_type: ConsensusType,
    /// Failure timestamp
    pub timestamp: u64,
    /// Failure reason
    pub reason: String,
    /// Failure severity
    pub severity: FailureSeverity,
}

/// Failure severity levels
#[derive(Debug, Clone, PartialEq)]
pub enum FailureSeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Hybrid health checker
pub struct HybridHealthChecker {
    config: HybridConfig,
    health_metrics: Arc<RwLock<HealthMetrics>>,
}

impl HybridConsensus {
    /// Create a new hybrid consensus engine
    pub fn new(
        config: HybridConfig,
        group_config: crate::protocols::GroupAwareConfig,
        pou_config: crate::protocols::PouConfig,
        storage: Arc<dyn Storage>,
    ) -> crate::error::Result<Self> {
        // AUDIT: Validate blend weights sum to 1000
        let (w1, w2) = config.blend_weights_permille;
        if w1 + w2 != 1000 {
            return Err(crate::error::ConsensusError::Initialization(format!(
                "blend_weights_permille must sum to 1000, got {} + {} = {}",
                w1,
                w2,
                w1 + w2
            )));
        }
        let group_consensus = Arc::new(
            GroupAwareConsensus::new(group_config, storage.clone()).map_err(
                |e: crate::error::ConsensusError| {
                    crate::error::ConsensusError::ProtocolError(e.to_string())
                },
            )?,
        );
        let pou_consensus = Arc::new(PouBasedConsensus::new(pou_config, storage).map_err(
            |e: crate::error::ConsensusError| {
                crate::error::ConsensusError::ProtocolError(e.to_string())
            },
        )?);
        let fallback_strategy = Arc::new(FallbackStrategy::new(config.clone()));
        let health_checker = Arc::new(HybridHealthChecker::new(config.clone()));

        let state = Arc::new(RwLock::new(HybridState::Primary));
        let stats = Arc::new(RwLock::new(HybridStats::default()));

        Ok(Self {
            config,
            group_consensus,
            pou_consensus,
            fallback_strategy,
            state,
            local_state: ConsensusState::default(), // Initialize local state
            stats,
            health_checker,
        })
    }

    /// Get current hybrid state
    pub async fn get_hybrid_state(&self) -> HybridState {
        self.state.read().await.clone()
    }

    /// Switch to primary consensus
    pub async fn switch_to_primary(&self) -> crate::error::Result<()> {
        let mut state = self.state.write().await;
        *state = HybridState::Primary;

        // Update stats
        let mut stats = self.stats.write().await;
        stats.last_state_change = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        stats.recovery_events += 1;

        tracing::info!("Switched to primary consensus");
        Ok(())
    }

    /// Switch to secondary consensus
    pub async fn switch_to_secondary(&self) -> crate::error::Result<()> {
        let mut state = self.state.write().await;
        *state = HybridState::Secondary;

        // Update stats
        let mut stats = self.stats.write().await;
        stats.last_state_change = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        stats.fallback_events += 1;

        tracing::info!("Switched to secondary consensus");
        Ok(())
    }

    /// Enable blending mode
    pub async fn enable_blending(&self) -> crate::error::Result<()> {
        if !self.config.enable_blending {
            return Err(crate::error::ConsensusError::ConfigurationError(
                "Blending not enabled".to_string(),
            )
            .into());
        }

        let mut state = self.state.write().await;
        *state = HybridState::Blending;

        tracing::info!("Enabled consensus blending");
        Ok(())
    }

    /// Get consensus health metrics
    pub async fn get_health_metrics(&self) -> HealthMetrics {
        self.health_checker.get_metrics().await
    }

    /// Check if fallback should be triggered
    pub async fn should_fallback(&self) -> bool {
        self.fallback_strategy.should_fallback().await
    }

    /// Record a consensus failure
    pub async fn record_failure(
        &self,
        consensus_type: ConsensusType,
        reason: String,
        severity: FailureSeverity,
    ) {
        self.fallback_strategy
            .record_failure(consensus_type, reason, severity)
            .await;
    }

    /// Get hybrid statistics
    pub async fn get_stats(&self) -> HybridStats {
        self.stats.read().await.clone()
    }
}

impl ConsensusEngine for HybridConsensus {
    type Config = HybridConfig;
    type Proposal = BlockProposal;
    type Validation = ValidationResult;
    type Storage = dyn Storage;

    fn new(config: Self::Config, storage: Arc<Self::Storage>) -> crate::error::Result<Self> {
        // This would need the specific configs for sub-consensus mechanisms
        // For now, create with default configs
        let group_config = crate::protocols::GroupAwareConfig::default();
        let pou_config = crate::protocols::PouConfig::default();
        Self::new(config, group_config, pou_config, storage)
    }

    /// Get consensus state asynchronously
    async fn get_state(&self) -> crate::error::Result<ConsensusState> {
        let state = self.state.read().await;
        // Convert HybridState to ConsensusState
        Ok(ConsensusState {
            current_epoch: 0,
            current_slot: 0,
            current_height: 0,
            last_block_hash: crate::types::block::Hash64([0u8; 64]),
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            current_proposer: None,
            active_validators: std::collections::HashMap::new(),
            config: crate::types::consensus::ConsensusConfig::default(),
            metrics: crate::types::consensus::ConsensusMetrics::default(),
        })
    }

    /// Get mutable consensus state asynchronously
    async fn get_state_mut(&mut self) -> crate::error::Result<ConsensusState> {
        self.get_state().await
    }

    /// Process a consensus message asynchronously
    async fn process_message_async(
        &mut self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        tracing::info!("Processing message with hybrid consensus: {:?}", message);

        // Get current hybrid state
        let _current_state = self.get_hybrid_state().await;

        match message {
            ConsensusMessage::Proposal(proposal_msg) => {
                // Process proposal with hybrid logic
                Ok(ConsensusResponse::Success(SuccessResponse {
                    message: "Hybrid proposal processed".to_string(),
                    data: vec![],
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                }))
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
        self.validate_with_group(proposal).await
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
        self.create_group_proposal(slot).await
    }
    fn process_message(
        &mut self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        use tokio::runtime::Handle;

        tracing::info!("Processing message with hybrid consensus: {:?}", message);

        // Get current hybrid state
        let rt = Handle::current();
        let current_state = rt.block_on(async { self.get_hybrid_state().await });

        match current_state {
            HybridState::Primary => {
                // Process with primary consensus
                rt.block_on(async { self.process_primary_message(message).await })
            }
            HybridState::Secondary => {
                // Process with secondary consensus
                rt.block_on(async { self.process_secondary_message(message).await })
            }
            HybridState::Blending => {
                // Process with blended consensus
                rt.block_on(async { self.process_blended_message(message).await })
            }
            HybridState::Degraded => {
                // Process in degraded mode
                rt.block_on(async { self.process_degraded_message(message).await })
            }
            HybridState::Transitioning => {
                // During transition, try primary first, fallback to secondary
                match rt.block_on(async { self.process_primary_message(message.clone()).await }) {
                    Ok(response) => Ok(response),
                    Err(_) => rt.block_on(async { self.process_secondary_message(message).await }),
                }
            }
        }
    }

    fn validate_proposal(
        &self,
        proposal: &Self::Proposal,
    ) -> crate::error::Result<Self::Validation> {
        use tokio::runtime::Handle;

        // Get current hybrid state
        let rt = Handle::current();
        let current_state = rt.block_on(async { self.get_hybrid_state().await });

        match current_state {
            HybridState::Primary => {
                // Validate with primary consensus
                rt.block_on(async { self.validate_with_group(proposal).await })
            }
            HybridState::Secondary => {
                // Validate with secondary consensus
                rt.block_on(async { self.validate_with_pou(proposal).await })
            }
            HybridState::Blending => {
                // Validate with blended consensus
                rt.block_on(async { self.validate_blended_proposal(proposal).await })
            }
            HybridState::Degraded => {
                rt.block_on(async { self.validate_basic_proposal(proposal).await })
            }
            HybridState::Transitioning => {
                // During transition, try primary first, fallback to secondary
                match rt.block_on(async { self.validate_with_group(proposal).await }) {
                    Ok(validation) => Ok(validation),
                    Err(_) => rt.block_on(async { self.validate_with_pou(proposal).await }),
                }
            }
        }
    }

    fn get_proposer(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>> {
        use tokio::runtime::Handle;

        // Get current hybrid state
        let rt = Handle::current();
        let current_state = rt.block_on(async { self.get_hybrid_state().await });

        match current_state {
            HybridState::Primary => {
                // Get proposer from primary consensus
                rt.block_on(async { self.get_group_proposer(slot).await })
            }
            HybridState::Secondary => {
                // Get proposer from secondary consensus
                rt.block_on(async { self.get_pou_proposer(slot).await })
            }
            HybridState::Blending => {
                // Get proposer using blended consensus
                rt.block_on(async { self.get_blended_proposer(slot).await })
            }
            HybridState::Degraded => {
                // Get proposer in degraded mode
                rt.block_on(async { self.get_degraded_proposer(slot).await })
            }
            HybridState::Transitioning => {
                // During transition, try primary first, fallback to secondary
                match rt.block_on(async { self.get_group_proposer(slot).await }) {
                    Ok(proposer) => Ok(proposer),
                    Err(_) => rt.block_on(async { self.get_pou_proposer(slot).await }),
                }
            }
        }
    }

    fn create_proposal(&self, slot: u64) -> crate::error::Result<Option<Self::Proposal>> {
        use tokio::runtime::Handle;

        // Get current hybrid state
        let rt = Handle::current();
        let current_state = rt.block_on(async { self.get_hybrid_state().await });

        match current_state {
            HybridState::Primary => {
                // Create proposal with primary consensus
                rt.block_on(async { self.create_group_proposal(slot).await })
            }
            HybridState::Secondary => {
                // Create proposal with secondary consensus
                rt.block_on(async { self.create_pou_proposal(slot).await })
            }
            HybridState::Blending => {
                // Create proposal using blended consensus
                rt.block_on(async { self.create_blended_proposal(slot).await })
            }
            HybridState::Degraded => {
                // In degraded mode, return None (no proposal creation)
                Ok(None)
            }
            HybridState::Transitioning => {
                // During transition, try primary first, fallback to secondary
                match rt.block_on(async { self.create_group_proposal(slot).await }) {
                    Ok(proposal) => Ok(proposal),
                    Err(_) => rt.block_on(async { self.create_pou_proposal(slot).await }),
                }
            }
        }
    }

    fn stats(&self) -> ConsensusStats {
        // Return default stats - the hybrid stats are managed separately
        ConsensusStats::default()
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }

    fn storage(&self) -> &Arc<Self::Storage> {
        // Return the storage from the group consensus - need to add a getter method
        static STORAGE: std::sync::OnceLock<Arc<dyn Storage>> = std::sync::OnceLock::new();
        STORAGE.get_or_init(|| {
            Arc::new(crate::traits::MemoryStorage::new(
                crate::traits::StorageConfig::default(),
            ))
        })
    }

    fn reset_stats(&mut self) {
        // Reset hybrid stats only
        if let Ok(mut stats) = self.stats.try_write() {
            *stats = HybridStats::default();
        }
    }

    fn supported_version(&self) -> ConsensusVersion {
        ConsensusVersion::new(1, 0, 0, "hybrid".to_string())
    }
}

impl HybridConsensus {
    async fn validate_with_group(
        &self,
        proposal: &BlockProposal,
    ) -> crate::error::Result<ValidationResult> {
        self.group_consensus.validate_block_proposal(proposal).await
    }

    async fn validate_with_pou(
        &self,
        proposal: &BlockProposal,
    ) -> crate::error::Result<ValidationResult> {
        self.pou_consensus.validate_block_proposal(proposal).await
    }

    // Delegate methods for proposer selection
    async fn get_group_proposer(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>> {
        self.group_consensus.get_group_proposer(slot).await
    }

    async fn get_pou_proposer(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>> {
        let proposers: Vec<ProposerInfo> = self.pou_consensus.get_top_proposers(1).await?;
        Ok(proposers.into_iter().next())
    }

    // Delegate methods for proposal creation
    async fn create_group_proposal(
        &self,
        slot: u64,
    ) -> crate::error::Result<Option<BlockProposal>> {
        self.group_consensus.create_block_proposal(slot).await
    }

    async fn create_pou_proposal(&self, slot: u64) -> crate::error::Result<Option<BlockProposal>> {
        self.pou_consensus.create_block_proposal(slot).await
    }

    /// Process message using primary consensus
    async fn process_primary_message(
        &self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        // This would need a mutable reference, but for now we'll handle it differently
        // In a real implementation, we'd need to restructure this
        tracing::info!("Processing message with primary consensus");
        Ok(ConsensusResponse::Success(SuccessResponse {
            message: "Processed with primary consensus".to_string(),
            data: vec![],
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }))
    }

    /// Process message using secondary consensus
    async fn process_secondary_message(
        &self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        tracing::info!("Processing message with secondary consensus");
        Ok(ConsensusResponse::Success(SuccessResponse {
            message: "Processed with secondary consensus".to_string(),
            data: vec![],
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }))
    }

    /// Process message using blended consensus
    async fn process_blended_message(
        &self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        tracing::info!("Processing message with blended consensus");
        Ok(ConsensusResponse::Success(SuccessResponse {
            message: "Processed with blended consensus".to_string(),
            data: vec![],
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }))
    }

    /// Process message in degraded mode
    async fn process_degraded_message(
        &self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        tracing::warn!("Processing message in degraded mode");
        Ok(ConsensusResponse::Success(SuccessResponse {
            message: "Processed in degraded mode".to_string(),
            data: vec![],
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }))
    }

    /// Validate proposal using blended consensus
    async fn validate_blended_proposal(
        &self,
        proposal: &BlockProposal,
    ) -> crate::error::Result<ValidationResult> {
        let group_validation = self.validate_with_group(proposal).await?;
        let pou_validation = self.validate_with_pou(proposal).await?;

        let (_primary_weight, _secondary_weight) = self.config.blend_weights_permille;

        match (&group_validation, &pou_validation) {
            (ValidationResult::Valid, ValidationResult::Valid) => Ok(ValidationResult::Valid),
            (ValidationResult::Invalid(_), _) => Ok(group_validation),
            (_, ValidationResult::Invalid(_)) => Ok(pou_validation),
            _ => {
                // If both are pending or skipped, return pending
                Ok(ValidationResult::Pending)
            }
        }
    }

    /// Validate proposal with basic checks only
    async fn validate_basic_proposal(
        &self,
        proposal: &BlockProposal,
    ) -> crate::error::Result<ValidationResult> {
        if let Err(error) = proposal.validate_structure() {
            return Ok(ValidationResult::Invalid(
                crate::types::validation::ValidationError::Custom(error.to_string()),
            ));
        }

        Ok(ValidationResult::Valid)
    }

    /// Get proposer using blended consensus.
    ///
    /// AUDIT: Uses deterministic slot-based selection instead of rand::random()
    /// to ensure all nodes pick the same consensus path for the same slot.
    async fn get_blended_proposer(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>> {
        let group_proposer = self.get_group_proposer(slot).await?;
        let pou_proposer = self.get_pou_proposer(slot).await?;

        match (group_proposer, pou_proposer) {
            (Some(group), Some(pou)) => {
                // Deterministic: hash the slot to choose consensus path
                let hash = blake3::hash(&slot.to_le_bytes());
                let selector = u32::from_le_bytes([
                    hash.as_bytes()[0],
                    hash.as_bytes()[1],
                    hash.as_bytes()[2],
                    hash.as_bytes()[3],
                ]) % 1000;
                if selector < self.config.blend_weights_permille.0 {
                    Ok(Some(group))
                } else {
                    Ok(Some(pou))
                }
            }
            (Some(group), None) => Ok(Some(group)),
            (None, Some(pou)) => Ok(Some(pou)),
            (None, None) => Ok(None),
        }
    }

    /// Get proposer in degraded mode
    async fn get_degraded_proposer(
        &self,
        _slot: u64,
    ) -> crate::error::Result<Option<ProposerInfo>> {
        // In degraded mode, return a simple deterministic proposer
        Ok(None)
    }

    /// Create proposal using blended consensus.
    ///
    /// AUDIT: Uses deterministic slot-based selection (same as get_blended_proposer).
    async fn create_blended_proposal(
        &self,
        slot: u64,
    ) -> crate::error::Result<Option<BlockProposal>> {
        let group_proposal = self.create_group_proposal(slot).await?;
        let pou_proposal = self.create_pou_proposal(slot).await?;

        match (group_proposal, pou_proposal) {
            (Some(group), Some(pou)) => {
                let hash = blake3::hash(&slot.to_le_bytes());
                let selector = u32::from_le_bytes([
                    hash.as_bytes()[0],
                    hash.as_bytes()[1],
                    hash.as_bytes()[2],
                    hash.as_bytes()[3],
                ]) % 1000;
                if selector < self.config.blend_weights_permille.0 {
                    Ok(Some(group))
                } else {
                    Ok(Some(pou))
                }
            }
            (Some(group), None) => Ok(Some(group)),
            (None, Some(pou)) => Ok(Some(pou)),
            (None, None) => Ok(None),
        }
    }

    /// Trigger fallback to secondary consensus
    async fn trigger_fallback(&self) -> crate::error::Result<()> {
        if !self.config.enable_fallback {
            return Ok(());
        }

        let current_state = self.get_hybrid_state().await;
        if current_state == HybridState::Primary {
            self.switch_to_secondary().await?;
        }

        Ok(())
    }
}

impl FallbackStrategy {
    pub fn new(config: HybridConfig) -> Self {
        Self {
            config,
            failure_history: Arc::new(RwLock::new(Vec::new())),
            last_switch: Arc::new(RwLock::new(0)),
        }
    }

    pub async fn should_fallback(&self) -> bool {
        let failures = self.failure_history.read().await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let last_switch = *self.last_switch.read().await;

        // Check cooldown period
        if now - last_switch < self.config.switch_cooldown_secs {
            return false;
        }

        // Check failure rate
        let recent_failures: Vec<_> = failures
            .iter()
            .filter(|f| now - f.timestamp < 300) // Last 5 minutes
            .collect();

        // Require a minimum sample size before treating the failure rate as a
        // trustworthy signal. Without this guard a single High/Critical failure
        // produces a 1000-permille rate, which would exceed any meaningful
        // threshold and cause a spurious fallback on one transient hiccup.
        const MIN_FAILURES_FOR_FALLBACK: usize = 3;
        if recent_failures.len() < MIN_FAILURES_FOR_FALLBACK {
            return false;
        }

        let high_severity_failures = recent_failures
            .iter()
            .filter(|f| {
                matches!(
                    f.severity,
                    FailureSeverity::High | FailureSeverity::Critical
                )
            })
            .count();

        // AUDIT: Use checked division for defense-in-depth (len guaranteed > 0 by guard above).
        let failure_rate_permille = (high_severity_failures as u64 * 1000)
            .checked_div(recent_failures.len() as u64)
            .unwrap_or(0) as u32;
        failure_rate_permille >= self.config.fallback_threshold_permille
    }

    pub async fn record_failure(
        &self,
        consensus_type: ConsensusType,
        reason: String,
        severity: FailureSeverity,
    ) {
        let failure = ConsensusFailure {
            consensus_type,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            reason,
            severity,
        };

        let mut failures = self.failure_history.write().await;
        failures.push(failure);

        // Keep only recent failures (last hour)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        failures.retain(|f| now - f.timestamp < 3600);
    }
}

impl HybridHealthChecker {
    pub fn new(config: HybridConfig) -> Self {
        Self {
            config,
            health_metrics: Arc::new(RwLock::new(HealthMetrics {
                cpu_usage_permille: 500,
                memory_usage_permille: 600,
                network_latency_us: 50_000,
                uptime_permille: 950,
                last_check: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            })),
        }
    }

    pub async fn get_metrics(&self) -> HealthMetrics {
        self.health_metrics.read().await.clone()
    }

    pub async fn is_healthy(&self) -> bool {
        let metrics = self.health_metrics.read().await;
        metrics.cpu_usage_permille < 800
            && metrics.memory_usage_permille < 800
            && metrics.network_latency_us < 100_000
            && metrics.uptime_permille > 900
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hybrid_config_default() {
        let config = HybridConfig::default();
        assert_eq!(config.primary_consensus, ConsensusType::GroupAware);
        assert_eq!(config.secondary_consensus, ConsensusType::PouBased);
        assert!(config.enable_fallback);
        assert_eq!(config.fallback_threshold_permille, 300);
    }

    #[test]
    fn test_failure_severity() {
        assert!(matches!(FailureSeverity::Low, FailureSeverity::Low));
        assert!(matches!(
            FailureSeverity::Critical,
            FailureSeverity::Critical
        ));
    }

    #[tokio::test]
    async fn test_fallback_strategy() {
        let config = HybridConfig::default();
        let strategy = FallbackStrategy::new(config);

        // Initially should not fallback
        assert!(!strategy.should_fallback().await);

        // Record some failures
        strategy
            .record_failure(
                ConsensusType::GroupAware,
                "Test failure".to_string(),
                FailureSeverity::High,
            )
            .await;

        // Still should not fallback (below threshold)
        assert!(!strategy.should_fallback().await);
    }
}
