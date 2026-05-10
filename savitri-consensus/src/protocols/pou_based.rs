//! PoU-based consensus implementation for lightnodes
//!
//! This module implements the Proof-of-Unity based consensus mechanism used by lightnodes,

use crate::error::ConsensusError;
use crate::protocols::ConsensusHealthChecker;
use crate::scoring::ObservationStore;
use crate::traits::{
    ConsensusEngine, DefaultProposerContext, DefaultValidationContext, MockHealthChecker,
    MockProposalSigner, MockSignatureValidator, MockStateValidator, ProposalSigner, ProposalStats,
    Proposer, ProposerContext, ProposerHealthChecker, ProposerScore, SignatureValidator,
    StateValidator, Storage, ValidationContext, ValidationStats, Validator,
};
use crate::types::validation::{ValidationError, ValidationResult};
use crate::types::{
    AvailabilityProofData, Block, BlockProposal, ConsensusMessage, ConsensusResponse,
    ConsensusState, ConsensusStats, ConsensusVersion, DefaultScoreCalculator, ErrorResponse,
    GeographicInfo, GroupInfo, GroupProofData, GroupStatus, LatencyProofData, NodeType,
    PendingResponse, PouScore, PouScoreResult, Proposal, ProposalMessage, ProposalMetadata,
    ProposalTransaction, RttMeasurement, ScoreCalculator, ScoreComponents, ScoreConfig,
    ScoreThresholds, SuccessResponse, Transaction, ValidationProposerInfo, ValidatorInfo,
    ValidatorMetadata, ValidatorStatus,
};
use crate::ProposerInfo;
use hex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;
use tokio::sync::RwLock;

// ── SECURITY (C-08): VRF-like proposer selection ─────────────────────────
//
// Replaces deterministic `slot % len` with blake3-based verifiable random
// selection. Properties:
// - **Deterministic**: All nodes produce the same result for identical inputs
// - **Unpredictable**: Requires knowledge of the previous block hash
// - **Weighted**: Higher PoU scores give proportionally higher probability
// - **Verifiable**: Any node can verify the selection by recomputing the hash

/// Select a proposer index using VRF-like weighted random selection.
///
/// Uses `blake3("PROPOSER-VRF" || slot || last_block_hash)` as a
/// deterministic random seed, then performs weighted selection based on
/// PoU scores. Returns an index into `proposers`.
fn vrf_weighted_select(slot: u64, last_block_hash: &[u8; 64], proposers: &[ProposerInfo]) -> usize {
    // SECURITY: Return 0 as safe fallback for empty proposer list (caller must check)
    if proposers.is_empty() {
        return 0;
    }

    // Build the VRF input: domain tag + slot + chain randomness
    let mut input = Vec::with_capacity(14 + 8 + 64);
    input.extend_from_slice(b"PROPOSER-VRF");
    input.extend_from_slice(&slot.to_le_bytes());
    input.extend_from_slice(last_block_hash);

    let hash = blake3::hash(&input);
    let hash_bytes = hash.as_bytes();

    // Extract a u64 from the first 8 bytes for selection
    let rand_val = u64::from_le_bytes([
        hash_bytes[0],
        hash_bytes[1],
        hash_bytes[2],
        hash_bytes[3],
        hash_bytes[4],
        hash_bytes[5],
        hash_bytes[6],
        hash_bytes[7],
    ]);

    // Compute total weight (sum of PoU scores); minimum weight = 1 per proposer
    let total_weight: u64 = proposers.iter().map(|p| (p.score as u64).max(1)).sum();

    // SECURITY (MED-03): Rejection-sampling to eliminate modulo bias.
    // `rand_val % total_weight` is biased when total_weight does not divide
    // 2^64 evenly. We reject values in the "tail" region and use additional
    // bytes from the BLAKE3 hash to produce an unbiased sample.
    //
    // The maximum number of rejectable values per sample is total_weight - 1.
    // For total_weight <= 2^32 the expected number of extra hash bytes needed
    // is at most 1 additional u64 word.
    let threshold = u64::MAX - (u64::MAX % total_weight);
    // Use all 32 bytes of the BLAKE3 hash output (4 u64 words) before giving up.
    let candidates: [u64; 4] = [
        rand_val,
        u64::from_le_bytes([
            hash_bytes[8],
            hash_bytes[9],
            hash_bytes[10],
            hash_bytes[11],
            hash_bytes[12],
            hash_bytes[13],
            hash_bytes[14],
            hash_bytes[15],
        ]),
        u64::from_le_bytes([
            hash_bytes[16],
            hash_bytes[17],
            hash_bytes[18],
            hash_bytes[19],
            hash_bytes[20],
            hash_bytes[21],
            hash_bytes[22],
            hash_bytes[23],
        ]),
        u64::from_le_bytes([
            hash_bytes[24],
            hash_bytes[25],
            hash_bytes[26],
            hash_bytes[27],
            hash_bytes[28],
            hash_bytes[29],
            hash_bytes[30],
            hash_bytes[31],
        ]),
    ];

    let mut selected_rand = rand_val; // fallback if all 4 candidates are in the tail
    for &candidate in &candidates {
        if candidate <= threshold {
            selected_rand = candidate;
            break;
        }
    }

    let target = selected_rand % total_weight;
    let mut cumulative: u64 = 0;
    for (i, p) in proposers.iter().enumerate() {
        cumulative += (p.score as u64).max(1);
        if target < cumulative {
            return i;
        }
    }

    // Fallback (should not be reached if total_weight > 0)
    proposers.len() - 1
}

/// PoU-based consensus engine
///
/// This consensus engine uses Proof-of-Unity scores for proposer selection
pub struct PouBasedConsensus {
    config: PouConfig,
    storage: Arc<dyn Storage>,
    proposer: PouProposer,
    validator: PouValidator,
    state: Arc<RwLock<ConsensusState>>, // Only shared state - fully async
    stats: Arc<RwLock<ConsensusStats>>,
    pou_calculator: Arc<PouCalculator>,
    score_manager: Arc<ScoreManager>,
    health_checker: ConsensusHealthChecker,
    observations: Arc<ObservationStore>,
}

/// PoU-based consensus configuration
#[derive(Debug, Clone)]
pub struct PouConfig {
    /// Score update interval in seconds
    pub score_update_interval_secs: u64,
    /// Minimum proposer score
    pub min_proposer_score: PouScore,
    /// Enable latency proof
    pub enable_latency_proof: bool,
    /// Enable availability proof
    pub enable_availability_proof: bool,
    /// Maximum proposal size in bytes
    pub max_proposal_size: usize,
    /// Proposal timeout in milliseconds
    pub proposal_timeout_ms: u64,
    pub enable_signature_validation: bool,
    pub enable_state_validation: bool,
    /// Score decay rate (permille, 0-1000; e.g. 10 = 1.0% decay)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub score_decay_rate: u32,
    /// Minimum uptime percentage (permille, 0-1000; e.g. 800 = 80%)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub min_uptime_percentage: u32,
    /// Geographic weight in score calculation (permille, 0-1000)
    /// AUDIT-003 FIX: changed from f64 to u32 for cross-arch determinism
    pub geographic_weight: u32,
    /// Performance weight in score calculation (permille, 0-1000)
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
            max_proposal_size: 1024 * 1024, // 1MB
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

impl PouBasedConsensus {
    /// Create a new PoU-based consensus engine with a fresh observation store.
    ///
    /// Prefer `new_with_observations` when the P2P layer needs to share the
    /// same store for recording live measurements.
    pub fn new(config: PouConfig, storage: Arc<dyn Storage>) -> crate::error::Result<Self> {
        Self::new_with_observations(config, storage, Arc::new(ObservationStore::new()))
    }

    /// Create a PoU-based consensus engine wired to an externally-owned
    /// `ObservationStore`. The P2P layer should call
    /// `consensus.observations().record_latency(...)` on every ping / block
    /// round-trip so the scorer has real inputs instead of mock samples.
    pub fn new_with_observations(
        config: PouConfig,
        storage: Arc<dyn Storage>,
        observations: Arc<ObservationStore>,
    ) -> crate::error::Result<Self> {
        let proposer = PouProposer::new(config.clone());
        let validator = PouValidator::new(config.clone());
        let pou_calculator = Arc::new(PouCalculator::new_with_observations(
            config.clone(),
            Arc::clone(&observations),
        ));
        let score_manager = Arc::new(ScoreManager::new(config.clone()));
        let health_checker = ConsensusHealthChecker::new();

        let state = Arc::new(RwLock::new(ConsensusState::default()));
        let stats = Arc::new(RwLock::new(ConsensusStats::default()));

        Ok(Self {
            config,
            storage,
            proposer,
            validator,
            state,
            stats,
            pou_calculator,
            score_manager,
            health_checker,
            observations,
        })
    }

    /// Shared handle to the observation store. P2P handlers use this to
    /// record latency / availability / integrity samples as they happen.
    pub fn observations(&self) -> Arc<ObservationStore> {
        Arc::clone(&self.observations)
    }

    /// Get current PoU score for a node
    pub async fn get_node_score(&self, node_id: &str) -> crate::Result<Option<PouScoreResult>> {
        match self.storage.get_score(node_id).await {
            Ok(score) => Ok(score),
            Err(e) => Err(crate::ConsensusError::from(e)),
        }
    }

    /// Update PoU score for a node
    pub async fn update_node_score(
        &self,
        node_id: &str,
        score: PouScoreResult,
    ) -> crate::Result<()> {
        self.storage.store_score(node_id, &score).await?;
        self.score_manager.update_score(node_id, &score).await;
        Ok(())
    }

    /// Calculate PoU score for a node
    pub async fn calculate_score(&self, node_id: &str) -> crate::Result<PouScoreResult> {
        self.pou_calculator
            .calculate_score(node_id, &self.storage)
            .await
    }

    /// Get top proposers by score
    pub async fn get_top_proposers(&self, count: usize) -> crate::Result<Vec<ProposerInfo>> {
        let validators = self.storage.get_active_validators().await?;
        let mut proposers: Vec<_> = validators.into_iter()
            // SECURITY: Enforce both lower and upper bound on PoU scores (0–1000)
            .filter(|v| v.score >= self.config.min_proposer_score as u32 && v.score <= 1000)
            .map(|v| ProposerInfo {
                node_id: v.validator_id.clone(),
                peer_id: v.validator_id.clone(),
                public_key: v.public_key,
                score: v.score,
                group_id: None,
                region: v.metadata.region.clone(),
                capabilities: v.metadata.capabilities.clone(),
            })
            .collect();

        proposers.sort_by(|a, b| b.score.cmp(&a.score));
        proposers.truncate(count);

        Ok(proposers)
    }

    /// Check if node is eligible to be proposer
    pub async fn is_eligible_proposer(
        &self,
        node_id: &str,
        current_score: PouScore,
    ) -> crate::Result<bool> {
        // SECURITY: Check both minimum and maximum score bounds (0–1000)
        if current_score < self.config.min_proposer_score {
            return Ok(false);
        }
        if current_score > 1000 {
            tracing::warn!(
                node_id = node_id,
                score = current_score,
                "Rejecting proposer with out-of-range PoU score"
            );
            return Ok(false);
        }

        Ok(true)
    }

    /// Validate a block proposal
    pub async fn validate_block_proposal(
        &self,
        proposal: &BlockProposal,
    ) -> crate::Result<ValidationResult> {
        let context = DefaultValidationContext::new(
            proposal.round_id / 1000,
            proposal.round_id,
            proposal.height,
            "lightnode".to_string(),
        );
        Ok(self.validator.validate_proposal(proposal, &context))
    }

    /// Create a block proposal
    pub async fn create_block_proposal(&self, slot: u64) -> crate::Result<Option<BlockProposal>> {
        let top_proposers: Vec<ProposerInfo> = self.get_top_proposers(1).await?;
        if top_proposers.is_empty() {
            return Ok(None);
        }
        Err(crate::error::ConsensusError::ProtocolError(format!(
            "create_block_proposal: proposal builder not yet wired — \
             callers must supply block data for slot {slot}"
        )))
    }
}

impl ConsensusEngine for PouBasedConsensus {
    type Config = PouConfig;
    type Proposal = BlockProposal;
    type Validation = ValidationResult;
    type Storage = dyn Storage;

    fn new(config: Self::Config, storage: Arc<Self::Storage>) -> crate::error::Result<Self> {
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
        tracing::info!("Processing message with PoU-based consensus: {:?}", message);

        match message {
            ConsensusMessage::Proposal(proposal_msg) => {
                // Process proposal message asynchronously
                let proposal = self.deserialize_proposal(&proposal_msg.proposal_data)?;
                let validation_result = self.validate_proposal_async(&proposal).await?;

                match validation_result {
                    ValidationResult::Valid => {
                        // Update state asynchronously
                        let mut state = self.state.write().await;
                        state.current_height += 1;
                        state.last_updated = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();

                        Ok(ConsensusResponse::Success(SuccessResponse {
                            message: "Proposal validated and added".to_string(),
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
                            error_message: format!("Proposal validation failed: {}", error),
                            error_details: vec![error.to_string()],
                            timestamp: std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs(),
                        }))
                    }
                    ValidationResult::Pending => Ok(ConsensusResponse::Pending(PendingResponse {
                        message: "Proposal validation pending".to_string(),
                        estimated_completion: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs()
                            + 5,
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                    })),
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
        // Use the proposer to get the current proposer for this slot
        self.proposer.get_proposer(slot).await
    }

    /// Create proposal asynchronously
    async fn create_proposal_async(
        &self,
        slot: u64,
    ) -> crate::error::Result<Option<Self::Proposal>> {
        self.create_block_proposal(slot).await
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

        // Get top proposers by PoU score
        let rt = Handle::current();
        let top_proposers = rt.block_on(async { self.get_top_proposers(10).await })?;

        if top_proposers.is_empty() {
            return Ok(None);
        }

        // SECURITY (C-08): VRF-like proposer selection using blake3.
        //
        // Instead of deterministic `slot % len` (predictable, enables targeted DDoS),
        // we hash the slot together with the last block hash to produce an unpredictable
        // but deterministic and verifiable seed. This seed is then used for PoU-weighted
        // chosen, but the exact outcome is not predictable until the previous block is
        // finalized.
        let last_block_hash = rt.block_on(async {
            let state = self.state.read().await;
            state.last_block_hash
        });

        let selected = vrf_weighted_select(slot, &last_block_hash, &top_proposers);
        // SECURITY: bounds-check VRF output to prevent panic on invalid index
        let selected_proposer = match top_proposers.get(selected) {
            Some(p) => p,
            None => return Ok(None),
        };

        // Check if proposer is eligible
        let is_eligible = rt.block_on(async {
            self.is_eligible_proposer(
                &selected_proposer.node_id,
                selected_proposer.score as PouScore,
            )
            .await
        })?;

        if is_eligible {
            Ok(Some(selected_proposer.clone()))
        } else {
            // Fallback: try next candidate in VRF order
            for offset in 1..top_proposers.len() {
                let idx = (selected + offset) % top_proposers.len();
                let candidate = &top_proposers[idx];
                let eligible = rt.block_on(async {
                    self.is_eligible_proposer(&candidate.node_id, candidate.score as PouScore)
                        .await
                })?;
                if eligible {
                    return Ok(Some(candidate.clone()));
                }
            }
            Ok(None)
        }
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
        ConsensusVersion::new(1, 0, 0, "pou-based".to_string())
    }
}

// Helper methods for PouBasedConsensus
impl PouBasedConsensus {
    /// Helper method to deserialize proposal from message data
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

/// PoU calculator for calculating Proof-of-Unity scores
pub struct PouCalculator {
    config: PouConfig,
    score_calculator: Box<dyn ScoreCalculator>,
    observations: Arc<ObservationStore>,
}

impl PouCalculator {
    pub fn new(config: PouConfig) -> Self {
        Self::new_with_observations(config, Arc::new(ObservationStore::new()))
    }

    pub fn new_with_observations(config: PouConfig, observations: Arc<ObservationStore>) -> Self {
        Self {
            config,
            score_calculator: Box::new(DefaultScoreCalculator),
            observations,
        }
    }

    /// Expose the observation store so the P2P layer can record samples.
    pub fn observations(&self) -> Arc<ObservationStore> {
        Arc::clone(&self.observations)
    }

    /// Calculate PoU score for a node
    pub async fn calculate_score(
        &self,
        node_id: &str,
        storage: &Arc<dyn Storage>,
    ) -> crate::Result<PouScoreResult> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let current_epoch = current_time / 3600; // 1 hour epochs

        let validator_info = storage.get_validator(node_id).await?;

        // Get existing score if available
        let existing_score = storage.get_score(node_id).await?;

        // Calculate score components
        let mut components = ScoreComponents::default();

        // Calculate latency score from observed RTT samples.
        // Falls back to POU_SCORE_DEFAULT (500) inside calculate_latency_score
        // when no samples are available, so a brand-new peer is neutral.
        let latency_samples = self.observations.latency_measurements(node_id);
        components.latency_score = self
            .score_calculator
            .calculate_latency_score(&latency_samples);

        if let Some(validator) = &validator_info {
            let availability_score = if validator.last_active > 0 {
                // Elapsed seconds since last active, capped at 86400 (1 day)
                let elapsed = (current_time.saturating_sub(validator.last_active)).min(86400);
                // Score = (86400 - elapsed) * 1000 / 86400
                ((86400u64 - elapsed) * 1000 / 86400) as PouScore
            } else {
                0
            };
            components.availability_score = availability_score.min(1000);
        }

        // slash events. `calculate_integrity_score` in `types/score.rs`
        // handles zero-denominator peers as "perfect integrity" (1000).
        let integrity_measurement = self
            .observations
            .build_integrity_measurement(node_id, current_epoch);
        components.integrity_score = self
            .score_calculator
            .calculate_integrity_score(&integrity_measurement);

        // Federated Learning contribution integrity — rolling average of
        // per-round robust scores recorded via `record_fl_contribution`.
        // Non-FL peers default to 1000 (no penalty for non-participation).
        components.fl_integrity_score = self.observations.build_fl_integrity_score(node_id);

        // Calculate geographic score
        if let Some(validator) = &validator_info {
            components.geographic_score = self.score_calculator.calculate_geographic_score(
                &GeographicInfo {
                    node_id: node_id.to_string(),
                    region: validator.metadata.region.clone(),
                    country_code: "XX".to_string(),
                    latitude: 0.0,
                    longitude: 0.0,
                    timezone: "UTC".to_string(),
                },
                "global",
            );
        }

        // Calculate performance score (mock implementation)
        components.performance_score = 800; // Good performance

        // Calculate reputation score
        if let Some(existing) = &existing_score {
            components.reputation_score = existing.components.reputation_score;
        } else {
            components.reputation_score = 500; // Neutral reputation
        }

        let raw_score = if let Some(existing) = existing_score {
            // Decay: score * (1000 - decay_rate) / 1000
            let decayed_score: u64 = (existing.score as u64)
                * (1000u64.saturating_sub(self.config.score_decay_rate as u64))
                / 1000;
            let calculated_score: u64 =
                self.score_calculator
                    .calculate_score(&components, &ScoreConfig::default()) as u64;

            // Weighted average: 70% decayed + 30% calculated (integer)
            // (decayed * 700 + calculated * 300) / 1000
            let weighted_score = (decayed_score * 700 + calculated_score * 300) / 1000;
            (weighted_score as PouScore).min(1000)
        } else {
            self.score_calculator
                .calculate_score(&components, &ScoreConfig::default())
        };

        // SECURITY: Clamp score to valid range 0–1000
        let final_score = raw_score.min(1000);

        Ok(PouScoreResult {
            score: final_score,
            components,
            timestamp: current_time,
            epoch: current_epoch,
            node_id: node_id.to_string(),
            peer_id: node_id.to_string(),
        })
    }
}

/// Score manager for managing PoU scores
pub struct ScoreManager {
    config: PouConfig,
    score_cache: Arc<RwLock<HashMap<String, CachedScore>>>,
}

impl ScoreManager {
    pub fn new(config: PouConfig) -> Self {
        Self {
            config,
            score_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update score in cache
    pub async fn update_score(&self, node_id: &str, score: &PouScoreResult) {
        let mut cache = self.score_cache.write().await;
        cache.insert(node_id.to_string(), CachedScore::new(score.clone()));
    }

    /// Get cached score
    pub async fn get_cached_score(&self, node_id: &str) -> Option<PouScoreResult> {
        let cache = self.score_cache.read().await;
        cache.get(node_id).and_then(|cached| {
            if cached.is_valid(self.config.score_update_interval_secs) {
                Some(cached.score.clone())
            } else {
                None
            }
        })
    }

    /// Clean expired cache entries
    pub async fn clean_expired_cache(&self) {
        let mut cache = self.score_cache.write().await;
        cache.retain(|_, cached| cached.is_valid(self.config.score_update_interval_secs));
    }
}

/// Cached score information
#[derive(Debug, Clone)]
struct CachedScore {
    score: PouScoreResult,
    cached_at: u64,
}

impl CachedScore {
    pub fn new(score: PouScoreResult) -> Self {
        Self {
            score,
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

/// PoU proposer implementation
pub struct PouProposer {
    config: PouConfig,
    signer: MockProposalSigner,
    health_checker: MockHealthChecker,
}

impl PouProposer {
    pub fn new(config: PouConfig) -> Self {
        Self {
            config,
            signer: MockProposalSigner,
            health_checker: MockHealthChecker::new(true),
        }
    }

    /// Get proposer for a given slot (mock implementation for PouProposer)
    pub async fn get_proposer(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>> {
        // SECURITY (C-08): Even in mock, use VRF-like selection instead of
        // deterministic slot % 10 to avoid leaking predictable patterns.
        let mock_seed = blake3::hash(&slot.to_le_bytes());
        let seed_byte = mock_seed.as_bytes()[0] as usize;
        let index = seed_byte % 10;
        Ok(Some(ProposerInfo {
            node_id: format!("node_{}", index),
            peer_id: format!("peer_{}", index),
            public_key: [0u8; 32],
            score: 100,
            group_id: None,
            region: "default".to_string(),
            capabilities: vec!["consensus".to_string()],
        }))
    }
}

impl Proposer for PouProposer {
    type Context = DefaultProposerContext;

    fn create_proposal(&self, context: &Self::Context) -> crate::error::Result<Box<dyn Proposal>> {
        if !self.is_eligible(context) {
            return Err(crate::error::ConsensusError::ProtocolError(
                "Proposer not eligible".to_string(),
            ));
        }

        Err(crate::error::ConsensusError::ProtocolError(
            "create_proposal: proposal builder not yet wired — \
             callers must supply real block data"
                .to_string(),
        ))
    }

    fn sign_proposal(
        &self,
        proposal: &mut dyn Proposal,
    ) -> std::result::Result<(), crate::error::ConsensusError> {
        self.signer.sign_proposal(proposal)
    }

    fn is_eligible(&self, context: &Self::Context) -> bool {
        // Check health
        if !self.health_checker.is_healthy_sync() {
            return false;
        }

        // Check score threshold
        if context.current_score() < self.config.min_proposer_score as u32 {
            return false;
        }

        true
    }

    fn get_score(&self) -> ProposerScore {
        ProposerScore::default()
    }

    fn proposer_info(&self) -> ProposerInfo {
        ProposerInfo {
            node_id: "lightnode".to_string(),
            peer_id: "lightnode".to_string(),
            public_key: [0u8; 32],
            score: 500,
            group_id: None,
            region: "global".to_string(),
            capabilities: vec!["pou-proposer".to_string()],
        }
    }

    fn update_score(&self, _new_score: ProposerScore) -> crate::error::Result<()> {
        Ok(())
    }

    fn proposal_stats(&self) -> ProposalStats {
        ProposalStats::default()
    }

    fn is_healthy(&self) -> bool {
        self.health_checker.is_healthy_sync()
    }

    fn last_proposal_time(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

pub struct PouValidator {
    config: PouConfig,
    signature_validator: Arc<dyn SignatureValidator>,
    state_validator: Arc<dyn StateValidator>,
}

impl PouValidator {
    pub fn new(config: PouConfig) -> Self {
        Self {
            config,
            signature_validator: Arc::new(MockSignatureValidator),
            state_validator: Arc::new(MockStateValidator),
        }
    }
}

impl Validator for PouValidator {
    type Context = DefaultValidationContext;

    fn validate_block(&self, block: &Block, context: &Self::Context) -> ValidationResult {
        if !block.is_valid() {
            return ValidationResult::Invalid(ValidationError::InvalidBlock);
        }

        // Check block size (lightnodes have smaller limits)
        let block_size = block.size();
        if block_size > self.config.max_proposal_size as u64 {
            return ValidationResult::Invalid(ValidationError::InvalidBlock);
        }

        // Validate proposer score bounds
        let score = block.consensus_data.proposer_info.score;
        if score < self.config.min_proposer_score as u32 {
            return ValidationResult::Invalid(ValidationError::InsufficientPouScore);
        }
        // SECURITY: Upper bound check — PoU scores are defined on 0–1000 scale
        if score > 1000 {
            return ValidationResult::Invalid(ValidationError::InvalidBlock);
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
        // Check minimum score requirements
        let min_scores = context.get_min_scores();
        if proposer.score < min_scores.min_pou_score {
            return ValidationResult::Invalid(ValidationError::InsufficientPouScore);
        }

        // Lightnodes don't require group membership
        ValidationResult::Valid
    }

    fn validate_proposal(
        &self,
        proposal: &dyn Proposal,
        context: &Self::Context,
    ) -> ValidationResult {
        // Validate proposal structure
        if let Err(error) = proposal.validate_structure() {
            return ValidationResult::Invalid(ValidationError::Custom(format!("{:?}", error)));
        }

        // Validate proposer
        let proposer_info = proposal.proposer_info();
        self.validate_proposer(&proposer_info, context)
    }

    fn validator_info(&self) -> ValidatorInfo {
        ValidatorInfo {
            validator_id: "pou-validator".to_string(),
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
                node_type: NodeType::Lightnode,
                capabilities: vec!["pou-validation".to_string()],
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

    fn update_score(&self, _new_score: u32) -> crate::error::Result<()> {
        Ok(())
    }

    fn validation_stats(&self) -> ValidationStats {
        ValidationStats::default()
    }
}

impl LatencyProofData {
    pub fn new(
        round_id: u64,
        median_rtt_us: u64,
        peer_count: u32,
        rtt_measurements: Vec<RttMeasurement>,
        signature: [u8; 64],
        timestamp: u64,
    ) -> Self {
        Self {
            round_id,
            median_rtt_us,
            peer_count,
            rtt_measurements,
            signature: crate::types::block::Hash64(signature),
            timestamp,
        }
    }
}

impl AvailabilityProofData {
    pub fn new(
        round_id: u64,
        uptime_permille: u32,
        successful_pings: u32,
        total_pings: u32,
        last_seen: u64,
        signature: [u8; 64],
        timestamp: u64,
    ) -> Self {
        Self {
            round_id,
            uptime_permille,
            successful_pings,
            total_pings,
            last_seen,
            signature: crate::types::block::Hash64(signature),
            timestamp,
        }
    }
}

impl GroupProofData {
    pub fn new(
        group_id: String,
        epoch: u64,
        members: Vec<String>,
        is_proposer: bool,
        health_score_permille: u32,
        group_signature: [u8; 64],
        timestamp: u64,
    ) -> Self {
        Self {
            group_id,
            epoch,
            members,
            is_proposer,
            health_score_permille,
            group_signature: crate::types::block::Hash64(group_signature),
            timestamp,
        }
    }
}

impl BlockProposal {
    pub fn new(
        round_id: u64,
        height: u64,
        timestamp: u64,
        proposer_pubkey: [u8; 32],
        proposer_pou_score: u32,
        parent_hash: [u8; 64],
        state_root: [u8; 64],
        tx_root: [u8; 64],
        transactions: Vec<ProposalTransaction>,
        latency_proof: Option<LatencyProofData>,
        availability_proof: Option<AvailabilityProofData>,
        group_proof: Option<GroupProofData>,
        signature: [u8; 64],
        metadata: ProposalMetadata,
    ) -> Self {
        Self {
            round_id,
            height,
            timestamp,
            proposer_pubkey: crate::types::block::Hash32(proposer_pubkey),
            proposer_pou_score,
            parent_hash: crate::types::block::Hash64(parent_hash),
            state_root: crate::types::block::Hash64(state_root),
            tx_root: crate::types::block::Hash64(tx_root),
            transactions,
            latency_proof,
            availability_proof,
            group_proof,
            signature: crate::types::block::Hash64(signature),
            metadata,
        }
    }
}

impl ProposalTransaction {
    pub fn from(tx: Transaction) -> Self {
        Self {
            hash: tx.hash,
            from: tx.from,
            to: tx.to,
            amount: tx.amount,
            nonce: tx.nonce,
            fee: tx.fee,
            data: tx.data,
            signature: tx.signature,
        }
    }
}

// ── Tests for VRF proposer selection (C-08) ──────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_proposers(scores: &[u32]) -> Vec<ProposerInfo> {
        scores
            .iter()
            .enumerate()
            .map(|(i, &score)| ProposerInfo {
                node_id: format!("node_{}", i),
                peer_id: format!("peer_{}", i),
                public_key: [i as u8; 32],
                score,
                group_id: None,
                region: "test".to_string(),
                capabilities: vec![],
            })
            .collect()
    }

    #[test]
    fn test_vrf_select_deterministic() {
        let proposers = make_proposers(&[100, 200, 300]);
        let hash = [0u8; 64];
        let a = vrf_weighted_select(42, &hash, &proposers);
        let b = vrf_weighted_select(42, &hash, &proposers);
        assert_eq!(a, b, "same inputs must produce same output");
    }

    #[test]
    fn test_vrf_select_different_slots_vary() {
        let proposers = make_proposers(&[100, 200, 300, 400, 500]);
        let hash = [1u8; 64];
        let mut results = std::collections::HashSet::new();
        for slot in 0..100 {
            results.insert(vrf_weighted_select(slot, &hash, &proposers));
        }
        // With 5 proposers and 100 slots, we should see multiple different indices
        assert!(
            results.len() > 1,
            "VRF should produce varied selections across slots"
        );
    }

    #[test]
    fn test_vrf_select_different_hashes_vary() {
        let proposers = make_proposers(&[100, 200, 300]);
        let mut results = std::collections::HashSet::new();
        for seed in 0u8..50 {
            let mut hash = [0u8; 64];
            hash[0] = seed;
            results.insert(vrf_weighted_select(0, &hash, &proposers));
        }
        assert!(
            results.len() > 1,
            "different block hashes should produce different selections"
        );
    }

    #[test]
    fn test_vrf_select_weighted_distribution() {
        // One proposer with score 900, others with score 10
        let proposers = make_proposers(&[900, 10, 10, 10, 10]);
        let mut counts = vec![0u32; proposers.len()];
        for slot in 0u64..1000 {
            let mut hash = [0u8; 64];
            hash[..8].copy_from_slice(&slot.to_le_bytes());
            let idx = vrf_weighted_select(slot, &hash, &proposers);
            counts[idx] += 1;
        }
        // The high-score proposer (900/940 ≈ 95.7%) should be selected much more often
        assert!(
            counts[0] > counts[1] + counts[2] + counts[3] + counts[4],
            "high-score proposer should dominate: {:?}",
            counts
        );
    }

    #[test]
    fn test_vrf_select_single_proposer() {
        let proposers = make_proposers(&[500]);
        let hash = [42u8; 64];
        assert_eq!(vrf_weighted_select(0, &hash, &proposers), 0);
    }

    #[test]
    fn test_vrf_select_all_equal_scores() {
        let proposers = make_proposers(&[100, 100, 100, 100]);
        let mut counts = vec![0u32; 4];
        for slot in 0u64..1000 {
            let mut hash = [0u8; 64];
            hash[..8].copy_from_slice(&slot.to_le_bytes());
            let idx = vrf_weighted_select(slot, &hash, &proposers);
            counts[idx] += 1;
        }
        // With equal weights, distribution should be roughly uniform (each ~25%)
        for c in &counts {
            assert!(
                *c > 100,
                "each proposer should be selected at least 100 times: {:?}",
                counts
            );
        }
    }
}
