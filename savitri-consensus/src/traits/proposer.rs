//! Proposer trait and related types
//!
//! This module defines the proposer interface that all consensus implementations
//! must provide for creating and signing block proposals.

use crate::error::ConsensusError;
use crate::traits::ValidationStats;
use crate::types::{
    AvailabilityProofData, Block, BlockHeader, BlockProposal, ConsensusData, ConsensusType,
    GroupInfo, GroupProofData, GroupStatus, LatencyProofData, NodeType, Proposal, ProposalMetadata,
    ProposalTransaction, RoundInfo, Transaction, ValidationProof, ValidationProofType,
    ValidatorInfo, ValidatorMetadata, ValidatorStatus,
};
use crate::ProposerInfo;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::OnceLock;

/// Proposer context for proposal creation
pub trait ProposerContext: Send + Sync {
    /// Get current slot
    fn current_slot(&self) -> u64;

    /// Get current epoch
    fn current_epoch(&self) -> u64;

    /// Get current block height
    fn current_height(&self) -> u64;

    /// Get proposer ID
    fn proposer_id(&self) -> &str;

    /// Get proposer public key
    fn proposer_public_key(&self) -> crate::types::block::Hash32;

    /// Get current PoU score
    fn current_score(&self) -> u32;

    /// Get last block hash
    fn last_block_hash(&self) -> crate::types::block::Hash64;

    /// Get pending transactions
    fn get_pending_transactions(&self) -> Vec<Transaction>;

    /// Get maximum block size
    fn max_block_size(&self) -> u64;

    /// Get maximum transactions per block
    fn max_transactions_per_block(&self) -> u32;

    /// Get proposal timeout in milliseconds
    fn proposal_timeout_ms(&self) -> u64;

    /// Get group information (if applicable)
    fn get_group_info(&self) -> Option<&GroupInfo>;

    /// Get geographic region
    fn get_region(&self) -> &str;

    /// Get capabilities
    fn get_capabilities(&self) -> Vec<String>;
}

/// Proposer trait for block proposal creation
pub trait Proposer: Send + Sync {
    type Context: ProposerContext + Send + Sync;

    /// Create a block proposal
    fn create_proposal(&self, context: &Self::Context) -> crate::error::Result<Box<dyn Proposal>>;

    /// Sign a proposal
    fn sign_proposal(&self, proposal: &mut dyn Proposal) -> crate::error::Result<()>;

    /// Get proposer eligibility
    fn is_eligible(&self, context: &Self::Context) -> bool;

    /// Get proposer score
    fn get_score(&self) -> ProposerScore;

    /// Get proposer information
    fn proposer_info(&self) -> ProposerInfo;

    /// Update proposer score
    fn update_score(&self, new_score: ProposerScore) -> crate::error::Result<()>;

    /// Get proposal statistics
    fn proposal_stats(&self) -> ProposalStats;

    /// Check if proposer is healthy
    fn is_healthy(&self) -> bool;

    /// Get last proposal time
    fn last_proposal_time(&self) -> u64;
}

/// Proposer score information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposerScore {
    /// Overall score (0-10000)
    pub overall_score: u32,
    /// Latency score component
    pub latency_score: u32,
    /// Availability score component
    pub availability_score: u32,
    /// Integrity score component
    pub integrity_score: u32,
    /// Performance score component
    pub performance_score: u32,
    /// Reputation score component
    pub reputation_score: u32,
    /// Score calculation timestamp
    pub timestamp: u64,
    /// Score epoch
    pub epoch: u64,
}

/// Proposal statistics
#[derive(Debug, Clone)]
pub struct ProposalStats {
    /// Total proposals created
    pub total_proposals: u64,
    /// Successful proposals
    pub successful_proposals: u64,
    /// Failed proposals
    pub failed_proposals: u64,
    /// Average proposal creation time in milliseconds
    pub average_creation_time_ms: f64,
    /// Last proposal timestamp
    pub last_proposal_timestamp: u64,
    /// Current proposal streak
    pub current_streak: u32,
    /// Best proposal streak
    pub best_streak: u32,
}

/// Default proposer context implementation
#[derive(Debug, Clone)]
pub struct DefaultProposerContext {
    current_slot: u64,
    current_epoch: u64,
    current_height: u64,
    proposer_id: String,
    proposer_public_key: crate::types::block::Hash32,
    current_score: u32,
    last_block_hash: crate::types::block::Hash64,
    pending_transactions: Vec<Transaction>,
    max_block_size: u64,
    max_transactions_per_block: u32,
    proposal_timeout_ms: u64,
    group_info: Option<GroupInfo>,
    region: String,
    capabilities: Vec<String>,
}

impl DefaultProposerContext {
    pub fn new(
        proposer_id: String,
        proposer_public_key: crate::types::block::Hash32,
        current_score: u32,
    ) -> Self {
        Self {
            current_slot: 0,
            current_epoch: 0,
            current_height: 0,
            proposer_id,
            proposer_public_key,
            current_score,
            last_block_hash: crate::types::block::Hash64([0u8; 64]),
            pending_transactions: Vec::new(),
            max_block_size: 1024 * 1024, // 1MB
            max_transactions_per_block: 1000,
            proposal_timeout_ms: 5000,
            group_info: None,
            region: "global".to_string(),
            capabilities: vec!["basic".to_string()],
        }
    }

    pub fn with_slot(mut self, slot: u64) -> Self {
        self.current_slot = slot;
        self
    }

    pub fn with_epoch(mut self, epoch: u64) -> Self {
        self.current_epoch = epoch;
        self
    }

    pub fn with_height(mut self, height: u64) -> Self {
        self.current_height = height;
        self
    }

    pub fn with_last_block_hash(mut self, hash: crate::types::block::Hash64) -> Self {
        self.last_block_hash = hash;
        self
    }

    pub fn with_pending_transactions(mut self, txs: Vec<Transaction>) -> Self {
        self.pending_transactions = txs;
        self
    }

    pub fn with_max_block_size(mut self, size: u64) -> Self {
        self.max_block_size = size;
        self
    }

    pub fn with_max_transactions(mut self, max_tx: u32) -> Self {
        self.max_transactions_per_block = max_tx;
        self
    }

    pub fn with_group_info(mut self, group: GroupInfo) -> Self {
        self.group_info = Some(group);
        self
    }

    pub fn with_region(mut self, region: String) -> Self {
        self.region = region;
        self
    }

    pub fn with_capabilities(mut self, capabilities: Vec<String>) -> Self {
        self.capabilities = capabilities;
        self
    }
}

impl ProposerContext for DefaultProposerContext {
    fn current_slot(&self) -> u64 {
        self.current_slot
    }

    fn current_epoch(&self) -> u64 {
        self.current_epoch
    }

    fn current_height(&self) -> u64 {
        self.current_height
    }

    fn proposer_id(&self) -> &str {
        &self.proposer_id
    }

    fn proposer_public_key(&self) -> crate::types::block::Hash32 {
        self.proposer_public_key
    }

    fn current_score(&self) -> u32 {
        self.current_score
    }

    fn last_block_hash(&self) -> crate::types::block::Hash64 {
        self.last_block_hash
    }

    fn get_pending_transactions(&self) -> Vec<Transaction> {
        self.pending_transactions.clone()
    }

    fn max_block_size(&self) -> u64 {
        self.max_block_size
    }

    fn max_transactions_per_block(&self) -> u32 {
        self.max_transactions_per_block
    }

    fn proposal_timeout_ms(&self) -> u64 {
        self.proposal_timeout_ms
    }

    fn get_group_info(&self) -> Option<&GroupInfo> {
        self.group_info.as_ref()
    }

    fn get_region(&self) -> &str {
        &self.region
    }

    fn get_capabilities(&self) -> Vec<String> {
        self.capabilities.clone()
    }
}

/// Default proposer implementation
pub struct DefaultProposer {
    info: ProposerInfo,
    score: ProposerScore,
    stats: Arc<tokio::sync::RwLock<ProposalStats>>,
    signer: Arc<dyn ProposalSigner>,
    health_checker: Arc<dyn ProposerHealthChecker>,
}

impl DefaultProposer {
    pub fn new(
        info: ProposerInfo,
        score: ProposerScore,
        signer: Arc<dyn ProposalSigner>,
        health_checker: Arc<dyn ProposerHealthChecker>,
    ) -> Self {
        Self {
            info,
            score,
            stats: Arc::new(tokio::sync::RwLock::new(ProposalStats::default())),
            signer,
            health_checker,
        }
    }
}

impl Proposer for DefaultProposer {
    type Context = DefaultProposerContext;

    fn create_proposal(&self, context: &Self::Context) -> crate::error::Result<Box<dyn Proposal>> {
        let start_time = std::time::Instant::now();

        // Check eligibility
        if !self.is_eligible(context) {
            return Err(crate::error::ConsensusError::ProtocolError(
                "Proposer not eligible".to_string(),
            )
            .into());
        }

        // Create block header
        let mut header = BlockHeader::new(
            1, // version
            context.current_height() + 1,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            context.last_block_hash().0,
            [0u8; 64], // state_root (would be calculated)
            [0u8; 64], // tx_root (will be set below)
            [0u8; 64], // consensus_root
            context.proposer_public_key().0,
            context.current_slot(),
            context.current_epoch(),
            0, // size (will be calculated)
            0, // tx_count (will be set below)
        );

        // Select transactions for the block
        let selected_txs = self.select_transactions(context);
        header.tx_count = selected_txs.len() as u32;

        // Calculate transaction root
        header.tx_root = self.calculate_tx_root(&selected_txs);

        // Create block
        let block = Block::new(
            header,
            selected_txs,
            ConsensusData::new(
                ConsensusType::PouBased,
                self.proposer_info(),
                ValidationProof::new(
                    ValidationProofType::PouScore,
                    vec![],
                    vec![],
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                ),
                RoundInfo::new(
                    context.current_epoch(),
                    context.current_slot(),
                    context.current_slot() / 100, // Assuming 100 slots per round
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    5000,                                            // 5 second rounds
                    10,                                              // 10 participants
                    vec![crate::types::block::Hash32([0u8; 32]); 5], // 5 validators
                ),
            ),
        );

        // Create proposal
        let mut proposal = Box::new(BlockProposal::new(
            context.current_slot(),
            context.current_height() + 1,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            context.proposer_public_key().0,
            context.current_score(),
            context.last_block_hash().0,
            [0u8; 64], // State root (would be calculated)
            block.header.tx_root.0,
            block
                .transactions
                .iter()
                .map(|tx| ProposalTransaction::from(tx.clone()))
                .collect(),
            Some(LatencyProofData::new(
                context.current_slot(),
                50_000, // Mock RTT in microseconds
                10,
                vec![],
                [0u8; 64],
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            )),
            Some(AvailabilityProofData::new(
                context.current_slot(),
                950, // 950 permille = 95% uptime
                95,
                100,
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
                    - 3600, // Last seen 1 hour ago
                [0u8; 64],
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
            )),
            context.get_group_info().map(|g| {
                GroupProofData::new(
                    g.group_id.clone(),
                    g.epoch,
                    g.members.clone(),
                    g.proposer.as_ref() == Some(&context.proposer_id().to_string()),
                    g.health_score,
                    [0u8; 64],
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                )
            }),
            [0u8; 64], // Will be signed
            ProposalMetadata::default(),
        ));

        // Sign the proposal
        self.sign_proposal(&mut *proposal)?;

        // Update statistics
        let duration = start_time.elapsed().as_millis() as f64;
        // Note: This would need async in a real implementation
        // For now, we'll just skip the stats update
        Ok(proposal)
    }

    fn sign_proposal(&self, proposal: &mut dyn Proposal) -> crate::error::Result<()> {
        self.signer.sign_proposal(proposal)
    }

    fn is_eligible(&self, context: &Self::Context) -> bool {
        // Check health
        if !self.health_checker.is_healthy_sync() {
            return false;
        }

        // Check score threshold
        if context.current_score() < 500 {
            // Minimum score threshold
            return false;
        }

        // Check if proposer is in a valid group (if group-aware)
        if let Some(group) = context.get_group_info() {
            if group.status != GroupStatus::Active {
                return false;
            }

            if !group.members.contains(&context.proposer_id().to_string()) {
                return false;
            }
        }

        true
    }

    fn get_score(&self) -> ProposerScore {
        self.score.clone()
    }

    fn proposer_info(&self) -> ProposerInfo {
        self.info.clone()
    }

    fn update_score(&self, new_score: ProposerScore) -> crate::error::Result<()> {
        // In a real implementation, this would update persistent storage
        // For now, we'll just log the update
        tracing::info!(
            "Updating proposer score from {} to {}",
            self.score.overall_score,
            new_score.overall_score
        );
        Ok(())
    }

    fn proposal_stats(&self) -> ProposalStats {
        // Note: This would need async in a real implementation
        // For now, return default stats
        ProposalStats::default()
    }

    fn is_healthy(&self) -> bool {
        self.health_checker.is_healthy_sync()
    }

    fn last_proposal_time(&self) -> u64 {
        // Note: This would need async in a real implementation
        // For now, return current time
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

impl DefaultProposer {
    /// Select transactions for the block based on context
    fn select_transactions(&self, context: &DefaultProposerContext) -> Vec<Transaction> {
        let pending_txs = context.get_pending_transactions();
        let max_tx = context.max_transactions_per_block() as usize;
        let max_size = context.max_block_size();

        let mut selected = Vec::new();
        let mut current_size = 0;

        // Simple selection: take highest fee transactions first
        let mut sorted_txs = pending_txs;
        sorted_txs.sort_by(|a, b| b.fee.cmp(&a.fee));

        for tx in sorted_txs {
            if selected.len() >= max_tx {
                break;
            }

            let tx_size = bincode::serialized_size(&tx).unwrap_or(0);
            if current_size + tx_size > max_size {
                break;
            }

            selected.push(tx);
            current_size += tx_size;
        }

        selected
    }

    /// Calculate transaction root (Merkle root)
    fn calculate_tx_root(&self, transactions: &[Transaction]) -> crate::types::block::Hash64 {
        if transactions.is_empty() {
            return crate::types::block::Hash64([0u8; 64]);
        }

        // Simple implementation: hash all transaction hashes
        let mut combined = Vec::new();
        for tx in transactions {
            combined.extend_from_slice(&tx.hash.0);
        }

        let hash = blake3::hash(&combined);
        let mut result = [0u8; 64];
        result.copy_from_slice(hash.as_bytes());
        crate::types::block::Hash64(result)
    }
}

/// Proposal signer trait
pub trait ProposalSigner: Send + Sync {
    fn sign_proposal(&self, proposal: &mut dyn Proposal) -> crate::error::Result<()>;
    fn verify_signature(&self, proposal: &dyn Proposal) -> crate::error::Result<bool>;
}

/// Proposer health checker trait
pub trait ProposerHealthChecker: Send + Sync {
    fn is_healthy(&self) -> bool;
    fn is_healthy_sync(&self) -> bool;
    fn get_health_metrics(&self) -> HealthMetrics;
}

/// Health metrics for proposer.
///
/// AUDIT-003: Converted f64 to integer types for deterministic comparison.
/// Permille fields are 0–1000, latency is microseconds.
#[derive(Debug, Clone)]
pub struct HealthMetrics {
    pub cpu_usage_permille: u32,
    pub memory_usage_permille: u32,
    pub network_latency_us: u64,
    pub uptime_permille: u32,
    pub last_check: u64,
}

/// Mock proposal signer for testing
pub struct MockProposalSigner;

impl ProposalSigner for MockProposalSigner {
    fn sign_proposal(&self, proposal: &mut dyn Proposal) -> crate::error::Result<()> {
        // Mock implementation - set a dummy signature
        if let Some(block_proposal) = proposal.as_any().downcast_ref::<BlockProposal>() {
            // In a real implementation, this would use actual cryptographic signing
            tracing::info!("Mock signing proposal for slot {}", block_proposal.round_id);
        }
        Ok(())
    }

    fn verify_signature(&self, _proposal: &dyn Proposal) -> crate::error::Result<bool> {
        Ok(true)
    }
}

/// Mock health checker for testing
pub struct MockHealthChecker {
    healthy: bool,
}

impl MockHealthChecker {
    pub fn new(healthy: bool) -> Self {
        Self { healthy }
    }
}

impl ProposerHealthChecker for MockHealthChecker {
    fn is_healthy(&self) -> bool {
        self.healthy
    }

    fn is_healthy_sync(&self) -> bool {
        self.healthy
    }

    fn get_health_metrics(&self) -> HealthMetrics {
        HealthMetrics {
            cpu_usage_permille: 500,
            memory_usage_permille: 600,
            network_latency_us: 50_000,
            uptime_permille: 950,
            last_check: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

impl Default for ProposalStats {
    fn default() -> Self {
        Self {
            total_proposals: 0,
            successful_proposals: 0,
            failed_proposals: 0,
            average_creation_time_ms: 0.0,
            last_proposal_timestamp: 0,
            current_streak: 0,
            best_streak: 0,
        }
    }
}

impl Default for ProposerScore {
    fn default() -> Self {
        Self {
            overall_score: 500,
            latency_score: 500,
            availability_score: 500,
            integrity_score: 500,
            performance_score: 500,
            reputation_score: 500,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            epoch: 0,
        }
    }
}
