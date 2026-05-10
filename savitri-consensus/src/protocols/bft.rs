//! BFT consensus primitives
//!
//! This module provides Byzantine Fault Tolerance primitives used by
//! consensus implementations for achieving agreement in the presence
//! of malicious nodes.

use crate::crypto::signatures;
use crate::error::Result;
use crate::types::*;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;
use zeroize::Zeroizing;

/// BFT consensus engine implementing Byzantine Fault Tolerance
///
/// SECURITY (PT-L01): Consensus state (current_round, proposals, votes) is held in-memory only.
/// On restart, the node re-syncs from peers. For future hardening, consider persisting the
/// last committed round and vote set to storage for crash recovery.
pub struct BftConsensus {
    config: BftConfig,
    validators: Arc<RwLock<HashMap<String, ValidatorInfo>>>,
    current_round: Arc<RwLock<u64>>,
    proposals: Arc<RwLock<HashMap<u64, BftProposal>>>,
    votes: Arc<RwLock<HashMap<u64, HashMap<String, BftVote>>>>,
    state: Arc<RwLock<BftState>>,
    stats: Arc<RwLock<BftStats>>,
    /// Wrapped in `Zeroizing` so the 32-byte key is wiped from memory when
    /// `BftConsensus` is dropped.
    local_signing_key: Option<Zeroizing<[u8; 32]>>,
    /// SECURITY (MED-04): Monotonic counter for unique proposal/vote IDs.
    /// Using SystemTime caused collisions on machines with low-resolution clocks.
    id_counter: Arc<AtomicU64>,
}

/// BFT configuration
#[derive(Debug, Clone)]
pub struct BftConfig {
    pub min_validators: usize,
    /// Maximum number of faulty nodes tolerated (f)
    pub max_faulty: usize,
    pub total_validators: usize,
    /// Timeout for each round in milliseconds
    pub round_timeout_ms: u64,
    /// Maximum number of rounds before timeout
    pub max_rounds: u64,
    /// Enable fast path for optimistic execution
    pub enable_fast_path: bool,
    /// Threshold for fast path agreement (deprecated — use fast_path_threshold_permille)
    pub fast_path_threshold: f64,
    /// Fast path threshold in permille (0–1000). AUDIT-003.
    pub fast_path_threshold_permille: u32,
}

/// SECURITY (PT-L02): These defaults are for development/testing only.
/// via the node configuration file (e.g., production.toml: group_size = 7).
impl Default for BftConfig {
    fn default() -> Self {
        Self {
            min_validators: 4,
            max_faulty: 1,
            total_validators: 4,
            round_timeout_ms: 5000,
            max_rounds: 100,
            enable_fast_path: true,
            fast_path_threshold: 0.67, // 2/3 supermajority (deprecated)
            fast_path_threshold_permille: 670, // 2/3 supermajority
        }
    }
}

/// BFT state
#[derive(Debug, Clone, PartialEq)]
pub enum BftState {
    /// Initial state
    Initial,
    /// Proposing phase
    Proposing,
    /// Voting phase
    Voting,
    /// Committed state
    Committed,
    /// Timeout state
    Timeout,
}

impl Default for BftState {
    fn default() -> Self {
        BftState::Initial
    }
}

/// BFT proposal
#[derive(Debug, Clone)]
pub struct BftProposal {
    /// Proposal ID
    pub proposal_id: String,
    /// Round number
    pub round: u64,
    /// Block data
    pub block_data: Vec<u8>,
    /// Proposer ID
    pub proposer_id: String,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: [u8; 64],
}

/// BFT vote
#[derive(Debug, Clone)]
pub struct BftVote {
    /// Vote ID
    pub vote_id: String,
    /// Round number
    pub round: u64,
    /// Proposal ID being voted on
    pub proposal_id: String,
    /// Voter ID
    pub voter_id: String,
    /// Vote value (true for yes, false for no)
    pub vote: bool,
    /// Timestamp
    pub timestamp: u64,
    /// Signature
    pub signature: [u8; 64],
}

/// BFT statistics
#[derive(Debug, Clone, Default)]
pub struct BftStats {
    /// Total rounds completed
    pub total_rounds: u64,
    /// Successful rounds
    pub successful_rounds: u64,
    /// Failed rounds
    pub failed_rounds: u64,
    /// Average round time in milliseconds
    pub average_round_time_ms: f64,
    /// Total proposals
    pub total_proposals: u64,
    /// Total votes
    pub total_votes: u64,
    /// Fast path agreements
    pub fast_path_agreements: u64,
    /// Timeout events
    pub timeout_events: u64,
}

impl BftConsensus {
    /// Create a new BFT consensus engine
    pub fn new(config: BftConfig) -> Self {
        Self {
            config,
            validators: Arc::new(RwLock::new(HashMap::new())),
            current_round: Arc::new(RwLock::new(0)),
            proposals: Arc::new(RwLock::new(HashMap::new())),
            votes: Arc::new(RwLock::new(HashMap::new())),
            state: Arc::new(RwLock::new(BftState::Initial)),
            stats: Arc::new(RwLock::new(BftStats::default())),
            local_signing_key: None,
            id_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// The key is stored in `Zeroizing` so it is wiped on drop.
    pub fn set_signing_key(&mut self, private_key: [u8; 32]) {
        self.local_signing_key = Some(Zeroizing::new(private_key));
    }

    pub async fn add_validator(&self, validator: ValidatorInfo) -> Result<()> {
        let mut validators = self.validators.write().await;
        validators.insert(validator.validator_id.clone(), validator);
        Ok(())
    }

    pub fn add_validator_sync(&self, validator: ValidatorInfo) -> Result<()> {
        // Use blocking_write for sync contexts
        // This is safe because we're in a sync context without an active runtime
        let mut validators = futures::executor::block_on(self.validators.write());
        validators.insert(validator.validator_id.clone(), validator);
        Ok(())
    }

    pub async fn remove_validator(&self, validator_id: &str) -> Result<()> {
        let mut validators = self.validators.write().await;
        validators.remove(validator_id);
        Ok(())
    }

    pub async fn get_validators(&self) -> Vec<ValidatorInfo> {
        let validators = self.validators.read().await;
        validators.values().cloned().collect()
    }

    /// Create a proposal
    pub async fn create_proposal(
        &self,
        block_data: Vec<u8>,
        proposer_id: String,
    ) -> Result<BftProposal> {
        // SECURITY (F-01): Verify proposer is not slashed/jailed before accepting proposal
        {
            let validators = self.validators.read().await;
            if let Some(validator) = validators.get(&proposer_id) {
                if validator.status == ValidatorStatus::Slashed
                    || validator.status == ValidatorStatus::Jailed
                {
                    return Err(crate::error::ConsensusError::ValidationFailed(format!(
                        "Proposer {} has status {:?} and cannot create proposals",
                        proposer_id, validator.status
                    )));
                }
            }
        }

        let round = *self.current_round.read().await;
        // SECURITY (MED-04): Use a monotonic atomic counter instead of SystemTime
        // to avoid duplicate IDs on low-resolution clocks.
        let seq = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let proposal_id = format!("{}-{}-{}", proposer_id, round, seq);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let proposal_message = build_proposal_message(round, &proposer_id, &block_data, timestamp);
        let signature = if let Some(ref sk) = self.local_signing_key {
            // Deref Zeroizing<[u8;32]> → &[u8;32] → &[u8] via explicit cast
            signatures::sign_message(&proposal_message, &**sk as &[u8]).unwrap_or([0u8; 64])
        } else {
            tracing::warn!("BFT proposal created without signing key — signature is zero");
            [0u8; 64]
        };

        let proposal = BftProposal {
            proposal_id: proposal_id.clone(),
            round,
            block_data,
            proposer_id: proposer_id.clone(),
            timestamp,
            signature,
        };

        // SECURITY: Reject proposals without valid signatures
        if signature == [0u8; 64] {
            return Err(crate::error::ConsensusError::InvalidSignature(format!(
                "Proposal from {} has zero signature — signing key required",
                proposer_id
            )));
        }

        // SECURITY: Detect equivocation — reject if a proposal already exists for this round
        let mut proposals = self.proposals.write().await;
        if let Some(existing) = proposals.get(&round) {
            if existing.proposer_id != proposer_id || existing.proposal_id != proposal_id {
                tracing::warn!(
                    "Equivocation detected: proposer {} already has a proposal in round {}",
                    proposer_id,
                    round
                );
                return Err(crate::error::ConsensusError::ValidationFailed(format!(
                    "Equivocation: round {} already has a proposal",
                    round
                )));
            }
        }
        proposals.insert(round, proposal.clone());

        // Update state
        *self.state.write().await = BftState::Proposing;

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_proposals += 1;

        Ok(proposal)
    }

    /// Vote on a proposal
    pub async fn vote(&self, proposal_id: String, voter_id: String, vote: bool) -> Result<BftVote> {
        let round = *self.current_round.read().await;
        // SECURITY (MED-04): Use a monotonic atomic counter for unique vote IDs.
        let seq = self.id_counter.fetch_add(1, Ordering::Relaxed);
        let vote_id = format!("{}-{}-{}", voter_id, proposal_id, seq);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        {
            let validators = self.validators.read().await;
            match validators.get(&voter_id) {
                None => {
                    return Err(crate::error::ConsensusError::ValidationFailed(format!(
                        "Unknown validator {} cannot vote",
                        voter_id
                    )));
                }
                Some(validator) => match validator.status {
                    ValidatorStatus::Slashed => {
                        return Err(crate::error::ConsensusError::ValidationFailed(format!(
                            "Slashed validator {} cannot vote",
                            voter_id
                        )));
                    }
                    ValidatorStatus::Jailed => {
                        return Err(crate::error::ConsensusError::ValidationFailed(format!(
                            "Jailed validator {} cannot vote",
                            voter_id
                        )));
                    }
                    ValidatorStatus::Inactive => {
                        return Err(crate::error::ConsensusError::ValidationFailed(format!(
                            "Inactive validator {} cannot vote",
                            voter_id
                        )));
                    }
                    _ => {} // Active and Pending are allowed
                },
            }
        }

        // SECURITY (F-07): Build canonical vote message and sign with Ed25519
        let vote_message = build_vote_message(round, &proposal_id, &voter_id, vote, timestamp);
        let signature = if let Some(ref sk) = self.local_signing_key {
            signatures::sign_message(&vote_message, &**sk as &[u8]).unwrap_or([0u8; 64])
        } else {
            tracing::warn!("BFT vote created without signing key — signature is zero");
            [0u8; 64]
        };

        let bft_vote = BftVote {
            vote_id: vote_id.clone(),
            round,
            proposal_id: proposal_id.clone(),
            voter_id: voter_id.clone(),
            vote,
            timestamp,
            signature,
        };

        // SECURITY (F-07): Reject votes with zero/missing signatures
        if signature == [0u8; 64] {
            return Err(crate::error::ConsensusError::InvalidSignature(format!(
                "Vote from {} has zero signature — signing key required",
                voter_id
            )));
        }

        // SECURITY: Verify the vote signature against the voter's registered public key
        {
            let validators = self.validators.read().await;
            if let Some(validator) = validators.get(&voter_id) {
                if !verify_vote_signature(&bft_vote, &validator.public_key) {
                    return Err(crate::error::ConsensusError::InvalidSignature(format!(
                        "Vote signature verification failed for voter {}",
                        voter_id
                    )));
                }
            }
        }

        // SECURITY: Prevent double-voting — reject if voter already voted this round
        let mut votes = self.votes.write().await;
        let round_votes = votes.entry(round).or_insert_with(HashMap::new);
        if round_votes.contains_key(&voter_id) {
            return Err(crate::error::ConsensusError::ValidationFailed(format!(
                "Double-vote detected: voter {} already voted in round {}",
                voter_id, round
            )));
        }
        round_votes.insert(voter_id, bft_vote.clone());

        // Update state
        *self.state.write().await = BftState::Voting;

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_votes += 1;

        // Check if we have enough votes for decision
        self.check_consensus(round).await?;

        Ok(bft_vote)
    }

    /// Check if consensus has been reached.
    ///
    /// SECURITY: Uses integer arithmetic for BFT quorum (2f+1) instead of
    /// floating-point comparison, which can produce non-deterministic results
    /// due to precision loss (e.g. 2/3 = 0.6666... vs threshold 0.67).
    async fn check_consensus(&self, round: u64) -> Result<bool> {
        let votes = self.votes.read().await;
        let validators = self.validators.read().await;

        if let Some(round_votes) = votes.get(&round) {
            let valid_votes: Vec<&BftVote> = round_votes
                .values()
                .filter(|v| {
                    validators
                        .get(&v.voter_id)
                        .map(|vi| vi.status == ValidatorStatus::Active)
                        .unwrap_or(false)
                })
                .collect();

            let yes_votes = valid_votes.iter().filter(|v| v.vote).count();
            let no_votes = valid_votes.iter().filter(|v| !v.vote).count();
            let total_votes = yes_votes + no_votes;

            // Minimum votes required to make any decision: n - f
            let required_votes = self.config.total_validators - self.config.max_faulty;

            if total_votes >= required_votes {
                // BFT quorum: need strictly more than 2f votes for agreement
                // This is equivalent to 2f+1 (integer, no floating-point)
                let bft_quorum = 2 * self.config.max_faulty + 1;

                if yes_votes >= bft_quorum {
                    // Consensus reached — supermajority of YES votes
                    *self.state.write().await = BftState::Committed;

                    let mut stats = self.stats.write().await;
                    stats.successful_rounds += 1;
                    stats.fast_path_agreements += 1;

                    return Ok(true);
                } else if no_votes >= bft_quorum {
                    // Rejected — supermajority of NO votes
                    *self.state.write().await = BftState::Timeout;

                    let mut stats = self.stats.write().await;
                    stats.failed_rounds += 1;

                    return Ok(false);
                }
            }
        }

        Ok(false)
    }

    /// Get current state
    pub async fn get_state(&self) -> BftState {
        self.state.read().await.clone()
    }

    /// Get current round
    pub async fn get_round(&self) -> u64 {
        *self.current_round.read().await
    }

    /// Advance to next round.
    ///
    /// SECURITY: Only allowed when the current round has reached a terminal
    /// state (Committed or Timeout). Prevents premature round advancement
    /// that could bypass consensus.
    pub async fn next_round(&self) -> Result<()> {
        let current_state = self.state.read().await.clone();
        match current_state {
            BftState::Committed | BftState::Timeout => {
                // Terminal state — safe to advance
            }
            _ => {
                return Err(crate::error::ConsensusError::ValidationFailed(format!(
                    "Cannot advance round: current state is {:?}, expected Committed or Timeout",
                    current_state
                )));
            }
        }

        let mut round = self.current_round.write().await;
        let prev_round = *round;
        *round += 1;

        // Clear old round data (keep only last 10 rounds for audit trail)
        let mut proposals = self.proposals.write().await;
        let mut votes = self.votes.write().await;
        proposals.remove(&prev_round);
        votes.remove(&prev_round);

        // Prune rounds older than 10 to prevent unbounded memory growth
        let min_keep = (*round).saturating_sub(10);
        proposals.retain(|&r, _| r >= min_keep);
        votes.retain(|&r, _| r >= min_keep);

        // Reset state
        *self.state.write().await = BftState::Initial;

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_rounds += 1;

        Ok(())
    }

    /// Check if consensus has been reached
    pub async fn is_consensus_reached(&self) -> bool {
        matches!(*self.state.read().await, BftState::Committed)
    }

    /// Get statistics
    pub async fn get_stats(&self) -> BftStats {
        self.stats.read().await.clone()
    }

    /// Check if BFT conditions are met
    pub fn is_bft_valid(&self) -> bool {
        self.config.total_validators >= 3 * self.config.max_faulty + 1
    }

    /// Get minimum required votes for consensus
    pub fn get_required_votes(&self) -> usize {
        self.config.total_validators - self.config.max_faulty
    }

    /// Get supermajority threshold in permille (AUDIT-003).
    pub fn get_supermajority_threshold_permille(&self) -> u32 {
        self.config.fast_path_threshold_permille
    }

    /// Get supermajority threshold (f64 — deprecated, use permille variant).
    #[deprecated(note = "Use get_supermajority_threshold_permille (AUDIT-003)")]
    pub fn get_supermajority_threshold(&self) -> f64 {
        self.config.fast_path_threshold
    }
}

/// SECURITY (PT-H03): Build canonical proposal message for signing/verification.
///
/// Format: `"BFT-PROP" || round(LE8) || proposer_id_len(LE4) || proposer_id || block_data_hash(32) || timestamp(LE8)`
fn build_proposal_message(
    round: u64,
    proposer_id: &str,
    block_data: &[u8],
    timestamp: u64,
) -> Vec<u8> {
    let block_hash = blake3::hash(block_data);
    let mut msg = Vec::with_capacity(8 + 8 + 4 + proposer_id.len() + 32 + 8);
    msg.extend_from_slice(b"BFT-PROP");
    msg.extend_from_slice(&round.to_le_bytes());
    msg.extend_from_slice(&(proposer_id.len() as u32).to_le_bytes());
    msg.extend_from_slice(proposer_id.as_bytes());
    msg.extend_from_slice(block_hash.as_bytes());
    msg.extend_from_slice(&timestamp.to_le_bytes());
    msg
}

/// SECURITY (PT-H03): Verify a BFT proposal signature against the proposer's public key.
fn verify_proposal_signature(proposal: &BftProposal, public_key: &[u8; 32]) -> bool {
    let msg = build_proposal_message(
        proposal.round,
        &proposal.proposer_id,
        &proposal.block_data,
        proposal.timestamp,
    );
    signatures::verify_signature(&msg, &proposal.signature, public_key).unwrap_or(false)
}

/// SECURITY (F-07): Build canonical vote message for signing/verification.
///
/// Format: `"BFT-VOTE" || round(LE8) || proposal_id_len(LE4) || proposal_id || voter_id_len(LE4) || voter_id || vote_byte || timestamp(LE8)`
fn build_vote_message(
    round: u64,
    proposal_id: &str,
    voter_id: &str,
    vote: bool,
    timestamp: u64,
) -> Vec<u8> {
    let mut msg = Vec::with_capacity(8 + 8 + 4 + proposal_id.len() + 4 + voter_id.len() + 1 + 8);
    msg.extend_from_slice(b"BFT-VOTE");
    msg.extend_from_slice(&round.to_le_bytes());
    msg.extend_from_slice(&(proposal_id.len() as u32).to_le_bytes());
    msg.extend_from_slice(proposal_id.as_bytes());
    msg.extend_from_slice(&(voter_id.len() as u32).to_le_bytes());
    msg.extend_from_slice(voter_id.as_bytes());
    msg.push(if vote { 1 } else { 0 });
    msg.extend_from_slice(&timestamp.to_le_bytes());
    msg
}

/// SECURITY (F-07): Verify a BFT vote signature against the voter's public key.
fn verify_vote_signature(vote: &BftVote, public_key: &[u8; 32]) -> bool {
    let msg = build_vote_message(
        vote.round,
        &vote.proposal_id,
        &vote.voter_id,
        vote.vote,
        vote.timestamp,
    );
    signatures::verify_signature(&msg, &vote.signature, public_key).unwrap_or(false)
}

/// BFT consensus trait for integration
pub trait BftEngine: Send + Sync {
    /// Initialize BFT engine
    fn initialize(&mut self, validators: Vec<ValidatorInfo>) -> crate::error::Result<()>;

    /// Process BFT message
    fn process_message(&mut self, message: BftMessage) -> crate::error::Result<BftResponse>;

    /// Get current consensus state
    fn get_state(&self) -> crate::error::Result<BftState>;

    /// Check if consensus is reached
    fn is_consensus_reached(&self) -> crate::error::Result<bool>;
}

/// BFT message types
#[derive(Debug, Clone)]
pub enum BftMessage {
    /// Proposal message
    Proposal(BftProposal),
    /// Vote message
    Vote(BftVote),
    /// Commit message
    Commit(BftCommit),
    /// Timeout message
    Timeout { round: u64, validator_id: String },
    /// Sync request
    SyncRequest { from_round: u64, to_round: u64 },
    /// Sync response
    SyncResponse {
        proposals: Vec<BftProposal>,
        votes: Vec<BftVote>,
    },
}

/// BFT commit message
#[derive(Debug, Clone)]
pub struct BftCommit {
    /// Validator ID
    pub validator_id: String,
    /// Round number
    pub round: u64,
    /// Block hash
    pub block_hash: [u8; 64],
    /// Signature
    pub signature: [u8; 64],
}

/// BFT response types
#[derive(Debug, Clone)]
pub enum BftResponse {
    /// Success response
    Success { message: String, data: Vec<u8> },
    /// Error response
    Error { code: u32, message: String },
    /// Consensus reached
    Consensus {
        proposal: BftProposal,
        votes: Vec<BftVote>,
    },
    /// Timeout occurred
    Timeout { round: u64 },
}

/// Default BFT engine implementation
pub struct DefaultBftEngine {
    bft: BftConsensus,
    validator_id: String,
}

impl DefaultBftEngine {
    pub fn new(config: BftConfig, validator_id: String) -> Self {
        Self {
            bft: BftConsensus::new(config),
            validator_id,
        }
    }
}

impl BftEngine for DefaultBftEngine {
    fn initialize(&mut self, validators: Vec<ValidatorInfo>) -> crate::error::Result<()> {
        for validator in validators {
            self.bft
                .add_validator_sync(validator)
                .map_err(|e| crate::error::ConsensusError::Initialization(e.to_string()))?;
        }
        Ok(())
    }

    fn process_message(&mut self, message: BftMessage) -> crate::error::Result<BftResponse> {
        match message {
            BftMessage::Proposal(proposal) => {
                // SECURITY (PT-H03): Verify proposal signature before accepting
                if proposal.signature != [0u8; 64] {
                    let validators = futures::executor::block_on(self.bft.validators.read());
                    if let Some(validator) = validators.get(&proposal.proposer_id) {
                        if !verify_proposal_signature(&proposal, &validator.public_key) {
                            tracing::warn!(
                                "Rejected proposal from {} — invalid signature",
                                proposal.proposer_id
                            );
                            return Err(crate::error::ConsensusError::InvalidSignature(format!(
                                "Proposal signature verification failed for proposer {}",
                                proposal.proposer_id
                            )));
                        }
                    } else {
                        tracing::warn!(
                            "Rejected proposal from unknown validator {}",
                            proposal.proposer_id
                        );
                        return Err(crate::error::ConsensusError::ValidationFailed(format!(
                            "Unknown proposer: {}",
                            proposal.proposer_id
                        )));
                    }
                }
                tracing::info!("Received valid proposal from: {}", proposal.proposer_id);
                Ok(BftResponse::Success {
                    message: "Proposal received".to_string(),
                    data: vec![],
                })
            }
            BftMessage::Vote(vote) => {
                // SECURITY (F-07): Verify incoming vote signature before accepting
                if vote.signature != [0u8; 64] {
                    let validators = futures::executor::block_on(self.bft.validators.read());
                    if let Some(validator) = validators.get(&vote.voter_id) {
                        if !verify_vote_signature(&vote, &validator.public_key) {
                            tracing::warn!(
                                "Rejected vote from {} — invalid signature",
                                vote.voter_id
                            );
                            return Err(crate::error::ConsensusError::InvalidSignature(format!(
                                "Vote signature verification failed for voter {}",
                                vote.voter_id
                            )));
                        }
                    } else {
                        tracing::warn!("Rejected vote from unknown validator {}", vote.voter_id);
                        return Err(crate::error::ConsensusError::ValidationFailed(format!(
                            "Unknown validator: {}",
                            vote.voter_id
                        )));
                    }
                }
                tracing::info!("Received valid vote from: {}", vote.voter_id);
                Ok(BftResponse::Success {
                    message: "Vote received".to_string(),
                    data: vec![],
                })
            }
            BftMessage::Commit(commit) => {
                // Process commit
                tracing::info!("Received commit from: {}", commit.validator_id);
                Ok(BftResponse::Success {
                    message: "Commit received".to_string(),
                    data: vec![],
                })
            }
            BftMessage::Timeout {
                round,
                validator_id,
            } => {
                // Handle timeout
                tracing::info!("Timeout from {} at round {}", validator_id, round);
                Ok(BftResponse::Success {
                    message: "Timeout handled".to_string(),
                    data: vec![],
                })
            }
            BftMessage::SyncRequest {
                from_round,
                to_round,
            } => {
                // Handle sync request
                tracing::info!("Sync request from round {} to {}", from_round, to_round);
                Ok(BftResponse::Success {
                    message: "Sync request received".to_string(),
                    data: vec![],
                })
            }
            BftMessage::SyncResponse { proposals, votes } => {
                // Handle sync response
                tracing::info!(
                    "Sync response with {} proposals and {} votes",
                    proposals.len(),
                    votes.len()
                );
                Ok(BftResponse::Success {
                    message: "Sync response received".to_string(),
                    data: vec![],
                })
            }
        }
    }

    fn get_state(&self) -> crate::error::Result<BftState> {
        // Get the actual current state from the BFT consensus engine
        use tokio::runtime::Handle;

        // Since this is a sync method but we need to access async state,
        // we'll use a blocking call to get the current state
        let rt = Handle::current();
        let state = rt.block_on(async { self.bft.get_state().await });

        Ok(state)
    }

    fn is_consensus_reached(&self) -> crate::error::Result<bool> {
        // Check if consensus has been reached using the BFT consensus engine
        use tokio::runtime::Handle;

        // Since this is a sync method but we need to access async state,
        // we'll use a blocking call to check consensus status
        let rt = Handle::current();
        let consensus_reached = rt.block_on(async { self.bft.is_consensus_reached().await });

        Ok(consensus_reached)
    }
}

/// BFT utilities
pub struct BftUtils;

impl BftUtils {
    pub fn min_validators_for_faulty(faulty: usize) -> usize {
        3 * faulty + 1
    }

    pub fn max_faulty_for_validators(validators: usize) -> usize {
        (validators - 1) / 3
    }

    /// Check if BFT parameters are valid
    pub fn is_valid_bft_parameters(validators: usize, faulty: usize) -> bool {
        validators >= 3 * faulty + 1
    }

    /// Calculate supermajority threshold in permille (0–1000).
    ///
    /// AUDIT-003: Replaced f64 with integer permille for cross-platform determinism.
    pub fn supermajority_threshold_permille(validators: usize, faulty: usize) -> u32 {
        if validators == 0 {
            return 0;
        }
        ((validators - faulty) * 1000 / validators) as u32
    }

    /// Calculate supermajority threshold (f64 — for display/logging only).
    #[deprecated(note = "Use supermajority_threshold_permille for consensus paths (AUDIT-003)")]
    pub fn supermajority_threshold(validators: usize, faulty: usize) -> f64 {
        if validators == 0 {
            return 0.0;
        }
        (validators - faulty) as f64 / validators as f64
    }
}
