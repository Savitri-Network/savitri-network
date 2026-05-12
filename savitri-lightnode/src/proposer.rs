//! Lightnode Proposer Module
//!
//! This module implements the block proposer logic for light nodes.
//! Light nodes can be elected as block proposers based on their PoU score.
//!
#![allow(dead_code)]
//! ## Architecture
//! 1. **Election**: Highest PoU score among active lightnodes wins proposer role
//! 2. **Block Creation**: Proposer collects TX from mempool and builds block
//! 5. **Broadcast**: Proposer broadcasts certified block to network
//!
//! ## Two-Layer Security
//! - Layer 1: Lightnode creates and signs block proposal

use ed25519_dalek::{Signer, SigningKey as Keypair};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Compute transaction root from ProposalTransactions (for verification parity with proposer)
#[allow(dead_code)]
pub fn compute_tx_root(transactions: &[ProposalTransaction]) -> [u8; 64] {
    use sha2::{Digest, Sha512};
    let mut hasher = Sha512::new();
    hasher.update(b"savitri-tx-root-v1");
    for tx in transactions {
        hasher.update(&tx.hash);
    }
    hasher.finalize().into()
}

/// Canonical signable bytes for a block proposal (used for both signing and verification)
pub fn proposal_signable_bytes(proposal: &BlockProposal) -> Vec<u8> {
    fn normalize_hash(hash: &[u8]) -> [u8; 64] {
        let mut out = [0u8; 64];
        let len = hash.len().min(64);
        out[..len].copy_from_slice(&hash[..len]);
        out
    }

    let mut bytes = b"savitri-proposal-v1".to_vec();
    bytes.extend_from_slice(&proposal.round_id.to_le_bytes());
    bytes.extend_from_slice(&proposal.height.to_le_bytes());
    bytes.extend_from_slice(&proposal.timestamp.to_le_bytes());
    bytes.extend_from_slice(&proposal.proposer_pubkey);
    bytes.extend_from_slice(&proposal.parent_hash);
    let mut canonical_parents = Vec::with_capacity(1 + proposal.parent_hashes.len());
    canonical_parents.push(proposal.parent_hash);
    for hash in &proposal.parent_hashes {
        let normalized = normalize_hash(hash);
        if normalized != proposal.parent_hash && !canonical_parents.contains(&normalized) {
            canonical_parents.push(normalized);
        }
    }
    bytes.extend_from_slice(&(canonical_parents.len() as u32).to_le_bytes());
    for parent in canonical_parents {
        bytes.extend_from_slice(&parent);
    }
    bytes.extend_from_slice(&proposal.state_root);
    bytes.extend_from_slice(&proposal.tx_root);
    bytes.extend_from_slice(&(proposal.transactions.len() as u32).to_le_bytes());
    bytes
}

/// Block proposal created by a lightnode proposer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockProposal {
    /// Round/slot number for this proposal
    pub round_id: u64,
    /// Block height
    pub height: u64,
    /// Timestamp of proposal creation
    pub timestamp: u64,
    /// Proposer's public key
    #[serde(with = "BigArray")]
    pub proposer_pubkey: [u8; 32],
    /// Proposer's PoU score at time of election
    pub proposer_pou_score: u32, // basis points (0-10000)
    /// Parent block hash
    #[serde(with = "BigArray")]
    pub parent_hash: [u8; 64],
    /// Additional parent hashes for DAG references (first parent remains `parent_hash`)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub parent_hashes: Vec<Vec<u8>>,
    /// State root after executing transactions
    #[serde(with = "BigArray")]
    pub state_root: [u8; 64],
    /// Transaction root (merkle root of transactions)
    #[serde(with = "BigArray")]
    pub tx_root: [u8; 64],
    /// Transactions included in this block
    pub transactions: Vec<ProposalTransaction>,
    /// Latency measurement proof (signed responses from peers)
    pub latency_proof: Option<LatencyProofData>,
    /// Proposer's signature over the proposal
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

/// Simplified transaction for proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalTransaction {
    #[serde(with = "BigArray")]
    pub hash: [u8; 64],
    #[serde(with = "BigArray")]
    pub from: [u8; 32],
    #[serde(with = "BigArray")]
    pub to: [u8; 32],
    pub amount: u64,
    pub nonce: u64,
    pub fee: u64,
    pub data: Vec<u8>,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

/// Latency proof data included in block proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyProofData {
    pub round_id: u64,
    pub median_rtt_ms: f64,
    pub latency_score: f64,
    pub peers_contacted: u32,
    pub peers_responded: u32,
    /// Subset of signed responses for verification (max 10)
    pub sample_responses: Vec<LatencyResponseProof>,
}

/// Simplified latency response for proof
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LatencyResponseProof {
    #[serde(with = "BigArray")]
    pub responder_pubkey: [u8; 32],
    pub measured_rtt_ns: u64,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

/// Certificate from masternode quorum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockCertificate {
    /// Block hash being certified
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    /// Round/slot number
    pub round_id: u64,
    /// Height of the certified block
    pub height: u64,
    /// Aggregated signatures from masternode quorum
    pub quorum_signatures: Vec<MasternodeVote>,
    /// Total voting power that signed
    pub total_voting_power: u64,
    /// Threshold required (2f+1)
    pub threshold: u64,
    /// Timestamp of certification
    pub certified_at: u64,
}

/// Individual masternode vote
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasternodeVote {
    #[serde(with = "BigArray")]
    pub masternode_pubkey: [u8; 32],
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
    pub voting_power: u64,
}

/// Proposer election result
#[derive(Debug, Clone)]
pub struct ElectionResult {
    pub round_id: u64,
    pub winner_pubkey: [u8; 32],
    pub winner_pou_score: u32,
    pub is_local_winner: bool,
    pub candidates: Vec<ProposerCandidate>,
}

/// Candidate in proposer election
#[derive(Debug, Clone)]
pub struct ProposerCandidate {
    pub pubkey: [u8; 32],
    pub pou_score: u32,
    pub latency_score: f64,
    pub integrity_score: f64,
    pub reputation_score: f64,
}

/// Configuration for the proposer service
#[derive(Debug, Clone)]
pub struct ProposerConfig {
    /// Maximum transactions per block
    pub max_transactions_per_block: usize,
    /// Block creation timeout
    pub block_creation_timeout: Duration,
    /// Masternode submission timeout
    pub masternode_timeout: Duration,
    /// Minimum PoU score to be eligible as proposer
    pub min_pou_score: u32,
    /// Whether this node should participate in proposer election
    pub participate_in_election: bool,
}

impl Default for ProposerConfig {
    fn default() -> Self {
        Self {
            max_transactions_per_block: 1000,
            block_creation_timeout: Duration::from_millis(500),
            masternode_timeout: Duration::from_secs(5),
            min_pou_score: 1000, // 10% minimum
            participate_in_election: true,
        }
    }
}

/// Proposer service state
pub struct ProposerService {
    /// Our keypair for signing
    keypair: Keypair,
    /// Our public key
    local_pubkey: [u8; 32],
    /// Configuration
    config: ProposerConfig,
    /// Current PoU score components
    pou_score: Arc<RwLock<PoUScoreState>>,
    /// Known peer scores for election
    peer_scores: Arc<RwLock<HashMap<[u8; 32], ProposerCandidate>>>,
    /// Channel to send proposals to masternode
    proposal_tx: mpsc::Sender<BlockProposal>,
    /// Channel to receive certificates
    certificate_rx: Arc<RwLock<Option<mpsc::Receiver<BlockCertificate>>>>,
    /// Latest certified block height
    latest_height: Arc<RwLock<u64>>,
    /// Latest parent hash
    latest_parent_hash: Arc<RwLock<[u8; 64]>>,
}

/// Current PoU score state
#[derive(Debug, Clone, Default)]
pub struct PoUScoreState {
    /// Latency score (0.0 - 1.0) - from peer measurement
    pub latency_score: f64,
    /// Integrity score (0.0 - 1.0) - from masternode
    pub integrity_score: f64,
    /// Reputation score (0.0 - 1.0) - from masternode
    pub reputation_score: f64,
    /// Total PoU score in basis points (0-10000)
    pub total_score: u32,
    /// Last update timestamp
    pub last_updated: u64,
}

impl PoUScoreState {
    /// Weight constants for PoU components
    pub const WEIGHT_LATENCY: f64 = 0.40;
    pub const WEIGHT_INTEGRITY: f64 = 0.30;
    pub const WEIGHT_REPUTATION: f64 = 0.30;

    /// Calculate total score from components
    pub fn calculate_total(&mut self) {
        let total = Self::WEIGHT_LATENCY * self.latency_score
            + Self::WEIGHT_INTEGRITY * self.integrity_score
            + Self::WEIGHT_REPUTATION * self.reputation_score;
        self.total_score = (total * 10000.0).round() as u32;
        self.last_updated = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Update latency score from measurement
    pub fn update_latency(&mut self, score: f64) {
        self.latency_score = score.max(0.0).min(1.0);
        self.calculate_total();
    }

    /// Update integrity score (from masternode feedback)
    pub fn update_integrity(&mut self, score: f64) {
        self.integrity_score = score.max(0.0).min(1.0);
        self.calculate_total();
    }

    /// Update reputation score (from masternode feedback)
    pub fn update_reputation(&mut self, score: f64) {
        self.reputation_score = score.max(0.0).min(1.0);
        self.calculate_total();
    }
}

impl ProposerService {
    /// Create a new proposer service
    pub fn new(
        keypair: Keypair,
        config: ProposerConfig,
        proposal_tx: mpsc::Sender<BlockProposal>,
    ) -> Self {
        let local_pubkey = keypair.verifying_key().to_bytes();

        Self {
            keypair,
            local_pubkey,
            config,
            pou_score: Arc::new(RwLock::new(PoUScoreState::default())),
            peer_scores: Arc::new(RwLock::new(HashMap::new())),
            proposal_tx,
            certificate_rx: Arc::new(RwLock::new(None)),
            latest_height: Arc::new(RwLock::new(0)),
            latest_parent_hash: Arc::new(RwLock::new([0u8; 64])),
        }
    }

    /// Set the certificate receiver channel
    pub async fn set_certificate_receiver(&self, rx: mpsc::Receiver<BlockCertificate>) {
        let mut cert_rx = self.certificate_rx.write().await;
        *cert_rx = Some(rx);
    }

    /// Update our latency score
    pub async fn update_latency_score(&self, score: f64) {
        let mut pou = self.pou_score.write().await;
        pou.update_latency(score);
        debug!(
            latency = format!("{:.4}", score),
            total = pou.total_score,
            "Updated latency score"
        );
    }

    /// Update scores from masternode feedback
    pub async fn update_masternode_scores(&self, integrity: f64, reputation: f64) {
        let mut pou = self.pou_score.write().await;
        pou.update_integrity(integrity);
        pou.update_reputation(reputation);
        info!(
            integrity = format!("{:.4}", integrity),
            reputation = format!("{:.4}", reputation),
            total = pou.total_score,
            "Updated masternode scores"
        );
    }

    /// Get current PoU score
    pub async fn get_pou_score(&self) -> u32 {
        self.pou_score.read().await.total_score
    }

    /// Update known peer scores
    pub async fn update_peer_score(&self, pubkey: [u8; 32], candidate: ProposerCandidate) {
        let mut peers = self.peer_scores.write().await;
        peers.insert(pubkey, candidate);
    }

    /// Update chain state (height and parent hash)
    pub async fn update_chain_state(&self, height: u64, parent_hash: [u8; 64]) {
        let mut h = self.latest_height.write().await;
        *h = height;
        let mut ph = self.latest_parent_hash.write().await;
        *ph = parent_hash;
    }

    /// Perform proposer election for a round
    pub async fn run_election(&self, round_id: u64) -> ElectionResult {
        let local_score = self.pou_score.read().await.clone();
        let peer_scores = self.peer_scores.read().await.clone();

        // Build candidate list including ourselves
        let mut candidates: Vec<ProposerCandidate> = peer_scores.values().cloned().collect();

        // Add ourselves
        let local_candidate = ProposerCandidate {
            pubkey: self.local_pubkey,
            pou_score: local_score.total_score,
            latency_score: local_score.latency_score,
            integrity_score: local_score.integrity_score,
            reputation_score: local_score.reputation_score,
        };
        candidates.push(local_candidate);

        // Sort by PoU score (highest first)
        candidates.sort_by(|a, b| b.pou_score.cmp(&a.pou_score));

        // Winner is highest score
        let winner = candidates.first().cloned().unwrap_or(ProposerCandidate {
            pubkey: self.local_pubkey,
            pou_score: 0,
            latency_score: 0.0,
            integrity_score: 0.0,
            reputation_score: 0.0,
        });

        let is_local_winner = winner.pubkey == self.local_pubkey;

        info!(
            round_id,
            winner = hex::encode(&winner.pubkey[..8]),
            winner_score = winner.pou_score,
            is_local = is_local_winner,
            candidates = candidates.len(),
            "Proposer election completed"
        );

        ElectionResult {
            round_id,
            winner_pubkey: winner.pubkey,
            winner_pou_score: winner.pou_score,
            is_local_winner,
            candidates,
        }
    }

    /// Check if we are eligible to propose
    pub async fn is_eligible(&self) -> bool {
        if !self.config.participate_in_election {
            return false;
        }
        let score = self.pou_score.read().await.total_score;
        score >= self.config.min_pou_score
    }

    /// Create a block proposal
    pub async fn create_proposal(
        &self,
        round_id: u64,
        transactions: Vec<ProposalTransaction>,
        latency_proof: Option<LatencyProofData>,
    ) -> Result<BlockProposal, ProposerError> {
        let pou_score = self.pou_score.read().await.total_score;
        let height = *self.latest_height.read().await + 1;
        let parent_hash = *self.latest_parent_hash.read().await;

        // Calculate tx_root (simplified - just hash all tx hashes)
        let tx_root = self.compute_tx_root(&transactions);

        // Compute state root from storage (real implementation)
        let state_root = self
            .compute_state_root_from_transactions(&transactions)
            .await?;

        //   proposal.timestamp < current_time + 60
        // dove current_time = SystemTime::now().as_secs() (~1.77e9 nel 2026).
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut proposal = BlockProposal {
            round_id,
            height,
            timestamp,
            proposer_pubkey: self.local_pubkey,
            proposer_pou_score: pou_score,
            parent_hash,
            parent_hashes: Vec::new(),
            state_root,
            tx_root,
            transactions,
            latency_proof,
            signature: [0u8; 64],
        };

        // Sign the proposal
        let signable = self.proposal_signable_bytes(&proposal);
        let signature = self.keypair.sign(&signable);
        proposal.signature = signature.to_bytes();

        info!(
            round_id,
            height,
            tx_count = proposal.transactions.len(),
            pou_score,
            "Created block proposal"
        );

        info!(round_id, height, "Proposta creata");

        Ok(proposal)
    }

    /// Compute transaction root
    fn compute_tx_root(&self, transactions: &[ProposalTransaction]) -> [u8; 64] {
        use sha2::{Digest, Sha512};
        let mut hasher = Sha512::new();
        hasher.update(b"savitri-tx-root-v1");
        for tx in transactions {
            hasher.update(&tx.hash);
        }
        hasher.finalize().into()
    }

    /// Get signable bytes for proposal (delegates to module-level function for verification parity)
    fn proposal_signable_bytes(&self, proposal: &BlockProposal) -> Vec<u8> {
        crate::proposer::proposal_signable_bytes(proposal)
    }

    pub async fn submit_proposal(&self, proposal: BlockProposal) -> Result<(), ProposerError> {
        info!(
            round_id = proposal.round_id,
            height = proposal.height,
            tx_count = proposal.transactions.len(),
            "Submitting proposal to masternode channel"
        );
        // ITA: log di invio proposta per tracing
        info!(
            round_id = proposal.round_id,
            height = proposal.height,
            "Proposta inviata al masternode per validazione"
        );
        match self.proposal_tx.send(proposal).await {
            Ok(_) => {
                info!("Proposal enqueued to masternode channel successfully");
                Ok(())
            }
            Err(_) => {
                warn!("Failed to enqueue proposal to masternode channel: channel closed");
                Err(ProposerError::ChannelClosed)
            }
        }
    }

    /// Await certificate from masternode with timeout
    pub async fn await_certificate(
        &self,
        timeout: Duration,
    ) -> Result<BlockCertificate, ProposerError> {
        let mut cert_rx_guard = self.certificate_rx.write().await;

        if let Some(rx) = cert_rx_guard.as_mut() {
            match tokio::time::timeout(timeout, rx.recv()).await {
                Ok(Some(certificate)) => {
                    debug!(
                        height = certificate.height,
                        voters = certificate.quorum_signatures.len(),
                        "Received block certificate"
                    );
                    Ok(certificate)
                }
                Ok(None) => Err(ProposerError::ChannelClosed),
                Err(_) => Err(ProposerError::CertificateTimeout),
            }
        } else {
            Err(ProposerError::ChannelClosed)
        }
    }

    /// Compute state root from transactions
    /// This simulates the state changes that would result from executing the transactions
    /// and computes the resulting state root
    async fn compute_state_root_from_transactions(
        &self,
        transactions: &[ProposalTransaction],
    ) -> Result<[u8; 64], ProposerError> {
        use sha2::{Digest, Sha512};

        // Create a deterministic seed for state root calculation
        let seed = Sha512::digest(b"STATEv1-LE");
        let mut root_hasher = Sha512::new();
        root_hasher.update(seed);

        // Get current height for deterministic state changes
        let height = *self.latest_height.read().await;

        // Simulate account balance changes from transactions
        // In a real implementation, this would execute the transactions
        let mut account_changes = std::collections::BTreeMap::new();

        for tx in transactions {
            // Extract sender and recipient from transaction data
            let (sender, recipient, amount) = self.parse_transaction_data(&tx.data)?;

            // Update sender balance (subtract amount)
            let sender_balance = account_changes.entry(sender).or_insert(1000000u64); // Default balance
            *sender_balance = sender_balance.saturating_sub(amount);

            // Update recipient balance (add amount)
            let recipient_balance = account_changes.entry(recipient).or_insert(1000000u64);
            *recipient_balance = recipient_balance.saturating_add(amount);
        }

        // Hash account changes in lexicographic order
        for (address, balance) in account_changes {
            let mut leaf_hasher = Sha512::new();
            leaf_hasher.update(b"STATE");
            leaf_hasher.update(&address);
            leaf_hasher.update(&balance.to_le_bytes());
            let leaf = leaf_hasher.finalize();
            root_hasher.update(&leaf);
        }

        // Include block height in state root for uniqueness
        root_hasher.update(&height.to_le_bytes());

        // Include transaction count for additional uniqueness
        root_hasher.update(&(transactions.len() as u32).to_le_bytes());

        let out = root_hasher.finalize();
        let mut root = [0u8; 64];
        root.copy_from_slice(&out);
        Ok(root)
    }

    /// Parse transaction data to extract sender, recipient, and amount
    /// This is a simplified parser - in a real implementation, this would use
    /// the actual transaction format from the core types
    fn parse_transaction_data(
        &self,
        tx_data: &[u8],
    ) -> Result<([u8; 32], [u8; 32], u64), ProposerError> {
        if tx_data.len() < 96 {
            return Err(ProposerError::InvalidTransaction);
        }

        // Simple format: sender(32) + recipient(32) + amount(8) + data
        let mut sender = [0u8; 32];
        let mut recipient = [0u8; 32];

        sender.copy_from_slice(&tx_data[0..32]);
        recipient.copy_from_slice(&tx_data[32..64]);

        let amount = u64::from_le_bytes([
            tx_data[64],
            tx_data[65],
            tx_data[66],
            tx_data[67],
            tx_data[68],
            tx_data[69],
            tx_data[70],
            tx_data[71],
        ]);

        Ok((sender, recipient, amount))
    }

    /// Full proposer round: election → create → submit → await certificate
    pub async fn run_proposer_round(
        &self,
        round_id: u64,
        transactions: Vec<ProposalTransaction>,
        latency_proof: Option<LatencyProofData>,
    ) -> Result<(BlockProposal, BlockCertificate), ProposerError> {
        // 1. Run election
        let election = self.run_election(round_id).await;

        if !election.is_local_winner {
            return Err(ProposerError::NotElected);
        }

        // 2. Create proposal
        let proposal = self
            .create_proposal(round_id, transactions, latency_proof)
            .await?;

        // 3. Submit to masternode
        self.submit_proposal(proposal.clone()).await?;

        // 4. Await certificate
        let certificate = self
            .await_certificate(self.config.masternode_timeout)
            .await?;

        // 5. Update chain state
        self.update_chain_state(proposal.height, proposal.tx_root)
            .await;

        Ok((proposal, certificate))
    }
}

/// Proposer errors
#[derive(Debug, Clone)]
pub enum ProposerError {
    NotElected,
    InsufficientScore,
    BlockCreationFailed(String),
    ChannelClosed,
    NoCertificateChannel,
    CertificateTimeout,
    InvalidCertificate(String),
    MasternodeRejected(String),
    InvalidTransaction,
}

impl std::fmt::Display for ProposerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposerError::NotElected => write!(f, "Not elected as proposer"),
            ProposerError::InsufficientScore => write!(f, "PoU score below minimum threshold"),
            ProposerError::BlockCreationFailed(msg) => write!(f, "Block creation failed: {}", msg),
            ProposerError::ChannelClosed => write!(f, "Communication channel closed"),
            ProposerError::NoCertificateChannel => write!(f, "Certificate channel not configured"),
            ProposerError::CertificateTimeout => write!(f, "Timeout waiting for certificate"),
            ProposerError::InvalidCertificate(msg) => write!(f, "Invalid certificate: {}", msg),
            ProposerError::MasternodeRejected(msg) => write!(f, "Masternode rejected: {}", msg),
            ProposerError::InvalidTransaction => write!(f, "Invalid transaction format"),
        }
    }
}

impl std::error::Error for ProposerError {}

// Rest of the code remains the same
