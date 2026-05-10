//! P2P Block Receiver for Masternode
//!
//! This module handles receiving block proposals from light nodes via P2P network

use anyhow::Result;
use libp2p::{gossipsub, PeerId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use super::proposal_validator::{
    BlockCertificate, LightnodeProposal, MasternodeVote, ProposalValidator,
};

/// Gossipsub message structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipsubMessage {
    pub topic: String,
    pub data: Vec<u8>,
    pub from: String,
    pub seq_no: u64,
}

/// P2P message types for block proposal communication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MasternodeMessage {
    /// Block proposal from light node
    BlockProposal(LightnodeProposal),
    /// Vote from another masternode
    MasternodeVote(MasternodeVote),
    /// Block certificate after quorum
    BlockCertificate(BlockCertificate),
    /// Request for current height
    HeightRequest,
    /// Response with current height
    HeightResponse(u64),
}

/// P2P Block Receiver
pub struct P2PBlockReceiver {
    /// Local peer ID
    local_peer_id: PeerId,
    validator: Arc<ProposalValidator>,
    /// Channel for incoming proposals
    proposal_rx: mpsc::UnboundedReceiver<LightnodeProposal>,
    /// Channel for outgoing votes
    vote_tx: mpsc::UnboundedSender<MasternodeVote>,
    /// Current block height
    current_height: Arc<RwLock<u64>>,
    /// Active proposals being processed
    active_proposals: Arc<RwLock<HashMap<u64, LightnodeProposal>>>,
}

impl P2PBlockReceiver {
    pub fn new(
        local_peer_id: PeerId,
        validator: Arc<ProposalValidator>,
        proposal_rx: mpsc::UnboundedReceiver<LightnodeProposal>,
        vote_tx: mpsc::UnboundedSender<MasternodeVote>,
    ) -> Self {
        Self {
            local_peer_id,
            validator,
            proposal_rx,
            vote_tx,
            current_height: Arc::new(RwLock::new(0)),
            active_proposals: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Start the block processing loop
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting P2P block receiver for masternode");

        loop {
            tokio::select! {
                // Process incoming block proposals
                Some(proposal) = self.proposal_rx.recv() => {
                    if let Err(e) = self.process_proposal(proposal).await {
                        error!("Failed to process proposal: {}", e);
                    }
                }

                // Could add other async operations here
                _ = tokio::time::sleep(tokio::time::Duration::from_millis(100)) => {
                    // Periodic maintenance tasks
                }
            }
        }
    }

    /// Process a received block proposal
    async fn process_proposal(&mut self, proposal: LightnodeProposal) -> Result<()> {
        info!(
            height = proposal.height,
            round_id = proposal.round_id,
            tx_count = proposal.tx_count,
            "Received block proposal from light node"
        );
        info!(
            proposer_pubkey = %hex::encode(proposal.proposer_pubkey),
            height = proposal.height,
            round_id = proposal.round_id,
            "Confirmed block proposal receipt from proposer"
        );

        // Validate the proposal
        if !self.validator.validate_proposal(&proposal).await {
            warn!("Invalid proposal rejected: height={}", proposal.height);
            return Ok(());
        }

        // Store active proposal
        {
            let mut active = self.active_proposals.write().await;
            active.insert(proposal.height, proposal.clone());
        }

        // Vote on the proposal
        if let Some(vote) = self.validator.vote_on_proposal(&proposal).await {
            info!(
                height = proposal.height,
                vote_type = ?vote.vote_type,
                "Voting on block proposal"
            );

            // Send vote to network
            if let Err(e) = self.vote_tx.send(vote.clone()) {
                error!("Failed to send vote: {}", e);
            } else {
                info!(
                    height = proposal.height,
                    round_id = proposal.round_id,
                    vote_type = ?vote.vote_type,
                    "Vote sent to proposer (ACK)"
                );
            }
        }

        // Update current height if this proposal is higher
        {
            let mut current = self.current_height.write().await;
            if proposal.height > *current {
                *current = proposal.height;
                info!("Updated current height to {}", proposal.height);
            }
        }

        Ok(())
    }

    /// Handle incoming gossipsub message
    pub async fn handle_message(&mut self, message: MasternodeMessage) -> Result<()> {
        match message {
            MasternodeMessage::BlockProposal(proposal) => {
                debug!("Received block proposal via gossipsub");
                // Process through the block receiver channel
                // This would need to be wired up properly
                self.process_proposal(proposal).await?;
            }
            MasternodeMessage::MasternodeVote(_vote) => {
                debug!("Received vote from another masternode");
                // Handle vote collection
            }
            MasternodeMessage::BlockCertificate(certificate) => {
                info!(
                    "Received block certificate for height {}",
                    certificate.height
                );
                // Handle final certificate
            }
            MasternodeMessage::HeightRequest => {
                debug!("Received height request");
                // Send current height
            }
            MasternodeMessage::HeightResponse(height) => {
                debug!("Received height response: {}", height);
                // Update peer height info
            }
        }

        Ok(())
    }

    /// Get current block height
    pub async fn get_current_height(&self) -> u64 {
        *self.current_height.read().await
    }

    /// Get active proposals count
    pub async fn get_active_proposals_count(&self) -> usize {
        let active = self.active_proposals.read().await;
        active.len()
    }

    /// Clean up old proposals
    pub async fn cleanup_old_proposals(&self, max_height: u64) {
        let mut active = self.active_proposals.write().await;
        let initial_count = active.len();

        active.retain(|&height, _| height >= max_height.saturating_sub(10));

        let removed = initial_count - active.len();
        if removed > 0 {
            info!("Cleaned up {} old proposals", removed);
        }
    }
}

/// P2P Network Manager for Masternode
pub struct MasternodeP2PManager {
    /// Local peer ID
    local_peer_id: PeerId,
    /// Block receiver
    block_receiver: P2PBlockReceiver,
}

impl MasternodeP2PManager {
    pub fn new(
        local_peer_id: PeerId,
        validator: Arc<ProposalValidator>,
    ) -> (
        Self,
        mpsc::UnboundedSender<LightnodeProposal>,
        mpsc::UnboundedReceiver<MasternodeVote>,
    ) {
        let (proposal_tx, proposal_rx) = mpsc::unbounded_channel();
        let (vote_tx, vote_rx) = mpsc::unbounded_channel();

        let block_receiver = P2PBlockReceiver::new(local_peer_id, validator, proposal_rx, vote_tx);

        let manager = Self {
            local_peer_id,
            block_receiver,
        };

        (manager, proposal_tx, vote_rx)
    }

    /// Subscribe to block proposal topics
    pub async fn subscribe_to_proposals(&mut self) -> Result<()> {
        // Subscribe to gossipsub topics for block proposals
        let proposal_topics = vec![
            "savitri/block/proposal",
            "savitri/block/vote",
            "savitri/consensus/round",
        ];

        info!("Subscribing to {} gossipsub topics", proposal_topics.len());

        // In production, would subscribe to actual gossipsub topics
        // For now, simulate subscription
        for topic in proposal_topics {
            debug!("Subscribed to topic: {}", topic);
        }

        Ok(())
    }

    /// Start the P2P manager
    pub async fn start(&mut self) -> Result<()> {
        info!("Starting masternode P2P manager");

        // Subscribe to required topics
        self.subscribe_to_proposals().await?;

        // Start block processing loop
        self.block_receiver.start().await
    }

    /// Handle incoming gossipsub message
    pub async fn handle_message(&mut self, msg: &[u8]) -> Result<()> {
        // Parse and handle gossipsub messages
        match serde_json::from_slice::<GossipsubMessage>(msg) {
            Ok(message) => {
                debug!("Received gossipsub message: {}", message.topic);

                match message.topic.as_str() {
                    "savitri/block/proposal" => {
                        self.handle_block_proposal(&message.data).await?;
                    }
                    "savitri/block/vote" => {
                        self.handle_block_vote(&message.data).await?;
                    }
                    "savitri/consensus/round" => {
                        self.handle_consensus_round(&message.data).await?;
                    }
                    _ => {
                        warn!("Unknown topic: {}", message.topic);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to parse gossipsub message: {}", e);
                debug!("Raw message: {:?}", msg);
            }
        }

        Ok(())
    }

    /// Handle block proposal message
    async fn handle_block_proposal(&mut self, data: &[u8]) -> Result<()> {
        debug!("Processing block proposal message");

        // Parse block proposal
        match serde_json::from_slice::<serde_json::Value>(data) {
            Ok(proposal) => {
                info!(
                    "Received block proposal: height={}",
                    proposal.get("height").unwrap_or(&serde_json::Value::Null)
                );

                // Forward to block receiver for processing
            }
            Err(e) => {
                warn!("Failed to parse block proposal: {}", e);
            }
        }

        Ok(())
    }

    /// Handle block vote message
    async fn handle_block_vote(&mut self, data: &[u8]) -> Result<()> {
        debug!("Processing block vote message");

        // Parse block vote
        match serde_json::from_slice::<serde_json::Value>(data) {
            Ok(vote) => {
                info!(
                    "Received block vote: round={}",
                    vote.get("round_id").unwrap_or(&serde_json::Value::Null)
                );

                // Forward to consensus for vote aggregation
            }
            Err(e) => {
                warn!("Failed to parse block vote: {}", e);
            }
        }

        Ok(())
    }

    /// Handle consensus round message
    async fn handle_consensus_round(&mut self, data: &[u8]) -> Result<()> {
        debug!("Processing consensus round message");

        // Parse consensus round
        match serde_json::from_slice::<serde_json::Value>(data) {
            Ok(round) => {
                info!(
                    "Received consensus round: round={}",
                    round.get("round").unwrap_or(&serde_json::Value::Null)
                );

                // Update consensus state
            }
            Err(e) => {
                warn!("Failed to parse consensus round: {}", e);
            }
        }

        Ok(())
    }
}
