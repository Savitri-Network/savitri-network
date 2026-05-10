//! Group-Aware Consensus Messaging
//!
//! Implements consensus message routing and handling for group-based
//! consensus communication with priority queuing and reliable delivery.

use anyhow::Result;
use ed25519_dalek::{Signer, SigningKey};
use libp2p::PeerId;
use rand::rngs::OsRng;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

use super::group_manager::{
    GroupInfo, GroupMessage, GroupMessageType, GroupStatus, MessagePriority, P2PGroupManager,
};
use serde_json;

/// Group proposal structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupProposal {
    pub proposed_group_id: String,
    pub proposer_members: Vec<PeerId>,
    pub required_members: usize,
    pub group_config: GroupConfig,
    pub proposal_data: Vec<u8>,
    pub timestamp: u64,
}

/// Group configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupConfig {
    pub max_size: usize,
    pub consensus_algorithm: String,
    pub timeout_seconds: u64,
    pub retry_attempts: u32,
}

/// Block proposal structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockProposal {
    pub group_id: String,
    pub block_hash: Vec<u8>,
    pub height: u64,
    pub proposer: PeerId,
    pub block_data: Vec<u8>,
    pub timestamp: u64,
    pub gas_limit: u64,
}

/// Block vote structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockVote {
    pub block_hash: Vec<u8>,
    pub height: u64,
    pub voter: PeerId,
    pub vote_type: VoteType,
    pub timestamp: u64,
    pub signature: Vec<u8>,
    pub reason: Option<String>,
}

/// Vote type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum VoteType {
    Approve,
    Reject,
}

#[derive(Debug, Clone)]
pub struct BlockValidationResult {
    pub is_valid: bool,
    pub reason: String,
}

impl BlockValidationResult {
    pub fn valid() -> Self {
        Self {
            is_valid: true,
            reason: "Valid proposal".to_string(),
        }
    }

    pub fn invalid(reason: &str) -> Self {
        Self {
            is_valid: false,
            reason: reason.to_string(),
        }
    }
}

/// Message routing configuration
#[derive(Debug, Clone)]
pub struct MessageRoutingConfig {
    /// Enable priority queuing
    pub enable_priority_queuing: bool,
    /// Maximum queue size per priority
    pub max_queue_size_per_priority: usize,
    /// Message timeout in seconds
    pub message_timeout_secs: u64,
    /// Enable message acknowledgment
    pub enable_acknowledgment: bool,
    /// Retry attempts for failed messages
    pub max_retry_attempts: u32,
    /// Enable message compression
    pub enable_compression: bool,
    /// Batch message sending
    pub enable_batch_sending: bool,
    /// Batch size
    pub batch_size: usize,
}

impl Default for MessageRoutingConfig {
    fn default() -> Self {
        Self {
            enable_priority_queuing: true,
            max_queue_size_per_priority: 5000,
            message_timeout_secs: 30,
            enable_acknowledgment: true,
            max_retry_attempts: 3,
            enable_compression: true,
            enable_batch_sending: true,
            batch_size: 10,
        }
    }
}

/// Group consensus message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupConsensusMessage {
    pub message_id: String,
    pub group_id: String,
    pub sender: PeerId,
    pub message_type: ConsensusMessageType,
    pub payload: Vec<u8>,
    pub timestamp: u64,
    pub priority: MessagePriority,
    pub requires_ack: bool,
    pub retry_count: u32,
}

/// Consensus message types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConsensusMessageType {
    /// Group formation proposal
    GroupProposal,
    /// Group formation vote
    GroupVote,
    /// Proposer election
    ProposerElection,
    /// Block proposal
    BlockProposal,
    /// Block vote
    BlockVote,
    /// Consensus certificate
    ConsensusCertificate,
    /// Heartbeat
    Heartbeat,
    /// Sync request
    SyncRequest,
    /// Sync response
    SyncResponse,
}

/// Message priority levels (re-export for convenience)

/// Routing statistics
#[derive(Debug, Clone, Default)]
pub struct RoutingStats {
    pub total_messages_routed: u64,
    pub messages_by_priority: HashMap<MessagePriority, u64>,
    pub failed_messages: u64,
    pub retried_messages: u64,
    pub average_routing_time_ms: f64,
    pub queue_sizes: HashMap<MessagePriority, usize>,
    pub acknowledgments_sent: u64,
    pub acknowledgments_received: u64,
}

/// Consensus Message Router
pub struct ConsensusMessageRouter {
    config: MessageRoutingConfig,
    local_peer_id: PeerId,
    group_manager: Arc<P2PGroupManager>,
    message_queues: Arc<RwLock<HashMap<MessagePriority, VecDeque<GroupConsensusMessage>>>>,
    pending_messages: Arc<RwLock<HashMap<String, GroupConsensusMessage>>>,
    stats: Arc<RwLock<RoutingStats>>,
    message_tx: mpsc::UnboundedSender<GroupConsensusMessage>,
    message_rx: Arc<RwLock<mpsc::UnboundedReceiver<GroupConsensusMessage>>>,
    signing_key: SigningKey,
    /// Track votes already emitted: (height, block_hash_prefix) to avoid duplicate voting
    emitted_votes: Arc<RwLock<std::collections::HashSet<(u64, Vec<u8>)>>>,
}

impl ConsensusMessageRouter {
    pub fn new(
        config: MessageRoutingConfig,
        local_peer_id: PeerId,
        group_manager: Arc<P2PGroupManager>,
    ) -> Self {
        let (message_tx, message_rx) = mpsc::unbounded_channel();

        let mut message_queues = HashMap::new();
        message_queues.insert(MessagePriority::Critical, VecDeque::new());
        message_queues.insert(MessagePriority::High, VecDeque::new());
        message_queues.insert(MessagePriority::Normal, VecDeque::new());
        message_queues.insert(MessagePriority::Low, VecDeque::new());

        // Generate signing key for this node
        let signing_key = SigningKey::generate(&mut OsRng);

        Self {
            config,
            local_peer_id,
            group_manager,
            message_queues: Arc::new(RwLock::new(message_queues)),
            pending_messages: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(RoutingStats::default())),
            message_tx,
            message_rx: Arc::new(RwLock::new(message_rx)),
            signing_key,
            emitted_votes: Arc::new(RwLock::new(std::collections::HashSet::new())),
        }
    }

    /// Send consensus message to group
    pub async fn send_consensus_message(
        &self,
        group_id: &str,
        message_type: ConsensusMessageType,
        payload: Vec<u8>,
        priority: MessagePriority,
    ) -> Result<String> {
        let message_id = format!(
            "consensus_{}_{}_{}",
            group_id,
            self.local_peer_id,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        );

        let message = GroupConsensusMessage {
            message_id: message_id.clone(),
            group_id: group_id.to_string(),
            sender: self.local_peer_id,
            message_type,
            payload,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
            priority,
            requires_ack: self.config.enable_acknowledgment,
            retry_count: 0,
        };

        // Add to appropriate queue
        if self.config.enable_priority_queuing {
            let mut queues = self.message_queues.write().await;
            if let Some(queue) = queues.get_mut(&priority) {
                if queue.len() < self.config.max_queue_size_per_priority {
                    queue.push_back(message);
                } else {
                    warn!("Message queue full for priority {:?}", priority);
                    return Err(anyhow::anyhow!("Message queue full"));
                }
            }
        } else {
            // Send immediately
            self.send_message_immediate(message).await?;
        }

        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_messages_routed += 1;
        *stats.messages_by_priority.entry(priority).or_insert(0) += 1;

        Ok(message_id)
    }

    /// Send message immediately (bypass queue)
    async fn send_message_immediate(&self, message: GroupConsensusMessage) -> Result<()> {
        // Add to pending messages
        {
            let mut pending = self.pending_messages.write().await;
            pending.insert(message.message_id.clone(), message.clone());
        }

        // Convert to GroupMessage and send via group manager
        let group_message = GroupMessage {
            message_id: message.message_id.clone(),
            group_id: message.group_id.clone(),
            sender: message.sender,
            recipient: None, // Broadcast to group
            message_type: self.convert_consensus_to_group_type(message.message_type),
            payload: message.payload,
            timestamp: message.timestamp,
            priority: message.priority,
        };

        self.group_manager
            .send_group_message(
                &message.group_id,
                group_message.message_type,
                group_message.payload,
                group_message.priority,
            )
            .await?;

        Ok(())
    }

    /// Convert consensus message type to group message type
    fn convert_consensus_to_group_type(
        &self,
        consensus_type: ConsensusMessageType,
    ) -> GroupMessageType {
        match consensus_type {
            ConsensusMessageType::GroupProposal => GroupMessageType::GroupFormation,
            ConsensusMessageType::GroupVote => GroupMessageType::ConsensusVote,
            ConsensusMessageType::ProposerElection => GroupMessageType::ProposerElection,
            ConsensusMessageType::BlockProposal => GroupMessageType::BlockProposal,
            ConsensusMessageType::BlockVote => GroupMessageType::ConsensusVote,
            ConsensusMessageType::ConsensusCertificate => {
                GroupMessageType::Custom("consensus_certificate".to_string())
            }
            ConsensusMessageType::Heartbeat => GroupMessageType::HealthCheck,
            ConsensusMessageType::SyncRequest => {
                GroupMessageType::Custom("sync_request".to_string())
            }
            ConsensusMessageType::SyncResponse => {
                GroupMessageType::Custom("sync_response".to_string())
            }
        }
    }

    /// Handle received consensus message
    pub async fn handle_received_consensus_message(
        &self,
        message: GroupConsensusMessage,
    ) -> Result<()> {
        // Update stats
        let mut stats = self.stats.write().await;
        stats.total_messages_routed += 1;

        // Send acknowledgment if required
        if message.requires_ack {
            if let Err(e) = self.send_acknowledgment(&message).await {
                error!("Failed to send acknowledgment: {}", e);
            }
        }

        // Process message based on type
        match message.message_type {
            ConsensusMessageType::GroupProposal => {
                self.handle_group_proposal(message).await?;
            }
            ConsensusMessageType::BlockProposal => {
                self.handle_block_proposal(message).await?;
            }
            ConsensusMessageType::Heartbeat => {
                self.handle_heartbeat(message).await?;
            }
            _ => {
                debug!(
                    "Received consensus message type: {:?}",
                    message.message_type
                );
            }
        }

        Ok(())
    }

    /// Handle group proposal message
    async fn handle_group_proposal(&self, message: GroupConsensusMessage) -> Result<()> {
        info!(
            message_id = %message.message_id,
            sender = %message.sender,
            "Handling group proposal"
        );

        // Validate the group proposal
        if message.payload.is_empty() {
            warn!("Group proposal payload is empty");
            return Err(anyhow::anyhow!("Empty group proposal payload"));
        }

        // Deserialize the group proposal payload
        let proposal = match serde_json::from_slice::<GroupProposal>(&message.payload) {
            Ok(proposal) => proposal,
            Err(e) => {
                error!("Failed to deserialize group proposal: {}", e);
                return Err(anyhow::anyhow!("Invalid group proposal format"));
            }
        };

        // Validate proposal data
        if proposal.proposed_group_id.is_empty() {
            return Err(anyhow::anyhow!("Empty group ID in proposal"));
        }

        if proposal.proposer_members.is_empty() {
            return Err(anyhow::anyhow!("No proposer members in proposal"));
        }

        if proposal.required_members == 0 {
            return Err(anyhow::anyhow!("Invalid required members count"));
        }

        if proposal.proposer_members.len() > proposal.required_members {
            return Err(anyhow::anyhow!("Too many proposer members"));
        }

        // Check if we're already part of this group
        let current_groups: Vec<GroupInfo> = self.group_manager.get_active_groups().await;
        if current_groups
            .iter()
            .any(|g| g.group_id == proposal.proposed_group_id)
        {
            warn!(
                "Group {} already exists, rejecting proposal",
                proposal.proposed_group_id
            );
            return Err(anyhow::anyhow!("Group already exists"));
        }

        // Check if we should join this group
        let should_join = self.should_join_group(&proposal).await?;

        if should_join {
            // Send acknowledgment if required
            if message.requires_ack {
                self.send_acknowledgment(&message).await?;
            }

            // Add ourselves to the group
            self.group_manager
                .register_group(
                    proposal.proposed_group_id.clone(),
                    vec![self.local_peer_id, message.sender],
                    Some(message.sender),
                )
                .await?;

            info!(
                "Joined group {} from proposal by {}",
                proposal.proposed_group_id, message.sender
            );

            // Broadcast our acceptance to other group members
            self.broadcast_group_acceptance(&proposal).await?;
        } else {
            info!(
                "Rejected group proposal {} from {}",
                proposal.proposed_group_id, message.sender
            );
        }

        Ok(())
    }

    /// Handle block proposal message
    async fn handle_block_proposal(&self, message: GroupConsensusMessage) -> Result<()> {
        info!(
            message_id = %message.message_id,
            sender = %message.sender,
            "Handling block proposal"
        );

        // Validate the block proposal
        if message.payload.is_empty() {
            warn!("Block proposal payload is empty");
            return Err(anyhow::anyhow!("Empty block proposal payload"));
        }

        // Deserialize the block proposal payload
        let proposal = match serde_json::from_slice::<BlockProposal>(&message.payload) {
            Ok(proposal) => proposal,
            Err(e) => {
                error!("Failed to deserialize block proposal: {}", e);
                return Err(anyhow::anyhow!("Invalid block proposal format"));
            }
        };

        // Validate proposal data
        if proposal.block_hash.is_empty() {
            return Err(anyhow::anyhow!("Empty block hash in proposal"));
        }

        if proposal.height == 0 {
            return Err(anyhow::anyhow!("Invalid block height"));
        }

        // PeerId is always valid, no need to check for empty

        // Check if we're part of the consensus group
        let group_id = proposal.group_id.clone();
        let group_info = match self.group_manager.get_group_info(&group_id).await {
            Some(info) => info,
            None => {
                warn!("Not a member of group {}: group not found", group_id);
                return Err(anyhow::anyhow!("Not a member of consensus group"));
            }
        };

        // Validate we're in the right state to vote
        if group_info.status != GroupStatus::Active {
            return Err(anyhow::anyhow!("Group is not active"));
        }

        // Validate the proposer is a valid group member
        if !group_info.members.contains(&proposal.proposer) {
            warn!(
                "Proposer {} is not a member of group {}",
                proposal.proposer, group_id
            );
            return Err(anyhow::anyhow!("Invalid proposer"));
        }

        // Send acknowledgment if required
        if message.requires_ack {
            self.send_acknowledgment(&message).await?;
        }

        {
            let vote_key = (proposal.height, proposal.block_hash.clone());
            let emitted = self.emitted_votes.read().await;
            if emitted.contains(&vote_key) {
                debug!(
                    height = proposal.height,
                    block_hash = %hex::encode(&proposal.block_hash[..std::cmp::min(8, proposal.block_hash.len())]),
                    "Already voted for this proposal, skipping duplicate vote"
                );
                return Ok(());
            }
        }

        // Validate the block proposal
        let validation_result = self.validate_block_proposal(&proposal).await?;

        if validation_result.is_valid {
            // Vote for the block proposal with real signature
            let vote = BlockVote {
                block_hash: proposal.block_hash.clone(),
                height: proposal.height,
                voter: self.local_peer_id,
                vote_type: VoteType::Approve,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                signature: self.sign_vote_message(&proposal, VoteType::Approve)?,
                reason: None,
            };

            // Send our vote to the group
            self.send_vote_to_group(&group_id, vote).await?;

            // Registra il voto emesso per deduplicazione
            {
                let mut emitted = self.emitted_votes.write().await;
                emitted.insert((proposal.height, proposal.block_hash.clone()));
            }

            info!(
                "Voted to approve block {} at height {}",
                hex::encode(&proposal.block_hash[..8]),
                proposal.height
            );
        } else {
            // Vote against the block proposal with real signature
            let vote = BlockVote {
                block_hash: proposal.block_hash.clone(),
                height: proposal.height,
                voter: self.local_peer_id,
                vote_type: VoteType::Reject,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                signature: self.sign_vote_message(&proposal, VoteType::Reject)?,
                reason: Some(validation_result.reason.clone()),
            };

            // Send our vote to the group
            self.send_vote_to_group(&group_id, vote).await?;

            // Registra il voto emesso per deduplicazione
            {
                let mut emitted = self.emitted_votes.write().await;
                emitted.insert((proposal.height, proposal.block_hash.clone()));
            }

            info!(
                "Voted to reject block {} at height {}: {}",
                hex::encode(&proposal.block_hash[..8]),
                proposal.height,
                validation_result.reason
            );
        }

        Ok(())
    }

    /// Handle heartbeat message
    async fn handle_heartbeat(&self, message: GroupConsensusMessage) -> Result<()> {
        debug!(
            sender = %message.sender,
            "Received heartbeat"
        );

        // Update group activity
        self.group_manager
            .update_group_health(&message.group_id, 1.0)
            .await?;
        Ok(())
    }

    /// Process message queues
    async fn process_message_queues(&self) -> Result<()> {
        let priorities = [
            MessagePriority::Critical,
            MessagePriority::High,
            MessagePriority::Normal,
            MessagePriority::Low,
        ];

        for priority in priorities {
            let mut messages_to_send = Vec::new();

            // Collect messages to send
            {
                let mut queues = self.message_queues.write().await;
                if let Some(queue) = queues.get_mut(&priority) {
                    let batch_size: usize = if self.config.enable_batch_sending {
                        std::cmp::min(queue.len(), self.config.batch_size)
                    } else {
                        std::cmp::min(queue.len(), 1)
                    };

                    for _ in 0..batch_size {
                        if let Some(message) = queue.pop_front() {
                            messages_to_send.push(message);
                        }
                    }
                }
            }

            // Send messages outside of lock
            for message in messages_to_send {
                if let Err(e) = self.send_message_immediate(message).await {
                    error!("Failed to send queued message: {}", e);

                    let mut stats = self.stats.write().await;
                    stats.failed_messages += 1;
                }
            }
        }

        Ok(())
    }

    /// Retry failed messages
    async fn retry_failed_messages(&self) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let mut pending = self.pending_messages.write().await;
        let mut messages_to_retry = Vec::new();

        pending.retain(|_message_id, message| {
            let should_retry = message.retry_count < self.config.max_retry_attempts
                && (current_time - message.timestamp) > self.config.message_timeout_secs;

            if should_retry {
                messages_to_retry.push(message.clone());
            }

            !should_retry // Remove from pending if not retrying
        });

        for mut message in messages_to_retry {
            // Skip retry per voti gia' emessi con successo (riduce flood di duplicati gossipsub)
            if message.message_type == ConsensusMessageType::BlockVote {
                if let Ok(vote) = serde_json::from_slice::<BlockVote>(&message.payload) {
                    let vote_key = (vote.height, vote.block_hash.clone());
                    let emitted = self.emitted_votes.read().await;
                    if emitted.contains(&vote_key) {
                        debug!(
                            message_id = %message.message_id,
                            height = vote.height,
                            "Skipping retry for already-emitted vote (would cause Duplicate)"
                        );
                        continue;
                    }
                }
            }

            message.retry_count += 1;
            message.timestamp = current_time;

            if let Err(e) = self.send_message_immediate(message.clone()).await {
                error!(
                    message_id = %message.message_id,
                    retry_count = message.retry_count,
                    error = %e,
                    "Failed to retry message"
                );

                let mut stats = self.stats.write().await;
                stats.failed_messages += 1;
            } else {
                let mut stats = self.stats.write().await;
                stats.retried_messages += 1;
            }
        }

        Ok(())
    }

    /// Get routing statistics
    pub async fn get_stats(&self) -> RoutingStats {
        let stats = self.stats.read().await;
        let queues = self.message_queues.read().await;

        let mut result = stats.clone();
        result.queue_sizes = queues
            .iter()
            .map(|(priority, queue)| (*priority, queue.len()))
            .collect();

        result
    }

    /// Start message processing tasks
    pub async fn start(&self) -> Result<()> {
        info!("Starting consensus message router");

        // Start message queue processor
        let router = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(100));

            loop {
                interval.tick().await;
                if let Err(e) = router.process_message_queues().await {
                    error!("Failed to process message queues: {}", e);
                }
            }
        });

        // Start retry task
        let router = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(5));

            loop {
                interval.tick().await;
                if let Err(e) = router.retry_failed_messages().await {
                    error!("Failed to retry messages: {}", e);
                }
            }
        });

        Ok(())
    }

    /// Helper method to determine if we should join a group
    async fn should_join_group(&self, proposal: &GroupProposal) -> Result<bool> {
        // Check if we have capacity for another group
        let current_groups: Vec<GroupInfo> = self.group_manager.get_active_groups().await;
        if current_groups.len() >= 10 {
            // Max 10 active groups
            return Ok(false);
        }

        // Check if the group size is reasonable
        if proposal.required_members > 50 {
            return Ok(false);
        }

        // Check if the group configuration is acceptable
        if proposal.group_config.max_size > 1000 {
            return Ok(false);
        }

        // Check if we know any of the proposer members
        let mut known_members = 0;
        for member in &proposal.proposer_members {
            if self
                .group_manager
                .is_known_peer(member)
                .await
                .unwrap_or(false)
            {
                known_members += 1;
            }
        }

        // Join if we know at least one member or the group is small
        Ok(known_members > 0 || proposal.required_members <= 5)
    }

    /// Send acknowledgment for a message
    async fn send_acknowledgment(&self, message: &GroupConsensusMessage) -> Result<()> {
        let ack = GroupConsensusMessage {
            message_id: format!("ack-{}", message.message_id),
            group_id: message.group_id.clone(),
            sender: self.local_peer_id,
            message_type: ConsensusMessageType::SyncResponse,
            payload: serde_json::to_vec(&serde_json::json!({
                "original_message_id": message.message_id,
                "acknowledgment": true,
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            }))?,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            priority: MessagePriority::Normal,
            requires_ack: false,
            retry_count: 0,
        };

        self.send_message_to_group(&message.group_id, ack).await
    }

    /// Broadcast group acceptance to other members
    async fn broadcast_group_acceptance(&self, proposal: &GroupProposal) -> Result<()> {
        let acceptance = GroupConsensusMessage {
            message_id: format!("accept-{}", proposal.proposed_group_id),
            group_id: proposal.proposed_group_id.clone(),
            sender: self.local_peer_id,
            message_type: ConsensusMessageType::GroupVote,
            payload: serde_json::to_vec(&serde_json::json!({
                "vote": "accept",
                "group_id": proposal.proposed_group_id,
                "voter": self.local_peer_id.to_string(),
                "timestamp": std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            }))?,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            priority: MessagePriority::High,
            requires_ack: true,
            retry_count: 0,
        };

        self.send_message_to_group(&proposal.proposed_group_id, acceptance)
            .await
    }

    /// Send a vote to the group
    async fn send_vote_to_group(&self, group_id: &str, vote: BlockVote) -> Result<()> {
        // message_id unico: include height + block_hash + voter per evitare duplicati gossipsub
        let block_hash_prefix = if vote.block_hash.len() >= 8 {
            hex::encode(&vote.block_hash[..8])
        } else {
            hex::encode(&vote.block_hash)
        };
        let vote_message = GroupConsensusMessage {
            message_id: format!("vote-{}-{}-{}", vote.height, block_hash_prefix, vote.voter),
            group_id: group_id.to_string(),
            sender: vote.voter,
            message_type: ConsensusMessageType::BlockVote,
            payload: serde_json::to_vec(&vote)?,
            timestamp: vote.timestamp,
            priority: MessagePriority::High,
            requires_ack: true,
            retry_count: 0,
        };

        self.send_message_to_group(group_id, vote_message).await
    }

    /// Send a message to a specific group
    async fn send_message_to_group(
        &self,
        group_id: &str,
        message: GroupConsensusMessage,
    ) -> Result<()> {
        // Get group members
        let group_info = self
            .group_manager
            .get_group_info(group_id)
            .await
            .ok_or_else(|| anyhow::anyhow!("Group {} not found", group_id))?;

        // Send to all group members except ourselves
        for member in &group_info.members {
            if *member != self.local_peer_id {
                // In a real implementation, this would send via P2P network
                debug!("Sending message to group member {}", member);

                // Add to message queue for the member
                let mut queues = self.message_queues.write().await;
                let queue = queues.entry(message.priority).or_insert_with(VecDeque::new);
                queue.push_back(message.clone());
            }
        }

        Ok(())
    }

    /// Validate a block proposal
    async fn validate_block_proposal(
        &self,
        proposal: &BlockProposal,
    ) -> Result<BlockValidationResult> {
        // Check block hash format
        if proposal.block_hash.len() != 32 && proposal.block_hash.len() != 64 {
            return Ok(BlockValidationResult::invalid("Invalid block hash length"));
        }

        // Check height is reasonable
        if proposal.height > 1000000 {
            return Ok(BlockValidationResult::invalid("Block height too high"));
        }

        // Check gas limit is reasonable
        if proposal.gas_limit == 0 {
            return Ok(BlockValidationResult::invalid("Gas limit cannot be zero"));
        }

        if proposal.gas_limit > 10000000 {
            return Ok(BlockValidationResult::invalid("Gas limit too high"));
        }

        // Check timestamp is recent
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if proposal.timestamp > now + 300 {
            // 5 minutes in future
            return Ok(BlockValidationResult::invalid(
                "Proposal timestamp is too far in future",
            ));
        }

        if proposal.timestamp < now.saturating_sub(3600) {
            // 1 hour ago
            return Ok(BlockValidationResult::invalid(
                "Proposal timestamp is too old",
            ));
        }

        // Check block data size is reasonable
        if proposal.block_data.len() > 1000000 {
            // 1MB limit
            return Ok(BlockValidationResult::invalid("Block data too large"));
        }

        Ok(BlockValidationResult::valid())
    }

    /// Stop the message router
    pub async fn stop(&self) -> Result<()> {
        info!("Stopping consensus message router");
        Ok(())
    }
}

impl Clone for ConsensusMessageRouter {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            local_peer_id: self.local_peer_id,
            group_manager: self.group_manager.clone(),
            message_queues: self.message_queues.clone(),
            pending_messages: self.pending_messages.clone(),
            stats: self.stats.clone(),
            message_tx: self.message_tx.clone(),
            message_rx: self.message_rx.clone(),
            signing_key: self.signing_key.clone(),
            emitted_votes: self.emitted_votes.clone(),
        }
    }
}

impl ConsensusMessageRouter {
    /// Sign a vote message with real Ed25519 signature
    fn sign_vote_message(&self, proposal: &BlockProposal, vote_type: VoteType) -> Result<Vec<u8>> {
        use sha2::{Digest, Sha256};

        // Create message to sign: block_hash || height || voter || vote_type || timestamp
        let mut message = Vec::new();
        message.extend_from_slice(&proposal.block_hash);
        message.extend_from_slice(&proposal.height.to_le_bytes());
        message.extend_from_slice(&self.local_peer_id.to_bytes());
        message.extend_from_slice(match vote_type {
            VoteType::Approve => b"APPROVE",
            VoteType::Reject => b"REJECT",
        });
        message.extend_from_slice(
            &std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .to_le_bytes(),
        );

        // Hash the message for signing
        let mut hasher = Sha256::new();
        hasher.update(&message);
        let message_hash = hasher.finalize();

        // Sign the message using Ed25519
        let signature = self.signing_key.sign(&message_hash);

        info!(
            block_hash = %hex::encode(&proposal.block_hash[..8]),
            height = proposal.height,
            vote_type = ?vote_type,
            "✅ Signed vote message with Ed25519 signature"
        );

        Ok(signature.to_bytes().to_vec())
    }
}
