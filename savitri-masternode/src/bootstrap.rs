//! Bootstrap Protocol Handler for Masternode
//!
//! This module handles bootstrap requests from light nodes and provides chain data

use anyhow::Result;
use libp2p::{
    gossipsub::{Behaviour as Gossipsub, IdentTopic},
    PeerId,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

// Import group formation types
use super::group_formation::{GroupFormationManager, LightNodeInfo, NodeAssignmentStatus};

// Message wrapper types - MUST match lightnode exactly
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestMessage {
    Bootstrap(BootstrapRequest),
    Block(Vec<u8>),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResponseMessage {
    Bootstrap(BootstrapReply),
    Block(Vec<u8>),
    MonolithReply(MonolithReply),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonolithReply {
    pub req_id: u64,
    pub header: Option<MonolithHeader>,
    pub header_leaf_hashes: Vec<Vec<u8>>,
    pub missing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonolithHeader {
    pub monolith_id: Vec<u8>,
}

// Bootstrap message types (matching lightnode)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapRequest {
    pub version: u32,
    pub end_height: u64,
    pub max_blocks: u32,
}

impl BootstrapRequest {
    pub fn validate(&self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapReply {
    pub peers: Vec<BootstrapPeerInfo>,
    pub accounts: Vec<BootstrapAccountInfo>,
    pub blocks: Vec<BootstrapBlockInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapPeerInfo {
    pub peer_id: String,
    pub addresses: Vec<String>,
    pub is_light_node: bool,
    pub is_masternode: bool,
    pub is_validator: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapBlockInfo {
    pub height: u64,
    pub hash: Vec<u8>,
    pub timestamp: u64,
    pub tx_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapAccountInfo {
    pub address: Vec<u8>,
    pub balance: u64,
    pub nonce: u64,
    pub last_seen: u64,
    pub is_validator: bool,
    pub is_masternode: bool,
}

/// Internal peer info for tracking known peers
#[derive(Debug, Clone)]
pub struct PeerInfo {
    pub peer_id: PeerId,
    pub addresses: Vec<String>,
    pub is_light_node: bool,
}

/// Bootstrap handler for masternode
pub struct BootstrapHandler {
    current_height: u64,
    /// Known peers
    known_peers: HashMap<PeerId, PeerInfo>,
    /// Bootstrap request topic
    bootstrap_req_topic: IdentTopic,
    /// Bootstrap response topic  
    bootstrap_resp_topic: IdentTopic,
    /// Legacy bootstrap request topic kept for backwards compatibility
    legacy_bootstrap_req_topic: IdentTopic,
    /// Group formation manager for automatic lightnode registration
    group_manager: Option<Arc<tokio::sync::RwLock<GroupFormationManager>>>,
}

impl BootstrapHandler {
    /// Creates il handler con altezza iniziale 0 (catena reale: solo genesis fino a blocchi prodotti).
    pub fn new() -> Self {
        Self::with_initial_height(0)
    }

    /// Creates il handler con altezza iniziale specificata (0 = reale; >0 solo per dev/test locale).
    pub fn with_initial_height(initial_height: u64) -> Self {
        Self {
            current_height: initial_height,
            known_peers: HashMap::new(),
            bootstrap_req_topic: IdentTopic::new("/savitri/bootstrap/req/1"),
            bootstrap_resp_topic: IdentTopic::new("/savitri/bootstrap/resp/1"),
            legacy_bootstrap_req_topic: IdentTopic::new("bootstrap/request"),
            group_manager: None,
        }
    }

    pub fn with_group_manager(
        group_manager: Arc<tokio::sync::RwLock<GroupFormationManager>>,
        initial_height: u64,
    ) -> Self {
        Self {
            current_height: initial_height,
            known_peers: HashMap::new(),
            bootstrap_req_topic: IdentTopic::new("/savitri/bootstrap/req/1"),
            bootstrap_resp_topic: IdentTopic::new("/savitri/bootstrap/resp/1"),
            legacy_bootstrap_req_topic: IdentTopic::new("bootstrap/request"),
            group_manager: Some(group_manager),
        }
    }

    /// Set group formation manager
    pub fn set_group_manager(
        &mut self,
        group_manager: Arc<tokio::sync::RwLock<GroupFormationManager>>,
    ) {
        self.group_manager = Some(group_manager);
    }

    /// Add a peer to known peers (called when connecting to bootstrap peers)
    pub fn add_known_peer(&mut self, peer_id: PeerId, addresses: Vec<String>, is_light_node: bool) {
        let peer_info = PeerInfo {
            peer_id: peer_id.clone(),
            addresses,
            is_light_node,
        };
        self.known_peers.insert(peer_id, peer_info);
        debug!(
            "Added peer to known_peers: {} (is_light_node: {})",
            peer_id, is_light_node
        );
    }

    /// Get all known peers
    pub fn get_known_peers(&self) -> Vec<(PeerId, PeerInfo)> {
        self.known_peers
            .iter()
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }

    /// Get the bootstrap request topic
    pub fn request_topic(&self) -> &IdentTopic {
        &self.bootstrap_req_topic
    }

    /// Get the bootstrap response topic
    pub fn response_topic(&self) -> &IdentTopic {
        &self.bootstrap_resp_topic
    }

    /// Update current block height
    pub fn update_height(&mut self, height: u64) {
        self.current_height = height;
        debug!("Updated bootstrap handler height to {}", height);
    }

    /// Add known peer
    pub fn add_peer(&mut self, peer_id: PeerId, addresses: Vec<String>, is_light_node: bool) {
        let peer_info = PeerInfo {
            peer_id,
            addresses,
            is_light_node,
        };
        self.known_peers.insert(peer_id, peer_info);
    }

    /// Handle incoming bootstrap request and register lightnode for group formation
    ///
    /// `_peer_multiaddr`: IGNORED. Previously used the connection address from shared_peers,
    /// but that contains OS-assigned ephemeral ports from endpoint.get_remote_address().
    /// Using it caused other lightnodes to dial ephemeral ports ("connection refused").
    /// The lightnode's correct listen address arrives via gossipsub registration later.
    pub async fn handle_bootstrap_request(
        &mut self,
        request: BootstrapRequest,
        peer_id: Option<PeerId>,
        _peer_multiaddr: Option<String>,
    ) -> Result<BootstrapReply> {
        info!(
            version = request.version,
            end_height = request.end_height,
            max_blocks = request.max_blocks,
            peer = ?peer_id,
            "Received bootstrap request from light node"
        );

        // Validate request
        if request.validate().is_err() {
            warn!("Invalid bootstrap request version: {}", request.version);
            return Ok(BootstrapReply {
                peers: vec![],
                accounts: vec![],
                blocks: vec![],
            });
        }

        // Do NOT register the lightnode for group formation at bootstrap time.
        // The peer_multiaddr from ConnectionEstablished contains an OS-assigned
        // ephemeral source port — it's not the lightnode's listen address.
        // Registering with an empty multiaddr causes group_formation to reject
        // the node ("private/unreachable IP"), blocking it from joining any group.
        //
        // The lightnode's correct listen address arrives via gossipsub registration
        // (handle_lightnode_registration in libp2p_network.rs), which carries the
        // self-reported listen port. That is the only registration path.
        if let Some(peer_id) = &peer_id {
            debug!(
                peer_id = %peer_id,
                "Bootstrap: skipping group formation registration (will register via gossipsub with correct multiaddr)"
            );
        }

        // Build bootstrap reply with current chain state
        let blocks = self.build_block_info(request.end_height, request.max_blocks)?;
        let accounts = self.build_account_info()?;
        let peers = self.build_peer_info()?;

        Ok(BootstrapReply {
            blocks,
            accounts,
            peers,
        })
    }

    /// Publish bootstrap reply to network
    pub fn publish_bootstrap_reply(
        &self,
        gossipsub: &mut Gossipsub,
        reply: BootstrapReply,
    ) -> Result<()> {
        // Wrap in ResponseMessage to match lightnode format
        let response = ResponseMessage::Bootstrap(reply);
        let payload = serde_json::to_vec(&response)?;
        gossipsub.publish(self.bootstrap_resp_topic.clone(), payload)?;
        info!("Published bootstrap reply (wrapped in ResponseMessage)");
        Ok(())
    }

    /// Build block information for bootstrap reply
    fn build_block_info(
        &self,
        end_height: u64,
        max_blocks: u32,
    ) -> Result<Vec<BootstrapBlockInfo>> {
        let mut blocks = Vec::new();

        // Fetch blocks from storage
        if self.current_height > 0 {
            let start_height = if end_height == u64::MAX {
                self.current_height.saturating_sub(max_blocks as u64)
            } else {
                std::cmp::max(0, end_height.saturating_sub(max_blocks as u64))
            };

            for height in start_height..=std::cmp::min(end_height, self.current_height) {
                // In production, fetch actual block data from storage
                // For now, generate realistic block info
                let hash = self.generate_block_hash(height);
                let timestamp = self.calculate_block_timestamp(height);
                let tx_count = self.estimate_tx_count(height);

                blocks.push(BootstrapBlockInfo {
                    height,
                    hash,
                    timestamp,
                    tx_count,
                });
            }
        } else {
            // Option B: if storage is empty, simulate a bootstrap chain
            let simulated_end = if end_height == u64::MAX {
                max_blocks.saturating_sub(1) as u64
            } else {
                std::cmp::min(end_height, max_blocks.saturating_sub(1) as u64)
            };
            for height in 0..=simulated_end {
                let hash = self.generate_block_hash(height);
                let timestamp = self.calculate_block_timestamp(height);
                let tx_count = self.estimate_tx_count(height);

                blocks.push(BootstrapBlockInfo {
                    height,
                    hash,
                    timestamp,
                    tx_count,
                });
            }
        }

        debug!("Built {} blocks for bootstrap reply", blocks.len());
        Ok(blocks)
    }

    /// Generate realistic block hash for height
    fn generate_block_hash(&self, height: u64) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(height.to_le_bytes());
        hasher.update(b"savitri-block");
        hasher.update(self.current_height.to_le_bytes());
        hasher.finalize().to_vec()
    }

    /// Calculate block timestamp based on height
    fn calculate_block_timestamp(&self, height: u64) -> u64 {
        let block_time = 5; // 5 seconds per block
        let genesis_time = 1700000000; // Approximate genesis timestamp
        genesis_time + (height * block_time)
    }

    /// Estimate transaction count for block
    fn estimate_tx_count(&self, height: u64) -> u32 {
        // Simulate varying transaction counts
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        let mut hasher = DefaultHasher::new();
        hasher.write_u64(height);
        let hash = hasher.finish();

        // Range from 0 to 100 transactions
        (hash % 101) as u32
    }

    /// Build account information for bootstrap reply
    fn build_account_info(&self) -> Result<Vec<BootstrapAccountInfo>> {
        let mut accounts = Vec::new();

        // Generate sample accounts for bootstrap
        // In production, fetch actual accounts from storage
        let sample_accounts = vec![
            ("validator1", 1_000_000u64),
            ("validator2", 1_000_000u64),
            ("validator3", 1_000_000u64),
            ("masternode1", 5_000_000u64),
            ("masternode2", 5_000_000u64),
        ];

        for (name, balance) in sample_accounts {
            let address = self.generate_account_address(name);
            let account = BootstrapAccountInfo {
                address,
                balance,
                nonce: 0,
                last_seen: chrono::Utc::now().timestamp() as u64,
                is_validator: name.starts_with("validator"),
                is_masternode: name.starts_with("masternode"),
            };
            accounts.push(account);
        }

        debug!("Built {} accounts for bootstrap reply", accounts.len());
        Ok(accounts)
    }

    /// Generate account address from name
    fn generate_account_address(&self, name: &str) -> Vec<u8> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(name.as_bytes());
        hasher.update(b"savitri-account");
        let hash = hasher.finalize();

        // Take first 32 bytes as address
        hash[..32].to_vec()
    }

    /// Build peer information for bootstrap reply
    fn build_peer_info(&self) -> Result<Vec<BootstrapPeerInfo>> {
        let peers: Vec<BootstrapPeerInfo> = self
            .known_peers
            .values()
            .map(|p| BootstrapPeerInfo {
                peer_id: p.peer_id.to_string(),
                addresses: p.addresses.clone(),
                is_light_node: p.is_light_node,
                is_masternode: false, // Default for bootstrap peers
                is_validator: false,  // Default for bootstrap peers
            })
            .collect();
        debug!("Built {} peers for bootstrap reply", peers.len());
        Ok(peers)
    }

    /// Process incoming message and handle bootstrap requests
    ///
    /// `peer_multiaddr`: optional address for the peer (e.g. from shared_peers lookup).
    /// NOTE: This parameter is no longer used for group formation registration because
    /// shared_peers stores ephemeral connection ports, not listen ports. It is kept in
    /// the signature for API compatibility but ignored by handle_bootstrap_request.
    pub async fn process_message(
        &mut self,
        gossipsub: &mut Gossipsub,
        topic_hash: &libp2p::gossipsub::TopicHash,
        message_data: &[u8],
        peer_id: Option<PeerId>,
        peer_multiaddr: Option<String>,
    ) -> Result<()> {
        // SECURITY: Reject oversized messages before deserialization
        const MAX_MESSAGE_SIZE: usize = 1_048_576; // 1 MB
        if message_data.len() > MAX_MESSAGE_SIZE {
            error!(
                "Rejecting oversized bootstrap message: {} bytes (max {})",
                message_data.len(),
                MAX_MESSAGE_SIZE
            );
            return Ok(());
        }

        // Check if this is a bootstrap request
        if *topic_hash == self.bootstrap_req_topic.hash()
            || *topic_hash == self.legacy_bootstrap_req_topic.hash()
        {
            // Decode the RequestMessage wrapper first
            match serde_json::from_slice::<RequestMessage>(message_data) {
                Ok(RequestMessage::Bootstrap(request)) => {
                    info!("Processing bootstrap request from lightnode");
                    let reply = self
                        .handle_bootstrap_request(request, peer_id, peer_multiaddr)
                        .await?;
                    self.publish_bootstrap_reply(gossipsub, reply)?;
                }
                Ok(_) => {
                    debug!("Received non-bootstrap request on bootstrap topic");
                }
                Err(e) => {
                    error!("Failed to decode bootstrap request: {}", e);
                }
            }
        }

        Ok(())
    }
}

impl Default for BootstrapHandler {
    fn default() -> Self {
        Self::new()
    }
}
