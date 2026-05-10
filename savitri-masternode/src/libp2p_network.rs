//! Real libp2p Network Implementation for Masternode
//!
//! This module provides true libp2p networking compatible with lightnodes.
//! Uses TCP + DNS + Noise + Yamux + Gossipsub - EXACTLY like the lightnode.

use anyhow::{anyhow, Context, Result};
use hex;
use libp2p::futures::StreamExt;
use libp2p::{
    core::{muxing::StreamMuxerBox, transport::Boxed, upgrade},
    gossipsub::{
        Behaviour as Gossipsub, ConfigBuilder as GossipsubConfigBuilder, Event as GossipsubEvent,
        IdentTopic, MessageAuthenticity, MessageId, PublishError, SubscriptionError,
        ValidationMode,
    },
    identity::Keypair,
    kad::{
        store::MemoryStore, Behaviour as Kademlia, Event as KademliaEvent, GetRecordOk, Mode,
        QueryResult, Quorum, Record, RecordKey,
    },
    multiaddr::Protocol,
    swarm::{NetworkBehaviour, Swarm, SwarmEvent},
    Multiaddr, PeerId, Transport,
};
use metrics::{counter, gauge};
use savitri_storage::storage::{CF_BLOCKS, CF_METADATA};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_big_array::BigArray;
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, error, info, warn};

fn is_routable_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => {
            !v4.is_unspecified()
                && !v4.is_loopback()
                && !v4.is_private()
                && !v4.is_link_local()
                && !v4.is_broadcast()
                && !v4.is_documentation()
        }
        IpAddr::V6(v6) => {
            let segments = v6.segments();
            let is_unique_local = (segments[0] & 0xfe00) == 0xfc00;
            let is_unicast_link_local = (segments[0] & 0xffc0) == 0xfe80;
            !v6.is_unspecified()
                && !v6.is_loopback()
                && !v6.is_multicast()
                && !is_unique_local
                && !is_unicast_link_local
        }
    }
}

fn is_routable_multiaddr(addr: &Multiaddr) -> bool {
    addr.iter().any(|proto| match proto {
        Protocol::Ip4(ip) => is_routable_ip(&IpAddr::V4(ip)),
        Protocol::Ip6(ip) => is_routable_ip(&IpAddr::V6(ip)),
        _ => false,
    })
}

fn build_advertised_multiaddr(listener: &Multiaddr, ip: IpAddr) -> Option<Multiaddr> {
    let mut tcp_port = None;
    let mut udp_port = None;
    let mut has_quic_v1 = false;

    for proto in listener.iter() {
        match proto {
            Protocol::Tcp(port) => tcp_port = Some(port),
            Protocol::Udp(port) => udp_port = Some(port),
            Protocol::QuicV1 => has_quic_v1 = true,
            _ => {}
        }
    }

    let ip_part = match ip {
        IpAddr::V4(v4) => format!("/ip4/{}", v4),
        IpAddr::V6(v6) => format!("/ip6/{}", v6),
    };

    if let Some(port) = tcp_port {
        return format!("{}/tcp/{}", ip_part, port).parse().ok();
    }
    if has_quic_v1 {
        if let Some(port) = udp_port {
            return format!("{}/udp/{}/quic-v1", ip_part, port).parse().ok();
        }
    }
    None
}

/// Extracts the first IPv4 address from a multiaddr (e.g. connection address).
/// Used to replace 127.0.0.1 in lightnode registration when nodes are on different VMs/hosts.
fn try_extract_ipv4_from_multiaddr(addr: &Multiaddr) -> Option<std::net::Ipv4Addr> {
    for proto in addr.iter() {
        if let Protocol::Ip4(ip) = proto {
            return Some(ip);
        }
    }
    None
}

#[derive(NetworkBehaviour)]
#[behaviour(
    to_swarm = "MyBehaviourEvent",
    prelude = "libp2p::swarm::derive_prelude"
)]
struct MyBehaviour {
    pub gossipsub: Gossipsub,
    pub kademlia: Kademlia<MemoryStore>,
    pub consensus: libp2p::request_response::Behaviour<super::consensus_protocol::ConsensusCodec>,
    pub relay: libp2p::relay::Behaviour,
    pub identify: libp2p::identify::Behaviour,
    pub autonat: libp2p::autonat::Behaviour,
    pub dcutr: libp2p::dcutr::Behaviour,
    pub upnp: libp2p::upnp::tokio::Behaviour,
}

#[derive(Debug)]
enum MyBehaviourEvent {
    Gossipsub(GossipsubEvent),
    Kademlia(KademliaEvent),
    Consensus(
        libp2p::request_response::Event<
            super::consensus_protocol::ConsensusMessage,
            super::consensus_protocol::ConsensusAck,
        >,
    ),
    Relay(libp2p::relay::Event),
    Identify(libp2p::identify::Event),
    Autonat(libp2p::autonat::Event),
    Dcutr(libp2p::dcutr::Event),
    Upnp(libp2p::upnp::Event),
}

impl From<GossipsubEvent> for MyBehaviourEvent {
    fn from(event: GossipsubEvent) -> Self {
        MyBehaviourEvent::Gossipsub(event)
    }
}

impl From<KademliaEvent> for MyBehaviourEvent {
    fn from(event: KademliaEvent) -> Self {
        MyBehaviourEvent::Kademlia(event)
    }
}

impl
    From<
        libp2p::request_response::Event<
            super::consensus_protocol::ConsensusMessage,
            super::consensus_protocol::ConsensusAck,
        >,
    > for MyBehaviourEvent
{
    fn from(
        event: libp2p::request_response::Event<
            super::consensus_protocol::ConsensusMessage,
            super::consensus_protocol::ConsensusAck,
        >,
    ) -> Self {
        MyBehaviourEvent::Consensus(event)
    }
}

impl From<libp2p::relay::Event> for MyBehaviourEvent {
    fn from(event: libp2p::relay::Event) -> Self {
        MyBehaviourEvent::Relay(event)
    }
}

impl From<libp2p::identify::Event> for MyBehaviourEvent {
    fn from(event: libp2p::identify::Event) -> Self {
        MyBehaviourEvent::Identify(event)
    }
}

impl From<libp2p::autonat::Event> for MyBehaviourEvent {
    fn from(event: libp2p::autonat::Event) -> Self {
        MyBehaviourEvent::Autonat(event)
    }
}

impl From<libp2p::dcutr::Event> for MyBehaviourEvent {
    fn from(event: libp2p::dcutr::Event) -> Self {
        MyBehaviourEvent::Dcutr(event)
    }
}

impl From<libp2p::upnp::Event> for MyBehaviourEvent {
    fn from(event: libp2p::upnp::Event) -> Self {
        MyBehaviourEvent::Upnp(event)
    }
}

impl MyBehaviour {
    fn publish(
        &mut self,
        topic: impl Into<libp2p::gossipsub::TopicHash>,
        data: impl Into<Vec<u8>>,
    ) -> Result<MessageId, PublishError> {
        self.gossipsub.publish(topic, data)
    }

    fn subscribe(&mut self, topic: &IdentTopic) -> Result<bool, SubscriptionError> {
        self.gossipsub.subscribe(topic)
    }
}

// Import bootstrap and group formation types
use super::adaptive_batch_collector::{AdaptiveBatchConfig, AdaptivePeerInfoBatchCollector};
use super::batch_collector::{BatchConfig, PeerInfoBatchCollector};
use super::bootstrap::{BootstrapHandler, BootstrapRequest as BootstrapHandlerRequest};
use super::group_formation::GroupFormationManager;

// NEW: Import P2P module as alternative to masternode_p2p
use savitri_p2p::{NetworkConfig, NetworkManager, NetworkStats, P2PConfig, P2PManager};

use crate::block_messages::Transaction;
use crate::masternode_p2p::MasternodeMessage;
use crate::transaction_validator::TransactionValidator as RealTransactionValidator;
use crate::transaction_validator::{
    CacheStats, ExecutionStatus, ValidatedTransaction, ValidationResult,
};
// Note: BlockProposal, BlockValidationResult
// are defined locally in this file to avoid type conflicts

// Cryptographic imports for signature verification
use ed25519_dalek::{Signature, Signer, Verifier, VerifyingKey};
use sha2::{Digest, Sha256};

// Custom serialization for Option<[u8; 64]>
fn serialize_big_array_option<S>(
    option: &Option<[u8; 64]>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match option {
        Some(arr) => {
            // Convert to hex string for serialization
            let hex_str = hex::encode(arr);
            serializer.serialize_some(&hex_str)
        }
        None => serializer.serialize_none(),
    }
}

fn deserialize_big_array_option<'de, D>(deserializer: D) -> Result<Option<[u8; 64]>, D::Error>
where
    D: Deserializer<'de>,
{
    // Deserialize as hex string then convert to array
    let hex_str: Option<String> = Option::deserialize(deserializer)?;
    match hex_str {
        Some(s) => {
            let bytes =
                hex::decode(&s).map_err(|_| serde::de::Error::custom("invalid hex string"))?;
            if bytes.len() != 64 {
                return Err(serde::de::Error::custom("invalid array length"));
            }
            let mut arr = [0u8; 64];
            arr.copy_from_slice(&bytes);
            Ok(Some(arr))
        }
        None => Ok(None),
    }
}

// Enhanced TransactionValidator with signature verification
#[derive(Debug, Clone)]
pub struct TransactionValidator {
    inner_validator: RealTransactionValidator,
}

impl TransactionValidator {
    pub fn new() -> Self {
        Self {
            inner_validator: RealTransactionValidator::new(),
        }
    }

    pub fn with_threshold(threshold: f64) -> Self {
        Self {
            inner_validator: RealTransactionValidator::with_threshold(threshold),
        }
    }

    /// Validate transactions with proper signature verification
    pub fn validate_block_transactions(
        &mut self,
        txs: Vec<ValidatedTransaction>,
        group_id: String,
    ) -> ValidationResult {
        let mut validated_transactions = Vec::new();
        let mut duplicate_hashes = Vec::new();
        let mut seen_hashes = std::collections::HashSet::new();

        for tx in txs {
            // Check for duplicate transactions
            if seen_hashes.contains(&tx.tx_hash) {
                duplicate_hashes.push(tx.tx_hash);
                continue;
            }
            seen_hashes.insert(tx.tx_hash);

            let is_valid = self.validate_transaction(&tx);

            if is_valid {
                validated_transactions.push(tx);
            }
        }

        let total_transactions = validated_transactions.len() + duplicate_hashes.len();
        let unique_transactions = validated_transactions.len();
        let uniqueness_ratio = if total_transactions > 0 {
            unique_transactions as f64 / total_transactions as f64
        } else {
            0.0
        };

        ValidationResult {
            validated_transactions,
            duplicate_hashes,
            total_transactions,
            unique_transactions,
            uniqueness_ratio,
            is_accepted: uniqueness_ratio >= 0.8, // 80% threshold
        }
    }

    /// Validate individual transaction with signature verification
    fn validate_transaction(&self, tx: &ValidatedTransaction) -> bool {
        if tx.amount == 0 || tx.sender == [0u8; 32] || tx.receiver == [0u8; 32] {
            return false;
        }

        // Signature verification
        if tx.signature != [0u8; 64] {
            if let Some(block_hash) = &tx.block_hash {
                // Verify signature against transaction hash and block hash
                self.verify_transaction_signature(tx, &tx.signature, block_hash)
            } else {
                // If no block hash, verify against transaction hash only
                self.verify_transaction_signature_simple(tx, &tx.signature)
            }
        } else {
            // No signature provided
            false
        }
    }

    /// Verify transaction signature with block hash context
    fn verify_transaction_signature(
        &self,
        tx: &ValidatedTransaction,
        signature: &[u8; 64],
        block_hash: &[u8; 64],
    ) -> bool {
        // Create message to verify: tx_hash || block_hash
        let mut message = Vec::new();
        message.extend_from_slice(&tx.tx_hash);
        message.extend_from_slice(block_hash);

        // Hash the message
        let message_hash = Sha256::digest(&message);

        // Parse signature
        let sig = Signature::from_bytes(signature);

        // Parse public key
        let public_key = match VerifyingKey::from_bytes(&tx.sender) {
            Ok(key) => key,
            Err(_) => return false,
        };

        // Verify signature
        public_key.verify(&message_hash, &sig).is_ok()
    }

    /// Verify transaction signature without block hash context
    fn verify_transaction_signature_simple(
        &self,
        tx: &ValidatedTransaction,
        signature: &[u8; 64],
    ) -> bool {
        // Create message to verify: tx_hash || amount || receiver || nonce
        let mut message = Vec::new();
        message.extend_from_slice(&tx.tx_hash);
        message.extend_from_slice(&tx.amount.to_le_bytes());
        message.extend_from_slice(&tx.receiver);
        message.extend_from_slice(&tx.nonce.to_le_bytes());

        // Hash the message
        let message_hash = Sha256::digest(&message);

        // Parse signature
        let sig = Signature::from_bytes(signature);

        // Parse public key
        let public_key = match VerifyingKey::from_bytes(&tx.sender) {
            Ok(key) => key,
            Err(_) => return false,
        };

        // Verify signature
        public_key.verify(&message_hash, &sig).is_ok()
    }

    pub fn get_cache_stats(&self) -> CacheStats {
        self.inner_validator.get_cache_stats()
    }

    /// Set current block height
    pub fn set_current_block_height(&mut self, height: u64) {
        self.inner_validator.set_current_block_height(height);
    }

    /// Get current block height
    pub fn get_current_block_height(&self) -> u64 {
        self.inner_validator.get_current_block_height()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockProposal {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub proposer_group_id: String,
    pub height: u64,
    pub transactions: Vec<LocalTransaction>,
    pub timestamp: u64,
    #[serde(with = "BigArray")]
    pub proposer_signature: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalTransaction {
    pub tx_hash: [u8; 32],
    pub sender: [u8; 32],
    pub receiver: [u8; 32],
    pub amount: u64,
    pub nonce: u64,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockValidationResult {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub proposer_group_id: String,
    pub validation_result: ValidationResult,
    pub is_accepted: bool,
    pub timestamp: u64,
}

impl BlockValidationResult {
    pub fn new(
        block_hash: [u8; 64],
        proposer_group_id: String,
        validation_result: ValidationResult,
        is_accepted: bool,
    ) -> Self {
        Self {
            block_hash,
            proposer_group_id,
            validation_result,
            is_accepted,
            timestamp: current_timestamp(),
        }
    }

    pub fn get_summary(&self) -> String {
        format!(
            "{}: accepted={}, unique_txs={}/{}",
            hex::encode(&self.block_hash[..8]),
            self.is_accepted,
            self.validation_result.unique_transactions,
            self.validation_result.total_transactions
        )
    }
}

impl From<LocalTransaction> for ValidatedTransaction {
    fn from(tx: LocalTransaction) -> Self {
        Self {
            tx_hash: tx.tx_hash,
            sender: tx.sender,
            receiver: tx.receiver,
            amount: tx.amount,
            nonce: tx.nonce,
            signature: tx.signature,
            processing_group_id: None,
            execution_status: ExecutionStatus::Pending,
            processed_at: None,
            block_hash: None,
            is_duplicate: false,
        }
    }
}

// NEW: Peer discovery messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDiscoveryRequest {
    pub requesting_peer: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerDiscoveryResponse {
    pub requesting_peer: String,
    pub masternode_peers: Vec<String>, // List of masternode multiaddrs (with /p2p/<peer_id>)
}

// NEW: Peer registry announcement (gossip + TTL)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerRegistryAnnounce {
    pub peer_id: String,
    pub multiaddr: String,
    pub role: String,
    pub timestamp: u64,
    pub ttl_secs: u64,
}

// NEW: Heartbeat message type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    pub timestamp: u64,
    pub nonce: u64,
    pub kind: HeartbeatKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeartbeatKind {
    Ping,
    Pong,
}

// NEW: PoU broadcast message type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouBroadcast {
    pub peer_id: String,
    pub epoch: u64,
    pub score: u16,
    pub index: u16,
    pub timestamp: u64,
}

// NEW: Peer info message type (raw format for direct use)
// CRITICAL: Must use BigArray to match lightnode's serialization format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfoMessage {
    #[serde(with = "BigArray")]
    pub account: [u8; 32],
}

// NEW: GossipMessage wrapper enum - matches lightnode's broadcast.rs format
// This is critical: lightnode sends {"PeerInfo":{"account":[...]}} not {"account":[...]}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipMessage {
    Tx(TxMessageGossip),
    HaveTx(HaveTxGossip),
    Heartbeat(HeartbeatMessage),
    HaveBlock(HaveBlockGossip),
    Block(BlockMessageGossip),
    PeerInfo(PeerInfoMessage),
    // NEW: Lightnode registration for group formation
    LightnodeRegistration(LightnodeRegistrationMessage),
}

// NEW: Lightnode registration message - contains full node info for group formation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightnodeRegistrationMessage {
    pub node_id: String,
    pub peer_id: String,
    pub multiaddr: String,
    pub geographic_region: String,
    pub pou_score: f64,
    pub capabilities: Vec<String>,
    pub uptime_percentage: f64,
    #[serde(with = "BigArray")]
    pub account: [u8; 32],
}

// Supporting structs for GossipMessage variants
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxMessageGossip {
    pub tx: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HaveTxGossip {
    // Use Vec<Vec<u8>> for compatibility - serde can't serialize Vec<[u8; 64]> by default
    pub tx_hashes: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HaveBlockGossip {
    pub exec_height: u64,
    pub tx_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMessageGossip {
    #[serde(with = "BigArray")]
    pub hash: [u8; 64],
}

// NEW: Consensus certificate type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsensusCertificate {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub height: u64,
    #[serde(default, alias = "group_id")]
    pub proposer_group_id: String,
    #[serde(default)]
    pub validation_timestamp: u64,
    pub voter_signatures: Vec<String>,
    #[serde(with = "BigArray")]
    pub aggregated_signature: [u8; 64],
}

impl ConsensusCertificate {
    /// Validate certificate with proper signature verification
    pub fn is_valid(&self) -> bool {
        if self.voter_signatures.is_empty() {
            return false;
        }

        if self.aggregated_signature == [0u8; 64] {
            return false;
        }

        // Validate timestamp (not too old or in future); skip if not set (0) for backward compat
        if self.validation_timestamp != 0 {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            if self.validation_timestamp > now + 300
                || self.validation_timestamp < now.saturating_sub(3600)
            {
                return false;
            }
        }

        // Validate proposer group ID
        if self.proposer_group_id.is_empty() {
            return false;
        }

        // Validate height is reasonable
        if self.height == 0 {
            return false; // Height 0 should not have a certificate
        }

        // Validate block hash is not all zeros
        if self.block_hash == [0u8; 64] {
            return false;
        }

        // Verify aggregated signature format
        let _sig = Signature::from_bytes(&self.aggregated_signature);

        // In a full implementation, we would:
        // 1. Parse voter signatures from hex strings
        // 2. Aggregate voter public keys
        // 3. Verify the aggregated signature against the aggregated public key

        true
    }

    pub fn verify_detailed(&self) -> Result<(), String> {
        // Check basic validity first
        if !self.is_valid() {
            return Err("Certificate failed basic validation".to_string());
        }

        if self.height > 1000000 {
            return Err("Height is unreasonably high".to_string());
        }

        if self.proposer_group_id.len() > 256 {
            return Err("Proposer group ID is too long".to_string());
        }

        // Validate voter signatures format (should be hex strings)
        for (i, sig_hex) in self.voter_signatures.iter().enumerate() {
            if sig_hex.len() != 128 {
                // 64 bytes * 2 hex chars
                return Err(format!(
                    "Invalid voter signature format at index {}: length {}",
                    i,
                    sig_hex.len()
                ));
            }

            // Try to decode as hex
            if hex::decode(sig_hex).is_err() {
                return Err(format!(
                    "Invalid hex format for voter signature at index {}",
                    i
                ));
            }
        }

        // Validate aggregated signature
        let _sig = Signature::from_bytes(&self.aggregated_signature);

        Ok(())
    }

    /// Get certificate summary information
    pub fn get_summary(&self) -> CertificateSummary {
        CertificateSummary {
            height: self.height,
            proposer_group_id: self.proposer_group_id.clone(),
            voter_count: self.voter_signatures.len(),
            validation_timestamp: self.validation_timestamp,
            block_hash: hex::encode(&self.block_hash),
            is_valid: self.is_valid(),
        }
    }

    /// Get certificate age in seconds
    pub fn age_seconds(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now.saturating_sub(self.validation_timestamp)
    }

    /// Check if certificate is recent (within last N seconds)
    pub fn is_recent(&self, within_seconds: u64) -> bool {
        self.age_seconds() <= within_seconds
    }

    /// Create mock certificate for testing
    pub fn create_mock(
        block_hash: [u8; 64],
        height: u64,
        proposer_group_id: String,
        voter_signatures: Vec<String>,
    ) -> Self {
        use ed25519_dalek::SigningKey;
        use rand::rngs::OsRng;

        let mut csprng = OsRng {};
        let signing_key = SigningKey::generate(&mut csprng);
        let signature = signing_key.sign(&block_hash);

        Self {
            block_hash,
            height,
            proposer_group_id,
            validation_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            voter_signatures,
            aggregated_signature: signature.to_bytes(),
        }
    }
}

/// Certificate summary for quick display
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateSummary {
    pub height: u64,
    pub proposer_group_id: String,
    pub voter_count: usize,
    pub validation_timestamp: u64,
    pub block_hash: String,
    pub is_valid: bool,
}

impl CertificateSummary {
    /// Get formatted timestamp
    pub fn formatted_timestamp(&self) -> String {
        let datetime = chrono::DateTime::from_timestamp(self.validation_timestamp as i64, 0)
            .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
        datetime.format("%Y-%m-%d %H:%M:%S UTC").to_string()
    }

    /// Get formatted age
    pub fn formatted_age(&self) -> String {
        let age = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            .saturating_sub(self.validation_timestamp);

        if age < 60 {
            format!("{}s", age)
        } else if age < 3600 {
            format!("{}m {}s", age / 60, age % 60)
        } else if age < 86400 {
            format!("{}h {}m", age / 3600, (age % 3600) / 60)
        } else {
            format!("{}d {}h", age / 86400, (age % 86400) / 3600)
        }
    }
}

// NEW: Mempool sync message type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MempoolSyncMessage {
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub confirmed_transactions: Vec<[u8; 32]>,
    pub rejected_transactions: Vec<[u8; 32]>,
    pub timestamp: u64,
}

impl MempoolSyncMessage {
    pub fn new(
        block_hash: [u8; 64],
        confirmed_transactions: Vec<[u8; 32]>,
        rejected_transactions: Vec<[u8; 32]>,
    ) -> Self {
        Self {
            block_hash,
            confirmed_transactions,
            rejected_transactions,
            timestamp: current_timestamp(),
        }
    }

    pub fn total_transactions(&self) -> usize {
        self.confirmed_transactions.len() + self.rejected_transactions.len()
    }
}

fn current_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Detect deprecated/non-certificate payloads that may still arrive on
/// /savitri/consensus/cert/1 from older nodes.
///
/// Masternodes must process block certificates only on this topic.
fn detect_non_certificate_payload_kind(data: &[u8]) -> Option<&'static str> {
    let value: serde_json::Value = serde_json::from_slice(data).ok()?;
    detect_non_certificate_payload_kind_value(&value)
}

fn detect_non_certificate_payload_kind_value(value: &serde_json::Value) -> Option<&'static str> {
    let obj = value.as_object()?;

    // Direct (legacy) BlockValidationResult-like payload
    if obj.contains_key("validation_result")
        && obj.contains_key("proposer_group_id")
        && !obj.contains_key("height")
    {
        return Some("BlockValidationResult");
    }

    // Direct (legacy) MempoolSyncMessage-like payload
    if obj.contains_key("block_hash")
        && (obj.contains_key("confirmed_transactions") || obj.contains_key("rejected_transactions"))
        && !obj.contains_key("height")
    {
        return Some("MempoolSyncMessage");
    }

    // Wrapper variants seen in mixed-version networks.
    for key in [
        "BlockValidationResult",
        "MempoolSyncMessage",
        "MempoolSync",
        "MempoolSyncUpdate",
    ] {
        if let Some(inner) = obj.get(key) {
            if let Some(kind) = detect_non_certificate_payload_kind_value(inner) {
                return Some(kind);
            }
            // If wrapper exists but nested shape is not obvious, still treat as non-certificate.
            return Some(if key.starts_with("BlockValidationResult") {
                "BlockValidationResult"
            } else {
                "MempoolSyncMessage"
            });
        }
    }

    // Common envelope keys used by older relays/wrappers.
    for key in ["payload", "data", "message", "msg"] {
        if let Some(inner) = obj.get(key) {
            if let Some(kind) = detect_non_certificate_payload_kind_value(inner) {
                return Some(kind);
            }
        }
    }

    // Generic single-key wrapper fallback.
    if obj.len() == 1 {
        if let Some((wrapper_key, inner)) = obj.iter().next() {
            if let Some(kind) = detect_non_certificate_payload_kind_value(inner) {
                return Some(kind);
            }
            let wrapper_key_lower = wrapper_key.to_ascii_lowercase();
            if wrapper_key_lower.contains("mempool") {
                return Some("MempoolSyncMessage");
            }
            if wrapper_key_lower.contains("validation") {
                return Some("BlockValidationResult");
            }
        }
    }

    // Tag-based envelopes, e.g. {"type":"MempoolSyncMessage","data":{...}}
    if let Some(tag) = obj.get("type").and_then(|v| v.as_str()) {
        let tag_lower = tag.to_ascii_lowercase();
        if tag_lower.contains("mempool") {
            return Some("MempoolSyncMessage");
        }
        if tag_lower.contains("validation") {
            return Some("BlockValidationResult");
        }
    }

    None
}

const DEFAULT_REGISTRY_TTL_SECS: u64 = 300;
const REGISTRY_ANNOUNCE_INTERVAL_SECS: u64 = 10;
const REGISTRY_PRUNE_INTERVAL_SECS: u64 = 15;
/// Pending block proposals (non-owner MN): reject and remove if no BlockAcceptanceCertificate within this time
const PENDING_BLOCK_TIMEOUT_SECS: u64 = 420; // 7 minutes
const PENDING_CLEANUP_INTERVAL_SECS: u64 = 60; // check every minute

// NEW: Peer activity tracking for PoU scoring
#[derive(Debug, Clone)]
pub struct PeerActivityTracker {
    pub last_heartbeat: u64,
    pub heartbeat_count: u64,
    pub missed_heartbeats: u64,
    pub uptime_percentage: f64,
}

impl Default for PeerActivityTracker {
    fn default() -> Self {
        Self {
            last_heartbeat: current_timestamp(),
            heartbeat_count: 0,
            missed_heartbeats: 0,
            uptime_percentage: 100.0,
        }
    }
}

// NEW: PoU score entry for scoring database
#[derive(Debug, Clone)]
pub struct PouScoreEntry {
    pub peer_id: String,
    pub epoch: u64,
    pub score: u16,
    pub index: u16,
    pub last_updated: u64,
}

// NEW: Peer registry entry for peer discovery
#[derive(Debug, Clone)]
pub struct PeerRegistryEntry {
    pub account: [u8; 32],
    pub first_seen: u64,
    pub last_seen: u64,
    pub is_active: bool,
    pub multiaddr: Option<String>,
}

#[derive(Debug, Clone)]
pub struct PeerRegistryRecord {
    pub peer_id: String,
    pub multiaddr: String,
    pub role: String,
    pub last_seen: u64,
    pub expires_at: u64,
}

// NEW: Block finality tracker
#[derive(Debug, Clone)]
pub struct BlockFinalityEntry {
    pub block_hash: [u8; 64],
    pub height: u64,
    pub proposer_group_id: String,
    pub confirmed_transactions: Vec<[u8; 32]>,
    pub rejected_transactions: Vec<[u8; 32]>,
    pub finalized_at: u64,
}

/// Certificate accepted but waiting for corresponding block payload in cache.
#[derive(Debug, Clone)]
pub struct PendingCertificateFinality {
    pub block_hash: [u8; 64],
    pub height: u64,
    pub proposer_group_id: String,
    pub received_at: u64,
}

// Pending proposal entry (non-owner MN waits for BlockAcceptanceCertificate)
#[derive(Debug, Clone)]
pub struct PendingProposalEntry {
    pub proposal: super::proposal_validator::LightnodeProposal,
    pub received_at: u64,
}

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

/// Bootstrap message types (matching lightnode exactly)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapRequest {
    pub version: u32,
    pub end_height: u64,
    pub max_blocks: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapReply {
    pub peers: Vec<BootstrapPeerInfo>,
    pub accounts: Vec<BootstrapAccountInfo>,
    pub blocks: Vec<BootstrapBlockInfo>,
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BootstrapPeerInfo {
    pub peer_id: String,
    pub addresses: Vec<String>,
    pub is_light_node: bool,
    pub is_masternode: bool,
    pub is_validator: bool,
}

// Compat: lightnode block proposal wire format (from savitri-lightnode::proposer::BlockProposal)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightnodeProposalTransactionCompat {
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightnodeBlockProposalCompat {
    pub round_id: u64,
    pub height: u64,
    pub timestamp: u64,
    #[serde(with = "BigArray")]
    pub proposer_pubkey: [u8; 32],
    pub proposer_pou_score: u32,
    #[serde(with = "BigArray")]
    pub parent_hash: [u8; 64],
    #[serde(with = "BigArray")]
    pub state_root: [u8; 64],
    #[serde(with = "BigArray")]
    pub tx_root: [u8; 64],
    pub transactions: Vec<LightnodeProposalTransactionCompat>,
    #[serde(default)]
    pub latency_proof: Option<serde_json::Value>,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

// ─── Wire types for Block+Certificate (MN→LN single message) ─────────────────
// Must match lightnode BlockMessage / GossipMessage::Block for cache decode.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockHeaderWire {
    pub exec_height: u64,
    #[serde(with = "BigArray")]
    pub proposer: [u8; 32],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockMessageWire {
    #[serde(with = "BigArray")]
    pub hash: [u8; 64],
    pub header: BlockHeaderWire,
    pub txs: Vec<Vec<u8>>,
}

/// Wrapper to decode only Block variant from lightnode gossip on block_topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum GossipBlockOnlyWire {
    Block(BlockMessageWire),
}

/// Single message: block + certificate (MN publishes to /savitri/block_final/1).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockWithCertificateWire {
    pub block: BlockMessageWire,
    pub certificate: crate::proposal_validator::BlockCertificate,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupFormedAck {
    pub group_id: String,
    pub epoch: u64,
    pub peer_id: String,
    pub timestamp: u64,
    pub connected_peers: usize,
    pub total_peers: usize,
}

// ═══════════════════════════════════════════════════════════════════
// Proposer Whitelist: handshake 3-fasi per block proposal
// (1) LN proposer invia certificato elezione firmato dal gruppo
// (2) MN check, whitelist + explicit_peer, invia ACK
// ═══════════════════════════════════════════════════════════════════

/// Entry in the whitelist temporanea dei proposer autorizzati
#[derive(Debug, Clone)]
pub struct ProposerWhitelistEntry {
    pub peer_id: PeerId,
    pub group_id: String,
    pub round: u64,
    pub proposer_pubkey: [u8; 32],
    pub added_at: std::time::Instant,
    pub timeout: Duration,
}

/// Messaggio di certificato di elezione inviato dal LN proposer al MN
/// Inviato su topic /savitri/masternode/election/cert/1
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposerElectionCertMessage {
    pub group_id: String,
    pub round_id: u64,
    pub proposer_peer_id: String,
    #[serde(with = "BigArray")]
    pub proposer_pubkey: [u8; 32],
    pub proposer_pou_score: u32,
    pub election_timestamp: u64,
    pub candidates: Vec<(String, u32, f64)>,
    pub attestations: Vec<super::proposal_validator::ElectionAttestation>,
}

/// ACK di whitelist inviato dal MN al LN proposer
/// Inviato su topic /savitri/masternode/election/ack/1
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposerWhitelistAck {
    /// PeerId of the proposer autorizzato (destinatario)
    pub target_proposer_peer_id: String,
    pub masternode_peer_id: String,
    /// Group ID di riferimento
    pub group_id: String,
    /// Round di riferimento
    pub round_id: u64,
    /// Timestamp dell'autorizzazione
    pub timestamp: u64,
    /// Durata validita' in secondi
    pub validity_secs: u64,
}

/// Real libp2p network manager for masternode - matches lightnode implementation exactly
pub struct Libp2pNetwork {
    swarm: Swarm<MyBehaviour>,
    external_ip: Option<String>,
    // ... rest of the code remains the same ...
    // Existing bootstrap topics (keep for backward compatibility)
    bootstrap_req_topic: IdentTopic,
    bootstrap_resp_topic: IdentTopic,
    // NEW: Aligned topics (additive)
    bootstrap_req_aligned_topic: IdentTopic,
    bootstrap_resp_aligned_topic: IdentTopic,
    // NEW: Missing critical topics
    heartbeat_topic: IdentTopic,
    pou_topic: IdentTopic,
    peer_info_topic: IdentTopic,
    block_topic: IdentTopic,
    /// Topic for receiving LN transactions via gossipsub
    tx_topic: IdentTopic,
    /// Topic for block+certificate single message (MN→LN)
    block_final_topic: IdentTopic,
    consensus_cert_topic: IdentTopic,
    peer_discovery_topic: IdentTopic,
    peer_registry_topic: IdentTopic,
    // NEW: Topic for lightnode registration (for group formation)
    registration_topic: IdentTopic,
    // NEW: Topic for lightnode group formed ACK
    group_formed_topic: IdentTopic,
    // NEW: Topic for lightnode proposals
    lightnode_proposal_topic: IdentTopic,
    // NEW: Topic for masternode votes
    masternode_vote_topic: IdentTopic,
    // NEW: Topic for leader election
    leader_election_topic: IdentTopic,
    // NEW: Topic for lightnode list sync
    lightnode_list_sync_topic: IdentTopic,
    /// Topic for block acceptance certificates (owner MN → other MN)
    block_acceptance_topic: IdentTopic,
    /// Topic per ricezione certificati di elezione proposer (LN → MN)
    election_cert_topic: IdentTopic,
    /// Topic per invio ACK whitelist al proposer (MN → LN)
    election_ack_topic: IdentTopic,
    /// Whitelist temporanea dei proposer autorizzati (peer_id → entry)
    proposer_whitelist: HashMap<PeerId, ProposerWhitelistEntry>,
    // NEW: Channels for block proposals and votes
    proposal_tx: Option<mpsc::UnboundedSender<super::proposal_validator::ProposalWithRole>>,
    vote_tx: Option<mpsc::UnboundedSender<super::proposal_validator::MasternodeVote>>,
    // NEW: Masternode message channel for main loop
    masternode_message_sender: mpsc::UnboundedSender<(PeerId, MasternodeMessage)>,
    // NEW: Outgoing masternode messages to publish via gossipsub
    masternode_publish_rx: mpsc::UnboundedReceiver<MasternodeMessage>,
    // NEW: Receiver for outgoing votes to broadcast
    vote_broadcast_rx: Option<mpsc::UnboundedReceiver<super::proposal_validator::MasternodeVote>>,
    // NEW: Receiver for outgoing certificates to broadcast
    certificate_broadcast_rx:
        Option<mpsc::UnboundedReceiver<super::proposal_validator::BlockCertificate>>,
    /// Receiver for block acceptance certs to publish (owner MN sends here from main)
    block_acceptance_publish_rx:
        mpsc::UnboundedReceiver<super::proposal_validator::BlockAcceptanceCertificate>,
    /// Proposals waiting for owner's acceptance cert (key = (group_id, height, round_id))
    pending_proposals:
        std::sync::Arc<tokio::sync::RwLock<HashMap<(String, u64, u64), PendingProposalEntry>>>,
    transaction_validator: Arc<std::sync::RwLock<TransactionValidator>>,
    // NEW: Group formation manager
    group_formation_manager:
        Option<std::sync::Arc<tokio::sync::RwLock<super::group_formation::GroupFormationManager>>>,
    // NEW: Batch collector for parallel processing
    peer_info_batch_collector: AdaptivePeerInfoBatchCollector,
    connected_peers: HashSet<PeerId>,
    current_height: u64,
    bootstrap_handler: BootstrapHandler,
    // NEW: P2P manager using savitri-p2p module
    p2p_manager: Option<P2PManager>,
    // NEW: Tracking data structures
    peer_activity: HashMap<PeerId, PeerActivityTracker>,
    pou_scores: HashMap<String, PouScoreEntry>,
    peer_registry: HashMap<PeerId, PeerRegistryEntry>,
    registry_records: HashMap<String, PeerRegistryRecord>,
    registry_ttl_secs: u64,
    masternode_bootstrap_addrs: Vec<String>, // NEW: Moved this field above finalized_blocks
    finalized_blocks: HashMap<[u8; 64], BlockFinalityEntry>,
    /// PERF: Dedup finalization per height — prevents counting same height multiple times
    finalized_heights: std::collections::HashSet<u64>,
    /// Certificates received before payload availability (race block_topic vs consensus_cert_topic).
    pending_certificate_finality: HashMap<[u8; 64], PendingCertificateFinality>,
    // Known masternode PeerIDs parsed from bootstrap_peers config
    known_masternode_peer_ids: HashSet<PeerId>,
    // UNIFIED PEERS: Shared peer map between Libp2pNetwork and MonolithP2PManager.
    // Libp2pNetwork writes on ConnectionEstablished/ConnectionClosed;
    // MonolithP2PManager reads for distribute_groups / get_stats.
    shared_peers:
        std::sync::Arc<tokio::sync::RwLock<HashMap<PeerId, crate::monolith_p2p::PeerInfo>>>,
    /// Cache best-effort per evitare di ripubblicare più volte lo stesso
    /// messaggio masternode in un breve intervallo, riducendo gli errori `Duplicate`
    /// provenienti da libp2p gossipsub.
    masternode_dedupe_cache: std::collections::HashSet<String>,
    /// Bootstrap phase tracking: true during initial network formation
    bootstrap_phase: bool,
    /// Expected total masternodes (for bootstrap phase detection)
    expected_masternodes: usize,
    /// Last mesh status log time (to avoid spam)
    last_mesh_status_log: std::time::Instant,
    /// Cache block by hash for BlockWithCertificate (best-effort LRU)
    block_cache: HashMap<[u8; 64], BlockMessageWire>,
    /// Insertion-order tracking for LRU eviction of block_cache
    block_cache_order: std::collections::VecDeque<[u8; 64]>,
    /// Persistent chain storage (RocksDB) used to save finalized block payloads.
    chain_storage: Arc<savitri_storage::Storage>,
}

/// Max blocks to cache for block+certificate combined message.
/// Increased from 200 to 500 to handle 4+ groups pipelining blocks.
const BLOCK_CACHE_MAX: usize = 500;
/// Max age for pending certificates without payload before eviction.
const PENDING_CERT_TTL_SECS: u64 = 120;
/// Safety cap for pending certificate entries.
const PENDING_CERT_MAX: usize = 2048;

impl Libp2pNetwork {
    fn dial_bootstrap_peers(&mut self) -> Result<()> {
        for entry in &self.masternode_bootstrap_addrs {
            let (peer_id, addr) = match entry.split_once('@') {
                Some((peer_id, addr)) => (peer_id, addr),
                None => {
                    warn!("bootstrap peer missing '@' separator: {}", entry);
                    continue;
                }
            };

            let peer_id = match PeerId::from_str(peer_id) {
                Ok(peer_id) => peer_id,
                Err(e) => {
                    warn!("invalid bootstrap peer id {}: {}", peer_id, e);
                    continue;
                }
            };

            let addr = match Multiaddr::from_str(addr) {
                Ok(addr) => addr,
                Err(e) => {
                    warn!("invalid bootstrap multiaddr {}: {}", addr, e);
                    continue;
                }
            };

            let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
                .addresses(vec![addr.clone()])
                .build();

            if let Err(err) = self.swarm.dial(dial_opts) {
                warn!("dial error to bootstrap peer {}: {}", peer_id, err);
            } else {
                info!("Dialing bootstrap masternode peer {}", peer_id);
                // Add to known_peers for bootstrap replies
                self.bootstrap_handler.add_known_peer(
                    peer_id,
                    vec![addr.to_string()],
                    false, // is_light_node = false, it's a masternode
                );
            }
        }

        Ok(())
    }

    /// Costruisce una chiave sintetica per la deduplica locale dei messaggi masternode.
    /// Usa gli helper definiti su `MasternodeMessage` per estrarre id/epoch/gruppi.
    /// GroupSyncRequest è escluso: va inviato periodicamente per allineare active_groups.
    fn build_masternode_dedupe_key(&self, message: &MasternodeMessage) -> Option<String> {
        if matches!(message, MasternodeMessage::GroupSyncRequest { .. }) {
            return None;
        }
        let proposal_id = message.get_proposal_id();
        let epoch = message.get_epoch();
        let groups = message.get_groups_count();

        if proposal_id == "unknown" && epoch == 0 && groups == 0 {
            return None;
        }

        Some(format!("{}:{}:{}", proposal_id, epoch, groups))
    }

    /// Create new libp2p network with group formation manager
    pub async fn with_group_manager(
        keypair: Keypair,
        port: u16,
        external_ip: Option<String>,
        group_manager: Arc<tokio::sync::RwLock<GroupFormationManager>>,
        bootstrap_peers: Vec<String>,
        registry_ttl_secs: u64,
        initial_bootstrap_blocks: u64,
        masternode_message_sender: mpsc::UnboundedSender<(PeerId, MasternodeMessage)>,
        masternode_publish_rx: mpsc::UnboundedReceiver<MasternodeMessage>,
        shared_peers: std::sync::Arc<
            tokio::sync::RwLock<HashMap<PeerId, crate::monolith_p2p::PeerInfo>>,
        >,
        chain_storage: Arc<savitri_storage::Storage>,
    ) -> Result<Self> {
        let local_peer_id = PeerId::from(keypair.public());

        // Build transport: QUIC (preferred) + TCP (fallback)
        let transport = {
            // TCP + DNS + Noise + Yamux
            let tcp =
                libp2p::tcp::tokio::Transport::new(libp2p::tcp::Config::default().nodelay(true));
            let dns_tcp = libp2p::dns::tokio::Transport::system(tcp)?;
            let noise_config = libp2p::noise::Config::new(&keypair)?;
            let yamux_config = libp2p::yamux::Config::default();
            let tcp_upgraded = dns_tcp
                .upgrade(upgrade::Version::V1)
                .authenticate(noise_config)
                .multiplex(yamux_config)
                .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)));

            // QUIC transport (built-in TLS 1.3, no Noise needed)
            let quic = libp2p::quic::tokio::Transport::new(libp2p::quic::Config::new(&keypair));

            // Prefer QUIC, fallback to TCP
            quic.or_transport(tcp_upgraded)
                .map(|either, _| match either {
                    futures::future::Either::Left((peer_id, muxer)) => {
                        (peer_id, StreamMuxerBox::new(muxer))
                    }
                    futures::future::Either::Right((peer_id, muxer)) => (peer_id, muxer),
                })
                .boxed()
        };

        // Build gossipsub behavior
        // CRITICAL FIX: Single gossipsub with flood publishing to solve InsufficientPeers
        let gossipsub = libp2p::gossipsub::Behaviour::new(
            libp2p::gossipsub::MessageAuthenticity::Signed(keypair.clone()),
            libp2p::gossipsub::ConfigBuilder::default()
                .heartbeat_interval(std::time::Duration::from_millis(7000)) // 7s: ottimizzato per testnet 120 nodi
                .validation_mode(libp2p::gossipsub::ValidationMode::Permissive) // Permissive accepts messages whose signature could not be verified; switch to Strict for stronger validation if all peers sign reliably.
                // ROUND 8: Scaled for 25-30 node networks (20 LN + 5 MN + TX gen)
                .mesh_n(12) // Target 12 peer (was 8) — covers ~50% of network
                .mesh_n_low(6) // Min 6 peer (was 4) — ensures quorum connectivity
                .mesh_n_high(18) // Max 18 peer (was 12) — room for full network
                .mesh_outbound_min(4) // Min 4 outbound (was 3)
                .history_gossip(5) // Aumentato per più gossip history (da 2)
                .history_length(10) // Aumentato per buffer più grande (da 3)
                .graft_flood_threshold(std::time::Duration::from_millis(500)) // CRITICAL FIX: Ridotto a 500ms
                .prune_peers(10) // Prune after 10 heartbeats without messages (70s with 7s heartbeat)
                // NOTE: During bootstrap, mesh warmup keepalive prevents premature pruning
                // flood_publish(true) ensures messages reach all peers even if mesh is small
                .duplicate_cache_time(std::time::Duration::from_secs(60)) // 60s: cache più lunga per reti grandi (da 500ms)
                .max_transmit_size(4_194_304) // 4MB per message (supports blocks with 2000+ TXs in JSON serialization)
                .flood_publish(true) // CRITICAL FIX: Enable flood publishing to solve InsufficientPeers
                // ROUND 7: Increased from 25K to 50K (matching LN)
                .connection_handler_queue_len(50000)
                .message_id_fn(|message| {
                    // Use message content for ID to ensure proper deduplication
                    use std::collections::hash_map::DefaultHasher;
                    use std::hash::{Hash, Hasher};
                    let mut hasher = DefaultHasher::new();
                    std::hash::Hash::hash(&message.data, &mut hasher);
                    libp2p::gossipsub::MessageId::from(hasher.finish().to_be_bytes().to_vec())
                })
                .build()
                .map_err(|e| anyhow!("Invalid gossipsub configuration: {}", e))?,
        )
        .map_err(|e| anyhow!("Failed to create gossipsub behaviour: {}", e))?;

        let mut kademlia = Kademlia::new(local_peer_id, MemoryStore::new(local_peer_id));
        kademlia.set_mode(Some(Mode::Server));
        let consensus = super::consensus_protocol::build_consensus_behaviour();

        // NAT traversal: relay server (tuned for blockchain) + identify + autonat + dcutr
        let relay_config = libp2p::relay::Config {
            max_reservations: 512, // support up to 500 NATted lightnodes
            max_reservations_per_peer: 4,
            reservation_duration: Duration::from_secs(3600), // 1 hour
            reservation_rate_limiters: vec![],               // trust our known peers
            max_circuits: 256,                               // concurrent relayed connections
            max_circuits_per_peer: 8,
            max_circuit_duration: Duration::from_secs(300), // 5 min for block finalization
            max_circuit_bytes: 4_194_304,                   // 4MB — match block max_transmit_size
            circuit_src_rate_limiters: vec![],
        };
        let relay = libp2p::relay::Behaviour::new(local_peer_id, relay_config);
        let identify = libp2p::identify::Behaviour::new(
            libp2p::identify::Config::new("/savitri/1.0.0".to_string(), keypair.public())
                .with_agent_version("savitri-masternode/0.1.0".to_string()),
        );
        let autonat =
            libp2p::autonat::Behaviour::new(local_peer_id, libp2p::autonat::Config::default());
        let dcutr = libp2p::dcutr::Behaviour::new(local_peer_id);
        let upnp = libp2p::upnp::tokio::Behaviour::default();

        let behaviour = MyBehaviour {
            gossipsub,
            kademlia,
            consensus,
            relay,
            identify,
            autonat,
            dcutr,
            upnp,
        };

        // Create swarm with gossipsub + kademlia
        // NOTE: No idle connection timeout for masternodes - they must stay connected
        let swarm = libp2p::swarm::Swarm::new(
            transport,
            behaviour,
            local_peer_id,
            libp2p::swarm::Config::with_tokio_executor(),
        );

        // Initialize bootstrap handler (initial_bootstrap_blocks=0 in testnet reali)
        let bootstrap_handler =
            BootstrapHandler::with_group_manager(group_manager.clone(), initial_bootstrap_blocks);

        // Use masternode bootstrap addresses from configuration
        let masternode_bootstrap_addrs = bootstrap_peers;

        let registry_ttl_secs = if registry_ttl_secs == 0 {
            DEFAULT_REGISTRY_TTL_SECS
        } else {
            registry_ttl_secs
        };

        let mut network = Self {
            swarm,
            external_ip,
            // Existing bootstrap topics (keep for backward compatibility)
            bootstrap_req_topic: IdentTopic::new("bootstrap/request"),
            bootstrap_resp_topic: IdentTopic::new("bootstrap/response"),
            // NEW: Aligned topics (additive)
            bootstrap_req_aligned_topic: IdentTopic::new("/savitri/bootstrap/req/1"),
            bootstrap_resp_aligned_topic: IdentTopic::new("/savitri/bootstrap/resp/1"),
            // NEW: Missing critical topics
            heartbeat_topic: IdentTopic::new("/savitri/heartbeat/1"),
            pou_topic: IdentTopic::new("/savitri/pou/1"),
            peer_info_topic: IdentTopic::new("/savitri/peerinfo/1"),
            block_topic: IdentTopic::new("/savitri/block/1"),
            tx_topic: IdentTopic::new("/savitri/tx/1"),
            block_final_topic: IdentTopic::new("/savitri/block_final/1"),
            consensus_cert_topic: IdentTopic::new("/savitri/consensus/cert/1"),
            peer_discovery_topic: IdentTopic::new("/savitri/peer_discovery/1"),
            peer_registry_topic: IdentTopic::new("/savitri/peer_registry/1"),
            registration_topic: IdentTopic::new("/savitri/registration/1"),
            group_formed_topic: IdentTopic::new("/savitri/lightnode/group/formed/1"),
            lightnode_proposal_topic: IdentTopic::new("/savitri/masternode/proposal/1"),
            masternode_vote_topic: IdentTopic::new("/savitri/masternode/vote/1"),
            leader_election_topic: IdentTopic::new("/savitri/masternode/leader/election/1"),
            lightnode_list_sync_topic: IdentTopic::new("/savitri/masternode/lightnode_list/sync/1"),
            block_acceptance_topic: IdentTopic::new("/savitri/masternode/block_acceptance/1"),
            // Handshake 3-fasi: topic + whitelist per proposer autorizzati
            election_cert_topic: IdentTopic::new("/savitri/masternode/election/cert/1"),
            election_ack_topic: IdentTopic::new("/savitri/masternode/election/ack/1"),
            proposer_whitelist: HashMap::new(),
            // NEW: Channels for proposals and votes (will be set later)
            proposal_tx: None,
            vote_tx: None,
            masternode_message_sender,
            masternode_publish_rx,
            vote_broadcast_rx: None,
            certificate_broadcast_rx: None,
            block_acceptance_publish_rx: mpsc::unbounded_channel().1, // Dummy, will be set via set_block_acceptance_channel
            pending_proposals: std::sync::Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            transaction_validator: Arc::new(RwLock::new(TransactionValidator::new())),
            // NEW: Group formation manager
            group_formation_manager: Some(group_manager),
            // NEW: Batch collector for parallel processing
            peer_info_batch_collector: AdaptivePeerInfoBatchCollector::new(),
            connected_peers: HashSet::new(),
            current_height: 0,
            bootstrap_handler,
            // NEW: P2P manager using savitri-p2p module
            p2p_manager: None,
            // NEW: Tracking data structures
            peer_activity: HashMap::new(),
            pou_scores: HashMap::new(),
            peer_registry: HashMap::new(),
            registry_records: HashMap::new(),
            registry_ttl_secs,
            masternode_bootstrap_addrs: masternode_bootstrap_addrs.clone(),
            finalized_blocks: HashMap::new(),
            finalized_heights: std::collections::HashSet::new(),
            pending_certificate_finality: HashMap::new(),
            // Known masternode PeerIDs parsed from bootstrap addresses
            known_masternode_peer_ids: masternode_bootstrap_addrs
                .iter()
                .filter_map(|entry| {
                    entry
                        .split_once('@')
                        .and_then(|(peer_id_str, _)| PeerId::from_str(peer_id_str).ok())
                })
                .collect(),
            // UNIFIED PEERS: shared with MonolithP2PManager
            shared_peers,
            masternode_dedupe_cache: std::collections::HashSet::new(),
            bootstrap_phase: true, // Start in bootstrap phase
            expected_masternodes: masternode_bootstrap_addrs.len().max(5), // At least 5 expected
            last_mesh_status_log: std::time::Instant::now(),
            block_cache: HashMap::new(),
            block_cache_order: std::collections::VecDeque::new(),
            chain_storage,
        };

        if !masternode_bootstrap_addrs.is_empty() {
            let response = PeerDiscoveryResponse {
                requesting_peer: local_peer_id.to_string(),
                masternode_peers: masternode_bootstrap_addrs.clone(),
            };
            match serde_json::to_vec(&response) {
                Ok(payload) => {
                    if let Err(e) = network.put_kad_record("masternode_peers", payload) {
                        warn!("Failed to publish masternode peers via Kademlia: {}", e);
                    } else {
                        info!("Published masternode peers list via Kademlia");
                    }
                }
                Err(e) => {
                    warn!("Failed to encode masternode peers record: {}", e);
                }
            }
        }

        // Listen on the specified port (TCP + QUIC)
        let listen_addr: libp2p::Multiaddr = format!("/ip4/0.0.0.0/tcp/{}", port).parse()?;
        network.swarm.listen_on(listen_addr.clone())?;
        info!(%listen_addr, port, "Masternode TCP listen requested");

        let quic_listen_addr: libp2p::Multiaddr =
            format!("/ip4/0.0.0.0/udp/{}/quic-v1", port).parse()?;
        network.swarm.listen_on(quic_listen_addr.clone())?;
        info!(%quic_listen_addr, port, "Masternode QUIC listen requested");

        if let Some(ref ext_ip) = network.external_ip {
            let parsed: Result<IpAddr, _> = ext_ip.parse();
            match parsed {
                Ok(ip) if !is_routable_ip(&ip) => {
                    warn!(
                        external_ip = %ext_ip,
                        "Configured external_ip is private/local; skipping external address registration"
                    );
                }
                Ok(IpAddr::V4(v4)) => {
                    let ext_addr: Multiaddr = format!("/ip4/{}/tcp/{}", v4, port).parse()?;
                    network.swarm.add_external_address(ext_addr.clone());
                    info!(%ext_addr, "Registered masternode TCP external address");
                    let quic_ext: Multiaddr =
                        format!("/ip4/{}/udp/{}/quic-v1", v4, port).parse()?;
                    network.swarm.add_external_address(quic_ext.clone());
                    info!(%quic_ext, "Registered masternode QUIC external address");
                }
                Ok(IpAddr::V6(v6)) => {
                    let ext_addr: Multiaddr = format!("/ip6/{}/tcp/{}", v6, port).parse()?;
                    network.swarm.add_external_address(ext_addr.clone());
                    info!(%ext_addr, "Registered masternode TCP external address");
                    let quic_ext: Multiaddr =
                        format!("/ip6/{}/udp/{}/quic-v1", v6, port).parse()?;
                    network.swarm.add_external_address(quic_ext.clone());
                    info!(%quic_ext, "Registered masternode QUIC external address");
                }
                Err(_) => {
                    warn!(
                        external_ip = %ext_ip,
                        "Invalid external_ip in config; skipping external address registration"
                    );
                }
            }
        }

        // Subscribe to all topics using single gossipsub
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.bootstrap_req_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.bootstrap_resp_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.bootstrap_req_aligned_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.bootstrap_resp_aligned_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.heartbeat_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.pou_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.peer_info_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.block_topic)?;
        // MN does not need tx_topic: TXs arrive inside LightnodeProposal on block_topic.
        // Subscribing causes gossipsub to forward all LN transactions through the MN mesh,
        // saturating the send queue (~550K "Send Queue full" errors per MN in 3-min test).
        // network.swarm.behaviour_mut().subscribe(&network.tx_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.consensus_cert_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.peer_discovery_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.peer_registry_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.registration_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.group_formed_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.lightnode_proposal_topic)?;
        // MN votes and block acceptance now use direct P2P (request-response),
        // no longer need gossipsub subscription for these topics.
        // network.swarm.behaviour_mut().subscribe(&network.masternode_vote_topic)?;
        // network.swarm.behaviour_mut().subscribe(&network.block_acceptance_topic)?;
        // Handshake 3-fasi: subscribe ai topic per certificati di elezione e ACK
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.election_cert_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.election_ack_topic)?;
        info!("Subscribed to proposer election handshake topics: election/cert + election/ack");

        // Define additional topics
        let group_proposal_topic = IdentTopic::new("/savitri/masternode/group/proposal/1");
        let group_vote_topic = IdentTopic::new("/savitri/masternode/group/vote/1");
        let group_sync_topic = IdentTopic::new("/savitri/masternode/group/sync/1");

        network
            .swarm
            .behaviour_mut()
            .subscribe(&group_proposal_topic)?;
        network.swarm.behaviour_mut().subscribe(&group_vote_topic)?;
        network.swarm.behaviour_mut().subscribe(&group_sync_topic)?;

        // Subscribe to leader election + lightnode list sync topics
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.leader_election_topic)?;
        network
            .swarm
            .behaviour_mut()
            .subscribe(&network.lightnode_list_sync_topic)?;
        info!("Subscribed to leader election and lightnode list sync topics");

        // Dial bootstrap peers to establish masternode-to-masternode connections
        info!(
            "Dialing {} bootstrap masternode peers",
            masternode_bootstrap_addrs.len()
        );
        if let Err(e) = network.dial_bootstrap_peers() {
            warn!("Failed to dial some bootstrap peers: {}", e);
        }

        Ok(network)
    }

    fn persist_finalized_block_to_db(&self, block_hash: &[u8; 64], height: u64) -> Result<bool> {
        let Some(block_wire) = self.block_cache.get(block_hash).cloned() else {
            return Ok(false);
        };

        let block_bytes = serde_json::to_vec(&block_wire)?;
        self.chain_storage
            .put_cf(CF_BLOCKS, block_hash, &block_bytes)?;

        // Keep chain_head encoding compatible with startup probe in main.rs:
        // [0..64)=block_hash, [64..72)=little-endian height.
        let mut chain_head = Vec::with_capacity(72);
        chain_head.extend_from_slice(block_hash);
        chain_head.extend_from_slice(&height.to_le_bytes());
        self.chain_storage
            .put_cf(CF_METADATA, b"chain_head", &chain_head)?;
        Ok(true)
    }

    fn prune_pending_certificate_finality(&mut self) {
        let now = current_timestamp();
        let before = self.pending_certificate_finality.len();
        self.pending_certificate_finality
            .retain(|_, entry| now.saturating_sub(entry.received_at) <= PENDING_CERT_TTL_SECS);
        let expired = before.saturating_sub(self.pending_certificate_finality.len());
        if expired > 0 {
            warn!(
                expired,
                remaining = self.pending_certificate_finality.len(),
                ttl_secs = PENDING_CERT_TTL_SECS,
                "Evicted expired pending certificate finality entries"
            );
        }

        if self.pending_certificate_finality.len() > PENDING_CERT_MAX {
            let mut by_age: Vec<([u8; 64], u64)> = self
                .pending_certificate_finality
                .iter()
                .map(|(hash, entry)| (*hash, entry.received_at))
                .collect();
            by_age.sort_by_key(|(_, ts)| *ts);

            let to_remove = self.pending_certificate_finality.len() - PENDING_CERT_MAX;
            for (hash, _) in by_age.into_iter().take(to_remove) {
                self.pending_certificate_finality.remove(&hash);
            }
            warn!(
                removed = to_remove,
                max = PENDING_CERT_MAX,
                remaining = self.pending_certificate_finality.len(),
                "Pending certificate finality map trimmed to cap"
            );
        }
    }

    fn finalize_with_cached_payload(
        &mut self,
        block_hash: [u8; 64],
        height: u64,
        proposer_group_id: String,
        reason: &'static str,
    ) -> Result<bool> {
        if self.finalized_blocks.contains_key(&block_hash) {
            self.pending_certificate_finality.remove(&block_hash);
            debug!(
                hash = %hex::encode(&block_hash[..8]),
                height,
                reason,
                "Certificate refers to already finalized block (hash dedup)"
            );
            return Ok(true);
        }
        // PERF: Also dedup by height — different block hashes at the same height
        // (from proposer rotation) should not be counted twice
        if self.finalized_heights.contains(&height) {
            debug!(
                hash = %hex::encode(&block_hash[..8]),
                height,
                reason,
                "Block at this height already finalized (height dedup)"
            );
            return Ok(true);
        }

        let cached_block = match self.block_cache.get(&block_hash) {
            Some(block) => block,
            None => return Ok(false),
        };

        // Extract TX count from cached block payload for accurate logging
        let tx_count = cached_block.txs.len();
        let confirmed_txs: Vec<[u8; 32]> = Vec::new();
        let rejected_txs: Vec<[u8; 32]> = Vec::new();
        let finality_entry = BlockFinalityEntry {
            block_hash,
            height,
            proposer_group_id,
            confirmed_transactions: confirmed_txs.clone(),
            rejected_transactions: rejected_txs.clone(),
            finalized_at: current_timestamp(),
        };
        self.finalized_blocks.insert(block_hash, finality_entry);
        self.finalized_heights.insert(height);
        self.pending_certificate_finality.remove(&block_hash);

        match self.persist_finalized_block_to_db(&block_hash, height) {
            Ok(true) => {
                info!(
                    block_hash = %hex::encode(&block_hash[..8]),
                    height,
                    reason,
                    "💾 Persisted finalized block payload to RocksDB"
                );
            }
            Ok(false) => {
                warn!(
                    block_hash = %hex::encode(&block_hash[..8]),
                    height,
                    reason,
                    "Finalized block not persisted: payload missing in block cache"
                );
            }
            Err(e) => {
                error!(
                    block_hash = %hex::encode(&block_hash[..8]),
                    height,
                    reason,
                    error = %e,
                    "Failed to persist finalized block payload to RocksDB"
                );
            }
        }

        info!(
            block_hash = %hex::encode(&block_hash[..8]),
            height,
            txs_in_block = tx_count,
            total_finalized = self.finalized_blocks.len(),
            reason,
            "📥 [MN] Block finalized and stored with payload"
        );
        Ok(true)
    }

    fn enqueue_pending_certificate_finality(
        &mut self,
        block_hash: [u8; 64],
        height: u64,
        proposer_group_id: String,
        source: Option<PeerId>,
        cert_kind: &'static str,
    ) {
        self.prune_pending_certificate_finality();
        let now = current_timestamp();

        match self.pending_certificate_finality.get_mut(&block_hash) {
            Some(entry) => {
                entry.height = height;
                entry.proposer_group_id = proposer_group_id;
                entry.received_at = now;
                warn!(
                    block_hash = %hex::encode(&block_hash[..8]),
                    height,
                    cert_kind,
                    source = ?source,
                    pending = self.pending_certificate_finality.len(),
                    "Block payload still missing; refreshed pending certificate entry"
                );
            }
            None => {
                self.pending_certificate_finality.insert(
                    block_hash,
                    PendingCertificateFinality {
                        block_hash,
                        height,
                        proposer_group_id,
                        received_at: now,
                    },
                );
                warn!(
                    block_hash = %hex::encode(&block_hash[..8]),
                    height,
                    cert_kind,
                    source = ?source,
                    pending = self.pending_certificate_finality.len(),
                    "Block payload missing for certificate; queued pending finalization"
                );
            }
        }
    }

    fn try_finalize_pending_from_cache(
        &mut self,
        block_hash: [u8; 64],
        cached_height: u64,
    ) -> Result<bool> {
        self.prune_pending_certificate_finality();
        let Some(pending) = self.pending_certificate_finality.get(&block_hash).cloned() else {
            return Ok(false);
        };

        if pending.height != cached_height {
            warn!(
                block_hash = %hex::encode(&block_hash[..8]),
                cert_height = pending.height,
                block_height = cached_height,
                "Pending certificate height differs from cached block height"
            );
        }

        self.finalize_with_cached_payload(
            pending.block_hash,
            pending.height,
            pending.proposer_group_id,
            "pending-certificate-drain",
        )
    }

    /// Get local peer ID
    pub fn local_peer_id(&self) -> PeerId {
        *self.swarm.local_peer_id()
    }

    /// Get a shared reference to pending_proposals so that the backup timeout
    /// task (spawned from main.rs) can check and remove entries.
    pub fn pending_proposals_ref(
        &self,
    ) -> std::sync::Arc<tokio::sync::RwLock<HashMap<(String, u64, u64), PendingProposalEntry>>>
    {
        self.pending_proposals.clone()
    }

    /// Update current chain height
    pub fn set_height(&mut self, height: u64) {
        self.current_height = height;
    }

    /// Get connected peer count
    pub fn connected_peer_count(&self) -> usize {
        self.connected_peers.len()
    }

    /// Publish bootstrap reply (wrapped in ResponseMessage to match lightnode).
    /// Publishes on both legacy topic and aligned topic so lightnodes receive the reply.
    fn publish_bootstrap_reply(&mut self, reply: &BootstrapReply) -> Result<()> {
        // Wrap in ResponseMessage to match lightnode format
        let response = ResponseMessage::Bootstrap(reply.clone());
        let data = serde_json::to_vec(&response)?;
        let mut published = false;
        match self
            .swarm
            .behaviour_mut()
            .publish(self.bootstrap_resp_topic.clone(), data.clone())
        {
            Ok(_) => {
                published = true;
            }
            Err(e) => {
                debug!(error = %e, "Bootstrap reply publish (old topic) skipped: {}", e);
            }
        }
        // Publish also on aligned topic so lightnodes subscribed to /savitri/bootstrap/resp/1 receive it
        match self
            .swarm
            .behaviour_mut()
            .publish(self.bootstrap_resp_aligned_topic.clone(), data)
        {
            Ok(_) => {
                published = true;
            }
            Err(e) => {
                debug!(error = %e, "Bootstrap reply publish (aligned topic) skipped: {}", e);
            }
        }
        if published {
            info!("Published bootstrap reply via gossipsub (wrapped in ResponseMessage)");
        } else {
            warn!("Bootstrap reply could not be published on any topic");
        }
        Ok(())
    }

    /// Handle bootstrap request and send reply
    fn handle_bootstrap_request(&mut self, data: &[u8]) -> Result<()> {
        // SECURITY: Reject oversized messages before deserialization
        const MAX_MESSAGE_SIZE: usize = 1_048_576; // 1 MB
        if data.len() > MAX_MESSAGE_SIZE {
            anyhow::bail!(
                "Rejecting oversized bootstrap request: {} bytes (max {})",
                data.len(),
                MAX_MESSAGE_SIZE
            );
        }
        // Decode the RequestMessage wrapper first (matching lightnode format)
        match serde_json::from_slice::<RequestMessage>(data) {
            Ok(RequestMessage::Bootstrap(request)) => {
                info!(
                    version = request.version,
                    end_height = request.end_height,
                    max_blocks = request.max_blocks,
                    "Received bootstrap request from lightnode"
                );

                // Build reply with current chain state
                // Get peers from bootstrap handler's known_peers (includes bootstrap masternodes)
                let peers: Vec<BootstrapPeerInfo> = self
                    .bootstrap_handler
                    .get_known_peers()
                    .into_iter()
                    .map(|(peer_id, peer_info)| BootstrapPeerInfo {
                        peer_id: peer_id.to_string(),
                        addresses: peer_info.addresses,
                        is_light_node: peer_info.is_light_node,
                        is_masternode: !peer_info.is_light_node,
                        is_validator: false,
                    })
                    .collect();

                let reply = BootstrapReply {
                    blocks: vec![BootstrapBlockInfo {
                        height: self.current_height,
                        hash: vec![0u8; 64],
                        timestamp: std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs(),
                        tx_count: 0,
                    }],
                    accounts: vec![],
                    peers,
                };

                // Send reply (non-fatal: Duplicate/InsufficientPeers should not crash)
                if let Err(e) = self.publish_bootstrap_reply(&reply) {
                    warn!(error = %e, "Bootstrap reply publish failed (non-fatal)");
                } else {
                    info!("Bootstrap reply sent successfully");
                }
            }
            Ok(_) => {
                debug!("Received non-bootstrap request on bootstrap topic");
            }
            Err(e) => {
                warn!("Failed to decode bootstrap request: {}", e);
            }
        }
        Ok(())
    }

    /// Initialize P2P manager using savitri-p2p module
    pub fn with_p2p_manager(mut self, p2p_manager: P2PManager) -> Self {
        self.p2p_manager = Some(p2p_manager);
        self
    }

    /// Get P2P manager reference
    pub fn get_p2p_manager(&self) -> Option<&P2PManager> {
        self.p2p_manager.as_ref()
    }

    /// Set proposal, vote, and certificate channels
    pub fn set_proposal_channels(
        &mut self,
        proposal_tx: mpsc::UnboundedSender<super::proposal_validator::ProposalWithRole>,
        vote_tx: mpsc::UnboundedSender<super::proposal_validator::MasternodeVote>,
        vote_broadcast_rx: mpsc::UnboundedReceiver<super::proposal_validator::MasternodeVote>,
        certificate_broadcast_rx: mpsc::UnboundedReceiver<
            super::proposal_validator::BlockCertificate,
        >,
    ) {
        self.proposal_tx = Some(proposal_tx);
        self.vote_tx = Some(vote_tx);
        self.vote_broadcast_rx = Some(vote_broadcast_rx);
        self.certificate_broadcast_rx = Some(certificate_broadcast_rx);
        info!("Proposal, vote, and certificate channels configured for P2P network");
    }

    pub fn set_block_acceptance_channel(
        &mut self,
        block_acceptance_publish_rx: mpsc::UnboundedReceiver<
            super::proposal_validator::BlockAcceptanceCertificate,
        >,
    ) {
        self.block_acceptance_publish_rx = block_acceptance_publish_rx;
        info!("BlockAcceptanceCertificate channel configured");
    }

    /// Broadcast a vote to other masternodes via direct P2P (request-response)
    pub fn broadcast_vote(
        &mut self,
        vote: &super::proposal_validator::MasternodeVote,
    ) -> Result<()> {
        info!(
            height = vote.height,
            round_id = vote.round_id,
            vote_type = ?vote.vote_type,
            "📤 [MN->MN] Sending block vote to other masternodes (direct P2P)"
        );
        let data = serde_json::to_vec(vote)?;
        let msg = super::consensus_protocol::ConsensusMessage::Vote(data);

        // Send directly to each connected masternode
        let local_id = *self.swarm.local_peer_id();
        let mn_peers: Vec<PeerId> = self
            .known_masternode_peer_ids
            .iter()
            .filter(|p| **p != local_id && self.connected_peers.contains(p))
            .cloned()
            .collect();

        let mut sent = 0;
        for peer_id in &mn_peers {
            let _req_id = self
                .swarm
                .behaviour_mut()
                .consensus
                .send_request(peer_id, msg.clone());
            sent += 1;
        }
        debug!(
            sent,
            total_known = self.known_masternode_peer_ids.len(),
            height = vote.height,
            "Vote sent to masternodes via direct P2P"
        );

        Ok(())
    }

    /// Publish block+certificate in a single message (preferred path for lightnodes).
    /// Returns true if block was in cache and combined message was published.
    pub fn broadcast_block_with_certificate(
        &mut self,
        certificate: &super::proposal_validator::BlockCertificate,
    ) -> Result<bool> {
        let block = match self.block_cache.get(&certificate.block_hash).cloned() {
            Some(b) => b,
            None => {
                warn!(
                    height = certificate.height,
                    hash = %hex::encode(&certificate.block_hash[..8]),
                    "No cached block for cert; publishing cert only (fallback). LN should publish block to block_topic before proposal."
                );
                return Ok(false);
            }
        };
        let payload = BlockWithCertificateWire {
            block: block.clone(),
            certificate: certificate.clone(),
        };
        let data = serde_json::to_vec(&payload)?;
        match self
            .swarm
            .behaviour_mut()
            .publish(self.block_final_topic.clone(), data)
        {
            Ok(_) => {
                info!(
                    height = certificate.height,
                    round_id = certificate.round_id,
                    hash = %hex::encode(&certificate.block_hash[..8]),
                    "📤 [MN->LN] BlockWithCertificate published on block_final (single message)"
                );
                Ok(true)
            }
            Err(e) => {
                error!(
                    error = %e,
                    height = certificate.height,
                    "broadcast_block_with_certificate publish failed (non-fatal)"
                );
                Ok(false)
            }
        }
    }

    /// Broadcast a block certificate alone (fallback when block not in cache; backward compat).
    pub fn broadcast_certificate(
        &mut self,
        certificate: &super::proposal_validator::BlockCertificate,
    ) -> Result<()> {
        info!(
            height = certificate.height,
            round_id = certificate.round_id,
            votes = certificate.votes.len(),
            topic = %self.consensus_cert_topic,
            "📤 [MN->LN] Broadcasting block certificate only (fallback / legacy)"
        );

        let data = serde_json::to_vec(certificate)?;
        match self
            .swarm
            .behaviour_mut()
            .publish(self.consensus_cert_topic.clone(), data)
        {
            Ok(_) => {
                info!(
                    height = certificate.height,
                    round_id = certificate.round_id,
                    "📤 [MN->LN] Block certificate published (consensus_cert topic)"
                );
            }
            Err(e) => {
                error!(
                    error = %e,
                    height = certificate.height,
                    round_id = certificate.round_id,
                    "broadcast_certificate publish failed (non-fatal)"
                );
            }
        }

        Ok(())
    }

    /// Broadcast BlockAcceptanceCertificate to other masternodes via direct P2P
    pub fn broadcast_block_acceptance_certificate(
        &mut self,
        cert: &super::proposal_validator::BlockAcceptanceCertificate,
    ) -> Result<()> {
        info!(
            height = cert.height,
            round_id = cert.round_id,
            group_id = %cert.group_id,
            owner = %cert.owner_masternode_id,
            "📤 [MN->MN] Broadcasting BlockAcceptanceCertificate to other masternodes (direct P2P)"
        );
        let data = serde_json::to_vec(cert)?;
        let msg = super::consensus_protocol::ConsensusMessage::BlockAcceptance(data);

        // Send directly to each connected masternode
        let local_id = *self.swarm.local_peer_id();
        let mn_peers: Vec<PeerId> = self
            .known_masternode_peer_ids
            .iter()
            .filter(|p| **p != local_id && self.connected_peers.contains(p))
            .cloned()
            .collect();

        let mut sent = 0;
        for peer_id in &mn_peers {
            let _req_id = self
                .swarm
                .behaviour_mut()
                .consensus
                .send_request(peer_id, msg.clone());
            sent += 1;
        }

        if sent > 0 {
            info!(
                sent,
                height = cert.height,
                round_id = cert.round_id,
                "BlockAcceptanceCertificate sent to masternodes via direct P2P"
            );
        } else {
            warn!(
                height = cert.height,
                round_id = cert.round_id,
                "No connected masternodes to send BlockAcceptanceCertificate"
            );
        }
        Ok(())
    }

    /// Handle BlockAcceptanceCertificate received from leader (or backup) masternode.
    ///
    /// received the proposal.  The only purpose of pending_proposals is to let the
    /// **backup** MN know that the leader published successfully, so the backup
    /// timeout can be cancelled (entry removed).
    async fn handle_block_acceptance_certificate(
        &mut self,
        data: &[u8],
        source: Option<PeerId>,
    ) -> Result<()> {
        if let Ok(cert) =
            serde_json::from_slice::<super::proposal_validator::BlockAcceptanceCertificate>(data)
        {
            info!(
                height = cert.height,
                round_id = cert.round_id,
                group_id = %cert.group_id,
                owner = %cert.owner_masternode_id,
                source = ?source,
                "📥 [MN] BlockAcceptanceCertificate received — verifying signature"
            );

            // Verify certificate signature using the signer's public key embedded in the cert
            if !super::proposal_validator::ProposalValidator::verify_block_acceptance(&cert) {
                warn!(
                    owner = %cert.owner_masternode_id,
                    height = cert.height,
                    round_id = cert.round_id,
                    "❌ [MN] BlockAcceptanceCertificate signature verification FAILED — REJECTING"
                );
                return Ok(());
            }
            info!(
                "✅ [MN] BlockAcceptanceCertificate signature verified from masternode {}",
                cert.owner_masternode_id
            );

            // Remove from pending_proposals if present (cancels backup timeout).
            let key = (cert.group_id.clone(), cert.height, cert.round_id);
            let mut pending = self.pending_proposals.write().await;
            if pending.remove(&key).is_some() {
                info!(
                    height = cert.height,
                    round_id = cert.round_id,
                    group_id = %cert.group_id,
                    "✅ [MN-BACKUP] Leader published cert — backup timeout cancelled"
                );
            } else {
                debug!(
                    height = cert.height,
                    round_id = cert.round_id,
                    group_id = %cert.group_id,
                    "BlockAcceptanceCertificate received (no pending entry — normal for non-backup MNs)"
                );
            }
        }
        Ok(())
    }

    /// Handle lightnode proposal message (async for group/certificate verification)
    async fn handle_lightnode_proposal(
        &mut self,
        data: &[u8],
        source: Option<PeerId>,
    ) -> Result<()> {
        info!(
            source = ?source,
            data_len = data.len(),
            "📥 [MN<-LN] Step 1: Received block proposal from lightnode (gossipsub)"
        );

        // Try to decode the proposal
        match serde_json::from_slice::<super::proposal_validator::LightnodeProposal>(data) {
            Ok(proposal) => {
                info!(
                    height = proposal.height,
                    round_id = proposal.round_id,
                    tx_count = proposal.tx_count,
                    proposer = %hex::encode(&proposal.proposer_pubkey[..8]),
                    group_id = %proposal.proposer_group_id,
                    source = ?source,
                    "📥 [MN<-LN] Step 2: Decoded block proposal from lightnode"
                );

                if let Some(raw_txs) = proposal.raw_txs.as_ref() {
                    self.cache_block_payload(
                        proposal.block_hash,
                        proposal.height,
                        proposal.proposer_pubkey,
                        raw_txs.clone(),
                        "proposal-inline",
                    );
                } else if proposal.tx_count > 0 {
                    debug!(
                        block_hash = %hex::encode(&proposal.block_hash[..8]),
                        height = proposal.height,
                        tx_count = proposal.tx_count,
                        "Proposal has tx_count but no inline payload; relying on block_topic cache"
                    );
                }

                // If proposal has group_id, verify group and election certificate
                if !proposal.proposer_group_id.is_empty() {
                    let group_members: Vec<String> =
                        if let Some(ref gm) = self.group_formation_manager {
                            let manager = gm.read().await;
                            let groups = manager.get_active_groups().await;
                            groups
                                .into_iter()
                                .find(|g| g.group_id == proposal.proposer_group_id)
                                .map(|g| g.members)
                                .unwrap_or_default()
                        } else {
                            vec![]
                        };
                    if !super::proposal_validator::ProposalValidator::verify_proposal_group_and_certificate(
                        &proposal,
                        &group_members,
                    ) {
                        warn!(
                            group_id = %proposal.proposer_group_id,
                            "Proposal rejected: group or election certificate verification failed"
                        );
                        return Ok(());
                    }
                }

                // Determine the role of this masternode for this group:
                if !proposal.proposer_group_id.is_empty() {
                    let local_peer_id_str = self.swarm.local_peer_id().to_string();

                    let role = if let Some(ref gm) = self.group_formation_manager {
                        let manager = gm.read().await;
                        let groups = manager.get_active_groups().await;
                        // First try exact group_id match, then fall back to group-index match.
                        // Group IDs contain the epoch and change across re-formations
                        // (group_0_0_0 → group_1_0_1) but the group index (middle number)
                        // stays consistent. Without this fallback, all MNs become Participant
                        // after the first group re-formation.
                        // the manual `parts[2]` parsing into the canonical
                        // `group_index_from_id` primitive — same logic as the
                        // cert handler below and the vote_aggregator quorum
                        // lookup, so all three paths agree on the stable
                        // group identity across epoch drift.
                        let proposal_index =
                            savitri_consensus::primitives::group_id::group_index_from_id(
                                &proposal.proposer_group_id,
                            );
                        let group_match = groups
                            .iter()
                            .find(|g| g.group_id == proposal.proposer_group_id)
                            .or_else(|| {
                                proposal_index.and_then(|idx| {
                                    groups.iter().find(|g| {
                                        savitri_consensus::primitives::group_id::group_index_from_id(&g.group_id)
                                            == Some(idx)
                                    })
                                })
                            });

                        if let Some(group) = group_match {
                            let is_leader = group
                                .group_leader_masternode
                                .as_ref()
                                .map(|l| l == &local_peer_id_str)
                                .unwrap_or(false);
                            let is_backup = group
                                .backup_leader_masternode
                                .as_ref()
                                .map(|b| b == &local_peer_id_str)
                                .unwrap_or(false);

                            if is_leader {
                                super::proposal_validator::MnGroupRole::Leader
                            } else if is_backup {
                                super::proposal_validator::MnGroupRole::Backup
                            } else {
                                super::proposal_validator::MnGroupRole::Participant
                            }
                        } else {
                            warn!(
                                group_id = %proposal.proposer_group_id,
                                local_peer = %local_peer_id_str,
                                "Group not found in local state, participating as Participant"
                            );
                            super::proposal_validator::MnGroupRole::Participant
                        }
                    } else {
                        super::proposal_validator::MnGroupRole::Participant
                    };

                    info!(
                        height = proposal.height,
                        round_id = proposal.round_id,
                        group_id = %proposal.proposer_group_id,
                        role = ?role,
                        local_peer = %local_peer_id_str,
                        "🔍 [ROLE-DETECTION] Masternode role for this group proposal"
                    );

                    if let Some(ref tx) = self.proposal_tx {
                        let wrapped = super::proposal_validator::ProposalWithRole {
                            proposal: proposal.clone(),
                            role,
                        };
                        if let Err(e) = tx.send(wrapped) {
                            error!("Failed to send proposal through channel: {}", e);
                        } else {
                            info!(
                                height = proposal.height,
                                round_id = proposal.round_id,
                                role = ?role,
                                "📥 [MN] Step 3: Proposal forwarded to validation pipeline (role={:?})",
                                role,
                            );
                        }
                    } else {
                        warn!("Proposal channel not configured, proposal dropped");
                    }

                    // Backup MN: also save to pending_proposals so the timeout
                    // handler can check later whether a cert was received.
                    if role == super::proposal_validator::MnGroupRole::Backup {
                        let key = (
                            proposal.proposer_group_id.clone(),
                            proposal.height,
                            proposal.round_id,
                        );
                        let mut pending = self.pending_proposals.write().await;
                        pending.insert(
                            key.clone(),
                            PendingProposalEntry {
                                proposal: proposal.clone(),
                                received_at: current_timestamp(),
                            },
                        );
                        info!(
                            height = proposal.height,
                            round_id = proposal.round_id,
                            group_id = %proposal.proposer_group_id,
                            timeout_ms = super::proposal_validator::BACKUP_CERT_TIMEOUT_MS,
                            "⏱️ [MN-BACKUP] Saved proposal to pending — will publish cert if leader times out"
                        );
                    }
                } else {
                    // Legacy proposal without group_id — send directly to pipeline as Leader
                    if let Some(ref tx) = self.proposal_tx {
                        let wrapped = super::proposal_validator::ProposalWithRole {
                            proposal: proposal.clone(),
                            role: super::proposal_validator::MnGroupRole::Leader,
                        };
                        if let Err(e) = tx.send(wrapped) {
                            error!("Failed to send proposal through channel: {}", e);
                        } else {
                            info!(
                                height = proposal.height,
                                round_id = proposal.round_id,
                                "📥 [MN<-LN] Step 3: Legacy proposal forwarded to pipeline (no group_id)"
                            );
                        }
                    } else {
                        warn!("Proposal channel not configured, proposal dropped");
                    }
                }
            }
            Err(e) => {
                // Fallback: attempt to decode lightnode's full BlockProposal wire format
                match serde_json::from_slice::<LightnodeBlockProposalCompat>(data) {
                    Ok(compat) => {
                        let tx_count = compat.transactions.len() as u32;
                        let block_hash = compute_compat_block_hash(&compat);

                        let proposal = super::proposal_validator::LightnodeProposal {
                            round_id: compat.round_id,
                            height: compat.height,
                            timestamp: compat.timestamp,
                            proposer_pubkey: compat.proposer_pubkey,
                            block_hash,
                            tx_count,
                            signature: compat.signature,
                            parent_hash: compat.parent_hash.into(),
                            state_root: compat.state_root.into(),
                            tx_root: compat.tx_root.into(),
                            proposer_group_id: String::new(), // Legacy compat - no group_id
                            election_certificate: None,       // Legacy compat - no certificate
                            raw_txs: None,
                        };

                        info!(
                            height = proposal.height,
                            round_id = proposal.round_id,
                            tx_count = proposal.tx_count,
                            proposer = %hex::encode(&proposal.proposer_pubkey[..8]),
                            source = ?source,
                            "📥 [MN<-LN] Step 2: Decoded block proposal from lightnode (compat)"
                        );

                        if let Some(ref tx) = self.proposal_tx {
                            let wrapped = super::proposal_validator::ProposalWithRole {
                                proposal: proposal.clone(),
                                role: super::proposal_validator::MnGroupRole::Leader, // legacy compat = treat as leader
                            };
                            if let Err(err) = tx.send(wrapped) {
                                error!("Failed to send compat proposal through channel: {}", err);
                            } else {
                                info!(
                                    height = proposal.height,
                                    round_id = proposal.round_id,
                                    "📥 [MN<-LN] Step 3: Compat proposal forwarded to validation pipeline"
                                );
                            }
                        } else {
                            warn!("Proposal channel not configured, compat proposal dropped");
                        }
                    }
                    Err(_) => {
                        warn!("Failed to decode lightnode proposal: {}", e);
                        debug!(
                            "Raw data preview: {:?}",
                            &data.get(..std::cmp::min(data.len(), 100))
                        );
                    }
                }
            }
        }

        Ok(())
    }

    // ═══════════════════════════════════════════════════════════════════
    // Handshake 3-fasi: Proposer Election Certificate handling
    // ═══════════════════════════════════════════════════════════════════

    /// Fase 1: Ricevi certificato di elezione dal LN proposer.
    /// Verify the certificate, add to whitelist + explicit_peer, send ACK.
    async fn handle_election_certificate(
        &mut self,
        data: &[u8],
        source: Option<PeerId>,
    ) -> Result<()> {
        let cert: ProposerElectionCertMessage = match serde_json::from_slice(data) {
            Ok(c) => c,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize election certificate");
                return Ok(());
            }
        };

        info!(
            group_id = %cert.group_id,
            round_id = cert.round_id,
            proposer = %cert.proposer_peer_id,
            pou_score = cert.proposer_pou_score,
            attestations = cert.attestations.len(),
            source = ?source,
            "📜 [MN] Received proposer election certificate"
        );
        info!(
            group_id = %cert.group_id,
            round_id = cert.round_id,
            proposer = %cert.proposer_peer_id,
            attestations = cert.attestations.len(),
            attesters = ?cert.attestations
                .iter()
                .map(|a| a.signer_peer_id.clone())
                .collect::<Vec<_>>(),
            "GROUP_CHECK_DEBUG: election cert received"
        );

        // INDEX, not by epoch+index prefix. The prior `cert_group_prefix`
        // built `"group_{epoch}_{index}_"` and required the same epoch on
        // both sides — when the MN re-formed groups at epoch Y while the
        // proposer kept tagging votes with epoch X, the prefix never
        // matched and the cert handler fell into the "processed anyway"
        // branch with `group_members = []`. That made the attestation
        // check trivially pass (`required_majority = 1.max(1)`) and
        // silently whitelisted the wrong proposer (4302/10min observed).
        //
        // Index-based matching uses the same primitive as
        // bucketing now agree on the stable group identity.
        let cert_index =
            savitri_consensus::primitives::group_id::group_index_from_id(&cert.group_id);
        let (group_valid, is_owner) = if let Some(ref gm) = self.group_formation_manager {
            let manager = gm.read().await;
            let groups = manager.get_active_groups().await;
            let local_peer_id_str = self.swarm.local_peer_id().to_string();
            let group_match = groups
                .iter()
                .find(|g| g.group_id == cert.group_id)
                .or_else(|| {
                    cert_index.and_then(|idx| {
                        groups.iter().find(|g| {
                            savitri_consensus::primitives::group_id::group_index_from_id(
                                &g.group_id,
                            ) == Some(idx)
                        })
                    })
                });
            match group_match {
                Some(group) => {
                    let owner = group
                        .group_leader_masternode
                        .as_ref()
                        .map(|l| l == &local_peer_id_str)
                        .unwrap_or(false);
                    (true, owner)
                }
                None => (false, false),
            }
        } else {
            (false, false)
        };

        if !group_valid {
            // Multiple MNs may create groups with different timestamps (pre-R11),
            // or a group may have been dissolved but the LN still references it.
            // We log a warning but continue processing — the MN will verify the
            // cert attestations and, if valid, accept the proposer for this group.
            // This prevents the scenario where valid blocks are produced but the
            // MN rejects the election cert, causing "group not found" warnings.
            warn!(
                group_id = %cert.group_id,
                cert_index = ?cert_index,
                "Election certificate: group not found in local active_groups, processing anyway"
            );
            // Fall through — we'll still verify attestations below
        } else if !is_owner {
            debug!(
                group_id = %cert.group_id,
                "Election certificate ignored: this MN is not the group owner"
            );
            return Ok(());
        }

        // Check le attestazioni (minimo 2/3 dei membri of the gruppo devono aver firmato)
        // group_valid check above. Without this, a cross-epoch cert would
        // get `group_members = []` and the 2/3 threshold would collapse to
        // `required_majority = 1` — effectively no verification.
        let group_members: Vec<String> = if let Some(ref gm) = self.group_formation_manager {
            let manager = gm.read().await;
            let groups = manager.get_active_groups().await;
            groups
                .into_iter()
                .find(|g| {
                    g.group_id == cert.group_id
                        || cert_index
                            .and_then(|idx| {
                                savitri_consensus::primitives::group_id::group_index_from_id(
                                    &g.group_id,
                                )
                                .map(|gi| gi == idx)
                            })
                            .unwrap_or(false)
                })
                .map(|g| g.members)
                .unwrap_or_default()
        } else {
            vec![]
        };

        info!(
            group_id = %cert.group_id,
            round_id = cert.round_id,
            proposer = %cert.proposer_peer_id,
            group_size = group_members.len(),
            group_members = ?group_members,
            attesters = ?cert.attestations
                .iter()
                .map(|a| a.signer_peer_id.clone())
                .collect::<Vec<_>>(),
            "GROUP_CHECK_DEBUG: resolved group members and attesters"
        );

        let required_two_thirds = (group_members.len() * 2 + 2) / 3;
        let required_majority = (group_members.len() / 2).max(1);
        let has_enough = cert.attestations.len() >= required_two_thirds
            || (cert.attestations.len() >= required_majority && !cert.attestations.is_empty());
        if !has_enough {
            warn!(
                group_id = %cert.group_id,
                attestations = cert.attestations.len(),
                required_2_3 = required_two_thirds,
                required_majority = required_majority,
                group_size = group_members.len(),
                "Election certificate rejected: insufficient attestations (need 2/3 or majority)"
            );
            return Ok(());
        }

        // Parse proposer PeerId
        let proposer_peer_id = match PeerId::from_str(&cert.proposer_peer_id) {
            Ok(pid) => pid,
            Err(e) => {
                warn!(error = %e, "Invalid proposer PeerId in election certificate");
                return Ok(());
            }
        };

        // Aggiungi alla whitelist temporanea
        let whitelist_timeout = Duration::from_secs(86400); // 24h — effectively disabled, proposer tenure handles rotation
        self.proposer_whitelist.insert(
            proposer_peer_id,
            ProposerWhitelistEntry {
                peer_id: proposer_peer_id,
                group_id: cert.group_id.clone(),
                round: cert.round_id,
                proposer_pubkey: cert.proposer_pubkey,
                added_at: std::time::Instant::now(),
                timeout: whitelist_timeout,
            },
        );

        // No add_explicit_peer for proposer LN: gossipsub treats explicit peers
        // specially — it rejects their GRAFT requests and prevents normal mesh formation.
        // The proposer is already in peers_on_topic via subscription exchange, so
        // flood_publish(true) will deliver messages to it without explicit marking.

        info!(
            proposer = %proposer_peer_id,
            group_id = %cert.group_id,
            round_id = cert.round_id,
            timeout_secs = whitelist_timeout.as_secs(),
            "✅ [MN-WHITELIST] Proposer added to whitelist (normal mesh peer, not explicit)"
        );

        // Fase 2: Invia ACK di autorizzazione al proposer
        let ack = ProposerWhitelistAck {
            target_proposer_peer_id: cert.proposer_peer_id.clone(),
            masternode_peer_id: self.swarm.local_peer_id().to_string(),
            group_id: cert.group_id.clone(),
            round_id: cert.round_id,
            timestamp: current_timestamp(),
            validity_secs: whitelist_timeout.as_secs(),
        };

        match serde_json::to_vec(&ack) {
            Ok(payload) => {
                match self
                    .swarm
                    .behaviour_mut()
                    .publish(self.election_ack_topic.clone(), payload)
                {
                    Ok(_) => {
                        info!(
                            proposer = %cert.proposer_peer_id,
                            group_id = %cert.group_id,
                            round_id = cert.round_id,
                            "📤 [MN→LN] Whitelist ACK sent to proposer"
                        );
                    }
                    Err(e) => {
                        warn!(error = ?e, "Failed to publish whitelist ACK");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to serialize whitelist ACK");
            }
        }

        Ok(())
    }

    fn cleanup_expired_whitelist(&mut self) {
        let now = std::time::Instant::now();
        let expired: Vec<PeerId> = self
            .proposer_whitelist
            .iter()
            .filter(|(_, entry)| now.duration_since(entry.added_at) > entry.timeout)
            .map(|(peer_id, _)| *peer_id)
            .collect();

        for peer_id in expired {
            if let Some(entry) = self.proposer_whitelist.remove(&peer_id) {
                if !self.known_masternode_peer_ids.contains(&peer_id) {
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .remove_explicit_peer(&peer_id);
                }
                info!(
                    proposer = %peer_id,
                    group_id = %entry.group_id,
                    round = entry.round,
                    "🔄 [MN-WHITELIST] Proposer removed from whitelist (timeout expired)"
                );
            }
        }
    }

    /// Handle masternode vote message (synchronous - no await needed)
    fn handle_masternode_vote(&mut self, data: &[u8], source: Option<PeerId>) -> Result<()> {
        // Filter out mesh keepalive messages that share the same topic
        // Keepalive messages are JSON objects with a "type":"mesh_keepalive" field
        if let Ok(json_val) = serde_json::from_slice::<serde_json::Value>(data) {
            if json_val.get("type").and_then(|v| v.as_str()) == Some("mesh_keepalive") {
                debug!(
                    source = ?source,
                    "📥 [MN] Ignoring mesh keepalive message on vote topic"
                );
                return Ok(());
            }
        }

        info!(
            source = ?source,
            "📥 [MN<-MN] Received block vote from masternode peer"
        );

        // Try to decode the vote
        match serde_json::from_slice::<super::proposal_validator::MasternodeVote>(data) {
            Ok(vote) => {
                info!(
                    height = vote.height,
                    round_id = vote.round_id,
                    vote_type = ?vote.vote_type,
                    voter = %hex::encode(&vote.voter_pubkey[..8]),
                    source = ?source,
                    "📥 [MN<-MN] Decoded block vote - forwarding to aggregation"
                );

                // Send vote through the channel if configured
                if let Some(ref tx) = self.vote_tx {
                    if let Err(e) = tx.send(vote.clone()) {
                        error!("Failed to send vote through channel: {}", e);
                    }
                } else {
                    warn!("Vote channel not configured, vote dropped");
                }
            }
            Err(e) => {
                warn!("Failed to decode masternode vote: {}", e);
                debug!(
                    "Raw data preview: {:?}",
                    &data.get(..std::cmp::min(data.len(), 100))
                );
            }
        }

        Ok(())
    }

    /// Poll the swarm for events and handle them
    /// Takes ownership of self to avoid Sync requirements in tokio::spawn
    pub async fn poll(mut self) -> Result<()> {
        let mut registry_announce_timer =
            interval(Duration::from_secs(REGISTRY_ANNOUNCE_INTERVAL_SECS));
        let mut registry_prune_timer = interval(Duration::from_secs(REGISTRY_PRUNE_INTERVAL_SECS));
        // Mesh warmup: send keepalive messages more frequently during bootstrap (10s), then 30s
        // ENHANCED: More aggressive warmup during bootstrap to prevent mesh degradation
        let mesh_warmup_interval = if self.bootstrap_phase {
            Duration::from_secs(10) // 10s during bootstrap for faster mesh formation
        } else {
            Duration::from_secs(30) // 30s after bootstrap
        };
        let mut mesh_warmup_timer = interval(mesh_warmup_interval);

        // Mesh status logging: log mesh status every 60 seconds (or when state changes)
        let mut mesh_status_timer = interval(Duration::from_secs(60));

        // Masternode reconnection: check more frequently during bootstrap (5s), then 15s
        // ENHANCED: More aggressive reconnection during bootstrap
        let reconnect_interval = if self.bootstrap_phase {
            Duration::from_secs(5) // 5s during bootstrap
        } else {
            Duration::from_secs(15) // 15s after bootstrap
        };
        let mut reconnect_timer = interval(reconnect_interval);

        // Batch collector flush timer: use interval() instead of sleep() to avoid
        // starvation in select! — sleep creates a new future each iteration and
        // gets dropped whenever another branch fires, so the flush never triggers
        // in a busy network with gossipsub traffic.
        let mut batch_flush_timer = interval(Duration::from_secs(3));

        loop {
            tokio::select! {
                // Handle outgoing masternode messages to publish via gossipsub
                msg = self.masternode_publish_rx.recv() => {
                    if let Some(msg) = msg {
                        if let MasternodeMessage::LightnodeGroupAnnounce(ref announce) = msg {
                            info!(
                                group_id = %announce.group_id,
                                epoch = announce.epoch,
                                members_count = announce.members.len(),
                                "🔔 RACCOMANDAZIONE #3: Received LightnodeGroupAnnounce from masternode_publish_rx, publishing via gossipsub"
                            );
                        }
                        if let Err(e) = self.publish_masternode_message(msg) {
                            error!("❌ Failed to publish masternode message: {}", e);
                        }
                    } else {
                        warn!("Masternode publish channel closed");
                    }
                }
                // Handle swarm events
                event = self.swarm.select_next_some() => {
                    match event {
                        SwarmEvent::NewListenAddr { address, .. } => {
                            if is_routable_multiaddr(&address) {
                                info!("Listening on public address {}", address);
                            } else {
                                debug!("Listening on local/non-routable address {}", address);
                            }
                        }
                        SwarmEvent::ConnectionEstablished { peer_id, endpoint, .. } => {
                            let is_masternode = self.known_masternode_peer_ids.contains(&peer_id);
                            counter!("p2p_connection_attempts_total").increment(1);
                            info!(
                                peer_id = %peer_id,
                                is_masternode = is_masternode,
                                endpoint = ?endpoint,
                                "Connected to peer"
                            );
                            self.connected_peers.insert(peer_id);
                            gauge!("p2p_peers_connected").set(self.connected_peers.len() as f64);

                            // EXPLICIT PEER: If this is a masternode, add as explicit peer
                            // This ensures masternodes are prioritized in mesh formation
                            // Note: libp2p gossipsub manages mesh automatically - we can only mark peers as explicit
                            // The mesh will form naturally through heartbeats and message exchange
                            if is_masternode {
                                // Add as explicit peer - gossipsub will prioritize this peer for mesh inclusion
                                // This doesn't return a Result, it's a void operation
                                self.swarm.behaviour_mut().gossipsub.add_explicit_peer(&peer_id);
                                info!(
                                    peer_id = %peer_id,
                                    bootstrap_phase = self.bootstrap_phase,
                                    "✅ [MESH] Added masternode as explicit peer (will be prioritized for mesh formation)"
                                );

                                // Note: libp2p gossipsub doesn't have a direct `graft()` method
                                // The mesh is managed automatically through:
                                // 1. Heartbeat messages (every 7s)
                                // 2. Message exchange
                                // 3. Explicit peer marking (which we just did)
                                // Gossipsub will automatically graft explicit peers to subscribed topics
                            }

                            // UNIFIED PEERS: Write directly into shared peer map
                            // WARNING: endpoint.get_remote_address() returns the TCP connection's
                            // remote address, which for *inbound* connections contains an OS-assigned
                            // ephemeral source port (e.g. /ip4/10.0.0.5/tcp/50344). This address
                            // must NOT be used for peer advertisement, group announcements, or any
                            // context where other nodes would try to dial this address -- the
                            // ephemeral port is not a listening port and will yield "connection
                            // refused". Only use this address for connection-local bookkeeping
                            // (e.g. identifying which peer is on which IP). For dialable addresses,
                            // use the lightnode's self-reported listen address from registration.
                            {
                                let remote_addr = endpoint.get_remote_address().clone();
                                let peer_info = crate::monolith_p2p::PeerInfo {
                                    peer_id,
                                    multiaddr: remote_addr.clone(),
                                    is_light_node: !is_masternode,
                                    last_seen: std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_secs(),
                                    reputation: 1.0,
                                };
                                let mut peers = self.shared_peers.write().await;
                                peers.insert(peer_id, peer_info);
                                let total = peers.len();
                                let ln = peers.values().filter(|p| p.is_light_node).count();
                                let mn = total - ln;
                                drop(peers);
                                info!(
                                    total_peers = total,
                                    masternodes = mn,
                                    light_nodes = ln,
                                    "📊 UNIFIED PEERS: Peer registered (is_masternode={})",
                                    is_masternode
                                );

                                // Bootstrap reply: include this peer so lightnodes receive it in
                                // bootstrap reply and can dial peers on other VMs.
                                // For masternodes the remote_addr is their listen address (they
                                // initiate or accept on their configured port), so it is safe to
                                // advertise. For lightnodes the remote_addr contains an OS-assigned
                                // ephemeral source port and MUST NOT be advertised -- other nodes
                                // that try to dial it will get "connection refused" (os error 10061).
                                // Lightnodes will be added with their self-reported listen address
                                // once they complete registration via gossipsub.
                                if is_masternode {
                                    self.bootstrap_handler.add_known_peer(
                                        peer_id,
                                        vec![remote_addr.to_string()],
                                        false, // is_light_node = false
                                    );
                                }
                                // Lightnodes are intentionally NOT added here; they will be
                                // registered with their correct listen address during
                                // handle_lightnode_registration.

                                // BOOTSTRAP PHASE DETECTION: Exit bootstrap when we have enough masternodes
                                // Use 80% threshold to exit bootstrap early (more resilient)
                                let bootstrap_exit_threshold = (self.expected_masternodes as f64 * 0.8) as usize;
                                if self.bootstrap_phase && mn >= bootstrap_exit_threshold {
                                    self.bootstrap_phase = false;
                                    info!(
                                        connected_masternodes = mn,
                                        expected_masternodes = self.expected_masternodes,
                                        threshold = bootstrap_exit_threshold,
                                        "🎉 [BOOTSTRAP] Bootstrap phase complete - {} masternodes connected (threshold: {})",
                                        mn, bootstrap_exit_threshold
                                    );
                                } else if self.bootstrap_phase {
                                    debug!(
                                        connected_masternodes = mn,
                                        expected_masternodes = self.expected_masternodes,
                                        threshold = bootstrap_exit_threshold,
                                        "⏳ [BOOTSTRAP] Still in bootstrap phase - {}/{} masternodes (need {})",
                                        mn, self.expected_masternodes, bootstrap_exit_threshold
                                    );
                                }
                            }

                            let peerinfo_key = RecordKey::new(&format!("peerinfo:{}", peer_id));
                            self.swarm.behaviour_mut().kademlia.get_record(peerinfo_key);

                            let registration_key = RecordKey::new(&format!("lightnode_registration:{}", peer_id));
                            self.swarm.behaviour_mut().kademlia.get_record(registration_key);
                        }
                        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
                            let was_masternode = self.known_masternode_peer_ids.contains(&peer_id);
                            let connected_mn_before = self.connected_peers.iter()
                                .filter(|p| self.known_masternode_peer_ids.contains(p))
                                .count();

                            counter!("p2p_peers_disconnected_total").increment(1);
                            info!(
                                peer_id = %peer_id,
                                cause = ?cause,
                                is_masternode = was_masternode,
                                "Connection closed with peer"
                            );
                            self.connected_peers.remove(&peer_id);
                            gauge!("p2p_peers_connected").set(self.connected_peers.len() as f64);

                            // ENHANCED: Alert if masternode disconnected
                            if was_masternode {
                                let connected_mn_after = self.connected_peers.iter()
                                    .filter(|p| self.known_masternode_peer_ids.contains(p))
                                    .count();

                                warn!(
                                    peer_id = %peer_id,
                                    connected_mn_before = connected_mn_before,
                                    connected_mn_after = connected_mn_after,
                                    expected_mn = self.expected_masternodes,
                                    bootstrap_phase = self.bootstrap_phase,
                                    "🔴 [ALERT] Masternode disconnected! Mesh may be degraded. Connected: {}/{}",
                                    connected_mn_after,
                                    self.expected_masternodes
                                );

                                // If we're below threshold, trigger immediate reconnection attempt
                                if connected_mn_after < (self.expected_masternodes as f64 * 0.8) as usize {
                                    warn!(
                                        "🔴 [ALERT] Critical: Only {} masternodes connected (need {}). Triggering immediate reconnect.",
                                        connected_mn_after,
                                        self.expected_masternodes
                                    );
                                    // Trigger reconnection immediately
                                    self.reconnect_disconnected_masternodes();
                                }
                            }

                            // UNIFIED PEERS: Remove from shared peer map
                            {
                                let mut peers = self.shared_peers.write().await;
                                peers.remove(&peer_id);
                                let total = peers.len();
                                drop(peers);
                                info!(
                                    peer_id = %peer_id,
                                    remaining_peers = total,
                                    "📊 UNIFIED PEERS: Peer disconnected and removed"
                                );
                            }
                        }
                        SwarmEvent::OutgoingConnectionError { peer_id, error, .. } => {
                            counter!("p2p_connection_attempts_total").increment(1);
                            warn!(
                                peer_id = ?peer_id,
                                error = %error,
                                "Outgoing connection error (upgrade/negotiation/transport may have failed)"
                            );
                        }
                        SwarmEvent::Behaviour(event) => {
                            match event {
                                MyBehaviourEvent::Gossipsub(event) => {
                                    match event {
                                        GossipsubEvent::Message { message, .. } => {
                                            counter!("p2p_messages_received_total").increment(1);
                                            counter!("p2p_bytes_received_total").increment(message.data.len() as u64);
                                            counter!("gossip_messages_received_total").increment(1);
                                            let topic = message.topic.to_string();
                                            info!("Received gossipsub message on topic: {} (source: {:?}, {} bytes)",
                                                  topic, message.source, message.data.len());

                                            // Refresh last_seen for the source peer so cleanup_inactive()
                                            // doesn't remove alive peers (heartbeats go via AuxProtocol
                                            // which MN doesn't handle; this catches bootstrap/registration/etc.)
                                            if let Some(source_peer) = message.source {
                                                if let Some(ref gm) = self.group_formation_manager {
                                                    let gm_read = gm.read().await;
                                                    gm_read.update_last_seen_by_peer_id(&source_peer.to_string()).await;
                                                }
                                            }

                                            // ENHANCED: Handle both old and new bootstrap topics
                                            if message.topic == self.bootstrap_req_topic.hash()
                                                || message.topic == self.bootstrap_req_aligned_topic.hash()
                                            {
                                                let peer_id = match message.source {
                                                    Some(id) => id,
                                                    None => {
                                                        warn!("Bootstrap message without source, skipping");
                                                        continue;
                                                    }
                                                };
                                                let peer_multiaddr = self.shared_peers.read().await
                                                    .get(&peer_id)
                                                    .map(|p| p.multiaddr.to_string());
                                                if let Err(e) = self.bootstrap_handler
                                                    .process_message(
                                                        &mut self.swarm.behaviour_mut().gossipsub,
                                                        &message.topic,
                                                        &message.data,
                                                        Some(peer_id),
                                                        peer_multiaddr,
                                                    )
                                                    .await
                                                {
                                                    error!("Error handling bootstrap request: {}", e);
                                                }
                                            }

                                            // NEW: Handle missing critical topics
                                            // CRITICAL FIX: All handlers use if-let-Err instead of ? to prevent
                                            // PublishError::Duplicate (or any transient error) from crashing the
                                            // entire network task. A single Duplicate error was previously killing
                                            // the poll() loop, dropping the swarm, and closing ALL TCP connections.
                                            if message.topic == self.heartbeat_topic.hash() {
                                                if let Err(e) = self.handle_heartbeat_message(&message.data, message.source).await {
                                                    warn!(error = %e, topic = %topic, "handle_heartbeat_message failed (non-fatal)");
                                                }
                                            } else if message.topic == self.pou_topic.hash() {
                                                if let Err(e) = self.handle_pou_message(&message.data, message.source).await {
                                                    warn!(error = %e, topic = %topic, "handle_pou_message failed (non-fatal)");
                                                }
                                            } else if message.topic == self.peer_info_topic.hash() {
                                                if let Err(e) = self.handle_peer_info_message(&message.data, message.source).await {
                                                    warn!(error = %e, topic = %topic, "handle_peer_info_message failed (non-fatal)");
                                                }
                                            } else if message.topic == self.block_topic.hash() {
                                                // PERF: Spawn block proposal handling in parallel.
                                                // concurrently instead of blocking the select! loop.
                                                self.cache_block_from_gossip(&message.data);
                                                let data = message.data.clone();
                                                let source = message.source;
                                                let tx_validator = Arc::clone(&self.transaction_validator);
                                                tokio::spawn(async move {
                                                    if let Err(e) = Self::validate_block_proposal_parallel(
                                                        data, source, tx_validator
                                                    ).await {
                                                        tracing::error!(error = %e, "parallel block proposal validation failed");
                                                    }
                                                });
                                            } else if message.topic == self.consensus_cert_topic.hash() {
                                                if let Err(e) = self.handle_consensus_certificate(&message.data, message.source).await {
                                                    error!(error = %e, topic = %topic, "handle_consensus_certificate failed (non-fatal)");
                                                }
                                            } else if message.topic == self.peer_discovery_topic.hash() {
                                                if let Err(e) = self.handle_peer_discovery_message(&message.data, message.source).await {
                                                    warn!(error = %e, topic = %topic, "handle_peer_discovery_message failed (non-fatal)");
                                                }
                                            } else if message.topic == self.peer_registry_topic.hash() {
                                                if let Err(e) = self.handle_peer_registry_message(&message.data).await {
                                                    warn!(error = %e, topic = %topic, "handle_peer_registry_message failed (non-fatal)");
                                                }
                                            } else if message.topic == self.registration_topic.hash() {
                                                // 🔧 FIX B: Logging diagnostico per messaggi ricevuti
                                                info!(
                                                    topic = %message.topic,
                                                    data_len = message.data.len(),
                                                    source = ?message.source,
                                                    "📥 REGISTRATION MESSAGE RECEIVED - Attempting to deserialize..."
                                                );
                                                if let Err(e) = self.handle_lightnode_registration(&message.data, message.source).await {
                                                    error!(error = %e, topic = %topic, "handle_lightnode_registration failed (non-fatal)");
                                                }
                                            } else if message.topic == self.group_formed_topic.hash() {
                                                if let Err(e) = self.handle_group_formed_ack(&message.data, message.source).await {
                                                    warn!(error = %e, topic = %topic, "handle_group_formed_ack failed (non-fatal)");
                                                }
                                            } else if message.topic == self.lightnode_proposal_topic.hash() {
                                                if let Err(e) = self.handle_lightnode_proposal(&message.data, message.source).await {
                                                    warn!(error = %e, "handle_lightnode_proposal failed");
                                                }
                                            } else if message.topic == self.masternode_vote_topic.hash() {
                                                if let Err(e) = self.handle_masternode_vote(&message.data, message.source) {
                                                    warn!(error = %e, topic = %topic, "handle_masternode_vote failed (non-fatal)");
                                                }
                                            } else if message.topic == self.block_acceptance_topic.hash() {
                                                if let Err(e) = self.handle_block_acceptance_certificate(&message.data, message.source).await {
                                                    warn!(error = %e, "handle_block_acceptance_certificate failed");
                                                }
                                            } else if message.topic == self.election_cert_topic.hash() {
                                                if let Err(e) = self.handle_election_certificate(&message.data, message.source).await {
                                                    warn!(error = %e, "handle_election_certificate failed");
                                                }
                                            }

                                            // Handle masternode P2P topics (single shared gossipsub)
                                            let masternode_topics = [
                                                "/savitri/masternode/group/proposal/1",
                                                "/savitri/masternode/group/vote/1",
                                                "/savitri/masternode/group/sync/1",
                                                "/savitri/masternode/leader/election/1",
                                                "/savitri/masternode/lightnode_list/sync/1",
                                            ];

                                            for topic_str in &masternode_topics {
                                                let topic_hash = libp2p::gossipsub::IdentTopic::new(*topic_str).hash();
                                                if message.topic == topic_hash {
                                                    // Use savitri-p2p module for processing
                                                    debug!("Processing masternode P2P message on topic: {}", topic_str);

                                                    // Route message to appropriate handler based on topic
                                                    // All handlers use if-let-Err to prevent fatal propagation
                                                    match *topic_str {
                                                        "/savitri/masternode/group/proposal/1" => {
                                                            if let Some(source) = message.source {
                                                                if let Err(e) = self.handle_group_proposal(&message.data, source).await {
                                                                    warn!(error = %e, "handle_group_proposal failed (non-fatal)");
                                                                }
                                                            }
                                                        }
                                                        "/savitri/masternode/group/vote/1" => {
                                                            if let Some(source) = message.source {
                                                                if let Err(e) = self.handle_group_vote(&message.data, source).await {
                                                                    warn!(error = %e, "handle_group_vote failed (non-fatal)");
                                                                }
                                                            }
                                                        }
                                                        "/savitri/masternode/group/sync/1" => {
                                                            if let Some(source) = message.source {
                                                                if let Err(e) = self.handle_group_sync(&message.data, source).await {
                                                                    warn!(error = %e, "handle_group_sync failed (non-fatal)");
                                                                }
                                                            }
                                                        }
                                                        "/savitri/masternode/leader/election/1" => {
                                                            debug!("Received leader election message on {}", topic_str);
                                                        }
                                                        "/savitri/masternode/lightnode_list/sync/1" => {
                                                            debug!("Received lightnode list sync on {}", topic_str);
                                                        }
                                                        _ => {}
                                                    }

                                                    // Forward to main loop for consensus processing
                                                    // CRITICAL FIX: Do NOT forward group/proposal and group/vote topics
                                                    // to the main loop — they are already fully handled above by
                                                    // handle_group_proposal and handle_group_vote which call into
                                                    // the shared group_consensus. Forwarding them would cause dual
                                                    // processing (double voting, double distribution, potential deadlock).
                                                    let dominated_topics = [
                                                        "/savitri/masternode/group/proposal/1",
                                                        "/savitri/masternode/group/vote/1",
                                                    ];
                                                    if !dominated_topics.contains(topic_str) {
                                                        if let Ok(masternode_msg) = serde_json::from_slice::<MasternodeMessage>(&message.data) {
                                                            if let Some(source) = message.source {
                                                                if let Err(e) = self.masternode_message_sender.send((source, masternode_msg)) {
                                                                    warn!("Failed to forward masternode message to main loop: {}", e);
                                                                }
                                                            }
                                                        }
                                                    }

                                                    break;
                                                }
                                            }
                                        }
                                        GossipsubEvent::Subscribed { peer_id, topic } => {
                                            info!("Peer {} subscribed to {}", peer_id, topic);
                                        }
                                        GossipsubEvent::Unsubscribed { peer_id, topic } => {
                                            debug!("Peer {} unsubscribed from {}", peer_id, topic);
                                        }
                                        GossipsubEvent::SlowPeer { peer_id, .. } => {
                                            // consensus LNs under high traffic. Group cleanup handles
                                            // truly inactive peers via node_timeout_secs.
                                            debug!(
                                                %peer_id,
                                                "Gossipsub SlowPeer detected on masternode (not disconnecting — managed by group cleanup)"
                                            );
                                        }
                                        _ => {}
                                    }
                                }
                                // ── Consensus direct P2P (request-response) ──
                                MyBehaviourEvent::Consensus(event) => {
                                    match event {
                                        libp2p::request_response::Event::Message { peer, message, .. } => {
                                            match message {
                                                libp2p::request_response::Message::Request { request, channel, .. } => {
                                                    debug!(peer = %peer, "Received consensus direct message from masternode");
                                                    match request {
                                                        super::consensus_protocol::ConsensusMessage::Vote(data) => {
                                                            if let Ok(vote) = serde_json::from_slice::<super::proposal_validator::MasternodeVote>(&data) {
                                                                info!(height = vote.height, round_id = vote.round_id, source = %peer, "Received MN vote via direct P2P");
                                                                if let Some(ref tx) = self.vote_tx {
                                                                    if let Err(e) = tx.send(vote) {
                                                                        error!("Failed to send vote through channel: {}", e);
                                                                    }
                                                                }
                                                            }
                                                        }
                                                        super::consensus_protocol::ConsensusMessage::BlockAcceptance(data) => {
                                                            if let Err(e) = self.handle_block_acceptance_certificate(&data, Some(peer)).await {
                                                                warn!(error = ?e, peer = %peer, "Failed to handle BlockAcceptanceCertificate from direct P2P");
                                                            }
                                                        }
                                                        super::consensus_protocol::ConsensusMessage::MasternodeMessage(data) => {
                                                            if let Ok(mn_msg) = serde_json::from_slice::<MasternodeMessage>(&data) {
                                                                let _ = self.masternode_message_sender.send((peer, mn_msg));
                                                            }
                                                        }
                                                    }
                                                    let ack = super::consensus_protocol::ConsensusAck { ok: true };
                                                    let _ = self.swarm.behaviour_mut().consensus.send_response(channel, ack);
                                                }
                                                libp2p::request_response::Message::Response { response, .. } => {
                                                    if !response.ok {
                                                        warn!(peer = %peer, "Consensus peer responded with nack");
                                                    }
                                                }
                                            }
                                        }
                                        libp2p::request_response::Event::OutboundFailure { peer, error, .. } => {
                                            debug!(peer = %peer, error = ?error, "Consensus direct send to MN failed");
                                        }
                                        libp2p::request_response::Event::InboundFailure { peer, error, .. } => {
                                            debug!(peer = %peer, error = ?error, "Consensus direct receive from MN failed");
                                        }
                                        _ => {}
                                    }
                                }
                                MyBehaviourEvent::Kademlia(event) => {
                                    match event {
                                        KademliaEvent::OutboundQueryProgressed { result, .. } => {
                                            match result {
                                                QueryResult::GetRecord(Ok(get_record_ok)) => {
                                                    // GetRecordOk is an enum with tuple-like variants
                                                    match get_record_ok {
                                                        // FoundRecord is tuple-like, access with .0
                                                        libp2p::kad::GetRecordOk::FoundRecord(record) => {
                                                            let key = String::from_utf8_lossy(record.record.key.as_ref().as_ref()).to_string();
                                                            if let Some(peer_id_str) = key.strip_prefix("peerinfo:") {
                                                                if let Ok(peer_id) = PeerId::from_str(peer_id_str) {
                                                                    let batch = vec![(peer_id, record.record.value.clone())];
                                                                    if let Err(e) = self.handle_peer_info_batch(batch).await {
                                                                        warn!("Failed to process peerinfo record: {}", e);
                                                                    }
                                                                }
                                                            } else if let Some(peer_id_str) = key.strip_prefix("lightnode_registration:") {
                                                                if let Ok(peer_id) = PeerId::from_str(peer_id_str) {
                                                                    let batch = vec![(peer_id, record.record.value.clone())];
                                                                    if let Err(e) = self.handle_registration_batch(batch).await {
                                                                        warn!("Failed to process registration record: {}", e);
                                                                    }
                                                                }
                                                            } else if key.starts_with("peer_registry:") {
                                                                if let Err(e) = self.handle_peer_registry_message(&record.record.value).await {
                                                                    warn!("Failed to process peer registry record: {}", e);
                                                                }
                                                            } else {
                                                                debug!("Received record with unexpected key: {}", key);
                                                            }
                                                        }
                                                        // Try other possible variants - remove Finished since it doesn't exist
                                                        _ => {
                                                            debug!("Received GetRecordOk with unexpected variant");
                                                        }
                                                    }
                                                }
                                                QueryResult::GetRecord(Err(_)) => {
                                                    debug!("Kademlia get_record error");
                                                }
                                                QueryResult::Bootstrap(Ok(_)) => {
                                                    info!("✅ DHT Bootstrap completato!");
                                                }
                                                _ => {}
                                            }
                                        }
                                        KademliaEvent::RoutingUpdated { peer, .. } => {
                                            debug!(peer = %peer, "Kademlia routing updated");
                                        }
                                        _ => {}
                                    }
                                }
                                MyBehaviourEvent::Relay(event) => {
                                    #[allow(deprecated)]
                                    match &event {
                                        libp2p::relay::Event::ReservationReqAccepted { src_peer_id, renewed } => {
                                            info!(peer = %src_peer_id, renewed = renewed, "Relay: reservation accepted");
                                        }
                                        libp2p::relay::Event::ReservationReqDenied { src_peer_id } => {
                                            warn!(peer = %src_peer_id, "Relay: reservation denied (at capacity)");
                                        }
                                        libp2p::relay::Event::ReservationTimedOut { src_peer_id } => {
                                            debug!(peer = %src_peer_id, "Relay: reservation timed out");
                                        }
                                        libp2p::relay::Event::CircuitReqDenied { src_peer_id, dst_peer_id } => {
                                            warn!(src = %src_peer_id, dst = %dst_peer_id, "Relay: circuit denied");
                                        }
                                        _ => {
                                            debug!(?event, "Relay server event");
                                        }
                                    }
                                }
                                MyBehaviourEvent::Identify(event) => {
                                    if let libp2p::identify::Event::Received { peer_id, info, .. } = &event {
                                        debug!(
                                            %peer_id,
                                            agent = ?info.agent_version,
                                            protocols = ?info.protocols,
                                            "Identify received"
                                        );
                                        // so peer_id-only dials from other nodes can resolve via Kademlia.
                                        for addr in &info.listen_addrs {
                                            if !is_routable_multiaddr(addr) {
                                                debug!(
                                                    peer = %peer_id,
                                                    %addr,
                                                    "Ignoring non-routable identify listen address"
                                                );
                                                continue;
                                            }
                                            let s = addr.to_string();
                                            if s.contains("tcp/0") || s.contains("udp/0") {
                                                continue;
                                            }
                                            self.swarm.behaviour_mut().kademlia.add_address(peer_id, addr.clone());
                                        }
                                    }
                                }
                                MyBehaviourEvent::Autonat(event) => {
                                    if let libp2p::autonat::Event::StatusChanged { old, new } = &event {
                                        info!(?old, ?new, "AutoNAT: status changed (expected Public on masternode)");
                                    } else {
                                        debug!(?event, "AutoNAT server event");
                                    }
                                }
                                MyBehaviourEvent::Dcutr(event) => {
                                    match &event.result {
                                        Ok(conn_id) => {
                                            info!(
                                                peer = %event.remote_peer_id,
                                                ?conn_id,
                                                "DCUtR: hole-punch succeeded — direct connection established"
                                            );
                                        }
                                        Err(e) => {
                                            warn!(
                                                peer = %event.remote_peer_id,
                                                error = %e,
                                                "DCUtR: hole-punch failed"
                                            );
                                        }
                                    }
                                }
                                MyBehaviourEvent::Upnp(event) => {
                                    match event {
                                        libp2p::upnp::Event::NewExternalAddr(addr) => {
                                            info!(%addr, "UPnP: external address discovered");
                                        }
                                        libp2p::upnp::Event::ExpiredExternalAddr(addr) => {
                                            warn!(%addr, "UPnP: external address expired");
                                        }
                                        libp2p::upnp::Event::GatewayNotFound => {
                                            info!("UPnP: no IGD gateway found (router likely doesn't support UPnP)");
                                        }
                                        libp2p::upnp::Event::NonRoutableGateway => {
                                            warn!("UPnP: gateway found but address is non-routable");
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // Handle votes to broadcast
                Some(vote) = async {
                    match &mut self.vote_broadcast_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Err(e) = self.broadcast_vote(&vote) {
                        error!("Failed to broadcast vote: {}", e);
                    }
                }

                // Handle certificates to broadcast: prefer block+cert single message, fallback to cert only
                Some(certificate) = async {
                    match &mut self.certificate_broadcast_rx {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    // Publish on block_final_topic for lightnodes (block+cert together)
                    let _ = self.broadcast_block_with_certificate(&certificate);
                    // ALWAYS publish on consensus_cert_topic for other masternodes
                    if let Err(e) = self.broadcast_certificate(&certificate) {
                        error!("Failed to broadcast certificate: {}", e);
                    }
                }

                // Handle BlockAcceptanceCertificate to publish (owner MN → other MNs + LNs)
                Some(cert) = async {
                    self.block_acceptance_publish_rx.recv().await
                } => {
                    if let Err(e) = self.broadcast_block_acceptance_certificate(&cert) {
                        error!("Failed to broadcast BlockAcceptanceCertificate: {}", e);
                    }
                    // re-broadcast on block_final_topic. Previously the owner-signed
                    // BlockAcceptanceCertificate was wrapped in a single-vote BlockCertificate
                    // (votes: vec![synth_vote]) and published to LNs, producing voters=1
                    // certs that bypassed BFT quorum. Now LNs receive only the BFT-quorum
                    // multi-voter BlockCertificate via the certificate_broadcast_rx path
                    // above, which publishes the real vote_aggregator output.
                    {
                        // Fase 3 of the handshake: rimuovi proposer dalla whitelist DOPO
                        // che il BlockAcceptanceCertificate e' stato pubblicato con successo
                        let proposer_to_remove: Option<PeerId> = self.proposer_whitelist.iter()
                            .find(|(_, entry)| entry.group_id == cert.group_id && entry.round == cert.round_id)
                            .map(|(peer_id, _)| *peer_id);

                        if let Some(proposer_peer_id) = proposer_to_remove {
                            if let Some(entry) = self.proposer_whitelist.remove(&proposer_peer_id) {
                                if !self.known_masternode_peer_ids.contains(&proposer_peer_id) {
                                    self.swarm.behaviour_mut().gossipsub.remove_explicit_peer(&proposer_peer_id);
                                }
                                info!(
                                    proposer = %proposer_peer_id,
                                    group_id = %entry.group_id,
                                    round = entry.round,
                                    height = cert.height,
                                    "✅ [MN-WHITELIST] Proposer removed from whitelist (block validated + BlockAcceptanceCertificate published)"
                                );
                            }
                        }
                    }
                }

                // Periodic batch collector flush and retry processing
                _ = batch_flush_timer.tick() => {
                    // Process any ready retries first
                    let ready_retries = self.peer_info_batch_collector.get_ready_retries();
                    if !ready_retries.is_empty() {
                        info!(
                            retry_count = ready_retries.len(),
                            "🔄 RETRY: Processing {} ready retries",
                            ready_retries.len()
                        );

                        for (peer_id, registration) in ready_retries {
                            // Convert retry to batch format
                            let registration_data = match serde_json::to_vec(&registration) {
                                Ok(data) => data,
                                Err(e) => {
                                    error!("Failed to serialize retry registration: {}", e);
                                    self.peer_info_batch_collector.mark_retry_failed(&peer_id, format!("Serialization error: {}", e));
                                    continue;
                                }
                            };

                            // Process retry as single-item batch
                            if let Err(e) = self.handle_registration_batch(vec![(peer_id, registration_data)]).await {
                                error!("Failed to process retry batch: {}", e);
                                self.peer_info_batch_collector.mark_retry_failed(&peer_id, format!("Batch processing error: {}", e));
                            } else {
                                self.peer_info_batch_collector.mark_retry_success(&peer_id);
                            }
                        }
                    }

                    // Force flush any pending batch
                    if let Some(batch) = self.peer_info_batch_collector.force_flush() {
                        if let Err(e) = self.handle_registration_batch(batch).await {
                            error!("Failed to process periodic registration batch: {}", e);
                        }
                    }

                    // Perform maintenance tasks
                    self.peer_info_batch_collector.maintenance();
                }
                _ = registry_announce_timer.tick() => {
                    let gossipsub_enabled = if let Some(ref gm) = self.group_formation_manager {
                        let gm_read = gm.read().await;
                        let registered_count = gm_read.get_registered_nodes().await.len();
                        let min_required = gm_read.min_group_size();
                        if registered_count < min_required {
                            debug!(
                                registered = registered_count,
                                required = min_required,
                                "⏳ Registry gossipsub suspended: waiting for lightnodes ({}/{})",
                                registered_count, min_required
                            );
                            false
                        } else {
                            true
                        }
                    } else {
                        true // no group manager = always announce
                    };

                    if let Err(e) = self.broadcast_registry_announce(gossipsub_enabled).await {
                        warn!(error = %e, "broadcast_registry_announce failed (non-fatal)");
                    }
                }
                _ = registry_prune_timer.tick() => {
                    self.prune_registry();
                }

                // MESH WARMUP: Send keepalive messages to keep mesh active
                _ = mesh_warmup_timer.tick() => {
                    if let Err(e) = self.send_mesh_keepalive() {
                        debug!("Mesh keepalive failed (non-critical): {}", e);
                    }
                }

                // MESH STATUS LOGGING: Log mesh status periodically
                _ = mesh_status_timer.tick() => {
                    self.log_mesh_status();
                    // Cleanup proposer whitelist scadute
                    self.cleanup_expired_whitelist();
                }

                // MASTERNODE RECONNECTION: Re-dial disconnected masternode peers
                _ = reconnect_timer.tick() => {
                    self.reconnect_disconnected_masternodes();
                }
            }
        }
    }

    /// Send mesh keepalive message to prevent pruning
    /// During bootstrap phase, sends more frequently to ensure mesh formation
    /// ENHANCED: More aggressive keepalive and explicit peer marking
    fn send_mesh_keepalive(&mut self) -> Result<()> {
        let connected_mn_count = self
            .connected_peers
            .iter()
            .filter(|p| self.known_masternode_peer_ids.contains(p))
            .count();

        // ENHANCED: Always send keepalive if we're in bootstrap OR have fewer masternodes than expected
        // Also send if we're close to threshold to prevent degradation
        let should_send = self.bootstrap_phase
            || connected_mn_count < self.expected_masternodes
            || connected_mn_count < (self.expected_masternodes as f64 * 0.9) as usize; // 90% threshold

        if should_send {
            // ENHANCED: Explicitly mark all connected masternodes as explicit peers
            // This prevents gossipsub from pruning them from the mesh
            for peer_id in &self.connected_peers {
                if self.known_masternode_peer_ids.contains(peer_id) {
                    self.swarm
                        .behaviour_mut()
                        .gossipsub
                        .add_explicit_peer(peer_id);
                }
            }

            let keepalive = serde_json::json!({
                "type": "mesh_keepalive",
                "timestamp": current_timestamp(),
                "peer_id": self.swarm.local_peer_id().to_string(),
                "bootstrap_phase": self.bootstrap_phase,
                "connected_masternodes": connected_mn_count,
                "expected_masternodes": self.expected_masternodes,
            });

            if let Ok(data) = serde_json::to_vec(&keepalive) {
                // Send to masternode vote topic to keep that mesh active
                // flood_publish(true) ensures it reaches all connected peers, not just mesh
                if let Err(e) = self
                    .swarm
                    .behaviour_mut()
                    .gossipsub
                    .publish(self.masternode_vote_topic.clone(), data)
                {
                    debug!("Mesh keepalive publish failed: {}", e);
                } else {
                    info!(
                        bootstrap_phase = self.bootstrap_phase,
                        connected_mn = connected_mn_count,
                        expected_mn = self.expected_masternodes,
                        "🔥 [MESH WARMUP] Sent keepalive and marked {} masternodes as explicit peers",
                        connected_mn_count
                    );
                }
            }
        }
        Ok(())
    }

    /// Reconnect to disconnected masternode peers
    /// Checks which known masternodes are missing from connected_peers and re-dials them
    /// ENHANCED: More aggressive reconnection with logging
    fn reconnect_disconnected_masternodes(&mut self) {
        let local_peer_id = *self.swarm.local_peer_id();
        let connected_mn_count = self
            .connected_peers
            .iter()
            .filter(|p| self.known_masternode_peer_ids.contains(p))
            .count();

        // ENHANCED: Reconnect if we're missing any masternodes (not just below threshold)
        // This ensures we maintain full mesh connectivity
        if connected_mn_count >= self.expected_masternodes {
            return; // All masternodes connected
        }

        let mut reconnect_attempts = 0;
        for entry in &self.masternode_bootstrap_addrs.clone() {
            let (peer_id_str, addr_str) = match entry.split_once('@') {
                Some(parts) => parts,
                None => continue,
            };

            let peer_id = match PeerId::from_str(peer_id_str) {
                Ok(pid) => pid,
                Err(_) => continue,
            };

            // Skip self and already-connected peers
            if peer_id == local_peer_id || self.connected_peers.contains(&peer_id) {
                continue;
            }

            let addr = match Multiaddr::from_str(addr_str) {
                Ok(a) => a,
                Err(_) => continue,
            };

            let dial_opts = libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
                .addresses(vec![addr.clone()])
                .build();

            if let Err(err) = self.swarm.dial(dial_opts) {
                debug!(
                    peer_id = %peer_id,
                    addr = %addr,
                    error = %err,
                    "🔄 [RECONNECT] Failed to redial masternode"
                );
            } else {
                reconnect_attempts += 1;
                info!(
                    peer_id = %peer_id,
                    addr = %addr,
                    connected_mn = connected_mn_count,
                    expected_mn = self.expected_masternodes,
                    "🔄 [RECONNECT] Attempting to reconnect to disconnected masternode"
                );
            }
        }

        if reconnect_attempts > 0 {
            info!(
                reconnect_attempts = reconnect_attempts,
                connected_mn = connected_mn_count,
                expected_mn = self.expected_masternodes,
                "🔄 [RECONNECT] Initiated {} reconnection attempts to restore masternode mesh",
                reconnect_attempts
            );
        }
    }

    /// Log mesh status for monitoring
    fn log_mesh_status(&mut self) {
        // Avoid spam - only log if enough time has passed
        if self.last_mesh_status_log.elapsed() < Duration::from_secs(60) {
            return;
        }
        self.last_mesh_status_log = std::time::Instant::now();

        let connected_mn_count = self
            .connected_peers
            .iter()
            .filter(|p| self.known_masternode_peer_ids.contains(p))
            .count();

        // Get mesh peer counts for critical topics (if available via gossipsub API)
        // Note: libp2p gossipsub doesn't expose mesh peer list directly,
        // but we can infer from connected peers and subscriptions
        let critical_topics = [
            ("masternode_vote", &self.masternode_vote_topic),
            ("block_acceptance", &self.block_acceptance_topic),
            ("lightnode_proposal", &self.lightnode_proposal_topic),
        ];

        info!(
            bootstrap_phase = self.bootstrap_phase,
            connected_masternodes = connected_mn_count,
            expected_masternodes = self.expected_masternodes,
            total_connected_peers = self.connected_peers.len(),
            "📊 [MESH STATUS] Network state: {} masternodes connected (expected: {})",
            connected_mn_count,
            self.expected_masternodes
        );

        // Log warning if mesh might be degraded
        if connected_mn_count < self.expected_masternodes && !self.bootstrap_phase {
            warn!(
                connected_masternodes = connected_mn_count,
                expected_masternodes = self.expected_masternodes,
                "⚠️ [MESH STATUS] Fewer masternodes connected than expected - mesh may be degraded"
            );
        }
    }

    // Network message handlers for different topics
    async fn handle_heartbeat_message(
        &mut self,
        data: &[u8],
        source: Option<PeerId>,
    ) -> Result<()> {
        debug!("Received heartbeat from peer: {:?}", source);

        // CRITICAL FIX: Lightnode sends GossipMessage::Heartbeat wrapper
        // Format from lightnode: {"Heartbeat":{"timestamp":...,"nonce":...,"kind":...}}
        // We need to handle both formats for compatibility
        let heartbeat: HeartbeatMessage = match serde_json::from_slice::<GossipMessage>(data) {
            Ok(GossipMessage::Heartbeat(hb)) => {
                debug!("Decoded GossipMessage::Heartbeat wrapper");
                hb
            }
            Ok(other) => {
                warn!(
                    "Received non-Heartbeat gossip message on heartbeat topic: {:?}",
                    std::mem::discriminant(&other)
                );
                return Ok(());
            }
            Err(_gossip_err) => {
                // Fallback: try raw HeartbeatMessage format (masternode-to-masternode)
                match serde_json::from_slice::<HeartbeatMessage>(data) {
                    Ok(hb) => {
                        debug!("Decoded raw HeartbeatMessage (fallback)");
                        hb
                    }
                    Err(raw_err) => {
                        warn!("Failed to decode heartbeat message: {}", raw_err);
                        return Ok(());
                    }
                }
            }
        };

        // Update peer activity tracking
        if let Some(peer_id) = source {
            debug!(
                peer = %peer_id,
                nonce = %heartbeat.nonce,
                kind = ?heartbeat.kind,
                timestamp = %heartbeat.timestamp,
                "🫀 Processed heartbeat message"
            );

            // Update peer activity tracker for PoU scoring
            let activity = self
                .peer_activity
                .entry(peer_id)
                .or_insert_with(PeerActivityTracker::default);
            activity.heartbeat_count += 1;
            activity.last_heartbeat = current_timestamp();

            // Keep group formation state fresh for active lightnodes.
            if let Some(group_manager) = &self.group_formation_manager {
                let peer_id_str = peer_id.to_string();
                let updated = group_manager
                    .read()
                    .await
                    .update_last_seen_by_peer_id(&peer_id_str)
                    .await;
                if updated {
                    debug!("Updated last_seen from heartbeat for peer {}", peer_id_str);
                }
            }

            // Calculate uptime percentage
            let expected_heartbeats = activity.heartbeat_count + activity.missed_heartbeats;
            if expected_heartbeats > 0 {
                activity.uptime_percentage =
                    (activity.heartbeat_count as f64 / expected_heartbeats as f64) * 100.0;
            }

            // NOTE: Non rispondiamo con PONG via gossipsub broadcast.
            // Gossipsub è pub/sub: un PONG pubblicato viene ricevuto da TUTTI i peer,
            // causando amplificazione O(N*M) e "Send Queue full" (83K+ warnings nel test).
            // Il PING ricevuto è sufficiente per aggiornare il peer_activity_tracker
            // e il last_seen in the group_formation, che è tutto ciò che serve per PoU scoring.

            debug!(
                peer = %peer_id,
                heartbeat_count = activity.heartbeat_count,
                uptime = format!("{:.2}%", activity.uptime_percentage),
                "Updated peer activity tracker"
            );
        } else {
            warn!("Heartbeat message received without source peer");
        }

        Ok(())
    }

    async fn handle_pou_message(&mut self, data: &[u8], source: Option<PeerId>) -> Result<()> {
        debug!("Received PoU message from peer: {:?}", source);

        // Try to decode PoU broadcast message
        match serde_json::from_slice::<PouBroadcast>(data) {
            Ok(pou_data) => {
                if let Some(peer_id) = source {
                    info!(
                        peer = %peer_id,
                        node_id = %pou_data.peer_id,
                        epoch = %pou_data.epoch,
                        score = %pou_data.score,
                        index = %pou_data.index,
                        "Processed PoU broadcast message"
                    );

                    // Update PoU scoring database
                    let entry = PouScoreEntry {
                        peer_id: pou_data.peer_id.clone(),
                        epoch: pou_data.epoch,
                        score: pou_data.score,
                        index: pou_data.index,
                        last_updated: current_timestamp(),
                    };

                    self.pou_scores.insert(pou_data.peer_id.clone(), entry);

                    debug!(
                        node_id = %pou_data.peer_id,
                        epoch = pou_data.epoch,
                        score = pou_data.score,
                        total_tracked = self.pou_scores.len(),
                        "Updated PoU scoring database"
                    );
                } else {
                    warn!("PoU message received without source peer");
                }
            }
            Err(e) => {
                warn!("Failed to decode PoU message: {}", e);
            }
        }

        Ok(())
    }

    async fn handle_peer_info_message(
        &mut self,
        data: &[u8],
        source: Option<PeerId>,
    ) -> Result<()> {
        debug!("Received peer info message from peer: {:?}", source);

        // STEP 2: Decode the JSON payload
        // CRITICAL FIX: Lightnode sends GossipMessage::PeerInfo wrapper, NOT raw PeerInfoMessage
        // Format from lightnode: {"PeerInfo":{"account":[...]}}
        // We need to handle both formats for compatibility
        debug!("Decoding JSON payload ({} bytes)", data.len());

        // Validate minimum expected size
        if data.len() < 40 {
            warn!("Peer info message too small: {} bytes", data.len());
            return Ok(());
        }

        // Try to decode as GossipMessage wrapper first (lightnode format)
        let peer_info: PeerInfoMessage = match serde_json::from_slice::<GossipMessage>(data) {
            Ok(GossipMessage::PeerInfo(pi)) => {
                debug!(
                    "Decoded GossipMessage::PeerInfo wrapper - account: {}",
                    hex::encode(&pi.account[..8])
                );
                pi
            }
            Ok(_) => {
                warn!("Received different gossip message type in peer info topic");
                return Ok(());
            }
            Err(e) => {
                debug!(
                    "Failed to decode as GossipMessage, trying raw PeerInfoMessage: {}",
                    e
                );
                // Fallback: try raw PeerInfoMessage format (for masternode-to-masternode or future compatibility)
                match serde_json::from_slice::<PeerInfoMessage>(data) {
                    Ok(pi) => {
                        debug!(
                            "Decoded raw PeerInfoMessage (fallback) - account: {}",
                            hex::encode(&pi.account[..8])
                        );
                        pi
                    }
                    Err(e2) => {
                        warn!(
                            "Failed to decode peer info message in both formats: {} | {}",
                            e, e2
                        );
                        return Ok(());
                    }
                }
            }
        };

        // Add to batch collector for parallel processing
        if let Some(peer_id) = source {
            if let Some(batch) = self
                .peer_info_batch_collector
                .add_message(peer_id, data.to_vec())
            {
                // Process the batch
                self.handle_registration_batch(batch).await?;
            }
        } else {
            warn!("Peer info message received without source peer");
        }

        Ok(())
    }

    /// Handle a batch of peer info messages for parallel processing
    async fn handle_peer_info_batch(&mut self, batch: Vec<(PeerId, Vec<u8>)>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        info!(
            batch_size = batch.len(),
            "Processing peer info batch with {} messages",
            batch.len()
        );

        let mut last_account: Option<[u8; 32]> = None;
        let now = current_timestamp();

        // Decode all messages in the batch
        for (peer_id, data) in batch {
            match serde_json::from_slice::<GossipMessage>(&data) {
                Ok(GossipMessage::PeerInfo(pi)) => {
                    // Update peer registry only — NOT group formation.
                    // Group formation registration happens via handle_registration_batch
                    // which has the real multiaddr.
                    let is_new_peer = !self.peer_registry.contains_key(&peer_id);
                    let entry = self
                        .peer_registry
                        .entry(peer_id.clone())
                        .or_insert_with(|| PeerRegistryEntry {
                            account: pi.account,
                            first_seen: now,
                            last_seen: now,
                            is_active: true,
                            multiaddr: None,
                        });

                    let was_inactive = !entry.is_active;
                    entry.account = pi.account;
                    entry.last_seen = now;
                    entry.is_active = true;
                    last_account = Some(pi.account);

                    if is_new_peer {
                        info!(
                            "New peer registered: {} (account: {})",
                            peer_id,
                            hex::encode(&pi.account[..8])
                        );
                    } else if was_inactive {
                        info!("Previously inactive peer reactivated: {}", peer_id);
                    }
                }
                Ok(_) => {
                    warn!("Non-PeerInfo message in batch, skipping");
                }
                Err(e) => {
                    warn!("Failed to decode message in batch: {}", e);
                }
            }
        }

        // Send confirmation back if we processed any peer info messages
        if let Some(account) = last_account {
            let confirmation = GossipMessage::PeerInfo(PeerInfoMessage { account });
            if let Ok(confirmation_data) = serde_json::to_vec(&confirmation) {
                match self
                    .swarm
                    .behaviour_mut()
                    .publish(self.peer_info_topic.clone(), confirmation_data)
                {
                    Err(e) => {
                        debug!("Peer info confirmation publish: {}", e);
                    }
                    Ok(_) => {
                        debug!("Peer info confirmation sent");
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle explicit group formed ACK sent by lightnodes
    async fn handle_group_formed_ack(&mut self, data: &[u8], source: Option<PeerId>) -> Result<()> {
        match serde_json::from_slice::<GroupFormedAck>(data) {
            Ok(ack) => {
                info!(
                    group_id = %ack.group_id,
                    epoch = ack.epoch,
                    peer_id = %ack.peer_id,
                    connected = ack.connected_peers,
                    total = ack.total_peers,
                    source = ?source,
                    "Group formed ACK received from lightnode"
                );
                info!(
                    group_id = %ack.group_id,
                    peer_id = %ack.peer_id,
                    source = ?source,
                    "INTRAGROUP CONFIRMED (M) - lightnode reports P2P group mesh formed"
                );
            }
            Err(e) => {
                warn!("Failed to decode group formed ACK: {}", e);
                debug!(
                    "Raw data preview: {:?}",
                    &data.get(..std::cmp::min(data.len(), 100))
                );
            }
        }

        Ok(())
    }

    /// Handle lightnode registration for group formation
    /// This is the CRITICAL handler that registers lightnodes for group formation
    async fn handle_lightnode_registration(
        &mut self,
        data: &[u8],
        source: Option<PeerId>,
    ) -> Result<()> {
        info!(
            "📝 REGISTRATION: Received lightnode registration from peer: {:?}",
            source
        );

        // Add to batch collector for parallel processing
        if let Some(peer_id) = source {
            info!(
                peer_id = %peer_id,
                data_size = data.len(),
                "📦 REGISTRATION: Adding to batch collector"
            );

            if let Some(batch) = self
                .peer_info_batch_collector
                .add_message(peer_id, data.to_vec())
            {
                info!(
                    batch_size = batch.len(),
                    "🚀 REGISTRATION: Processing registration batch"
                );
                // Process the batch
                self.handle_registration_batch(batch).await?;
            } else {
                debug!("⏳ REGISTRATION: Batch not ready yet, message queued");
            }
        } else {
            warn!("Registration message received without source peer");
        }

        Ok(())
    }

    async fn handle_registration_batch(&mut self, batch: Vec<(PeerId, Vec<u8>)>) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        info!(
            batch_size = batch.len(),
            is_peak = self.peer_info_batch_collector.is_in_peak(),
            "🚀 REGISTRATION: Processing registration batch with {} messages",
            batch.len()
        );

        let mut lightnode_infos = Vec::new();
        let mut failed_registrations = Vec::new();
        let now = current_timestamp();

        // Decode all registration messages in the batch
        let batch_size = batch.len();
        for (peer_id, data) in batch {
            info!(
                peer_id = %peer_id,
                data_len = data.len(),
                data_preview = %hex::encode(&data[..data.len().min(50)]),
                "🔍 REGISTRATION: Attempting to deserialize registration message"
            );
            match serde_json::from_slice::<GossipMessage>(&data) {
                Ok(GossipMessage::LightnodeRegistration(registration)) => {
                    info!(
                        node_id = %registration.node_id,
                        peer_id = %peer_id,
                        region = %registration.geographic_region,
                        pou_score = registration.pou_score,
                        "✅ REGISTRATION DESERIALIZED - Adding to pending registrations"
                    );

                    // Convert to LightNodeInfo for group formation
                    // When lightnode registers with 127.0.0.1 AND this masternode's own
                    // external_ip is NOT 127.0.0.1 (i.e. cloud/multi-VM deployment), use the
                    // connection's remote IP so group peers on other VMs can dial.
                    // If this masternode is also on 127.0.0.1 (local test), keep it as-is.
                    let my_external_is_localhost = self.external_ip.as_deref() == Some("127.0.0.1");
                    let effective_multiaddr = if registration.multiaddr.contains("127.0.0.1")
                        && !my_external_is_localhost
                    {
                        let conn_addr = self
                            .shared_peers
                            .read()
                            .await
                            .get(&peer_id)
                            .map(|p| p.multiaddr.clone());
                        if let Some(conn_multiaddr) = conn_addr {
                            if let Some(ip) = try_extract_ipv4_from_multiaddr(&conn_multiaddr) {
                                let replaced =
                                    registration.multiaddr.replace("127.0.0.1", &ip.to_string());
                                info!(
                                    peer_id = %peer_id,
                                    connection_ip = %ip,
                                    "REGISTRATION: using connection IP for group dial (VM/cloud, was 127.0.0.1)"
                                );
                                replaced
                            } else {
                                registration.multiaddr.clone()
                            }
                        } else {
                            registration.multiaddr.clone()
                        }
                    } else {
                        registration.multiaddr.clone()
                    };
                    // SECURITY: Early-reject private/Docker IPs before they reach group_formation.
                    if effective_multiaddr.contains("/ip4/172.")
                        || effective_multiaddr.contains("/ip4/10.")
                        || effective_multiaddr.contains("/ip4/192.168.")
                        || effective_multiaddr.contains("/ip4/127.0.0.1")
                        || effective_multiaddr.contains("/ip4/0.0.0.0")
                    {
                        debug!(
                            peer_id = %peer_id,
                            multiaddr = %effective_multiaddr,
                            "REGISTRATION: rejected private/unreachable IP"
                        );
                        continue;
                    }

                    let multiaddr_valid =
                        !effective_multiaddr.is_empty() && !effective_multiaddr.contains("tcp/0");
                    info!(
                        peer_id = %registration.peer_id,
                        multiaddr = %effective_multiaddr,
                        multiaddr_valid = multiaddr_valid,
                        "REGISTRATION: lightnode multiaddr (valid = no empty, no tcp/0)"
                    );
                    let node_info = super::group_formation::LightNodeInfo {
                        node_id: registration.node_id.clone(),
                        peer_id: registration.peer_id.clone(),
                        multiaddr: effective_multiaddr,
                        geographic_region: registration.geographic_region.clone(),
                        pou_score: registration.pou_score,
                        capabilities: registration.capabilities.clone(),
                        last_seen: now,
                        uptime_percentage: registration.uptime_percentage,
                        account: registration.account,
                        assignment: super::group_formation::NodeAssignmentStatus::Free,
                    };

                    lightnode_infos.push(node_info);
                }
                Ok(_) => {
                    warn!(
                        peer_id = %peer_id,
                        "⚠️ Non-registration message in registration batch, skipping"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        peer_id = %peer_id,
                        data_preview = %hex::encode(&data[..data.len().min(50)]),
                        "❌ FAILED TO DESERIALIZE REGISTRATION"
                    );
                    // Add to failed registrations for retry
                    failed_registrations.push((peer_id, data));
                }
            }
        }

        // Register all lightnodes in batch with group formation manager
        if !lightnode_infos.is_empty() {
            if let Some(ref mut group_manager) = self.group_formation_manager {
                match group_manager
                    .write()
                    .await
                    .register_light_nodes_batch(lightnode_infos)
                    .await
                {
                    Ok(()) => {
                        info!(
                            "✅ REGISTRATION: Batch registration completed successfully for {} lightnodes",
                            batch_size
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to register lightnodes batch with group formation manager: {}",
                            e
                        );
                        // Add all registrations to retry queue
                        self.peer_info_batch_collector.process_batch_failure(
                            failed_registrations.clone(),
                            format!("Group formation manager error: {}", e),
                        );
                        return Err(anyhow!("Batch registration failed: {}", e));
                    }
                }
            } else {
                warn!("⚠️ REGISTRATION: Group formation manager not available, cannot register lightnodes");
                // Add to retry queue for later processing
                self.peer_info_batch_collector.process_batch_failure(
                    failed_registrations.clone(),
                    "Group formation manager not available".to_string(),
                );
            }
        }

        // Process any failed registrations
        if !failed_registrations.is_empty() {
            warn!(
                failed_count = failed_registrations.len(),
                "🔄 REGISTRATION: {} registrations failed, adding to retry queue",
                failed_registrations.len()
            );
            self.peer_info_batch_collector
                .process_batch_failure(failed_registrations, "Batch processing failed".to_string());
        }

        Ok(())
    }

    fn build_local_multiaddr(&self) -> Option<String> {
        let local_peer_id = self.local_peer_id().to_string();
        let listener = self.swarm.listeners().next()?.clone();
        let raw_addr = listener.to_string();
        let addr = if let Some(ext_ip) = self.external_ip.as_deref() {
            let ip = match ext_ip.parse::<IpAddr>() {
                Ok(ip) if is_routable_ip(&ip) => ip,
                Ok(_) => {
                    warn!(
                        external_ip = %ext_ip,
                        "Configured external_ip is private/local; skipping peer registry announce"
                    );
                    return None;
                }
                Err(e) => {
                    warn!(
                        external_ip = %ext_ip,
                        error = %e,
                        "Invalid external_ip; skipping peer registry announce"
                    );
                    return None;
                }
            };
            match build_advertised_multiaddr(&listener, ip) {
                Some(addr) => addr.to_string(),
                None => {
                    warn!(
                        %raw_addr,
                        "Could not build advertised multiaddr from listener; skipping peer registry announce"
                    );
                    return None;
                }
            }
        } else {
            if !is_routable_multiaddr(&listener) {
                warn!(
                    %raw_addr,
                    "No public external_ip configured; skipping non-routable peer registry announce"
                );
                return None;
            }
            raw_addr.trim_end_matches('/').to_string()
        };
        if addr.contains("/p2p/") {
            Some(addr)
        } else {
            Some(format!("{}/p2p/{}", addr, local_peer_id))
        }
    }

    async fn broadcast_registry_announce(&mut self, gossipsub_enabled: bool) -> Result<()> {
        let multiaddr = match self.build_local_multiaddr() {
            Some(addr) => addr,
            None => return Ok(()),
        };

        let announce = PeerRegistryAnnounce {
            peer_id: self.local_peer_id().to_string(),
            multiaddr,
            role: "masternode".to_string(),
            timestamp: current_timestamp(),
            ttl_secs: self.registry_ttl_secs,
        };

        let payload = serde_json::to_vec(&announce)?;
        let key = format!("peer_registry:{}", announce.peer_id);

        // Kademlia: ALWAYS publish (pull-based, low cost)
        if let Err(e) = self.put_kad_record(&key, payload.clone()) {
            warn!(
                "Failed to publish peer registry announce via Kademlia: {}",
                e
            );
        }

        // Gossipsub: only if enough LNs are registered to avoid Send Queue full errors
        if gossipsub_enabled {
            if let Err(e) = self
                .swarm
                .behaviour_mut()
                .gossipsub
                .publish(self.peer_registry_topic.clone(), payload)
            {
                debug!(
                    "Failed to publish registry announce via gossipsub (may have no peers yet): {}",
                    e
                );
            }
        }

        Ok(())
    }

    async fn handle_peer_registry_message(&mut self, data: &[u8]) -> Result<()> {
        match serde_json::from_slice::<PeerRegistryAnnounce>(data) {
            Ok(announce) => {
                let now = current_timestamp();
                if announce.ttl_secs == 0 || announce.multiaddr.is_empty() {
                    return Ok(());
                }

                let expires_at = now.saturating_add(announce.ttl_secs);
                let entry = PeerRegistryRecord {
                    peer_id: announce.peer_id.clone(),
                    multiaddr: announce.multiaddr.clone(),
                    role: announce.role.clone(),
                    last_seen: now,
                    expires_at,
                };
                self.registry_records
                    .insert(announce.peer_id.clone(), entry);

                // Dynamic MN discovery: if announce is from a masternode,
                // add it to known_masternode_peer_ids + explicit_peer + dial
                if announce.role == "masternode" {
                    if let Ok(peer_id) = PeerId::from_str(&announce.peer_id) {
                        let local_peer = *self.swarm.local_peer_id();
                        if peer_id != local_peer && self.known_masternode_peer_ids.insert(peer_id) {
                            self.swarm
                                .behaviour_mut()
                                .gossipsub
                                .add_explicit_peer(&peer_id);
                            // Dial if not already connected
                            if !self.connected_peers.contains(&peer_id) {
                                if let Ok(addr) = announce.multiaddr.parse::<Multiaddr>() {
                                    let dial_opts =
                                        libp2p::swarm::dial_opts::DialOpts::peer_id(peer_id)
                                            .addresses(vec![addr])
                                            .build();
                                    if let Err(e) = self.swarm.dial(dial_opts) {
                                        debug!(peer = %peer_id, error = %e, "Failed to dial newly discovered MN");
                                    }
                                }
                            }
                            info!(
                                peer = %peer_id,
                                multiaddr = %announce.multiaddr,
                                "🔍 [MN-DISCOVERY] New masternode discovered via registry announce, added to known_masternode_peer_ids + explicit_peer"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to decode peer registry announce: {}", e);
            }
        }

        Ok(())
    }

    fn prune_registry(&mut self) {
        let now = current_timestamp();
        self.registry_records
            .retain(|_, entry| entry.expires_at > now);
    }

    // NEW: Handle peer discovery requests from lightnodes
    async fn handle_peer_discovery_message(
        &mut self,
        data: &[u8],
        source: Option<PeerId>,
    ) -> Result<()> {
        debug!("Received peer discovery request from peer: {:?}", source);

        match serde_json::from_slice::<PeerDiscoveryRequest>(data) {
            Ok(request) => {
                info!(
                    requesting_peer = %request.requesting_peer,
                    "Processing peer discovery request"
                );

                let now = current_timestamp();
                let mut masternode_peers: Vec<String> = self
                    .registry_records
                    .values()
                    .filter(|entry| entry.role == "masternode" && entry.expires_at > now)
                    .map(|entry| entry.multiaddr.clone())
                    .collect();

                masternode_peers.sort();
                masternode_peers.dedup();

                let response = PeerDiscoveryResponse {
                    requesting_peer: request.requesting_peer,
                    masternode_peers,
                };

                if source.is_some() {
                    let payload = serde_json::to_vec(&response)?;
                    match self
                        .swarm
                        .behaviour_mut()
                        .publish(self.peer_discovery_topic.clone(), payload)
                    {
                        Ok(_) => {
                            info!(
                                peer_count = response.masternode_peers.len(),
                                "Sent peer discovery response to requesting peer"
                            );
                        }
                        Err(e) => {
                            // Duplicate is expected when multiple LNs send the same request
                            // in quick succession - the response payload is identical.
                            // InsufficientPeers may occur during mesh formation.
                            // Neither should crash the network task.
                            debug!(
                                error = %e,
                                peer_count = response.masternode_peers.len(),
                                "Peer discovery response publish skipped (non-fatal)"
                            );
                        }
                    }
                }
            }
            Err(e) => {
                warn!("Failed to decode peer discovery request: {}", e);
            }
        }

        Ok(())
    }

    // Helper method to determine if a peer is likely a masternode
    fn is_likely_masternode(&self, peer_id: PeerId) -> bool {
        // For now, assume all connected peers in our network are masternodes
        // In a real implementation, this could be based on peer behavior, port checking, etc.
        self.connected_peers.contains(&peer_id)
    }

    fn cache_block_payload(
        &mut self,
        hash: [u8; 64],
        height: u64,
        proposer: [u8; 32],
        txs: Vec<Vec<u8>>,
        source: &'static str,
    ) {
        // LRU eviction: remove oldest entry when cache is full
        while self.block_cache.len() >= BLOCK_CACHE_MAX {
            if let Some(oldest_hash) = self.block_cache_order.pop_front() {
                self.block_cache.remove(&oldest_hash);
            } else {
                break;
            }
        }

        // Remove previous order entry if this hash is being refreshed
        self.block_cache_order.retain(|h| h != &hash);
        self.block_cache_order.push_back(hash);

        let tx_count = txs.len();
        self.block_cache.insert(
            hash,
            BlockMessageWire {
                hash,
                header: BlockHeaderWire {
                    exec_height: height,
                    proposer,
                },
                txs,
            },
        );

        info!(
            hash = %hex::encode(&hash[..8]),
            height,
            tx_count,
            source,
            "Cached block payload for block_final"
        );
        match self.try_finalize_pending_from_cache(hash, height) {
            Ok(true) => {
                info!(
                    hash = %hex::encode(&hash[..8]),
                    height,
                    source,
                    "Pending certificate finalized after block payload cache fill"
                );
            }
            Ok(false) => {}
            Err(e) => {
                warn!(
                    hash = %hex::encode(&hash[..8]),
                    height,
                    source,
                    error = %e,
                    "Failed while draining pending certificate after block cache update"
                );
            }
        }
    }

    /// Cache block from lightnode gossip format for BlockWithCertificate (block_final) when cert is ready.
    fn cache_block_from_gossip(&mut self, data: &[u8]) {
        match serde_json::from_slice::<GossipBlockOnlyWire>(data) {
            Ok(GossipBlockOnlyWire::Block(block)) => {
                self.cache_block_payload(
                    block.hash,
                    block.header.exec_height,
                    block.header.proposer,
                    block.txs,
                    "block-topic",
                );
            }
            Err(e) => {
                // HaveBlock and other variants are expected to fail; only Block is cached
                if data.len() > 10 && data.starts_with(b"{\"Block\"") {
                    warn!(len = data.len(), error = %e, "block_topic Block decode failed (wire format LN vs MN?); check hash/header/txs serialization");
                }
            }
        }
    }

    /// loop can process other messages (including proposals from other groups)
    /// concurrently. This enables DAG-style parallel block certification.
    async fn validate_block_proposal_parallel(
        data: Vec<u8>,
        source: Option<PeerId>,
        tx_validator: Arc<std::sync::RwLock<TransactionValidator>>,
    ) -> Result<()> {
        // Skip LN gossip Block messages (not proposals)
        let looks_like_block_gossip = data.len() > 8
            && (data.starts_with(b"{\"Block\"") || data.starts_with(b"{\"HaveBlock\""));
        if looks_like_block_gossip {
            return Ok(());
        }

        let proposal: BlockProposal = match serde_json::from_slice(&data) {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!("Failed to decode block proposal (parallel): {}", e);
                return Ok(());
            }
        };

        let tx_count = proposal.transactions.len();
        let height = proposal.height;
        let group_id = proposal.proposer_group_id.clone();
        let block_hash_hex = hex::encode(&proposal.block_hash[..8]);

        tracing::info!(
            block_hash = %block_hash_hex,
            group_id = %group_id,
            height,
            tx_count,
            "PARALLEL: Validating block proposal"
        );

        // Convert to ValidatedTransaction
        let validated_txs: Vec<ValidatedTransaction> = proposal
            .transactions
            .into_iter()
            .map(|tx: LocalTransaction| tx.into())
            .collect();

        let validation_result = tx_validator
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .validate_block_transactions(validated_txs, group_id.clone());

        let is_accepted = validation_result.uniqueness_ratio >= 0.8
            && validation_result.duplicate_hashes.is_empty()
            && !validation_result.validated_transactions.is_empty();

        tracing::info!(
            block_hash = %block_hash_hex,
            group_id = %group_id,
            height,
            accepted = is_accepted,
            uniqueness = %validation_result.uniqueness_ratio,
            unique_txs = validation_result.unique_transactions,
            total_txs = validation_result.total_transactions,
            "PARALLEL: Block proposal validation completed"
        );

        Ok(())
    }

    async fn handle_block_proposal(&mut self, data: &[u8], source: Option<PeerId>) -> Result<()> {
        debug!("Received block proposal from peer: {:?}", source);
        // Try to cache as LN gossip Block for block+cert combined message
        self.cache_block_from_gossip(data);

        // On block_topic we often receive LN GossipMessage::Block; decoding as BlockProposal would fail (missing block_hash etc).
        // Only try BlockProposal path when payload looks like MN-style proposal (has "block_hash" key), to avoid noisy warns.
        let looks_like_block_gossip = data.len() > 8
            && (data.starts_with(b"{\"Block\"") || data.starts_with(b"{\"HaveBlock\""));

        // CONSERVATIVE: Try to decode, log if fails (don't crash)
        if looks_like_block_gossip {
            return Ok(());
        }
        match serde_json::from_slice::<BlockProposal>(data) {
            Ok(proposal) => {
                info!(
                    block_hash = %hex::encode(&proposal.block_hash[..8]),
                    group_id = %proposal.proposer_group_id,
                    height = proposal.height,
                    tx_count = proposal.transactions.len(),
                    "Processing block proposal"
                );

                // Convert to ValidatedTransaction format
                let validated_txs: Vec<ValidatedTransaction> = proposal
                    .transactions
                    .into_iter()
                    .map(|tx: LocalTransaction| tx.into())
                    .collect();

                // Validate transactions using anti-double spending logic
                // SECURITY: Use unwrap_or_else to handle poisoned RwLock gracefully
                // instead of panicking (audit LOW: expect/unwrap on network paths)
                let validation_result = self
                    .transaction_validator
                    .write()
                    .unwrap_or_else(|e| e.into_inner())
                    .validate_block_transactions(validated_txs, proposal.proposer_group_id.clone());

                let is_accepted = validation_result.uniqueness_ratio >= 0.8
                    && validation_result.duplicate_hashes.is_empty()
                    && !validation_result.validated_transactions.is_empty();

                let tx_validation_result = ValidationResult {
                    is_accepted,
                    validated_transactions: validation_result
                        .validated_transactions
                        .clone()
                        .into_iter()
                        .map(|vt| ValidatedTransaction {
                            tx_hash: vt.tx_hash,
                            sender: vt.sender,
                            receiver: vt.receiver,
                            amount: vt.amount,
                            nonce: vt.nonce,
                            signature: vt.signature,
                            block_hash: vt.block_hash,
                            execution_status: ExecutionStatus::Pending,
                            is_duplicate: false,
                            processed_at: Some(
                                std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_secs(),
                            ),
                            processing_group_id: Some(proposal.proposer_group_id.clone()),
                        })
                        .collect(),
                    duplicate_hashes: validation_result.duplicate_hashes.clone(),
                    total_transactions: validation_result.total_transactions,
                    unique_transactions: validation_result.unique_transactions,
                    uniqueness_ratio: validation_result.uniqueness_ratio,
                };

                // Generate masternode signature with real cryptographic signing
                let masternode_signature = self
                    .generate_masternode_signature(
                        &proposal.block_hash,
                        &tx_validation_result,
                        is_accepted,
                    )
                    .await?;

                let validation_msg = BlockValidationResult::new(
                    proposal.block_hash,
                    proposal.proposer_group_id,
                    validation_result,
                    is_accepted,
                );

                // channels: masternodes only certify block validity/finality.

                info!(
                    block_hash = %hex::encode(&proposal.block_hash[..8]),
                    group_id = %validation_msg.proposer_group_id,
                    accepted = %validation_msg.is_accepted,
                    uniqueness = %validation_msg.validation_result.uniqueness_ratio,
                    unique_txs = %validation_msg.validation_result.unique_transactions,
                    total_txs = %validation_msg.validation_result.total_transactions,
                    "Block proposal validation completed: {}",
                    validation_msg.get_summary()
                );
            }
            Err(e) => {
                warn!("Failed to decode block proposal: {}", e);
            }
        }

        Ok(())
    }

    async fn handle_consensus_certificate(
        &mut self,
        data: &[u8],
        source: Option<PeerId>,
    ) -> Result<()> {
        info!(
            source = ?source,
            data_len = data.len(),
            "📥 [MN<-MN/MN<-LN] Step 1: Received block certificate (block approval) from network"
        );

        // Strong guard-rail: this topic is certificate-only for masternodes.
        if let Some(kind) = detect_non_certificate_payload_kind(data) {
            debug!(
                source = ?source,
                payload_kind = kind,
                "Ignoring non-certificate payload on consensus cert topic (legacy/unsupported)"
            );
            return Ok(());
        }

        // Fallback: legacy ConsensusCertificate used by some older flows.
        if let Ok(certificate) =
            serde_json::from_slice::<crate::proposal_validator::BlockCertificate>(data)
        {
            let voters = certificate.votes.len();
            info!(
                block_hash = %hex::encode(&certificate.block_hash[..8]),
                height = certificate.height,
                group_id = %certificate.group_id,
                voters,
                source = ?source,
                "📥 [MN<-MN/MN<-LN] Step 2: Decoded block certificate (wire format) from network"
            );

            let is_valid = certificate.height > 0
                && certificate.block_hash != [0u8; 64]
                && !certificate.votes.is_empty();
            if !is_valid {
                warn!(
                    block_hash = %hex::encode(&certificate.block_hash[..8]),
                    height = certificate.height,
                    voters,
                    "Invalid block certificate received (wire format)"
                );
                return Ok(());
            }

            if !self.finalize_with_cached_payload(
                certificate.block_hash,
                certificate.height,
                certificate.group_id.clone(),
                "consensus-cert-wire",
            )? {
                self.enqueue_pending_certificate_finality(
                    certificate.block_hash,
                    certificate.height,
                    certificate.group_id.clone(),
                    source,
                    "wire",
                );
            }
            return Ok(());
        }

        // Backward-compat: older builds may publish non-certificate payloads on the
        // consensus cert topic. Ignore them explicitly to avoid noisy decode warnings.
        if serde_json::from_slice::<BlockValidationResult>(data).is_ok() {
            debug!(
                source = ?source,
                "Ignoring BlockValidationResult on consensus cert topic (deprecated payload)"
            );
            return Ok(());
        }
        if serde_json::from_slice::<MempoolSyncMessage>(data).is_ok() {
            debug!(
                source = ?source,
                "Ignoring MempoolSyncMessage on consensus cert topic (deprecated payload)"
            );
            return Ok(());
        }

        // Legacy format compatibility.
        match serde_json::from_slice::<crate::libp2p_network::ConsensusCertificate>(data) {
            Ok(certificate) => {
                info!(
                    block_hash = %hex::encode(&certificate.block_hash[..8]),
                    height = certificate.height,
                    group_id = %certificate.proposer_group_id,
                    voters = certificate.voter_signatures.len(),
                    source = ?source,
                    "📥 [MN<-MN/MN<-LN] Step 2: Decoded block certificate (legacy format) from network"
                );

                if certificate.is_valid() {
                    if !self.finalize_with_cached_payload(
                        certificate.block_hash,
                        certificate.height,
                        certificate.proposer_group_id.clone(),
                        "consensus-cert-legacy",
                    )? {
                        self.enqueue_pending_certificate_finality(
                            certificate.block_hash,
                            certificate.height,
                            certificate.proposer_group_id.clone(),
                            source,
                            "legacy",
                        );
                    }
                } else {
                    warn!(
                        block_hash = %hex::encode(&certificate.block_hash[..8]),
                        "Invalid consensus certificate received (legacy format)"
                    );
                }
            }
            Err(e) => {
                warn!(
                    "Failed to decode consensus certificate (all supported formats): {}",
                    e
                );
            }
        }

        Ok(())
    }

    /// Handle group proposal messages
    async fn handle_group_proposal(&mut self, data: &[u8], source: PeerId) -> Result<()> {
        info!(
            source = %source,
            bytes = data.len(),
            "Received group proposal on gossipsub"
        );

        // First try MasternodeMessage wrapper (current wire format)
        let proposal_value = if let Ok(msg) = serde_json::from_slice::<MasternodeMessage>(data) {
            match msg {
                MasternodeMessage::GroupProposal(proposal) => serde_json::to_value(proposal)?,
                _ => {
                    warn!(
                        "Non-group proposal message received on group proposal topic from {}",
                        source
                    );
                    return Ok(());
                }
            }
        } else {
            // Fallback: accept raw GroupProposal JSON
            serde_json::from_slice::<serde_json::Value>(data)?
        };

        debug!("Successfully decoded group proposal: {:?}", proposal_value);

        // Forward to group formation manager if available
        if let Some(group_manager) = &self.group_formation_manager {
            let mut manager = group_manager.write().await;
            manager
                .handle_proposal(&proposal_value, &source.to_string())
                .await?;
        }

        info!("Processed group proposal from {}", source);

        Ok(())
    }

    fn put_kad_record(&mut self, key: &str, value: Vec<u8>) -> Result<()> {
        let record = Record {
            key: RecordKey::new(&key),
            value,
            publisher: None,
            expires: None,
        };
        self.swarm
            .behaviour_mut()
            .kademlia
            .put_record(record, Quorum::One)
            .map_err(|e| anyhow!("failed to put kademlia record {key}: {e}"))?;
        Ok(())
    }

    /// Handle group vote messages
    async fn handle_group_vote(&mut self, data: &[u8], source: PeerId) -> Result<()> {
        info!(
            source = %source,
            bytes = data.len(),
            "Received group vote on gossipsub"
        );

        // First try MasternodeMessage wrapper (current wire format)
        let vote_value = if let Ok(msg) = serde_json::from_slice::<MasternodeMessage>(data) {
            match msg {
                MasternodeMessage::GroupVote(vote) => serde_json::to_value(vote)?,
                _ => {
                    warn!(
                        "Non-group vote message received on group vote topic from {}",
                        source
                    );
                    return Ok(());
                }
            }
        } else {
            // Fallback: legacy raw JSON vote
            serde_json::from_slice::<serde_json::Value>(data)?
        };

        // Try to decode as group vote message
        let vote = vote_value;
        debug!("Successfully decoded group vote: {:?}", vote);

        // Forward to group formation manager if available
        if let Some(group_manager) = &self.group_formation_manager {
            let mut manager = group_manager.write().await;
            manager.handle_vote(&vote, &source.to_string()).await?;
        }

        info!("Processed group vote from {}", source);

        Ok(())
    }

    /// Handle group sync messages
    async fn handle_group_sync(&mut self, data: &[u8], source: PeerId) -> Result<()> {
        debug!("Received group sync from {}", source);

        // Try to decode as group sync message
        match serde_json::from_slice::<serde_json::Value>(data) {
            Ok(sync) => {
                debug!("Successfully decoded group sync: {:?}", sync);

                // Forward to group formation manager if available
                if let Some(group_manager) = &self.group_formation_manager {
                    let mut manager = group_manager.write().await;
                    let sync_value: serde_json::Value = sync;
                    manager
                        .handle_sync(&sync_value, &source.to_string())
                        .await?;
                }

                info!("Processed group sync from {}", source);
            }
            Err(e) => {
                warn!("Failed to decode group sync from {}: {}", source, e);
                return Err(anyhow::anyhow!("Invalid group sync format"));
            }
        }

        Ok(())
    }

    /// Publish a masternode message via gossipsub (single shared behavior)
    fn publish_masternode_message(&mut self, message: MasternodeMessage) -> Result<()> {
        let topic = match &message {
            MasternodeMessage::GroupProposal(_) => {
                IdentTopic::new("/savitri/masternode/group/proposal/1")
            }
            MasternodeMessage::GroupVote(_) => IdentTopic::new("/savitri/masternode/group/vote/1"),
            MasternodeMessage::GroupApprovalCertificate(_) => {
                IdentTopic::new("/savitri/masternode/group/sync/1")
            }
            MasternodeMessage::AvailableLightnodesRequest { .. } => {
                IdentTopic::new("/savitri/masternode/group/sync/1")
            }
            MasternodeMessage::AvailableLightnodesResponse { .. } => {
                IdentTopic::new("/savitri/masternode/group/sync/1")
            }
            MasternodeMessage::GroupSyncRequest { .. } => {
                IdentTopic::new("/savitri/masternode/group/sync/1")
            }
            MasternodeMessage::GroupSyncResponse { .. } => {
                IdentTopic::new("/savitri/masternode/group/sync/1")
            }
            MasternodeMessage::LeaderElectionProposal(_) => self.leader_election_topic.clone(),
            MasternodeMessage::LeaderElectionCertificate(_) => self.leader_election_topic.clone(),
            MasternodeMessage::LightnodeListSync { .. } => self.lightnode_list_sync_topic.clone(),
            MasternodeMessage::LightnodeGroupAnnounce(_) => {
                IdentTopic::new("/savitri/lightnode/group/announce/1")
            }
        };

        let connected_peers = self.swarm.connected_peers().count();
        info!(
            topic = %topic,
            connected_peers,
            "Publishing masternode message via gossipsub"
        );
        if let MasternodeMessage::GroupVote(vote) = &message {
            info!(
                proposal_id = %vote.proposal_id,
                voter = %vote.voter_masternode,
                vote_type = ?vote.vote_type,
                "Publishing group vote via gossipsub"
            );
        }
        if let MasternodeMessage::GroupProposal(proposal) = &message {
            info!(
                proposal_id = %proposal.proposal_id,
                groups_count = proposal.groups.len(),
                proposer = %proposal.proposer_masternode,
                "Publishing group proposal via gossipsub"
            );
        }

        if let MasternodeMessage::LightnodeGroupAnnounce(announce) = &message {
            info!(
                group_id = %announce.group_id,
                epoch = announce.epoch,
                members_count = announce.members.len(),
                topic = %topic,
                connected_peers = self.swarm.connected_peers().count(),
                "🔔 Publishing LightnodeGroupAnnounce via gossipsub"
            );

            // Serializza e check
            let data = match serde_json::to_vec(announce) {
                Ok(d) => {
                    debug!(
                        data_len = d.len(),
                        data_preview = %hex::encode(&d[..d.len().min(100)]),
                        "Serialized LightnodeGroupAnnounce message"
                    );
                    d
                }
                Err(e) => {
                    error!(
                        error = %e,
                        group_id = %announce.group_id,
                        "❌ Failed to serialize LightnodeGroupAnnounce"
                    );
                    return Err(anyhow::anyhow!("Serialization failed: {}", e));
                }
            };

            let topic_clone = topic.clone();
            match self.swarm.behaviour_mut().publish(topic, data) {
                Ok(message_id) => {
                    info!(
                        group_id = %announce.group_id,
                        message_id = ?message_id,
                        topic = %topic_clone,
                        connected_peers = self.swarm.connected_peers().count(),
                        "✅ LightnodeGroupAnnounce published successfully via gossipsub"
                    );
                }
                Err(e) => {
                    error!(
                        error = %e,
                        group_id = %announce.group_id,
                        topic = %topic_clone,
                        connected_peers = self.swarm.connected_peers().count(),
                        "❌ Failed to publish LightnodeGroupAnnounce via gossipsub"
                    );
                    return Err(anyhow::anyhow!("Failed to publish: {}", e));
                }
            }
            return Ok(());
        }

        // Per evitare errori di tipo `Duplicate` dal livello gossipsub
        // (messaggi già pubblicati di recente), utilizziamo una cache
        // best-effort basata su proposal/election id ed epoch.
        //
        // eventuali meccanismi di deduplica globali sul gossip.
        if let Some(key) = self.build_masternode_dedupe_key(&message) {
            if self.masternode_dedupe_cache.contains(&key) {
                tracing::warn!(
                    dedupe_key = %key,
                    "Skipping masternode message publish because it was already published recently"
                );
                return Ok(());
            }

            // Manteniamo la cache entro un limit ragionevole per evitare crescita non controllata.
            const MAX_DEDUPE_ENTRIES: usize = 10_000;
            if self.masternode_dedupe_cache.len() >= MAX_DEDUPE_ENTRIES {
                // Strategia semplice: svuota completamente quando la threshold viene superata.
                // Per uso testnet è più che sufficiente.
                self.masternode_dedupe_cache.clear();
            }

            self.masternode_dedupe_cache.insert(key);
        }

        let data = serde_json::to_vec(&message)?;
        let data_len = data.len();
        match self.swarm.behaviour_mut().publish(topic.clone(), data) {
            Ok(_) => {
                counter!("p2p_messages_sent_total").increment(1);
                counter!("p2p_bytes_sent_total").increment(data_len as u64);
                counter!("gossip_messages_sent_total").increment(1);
            }
            Err(e) => {
                warn!(error = %e, topic = %topic, "publish_masternode_message failed (non-fatal)");
            }
        }
        Ok(())
    }

    /// Broadcast group proposal to network
    pub async fn broadcast_group_proposal(&mut self, proposal: &serde_json::Value) -> Result<()> {
        let data = serde_json::to_vec(proposal)?;
        let topic = libp2p::gossipsub::IdentTopic::new("/savitri/masternode/group/proposal/1");

        match self.swarm.behaviour_mut().publish(topic, data) {
            Ok(_) => {
                info!("Broadcasted group proposal to network");
            }
            Err(e) => {
                warn!(error = %e, "broadcast_group_proposal publish failed (non-fatal)");
            }
        }
        Ok(())
    }

    /// Broadcast group vote to network
    pub async fn broadcast_group_vote(&mut self, vote: &serde_json::Value) -> Result<()> {
        let data = serde_json::to_vec(vote)?;
        let topic = libp2p::gossipsub::IdentTopic::new("/savitri/masternode/group/vote/1");

        match self.swarm.behaviour_mut().publish(topic, data) {
            Ok(_) => {
                info!("Broadcasted group vote to network");
            }
            Err(e) => {
                warn!(error = %e, "broadcast_group_vote publish failed (non-fatal)");
            }
        }
        Ok(())
    }

    /// Broadcast group sync to network
    pub async fn broadcast_group_sync(&mut self, sync: &serde_json::Value) -> Result<()> {
        let data = serde_json::to_vec(sync)?;
        let topic = libp2p::gossipsub::IdentTopic::new("/savitri/masternode/group/sync/1");

        match self.swarm.behaviour_mut().publish(topic, data) {
            Ok(_) => {
                info!("Broadcasted group sync to network");
            }
            Err(e) => {
                warn!(error = %e, "broadcast_group_sync publish failed (non-fatal)");
            }
        }
        Ok(())
    }

    async fn generate_masternode_signature(
        &self,
        block_hash: &[u8; 64],
        validation_result: &ValidationResult,
        is_accepted: bool,
    ) -> Result<[u8; 64]> {
        use ed25519_dalek::{Signer, SigningKey};
        use sha2::{Digest, Sha256};

        let mut message = Vec::new();
        message.extend_from_slice(block_hash);
        message.extend_from_slice(&validation_result.uniqueness_ratio.to_le_bytes());
        message.extend_from_slice(
            &(validation_result.validated_transactions.len() as u64).to_le_bytes(),
        );
        message.extend_from_slice(&[if is_accepted { 1 } else { 0 }]);

        // Hash the message for signing
        let mut hasher = Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();

        // Get masternode private key (in real implementation, this would be from secure storage)
        let masternode_keypair = self.get_masternode_keypair().await?;

        // Sign the message
        let signature = masternode_keypair.sign(&message_hash);

        info!(
            block_hash = %hex::encode(&block_hash[..8]),
            accepted = %is_accepted,
            tx_count = validation_result.validated_transactions.len(),
            uniqueness = validation_result.uniqueness_ratio,
            "🔐 Generated masternode signature for block validation"
        );

        Ok(signature.to_bytes())
    }

    /// Get masternode keypair for signing
    async fn get_masternode_keypair(&self) -> Result<ed25519_dalek::SigningKey> {
        // In a real implementation, this would load the masternode's private key
        // from secure storage (HSM, encrypted file, etc.)
        // For now, we'll use a deterministic key based on the masternode ID

        let masternode_id = self.local_peer_id();
        let mut hasher = sha2::Sha512::new();
        hasher.update(b"MASTERNODE-KEYPAIR");
        hasher.update(masternode_id.to_bytes());
        let hash = hasher.finalize();

        // Use first 32 bytes as seed for keypair generation
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&hash[..32]);

        let keypair = ed25519_dalek::SigningKey::from_bytes(&seed);

        debug!("🔑 Generated masternode keypair for: {}", masternode_id);
        Ok(keypair)
    }
}

/// Compute the BFT signed-proposal hash that the lightnode signs and
/// the masternode verifies.
///
/// `compute_signed_proposal_hash` in savitri_consensus. Distinct from
/// the block-identity hash (`compute_block_hash`) because it includes
/// round-dependent fields (round_id, timestamp, proposer_pubkey,
/// tx_count). Wrapper kept under the legacy name for caller stability.
fn compute_compat_block_hash(proposal: &LightnodeBlockProposalCompat) -> [u8; 64] {
    savitri_consensus::primitives::hashing::compute_signed_proposal_hash(
        proposal.round_id,
        proposal.height,
        proposal.timestamp,
        &proposal.proposer_pubkey,
        &proposal.parent_hash,
        &proposal.state_root,
        &proposal.tx_root,
        proposal.transactions.len() as u32,
    )
}
