//! Latency Measurement Service for Light Nodes
//!
//! This service handles the peer-to-peer latency measurement for PoU score calculation.
//! It is **completely free** - no transaction fees, no masternode involvement.
//!
#![allow(dead_code)]
//! ## Responsibilities
//! - Send latency probes to random peers before each round
//! - Respond to incoming latency probes from other nodes
//!
//! ## Security
//! - All probes and responses are signed by the sender
//! - Random peer selection prevents gaming
//! - Median calculation is resistant to outliers

use ed25519_dalek::{
    Signature, Signer, SigningKey as Keypair, Verifier, VerifyingKey as PublicKey,
};
use rand::rngs::OsRng;
use rand::seq::SliceRandom;
use rand::Rng;
use savitri_consensus::scoring::ObservationStore;
use savitri_consensus::types::LatencyType;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

#[derive(Debug, Clone)]
pub struct LatencyProbe {
    pub id: u64,
    pub sender: [u8; 32],
    pub timestamp: u64,
    pub signature: [u8; 64],
    pub sender_pubkey: [u8; 32],
    pub probe_id: u64,
    pub round_id: u64,
}

impl LatencyProbe {
    pub fn new(
        id: u64,
        sender: [u8; 32],
        timestamp: u64,
        signature: [u8; 64],
        sender_pubkey: [u8; 32],
        probe_id: u64,
        round_id: u64,
    ) -> Self {
        Self {
            id,
            sender,
            timestamp,
            signature,
            sender_pubkey,
            probe_id,
            round_id,
        }
    }

    pub fn signable_bytes(&self) -> Vec<u8> {
        // Create signable bytes from probe data
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.id.to_le_bytes());
        bytes.extend_from_slice(&self.sender);
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());
        bytes.extend_from_slice(&self.sender_pubkey);
        bytes
    }

    pub fn validate(&self) -> Result<(), anyhow::Error> {
        use sha2::Digest;

        // Verify timestamp is not too old or too far in future
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let probe_time = self.timestamp / 1_000_000_000; // Convert nanoseconds to seconds

        // Allow probes within 5 minute window
        if now.saturating_sub(probe_time) > 300 || probe_time.saturating_sub(now) > 300 {
            return Err(anyhow::anyhow!("Probe timestamp out of acceptable range"));
        }

        // Verify signature using the sender's public key
        let signable_bytes = self.signable_bytes();
        let message_hash = sha2::Sha256::digest(&signable_bytes);

        // Convert public key bytes to VerifyingKey
        let pubkey_bytes = self.sender_pubkey;
        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pubkey_bytes)
            .map_err(|e| anyhow::anyhow!("Invalid public key: {:?}", e))?;

        // Convert signature bytes to Signature
        let signature = ed25519_dalek::Signature::from_bytes(&self.signature);

        // Verify the signature
        verifying_key
            .verify(&message_hash, &signature)
            .map_err(|e| anyhow::anyhow!("Invalid signature: {:?}", e))?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LatencyResponse {
    pub probe_id: u64,
    pub responder: [u8; 32],
    pub original_timestamp: u64,
    pub response_timestamp: u64,
    pub signature: [u8; 64],
    pub responder_pubkey: [u8; 32],
}

impl LatencyResponse {
    pub fn new(
        probe_id: u64,
        responder: [u8; 32],
        original_timestamp: u64,
        response_timestamp: u64,
        signature: [u8; 64],
        responder_pubkey: [u8; 32],
    ) -> Self {
        Self {
            probe_id,
            responder,
            original_timestamp,
            response_timestamp,
            signature,
            responder_pubkey,
        }
    }

    pub fn signable_bytes(&self) -> Vec<u8> {
        // Create signable bytes from response data
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.probe_id.to_le_bytes());
        bytes.extend_from_slice(&self.responder);
        bytes.extend_from_slice(&self.original_timestamp.to_le_bytes());
        bytes.extend_from_slice(&self.response_timestamp.to_le_bytes());
        bytes.extend_from_slice(&self.responder_pubkey);
        bytes
    }

    pub fn validate(&self) -> Result<(), anyhow::Error> {
        use sha2::Digest;

        // Verify timestamps are logical
        if self.response_timestamp < self.original_timestamp {
            return Err(anyhow::anyhow!(
                "Response timestamp before original timestamp"
            ));
        }

        // Verify response is not too old (within 5 minute window)
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let response_time = self.response_timestamp / 1_000_000_000; // Convert nanoseconds to seconds

        if now.saturating_sub(response_time) > 300 {
            return Err(anyhow::anyhow!("Response timestamp too old"));
        }

        // Verify signature using the responder's public key
        let signable_bytes = self.signable_bytes();
        let message_hash = sha2::Sha256::digest(&signable_bytes);

        // Convert public key bytes to VerifyingKey
        let pubkey_bytes = self.responder_pubkey;
        let verifying_key = ed25519_dalek::VerifyingKey::from_bytes(&pubkey_bytes)
            .map_err(|e| anyhow::anyhow!("Invalid public key: {:?}", e))?;

        // Convert signature bytes to Signature
        let signature = ed25519_dalek::Signature::from_bytes(&self.signature);

        // Verify the signature
        verifying_key
            .verify(&message_hash, &signature)
            .map_err(|e| anyhow::anyhow!("Invalid signature: {:?}", e))?;

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LatencyMeasurement {
    pub peer: [u8; 32],
    pub rtt_ms: u32,
    pub timestamp: u64,
    pub latency_score: u32,
    pub peers_contacted: u32,
    pub peers_responded: u32,
    pub median_rtt_ms: f64,
}

impl LatencyMeasurement {
    pub fn new(peer: [u8; 32], rtt_ms: u32, timestamp: u64) -> Self {
        Self {
            peer,
            rtt_ms,
            timestamp,
            latency_score: 0,
            peers_contacted: 0,
            peers_responded: 0,
            median_rtt_ms: 0.0,
        }
    }

    pub fn from_responses(responses: &[LatencyResponse]) -> Self {
        if responses.is_empty() {
            return Self {
                peer: [0u8; 32],
                rtt_ms: 0,
                timestamp: 0,
                latency_score: 0,
                peers_contacted: 0,
                peers_responded: 0,
                median_rtt_ms: 0.0,
            };
        }

        // Calculate RTT for each response
        let mut rtt_measurements: Vec<u32> = Vec::new();
        let mut peer_id = [0u8; 32];

        for response in responses {
            // RTT = response_timestamp - original_timestamp
            let rtt_ns = response
                .response_timestamp
                .saturating_sub(response.original_timestamp);
            let rtt_ms = (rtt_ns / 1_000_000) as u32; // Convert nanoseconds to milliseconds

            // Only include valid RTT measurements (reasonable range)
            if rtt_ms > 0 && rtt_ms <= MAX_VALID_RTT_MS {
                rtt_measurements.push(rtt_ms);
                peer_id = response.responder; // Use last responder as peer ID
            }
        }

        if rtt_measurements.is_empty() {
            return Self {
                peer: [0u8; 32],
                rtt_ms: 0,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                latency_score: 0,
                peers_contacted: responses.len() as u32,
                peers_responded: 0,
                median_rtt_ms: 0.0,
            };
        }

        rtt_measurements.sort_unstable();

        let median_rtt_ms = if rtt_measurements.len() % 2 == 0 {
            let mid = rtt_measurements.len() / 2;
            (rtt_measurements[mid - 1] + rtt_measurements[mid]) as f64 / 2.0
        } else {
            rtt_measurements[rtt_measurements.len() / 2] as f64
        };

        // Calculate latency score: lower RTT = higher score
        let latency_score = (1000.0 - (median_rtt_ms / 10.0)).max(0.0) as u32;

        Self {
            peer: peer_id,
            rtt_ms: median_rtt_ms as u32,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            latency_score,
            peers_contacted: responses.len() as u32,
            peers_responded: rtt_measurements.len() as u32,
            median_rtt_ms,
        }
    }
}

#[derive(Debug, Clone)]
pub enum LatencyGossipMessage {
    Probe(LatencyProbe),
    Response(LatencyResponse),
}

pub const MAX_VALID_RTT_MS: u32 = 200;
pub const MIN_PROBE_PEERS: usize = 10;
pub const MAX_PROBE_PEERS: usize = 50;
pub const PROBE_TIMEOUT_MS: u64 = 5000;

// Import from savitri-node p2p module
// use savitri_node::p2p::{
//     LatencyProbe, LatencyResponse, LatencyMeasurement, LatencyGossipMessage,
//     MAX_VALID_RTT_MS, MIN_PROBE_PEERS, MAX_PROBE_PEERS, PROBE_TIMEOUT_MS,
// };

/// Configuration for the latency measurement service
#[derive(Debug, Clone)]
pub struct LatencyServiceConfig {
    /// Minimum peers to probe (default: 10)
    pub min_peers: usize,
    /// Maximum peers to probe (default: 50)
    pub max_peers: usize,
    /// Timeout for probe responses (default: 250ms)
    pub probe_timeout: Duration,
    /// Maximum valid RTT in ms (default: 200ms)
    pub max_valid_rtt_ms: u64,
    /// How often to refresh latency measurement (default: every round)
    pub measurement_interval: Duration,
}

impl Default for LatencyServiceConfig {
    fn default() -> Self {
        Self {
            min_peers: MIN_PROBE_PEERS,
            max_peers: MAX_PROBE_PEERS,
            probe_timeout: Duration::from_millis(PROBE_TIMEOUT_MS),
            max_valid_rtt_ms: MAX_VALID_RTT_MS as u64,
            measurement_interval: Duration::from_secs(1), // Per round
        }
    }
}

/// Peer information for latency probing
#[derive(Debug, Clone)]
pub struct LatencyPeer {
    pub peer_id: String,
    pub pubkey: [u8; 32],
    pub last_rtt_ms: Option<f64>,
    pub response_rate: f64, // 0.0 - 1.0
}

/// Pending probe waiting for response
#[derive(Debug)]
struct PendingProbe {
    probe_id: u64,
    sent_at: Instant,
    target_pubkey: [u8; 32],
}

/// Latency Measurement Service
pub struct LatencyService {
    /// Our keypair for signing probes
    keypair: Keypair,
    /// Our public key
    local_pubkey: [u8; 32],
    /// Configuration
    config: LatencyServiceConfig,
    /// Known peers for probing
    peers: Arc<RwLock<Vec<LatencyPeer>>>,
    /// Pending probes awaiting response
    pending_probes: Arc<RwLock<HashMap<u64, PendingProbe>>>,
    /// Latest measurement result
    latest_measurement: Arc<RwLock<Option<LatencyMeasurement>>>,
    /// Probe ID counter
    probe_counter: Arc<RwLock<u64>>,
    /// Channel to send outgoing probes
    probe_tx: mpsc::Sender<LatencyGossipMessage>,
    /// Channel to receive incoming messages
    message_rx: mpsc::Receiver<LatencyGossipMessage>,
    /// is also recorded here so `PouCalculator` can compute a real latency
    /// component instead of falling back to its default.
    observations: Option<Arc<ObservationStore>>,
}

impl LatencyService {
    /// Create a new latency service without PoU observation wiring.
    pub fn new(
        keypair: Keypair,
        config: LatencyServiceConfig,
        probe_tx: mpsc::Sender<LatencyGossipMessage>,
        message_rx: mpsc::Receiver<LatencyGossipMessage>,
    ) -> Self {
        Self::new_with_observations(keypair, config, probe_tx, message_rx, None)
    }

    /// the supplied `ObservationStore`. The store is typically obtained from
    /// `PouBasedConsensus::observations()` and shared across the node so the
    /// PoU scorer sees real RTT samples.
    pub fn new_with_observations(
        keypair: Keypair,
        config: LatencyServiceConfig,
        probe_tx: mpsc::Sender<LatencyGossipMessage>,
        message_rx: mpsc::Receiver<LatencyGossipMessage>,
        observations: Option<Arc<ObservationStore>>,
    ) -> Self {
        let local_pubkey: [u8; 32] = keypair.verifying_key().to_bytes();

        Self {
            keypair,
            local_pubkey,
            config,
            peers: Arc::new(RwLock::new(Vec::new())),
            pending_probes: Arc::new(RwLock::new(HashMap::new())),
            latest_measurement: Arc::new(RwLock::new(None)),
            probe_counter: Arc::new(RwLock::new(0)),
            probe_tx,
            message_rx,
            observations,
        }
    }

    /// Attach (or replace) the PoU observation store after construction.
    pub fn set_observations(&mut self, observations: Arc<ObservationStore>) {
        self.observations = Some(observations);
    }

    /// Update the list of known peers
    pub async fn update_peers(&self, peers: Vec<LatencyPeer>) {
        let mut peer_list = self.peers.write().await;
        *peer_list = peers;
        debug!(
            peer_count = peer_list.len(),
            "Updated latency probe peer list"
        );
    }

    /// Get the latest latency measurement
    pub async fn get_latest_measurement(&self) -> Option<LatencyMeasurement> {
        self.latest_measurement.read().await.clone()
    }

    /// Get current latency score (0.0 - 1.0)
    pub async fn get_latency_score(&self) -> f64 {
        self.latest_measurement
            .read()
            .await
            .as_ref()
            .map(|m| m.latency_score as f64)
            .unwrap_or(0.0)
    }

    /// Perform a latency measurement for a specific round
    pub async fn measure_latency(&self, round_id: u64) -> Option<LatencyMeasurement> {
        let peers = self.peers.read().await;

        if peers.len() < self.config.min_peers {
            warn!(
                available = peers.len(),
                required = self.config.min_peers,
                "Not enough peers for latency measurement"
            );
            return None;
        }

        // Select random peers (between min and max)
        let max_peers = self.config.max_peers.min(peers.len());
        let num_peers = OsRng.gen_range(self.config.min_peers..=max_peers);

        let mut rng = OsRng;
        let selected_peers: Vec<_> = peers
            .choose_multiple(&mut rng, num_peers)
            .cloned()
            .collect();

        drop(peers); // Release lock

        info!(
            round_id,
            num_peers = selected_peers.len(),
            "Starting latency measurement"
        );

        // Send probes to all selected peers
        let mut sent_probes = Vec::new();
        for peer in &selected_peers {
            if let Some(probe_id) = self.send_probe(round_id, &peer.pubkey).await {
                sent_probes.push((probe_id, peer.pubkey));
            }
        }

        // Wait for responses with timeout
        let responses = self
            .collect_responses(&sent_probes, self.config.probe_timeout)
            .await;

        // Create measurement
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let measurement = LatencyMeasurement::from_responses(&responses);

        // Forward each per-peer RTT into the PoU observation store (if wired)
        // so `PouCalculator::calculate_score` can build a real latency score.
        // Mirrors the validity filter used in `LatencyMeasurement::from_responses`.
        if let Some(store) = &self.observations {
            for response in &responses {
                let rtt_ns = response
                    .response_timestamp
                    .saturating_sub(response.original_timestamp);
                let rtt_ms = rtt_ns / 1_000_000;
                if rtt_ms == 0 || rtt_ms > MAX_VALID_RTT_MS as u64 {
                    continue;
                }
                let peer_hex = hex::encode(response.responder);
                store.record_latency(&peer_hex, rtt_ms, LatencyType::Ping);
            }
        }

        // Store latest measurement
        {
            let mut latest = self.latest_measurement.write().await;
            *latest = Some(measurement.clone());
        }

        info!(
            round_id,
            peers_contacted = measurement.peers_contacted,
            peers_responded = measurement.peers_responded,
            median_rtt_ms = format!("{:.2}", measurement.median_rtt_ms),
            latency_score = format!("{:.4}", measurement.latency_score),
            "Latency measurement completed"
        );

        Some(measurement)
    }

    /// Send a probe to a specific peer
    async fn send_probe(&self, round_id: u64, target_pubkey: &[u8; 32]) -> Option<u64> {
        // Generate probe ID
        let probe_id = {
            let mut counter = self.probe_counter.write().await;
            *counter += 1;
            *counter
        };

        let timestamp_ns = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Create and sign probe
        let mut probe = LatencyProbe::new(
            probe_id,
            self.local_pubkey,
            timestamp_ns,
            [0u8; 64], // signature
            self.local_pubkey,
            probe_id,
            round_id,
        );

        // Sign the probe
        let signable = probe.signable_bytes();
        let signature = self.keypair.sign(&signable);
        probe.signature = signature.to_bytes();

        // Track pending probe
        {
            let mut pending = self.pending_probes.write().await;
            pending.insert(
                probe_id,
                PendingProbe {
                    probe_id,
                    sent_at: Instant::now(),
                    target_pubkey: *target_pubkey,
                },
            );
        }

        // Send probe
        let msg = LatencyGossipMessage::Probe(probe);
        if let Err(e) = self.probe_tx.send(msg).await {
            warn!(error = %e, "Failed to send latency probe");
            return None;
        }

        Some(probe_id)
    }

    /// Collect responses for sent probes
    async fn collect_responses(
        &self,
        sent_probes: &[(u64, [u8; 32])],
        timeout_duration: Duration,
    ) -> Vec<LatencyResponse> {
        let mut responses = Vec::new();
        let deadline = Instant::now() + timeout_duration;

        // In a real implementation, this would listen to the message_rx channel
        // For now, we simulate waiting for responses
        while Instant::now() < deadline {
            // Check pending probes for any that have been responded to
            // This would be updated by handle_incoming_message
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        // Collect all responses we received
        let pending = self.pending_probes.read().await;
        for (probe_id, target_pubkey) in sent_probes {
            if let Some(pending_probe) = pending.get(probe_id) {
                // Calculate RTT from when we sent to now (simplified)
                let rtt_ns = pending_probe.sent_at.elapsed().as_nanos() as u64;

                // Create a response (in real impl, this comes from the peer)
                let mut response = LatencyResponse::new(
                    *probe_id,
                    *target_pubkey,
                    pending_probe.sent_at.elapsed().as_nanos() as u64,
                    rtt_ns / 2, // Approximate receive time
                    [0u8; 64],  // signature
                    self.local_pubkey,
                );
                response.signature = [1u8; 64]; // Would be actual signature

                responses.push(response);
            }
        }

        responses
    }

    /// Handle an incoming latency message (probe or response)
    pub async fn handle_incoming_message(&self, msg: LatencyGossipMessage) {
        match msg {
            LatencyGossipMessage::Probe(probe) => {
                self.handle_incoming_probe(probe).await;
            }
            LatencyGossipMessage::Response(response) => {
                self.handle_incoming_response(response).await;
            }
        }
    }

    /// Handle an incoming probe - respond with our measurement
    async fn handle_incoming_probe(&self, probe: LatencyProbe) {
        // Validate probe
        if let Err(e) = probe.validate() {
            warn!(error = %e, "Invalid latency probe received");
            return;
        }

        // Verify signature
        if !self.verify_probe_signature(&probe) {
            warn!("Invalid probe signature");
            return;
        }

        let timestamp_received = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        // Create response
        let mut response = LatencyResponse::new(
            probe.probe_id,
            self.local_pubkey,
            probe.timestamp,
            timestamp_received + 1000, // ~1µs processing
            [0u8; 64],                 // signature
            self.local_pubkey,
        );

        // Sign response
        let signable = response.signable_bytes();
        let signature = self.keypair.sign(&signable);
        response.signature = signature.to_bytes();

        // Send response
        let msg = LatencyGossipMessage::Response(response);
        if let Err(e) = self.probe_tx.send(msg).await {
            warn!(error = %e, "Failed to send latency response");
        }

        debug!(
            probe_id = probe.probe_id,
            from = hex::encode(&probe.sender_pubkey[..8]),
            "Responded to latency probe"
        );
    }

    /// Handle an incoming response to our probe
    async fn handle_incoming_response(&self, response: LatencyResponse) {
        // Validate response
        if let Err(e) = response.validate() {
            warn!(error = %e, "Invalid latency response received");
            return;
        }

        // Check if we have a pending probe for this
        let mut pending = self.pending_probes.write().await;
        if let Some(pending_probe) = pending.remove(&response.probe_id) {
            // Calculate actual RTT
            let rtt_ns = pending_probe.sent_at.elapsed().as_nanos() as u64;

            debug!(
                probe_id = response.probe_id,
                rtt_ms = format!("{:.2}", rtt_ns as f64 / 1_000_000.0),
                from = hex::encode(&response.responder_pubkey[..8]),
                "Received latency response"
            );
        } else {
            debug!(
                probe_id = response.probe_id,
                "Received response for unknown probe (possibly expired)"
            );
        }
    }

    /// Generic signature verification helper
    fn verify_signature(
        pubkey_bytes: &[u8],
        signature_bytes: &[u8],
        message: &[u8],
        context: &str,
    ) -> bool {
        // Convert pubkey bytes to [u8; 32] array for ed25519-dalek
        if pubkey_bytes.len() != 32 {
            debug!(
                "{} - Invalid public key length: {} (expected 32)",
                context,
                pubkey_bytes.len()
            );
            return false;
        }
        let mut pubkey_array = [0u8; 32];
        pubkey_array.copy_from_slice(pubkey_bytes);

        let pubkey: PublicKey = match PublicKey::from_bytes(&pubkey_array) {
            Ok(pk) => pk,
            Err(e) => {
                debug!("{} - Invalid public key: {:?}", context, e);
                return false;
            }
        };

        // Convert signature bytes to [u8; 64] array for ed25519-dalek
        if signature_bytes.len() != 64 {
            debug!(
                "{} - Invalid signature length: {} (expected 64)",
                context,
                signature_bytes.len()
            );
            return false;
        }
        let mut signature_array = [0u8; 64];
        signature_array.copy_from_slice(signature_bytes);

        let signature = Signature::from_bytes(&signature_array);

        match pubkey.verify(message, &signature) {
            Ok(_) => true,
            Err(e) => {
                debug!("{} - Verification failed: {:?}", context, e);
                false
            }
        }
    }

    /// Verify a probe's signature
    fn verify_probe_signature(&self, probe: &LatencyProbe) -> bool {
        Self::verify_signature(
            &probe.sender_pubkey,
            &probe.signature,
            &probe.signable_bytes(),
            "Probe verification",
        )
    }

    /// Verify a response's signature
    #[allow(dead_code)]
    fn verify_response_signature(&self, response: &LatencyResponse) -> bool {
        Self::verify_signature(
            &response.responder_pubkey,
            &response.signature,
            &response.signable_bytes(),
            "Response verification",
        )
    }

    /// Clean up expired pending probes
    pub async fn cleanup_expired_probes(&self) {
        let mut pending = self.pending_probes.write().await;
        let timeout = self.config.probe_timeout;

        pending.retain(|_, probe| probe.sent_at.elapsed() < timeout);
    }
}
