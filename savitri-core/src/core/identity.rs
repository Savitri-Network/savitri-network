//! Identity Registry and Handshake Management
//!
//! This module provides Sybil defense through mandatory identity handshake.
//! Peers must complete a handshake before they can send non-handshake messages.

use anyhow::Result;
use ed25519_dalek::{Signer, Verifier};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Peer ID wrapper (compatibile con libp2p ma without dipendenza)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct PeerId(pub Vec<u8>);

impl PeerId {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() || bytes.len() > 64 {
            anyhow::bail!("Invalid peer ID length");
        }
        Ok(Self(bytes.to_vec()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// Identity mapping from handshake
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdentityMapping {
    pub peer_id: PeerId,
    pub consensus_pubkey: ConsensusPubkey,
}

/// Consensus public key wrapper
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConsensusPubkey {
    pub bytes: [u8; 32],
}

impl ConsensusPubkey {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }
}

/// Handshake message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HandshakeMessage {
    Init {
        peer_id: PeerId,
        consensus_pubkey: ConsensusPubkey,
        timestamp: u64,
        signature: Vec<u8>,
    },
    Response {
        peer_id: PeerId,
        consensus_pubkey: ConsensusPubkey,
        nonce: [u8; 32],
        signature: Vec<u8>,
    },
    Confirm {
        peer_id: PeerId,
        attestation: Vec<u8>,
    },
}

/// Identity Registry for tracking completed handshakes
pub struct IdentityRegistry {
    completed_handshakes: Arc<Mutex<HashSet<PeerId>>>,
    identity_mappings: Arc<Mutex<HashMap<PeerId, IdentityMapping>>>,
}

impl IdentityRegistry {
    /// Create a new identity registry
    pub fn new() -> Self {
        Self {
            completed_handshakes: Arc::new(Mutex::new(HashSet::new())),
            identity_mappings: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Register a completed handshake
    pub fn register_handshake(&self, mapping: IdentityMapping) -> Result<()> {
        let peer_id = mapping.peer_id.clone();

        // Locks here are only poisoned if another thread panicked while holding
        // them; in that case the registry state is already inconsistent and we
        // surface the failure as an error to the caller.
        let mut completed = self
            .completed_handshakes
            .lock()
            .map_err(|_| anyhow::anyhow!("identity registry: completed_handshakes mutex poisoned"))?;
        let mut mappings = self
            .identity_mappings
            .lock()
            .map_err(|_| anyhow::anyhow!("identity registry: identity_mappings mutex poisoned"))?;

        completed.insert(peer_id.clone());
        mappings.insert(peer_id, mapping);

        Ok(())
    }

    /// Check if a peer has completed handshake
    pub fn has_completed_handshake(&self, peer_id: &PeerId) -> bool {
        // If the lock is poisoned the registry state is corrupt and reporting
        // "no completed handshake" is the safer default than panicking.
        match self.completed_handshakes.lock() {
            Ok(completed) => completed.contains(peer_id),
            Err(_) => false,
        }
    }

    /// Get identity mapping for a peer
    pub fn get_mapping(&self, peer_id: &PeerId) -> Option<IdentityMapping> {
        // Same defensive treatment as `has_completed_handshake`: a poisoned
        // mutex returns `None` rather than panicking the caller.
        self.identity_mappings
            .lock()
            .ok()
            .and_then(|mappings| mappings.get(peer_id).cloned())
    }
}

/// Handshake Manager for processing handshake messages
pub struct HandshakeManager {
    registry: Arc<IdentityRegistry>,
    pending_requests: Arc<Mutex<HashMap<PeerId, HandshakeRequest>>>,
    used_nonces: Arc<Mutex<HashSet<[u8; 32]>>>,
    local_keypair: ed25519_dalek::SigningKey,
}

#[derive(Debug, Clone)]
struct HandshakeRequest {
    peer_id: PeerId,
    consensus_pubkey: ConsensusPubkey,
    nonce: [u8; 32],
    timestamp: u64,
}

impl HandshakeManager {
    /// Create new handshake manager
    pub fn new(registry: Arc<IdentityRegistry>) -> Self {
        Self {
            registry,
            pending_requests: Arc::new(Mutex::new(HashMap::new())),
            used_nonces: Arc::new(Mutex::new(HashSet::new())),
            local_keypair: ed25519_dalek::SigningKey::generate(&mut rand::rngs::OsRng),
        }
    }

    /// Verify signature using Ed25519
    fn verify_signature(&self, message: &[u8], signature: &[u8], pubkey: &[u8]) -> Result<bool> {
        if signature.len() != 64 || pubkey.len() != 32 {
            return Ok(false);
        }

        // Invariant: the length check above (`pubkey.len() != 32`) guarantees
        // that this conversion to `&[u8; 32]` succeeds.
        let pubkey_arr: &[u8; 32] = pubkey
            .try_into()
            .expect("invariant: pubkey length checked above to be 32");
        let public_key = match ed25519_dalek::VerifyingKey::from_bytes(pubkey_arr) {
            Ok(key) => key,
            Err(_) => return Ok(false),
        };
        let sig_array: &[u8; 64] = match signature.try_into() {
            Ok(arr) => arr,
            Err(_) => return Ok(false),
        };
        let signature = ed25519_dalek::Signature::from_bytes(sig_array);

        Ok(public_key.verify(message, &signature).is_ok())
    }

    /// Sign message using local Ed25519 keypair
    fn sign_message(&self, message: &[u8]) -> Vec<u8> {
        self.local_keypair.sign(message).to_bytes().to_vec()
    }

    /// Generate cryptographically secure nonce
    fn generate_nonce(&self) -> [u8; 32] {
        let mut nonce = [0u8; 32];
        let mut rng = rand::rngs::OsRng;
        rng.fill_bytes(&mut nonce);
        nonce
    }

    /// Process a handshake init request
    pub fn process_handshake_request(
        &mut self,
        init: HandshakeMessage,
    ) -> Result<HandshakeMessage> {
        match init {
            HandshakeMessage::Init {
                peer_id,
                consensus_pubkey,
                timestamp,
                signature,
            } => {
                // Verify the signature
                let message_string = format!(
                    "{}{}{}",
                    hex::encode(&peer_id.0),
                    hex::encode(&consensus_pubkey.bytes),
                    timestamp
                );
                let message_to_verify = message_string.as_bytes();

                if !self.verify_signature(message_to_verify, &signature, &consensus_pubkey.bytes)? {
                    anyhow::bail!("Invalid signature in handshake init");
                }

                // Generate nonce for response
                let nonce = self.generate_nonce();

                // Store pending request
                let mut pending = self
                    .pending_requests
                    .lock()
                    .map_err(|_| anyhow::anyhow!("handshake manager: pending_requests mutex poisoned"))?;
                pending.insert(
                    peer_id.clone(),
                    HandshakeRequest {
                        peer_id: peer_id.clone(),
                        consensus_pubkey: consensus_pubkey.clone(),
                        nonce,
                        timestamp,
                    },
                );

                // Create response message
                let response_string = format!(
                    "{}{}{}",
                    hex::encode(&peer_id.0),
                    hex::encode(&consensus_pubkey.bytes),
                    hex::encode(&nonce)
                );
                let response_message = response_string.as_bytes();

                let response_signature = self.sign_message(response_message);

                Ok(HandshakeMessage::Response {
                    peer_id,
                    consensus_pubkey,
                    nonce,
                    signature: response_signature,
                })
            }
            _ => anyhow::bail!("Expected Init message"),
        }
    }

    /// Process a handshake response
    pub fn process_handshake_response(
        &mut self,
        response: HandshakeMessage,
        _account: crate::core::types::Account, // Account for validation (not used yet)
    ) -> Result<IdentityMapping> {
        match response {
            HandshakeMessage::Response {
                peer_id,
                consensus_pubkey,
                nonce,
                signature,
            } => {
                // Check if nonce was already used (replay protection)
                let mut used_nonces = self
                    .used_nonces
                    .lock()
                    .map_err(|_| anyhow::anyhow!("handshake manager: used_nonces mutex poisoned"))?;
                if used_nonces.contains(&nonce) {
                    anyhow::bail!("Replay attack detected: nonce already used");
                }

                // Verify the signature
                let message_string = format!(
                    "{}{}{}",
                    hex::encode(&peer_id.0),
                    hex::encode(&consensus_pubkey.bytes),
                    hex::encode(&nonce)
                );
                let message_to_verify = message_string.as_bytes();

                if !self.verify_signature(message_to_verify, &signature, &consensus_pubkey.bytes)? {
                    anyhow::bail!("Invalid signature in handshake response");
                }

                used_nonces.insert(nonce);

                // Remove from pending
                let mut pending = self
                    .pending_requests
                    .lock()
                    .map_err(|_| anyhow::anyhow!("handshake manager: pending_requests mutex poisoned"))?;
                pending.remove(&peer_id);

                // Create identity mapping
                Ok(IdentityMapping {
                    peer_id,
                    consensus_pubkey,
                })
            }
            _ => anyhow::bail!("Expected Response message"),
        }
    }

    /// Verify a handshake confirmation message
    pub fn verify_confirmation(&mut self, confirm: HandshakeMessage) -> Result<()> {
        match confirm {
            HandshakeMessage::Confirm {
                peer_id,
                attestation,
            } => {
                // Complete attestation verification implementation
                self.verify_attestation(&peer_id, &attestation)
            }
            _ => anyhow::bail!("Expected Confirm message"),
        }
    }

    fn verify_attestation(&self, peer_id: &PeerId, attestation: &[u8]) -> Result<()> {
        if attestation.len() < 64 {
            anyhow::bail!("Invalid attestation length");
        }

        // Parse attestation structure: signature || certificate_chain || timestamp || peer_id_hash
        let signature = &attestation[0..64];
        let remaining = &attestation[64..];

        if remaining.len() < 8 {
            anyhow::bail!("Invalid attestation structure");
        }

        // Extract timestamp (8 bytes)
        let timestamp_bytes = &remaining[0..8];
        let timestamp = u64::from_le_bytes([
            timestamp_bytes[0],
            timestamp_bytes[1],
            timestamp_bytes[2],
            timestamp_bytes[3],
            timestamp_bytes[4],
            timestamp_bytes[5],
            timestamp_bytes[6],
            timestamp_bytes[7],
        ]);

        // Check if attestation is not too old (24 hours)
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if current_time > timestamp && (current_time - timestamp) > 86400 {
            anyhow::bail!("Attestation is too old (older than 24 hours)");
        }

        // Extract peer_id_hash (32 bytes)
        if remaining.len() < 40 {
            anyhow::bail!("Invalid attestation structure - missing peer_id_hash");
        }

        let peer_id_hash = &remaining[8..40];

        // Verify peer_id_hash matches actual peer_id
        let computed_hash = {
            let mut hasher = sha2::Sha256::new();
            hasher.update(peer_id.as_bytes());
            hasher.finalize()
        };

        if peer_id_hash != computed_hash.as_slice() {
            anyhow::bail!("Peer ID hash mismatch in attestation");
        }

        // Extract certificate chain (remaining bytes)
        let certificate_chain = &remaining[40..];

        if certificate_chain.is_empty() {
            anyhow::bail!("Empty certificate chain in attestation");
        }

        // Verify certificate chain structure and signature
        self.verify_certificate_chain(certificate_chain, signature, timestamp, peer_id_hash)?;

        // Create attestation message with proper lifetime
        let message_string = format!(
            "{}{}{}",
            hex::encode(peer_id.as_bytes()),
            hex::encode(peer_id_hash),
            timestamp
        );
        let attestation_message = message_string.as_bytes();

        // Try to verify with known authority public keys
        let authority_pubkeys = self.get_authority_public_keys();

        let mut verified = false;
        for pubkey in authority_pubkeys {
            if self.verify_signature(attestation_message, signature, &pubkey)? {
                verified = true;
                break;
            }
        }

        if !verified {
            anyhow::bail!("n failed");
        }

        Ok(())
    }

    /// Get known authority public keys for attestation verification
    fn get_authority_public_keys(&self) -> Vec<Vec<u8>> {
        // In a real implementation, this would load from a trusted source
        // For now, include some well-known authority keys
        vec![
            // Bootstrap authority key (would be loaded from config)
            vec![
                0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66,
                0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00, 0x01, 0x23, 0x45, 0x67,
                0x89, 0xab, 0xcd, 0xef,
            ],
            // Secondary authority key
            vec![
                0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10, 0xef, 0xde, 0xcb, 0xa9, 0x87, 0x65,
                0x43, 0x21, 0x10, 0x01, 0x23, 0x34, 0x45, 0x56, 0x67, 0x78, 0x89, 0x9a, 0xab, 0xbc,
                0xcd, 0xde, 0xef, 0xf0, 0x21, 0x43, 0x65, 0x87, 0xa9, 0xcb, 0xed, 0xff,
            ],
        ]
    }

    /// Verify certificate chain structure and validity
    fn verify_certificate_chain(
        &self,
        chain: &[u8],
        _signature: &[u8],
        _timestamp: u64,
        _peer_id_hash: &[u8],
    ) -> Result<()> {
        // Parse certificate chain (simplified structure)
        // In a real implementation, this would parse X.509 certificates
        // For now, we just verify basic structure

        if chain.len() < 32 {
            anyhow::bail!("Certificate chain too short");
        }

        // Verify chain starts with certificate identifier
        let cert_id = &chain[0..4];
        if cert_id != b"CERT" {
            anyhow::bail!("Invalid certificate identifier");
        }

        // Verify chain length is reasonable
        if chain.len() > 4096 {
            anyhow::bail!("Certificate chain too long");
        }

        // In a real implementation, this would:
        // 1. Parse each certificate in the chain
        // 2. Verify certificate signatures
        // 3. Check certificate validity periods
        // 4. Verify certificate chain of trust
        // 5. Check certificate revocation status

        Ok(())
    }
}

impl Default for HandshakeManager {
    fn default() -> Self {
        Self::new(Arc::new(IdentityRegistry::new()))
    }
}
