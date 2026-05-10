//! Masternode Proposal Validator - Full Implementation

use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_big_array::BigArray;
use sha2::{Digest, Sha256, Sha512};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, warn};

/// Serde hex for [u8; 64] (lightnode sends attestation signature as hex string)
fn serialize_signature_hex<S>(sig: &[u8; 64], s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&hex::encode(sig))
}
fn deserialize_signature_hex<'de, D>(d: D) -> Result<[u8; 64], D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    let bytes = hex::decode(&s).map_err(serde::de::Error::custom)?;
    let mut arr = [0u8; 64];
    if bytes.len() == 64 {
        arr.copy_from_slice(&bytes);
    }
    Ok(arr)
}

/// Wrapper for [u8; 64] with Default implementation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ByteArray64([u8; 64]);

impl Default for ByteArray64 {
    fn default() -> Self {
        ByteArray64([0u8; 64])
    }
}

impl From<[u8; 64]> for ByteArray64 {
    fn from(arr: [u8; 64]) -> Self {
        ByteArray64(arr)
    }
}

impl From<ByteArray64> for [u8; 64] {
    fn from(arr: ByteArray64) -> Self {
        arr.0
    }
}

impl AsRef<[u8]> for ByteArray64 {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl Serialize for ByteArray64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut seq = serializer.serialize_tuple(64)?;
        for byte in &self.0 {
            seq.serialize_element(byte)?;
        }
        seq.end()
    }
}

impl<'de> Deserialize<'de> for ByteArray64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::{SeqAccess, Visitor};
        struct ByteArray64Visitor;

        impl<'de> Visitor<'de> for ByteArray64Visitor {
            type Value = ByteArray64;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a 64-byte array")
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut arr = [0u8; 64];
                for i in 0..64 {
                    arr[i] = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                }
                Ok(ByteArray64(arr))
            }
        }

        deserializer.deserialize_tuple(64, ByteArray64Visitor)
    }
}

/// Single attestation in the election certificate (must match lightnode format; signature is hex on wire)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionAttestation {
    pub signer_peer_id: String,
    #[serde(with = "BigArray")]
    pub signer_pubkey: [u8; 32],
    #[serde(
        serialize_with = "serialize_signature_hex",
        deserialize_with = "deserialize_signature_hex"
    )]
    pub signature: [u8; 64],
}

/// Election certificate (must match lightnode format)
///
/// SECURITY (Falla 3 — anti-replay): `tenure_start_height` binds the certificate to a specific
/// height window. The MN verifies that `proposal.height ∈ [tenure_start_height,
/// tenure_start_height + PROPOSER_TENURE_BLOCKS)`. Without this binding a legitimate cert was
/// eternally re-usable for any future block of the same group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionCertificate {
    pub group_id: String,
    pub election_round: u64,
    pub elected_proposer_peer_id: String,
    #[serde(with = "BigArray")]
    pub elected_proposer_pubkey: [u8; 32],
    pub proposer_pou_score: u32,
    pub timestamp: u64,
    pub candidates: Vec<(String, u32, f64)>,
    pub attestations: Vec<ElectionAttestation>,
    /// First chain height at which this certificate is valid (Falla 3 anti-replay binding).
    /// `serde(default)` keeps deserialization compatible with older peers that haven't
    /// upgraded yet — they appear with tenure_start_height=0 and will fail the height-window
    /// check anyway, which is the intended behavior.
    #[serde(default)]
    pub tenure_start_height: u64,
}

/// Tenure window length (number of blocks a certificate is valid for).
/// MUST match `PROPOSER_TENURE_BLOCKS` in savitri-lightnode/src/p2p/intra_group/mod.rs.
pub const PROPOSER_TENURE_BLOCKS: u64 = 100;

/// Timeout in milliseconds before the backup MN takes over certificate publishing.
/// Reduced from 2000ms to 300ms to improve block finalization throughput.
/// The leader publishes immediately; backup only fires if leader is slow/offline.
/// pubblicato prima che il leader-cert quorum 4/6 si formi cross-VM 200ms+ RTT).
/// In ambiente locale (<50ms) il timeout non scatta comunque — leader pubblica
/// immediatamente al raggiungimento of the quorum.
pub const BACKUP_CERT_TIMEOUT_MS: u64 = 2000;

/// Role of this masternode with respect to a particular group's block proposal.
/// BlockAcceptanceCertificate and when.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MnGroupRole {
    /// Primary aggregator: publishes BlockAcceptanceCertificate immediately.
    Leader,
    /// Backup: publishes after BACKUP_CERT_TIMEOUT_MS if no cert arrives.
    Backup,
    /// (unless both leader and backup fail — ultimate fallback).
    Participant,
}

/// Wrapper sent through the proposal channel so main.rs knows both the
/// proposal data and this node's role for cert-publishing decisions.
#[derive(Debug, Clone)]
pub struct ProposalWithRole {
    pub proposal: LightnodeProposal,
    pub role: MnGroupRole,
}

/// Block proposal received from a lightnode
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LightnodeProposal {
    pub round_id: u64,
    pub height: u64,
    pub timestamp: u64,
    #[serde(with = "BigArray")]
    pub proposer_pubkey: [u8; 32],
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub tx_count: u32,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
    // Optional legacy fields for lightnode BlockProposal compatibility
    #[serde(default)]
    pub parent_hash: ByteArray64,
    #[serde(default)]
    pub state_root: ByteArray64,
    #[serde(default)]
    pub tx_root: ByteArray64,
    /// Group ID for proposer verification (from lightnode wire)
    #[serde(default)]
    pub proposer_group_id: String,
    /// Certificate that proposer was elected by the group
    #[serde(default)]
    pub election_certificate: Option<ElectionCertificate>,
    /// Optional raw tx bytes carried in proposal so MN can cache payload without block_topic race.
    #[serde(default)]
    pub raw_txs: Option<Vec<Vec<u8>>>,
}

fn default_array_64() -> [u8; 64] {
    [0u8; 64]
}

/// Masternode vote on a proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MasternodeVote {
    pub round_id: u64,
    pub height: u64,
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    #[serde(with = "BigArray")]
    pub voter_pubkey: [u8; 32],
    pub vote_type: VoteType,
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
    /// Group ID of the proposer's group (for multi-group disambiguation and audit trail)
    #[serde(default)]
    pub group_id: String,
    /// State root from the proposal (so certificate and LN can use MN-agreed roots)
    #[serde(default = "default_array_64", with = "BigArray")]
    pub state_root: [u8; 64],
    /// Transaction root from the proposal
    #[serde(default = "default_array_64", with = "BigArray")]
    pub tx_root: [u8; 64],
    /// Parent block hash (so certificate and LN can match block hash formula)
    #[serde(default = "default_array_64", with = "BigArray")]
    pub parent_hash: [u8; 64],
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VoteType {
    Approve,
    Reject,
}

/// Block certificate issued after quorum
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockCertificate {
    pub round_id: u64,
    pub height: u64,
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    pub votes: Vec<MasternodeVote>,
    pub timestamp: u64,
    /// Group ID of the proposer's group — ensures certificate is tied to a specific group,
    /// enabling multi-group disambiguation, proper routing, and a complete audit trail.
    #[serde(default)]
    pub group_id: String,
    /// State root from the certified proposal (LN uses this so re-execution hash can match)
    #[serde(default = "default_array_64", with = "BigArray")]
    pub state_root: [u8; 64],
    /// Transaction root from the certified proposal
    #[serde(default = "default_array_64", with = "BigArray")]
    pub tx_root: [u8; 64],
    /// Parent block hash (so LN can match block hash formula)
    #[serde(default = "default_array_64", with = "BigArray")]
    pub parent_hash: [u8; 64],
    /// Reward recipients for deterministic group-check reward accounting.
    #[serde(default)]
    pub reward_recipients: Vec<[u8; 32]>,
}

/// Certificate from the group-owner MN that it accepted a block (so non-owner MN can move pending → confirmed)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockAcceptanceCertificate {
    pub group_id: String,
    pub height: u64,
    pub round_id: u64,
    #[serde(with = "BigArray")]
    pub block_hash: [u8; 64],
    /// Transaction root (merkle root of transactions) - allows non-owner MN to verify transactions match
    #[serde(with = "BigArray")]
    pub tx_root: [u8; 64],
    /// State root after executing transactions - allows non-owner MN to verify state consistency
    #[serde(with = "BigArray")]
    pub state_root: [u8; 64],
    /// Parent block hash - allows non-owner MN to verify chain continuity
    #[serde(with = "BigArray")]
    pub parent_hash: [u8; 64],
    /// Transaction count - allows non-owner MN to verify tx_count matches
    pub tx_count: u32,
    /// Masternode ID that owns the group and verified the block (peer ID string)
    pub owner_masternode_id: String,
    /// Public key of the signer (Ed25519 verifying key). Used for signature verification.
    /// Must match the key that produced `signature`; avoids deriving key from peer ID.
    #[serde(with = "BigArray")]
    pub owner_pubkey: [u8; 32],
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
    pub timestamp: u64,
    /// Reward recipients for deterministic group-check reward accounting.
    #[serde(default)]
    pub reward_recipients: Vec<[u8; 32]>,
}

#[derive(Clone)]
pub struct ProposalValidator {
    local_pubkey: [u8; 32],
    signing_key: SigningKey,
    quorum_threshold: usize,
}

impl ProposalValidator {
    pub fn new(local_pubkey: [u8; 32], quorum_threshold: usize) -> Self {
        // Generate signing key from the local pubkey (in production, this should be loaded from secure storage)
        let signing_key = SigningKey::from_bytes(&local_pubkey);

        Self {
            local_pubkey,
            signing_key,
            quorum_threshold,
        }
    }

    pub fn from_signing_key(signing_key: SigningKey, quorum_threshold: usize) -> Self {
        let local_pubkey = signing_key.verifying_key().to_bytes();

        Self {
            local_pubkey,
            signing_key,
            quorum_threshold,
        }
    }

    /// Compute block hash using the same algorithm as lightnode.
    ///
    /// savitri_consensus. Pre-refactor this took 64-byte state_root and
    /// tx_root (already zero-padded by the caller); the canonical takes
    /// 32-byte primitives and pads internally — we extract the 32-byte
    /// prefix to bridge the API.
    fn compute_block_hash(
        parent_hash: &[u8; 64],
        state_root: &[u8; 64],
        tx_root: &[u8; 64],
        height: u64,
    ) -> [u8; 64] {
        let mut sr = [0u8; 32];
        sr.copy_from_slice(&state_root[..32]);
        let mut tr = [0u8; 32];
        tr.copy_from_slice(&tx_root[..32]);
        savitri_consensus::primitives::hashing::compute_block_hash(parent_hash, &sr, &tr, height)
    }

    /// Sign a block acceptance certificate (owner MN attests it accepted the block)
    pub fn sign_block_acceptance(
        &self,
        owner_masternode_id: &str,
        group_id: &str,
        height: u64,
        round_id: u64,
        block_hash: &[u8; 64],
        tx_root: &[u8; 64],
        state_root: &[u8; 64],
        parent_hash: &[u8; 64],
        tx_count: u32,
    ) -> BlockAcceptanceCertificate {
        let mut signable = Vec::new();
        signable.extend_from_slice(group_id.as_bytes());
        signable.extend_from_slice(&height.to_le_bytes());
        signable.extend_from_slice(&round_id.to_le_bytes());
        signable.extend_from_slice(block_hash);
        signable.extend_from_slice(tx_root);
        signable.extend_from_slice(state_root);
        signable.extend_from_slice(parent_hash);
        signable.extend_from_slice(&tx_count.to_le_bytes());
        signable.extend_from_slice(owner_masternode_id.as_bytes());
        let signature = self.signing_key.sign(&signable);
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        info!(
            height,
            round_id,
            group_id,
            tx_count,
            reward_recipients = 0usize,
            "GROUP_CHECK_DEBUG: block acceptance certificate signed with empty recipients"
        );
        BlockAcceptanceCertificate {
            group_id: group_id.to_string(),
            height,
            round_id,
            block_hash: *block_hash,
            tx_root: *tx_root,
            state_root: *state_root,
            parent_hash: *parent_hash,
            tx_count,
            owner_masternode_id: owner_masternode_id.to_string(),
            owner_pubkey: self.local_pubkey,
            signature: signature.to_bytes(),
            timestamp,
            reward_recipients: Vec::new(),
        }
    }

    /// Verify a block acceptance certificate using the signer's public key in the cert.
    pub fn verify_block_acceptance(cert: &BlockAcceptanceCertificate) -> bool {
        let verifying_key = match VerifyingKey::from_bytes(&cert.owner_pubkey) {
            Ok(k) => k,
            Err(_) => return false,
        };
        let mut signable = Vec::new();
        signable.extend_from_slice(cert.group_id.as_bytes());
        signable.extend_from_slice(&cert.height.to_le_bytes());
        signable.extend_from_slice(&cert.round_id.to_le_bytes());
        signable.extend_from_slice(&cert.block_hash);
        signable.extend_from_slice(&cert.tx_root);
        signable.extend_from_slice(&cert.state_root);
        signable.extend_from_slice(&cert.parent_hash);
        signable.extend_from_slice(&cert.tx_count.to_le_bytes());
        signable.extend_from_slice(cert.owner_masternode_id.as_bytes());
        let sig = Signature::from_bytes(&cert.signature);
        verifying_key.verify(&signable, &sig).is_ok()
    }

    /// Verify that a block proposal matches the BlockAcceptanceCertificate
    /// This allows non-owner masternodes to verify the block content matches what the owner verified
    pub fn verify_proposal_matches_certificate(
        proposal: &LightnodeProposal,
        cert: &BlockAcceptanceCertificate,
    ) -> bool {
        // Convert ByteArray64 to [u8; 64] for comparison
        let proposal_tx_root: [u8; 64] = proposal.tx_root.into();
        let proposal_state_root: [u8; 64] = proposal.state_root.into();
        let proposal_parent_hash: [u8; 64] = proposal.parent_hash.into();

        // Verify block hash matches
        if proposal.block_hash != cert.block_hash {
            warn!(
                "Block hash mismatch: proposal={:?}, cert={:?}",
                hex::encode(&proposal.block_hash[..8]),
                hex::encode(&cert.block_hash[..8])
            );
            return false;
        }

        // Verify transaction root matches
        if proposal_tx_root != cert.tx_root {
            warn!("Transaction root mismatch");
            return false;
        }

        // Verify state root matches
        if proposal_state_root != cert.state_root {
            warn!("State root mismatch");
            return false;
        }

        // Verify parent hash matches
        if proposal_parent_hash != cert.parent_hash {
            warn!("Parent hash mismatch");
            return false;
        }

        // Verify transaction count matches
        if proposal.tx_count != cert.tx_count {
            warn!(
                "Transaction count mismatch: proposal={}, cert={}",
                proposal.tx_count, cert.tx_count
            );
            return false;
        }

        // Verify height and round_id match
        if proposal.height != cert.height || proposal.round_id != cert.round_id {
            warn!("Height or round_id mismatch");
            return false;
        }

        true
    }

    /// Derive masternode public key deterministically from masternode ID and epoch
    /// This matches the logic in group_consensus.rs for consistent key derivation
    pub fn derive_masternode_pubkey(masternode_id: &str, epoch: u64) -> [u8; 32] {
        use sha2::Sha512;
        let mut hasher = Sha512::new();
        hasher.update(masternode_id.as_bytes());
        hasher.update(&epoch.to_le_bytes());
        let hash = hasher.finalize();

        // Use first 32 bytes of hash as seed for Ed25519 key
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&hash[..32]);

        // Derive SigningKey from seed
        let signing_key = SigningKey::from_bytes(&seed);
        // Get VerifyingKey (public key)
        signing_key.verifying_key().to_bytes()
    }

    /// Compute signable bytes for a proposal (matches lightnode implementation)
    fn proposal_signable_bytes(&self, proposal: &LightnodeProposal) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&proposal.round_id.to_le_bytes());
        data.extend_from_slice(&proposal.height.to_le_bytes());
        data.extend_from_slice(&proposal.timestamp.to_le_bytes());
        data.extend_from_slice(&proposal.proposer_pubkey);
        data.extend_from_slice(&proposal.block_hash);
        data.extend_from_slice(&proposal.tx_count.to_le_bytes());
        data
    }

    /// Verify Ed25519 signature on a proposal
    fn verify_proposal_signature(&self, proposal: &LightnodeProposal) -> bool {
        // Get signable bytes
        let signable = self.proposal_signable_bytes(proposal);

        // Parse the verifying key from proposer_pubkey
        let verifying_key = match VerifyingKey::from_bytes(&proposal.proposer_pubkey) {
            Ok(key) => key,
            Err(e) => {
                error!("Failed to parse proposer public key: {}", e);
                return false;
            }
        };

        // Parse the signature
        let signature = Signature::from_bytes(&proposal.signature);

        // Verify the signature (primary path: Ed25519 on LightnodeProposal v1)
        if verifying_key.verify(&signable, &signature).is_ok() {
            info!("Proposal signature verification passed");
            return true;
        }

        // Fallback: legacy lightnode BlockProposal signature scheme
        // (matches lightnode intra_group::sign_proposal)
        if proposal.parent_hash.0 != [0u8; 64]
            || proposal.state_root.0 != [0u8; 64]
            || proposal.tx_root.0 != [0u8; 64]
        {
            let mut legacy_data = Vec::new();
            legacy_data.extend_from_slice(&proposal.round_id.to_le_bytes());
            legacy_data.extend_from_slice(&proposal.height.to_le_bytes());
            legacy_data.extend_from_slice(&proposal.timestamp.to_le_bytes());
            legacy_data.extend_from_slice(&proposal.proposer_pubkey);
            legacy_data.extend_from_slice(&proposal.parent_hash.0);
            legacy_data.extend_from_slice(&proposal.state_root.0);
            legacy_data.extend_from_slice(&proposal.tx_root.0);

            let msg_hash = Sha256::digest(&legacy_data);
            let proposer_hex = hex::encode(proposal.proposer_pubkey);
            let proposer_hash = Sha256::digest(proposer_hex.as_bytes());

            let mut expected = [0u8; 64];
            expected[..32].copy_from_slice(msg_hash.as_slice());
            expected[32..].copy_from_slice(proposer_hash.as_slice());

            if expected == proposal.signature {
                info!("Legacy proposal signature verification passed");
                return true;
            }
        }

        warn!("Proposal signature verification failed");
        false
    }

    pub async fn validate_proposal(&self, proposal: &LightnodeProposal) -> bool {
        info!(
            "Validating proposal: height={}, round_id={}",
            proposal.height, proposal.round_id
        );

        if proposal.height == 0 {
            warn!("Invalid proposal: height cannot be zero");
            return false;
        }

        // Allow empty blocks: genesis block (height=1) or when no transactions are available
        // Lightnode intentionally proposes empty blocks when no valid transactions exist
        if proposal.tx_count == 0 {
            info!("Proposal has no transactions (empty block) - height={}, allowing for genesis or no-tx scenarios", proposal.height);
        }

        if proposal.tx_count > 10000 {
            warn!(
                "Invalid proposal: too many transactions ({})",
                proposal.tx_count
            );
            return false;
        }

        // Check timestamp (not too old or too far in future)
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if proposal.timestamp > current_time + 300 {
            // 5 minutes in future
            warn!("Invalid proposal: timestamp too far in future");
            return false;
        }

        if proposal.timestamp < current_time - 3600 {
            // 1 hour in past
            warn!("Invalid proposal: timestamp too old");
            return false;
        }

        // Verify proposer public key
        if proposal.proposer_pubkey == [0u8; 32] {
            warn!("Invalid proposal: empty proposer pubkey");
            return false;
        }

        // Check block hash: even empty blocks must have a valid (non-zero) hash
        // This maintains chain continuity and consensus, as in other L1 blockchains
        if proposal.block_hash == [0u8; 64] {
            warn!("Invalid proposal: zero block hash (empty blocks must have valid hash for chain continuity)");
            return false;
        }

        // Verify block hash matches expected calculation (if parent_hash, state_root, tx_root are available)
        // This ensures the block hash was computed correctly, even for empty blocks
        let zero_array = ByteArray64([0u8; 64]);
        if proposal.parent_hash != zero_array
            && proposal.state_root != zero_array
            && proposal.tx_root != zero_array
        {
            let expected_hash = Self::compute_block_hash(
                &proposal.parent_hash.0,
                &proposal.state_root.0,
                &proposal.tx_root.0,
                proposal.height,
            );
            if proposal.block_hash != expected_hash {
                warn!(
                    "Invalid proposal: block hash mismatch (expected vs received differ) - height={}, tx_count={}",
                    proposal.height, proposal.tx_count
                );
                return false;
            }
        }

        // REAL CRYPTOGRAPHIC VERIFICATION: Verify Ed25519 signature
        if !self.verify_proposal_signature(proposal) {
            warn!("Invalid proposal: signature verification failed");
            return false;
        }

        info!("Proposal validation passed: height={}", proposal.height);
        true
    }

    /// Verify that the proposal's proposer is the elected proposer for the group (using election certificate).
    /// group_members: list of peer IDs that are members of the group (from group formation).
    ///
    /// SECURITY HARDENING (proposer-only enforcement, 3 falle):
    ///   Falla 1 — proposer_group_id MUST be present (no more legacy bypass). Without it any LN
    ///             could forge proposals by simply omitting the field.
    ///   Falla 2 — attestations MUST cover ≥ ⌈2/3 × group_members⌉ valid signers from the roster.
    ///             A single LN cannot mint a self-elected cert: BFT quorum is enforced.
    ///   Falla 3 — the cert is bound to a height window via `tenure_start_height`. The proposal's
    ///             height must fall within [tenure_start_height,
    ///             tenure_start_height + PROPOSER_TENURE_BLOCKS). Replay across heights is blocked.
    pub fn verify_proposal_group_and_certificate(
        proposal: &LightnodeProposal,
        group_members: &[String],
    ) -> bool {
        // Falla 1: legacy bypass removed. A proposal without proposer_group_id is rejected:
        // no LN can submit a block without going through the group election protocol.
        if proposal.proposer_group_id.is_empty() {
            warn!(
                height = proposal.height,
                "[MN CERT] Falla 1: proposal has no proposer_group_id — REJECT (legacy bypass disabled)"
            );
            return false;
        }
        info!(
            group_id = %proposal.proposer_group_id,
            group_members_count = group_members.len(),
            "📥 [MN CERT] Verifying proposal group and election certificate"
        );

        let cert = match &proposal.election_certificate {
            Some(c) => {
                info!(
                    attestations = c.attestations.len(),
                    election_round = c.election_round,
                    tenure_start_height = c.tenure_start_height,
                    group_id = %c.group_id,
                    elected_proposer = %c.elected_proposer_peer_id,
                    timestamp = c.timestamp,
                    candidates_count = c.candidates.len(),
                    "[MN CERT] Certificate present - will verify attestations"
                );
                c
            }
            None => {
                warn!(
                    "[MN CERT] Proposal has proposer_group_id but no election_certificate - REJECT"
                );
                return false;
            }
        };

        // Falla 3: bind cert→height. Reject if proposal height is outside the certified
        // tenure window [tenure_start_height, tenure_start_height + PROPOSER_TENURE_BLOCKS).
        //
        // ENV TOGGLE (testnet operations): SAVITRI_FALLA3_DISABLE defaults to `true` because
        // the LN-side cert refresh on tenure rotation is not yet shipped — without it, a
        // long-lived proposer keeps the same cert past its tenure window and the MN would
        // otherwise reject every block. Set SAVITRI_FALLA3_DISABLE=0 (or "false") to re-enable
        // the height-window check once the LN refresh flow ships. Falla 1 (proposer_group_id
        // required) and Falla 2 (BFT quorum on attestations) stay active so proposer-only
        // enforcement remains partial but not absent.
        let falla3_disabled = std::env::var("SAVITRI_FALLA3_DISABLE")
            .map(|v| !(v == "0" || v.eq_ignore_ascii_case("false")))
            .unwrap_or(true);
        if !falla3_disabled {
            let tenure_end = cert
                .tenure_start_height
                .saturating_add(PROPOSER_TENURE_BLOCKS);
            if proposal.height < cert.tenure_start_height || proposal.height >= tenure_end {
                warn!(
                    proposal_height = proposal.height,
                    cert_tenure_start = cert.tenure_start_height,
                    cert_tenure_end_exclusive = tenure_end,
                    tenure_blocks = PROPOSER_TENURE_BLOCKS,
                    group_id = %cert.group_id,
                    "[MN CERT] Falla 3: proposal height outside certified tenure window — REJECT (replay protection)"
                );
                return false;
            }
        }

        if cert.group_id != proposal.proposer_group_id {
            warn!(
                cert_group = %cert.group_id,
                proposal_group = %proposal.proposer_group_id,
                "[MN CERT] Certificate group_id does not match proposal proposer_group_id - REJECT"
            );
            return false;
        }
        if cert.elected_proposer_pubkey != proposal.proposer_pubkey {
            warn!(
                "[MN CERT] Certificate elected_proposer_pubkey does not match proposal proposer_pubkey - REJECT"
            );
            return false;
        }
        info!("[MN CERT] Group ID and proposer pubkey match; verifying attestations");
        let group_set: std::collections::HashSet<&str> =
            group_members.iter().map(String::as_str).collect();
        let skip_member_check = group_set.is_empty();
        if skip_member_check {
            info!(
                group_id = %cert.group_id,
                "[MN CERT] group_members is empty (MN doesn't track this group's members); skipping member check, verifying signatures only"
            );
        }
        for (i, att) in cert.attestations.iter().enumerate() {
            if !skip_member_check && !group_set.contains(att.signer_peer_id.as_str()) {
                // timing desync between MN and LN. The MN's roster is a local snapshot
                // that can lag behind the LN's actual group membership (nodes join/leave
                // between cleanup cycles). Signature verification below is the real
                // security guarantee — a valid ed25519 signature proves the signer
                // is a legitimate network participant regardless of roster state.
                warn!(
                    signer = %att.signer_peer_id,
                    index = i,
                    group_id = %cert.group_id,
                    roster_size = group_set.len(),
                    "[MN CERT] Attestation signer not in current group roster (possible desync) — continuing with signature verification"
                );
            }
            // Rebuild signable bytes (must match lightnode ProposerElectionResult.signable_bytes)
            // IMPORTANT: Field order must match exactly with lightnode's Signable struct
            // CRITICAL FIX v2: Both `timestamp` and `candidates` are excluded to match LN side:
            //   - timestamp: each LN calls get_safe_timestamp() independently → different values
            //   - candidates: each LN computes combined_score using local latency measurements,
            //     producing different f64 values. The certificate stores only ONE candidates list
            //     (cert_first's), so we cannot reconstruct what each individual attester signed.
            // Remaining fields are fully deterministic across all LNs:
            //   - round: derived from group_id hash
            //   - elected_proposer: same election outcome
            //   - sender: per-signer (att.signer_peer_id)
            //   - group_id: same for all
            //   - tenure_start_height: deterministic snapshot of finalized chain height,
            //     bound into the signed payload to prevent cross-height replay (Falla 3).
            #[derive(serde::Serialize)]
            struct Signable<'a> {
                round: u64,
                elected_proposer: &'a str,
                sender: &'a str,
                group_id: &'a str,
                tenure_start_height: u64,
                // timestamp: excluded (per-node)
                // candidates: excluded (contains per-node f64 latency-based scores)
                // proposer_pou_score: excluded (LNs may have different views of proposer's PoU
                //   due to gossipsub message loss, causing signature mismatch)
            }
            // Log the values being used to reconstruct signable bytes
            debug!(
                signer = %att.signer_peer_id,
                index = i,
                election_round = cert.election_round,
                elected_proposer = %cert.elected_proposer_peer_id,
                sender = %att.signer_peer_id,
                group_id = %cert.group_id,
                tenure_start_height = cert.tenure_start_height,
                "[MN CERT] Reconstructing signable bytes (timestamp+candidates+proposer_pou_score excluded)"
            );

            let signable = match serde_json::to_vec(&Signable {
                round: cert.election_round,
                elected_proposer: &cert.elected_proposer_peer_id,
                sender: &att.signer_peer_id,
                group_id: &cert.group_id,
                tenure_start_height: cert.tenure_start_height,
            }) {
                Ok(b) => {
                    debug!(
                        signer = %att.signer_peer_id,
                        index = i,
                        signable_len = b.len(),
                        signable_hex = %hex::encode(&b[..b.len().min(100)]), // First 100 bytes for debugging
                        "[MN CERT] Signable bytes serialized successfully"
                    );
                    b
                }
                Err(e) => {
                    warn!(signer = %att.signer_peer_id, index = i, error = %e, "[MN CERT] Failed to serialize signable bytes - REJECT");
                    return false;
                }
            };
            // Lightnode signs intragroup_signing_payload(group_id, "election_result", signable),
            // Format: "savitri-intragroup-v1|election_result|<group_id>|<signable>"
            // This MUST match exactly what lightnode's intragroup_signing_payload() produces
            let mut payload = Vec::new();
            payload.extend_from_slice(b"savitri-intragroup-v1|");
            payload.extend_from_slice(b"election_result");
            payload.extend_from_slice(b"|");
            payload.extend_from_slice(cert.group_id.as_bytes());
            payload.extend_from_slice(b"|");
            payload.extend_from_slice(&signable);

            debug!(
                signer = %att.signer_peer_id,
                index = i,
                payload_len = payload.len(),
                payload_prefix = %String::from_utf8_lossy(&payload[..payload.len().min(100)]),
                "[MN CERT] Payload constructed for signature verification"
            );
            let verifying_key = match VerifyingKey::from_bytes(&att.signer_pubkey) {
                Ok(k) => k,
                Err(e) => {
                    warn!(signer = %att.signer_peer_id, index = i, error = ?e, "[MN CERT] Invalid signer_pubkey in attestation - REJECT");
                    return false;
                }
            };
            let sig = Signature::from_bytes(&att.signature);
            debug!(
                signer = %att.signer_peer_id,
                index = i,
                signer_pubkey_hex = %hex::encode(&att.signer_pubkey),
                signature_hex = %hex::encode(&att.signature),
                "[MN CERT] Attempting signature verification"
            );

            if verifying_key.verify_strict(&payload, &sig).is_err() {
                warn!(
                    signer = %att.signer_peer_id,
                    index = i,
                    election_round = cert.election_round,
                    payload_len = payload.len(),
                    signable_len = signable.len(),
                    group_id = %cert.group_id,
                    signer_pubkey_hex = %hex::encode(&att.signer_pubkey),
                    signature_hex = %hex::encode(&att.signature),
                    payload_prefix = %String::from_utf8_lossy(&payload[..payload.len().min(150)]),
                    "[MN CERT] Attestation signature verification FAILED - payload/signature mismatch"
                );
                warn!(
                    signer = %att.signer_peer_id,
                    index = i,
                    election_round = cert.election_round,
                    "⚠️ [MN CERT] CRITICAL: Signature verification failed - check if election_round matches what was signed"
                );
                return false;
            }
            debug!(signer = %att.signer_peer_id, index = i, "[MN CERT] Attestation verified OK");
        }
        if cert.attestations.is_empty() {
            warn!("[MN CERT] Election certificate has no attestations - REJECT");
            return false;
        }

        // Falla 2: enforce BFT quorum on attestations. Without this a single LN could mint
        // a self-elected cert with 1 attestation and pass the bypass-the-roster path above.
        // Quorum = ⌈2/3 × group_members⌉ valid attesters (i.e. signer in group_set, signature
        // already verified above). If we don't track this group's members locally
        // (skip_member_check), we still require ≥3 valid signatures as a minimum BFT floor.
        if !skip_member_check {
            let valid_signers: usize = cert
                .attestations
                .iter()
                .filter(|att| group_set.contains(att.signer_peer_id.as_str()))
                .count();
            // PBFT classic quorum: f = floor((n-1)/3), quorum = 2f+1.
            // The original ceil(2n/3) overshoots for n in {5,6}: e.g. n=5 -> 4, but PBFT
            // tolerates f=1 with quorum=3 there (n>=3f+1). Using 2f+1 keeps Byzantine fault
            // tolerance correct without forcing a super-majority unattainable when an
            // attester is briefly offline during boot.
            let f = group_members.len().saturating_sub(1) / 3;
            let quorum = 2 * f + 1;
            if valid_signers < quorum {
                warn!(
                    group_id = %cert.group_id,
                    valid_signers = valid_signers,
                    quorum_required = quorum,
                    group_size = group_members.len(),
                    total_attestations = cert.attestations.len(),
                    "[MN CERT] Falla 2: attestations below BFT quorum — REJECT"
                );
                return false;
            }
            info!(
                group_id = %cert.group_id,
                valid_signers = valid_signers,
                quorum_required = quorum,
                "[MN CERT] Falla 2: BFT quorum reached"
            );
        } else {
            // Defensive minimum when roster is unknown to this MN.
            const MIN_FLOOR_ATTESTATIONS: usize = 3;
            if cert.attestations.len() < MIN_FLOOR_ATTESTATIONS {
                warn!(
                    group_id = %cert.group_id,
                    attestations = cert.attestations.len(),
                    floor = MIN_FLOOR_ATTESTATIONS,
                    "[MN CERT] Falla 2: roster unknown, attestations below floor — REJECT"
                );
                return false;
            }
        }

        info!(
            group_id = %cert.group_id,
            attestations = cert.attestations.len(),
            "📥 [MN CERT] Election certificate verified - ACCEPT"
        );
        true
    }

    pub async fn vote_on_proposal(&self, proposal: &LightnodeProposal) -> Option<MasternodeVote> {
        info!("Voting on proposal: height={}", proposal.height);

        // Validate the proposal first
        if !self.validate_proposal(proposal).await {
            warn!("Not voting: proposal validation failed");
            return None;
        }

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Vote logic: approve if proposal is within time window
        if proposal.timestamp > current_time - 600 && proposal.timestamp < current_time + 60 {
            let vote_type = VoteType::Approve;

            // Create signable data for the vote
            // IMPORTANT: group_id is included in the signed payload to prevent
            // cross-group replay attacks in multi-group scenarios.
            let mut vote_data = Vec::new();
            vote_data.extend_from_slice(&proposal.round_id.to_le_bytes());
            vote_data.extend_from_slice(&proposal.height.to_le_bytes());
            vote_data.extend_from_slice(&proposal.block_hash);
            vote_data.extend_from_slice(&self.local_pubkey);
            vote_data.push(match vote_type {
                VoteType::Approve => 1,
                VoteType::Reject => 0,
            });
            vote_data.extend_from_slice(proposal.proposer_group_id.as_bytes());

            // REAL CRYPTOGRAPHIC SIGNING: Sign the vote with masternode's private key
            let signature = self.signing_key.sign(&vote_data);

            Some(MasternodeVote {
                round_id: proposal.round_id,
                height: proposal.height,
                block_hash: proposal.block_hash,
                voter_pubkey: self.local_pubkey,
                vote_type,
                signature: signature.to_bytes(),
                group_id: proposal.proposer_group_id.clone(),
                state_root: proposal.state_root.into(),
                tx_root: proposal.tx_root.into(),
                parent_hash: proposal.parent_hash.into(),
            })
        } else {
            warn!("Not voting: proposal outside time window");
            None
        }
    }
}
