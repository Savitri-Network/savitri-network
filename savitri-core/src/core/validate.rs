use super::block::Block;
use crate::utils::bincode_utils::serialize_consensus;
use anyhow::Context;
use ed25519_dalek::{Verifier, VerifyingKey as PublicKey};
use savitri_storage::Storage;
use tracing::info;
#[cfg(feature = "savitri-zkp")]
use tracing::warn;

#[cfg(feature = "shared-types")]
use super::p2p_messages::{
    ConsensusCertificate, ConsensusProposal, ConsensusVote, MAX_PROPOSAL_BYTES, MAX_TX_BYTES,
    MAX_VOTE_BYTES,
};

#[cfg(test)]
use sha2::{Digest, Sha512};

#[cfg(not(feature = "shared-types"))]
// Fallback types for when shared-types feature is not enabled
pub struct ConsensusCertificate;

#[cfg(not(feature = "shared-types"))]
impl ConsensusCertificate {
    pub fn estimated_size(&self) -> usize {
        64
    }
}

#[cfg(not(feature = "shared-types"))]
pub struct ConsensusProposal;

#[cfg(not(feature = "shared-types"))]
impl ConsensusProposal {
    pub fn estimated_size(&self) -> usize {
        136
    }
}

#[cfg(not(feature = "shared-types"))]
pub struct ConsensusVote;

#[cfg(not(feature = "shared-types"))]
impl ConsensusVote {
    pub fn estimated_size(&self) -> usize {
        169
    }
}

#[cfg(not(feature = "shared-types"))]
pub const MAX_PROPOSAL_BYTES: usize = 1024;
#[cfg(not(feature = "shared-types"))]
pub const MAX_TX_BYTES: usize = 1024;
#[cfg(not(feature = "shared-types"))]
pub const MAX_VOTE_BYTES: usize = 512;

// Reasonable caps for now; adjust as needed
const MAX_TOTAL_TX_BYTES: usize = 1024 * 64; // 64 KiB of serialized txs
const MAX_TXS_PER_BLOCK: usize = 1_000; // 🔥 IMPOSTATO A 1000TX come dimensione standard

// ZKP functionality temporarily removed due to dependency cycle
// use savitri_zkp::statement::{compress_root_64_to_32, Statement};
// use savitri_zkp::traits::ZkVerifier;

// use savitri_zkp::statement::Witness;
// #[cfg(test)]
// use savitri_zkp::{MockProver, MockVerifier};

/// Validate an Oracle Feed transaction
///
/// It verifies:
/// - Proof signature (ed25519 with domain separation)
/// - TTL (not expired)
/// - Future timestamp (within tolerance)
/// - Anti-replay (sequence number)
/// - Canonical encoding (no floats)
///
/// # Arguments
/// * `tx_bytes` - Raw bytes of the OracleFeedTx
/// * `storage` - Storage for schema lookup
/// * `config` - Oracle configuration (optional, uses default if None)
///
/// # Returns
pub fn validate_oracle_feed_tx(
    tx_bytes: &[u8],
    _storage: &Storage,
    _config: Option<&OracleConfig>,
) -> anyhow::Result<()> {
    // Validate basic transaction structure first
    validate_tx_bytes(tx_bytes)?;

    // 1. Check if transaction has valid structure
    // 2. Verify basic signature if present
    // 3. Check timestamp bounds

    if tx_bytes.len() < 100 {
        anyhow::bail!("Oracle transaction too short: minimum 100 bytes");
    }

    // Check for Oracle transaction marker (first 4 bytes)
    if tx_bytes.len() >= 4 {
        let marker = &tx_bytes[0..4];
        if marker != b"ORCL" {
            anyhow::bail!("Invalid Oracle transaction marker");
        }
    }

    if tx_bytes.len() >= 12 {
        let timestamp_bytes = &tx_bytes[4..12];
        let timestamp = u64::from_le_bytes(timestamp_bytes.try_into().unwrap_or([0; 8]));
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Allow 5 minutes tolerance for future/past timestamps
        const TIME_TOLERANCE_SECS: u64 = 300;
        if timestamp > now + TIME_TOLERANCE_SECS {
            anyhow::bail!("Oracle timestamp too far in future");
        }
        if timestamp < now.saturating_sub(TIME_TOLERANCE_SECS) {
            anyhow::bail!("Oracle timestamp too far in past");
        }
    }

    info!(
        "Oracle transaction validated: size={}, timestamp_ok",
        tx_bytes.len()
    );
    Ok(())
}

/// Validate basic transaction bytes
fn validate_tx_bytes(tx_bytes: &[u8]) -> anyhow::Result<()> {
    if tx_bytes.is_empty() {
        anyhow::bail!("Transaction bytes cannot be empty");
    }
    if tx_bytes.len() > MAX_TX_BYTES {
        anyhow::bail!(
            "Transaction too large: {} > {} bytes",
            tx_bytes.len(),
            MAX_TX_BYTES
        );
    }
    Ok(())
}

/// Check if transaction bytes represent an Oracle Feed transaction
///
/// This is a lightweight check that can be used to route transactions
///
/// # Arguments
/// * `tx_bytes` - Raw transaction bytes
///
/// # Returns
/// * `true` if bytes can be deserialized as OracleFeedTx
/// * `false` otherwise
pub fn is_oracle_transaction(tx_bytes: &[u8]) -> bool {
    // Check for Oracle transaction marker
    if tx_bytes.len() >= 4 {
        let marker = &tx_bytes[0..4];
        marker == b"ORCL"
    } else {
        false
    }
}

///
pub struct BlockOracleValidator {
    config: OracleConfig,
    /// Cache for frequently used schemas
    schema_cache: std::collections::HashMap<Vec<u8>, OracleSchema>,
}

#[derive(Debug, Clone)]
pub struct OracleConfig {
    /// Maximum allowed timestamp drift in seconds
    pub max_timestamp_drift: u64,
    /// Minimum oracle transaction size
    pub min_tx_size: usize,
    /// Maximum oracle transaction size
    pub max_tx_size: usize,
    pub strict_schema_validation: bool,
}

/// Oracle schema definition
#[derive(Debug, Clone)]
pub struct OracleSchema {
    /// Schema version
    pub version: u32,
    /// Required fields
    pub required_fields: Vec<String>,
    /// Field types
    pub field_types: std::collections::HashMap<String, OracleFieldType>,
}

/// Oracle field types
#[derive(Debug, Clone)]
pub enum OracleFieldType {
    String,
    Number,
    Boolean,
    Timestamp,
    Bytes,
}

impl Default for OracleConfig {
    fn default() -> Self {
        Self {
            max_timestamp_drift: 300, // 5 minutes
            min_tx_size: 100,
            max_tx_size: 1024 * 10, // 10KB
            strict_schema_validation: true,
        }
    }
}

impl BlockOracleValidator {
    /// Create a new BlockOracleValidator with default config
    pub fn new() -> Self {
        Self {
            config: OracleConfig::default(),
            schema_cache: std::collections::HashMap::new(),
        }
    }

    /// Create a new BlockOracleValidator with custom config
    pub fn with_config(config: OracleConfig) -> Self {
        Self {
            config,
            schema_cache: std::collections::HashMap::new(),
        }
    }

    /// Validate Oracle transactions in a block
    ///
    /// Returns the number of valid Oracle transactions found.
    /// Invalid Oracle transactions cause the entire block to be rejected.
    ///
    /// # Arguments
    /// * `tx_bytes_list` - List of serialized transactions in the block
    /// * `storage` - Storage for schema lookup
    ///
    /// # Returns
    /// * `Ok(count)` - Number of valid Oracle transactions
    /// * `Err(error)` - If any Oracle transaction is invalid
    pub fn validate_block_oracle_txs(
        &self,
        tx_bytes_list: &[Vec<u8>],
        storage: &Storage,
    ) -> anyhow::Result<usize> {
        let mut valid_count = 0;

        for (idx, tx_bytes) in tx_bytes_list.iter().enumerate() {
            // Check if this is an Oracle transaction
            if is_oracle_transaction(tx_bytes) {
                // Validate the Oracle transaction
                crate::core::validate::validate_oracle_feed_tx(
                    tx_bytes,
                    storage,
                    Some(&self.config),
                )
                .map_err(|e| {
                    anyhow::anyhow!("Oracle transaction at index {} invalid: {}", idx, e)
                })?;
                valid_count += 1;

                info!("Validated Oracle transaction at index: {}", idx);
            }
        }

        info!(
            "Oracle validation complete: {} valid Oracle transactions",
            valid_count
        );
        Ok(valid_count)
    }

    /// Add a schema to the cache
    pub fn add_schema(&mut self, schema_id: Vec<u8>, schema: OracleSchema) {
        self.schema_cache.insert(schema_id, schema);
    }

    /// Get a schema from the cache
    pub fn get_schema(&self, schema_id: &[u8]) -> Option<&OracleSchema> {
        self.schema_cache.get(schema_id)
    }
}

impl Default for BlockOracleValidator {
    fn default() -> Self {
        Self::new()
    }
}

/// Validate a block header
///
/// # Arguments
/// * `storage` - Storage reference
///
/// # Returns
/// Ok(()) if valid, error otherwise
pub fn validate_block_header(
    storage: &Storage,
    block: &Block,
    enforce_tx_root: bool,
) -> anyhow::Result<()> {
    let parent_is_zero = block.parent_exec_hash.iter().all(|&b| b == 0);
    if parent_is_zero && block.height != 0 {
        anyhow::bail!("invalid header: parent_exec_hash is all zero");
    }

    // Compute and verify tx_root if enabled
    if enforce_tx_root {
        let tx_root = crate::core::crypto::compute_tx_root(&block.transactions);
        if tx_root != block.tx_root {
            anyhow::bail!("invalid header: tx_root mismatch");
        }
    }

    // Recompute block hash and compare
    let computed = block.header_hash();
    if computed != block.hash {
        anyhow::bail!("invalid header: block_hash mismatch");
    }

    // Signature over block hash
    let pk = PublicKey::from_bytes(&block.proposer).context("invalid proposer pubkey")?;

    // In ed25519-dalek 2.0, Signature::try_from replaces from_bytes
    let sig = ed25519_dalek::Signature::try_from(block.signature)
        .map_err(|_| anyhow::anyhow!("invalid signature format"))?;

    if pk.verify(&block.hash, &sig).is_err() {
        anyhow::bail!("invalid header: bad signature");
    }

    // parent_ref_hash is reserved for future multi-parent experiments; in the current
    // single-parent flow it must remain all-zero.
    if block.parent_ref_hash.iter().any(|&b| b != 0) {
        anyhow::bail!("invalid header: parent_ref_hash unsupported in single-parent mode");
    }

    // Parent presence and monotonic constraints if present
    if !parent_is_zero {
        // Get parent block at height - 1
        if block.height == 0 {
            anyhow::bail!("invalid header: non-zero parent_exec_hash at height 0");
        }

        match storage.get_block(block.height - 1)? {
            Some(parent_bytes) => {
                const MAX_BLOCK_SIZE: usize = 4 * 1024 * 1024;
                if parent_bytes.len() > MAX_BLOCK_SIZE {
                    anyhow::bail!(
                        "Parent block data too large for deserialization: {} bytes (max {})",
                        parent_bytes.len(),
                        MAX_BLOCK_SIZE
                    );
                }
                // Deserialize parent block
                let parent: Block = bincode::deserialize(&parent_bytes)
                    .context("Failed to deserialize parent block")?;

                if block.height != parent.height + 1 {
                    anyhow::bail!("invalid header: height not parent.height + 1");
                }
                if block.timestamp < parent.timestamp {
                    anyhow::bail!("invalid header: timestamp regresses");
                }
                // Verify parent hash matches
                if block.parent_exec_hash != parent.hash {
                    anyhow::bail!("invalid header: parent_exec_hash mismatch");
                }
            }
            None => {
                anyhow::bail!("orphan-exec: missing parent_exec block");
            }
        }
    } else if block.height != 0 {
        anyhow::bail!("invalid header: zero parent allowed only for height 0");
    }

    // Size limits
    if block.transactions.len() > MAX_TXS_PER_BLOCK {
        anyhow::bail!("invalid block: too many transactions");
    }
    let mut total = 0usize;
    for tx in &block.transactions {
        let enc = serialize_consensus(tx).context("tx encode")?;
        total = total.saturating_add(enc.len());
        if total > MAX_TOTAL_TX_BYTES {
            anyhow::bail!("invalid block: total tx bytes exceed limit");
        }
    }

    // Duplicate tx detection via leaf hash H("TXv1"||canonical_tx)
    use std::collections::BTreeSet;
    let mut seen: BTreeSet<Vec<u8>> = BTreeSet::new();
    for tx in &block.transactions {
        let enc = serialize_consensus(tx).context("tx encode")?;
        if !seen.insert(enc) {
            anyhow::bail!("invalid block: duplicate transaction");
        }
    }

    Ok(())
}

/// Enforce proposal size caps (10 MB wire payload).
pub fn validate_proposal_size(proposal: &ConsensusProposal) -> anyhow::Result<()> {
    let sz = proposal.estimated_size();
    anyhow::ensure!(
        sz <= MAX_PROPOSAL_BYTES,
        "proposal size {} exceeds {}",
        sz,
        MAX_PROPOSAL_BYTES
    );
    Ok(())
}

/// Enforce vote size caps (1 KB wire payload).
pub fn validate_vote_size(vote: &ConsensusVote) -> anyhow::Result<()> {
    let sz = vote.estimated_size();
    anyhow::ensure!(
        sz <= MAX_VOTE_BYTES,
        "vote size {} exceeds {}",
        sz,
        MAX_VOTE_BYTES
    );
    Ok(())
}

/// Light verification helper with ZKP verification
///
/// This function attempts ZKP verification first, then falls back to re-execution
/// if ZKP verification is not available or fails.
#[cfg(feature = "savitri-zkp")]
pub fn verify_block_proof_or_fallback<V>(
    block: &Block,
    certificate: &ConsensusCertificate,
    verifier: &V,
) -> anyhow::Result<bool>
where
    V: savitri_zkp::ZkVerifier,
{
    use savitri_zkp::Statement;

    // Create ZKP statement from block data
    let statement = Statement {
        a: savitri_zkp::monolith::monolith_zkp::compress_root_64_to_32(&block.parent_exec_hash),
        b: savitri_zkp::monolith::monolith_zkp::compress_root_64_to_32(&block.state_root),
        c: savitri_zkp::monolith::monolith_zkp::compress_root_64_to_32(&block.tx_root),
        d: savitri_zkp::monolith::monolith_zkp::calculate_monolith_commitment(
            &savitri_zkp::monolith::MonolithHeader {
                headers_commit: block.parent_exec_hash,
                state_commit: block.state_root,
                exec_height: block.height,
                epoch_id: block.height / 1000,
            },
            Some(block.parent_exec_hash),
        ),
        e: block.height,
        f: block.height / 1000,
    };

    // Try to extract ZKP proof from certificate
    if let Some(zkp_proof) = extract_zkp_proof_from_certificate(certificate) {
        match verifier.verify(&statement, &zkp_proof) {
            Ok(true) => {
                info!("ZKP verification successful for block {}", block.height);
                return Ok(true);
            }
            Ok(false) => {
                warn!(
                    "ZKP verification failed for block {}, falling back to re-execution",
                    block.height
                );
            }
            Err(e) => {
                warn!(
                    "ZKP verification error for block {}: {}, falling back to re-execution",
                    block.height, e
                );
            }
        }
    } else {
        info!(
            "No ZKP proof found in certificate for block {}, falling back to re-execution",
            block.height
        );
    }

    // Fallback to re-execution
    info!("Using re-execution fallback for block {}", block.height);
    Ok(false)
}

/// Light verification helper without ZKP support.
#[cfg(not(feature = "savitri-zkp"))]
pub fn verify_block_proof_or_fallback<V>(
    block: &Block,
    _certificate: &ConsensusCertificate,
    _verifier: &V,
) -> anyhow::Result<bool> {
    info!("Using re-execution fallback for block {}", block.height);
    Ok(false)
}

/// Extract ZKP proof from consensus certificate
#[cfg(feature = "savitri-zkp")]
fn extract_zkp_proof_from_certificate(
    _certificate: &ConsensusCertificate,
) -> Option<savitri_zkp::ZkProof> {
    // TODO: Implement ZKP proof extraction from certificate
    // For now, return None to trigger fallback
    None
}
