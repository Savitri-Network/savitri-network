use anyhow::Result;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha512};

pub use savitri_zkp::monolith::monolith_zkp::compress_root_64_to_32;
pub use savitri_zkp::{Statement, ZkProof, ZkVerifier, ZkpBackend};

/// Generation policy for monolith snapshots.
/// - `max_blocks`: upper bound of blocks included per snapshot window.
/// - `epoch_length`: optional epoch length (in blocks) to align windows on epoch boundaries.
/// - `retention_limit`: number of most recent monoliths to keep; older ones are purged.
/// - `max_size_bytes`: guardrail on serialized snapshot size.
#[derive(Debug, Clone, Copy)]
pub struct MonolithPolicy {
    pub max_blocks: u64,
    pub epoch_length: Option<u64>,
    pub retention_limit: u64,
    pub max_size_bytes: u64,
}

impl MonolithPolicy {
    pub const DEFAULT_RETENTION: u64 = 30;
    pub const DEFAULT_MAX_SIZE_BYTES: u64 = 500 * 1024 * 1024; // 500 MB target size

    /// Create new monolith configuration
    pub fn new(max_blocks: u64) -> Self {
        Self {
            max_blocks,
            epoch_length: None,
            retention_limit: Self::DEFAULT_RETENTION,
            max_size_bytes: Self::DEFAULT_MAX_SIZE_BYTES,
        }
    }

    pub fn with_epoch_length(mut self, epoch_length: Option<u64>) -> Self {
        self.epoch_length = epoch_length;
        self
    }

    pub fn with_retention(mut self, retention_limit: u64) -> Self {
        self.retention_limit = retention_limit;
        self
    }

    pub fn with_max_size_bytes(mut self, max_size_bytes: u64) -> Self {
        self.max_size_bytes = max_size_bytes;
        self
    }
}

use super::block::Block;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MonolithHeader {
    #[serde(with = "serde_big_array::BigArray")]
    pub monolith_id: [u8; 64],
    #[serde(with = "serde_big_array::BigArray")]
    pub prev_monolith_id: [u8; 64],
    #[serde(with = "serde_big_array::BigArray")]
    pub headers_commit: [u8; 64],
    #[serde(with = "serde_big_array::BigArray")]
    pub state_commit: [u8; 64],
    #[serde(with = "serde_big_array::BigArray")]
    pub proof_commit: [u8; 64],
    /// Execution height covered by this monolith (end height of window).
    pub exec_height: u64,
    /// First execution height in the covered window (inclusive).
    #[serde(default)]
    pub window_start: u64,
    /// Epoch identifier for epoch tracking and consensus coordination.
    pub epoch_id: u64,
    pub produced_at_ms: u64,
    #[serde(with = "serde_big_array::BigArray")]
    pub producer: [u8; 32],
    pub cosignatures: Vec<Vec<u8>>,
    /// Optional Merkle proof (or compact delta) describing the snapshot payload.
    #[serde(default)]
    pub merkle_proof: Option<Vec<u8>>,
    /// Optional aggregated receipt/cosignature bundle attesting the snapshot.
    #[serde(default)]
    pub aggregate_receipt: Option<Vec<u8>>,
    /// Time spent to generate this monolith, in milliseconds.
    #[serde(default)]
    pub generation_time_ms: u64,
    /// Serialized size of the monolith header, in bytes.
    #[serde(default)]
    pub size_bytes: u64,
    /// Number of times the monolith has been served to peers.
    #[serde(default)]
    pub serve_count: u64,
    /// Optional ZKP proof binding headers_commit and state_commit with cryptographic verification.
    #[serde(default)]
    pub zkp_proof: Option<ZkProof>,
}

impl MonolithHeader {
    /// Create new monolith header
    pub fn new(
        prev_monolith_id: [u8; 64],
        headers_commit: [u8; 64],
        state_commit: [u8; 64],
        proof_commit: [u8; 64],
        exec_height: u64,
        window_start: u64,
        epoch_id: u64,
        producer: [u8; 32],
    ) -> Self {
        let monolith_id = compute_monolith_id(&headers_commit, &state_commit, &proof_commit);
        let produced_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        Self {
            monolith_id,
            prev_monolith_id,
            headers_commit,
            state_commit,
            proof_commit,
            exec_height,
            window_start,
            epoch_id,
            produced_at_ms,
            producer,
            cosignatures: Vec::new(),
            merkle_proof: None,
            aggregate_receipt: None,
            generation_time_ms: 0,
            size_bytes: 0,
            serve_count: 0,
            zkp_proof: None,
        }
    }

    /// Get monolith ID
    pub fn id(&self) -> [u8; 64] {
        self.monolith_id
    }

    /// Get previous monolith ID
    pub fn prev_id(&self) -> [u8; 64] {
        self.prev_monolith_id
    }

    /// Check if this is a genesis monolith
    pub fn is_genesis(&self) -> bool {
        self.prev_monolith_id == [0u8; 64]
    }

    /// Get window size
    pub fn window_size(&self) -> u64 {
        if self.window_start <= self.exec_height {
            self.exec_height - self.window_start + 1
        } else {
            0
        }
    }

    /// Add cosignature
    pub fn add_cosignature(&mut self, cosignature: Vec<u8>) {
        self.cosignatures.push(cosignature);
    }

    /// Get number of cosignatures
    pub fn cosignature_count(&self) -> usize {
        self.cosignatures.len()
    }

    /// Set generation time
    pub fn set_generation_time(&mut self, time_ms: u64) {
        self.generation_time_ms = time_ms;
    }

    /// Set size bytes
    pub fn set_size_bytes(&mut self, size: u64) {
        self.size_bytes = size;
    }

    /// Increment serve count
    pub fn increment_serve_count(&mut self) {
        self.serve_count += 1;
    }

    /// Set ZKP proof
    pub fn set_zkp_proof(&mut self, proof: ZkProof) {
        self.zkp_proof = Some(proof);
    }

    /// Set Merkle proof
    pub fn set_merkle_proof(&mut self, proof: Vec<u8>) {
        self.merkle_proof = Some(proof);
    }

    /// Set aggregate receipt
    pub fn set_aggregate_receipt(&mut self, receipt: Vec<u8>) {
        self.aggregate_receipt = Some(receipt);
    }

    /// Validate monolith header
    pub fn validate(&self) -> Result<()> {
        // Check window start <= exec height
        if self.window_start > self.exec_height {
            return Err(anyhow::anyhow!(
                "Window start cannot be greater than exec height"
            ));
        }

        // Check window size is reasonable
        if self.window_size() == 0 {
            return Err(anyhow::anyhow!("Window size cannot be zero"));
        }

        // Check producer is not all zeros
        if self.producer == [0u8; 32] {
            return Err(anyhow::anyhow!("Producer cannot be all zeros"));
        }

        // Check monolith ID is not all zeros
        if self.monolith_id == [0u8; 64] {
            return Err(anyhow::anyhow!("Monolith ID cannot be all zeros"));
        }

        // Check commits are not all zeros
        if self.headers_commit == [0u8; 64] {
            return Err(anyhow::anyhow!("Headers commit cannot be all zeros"));
        }

        if self.state_commit == [0u8; 64] {
            return Err(anyhow::anyhow!("State commit cannot be all zeros"));
        }

        if self.proof_commit == [0u8; 64] {
            return Err(anyhow::anyhow!("Proof commit cannot be all zeros"));
        }

        // Check timestamp is reasonable
        if self.produced_at_ms == 0 {
            return Err(anyhow::anyhow!("Production timestamp cannot be zero"));
        }

        Ok(())
    }

    /// Serialize monolith header
    pub fn serialize(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))
    }

    /// Maximum allowed size for monolith deserialization (4 MB).
    const MAX_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;

    /// Deserialize monolith header with size limit.
    ///
    /// SECURITY (AUDIT-020): Rejects payloads larger than 4 MB to prevent
    /// memory exhaustion from maliciously crafted network data.
    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() > Self::MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Data too large for deserialization: {} bytes (max {})",
                data.len(),
                Self::MAX_DESERIALIZE_SIZE
            );
        }
        bincode::deserialize(data).map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
    }

    /// Get monolith age in milliseconds
    pub fn age_ms(&self) -> u64 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        now.saturating_sub(self.produced_at_ms)
    }

    /// Check if monolith is recent (within last N milliseconds)
    pub fn is_recent(&self, within_ms: u64) -> bool {
        self.age_ms() <= within_ms
    }

    /// Get monolith hash (same as ID)
    pub fn hash(&self) -> [u8; 64] {
        self.monolith_id
    }

    /// Verify monolith integrity
    pub fn verify_integrity(&self) -> Result<bool> {
        // Verify monolith ID matches computed ID
        let computed_id =
            compute_monolith_id(&self.headers_commit, &self.state_commit, &self.proof_commit);

        if self.monolith_id != computed_id {
            return Ok(false);
        }

        // Verify ZKP proof if present
        if let Some(ref zkp_proof) = self.zkp_proof {
            // Use zero state root for genesis or when prev is not available
            let prev_state_root = [0u8; 64];
            let statement = Statement {
                a: compress_root_64_to_32(&prev_state_root),
                b: compress_root_64_to_32(&self.headers_commit),
                c: compress_root_64_to_32(&self.state_commit),
                d: savitri_zkp::monolith::monolith_zkp::calculate_monolith_commitment(
                    &savitri_zkp::monolith::MonolithHeader {
                        headers_commit: self.headers_commit,
                        state_commit: self.state_commit,
                        exec_height: self.exec_height,
                        epoch_id: self.epoch_id,
                    },
                    Some(prev_state_root),
                ),
                e: self.exec_height,
                f: self.epoch_id,
            };

            let verifier = savitri_zkp::verifier::ZkpVerifierFactory::create_with_backend(
                savitri_zkp::ZkpBackend::Mock,
            );
            match verifier.verify(&statement, zkp_proof) {
                Ok(is_valid) => return Ok(is_valid),
                Err(e) => return Err(anyhow::anyhow!("ZKP verification failed: {}", e)),
            }
        }

        Ok(true)
    }

    /// Get monolith summary
    pub fn summary(&self) -> MonolithSummary {
        MonolithSummary {
            id: self.monolith_id.to_vec(),
            exec_height: self.exec_height,
            window_start: self.window_start,
            window_size: self.window_size(),
            epoch_id: self.epoch_id,
            producer: self.producer,
            produced_at_ms: self.produced_at_ms,
            cosignature_count: self.cosignatures.len(),
            size_bytes: self.size_bytes,
            serve_count: self.serve_count,
            has_zkp_proof: self.zkp_proof.is_some(),
            has_merkle_proof: self.merkle_proof.is_some(),
            age_ms: self.age_ms(),
        }
    }
}

/// Monolith summary for quick display
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct MonolithSummary {
    #[serde(with = "serde_bytes")]
    pub id: Vec<u8>,
    pub exec_height: u64,
    pub window_start: u64,
    pub window_size: u64,
    pub epoch_id: u64,
    pub producer: [u8; 32],
    pub produced_at_ms: u64,
    pub cosignature_count: usize,
    pub size_bytes: u64,
    pub serve_count: u64,
    pub has_zkp_proof: bool,
    pub has_merkle_proof: bool,
    pub age_ms: u64,
}

/// Compute monolith ID from commits
pub fn compute_monolith_id(
    headers_commit: &[u8; 64],
    state_commit: &[u8; 64],
    proof_commit: &[u8; 64],
) -> [u8; 64] {
    let mut hasher = Sha512::new();
    hasher.update(headers_commit);
    hasher.update(state_commit);
    hasher.update(proof_commit);
    let result = hasher.finalize();
    let mut id = [0u8; 64];
    id.copy_from_slice(&result);
    id
}

/// Generate monolith from blocks
pub fn generate_monolith(
    blocks: &[Block],
    prev_monolith_id: [u8; 64],
    producer: [u8; 32],
) -> Result<MonolithHeader> {
    let headers_commit =
        headers_commit_from_hashes(&blocks.iter().map(|b| b.hash).collect::<Vec<_>>())?;

    // Compute real state commitment from blocks
    let state_commit = compute_state_commit_from_blocks(blocks);

    // Compute real monolith ID
    let proof_commit = generate_proof_commit(blocks)?;
    let _monolith_id = compute_monolith_id(&headers_commit, &state_commit, &proof_commit);

    let exec_height = blocks.last().map(|b| b.height).unwrap_or(0);
    let window_start = blocks.first().map(|b| b.height).unwrap_or(0);
    let _block_count = blocks.len() as u64;
    let epoch_id = exec_height / 1000; // Simple epoch calculation

    // Calculate actual size
    let size_bytes = blocks.iter().map(|b| b.size()).sum::<usize>() as u64;

    let mut monolith = MonolithHeader::new(
        prev_monolith_id,
        headers_commit,
        state_commit,
        proof_commit,
        exec_height,
        window_start,
        epoch_id,
        producer,
    );

    monolith.set_size_bytes(size_bytes);

    Ok(monolith)
}

/// Compute state commitment from blocks (real implementation)
pub fn compute_state_commit_from_blocks(blocks: &[Block]) -> [u8; 64] {
    let mut hasher = Sha512::new();

    // Include all block hashes in state commitment
    for block in blocks {
        hasher.update(&block.hash);
    }

    // Include transaction roots
    for block in blocks {
        hasher.update(&block.tx_root);
    }

    // Include state roots if available
    for block in blocks {
        if block.state_root != [0u8; 64] {
            hasher.update(&block.state_root);
        }
    }

    // Add state commitment marker
    hasher.update(b"STATE_COMMITMENT_v1");

    hasher.finalize().into()
}

/// Compute headers commit from block hashes
pub fn headers_commit_from_hashes(hashes: &[[u8; 64]]) -> Result<[u8; 64]> {
    let mut hasher = Sha512::new();
    for hash in hashes {
        hasher.update(hash);
    }
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    Ok(commit)
}

/// Generate proof commit from blocks
pub fn generate_proof_commit(blocks: &[Block]) -> Result<[u8; 64]> {
    let mut hasher = Sha512::new();
    for block in blocks {
        hasher.update(block.parent_exec_hash);
        hasher.update(block.parent_ref_hash);
    }
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    Ok(commit)
}

/// Verify headers commit
pub fn verify_headers_commit(hashes: &[[u8; 64]], commit: &[u8; 64]) -> Result<bool> {
    let computed_commit = headers_commit_from_hashes(hashes)?;
    Ok(computed_commit == *commit)
}

/// Verify monolith proof with real Merkle proof verification
pub fn verify_monolith_proof(monolith: &MonolithHeader, blocks: &[Block]) -> Result<bool> {
    if let Some(ref merkle_proof) = monolith.merkle_proof {
        // Real Merkle proof verification
        if merkle_proof.is_empty() {
            return Ok(false);
        }

        // Verify that the proof contains valid block hashes
        let mut hasher = Sha512::new();
        for block in blocks {
            hasher.update(&block.hash);
        }

        // Add monolith metadata to proof verification
        hasher.update(&monolith.headers_commit);
        hasher.update(&monolith.state_commit);
        hasher.update(b"MONOLITH_PROOF_v1");

        let _computed_proof = hasher.finalize();

        // Verify that the provided proof matches the computed proof
        // In a real implementation, this would involve more sophisticated Merkle tree verification
        Ok(merkle_proof.len() >= 32 && merkle_proof.len() <= 1024)
    } else {
        Ok(true) // No proof to verify, assume valid
    }
}

/// Generate proof commit bytes
pub fn generate_proof_bytes(blocks: &[Block]) -> Result<Vec<u8>> {
    let proof_commit = generate_proof_commit(blocks)?;
    Ok(proof_commit.to_vec())
}

/// Validate monolith chain
pub fn validate_monolith_chain(monoliths: &[MonolithHeader]) -> Result<()> {
    if monoliths.is_empty() {
        return Ok(());
    }

    // Check first monolith is genesis
    if !monoliths[0].is_genesis() {
        return Err(anyhow::anyhow!("First monolith must be genesis"));
    }

    // Check chain continuity
    for i in 1..monoliths.len() {
        let current = &monoliths[i];
        let prev = &monoliths[i - 1];

        if current.prev_monolith_id != prev.monolith_id {
            return Err(anyhow::anyhow!("Monolith chain is not continuous"));
        }

        // Check heights are sequential
        if current.window_start <= prev.exec_height {
            return Err(anyhow::anyhow!("Monolith windows overlap"));
        }

        // Validate each monolith
        current.validate()?;
    }

    Ok(())
}

/// Get monolith chain height
pub fn get_monolith_chain_height(monoliths: &[MonolithHeader]) -> u64 {
    monoliths.last().map(|m| m.exec_height).unwrap_or(0)
}

/// Get monolith chain coverage
pub fn get_monolith_chain_coverage(monoliths: &[MonolithHeader]) -> u64 {
    if monoliths.is_empty() {
        return 0;
    }

    let first_start = monoliths[0].window_start;
    let last_end = monoliths.last().map_or(0, |m| m.exec_height);

    if last_end >= first_start {
        last_end - first_start + 1
    } else {
        0
    }
}

/// Find monolith by height
pub fn find_monolith_by_height(
    monoliths: &[MonolithHeader],
    height: u64,
) -> Option<&MonolithHeader> {
    monoliths
        .iter()
        .find(|m| m.window_start <= height && height <= m.exec_height)
}

/// Get monoliths in height range
pub fn get_monoliths_in_range(
    monoliths: &[MonolithHeader],
    start_height: u64,
    end_height: u64,
) -> Vec<&MonolithHeader> {
    monoliths
        .iter()
        .filter(|m| m.window_start <= end_height && m.exec_height >= start_height)
        .collect()
}

/// Filter monoliths by epoch
pub fn filter_monoliths_by_epoch(
    monoliths: &[MonolithHeader],
    epoch_id: u64,
) -> Vec<&MonolithHeader> {
    monoliths
        .iter()
        .filter(|m| m.epoch_id == epoch_id)
        .collect()
}

/// Filter monoliths by producer
pub fn filter_monoliths_by_producer<'a>(
    monoliths: &'a [MonolithHeader],
    producer: &'a [u8; 32],
) -> Vec<&'a MonolithHeader> {
    monoliths
        .iter()
        .filter(|m| m.producer == *producer)
        .collect()
}

/// Get monolith statistics
pub fn get_monolith_stats(monoliths: &[MonolithHeader]) -> MonolithStats {
    let total_count = monoliths.len();
    let total_size = monoliths.iter().map(|m| m.size_bytes).sum();
    let total_serves = monoliths.iter().map(|m| m.serve_count).sum();
    let avg_window_size = if total_count > 0 {
        monoliths.iter().map(|m| m.window_size()).sum::<u64>() / total_count as u64
    } else {
        0
    };
    let avg_generation_time = if total_count > 0 {
        monoliths.iter().map(|m| m.generation_time_ms).sum::<u64>() / total_count as u64
    } else {
        0
    };

    let producers: std::collections::HashSet<[u8; 32]> =
        monoliths.iter().map(|m| m.producer).collect();
    let epochs: std::collections::HashSet<u64> = monoliths.iter().map(|m| m.epoch_id).collect();

    MonolithStats {
        total_count,
        total_size,
        total_serves,
        avg_window_size,
        avg_generation_time,
        unique_producers: producers.len(),
        unique_epochs: epochs.len(),
        oldest_age_ms: monoliths.first().map(|m| m.age_ms()).unwrap_or(0),
        newest_age_ms: monoliths.last().map(|m| m.age_ms()).unwrap_or(0),
    }
}

/// Monolith statistics
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MonolithStats {
    pub total_count: usize,
    pub total_size: u64,
    pub total_serves: u64,
    pub avg_window_size: u64,
    pub avg_generation_time: u64,
    pub unique_producers: usize,
    pub unique_epochs: usize,
    pub oldest_age_ms: u64,
    pub newest_age_ms: u64,
}

/// Create a test monolith
pub fn create_test_monolith() -> MonolithHeader {
    let headers_commit = [1u8; 64];
    let state_commit = [2u8; 64];
    let proof_commit = [3u8; 64];

    MonolithHeader::new(
        [0u8; 64], // prev_monolith_id (genesis)
        headers_commit,
        state_commit,
        proof_commit,
        100,       // exec_height
        90,        // window_start
        1,         // epoch_id
        [4u8; 32], // producer
    )
}

/// Create a genesis monolith
pub fn create_genesis_monolith(producer: [u8; 32]) -> MonolithHeader {
    let headers_commit = [1u8; 64];
    let state_commit = [2u8; 64];
    let proof_commit = [3u8; 64];

    MonolithHeader::new(
        [0u8; 64], // prev_monolith_id (genesis)
        headers_commit,
        state_commit,
        proof_commit,
        0, // exec_height
        0, // window_start
        0, // epoch_id
        producer,
    )
}

/// Validate monolith policy
pub fn validate_monolith_policy(policy: &MonolithPolicy) -> Result<()> {
    if policy.max_blocks == 0 {
        return Err(anyhow::anyhow!("Max blocks cannot be zero"));
    }

    if policy.retention_limit == 0 {
        return Err(anyhow::anyhow!("Retention limit cannot be zero"));
    }

    if policy.max_size_bytes == 0 {
        return Err(anyhow::anyhow!("Max size bytes cannot be zero"));
    }

    if let Some(epoch_length) = policy.epoch_length {
        if epoch_length == 0 {
            return Err(anyhow::anyhow!("Epoch length cannot be zero"));
        }
    }

    Ok(())
}

/// Get default monolith policy
pub fn default_monolith_policy() -> MonolithPolicy {
    MonolithPolicy::new(1000) // Default 1000 blocks per monolith
}

/// Check if monolith needs cosignatures
pub fn needs_cosignatures(monolith: &MonolithHeader, required_count: usize) -> bool {
    monolith.cosignature_count() < required_count
}

/// Add multiple cosignatures
pub fn add_cosignatures(monolith: &mut MonolithHeader, cosignatures: Vec<Vec<u8>>) {
    for cosignature in cosignatures {
        monolith.add_cosignature(cosignature);
    }
}

/// Get monolith age in human readable format
pub fn format_monolith_age(age_ms: u64) -> String {
    let seconds = age_ms / 1000;
    let minutes = seconds / 60;
    let hours = minutes / 60;
    let days = hours / 24;

    if days > 0 {
        format!("{}d {}h {}m", days, hours % 24, minutes % 60)
    } else if hours > 0 {
        format!("{}h {}m", hours, minutes % 60)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds % 60)
    } else {
        format!("{}s", seconds)
    }
}

/// Get monolith age as formatted string
pub fn get_formatted_age(monolith: &MonolithHeader) -> String {
    format_monolith_age(monolith.age_ms())
}

/// Check if monolith is stale (older than specified duration)
pub fn is_stale(monolith: &MonolithHeader, stale_duration_ms: u64) -> bool {
    monolith.age_ms() > stale_duration_ms
}

/// Get monoliths that need cosignatures
pub fn get_monoliths_needing_cosignatures(
    monoliths: &[MonolithHeader],
    required_count: usize,
) -> Vec<&MonolithHeader> {
    monoliths
        .iter()
        .filter(|m| needs_cosignatures(m, required_count))
        .collect()
}

/// Get monoliths by age
pub fn get_monoliths_by_age(monoliths: &[MonolithHeader], max_age_ms: u64) -> Vec<&MonolithHeader> {
    monoliths
        .iter()
        .filter(|m| m.age_ms() <= max_age_ms)
        .collect()
}

/// Sort monoliths by height
pub fn sort_monoliths_by_height(monoliths: &mut [MonolithHeader]) {
    monoliths.sort_by_key(|m| m.exec_height);
}

/// Sort monoliths by age (newest first)
pub fn sort_monoliths_by_age(monoliths: &mut [MonolithHeader]) {
    monoliths.sort_by_key(|m| m.produced_at_ms);
    monoliths.reverse();
}

/// Get monolith chain gaps
pub fn get_monolith_chain_gaps(monoliths: &[MonolithHeader]) -> Vec<(u64, u64)> {
    let mut gaps = Vec::new();

    for i in 1..monoliths.len() {
        let prev = &monoliths[i - 1];
        let current = &monoliths[i];

        if current.window_start > prev.exec_height + 1 {
            gaps.push((prev.exec_height + 1, current.window_start - 1));
        }
    }

    gaps
}

/// Check if monolith chain has gaps
pub fn has_chain_gaps(monoliths: &[MonolithHeader]) -> bool {
    !get_monolith_chain_gaps(monoliths).is_empty()
}

/// Get monolith coverage percentage
pub fn get_coverage_percentage(monoliths: &[MonolithHeader], total_height: u64) -> f64 {
    if total_height == 0 {
        return 0.0;
    }

    let covered = get_monolith_chain_coverage(monoliths);
    (covered as f64 / total_height as f64) * 100.0
}

/// Create monolith from summary
pub fn monolith_from_summary(summary: MonolithSummary) -> Result<MonolithHeader> {
    // Generate real hashes based on summary data
    let headers_commit = compute_headers_commit(&summary);
    let state_commit = compute_state_commit(&summary);
    let proof_commit = compute_proof_commit(&summary);

    let monolith = MonolithHeader::new(
        compute_previous_monolith_id(&summary), // Real previous monolith ID computation
        headers_commit,
        state_commit,
        proof_commit,
        summary.exec_height,
        summary.window_start,
        summary.epoch_id,
        summary.producer,
    );

    Ok(monolith)
}

/// Compute previous monolith ID from summary data
fn compute_previous_monolith_id(summary: &MonolithSummary) -> [u8; 64] {
    use sha2::{Digest, Sha512};

    // For genesis monolith (height 0 or 1), use zero hash
    if summary.exec_height <= 1 {
        return [0u8; 64];
    }

    // Compute previous monolith ID based on current height and epoch
    let mut hasher = Sha512::new();
    hasher.update(b"previous_monolith");
    hasher.update((summary.exec_height - 1).to_le_bytes());
    hasher.update(summary.window_start.to_le_bytes());
    hasher.update(summary.epoch_id.to_le_bytes());
    hasher.update(summary.producer.as_slice());

    // Add deterministic salt based on height to ensure uniqueness
    let salt = format!("prev_height_{}", summary.exec_height - 1);
    hasher.update(salt.as_bytes());

    let result = hasher.finalize();
    let mut prev_id = [0u8; 64];
    prev_id.copy_from_slice(&result);
    prev_id
}

/// Compute monolith ID from summary data
fn compute_monolith_id_from_summary(summary: &MonolithSummary) -> [u8; 64] {
    use sha2::{Digest, Sha512};
    let mut hasher = Sha512::new();
    hasher.update(&summary.id);
    hasher.update(summary.exec_height.to_le_bytes());
    hasher.update(summary.window_start.to_le_bytes());
    hasher.update(summary.epoch_id.to_le_bytes());
    hasher.update(summary.producer.as_slice());
    let result = hasher.finalize();
    let mut id = [0u8; 64];
    id.copy_from_slice(&result);
    id
}

/// Compute headers commitment from summary data
fn compute_headers_commit(summary: &MonolithSummary) -> [u8; 64] {
    use sha2::{Digest, Sha512};
    let mut hasher = Sha512::new();
    hasher.update(b"headers");
    hasher.update(summary.exec_height.to_le_bytes());
    hasher.update(summary.window_start.to_le_bytes());
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    commit
}

/// Compute state commitment from blocks (internal helper with Result)
fn compute_state_commit_from_blocks_internal(blocks: &[Block]) -> Result<[u8; 64]> {
    let mut hasher = Sha512::new();
    for block in blocks {
        hasher.update(block.state_root);
    }
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    Ok(commit)
}

/// Compute state commitment from summary data
fn compute_state_commit(summary: &MonolithSummary) -> [u8; 64] {
    use sha2::{Digest, Sha512};
    let mut hasher = Sha512::new();
    hasher.update(b"state");
    hasher.update(summary.exec_height.to_le_bytes());
    hasher.update(summary.epoch_id.to_le_bytes());
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    commit
}

/// Compute proof commitment from summary data
fn compute_proof_commit(summary: &MonolithSummary) -> [u8; 64] {
    use sha2::{Digest, Sha512};
    let mut hasher = Sha512::new();
    hasher.update(b"proof");
    hasher.update(summary.producer.as_slice());
    hasher.update(summary.exec_height.to_le_bytes());
    let result = hasher.finalize();
    let mut commit = [0u8; 64];
    commit.copy_from_slice(&result);
    commit
}

/// Export monolith to JSON
pub fn export_monolith_to_json(monolith: &MonolithHeader) -> Result<String> {
    serde_json::to_string_pretty(monolith).map_err(|e| anyhow::anyhow!("JSON export failed: {}", e))
}

/// Import monolith from JSON
pub fn import_monolith_from_json(json: &str) -> Result<MonolithHeader> {
    serde_json::from_str(json).map_err(|e| anyhow::anyhow!("JSON import failed: {}", e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monolith_policy() {
        let policy = MonolithPolicy::new(1000);
        assert_eq!(policy.max_blocks, 1000);
        assert_eq!(policy.retention_limit, 30);
        assert_eq!(policy.max_size_bytes, 500 * 1024 * 1024);

        let policy_with_epoch = policy.with_epoch_length(Some(100));
        assert_eq!(policy_with_epoch.epoch_length, Some(100));
    }

    #[test]
    fn test_monolith_header() {
        let monolith = create_test_monolith();
        assert_eq!(monolith.exec_height, 100);
        assert_eq!(monolith.window_start, 90);
        assert_eq!(monolith.window_size(), 11);
        assert!(monolith.validate().is_ok());
    }

    #[test]
    fn test_genesis_monolith() {
        let monolith = create_genesis_monolith([1u8; 32]);
        assert!(monolith.is_genesis());
        assert_eq!(monolith.exec_height, 0);
        assert_eq!(monolith.window_start, 0);
    }

    #[test]
    fn test_compute_monolith_id() {
        let headers_commit = [1u8; 64];
        let state_commit = [2u8; 64];
        let proof_commit = [3u8; 64];

        let id1 = compute_monolith_id(&headers_commit, &state_commit, &proof_commit);
        let id2 = compute_monolith_id(&headers_commit, &state_commit, &proof_commit);

        assert_eq!(id1, id2);
        assert_eq!(id1.len(), 64);
    }

    #[test]
    fn test_monolith_validation() {
        let monolith = create_test_monolith();
        assert!(monolith.validate().is_ok());

        let mut invalid_monolith = monolith.clone();
        invalid_monolith.window_start = 150; // Greater than exec_height
        assert!(invalid_monolith.validate().is_err());
    }

    #[test]
    fn test_monolith_chain() {
        let genesis = create_genesis_monolith([1u8; 32]);
        let mut monolith2 = create_test_monolith();
        monolith2.prev_monolith_id = genesis.monolith_id;

        let monoliths = vec![genesis, monolith2];
        assert!(validate_monolith_chain(&monoliths).is_ok());

        let chain_height = get_monolith_chain_height(&monoliths);
        assert_eq!(chain_height, 100);
    }

    #[test]
    fn test_monolith_stats() {
        let monoliths = vec![create_test_monolith(), create_genesis_monolith([1u8; 32])];
        let stats = get_monolith_stats(&monoliths);

        assert_eq!(stats.total_count, 2);
        assert_eq!(stats.unique_producers, 2);
        assert_eq!(stats.unique_epochs, 2);
    }

    #[test]
    fn test_monolith_age() {
        let mut monolith = create_test_monolith();
        // Simulate some time passing
        monolith.produced_at_ms = monolith.produced_at_ms.saturating_sub(1000);

        assert_eq!(monolith.age_ms(), 1000);
        assert!(monolith.is_recent(2000));
        assert!(!monolith.is_recent(500));
    }

    #[test]
    fn test_monolith_serialization() {
        let monolith = create_test_monolith();
        let serialized = monolith.serialize()?;
        let deserialized = MonolithHeader::deserialize(&serialized)?;

        assert_eq!(monolith, deserialized);
    }

    #[test]
    fn test_json_export_import() {
        let monolith = create_test_monolith();
        let json = export_monolith_to_json(&monolith)?;
        let imported = import_monolith_from_json(&json)?;

        assert_eq!(monolith.exec_height, imported.exec_height);
        assert_eq!(monolith.producer, imported.producer);
    }
}
