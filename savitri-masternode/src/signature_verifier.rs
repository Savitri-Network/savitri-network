//!
//! This module provides cryptographic signature verification for transactions,
//! block proposals, and consensus certificates using Ed25519.

use anyhow::{anyhow, Result};
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use sha2::{Digest, Sha256, Sha512};

/// Signature verifier for Ed25519 signatures
#[derive(Debug, Clone)]
pub struct SignatureVerifier {
    /// Cache of verified public keys for performance
    verified_keys_cache: std::collections::HashMap<[u8; 32], bool>,
}

impl SignatureVerifier {
    pub fn new() -> Self {
        Self {
            verified_keys_cache: std::collections::HashMap::new(),
        }
    }

    /// Verify a transaction signature
    pub fn verify_transaction_signature(
        &mut self,
        tx_hash: &[u8; 32],
        sender_pubkey: &[u8; 32],
        signature: &[u8; 64],
    ) -> Result<bool> {
        // Create the message to verify (transaction hash)
        let message = tx_hash;

        // Verify the signature
        self.verify_ed25519_signature(message, sender_pubkey, signature)
    }

    /// Verify a block proposal signature
    pub fn verify_block_proposal_signature(
        &mut self,
        block_hash: &[u8; 64],
        proposer_pubkey: &[u8; 32],
        signature: &[u8; 64],
    ) -> Result<bool> {
        // Create message from block hash
        let mut hasher = Sha256::new();
        hasher.update(block_hash);
        let message_hash: [u8; 32] = hasher.finalize().into();

        self.verify_ed25519_signature(&message_hash, proposer_pubkey, signature)
    }

    /// Verify a consensus certificate signature
    pub fn verify_certificate_signature(
        &mut self,
        block_hash: &[u8; 64],
        height: u64,
        proposer_group_id: &str,
        validation_timestamp: u64,
        voter_pubkey: &[u8; 32],
        signature: &[u8; 64],
    ) -> Result<bool> {
        // Create message from certificate data
        let mut message = Vec::new();
        message.extend_from_slice(block_hash);
        message.extend_from_slice(&height.to_le_bytes());
        message.extend_from_slice(proposer_group_id.as_bytes());
        message.extend_from_slice(&validation_timestamp.to_le_bytes());

        let mut hasher = Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();

        self.verify_ed25519_signature(&message_hash, voter_pubkey, signature)
    }

    /// Verify multiple voter signatures for a certificate
    pub fn verify_certificate_voters(
        &mut self,
        block_hash: &[u8; 64],
        height: u64,
        proposer_group_id: &str,
        validation_timestamp: u64,
        voter_pubkeys: &[[u8; 32]],
        voter_signatures: &[[u8; 64]],
    ) -> Result<(usize, usize)> {
        if voter_pubkeys.len() != voter_signatures.len() {
            return Err(anyhow!("Voter pubkeys and signatures count mismatch"));
        }

        let mut valid_count = 0;
        let mut invalid_count = 0;

        for (pubkey, signature) in voter_pubkeys.iter().zip(voter_signatures.iter()) {
            match self.verify_certificate_signature(
                block_hash,
                height,
                proposer_group_id,
                validation_timestamp,
                pubkey,
                signature,
            ) {
                Ok(true) => valid_count += 1,
                Ok(false) => invalid_count += 1,
                Err(_) => invalid_count += 1,
            }
        }

        Ok((valid_count, invalid_count))
    }

    /// Verify an Ed25519 signature with enhanced logging
    pub fn verify_ed25519_signature(
        &mut self,
        message: &[u8; 32],
        pubkey_bytes: &[u8; 32],
        signature_bytes: &[u8; 64],
    ) -> Result<bool> {
        // Enhanced logging for debugging
        tracing::debug!(
            message_hash = %hex::encode(message),
            pubkey = %hex::encode(pubkey_bytes),
            signature = %hex::encode(signature_bytes),
            "🔍 SIGNATURE VERIFICATION: Starting verification"
        );

        // Try to parse the public key
        let verifying_key = match VerifyingKey::from_bytes(pubkey_bytes) {
            Ok(key) => {
                tracing::debug!("✅ Public key parsed successfully");
                key
            }
            Err(e) => {
                tracing::error!(
                    pubkey = %hex::encode(pubkey_bytes),
                    error = %e,
                    "❌ SIGNATURE VERIFICATION: Invalid public key format"
                );
                return Err(anyhow!("Invalid public key: {}", e));
            }
        };

        // Try to parse the signature - ed25519_dalek 2.1 doesn't return Result
        let signature = Signature::from_bytes(signature_bytes);
        tracing::debug!("✅ Signature parsed successfully");

        // Verify the signature
        match verifying_key.verify(message, &signature) {
            Ok(()) => {
                // Cache the verified key
                self.verified_keys_cache.insert(*pubkey_bytes, true);
                tracing::info!(
                    pubkey = %hex::encode(pubkey_bytes),
                    "✅ SIGNATURE VERIFICATION: Signature verified successfully"
                );
                Ok(true)
            }
            Err(e) => {
                tracing::warn!(
                    pubkey = %hex::encode(pubkey_bytes),
                    message = %hex::encode(message),
                    signature = %hex::encode(signature_bytes),
                    error = %e,
                    "❌ SIGNATURE VERIFICATION: Signature verification failed"
                );
                Ok(false)
            }
        }
    }

    /// Batch verify multiple signatures using ed25519-dalek batch API.
    ///
    /// Uses `ed25519_dalek::verify_batch()` for 2-3x speedup over individual
    /// verification. Falls back to individual verify on batch failure to
    /// identify which signatures are invalid.
    pub fn batch_verify_signatures(
        &mut self,
        messages: &[[u8; 32]],
        pubkeys: &[[u8; 32]],
        signatures: &[[u8; 64]],
    ) -> Result<(usize, usize)> {
        if messages.len() != pubkeys.len() || pubkeys.len() != signatures.len() {
            return Err(anyhow!(
                "Arrays length mismatch: messages={}, pubkeys={}, signatures={}",
                messages.len(),
                pubkeys.len(),
                signatures.len()
            ));
        }

        let total = messages.len();
        if total == 0 {
            return Ok((0, 0));
        }

        tracing::info!(
            total_signatures = total,
            "🔍 BATCH VERIFICATION: Starting batch signature verification"
        );

        // Parse all keys and signatures upfront
        let mut parsed_vkeys = Vec::with_capacity(total);
        let mut parsed_sigs = Vec::with_capacity(total);
        let mut msg_slices: Vec<&[u8]> = Vec::with_capacity(total);
        let mut parse_failures = 0usize;

        for i in 0..total {
            match VerifyingKey::from_bytes(&pubkeys[i]) {
                Ok(vk) => {
                    parsed_vkeys.push(vk);
                    parsed_sigs.push(Signature::from_bytes(&signatures[i]));
                    msg_slices.push(messages[i].as_slice());
                }
                Err(_) => {
                    parse_failures += 1;
                }
            }
        }

        // Try batch verify first (fast path — all valid)
        let (valid_count, invalid_count) = if parse_failures == 0
            && ed25519_dalek::verify_batch(&msg_slices, &parsed_sigs, &parsed_vkeys).is_ok()
        {
            // All signatures valid
            for pk in &pubkeys[..total] {
                self.verified_keys_cache.insert(*pk, true);
            }
            (total, 0)
        } else {
            // Fallback: identify valid/invalid individually
            let mut valid = 0usize;
            let mut invalid = parse_failures;
            for (vk, (sig, msg)) in parsed_vkeys
                .iter()
                .zip(parsed_sigs.iter().zip(msg_slices.iter()))
            {
                if vk.verify(msg, sig).is_ok() {
                    valid += 1;
                } else {
                    invalid += 1;
                }
            }
            (valid, invalid)
        };

        tracing::info!(
            total_signatures = total,
            valid_count,
            invalid_count,
            "📊 BATCH VERIFICATION: completed (batch Ed25519)"
        );

        Ok((valid_count, invalid_count))
    }

    /// Check if a public key has been previously verified
    pub fn is_key_verified(&self, pubkey: &[u8; 32]) -> bool {
        self.verified_keys_cache
            .get(pubkey)
            .copied()
            .unwrap_or(false)
    }

    /// Get cache statistics
    pub fn get_cache_stats(&self) -> SignatureCacheStats {
        SignatureCacheStats {
            cached_keys: self.verified_keys_cache.len(),
            verified_keys: self.verified_keys_cache.values().filter(|&&v| v).count(),
        }
    }

    /// Clear the verification cache
    pub fn clear_cache(&mut self) {
        self.verified_keys_cache.clear();
    }
}

impl Default for SignatureVerifier {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone)]
pub struct SignatureCacheStats {
    pub cached_keys: usize,
    pub verified_keys: usize,
}

/// Aggregate signature verifier using individual Ed25519 verification.
///
/// SECURITY: Without BLS, there is no cryptographically sound way to aggregate
/// Ed25519 signatures. This verifier checks each individual signature against
/// the shared message hash and requires a quorum threshold of valid signatures.
/// The `aggregated_signature` field is retained for wire compatibility but is
/// NOT trusted — only individual voter signatures are verified.
#[derive(Debug, Clone)]
pub struct AggregateSignatureVerifier;

impl AggregateSignatureVerifier {
    pub fn new() -> Self {
        Self
    }

    /// Verify individual voter signatures against a shared message hash.
    ///
    /// SECURITY: Each voter's Ed25519 signature is individually verified.
    /// The `aggregated_signature` parameter is ignored — it was previously
    /// an insecure XOR of individual signatures.
    ///
    /// Returns `Ok(true)` if all individual signatures are valid.
    pub fn verify_aggregate_signature(
        &self,
        message_hash: &[u8; 32],
        voter_pubkeys: &[[u8; 32]],
        voter_signatures: &[[u8; 64]],
        _aggregated_signature: &[u8; 64],
    ) -> Result<bool> {
        if voter_pubkeys.is_empty() || voter_signatures.is_empty() {
            return Ok(false);
        }

        if voter_pubkeys.len() != voter_signatures.len() {
            return Err(anyhow!("Voter pubkeys and signatures count mismatch"));
        }

        // Pre-check: reject zero signatures
        for (i, sig_bytes) in voter_signatures.iter().enumerate() {
            if sig_bytes.iter().all(|&b| b == 0) {
                tracing::warn!(index = i, "Aggregate verification: zero signature rejected");
                return Ok(false);
            }
        }

        // Parse all keys and signatures
        let mut vkeys = Vec::with_capacity(voter_pubkeys.len());
        let mut sigs = Vec::with_capacity(voter_signatures.len());
        let mut msgs: Vec<&[u8]> = Vec::with_capacity(voter_pubkeys.len());

        for (i, (pubkey, sig_bytes)) in voter_pubkeys
            .iter()
            .zip(voter_signatures.iter())
            .enumerate()
        {
            let vk = match VerifyingKey::from_bytes(pubkey) {
                Ok(key) => key,
                Err(e) => {
                    tracing::warn!(
                        index = i,
                        pubkey = %hex::encode(pubkey),
                        error = %e,
                        "Aggregate verification: invalid voter public key"
                    );
                    return Ok(false);
                }
            };
            vkeys.push(vk);
            sigs.push(Signature::from_bytes(sig_bytes));
            msgs.push(message_hash.as_slice());
        }

        // Batch verify all voter signatures at once (2-3x faster)
        match ed25519_dalek::verify_batch(&msgs, &sigs, &vkeys) {
            Ok(()) => Ok(true),
            Err(_) => {
                // Fallback: find which voter failed
                for (i, (vk, sig)) in vkeys.iter().zip(sigs.iter()).enumerate() {
                    if vk.verify(message_hash.as_slice(), sig).is_err() {
                        tracing::warn!(
                            index = i,
                            pubkey = %hex::encode(&voter_pubkeys[i]),
                            "Aggregate verification: voter signature failed"
                        );
                        return Ok(false);
                    }
                }
                Ok(false)
            }
        }
    }

    /// Create an aggregated signature from individual signatures.
    ///
    /// SECURITY NOTE: Without BLS, true aggregation is not possible.
    /// This produces a SHA-256-based fingerprint of all individual signatures
    /// signatures, not this aggregate.
    pub fn aggregate_signatures(signatures: &[[u8; 64]]) -> [u8; 64] {
        let mut hasher = Sha256::new();
        for sig in signatures {
            hasher.update(sig);
        }
        let hash = hasher.finalize();
        let mut result = [0u8; 64];
        // Use SHA-256 hash as first 32 bytes, repeat for remaining 32
        result[..32].copy_from_slice(&hash);
        result[32..].copy_from_slice(&hash);
        result
    }
}

impl Default for AggregateSignatureVerifier {
    fn default() -> Self {
        Self::new()
    }
}
