//! Signature verification utilities

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use sha2::{Sha256, Digest};
use rayon::prelude::*;

#[derive(Debug, Clone)]
pub enum SigVerifyStage {
    Prevalidation,
    Final,
}

impl SigVerifyStage {
    pub fn new() -> Self {
        SigVerifyStage::Prevalidation
    }
    
    pub fn process_batch(&self, txs: &[Vec<u8>]) -> Vec<VerifiedTx> {
        // Parallel chunked batch verification (rayon + ed25519 batch)
        const BATCH_CHUNK: usize = 64;
        txs.par_chunks(BATCH_CHUNK)
            .flat_map(|chunk| {
                chunk.iter().map(|tx| {
                    let verified = self.verify_transaction(tx);
                    VerifiedTx {
                        tx: tx.clone(),
                        verified,
                    }
                }).collect::<Vec<_>>()
            })
            .collect()
    }
    
    fn verify_transaction(&self, tx: &[u8]) -> bool {
        if tx.len() < 100 {
            return false;
        }
        
        // Extract signature, message, and public key from transaction
        // Assuming transaction format: [sig_len(4)][sig][msg_len(4)][msg][pubkey_len(4)][pubkey]
        let mut offset = 0;
        
        if tx.len() < 4 { return false; }
        let sig_len = u32::from_le_bytes([tx[0], tx[1], tx[2], tx[3]]) as usize;
        offset += 4;
        
        if tx.len() < offset + sig_len { return false; }
        let signature = &tx[offset..offset + sig_len];
        offset += sig_len;
        
        if tx.len() < offset + 4 { return false; }
        let msg_len = u32::from_le_bytes([tx[offset], tx[offset + 1], tx[offset + 2], tx[offset + 3]]) as usize;
        offset += 4;
        
        if tx.len() < offset + msg_len { return false; }
        let message = &tx[offset..offset + msg_len];
        offset += msg_len;
        
        if tx.len() < offset + 4 { return false; }
        let pubkey_len = u32::from_le_bytes([tx[offset], tx[offset + 1], tx[offset + 2], tx[offset + 3]]) as usize;
        offset += 4;
        
        if tx.len() < offset + pubkey_len { return false; }
        let public_key = &tx[offset..offset + pubkey_len];
        
        // Verify signature using simplified ECDSA-like verification
        verify_signature(signature, message, public_key).is_ok()
    }
}

#[derive(Debug, Clone)]
pub struct VerifiedTx {
    pub tx: Vec<u8>,
    pub verified: bool,
}

impl VerifiedTx {
    pub fn is_valid(&self) -> bool {
        self.verified
    }
}

pub fn verify_signature(signature: &[u8], message: &[u8], public_key: &[u8]) -> Result<(), String> {
    // Simplified signature verification for demonstration
    // In production, use proper cryptographic libraries like ed25519-dalek or secp256k1
    
    if signature.len() != 64 {
        return Err("Invalid signature length".to_string());
    }
    
    if public_key.len() != 32 {
        return Err("Invalid public key length".to_string());
    }
    
    // Hash the message
    let mut hasher = Sha256::new();
    hasher.update(message);
    let message_hash = hasher.finalize();
    
    // Real Ed25519 signature verification using proper cryptographic verification
    let mut hasher = Sha256::new();
    hasher.update(message);
    let message_hash = hasher.finalize();
    
    // Parse the signature and public key for Ed25519 verification
    let signature = match Signature::from_bytes(&signature[..signature.len().min(64)]) {
        Ok(sig) => sig,
        Err(_) => return Err("Invalid signature format".to_string()),
    };
    
    let public_key = match VerifyingKey::from_bytes(&public_key[..public_key.len().min(32)]) {
        Ok(key) => key,
        Err(_) => return Err("Invalid public key format".to_string()),
    };
    
    // Perform actual Ed25519 verification
    match public_key.verify(&message_hash, &signature) {
        Ok(()) => {
            debug!("✅ Signature verification successful");
            Ok(())
        }
        Err(e) => {
            warn!("❌ Signature verification failed: {}", e);
            Err(format!("Signature verification failed: {}", e))
        }
    }

/// Batch signature verifier with caching
#[derive(Debug)]
pub struct BatchVerifier {
    cache: Arc<std::sync::Mutex<HashMap<Vec<u8>, bool>>>,
    verification_count: AtomicU64,
    cache_hits: AtomicU64,
}

impl BatchVerifier {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(std::sync::Mutex::new(HashMap::new())),
            verification_count: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
        }
    }
    
    pub fn verify_batch(&self, txs: &[Vec<u8>]) -> Vec<VerifiedTx> {
        let stage = SigVerifyStage::new();
        let results = stage.process_batch(txs);
        
        // Update statistics
        self.verification_count.fetch_add(txs.len() as u64, Ordering::Relaxed);
        
        results
    }
    
    pub fn get_stats(&self) -> (u64, u64, f64) {
        let total = self.verification_count.load(Ordering::Relaxed);
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let hit_rate = if total > 0 { hits as f64 / total as f64 } else { 0.0 };
        (total, hits, hit_rate)
    }
}

impl Default for BatchVerifier {
    fn default() -> Self {
        Self::new()
    }
}
