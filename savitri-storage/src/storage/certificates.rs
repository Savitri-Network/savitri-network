use super::{Storage, RocksDb};
use super::{Storage, CF_CERTIFICATES, RocksDb};
use crate::p2p::messages::ConsensusCertificate;
use crate::utils::bincode_utils::{serialize_consensus, deserialize_consensus};
use anyhow::Result;

#[cfg(test)]
mod tests;

/// Key format: height::<u64_be_bytes>
fn certificate_height_key(height: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.extend_from_slice(b"height::");
    key.extend_from_slice(&height.to_be_bytes());
    key
}

/// Key format: block_hash::<64_bytes>
fn certificate_block_key(block_hash: &[u8; 64]) -> Vec<u8> {
    let mut key = Vec::with_capacity(72);
    key.extend_from_slice(b"block::");
    key.extend_from_slice(block_hash);
    key
}

impl Storage<RocksDb> {
    /// Store a consensus certificate for a specific height and block hash
    pub fn put_certificate(&self, cert: &ConsensusCertificate) -> Result<()> {
        let cf = self.cf(CF_CERTIFICATES)?;
        // Use unified bincode configuration for consensus compatibility
        let value = serialize_consensus(cert)?;
        
        // Store by height
        let height_key = certificate_height_key(cert.height);
        self.db.put_cf(&cf, height_key, &value)?;
        
        // Store by block hash
        let block_key = certificate_block_key(&cert.block_hash.into_inner());
        self.db.put_cf(&cf, block_key, &value)?;
        
        Ok(())
    }
    
    /// Get certificate by height
    pub fn get_certificate_by_height(&self, height: u64) -> Result<Option<ConsensusCertificate>> {
        let cf = self.cf(CF_CERTIFICATES)?;
        let key = certificate_height_key(height);
        let value = self.db.get_cf(&cf, key)?;
        value.map(|v: &[u8]| {
            deserialize_consensus(&v)
                .map_err(|e| anyhow::anyhow!("failed to deserialize certificate at height {}: {} (data length: {})", height, e, v.len()))
        }).transpose()
    }
    
    /// Get certificate by block hash
    pub fn get_certificate_by_block_hash(&self, block_hash: &[u8; 64]) -> Result<Option<ConsensusCertificate>> {
        let cf = self.cf(CF_CERTIFICATES)?;
        let key = certificate_block_key(block_hash);
        let value = self.db.get_cf(&cf, key)?;
        value.map(|v: &[u8]| {
            deserialize_consensus(&v)
                .map_err(|e| anyhow::anyhow!("failed to deserialize certificate for block hash (data length: {}): {}", v.len(), e))
        }).transpose()
    }
    
    /// Check if a certificate exists for a given height
    pub fn has_certificate_at_height(&self, height: u64) -> Result<bool> {
        Ok(self.get_certificate_by_height(height)?.is_some())
    }
}
