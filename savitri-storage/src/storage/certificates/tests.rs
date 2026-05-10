#[cfg(test)]
mod tests {
    use crate::storage::Storage;
    use crate::p2p::messages::{ConsensusCertificate, Hash64};
    use tempfile::TempDir;

    #[test]
    fn test_certificate_bincode_roundtrip() -> anyhow::Result<()> {
        // Create test storage
        let temp_dir = TempDir::new()?;
        let storage = Storage<RocksDb>::new(temp_dir.path())?;
        
        // Create test certificate
        let block_hash = [0x42u8; 64];
        let cert = ConsensusCertificate::new(
            1, // epoch_id
            0, // committee_id  
            0, // height
            0, // round
            Hash64::new(block_hash),
            vec![[1u8; 32], [2u8; 32], [3u8; 32]], // voters
            vec![0xAB; 64], // mock aggregated signature
        );
        
        // Test storage round-trip
        storage.put_certificate(&cert)?;
        
        let retrieved_cert = storage.get_certificate_by_height(0)?
            .ok_or_else(|| anyhow::anyhow!("Certificate not found"))?;
        
        assert_eq!(cert.height, retrieved_cert.height);
        assert_eq!(cert.epoch_id, retrieved_cert.epoch_id);
        assert_eq!(cert.block_hash, retrieved_cert.block_hash);
        assert_eq!(cert.voters, retrieved_cert.voters);
        
        // Test retrieval by block hash
        let retrieved_by_hash = storage.get_certificate_by_block_hash(&block_hash)?
            .ok_or_else(|| anyhow::anyhow!("Certificate not found by hash"))?;
        
        assert_eq!(cert.height, retrieved_by_hash.height);
        assert_eq!(cert.epoch_id, retrieved_by_hash.epoch_id);
        
        // Test has_certificate_at_height
        assert!(storage.has_certificate_at_height(0)?);
        assert!(!storage.has_certificate_at_height(999)?);
        
        Ok(())
    }
}
