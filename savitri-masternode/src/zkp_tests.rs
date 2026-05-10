//! ZKP Integration Tests for Savitri Masternode

#[cfg(feature = "zkp")]
#[cfg(test)]
mod tests {
    use super::*;
    use crate::zkp_integration::{ZkpIntegrationManager, ZkpUtils};
    use savitri_consensus::{BlockHeader, BlockProposal};
    use savitri_zkp::{ZkpBackend, ZkpConfig};

    #[tokio::test]
    async fn test_zkp_integration_manager() {
        let config = ZkpConfig::development();
        let manager = ZkpIntegrationManager::new(config).unwrap();

        assert!(manager.is_enabled()); // Development uses Mock, should be false

        let proposal = BlockProposal {
            round_id: 1,
            height: 100,
            timestamp: 1234567890,
            proposer_pubkey: [1u8; 32],
            proposer_pou_score: 800,
            parent_hash: [2u8; 64],
            state_root: [3u8; 64],
            tx_root: [4u8; 64],
            transactions: vec![],
            signature: [5u8; 64],
            zkp_proof: None,
        };

        // Generate proof
        let proof: Vec<u8> = manager.generate_block_proof(&proposal).await.unwrap();
        assert!(!proof.is_empty());

        // Validate proof
        let is_valid: bool = manager
            .validate_block_proof(&proposal, &proof)
            .await
            .unwrap();
        assert!(is_valid);
    }

    #[test]
    fn test_zkp_utils() {
        let header = BlockHeader {
            version: 1,
            height: 100,
            timestamp: 1234567890,
            parent_hash: [1u8; 64],
            state_root: [2u8; 64],
            tx_root: [3u8; 64],
            proposer: [4u8; 32],
            slot: 1,
            epoch: 1,
            tx_count: 10,
            zkp_proof: None,
        };

        let statement = ZkpUtils::block_header_to_statement(&header);
        assert_eq!(statement.e, 100);
        assert_eq!(statement.f, 1234567890);

        let proposal = BlockProposal {
            round_id: 1,
            height: 100,
            timestamp: 1234567890,
            proposer_pubkey: [1u8; 32],
            proposer_pou_score: 800,
            parent_hash: [2u8; 64],
            state_root: [3u8; 64],
            tx_root: [4u8; 64],
            transactions: vec![],
            signature: [5u8; 64],
            zkp_proof: None,
        };

        let statement2 = ZkpUtils::block_proposal_to_statement(&proposal);
        assert_eq!(statement2.e, 100);
        assert_eq!(statement2.f, 1234567890);

        let proof_hash = ZkpUtils::proof_hash(&[1, 2, 3, 4]);
        assert_ne!(proof_hash, [0u8; 32]);
    }

    #[test]
    fn test_zkp_configurations() {
        // Test different configurations
        let dev_config = ZkpConfig::development();
        assert!(matches!(dev_config.backend, ZkpBackend::Mock));

        let prod_config = ZkpConfig::production();
        assert!(matches!(prod_config.backend, ZkpBackend::Arkworks));

        let test_config = ZkpConfig::testing();
        assert!(matches!(test_config.backend, ZkpBackend::Mock));
    }
}
