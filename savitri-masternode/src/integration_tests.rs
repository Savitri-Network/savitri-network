//! Integration Tests for Lightnode → Masternode Communication Flow
//!
//! This module provides comprehensive tests for verifying the complete
//! communication flow between lightnodes and masternodes.

#[cfg(test)]
mod tests {
    use crate::block_messages::{
        BlockProposal, BlockValidationResult, ConsensusCertificate, MempoolSyncMessage, Transaction,
    };
    use crate::libp2p_network::{HeartbeatKind, HeartbeatMessage, PeerInfoMessage, PouBroadcast};
    use crate::transaction_validator::{
        ExecutionStatus, TransactionValidator, ValidatedTransaction,
    };

    fn current_timestamp() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    // ============================================
    // TRANSACTION VALIDATION TESTS
    // ============================================

    #[test]
    fn test_transaction_validation_all_unique() {
        let mut validator = TransactionValidator::new();
        let transactions = create_test_transactions(5);

        let result = validator.validate_block_transactions(transactions, "group_a".to_string());

        assert!(result.is_accepted);
        assert_eq!(result.total_transactions, 5);
        assert_eq!(result.unique_transactions, 5);
        assert_eq!(result.uniqueness_ratio, 1.0);
        assert!(result.duplicate_hashes.is_empty());
    }

    #[test]
    fn test_transaction_validation_with_duplicates_below_threshold() {
        let mut validator = TransactionValidator::new();

        // First batch - all unique
        let batch1 = create_test_transactions(5);
        let result1 = validator.validate_block_transactions(batch1.clone(), "group_a".to_string());
        assert!(result1.is_accepted);

        // Second batch - 3 duplicates out of 5 (40% unique < 80% threshold)
        let mut batch2 = vec![
            batch1[0].clone(), // duplicate
            batch1[1].clone(), // duplicate
            batch1[2].clone(), // duplicate
            create_test_transaction(100),
            create_test_transaction(101),
        ];

        let result2 = validator.validate_block_transactions(batch2, "group_b".to_string());

        assert!(!result2.is_accepted); // Should be rejected (40% < 80%)
        assert_eq!(result2.total_transactions, 5);
        assert_eq!(result2.unique_transactions, 2);
        assert_eq!(result2.duplicate_hashes.len(), 3);
    }

    #[test]
    fn test_transaction_validation_exactly_80_percent() {
        let mut validator = TransactionValidator::new();

        // First batch - cache 1 transaction
        let tx1 = create_test_transaction(1);
        let batch1 = vec![tx1.clone()];
        validator.validate_block_transactions(batch1, "group_a".to_string());

        // Second batch - 4 unique + 1 duplicate = 80% unique
        let batch2 = vec![
            tx1.clone(), // duplicate
            create_test_transaction(2),
            create_test_transaction(3),
            create_test_transaction(4),
            create_test_transaction(5),
        ];

        let result = validator.validate_block_transactions(batch2, "group_b".to_string());

        assert!(result.is_accepted); // Exactly 80% should pass
        assert_eq!(result.uniqueness_ratio, 0.8);
    }

    #[test]
    fn test_transaction_validation_empty_block() {
        let mut validator = TransactionValidator::new();
        let result = validator.validate_block_transactions(vec![], "group_a".to_string());

        assert!(!result.is_accepted); // Empty blocks should be rejected
        assert_eq!(result.total_transactions, 0);
        assert_eq!(result.uniqueness_ratio, 0.0);
    }

    // ============================================
    // BLOCK PROPOSAL TESTS
    // ============================================

    #[test]
    fn test_block_proposal_creation() {
        let transactions = vec![
            Transaction {
                tx_hash: [1u8; 32],
                sender: [2u8; 32],
                receiver: [3u8; 32],
                amount: 100,
                nonce: 1,
                signature: [4u8; 64],
            },
            Transaction {
                tx_hash: [5u8; 32],
                sender: [6u8; 32],
                receiver: [7u8; 32],
                amount: 200,
                nonce: 2,
                signature: [8u8; 64],
            },
        ];

        let proposal = BlockProposal::new(
            [9u8; 64],
            "group_a".to_string(),
            100,
            transactions,
            [10u8; 64],
        );

        assert_eq!(proposal.proposer_group_id, "group_a");
        assert_eq!(proposal.height, 100);
        assert_eq!(proposal.transaction_count(), 2);
        assert_eq!(proposal.get_transaction_hashes().len(), 2);
    }

    #[test]
    fn test_block_validation_result_summary() {
        let validation_result = crate::transaction_validator::ValidationResult {
            validated_transactions: vec![],
            duplicate_hashes: vec![],
            total_transactions: 10,
            unique_transactions: 8,
            uniqueness_ratio: 0.8,
            is_accepted: true,
        };

        let result = BlockValidationResult::new(
            [1u8; 64],
            "group_a".to_string(),
            validation_result,
            [2u8; 64],
        );

        assert!(result.is_accepted());
        let summary = result.get_summary();
        assert!(summary.contains("ACCEPTED"));
        assert!(summary.contains("80.0%"));
    }

    // ============================================
    // CONSENSUS CERTIFICATE TESTS
    // ============================================

    #[test]
    fn test_consensus_certificate_valid() {
        let voter_sigs = vec![[1u8; 64], [2u8; 64], [3u8; 64]];
        let agg_sig = [4u8; 64];

        let cert = ConsensusCertificate::new(
            [5u8; 64],
            100,
            "group_a".to_string(),
            current_timestamp(),
            voter_sigs,
            agg_sig,
        );

        assert!(cert.is_valid());
        assert_eq!(cert.voter_count(), 3);
    }

    #[test]
    fn test_consensus_certificate_invalid_empty_voters() {
        let cert = ConsensusCertificate::new(
            [1u8; 64],
            100,
            "group_a".to_string(),
            current_timestamp(),
            vec![], // Empty voters
            [2u8; 64],
        );

        assert!(!cert.is_valid());
    }

    #[test]
    fn test_consensus_certificate_invalid_zero_signature() {
        let cert = ConsensusCertificate::new(
            [1u8; 64],
            100,
            "group_a".to_string(),
            current_timestamp(),
            vec![[1u8; 64]],
            [0u8; 64], // Zero aggregated signature
        );

        assert!(!cert.is_valid());
    }

    // ============================================
    // MEMPOOL SYNC TESTS
    // ============================================

    #[test]
    fn test_mempool_sync_message_creation() {
        let confirmed = vec![[1u8; 32], [2u8; 32], [3u8; 32]];
        let rejected = vec![[4u8; 32]];

        let sync_msg = MempoolSyncMessage::new([5u8; 64], confirmed.clone(), rejected.clone());

        assert_eq!(sync_msg.total_transactions(), 4);
        assert_eq!(sync_msg.confirmed_transactions.len(), 3);
        assert_eq!(sync_msg.rejected_transactions.len(), 1);
    }

    // ============================================
    // MESSAGE SERIALIZATION TESTS
    // ============================================

    #[test]
    fn test_heartbeat_message_serialization() {
        let heartbeat = HeartbeatMessage {
            timestamp: current_timestamp(),
            nonce: 12345,
            kind: HeartbeatKind::Ping,
        };

        let serialized = serde_json::to_vec(&heartbeat).unwrap();
        let deserialized: HeartbeatMessage = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.nonce, 12345);
    }

    #[test]
    fn test_pou_broadcast_serialization() {
        let pou = PouBroadcast {
            peer_id: "12D3KooWtest".to_string(),
            epoch: 100,
            score: 500,
            index: 1,
            timestamp: current_timestamp(),
        };

        let serialized = serde_json::to_vec(&pou).unwrap();
        let deserialized: PouBroadcast = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.peer_id, "12D3KooWtest");
        assert_eq!(deserialized.score, 500);
    }

    #[test]
    fn test_peer_info_message_serialization() {
        let peer_info = PeerInfoMessage {
            account: [42u8; 32],
        };

        let serialized = serde_json::to_vec(&peer_info).unwrap();
        let deserialized: PeerInfoMessage = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.account, [42u8; 32]);
    }

    #[test]
    fn test_block_proposal_serialization() {
        let proposal = BlockProposal::new(
            [1u8; 64],
            "group_a".to_string(),
            100,
            vec![Transaction {
                tx_hash: [2u8; 32],
                sender: [3u8; 32],
                receiver: [4u8; 32],
                amount: 1000,
                nonce: 1,
                signature: [5u8; 64],
            }],
            [6u8; 64],
        );

        let serialized = serde_json::to_vec(&proposal).unwrap();
        let deserialized: BlockProposal = serde_json::from_slice(&serialized).unwrap();

        assert_eq!(deserialized.height, 100);
        assert_eq!(deserialized.transaction_count(), 1);
    }

    // ============================================
    // MULTI-GROUP CONSENSUS FLOW TESTS
    // ============================================

    #[test]
    fn test_multi_group_transaction_isolation() {
        let mut validator = TransactionValidator::new();

        // Group A processes transactions
        let group_a_txs = create_test_transactions(5);
        let result_a =
            validator.validate_block_transactions(group_a_txs.clone(), "group_a".to_string());
        assert!(result_a.is_accepted);

        // Group B tries to process same transactions
        let result_b =
            validator.validate_block_transactions(group_a_txs.clone(), "group_b".to_string());
        assert!(!result_b.is_accepted); // All duplicates = 0% unique
        assert_eq!(result_b.unique_transactions, 0);

        // Group C processes different transactions
        let group_c_txs = create_test_transactions_with_offset(5, 100);
        let result_c = validator.validate_block_transactions(group_c_txs, "group_c".to_string());
        assert!(result_c.is_accepted); // All unique
    }

    #[test]
    fn test_sequential_block_validation() {
        let mut validator = TransactionValidator::new();

        // Block 1 - 10 transactions
        let block1_txs = create_test_transactions(10);
        let result1 = validator.validate_block_transactions(block1_txs, "group_a".to_string());
        assert!(result1.is_accepted);

        // Block 2 - 10 new transactions
        let block2_txs = create_test_transactions_with_offset(10, 100);
        let result2 = validator.validate_block_transactions(block2_txs, "group_a".to_string());
        assert!(result2.is_accepted);

        // Block 3 - 10 new transactions
        let block3_txs = create_test_transactions_with_offset(10, 200);
        let result3 = validator.validate_block_transactions(block3_txs, "group_a".to_string());
        assert!(result3.is_accepted);

        // Verify cache growth
        let stats = validator.get_cache_stats();
        assert_eq!(stats.total_cached, 30);
    }

    // ============================================
    // HELPER FUNCTIONS
    // ============================================

    fn create_test_transaction(id: u32) -> ValidatedTransaction {
        ValidatedTransaction {
            tx_hash: create_hash_from_id(id),
            sender: [id as u8; 32],
            receiver: [(id + 100) as u8; 32],
            amount: id as u64 * 100,
            nonce: id as u64,
            signature: [id as u8; 64],
            processing_group_id: None,
            execution_status: ExecutionStatus::Pending,
            processed_at: None,
            block_hash: None,
            is_duplicate: false,
        }
    }

    fn create_test_transactions(count: usize) -> Vec<ValidatedTransaction> {
        (1..=count as u32).map(create_test_transaction).collect()
    }

    fn create_test_transactions_with_offset(
        count: usize,
        offset: u32,
    ) -> Vec<ValidatedTransaction> {
        ((offset + 1)..=(offset + count as u32))
            .map(create_test_transaction)
            .collect()
    }

    fn create_hash_from_id(id: u32) -> [u8; 32] {
        let mut hash = [0u8; 32];
        let id_bytes = id.to_le_bytes();
        hash[0..4].copy_from_slice(&id_bytes);
        hash
    }
}
