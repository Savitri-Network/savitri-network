//! Integration Tests for Savitri Light Node Stub Implementation
//!
//! Tests verify that stub implementations compile and maintain API compatibility
//! with expected real implementations.

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use savitri_lightnode::tx::{
        Account, Block, MempoolPipeline, SigVerifyStage, SignedTx, Storage,
    };

    #[test]
    fn test_stub_compatibility() {
        // Test that stub types can be created and used
        let tx = SignedTx {
            from: vec![1, 2, 3],
            to: vec![4, 5, 6],
            amount: 1000,
            fee: 10,
            nonce: 1,
            signature: vec![7, 8, 9],
            memo: vec![],
        };

        assert_eq!(tx.amount, 1000);
        assert_eq!(tx.nonce, 1);
    }

    #[test]
    fn test_block_stub() {
        let tx = SignedTx {
            from: vec![1, 2, 3],
            to: vec![4, 5, 6],
            amount: 1000,
            fee: 10,
            nonce: 1,
            signature: vec![7, 8, 9],
            memo: vec![],
        };

        let block = Block {
            header: savitri_lightnode::tx::BlockHeader {
                version: 1,
                exec_height: 100,
                timestamp: 1234567890,
                parent_exec_hash: [0u8; 64],
                parent_ref_hash: None,
                state_root: [1u8; 64],
                tx_root: [2u8; 64],
                proposer: [3u8; 32],
            },
            transactions: vec![tx],
        };

        assert_eq!(block.header.exec_height, 100);
        assert_eq!(block.transactions.len(), 1);
    }

    #[test]
    fn test_storage_stub() -> Result<()> {
        let storage = Storage::new();
        let addr = [1u8; 32];

        // Test storage methods
        let account = storage.get_account(&addr)?;
        assert_eq!(account.balance, 0);
        assert_eq!(account.nonce, 0);

        storage.put_account(&addr, &account)?;

        let head = storage.get_chain_head()?;
        assert!(head.is_none());

        Ok(())
    }

    #[test]
    fn test_mempool_stub() {
        let _mempool = MempoolPipeline::new();
        // Test that MempoolPipeline can be created
        // More complex tests would require actual implementation
    }

    #[test]
    fn test_sigverify_stub() {
        let _sigverify = SigVerifyStage::new();
        // Test that SigVerifyStage can be created
        // More complex tests would require actual implementation
    }

    #[test]
    fn test_account_default() {
        let account = Account::default();
        assert_eq!(account.balance, 0);
        assert_eq!(account.nonce, 0);
    }

    #[test]
    fn test_serialization_compatibility() {
        let tx = SignedTx {
            from: vec![1, 2, 3],
            to: vec![4, 5, 6],
            amount: 1000,
            fee: 10,
            nonce: 1,
            signature: vec![7, 8, 9],
            memo: vec![],
        };

        // Test that stub types are serializable
        let serialized = bincode::serialize(&tx).expect("Failed to serialize");
        let deserialized: SignedTx =
            bincode::deserialize(&serialized).expect("Failed to deserialize");

        assert_eq!(tx.amount, deserialized.amount);
        assert_eq!(tx.nonce, deserialized.nonce);
    }
}
