use super::{Storage, RocksDb};
use super::{Storage, CF_BLOCKS, CF_TX};
use crate::core::block::Block;
use crate::core::types::Transaction;
use rocksdb::IteratorMode;

impl Storage<RocksDb> {
    // Typed wrappers: Blocks
    pub fn put_block(&self, block: &Block) -> anyhow::Result<()> {
        let cf = self.cf(CF_BLOCKS)?;
        let key = &block.hash;
        let value = bincode::serialize(block)?;
        Ok(self.db.put_cf(&cf, key, value)?)
    }

    // Convenience: compute state root and store block with it
    pub fn put_block_with_state_root(&self, mut block: Block) -> anyhow::Result<()> {
        block.state_root = self.compute_state_root()?;
        self.put_block(&block)
    }

    pub fn get_block(&self, hash: &[u8; 64]) -> anyhow::Result<Option<Block>> {
        let cf = self.cf(CF_BLOCKS)?;
        let value = self.db.get_cf(&cf, hash)?;
        Ok(value.map(|v: Vec<u8>| crate::safe_deserialize(&v)).transpose()?)
    }

    /// Clear all blocks from storage (used for bootstrap reset)
    pub fn clear_blocks(&self) -> anyhow::Result<()> {
        let cf = self.cf(CF_BLOCKS)?;
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        for entry in iter {
            let (key, _): (Box<[u8]>, Box<[u8]>) = entry?;
            self.db.delete_cf(&cf, key)?;
        }
        Ok(())
    }

    /// Clear all height->hash mappings from meta (used for bootstrap reset)
    pub fn clear_height_mappings(&self) -> anyhow::Result<()> {
        use super::CF_META;
        let cf = self.cf(CF_META)?;
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);
        let height_prefix = b"height::";
        for entry in iter {
            let (key, _): (Box<[u8]>, Box<[u8]>) = entry?;
            // Only delete height mappings, not other meta keys
            if key.starts_with(height_prefix) {
                self.db.delete_cf(&cf, key)?;
            }
        }
        Ok(())
    }

    // Typed wrappers: Transactions (key supplied by caller, e.g., tx hash)
    pub fn put_tx<K: AsRef<[u8]>>(&self, key: K, tx: &Transaction) -> anyhow::Result<()> {
        let cf = self.cf(CF_TX)?;
        let value = bincode::serialize(tx)?;
        Ok(self.db.put_cf(&cf, key.as_ref(), value)?)
    }

    pub fn get_tx<K: AsRef<[u8]>>(&self, key: K) -> anyhow::Result<Option<Transaction>> {
        let cf = self.cf(CF_TX)?;
        let value = self.db.get_cf(&cf, key.as_ref())?;
        Ok(value.map(|v: Vec<u8>| crate::safe_deserialize(&v)).transpose()?)
    }
}
