use super::{Storage, RocksDb};
use super::{Storage, CF_RECEIPTS, RocksDb};

impl Storage<RocksDb> {
    pub fn put_receipt<K: AsRef<[u8]>, V: AsRef<[u8]>>(
        &self,
        key: K,
        value: V,
    ) -> anyhow::Result<()> {
        self.put_cf(CF_RECEIPTS, key, value)
    }
    pub fn get_receipt<K: AsRef<[u8]>>(&self, key: K) -> anyhow::Result<Option<Vec<u8>>> {
        self.get_cf(CF_RECEIPTS, key)
    }
}
