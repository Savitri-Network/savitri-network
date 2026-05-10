use super::{Storage, RocksDb};
// Storage sharding helpers: shard-mapped column families and 2PC-friendly locks.
// - Column families: account_to_shard, account_locks, accounts_shard_{0..7}
// - Mapping: address -> shard_id is persisted for fast lookup
// - Locking: lightweight lock record with expiry (ms) to support 2PC timeout/rollback

use crate::sharding::ShardingConfig;
use crate::storage::{
    CF_ACCOUNT_LOCKS, CF_ACCOUNT_TO_SHARD, CF_ACCOUNTS_SHARDS, Storage,
};
use crate::core::types::Account;
use anyhow::Context;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn shard_cf_name(shard: u16) -> anyhow::Result<&'static str> {
    let idx = shard as usize;
    if idx >= CF_ACCOUNTS_SHARDS.len() {
        anyhow::bail!("shard id {} out of bounds (max {})", shard, CF_ACCOUNTS_SHARDS.len() - 1);
    }
    Ok(CF_ACCOUNTS_SHARDS[idx])
}

impl Storage<RocksDb> {
    /// Resolve shard id for an address using persisted mapping; if missing, compute and persist.
    pub fn account_shard_id(&self, addr: &[u8], cfg: &ShardingConfig) -> anyhow::Result<u16> {
        if let Some(raw) = self.get_cf(CF_ACCOUNT_TO_SHARD, addr)? {
            if raw.len() == 2 {
                let mut bytes = [0u8; 2];
                bytes.copy_from_slice(&raw);
                return Ok(u16::from_le_bytes(bytes));
            }
        }
        let shard = cfg.shard_for_address(addr);
        self.put_cf(CF_ACCOUNT_TO_SHARD, addr, shard.to_le_bytes())?;
        Ok(shard)
    }

    /// Put account into its shard CF and update mapping; deletes mapping on empty account.
    pub fn put_account_sharded(
        &self,
        addr: &[u8],
        account: &Account,
        cfg: &ShardingConfig,
    ) -> anyhow::Result<()> {
        let shard = self.account_shard_id(addr, cfg)?;
        let cf_name = shard_cf_name(shard)?;

        if *account == Account::default() {
            self.delete_cf(cf_name, addr)?;
            self.delete_cf(CF_ACCOUNT_TO_SHARD, addr)?;
            self.cache.invalidate_account(addr);
            return Ok(());
        }

        let enc = account.encode();
        self.put_cf(cf_name, addr, enc)?;
        self.put_cf(CF_ACCOUNT_TO_SHARD, addr, shard.to_le_bytes())?;
        self.cache.invalidate_account(addr);
        Ok(())
    }

    /// Get account from its shard CF; falls back to mapping compute if missing.
    pub fn get_account_sharded(
        &self,
        addr: &[u8],
        cfg: &ShardingConfig,
    ) -> anyhow::Result<Option<Account>> {
        let shard = self.account_shard_id(addr, cfg)?;
        let cf_name = shard_cf_name(shard)?;

        match self.get_cf(cf_name, addr)? {
            Some(ref bytes) => Ok(Some(
                Account::decode(&bytes).context("decode sharded account")?,
            )),
            None => Ok(None),
        }
    }

    /// Delete account from shard CF and mapping.
    pub fn delete_account_sharded(&self, addr: &[u8], cfg: &ShardingConfig) -> anyhow::Result<()> {
        let shard = self.account_shard_id(addr, cfg)?;
        let cf_name = shard_cf_name(shard)?;
        self.delete_cf(cf_name, addr)?;
        self.delete_cf(CF_ACCOUNT_TO_SHARD, addr)?;
        self.cache.invalidate_account(addr);
        Ok(())
    }

    /// Try to acquire a 2PC lock for an address/shard with TTL. Returns true if acquired.
    pub fn try_lock_account_shard(
        &self,
        addr: &[u8],
        shard: u16,
        ttl: Duration,
    ) -> anyhow::Result<bool> {
        let now = now_millis();
        if let Some(raw) = self.get_cf(CF_ACCOUNT_LOCKS, addr)? {
            if raw.len() >= 10 {
                let mut shard_bytes = [0u8; 2];
                shard_bytes.copy_from_slice(&raw[0..2]);
                let locked_shard = u16::from_le_bytes(shard_bytes);

                let mut expiry_bytes = [0u8; 8];
                expiry_bytes.copy_from_slice(&raw[2..10]);
                let expiry = u64::from_le_bytes(expiry_bytes);

                if locked_shard == shard && now < expiry {
                    return Ok(false); // already locked and not expired
                }
            }
        }

        let expiry = now.saturating_add(ttl.as_millis() as u64);
        let mut buf = Vec::with_capacity(10);
        buf.extend_from_slice(&shard.to_le_bytes());
        buf.extend_from_slice(&expiry.to_le_bytes());
        self.put_cf(CF_ACCOUNT_LOCKS, addr, buf)?;
        Ok(true)
    }

    /// Release a lock for an address.
    pub fn release_account_lock(&self, addr: &[u8]) -> anyhow::Result<()> {
        self.delete_cf(CF_ACCOUNT_LOCKS, addr)?;
        Ok(())
    }

    /// Sweep expired locks (best-effort; safe to call periodically).
    pub fn purge_expired_account_locks(&self) -> anyhow::Result<u64> {
        let now = now_millis();
        let mut removed = 0u64;
        let cf = self.cf(CF_ACCOUNT_LOCKS).context("lock cf missing")?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        for item in iter {
            if let Ok((key, val)) = item {
                if val.len() >= 10 {
                    let mut expiry_bytes = [0u8; 8];
                    expiry_bytes.copy_from_slice(&val[2..10]);
                    let expiry = u64::from_le_bytes(expiry_bytes);
                    if now >= expiry {
                        self.delete_cf(CF_ACCOUNT_LOCKS, key)?;
                        removed += 1;
                    }
                }
            }
        }
        Ok(removed)
    }
}
