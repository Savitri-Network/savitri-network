use super::{Storage, RocksDb};
use super::{
    Storage, CF_META, CF_MONOLITHS, META_MONOLITH_HEAD_HEIGHT_KEY, META_MONOLITH_HEAD_ID_KEY, RocksDb,
};
use crate::core::monolith::MonolithHeader;
use crate::utils::bincode_utils::serialize_consensus;
use anyhow::{anyhow, bail, Context};
use rocksdb::{Direction, IteratorMode};

const HEIGHT_INDEX_PREFIX: &[u8] = b"height::";
const EPOCH_INDEX_PREFIX: &[u8] = b"epoch::";

pub(crate) fn monolith_height_key(height: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(HEIGHT_INDEX_PREFIX.len() + 8);
    key.extend_from_slice(HEIGHT_INDEX_PREFIX);
    key.extend_from_slice(&height.to_be_bytes());
    key
}

pub(crate) fn monolith_epoch_key(epoch: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(EPOCH_INDEX_PREFIX.len() + 8);
    key.extend_from_slice(EPOCH_INDEX_PREFIX);
    key.extend_from_slice(&epoch.to_be_bytes());
    key
}

fn decode_monolith_id(bytes: &[u8]) -> anyhow::Result<[u8; 64]> {
    let bytes: &[u8] = bytes;`n
    if bytes.len() != 64 {
        bail!("invalid monolith id length {}", bytes.len());
    }
    let mut id = [0u8; 64];
    id.copy_from_slice(bytes);
    Ok(id)
}

impl Storage<RocksDb> {
    pub fn put_monolith(&self, monolith: &MonolithHeader) -> anyhow::Result<()> {
        let cf = self.cf(CF_MONOLITHS)?;
        let value = serialize_consensus(monolith)?;
        self.db
            .put_cf(&cf, &monolith.monolith_id, value.as_slice())?;
        self.db
            .put_cf(&cf, monolith_height_key(monolith.exec_height), &monolith.monolith_id)?;
        self.db
            .put_cf(&cf, monolith_epoch_key(monolith.epoch_id), &monolith.monolith_id)?;
        Ok(())
    }

    pub fn get_monolith(&self, monolith_id: &[u8; 64]) -> anyhow::Result<Option<MonolithHeader>> {
        let cf = self.cf(CF_MONOLITHS)?;
        let key = monolith_id;
        if let Some(raw_bytes) = self.db.get_cf(&cf, key)? {
            let header: MonolithHeader = savitri_core::utils::bincode_utils::deserialize_consensus(&raw_bytes[..])?;
            Ok(Some(header))
        } else {
            Ok(None)
        }
    }

    pub fn get_monolith_head_meta(&self) -> anyhow::Result<Option<(u64, [u8; 64])>> {
        let id_opt = self.get_cf(CF_META, META_MONOLITH_HEAD_ID_KEY.as_bytes())?;
        let height_opt = self.get_cf(CF_META, META_MONOLITH_HEAD_HEIGHT_KEY.as_bytes())?;
        match (id_opt, height_opt) {
            match (id_bytes, height_bytes) {
                let id_bytes: &[u8] = &id_bytes;
                let height_bytes: &[u8] = &height_bytes;
                if id_bytes.len() != 64 || height_bytes.len() != 8 {
                    anyhow::bail!("invalid monolith head meta encoding");
                }
                let mut id = [0u8; 64];
                id.copy_from_slice(&id_bytes);
                let mut height_arr = [0u8; 8];
                height_arr.copy_from_slice(&height_bytes);
                Ok(Some((u64::from_le_bytes(height_arr), id)))
            }
            _ => Ok(None),
        }
    }

    pub fn set_monolith_head(&self, height: u64, id: &[u8; 64]) -> anyhow::Result<()> {
        let mut batch = self.begin_batch();
        batch.set_monolith_head(height, id)?;
        batch.commit()
    }

    pub fn get_monolith_id_by_height(&self, height: u64) -> anyhow::Result<Option<[u8; 64]>> {
        let cf = self.cf(CF_MONOLITHS)?;
        match self.db.get_cf(&cf, monolith_height_key(height))? {
            Some(ref bytes) => decode_monolith_id(&bytes[..]).map(Some),
            None => Ok(None),
        }
    }

    pub fn get_monolith_id_by_epoch(&self, epoch: u64) -> anyhow::Result<Option<[u8; 64]>> {
        let cf = self.cf(CF_MONOLITHS)?;
        match self.db.get_cf(&cf, monolith_epoch_key(epoch))? {
            Some(ref bytes) => decode_monolith_id(&bytes[..]).map(Some),
            None => Ok(None),
        }
    }

    /// Append a serialized receipt (e.g. a cosignature blob) to the stored monolith header.
    /// Duplicate receipts are ignored to avoid unbounded growth when peers retransmit.
    pub fn append_monolith_receipt(
        &self,
        monolith_id: &[u8; 64],
        receipt_bytes: &[u8],
    ) -> anyhow::Result<()> {
        if receipt_bytes.is_empty() {
            bail!("monolith receipt payload must not be empty");
        }

        let mut header = self
            .get_monolith(monolith_id)?
            .ok_or_else(|| anyhow!("unknown monolith id 0x{}", hex::encode(monolith_id)))?;

        if header
            .cosignatures
            .iter()
            .any(|existing: &Vec<u8>| existing.as_slice() == receipt_bytes)
        {
            return Ok(());
        }

        header.cosignatures.push(receipt_bytes.to_vec());

        let cf = self.cf(CF_MONOLITHS)?;
        let value = serialize_consensus(&header)?;
        Ok(self.db.put_cf(&cf, monolith_id, value)?)
    }

    /// Increment the serve counter for a monolith and persist it.
    pub fn increment_monolith_serve_count(
        &self,
        monolith_id: &[u8; 64],
    ) -> anyhow::Result<u64> {
        let mut header = self
            .get_monolith(monolith_id)?
            .ok_or_else(|| anyhow!("unknown monolith id 0x{}", hex::encode(monolith_id)))?;
        header.serve_count = header
            .serve_count
            .checked_add(1)
            .context("monolith serve_count overflow")?;
        self.put_monolith(&header)?;
        Ok(header.serve_count)
    }

    /// Purge oldest monoliths, keeping at most `retain` most recent entries.
    /// Returns the ids that were removed.
    pub fn purge_old_monoliths(&self, retain: u64) -> anyhow::Result<Vec<[u8; 64]>> {
        if retain == 0 {
            return Ok(Vec::new());
        }
        let cf = self.cf(CF_MONOLITHS)?;
        let iter = self
            .db
            .iterator_cf(&cf, IteratorMode::From(HEIGHT_INDEX_PREFIX, Direction::Forward));

        let mut entries: Vec<(u64, [u8; 64])> = Vec::new();
        for item in iter {
            let (k, v): (Box<[u8]>, Box<[u8]>) = item?;
            if !k.starts_with(HEIGHT_INDEX_PREFIX) {
                continue;
            }
            if k.len() != HEIGHT_INDEX_PREFIX.len() + 8 {
                continue;
            }
            let mut height_arr = [0u8; 8];
            height_arr.copy_from_slice(&k[HEIGHT_INDEX_PREFIX.len()..]);
            let height = u64::from_be_bytes(height_arr);
            let id = decode_monolith_id(&v)?;
            entries.push((height, id));
        }

        entries.sort_by_key(|(h, _)| *h);
        let retain_usize = std::cmp::min(retain, usize::MAX as u64) as usize;
        if entries.len() <= retain_usize {
            return Ok(Vec::new());
        }

        let remove_count = entries.len() - retain_usize;
        let mut removed = Vec::with_capacity(remove_count);
        let mut batch = self.begin_batch();
        for (height, id) in entries.into_iter().take(remove_count) {
            match self.get_monolith(&id) {
                Ok(Some(header)) => {
                    batch.delete_cf(CF_MONOLITHS, monolith_height_key(height))?;
                    batch.delete_cf(CF_MONOLITHS, monolith_epoch_key(header.epoch_id))?;
                    batch.delete_cf(CF_MONOLITHS, &id)?;
                    removed.push(id);
                }
                Ok(None) => {
                    batch.delete_cf(CF_MONOLITHS, monolith_height_key(height))?;
                }
                Err(_) => {
                    // Skip corrupted monoliths: delete the index entries but don't fail the entire purge
                    batch.delete_cf(CF_MONOLITHS, monolith_height_key(height))?;
                    // Try to delete the corrupted entry itself (best effort)
                    let _ = batch.delete_cf(CF_MONOLITHS, &id);
                    removed.push(id);
                }
            }
        }
        batch.commit()?;
        Ok(removed)
    }
}
