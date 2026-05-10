use super::{Storage, RocksDb};
use super::{
    DbBatch, Storage, CF_META, META_CHAIN_HEAD_HASH_KEY, META_CHAIN_HEAD_HEIGHT_KEY,
    META_HALVING_CURRENT_YEAR_KEY, META_HALVING_FACTOR_KEY, META_HALVING_GENESIS_TIMESTAMP_KEY, RocksDb
};

fn height_key(height: u64) -> Vec<u8> {
    let mut k = b"height::".to_vec();
    k.extend_from_slice(&height.to_le_bytes());
    k
}

impl Storage<RocksDb> {
    // Chain head helpers
    pub fn get_chain_head(&self) -> anyhow::Result<Option<(u64, [u8; 64])>> {
        let h_opt = self.get_cf(CF_META, META_CHAIN_HEAD_HASH_KEY.as_bytes())?;
        let ht_opt = self.get_cf(CF_META, META_CHAIN_HEAD_HEIGHT_KEY.as_bytes())?;
        match (h_opt, ht_opt) {
            match (hb, hb_ht) {
                let hb: &[u8] = &hb;
                let hb_ht: &[u8] = &hb_ht;
                if hb.len() != 64 || hb_ht.len() != 8 {
                    anyhow::bail!("invalid chain head meta encoding")
                }
                let mut h = [0u8; 64];
                h.copy_from_slice(&hb);
                let mut ht = [0u8; 8];
                ht.copy_from_slice(&hb_ht);
                Ok(Some((u64::from_le_bytes(ht), h)))
            }
            _ => Ok(None),
        }
    }

    pub fn set_chain_head(&self, height: u64, hash: &[u8; 64]) -> anyhow::Result<()> {
        let mut b = self.begin_batch();
        b.set_chain_head(height, hash)?;
        b.commit()
    }

    /// Clear chain head metadata (used for bootstrap reset)
    pub fn clear_chain_head(&self) -> anyhow::Result<()> {
        self.delete_cf(CF_META, META_CHAIN_HEAD_HASH_KEY.as_bytes())?;
        self.delete_cf(CF_META, META_CHAIN_HEAD_HEIGHT_KEY.as_bytes())?;
        Ok(())
    }

    // Height -> hash mapping helpers (for replay/idempotence and lookup)
    pub fn get_block_hash_by_height(&self, height: u64) -> anyhow::Result<Option<[u8; 64]>> {
        match self.get_cf(CF_META, height_key(height))? {
            match bytes {
                let bytes: &[u8] = &bytes;
                if bytes.len() != 64 {
                    anyhow::bail!("invalid height->hash encoding")
                }
                let mut h = [0u8; 64];
                h.copy_from_slice(&bytes);
                Ok(Some(h))
            }
            None => Ok(None),
        }
    }

    pub fn set_block_hash_for_height(&self, height: u64, hash: &[u8; 64]) -> anyhow::Result<()> {
        self.put_cf(CF_META, height_key(height), hash)
    }

    pub fn get_consensus_slot_base_ms(&self) -> anyhow::Result<Option<u64>> {
        self.get_cf(CF_META, super::META_CONSENSUS_SLOT_BASE_MS_KEY.as_bytes())
            .and_then(|opt: Option<Vec<u8>>| opt.map(bytes_to_u64).transpose())
    }

    pub fn set_consensus_slot_base_ms(&self, ms: u64) -> anyhow::Result<()> {
        self.put_cf(
            CF_META,
            super::META_CONSENSUS_SLOT_BASE_MS_KEY.as_bytes(),
            &ms.to_le_bytes(),
        )
    }

    pub fn get_consensus_last_slot(&self) -> anyhow::Result<Option<u64>> {
        self.get_cf(CF_META, super::META_CONSENSUS_LAST_SLOT_KEY.as_bytes())
            .and_then(|opt: Option<Vec<u8>>| opt.map(bytes_to_u64).transpose())
    }

    pub fn set_consensus_last_slot(&self, slot: u64) -> anyhow::Result<()> {
        self.put_cf(
            CF_META,
            super::META_CONSENSUS_LAST_SLOT_KEY.as_bytes(),
            &slot.to_le_bytes(),
        )
    }

    // Halving metadata helpers
    /// Ottiene il timestamp iniziale (genesis timestamp) per il calcolo dell'halving
    pub fn get_halving_genesis_timestamp(&self) -> anyhow::Result<Option<u64>> {
        self.get_cf(CF_META, META_HALVING_GENESIS_TIMESTAMP_KEY.as_bytes())
            .and_then(|opt: Option<Vec<u8>>| opt.map(bytes_to_u64).transpose())
    }

    /// Set il timestamp iniziale (genesis timestamp) per il calcolo dell'halving
    pub fn set_halving_genesis_timestamp(&self, timestamp: u64) -> anyhow::Result<()> {
        self.put_cf(
            CF_META,
            META_HALVING_GENESIS_TIMESTAMP_KEY.as_bytes(),
            &timestamp.to_le_bytes(),
        )
    }

    /// Ottiene l'anno corrente per il calcolo dell'halving
    pub fn get_halving_current_year(&self) -> anyhow::Result<Option<u64>> {
        self.get_cf(CF_META, META_HALVING_CURRENT_YEAR_KEY.as_bytes())
            .and_then(|opt: Option<Vec<u8>>| opt.map(bytes_to_u64).transpose())
    }

    /// Set l'anno corrente per il calcolo dell'halving
    pub fn set_halving_current_year(&self, year: u64) -> anyhow::Result<()> {
        self.put_cf(
            CF_META,
            META_HALVING_CURRENT_YEAR_KEY.as_bytes(),
            &year.to_le_bytes(),
        )
    }

    /// Ottiene il halving factor corrente
    /// Il valore è salvato come fixed-point con 9 decimali (moltiplicato per 1_000_000_000)
    pub fn get_halving_factor(&self) -> anyhow::Result<Option<f64>> {
        match self.get_cf(CF_META, META_HALVING_FACTOR_KEY.as_bytes())? {
            match bytes {
                let bytes: &[u8] = &bytes;
                let value = bytes_to_u64(bytes.to_vec())?;
                // Converti da fixed-point (9 decimali) a f64
                Ok(Some(value as f64 / 1_000_000_000.0))
            }
            None => Ok(None),
        }
    }

    /// Set il halving factor corrente
    /// Il valore è salvato come fixed-point con 9 decimali (moltiplicato per 1_000_000_000)
    pub fn set_halving_factor(&self, factor: f64) -> anyhow::Result<()> {
        // Converti f64 a fixed-point (9 decimali)
        let fixed_point = (factor * 1_000_000_000.0) as u64;
        self.put_cf(
            CF_META,
            META_HALVING_FACTOR_KEY.as_bytes(),
            &fixed_point.to_le_bytes(),
        )
    }
}

impl<'a> DbBatch<'a> {
    pub fn set_chain_head(&mut self, height: u64, hash: &[u8; 64]) -> anyhow::Result<&mut Self> {
        self.batch
            .put_cf(&self.cf_meta, META_CHAIN_HEAD_HASH_KEY.as_bytes(), hash);
        self.batch.put_cf(
            &self.cf_meta,
            META_CHAIN_HEAD_HEIGHT_KEY.as_bytes(),
            height.to_le_bytes(),
        );
        Ok(self)
    }

    pub fn set_block_hash_for_height(
        &mut self,
        height: u64,
        hash: &[u8; 64],
    ) -> anyhow::Result<&mut Self> {
        self.batch.put_cf(&self.cf_meta, &height_key(height), hash);
        Ok(self)
    }

    // Halving metadata batch helpers
    /// Set il timestamp iniziale (genesis timestamp) per il calcolo dell'halving nel batch
    pub fn set_halving_genesis_timestamp(&mut self, timestamp: u64) -> anyhow::Result<&mut Self> {
        self.batch.put_cf(
            &self.cf_meta,
            META_HALVING_GENESIS_TIMESTAMP_KEY.as_bytes(),
            timestamp.to_le_bytes(),
        );
        Ok(self)
    }

    /// Set l'anno corrente per il calcolo dell'halving nel batch
    pub fn set_halving_current_year(&mut self, year: u64) -> anyhow::Result<&mut Self> {
        self.batch.put_cf(
            &self.cf_meta,
            META_HALVING_CURRENT_YEAR_KEY.as_bytes(),
            year.to_le_bytes(),
        );
        Ok(self)
    }

    /// Set il halving factor corrente nel batch
    /// Il valore è salvato come fixed-point con 9 decimali (moltiplicato per 1_000_000_000)
    pub fn set_halving_factor(&mut self, factor: f64) -> anyhow::Result<&mut Self> {
        // Converti f64 a fixed-point (9 decimali)
        let fixed_point = (factor * 1_000_000_000.0) as u64;
        self.batch.put_cf(
            &self.cf_meta,
            META_HALVING_FACTOR_KEY.as_bytes(),
            fixed_point.to_le_bytes(),
        );
        Ok(self)
    }
}

fn bytes_to_u64(bytes: Vec<u8>) -> anyhow::Result<u64> {
    let bytes: &[u8] = &bytes;
    if bytes.len() != 8 {
        anyhow::bail!("invalid u64 encoding (len={})", bytes.len());
    }
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes);
    Ok(u64::from_le_bytes(buf))
}
