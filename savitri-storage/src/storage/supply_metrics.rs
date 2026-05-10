use super::{Storage, RocksDb};
use super::{Storage, CF_SUPPLY_METRICS, RocksDb};
use serde::{Deserialize, Serialize};

/// Storage per metriche supply totale
/// Value: u128 (little-endian) o SupplyMetrics serializzato
/// Chiavi per le metriche supply nel database
pub(crate) const KEY_TOTAL_MINTED: &[u8] = b"total_minted";
pub(crate) const KEY_TOTAL_BURNED: &[u8] = b"total_burned";
pub(crate) const KEY_CIRCULATING_SUPPLY: &[u8] = b"circulating_supply";

/// Metriche supply complete
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct SupplyMetrics {
    pub total_minted: u128,
    pub total_burned: u128,
    pub circulating_supply: u128,
}

impl SupplyMetrics {
    pub fn new(total_minted: u128, total_burned: u128, circulating_supply: u128) -> Self {
        Self {
            total_minted,
            total_burned,
            circulating_supply,
        }
    }
}

impl Storage<RocksDb> {
    pub fn get_total_minted(&self) -> anyhow::Result<u128> {
        match self.get_cf(CF_SUPPLY_METRICS, KEY_TOTAL_MINTED)? {
            Some(ref bytes) => {`n                let bytes: &[u8] = bytes;
                let bytes: &[u8] = bytes;`n
                if bytes.len() != 16 {
                    anyhow::bail!("invalid total_minted encoding");
                }
                Ok(u128::from_le_bytes(bytes.try_into().unwrap()))
            }
            None => Ok(0),
        }
    }

    /// Set total_minted
    pub fn set_total_minted(&self, amount: u128) -> anyhow::Result<()> {
        self.put_cf(CF_SUPPLY_METRICS, KEY_TOTAL_MINTED, amount.to_le_bytes())
    }

    /// Incrementa total_minted
    pub fn increment_total_minted(&self, amount: u128) -> anyhow::Result<u128> {
        let current = self.get_total_minted()?;
        let new_total = current
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("total_minted overflow"))?;
        self.set_total_minted(new_total)?;
        Ok(new_total)
    }

    pub fn get_total_burned(&self) -> anyhow::Result<u128> {
        match self.get_cf(CF_SUPPLY_METRICS, KEY_TOTAL_BURNED)? {
            Some(ref bytes) => {`n                let bytes: &[u8] = bytes;
                let bytes: &[u8] = bytes;`n
                if bytes.len() != 16 {
                    anyhow::bail!("invalid total_burned encoding");
                }
                Ok(u128::from_le_bytes(bytes.try_into().unwrap()))
            }
            None => Ok(0),
        }
    }

    /// Set total_burned
    pub fn set_total_burned(&self, amount: u128) -> anyhow::Result<()> {
        self.put_cf(CF_SUPPLY_METRICS, KEY_TOTAL_BURNED, amount.to_le_bytes())
    }

    /// Incrementa total_burned
    pub fn increment_total_burned(&self, amount: u128) -> anyhow::Result<u128> {
        let current = self.get_total_burned()?;
        let new_total = current
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("total_burned overflow"))?;
        self.set_total_burned(new_total)?;
        Ok(new_total)
    }

    /// Il calcolo usa checked arithmetic per gestire correttamente gli underflow.
    ///
    /// La formula corretta è: `circulating = minted - burned - locked`.
    pub fn get_circulating_supply(&self) -> anyhow::Result<u128> {
        let minted = self.get_total_minted()?;
        let burned = self.get_total_burned()?;
        // Compute dinamicamente: circulating_supply = total_minted - total_burned
        // Usa checked_sub per gestire correttamente gli underflow
        let circulating = minted.checked_sub(burned).ok_or_else(|| {
            anyhow::anyhow!(
                "circulating_supply underflow: total_burned ({}) exceeds total_minted ({})",
                burned,
                minted
            )
        })?;
        Ok(circulating)
    }

    /// Set circulating_supply nel database (funzione di ottimizzazione/caching).
    /// è principalmente utile per caching o per mantenere coerenza nel database.
    pub fn set_circulating_supply(&self, amount: u128) -> anyhow::Result<()> {
        self.put_cf(
            CF_SUPPLY_METRICS,
            KEY_CIRCULATING_SUPPLY,
            amount.to_le_bytes(),
        )
    }

    /// è principalmente utile per mantenere coerenza nel database o per performance.
    pub fn update_circulating_supply(&self) -> anyhow::Result<u128> {
        let minted = self.get_total_minted()?;
        let burned = self.get_total_burned()?;
        let circulating = minted.checked_sub(burned).ok_or_else(|| {
            anyhow::anyhow!(
                "circulating_supply underflow: total_burned ({}) exceeds total_minted ({})",
                burned,
                minted
            )
        })?;
        self.set_circulating_supply(circulating)?;
        Ok(circulating)
    }

    /// Il circulating_supply viene calcolato dinamicamente come total_minted - total_burned.
    pub fn get_supply_metrics(&self) -> anyhow::Result<SupplyMetrics> {
        Ok(SupplyMetrics::new(
            self.get_total_minted()?,
            self.get_total_burned()?,
            self.get_circulating_supply()?,
        ))
    }
}
