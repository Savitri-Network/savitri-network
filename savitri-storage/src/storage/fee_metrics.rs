use super::{Storage, RocksDb};
use super::{Storage, CF_FEE_METRICS, RocksDb};
use serde::{Deserialize, Serialize};

pub const ROLLING_WINDOW_SECONDS: u64 = 24 * 60 * 60;

/// Intervallo di aggregazione per il volume in secondi (1 ora)
pub const VOLUME_AGGREGATION_INTERVAL_SECONDS: u64 = 60 * 60; // 1 ora

/// Key: timestamp (u64, little-endian) per tracking 24h rolling window
/// Value: FeeMetrics serializzato
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FeeMetrics {
    pub volume: u128,
    /// Amount totale bruciato
    pub burned: u128,
    /// Timestamp of the periodo
    pub timestamp: u64,
}

impl FeeMetrics {
    pub fn new(volume: u128, burned: u128, timestamp: u64) -> Self {
        Self {
            volume,
            burned,
            timestamp,
        }
    }

    pub fn zero(timestamp: u64) -> Self {
        Self {
            volume: 0,
            burned: 0,
            timestamp,
        }
    }
}

impl Storage<RocksDb> {
    /// Salva metriche fee per un timestamp specifico
    pub fn put_fee_metrics(&self, timestamp: u64, metrics: &FeeMetrics) -> anyhow::Result<()> {
        let key = timestamp.to_le_bytes();
        let value = bincode::serialize(metrics)?;
        self.put_cf(CF_FEE_METRICS, key, value)
    }

    pub fn get_fee_metrics(&self, timestamp: u64) -> anyhow::Result<Option<FeeMetrics>> {
        let key = timestamp.to_le_bytes();
        match self.get_cf(CF_FEE_METRICS, key)? {
            Some(ref bytes) => Ok(Some(crate::safe_deserialize(&bytes[..])?)),
            None => Ok(None),
        }
    }

    /// Elimina metriche fee per un timestamp specifico
    pub fn delete_fee_metrics(&self, timestamp: u64) -> anyhow::Result<()> {
        let key = timestamp.to_le_bytes();
        self.delete_cf(CF_FEE_METRICS, key)
    }

    pub fn get_all_fee_metrics(&self) -> anyhow::Result<Vec<(u64, FeeMetrics)>> {
        let cf = self.cf(CF_FEE_METRICS)?;
        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        let mut out = Vec::new();
        for entry in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = entry?;
            let timestamp = u64::from_le_bytes(
                key.as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid timestamp key length"))?,
            );
            let metrics: FeeMetrics = crate::safe_deserialize(&value[..])?;
            out.push((timestamp, metrics));
        }
        Ok(out)
    }

    /// Arrotonda un timestamp all'intervallo di aggregazione più vicino.
    /// riducendo drasticamente il numero di entry nel database.
    fn round_timestamp_to_interval(timestamp: u64) -> u64 {
        (timestamp / VOLUME_AGGREGATION_INTERVAL_SECONDS) * VOLUME_AGGREGATION_INTERVAL_SECONDS
    }

    /// Adds volume di una transazione alle metriche per un timestamp specifico.
    /// Il timestamp viene arrotondato all'intervallo di aggregazione per ridurre il numero di entry.
    pub fn add_transaction_volume(&self, timestamp: u64, fee_amount: u128) -> anyhow::Result<()> {
        let aggregated_timestamp = Self::round_timestamp_to_interval(timestamp);

        let existing = self.get_fee_metrics(aggregated_timestamp)?;
        let updated = match existing {
            Some(mut metrics) => {
                metrics.volume = metrics
                    .volume
                    .checked_add(fee_amount)
                    .ok_or_else(|| anyhow::anyhow!("volume overflow"))?;
                metrics
            }
            None => {
                // Creates nuova entry con volume iniziale
                FeeMetrics::new(fee_amount, 0, aggregated_timestamp)
            }
        };
        self.put_fee_metrics(aggregated_timestamp, &updated)
    }

    /// Adds amount bruciato alle metriche per un timestamp specifico.
    /// Il timestamp viene arrotondato all'intervallo di aggregazione per ridurre il numero di entry.
    pub fn add_burned_amount(&self, timestamp: u64, burned_amount: u128) -> anyhow::Result<()> {
        let aggregated_timestamp = Self::round_timestamp_to_interval(timestamp);

        let existing = self.get_fee_metrics(aggregated_timestamp)?;
        let updated = match existing {
            Some(mut metrics) => {
                metrics.burned = metrics
                    .burned
                    .checked_add(burned_amount)
                    .ok_or_else(|| anyhow::anyhow!("burned amount overflow"))?;
                metrics
            }
            None => {
                // Creates nuova entry con burned amount iniziale e volume zero
                FeeMetrics::new(0, burned_amount, aggregated_timestamp)
            }
        };
        self.put_fee_metrics(aggregated_timestamp, &updated)
    }

    pub fn get_volume_24h(&self, current_timestamp: u64) -> anyhow::Result<u128> {
        let cutoff_timestamp = current_timestamp.saturating_sub(ROLLING_WINDOW_SECONDS);

        // Arrotonda il cutoff all'intervallo di aggregazione per allinearsi con le entry salvate
        let cutoff_aggregated = Self::round_timestamp_to_interval(cutoff_timestamp);

        // partiamo da un bucket prima of the cutoff aggregato
        let iterator_start = cutoff_aggregated.saturating_sub(VOLUME_AGGREGATION_INTERVAL_SECONDS);

        let cf = self.cf(CF_FEE_METRICS)?;

        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        let mut total_volume = 0u128;
        let current_aggregated = Self::round_timestamp_to_interval(current_timestamp);

        for entry in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = entry?;
            let bucket_start = u64::from_le_bytes(
                key.as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid timestamp key length"))?,
            );

            // Salta i bucket che iniziano prima di iterator_start
            if bucket_start < iterator_start {
                continue;
            }

            // Includiamo anche i bucket che iniziano esattamente a current_aggregated
            if bucket_start > current_aggregated {
                break;
            }

            // Deserializza le metriche
            let metrics: FeeMetrics = crate::safe_deserialize(&value[..])?;

            // Compute i confini of the bucket
            let bucket_end = bucket_start + VOLUME_AGGREGATION_INTERVAL_SECONDS;

            let window_start = cutoff_timestamp;
            let window_end = current_timestamp;

            // Un bucket si sovrappone con la finestra se:
            // esattamente a window_start non contribuisce alla finestra (window_start è incluso).
            let overlaps = bucket_end > window_start && bucket_start <= window_end;

            if overlaps {
                // Bucket si sovrappone con la finestra: includi tutto il volume
                total_volume = total_volume
                    .checked_add(metrics.volume)
                    .ok_or_else(|| anyhow::anyhow!("total volume overflow"))?;
            }
        }

        Ok(total_volume)
    }

    pub fn cleanup_old_metrics(&self, current_timestamp: u64) -> anyhow::Result<usize> {
        log::debug!(
            "Starting cleanup_old_metrics with current_timestamp: {}",
            current_timestamp
        );

        let cutoff_timestamp = current_timestamp - ROLLING_WINDOW_SECONDS;

        log::debug!(
            "Cutoff timestamp (current_timestamp - 24h): {}",
            cutoff_timestamp
        );

        // Arrotonda il cutoff all'intervallo di aggregazione per allinearsi con i bucket
        let cutoff_aggregated = Self::round_timestamp_to_interval(cutoff_timestamp);
        log::debug!("Rounded cutoff_aggregated: {}", cutoff_aggregated);
        log::debug!(
            "Aggregation interval: {} seconds",
            VOLUME_AGGREGATION_INTERVAL_SECONDS
        );

        let cf = self.cf(CF_FEE_METRICS)?;

        let iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        let mut deleted_count = 0;
        let mut keys_to_delete = Vec::new();
        let mut processed_count = 0;

        log::debug!("Starting iteration through metrics...");

        for entry in iter {
            let (key, _): (Box<[u8]>, Box<[u8]>) = entry?;
            let bucket_start = u64::from_le_bytes(
                key.as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid timestamp key length"))?,
            );

            processed_count += 1;

            // Compute quando finisce il bucket (bucket_start è già aggregato)
            let bucket_end = bucket_start + VOLUME_AGGREGATION_INTERVAL_SECONDS;

            log::debug!(
                "Processing bucket - start: {}, end: {} (cutoff_timestamp: {})",
                bucket_start,
                bucket_end,
                cutoff_timestamp
            );

            // Se il bucket finisce dopo il cutoff, andiamo al prossimo per vedere se è ancora valido
            if bucket_end > cutoff_timestamp {
                log::debug!("Bucket ends after cutoff, checking next bucket...");
                continue;
            }

            // Se arriviamo qui, il bucket è completamente prima of the cutoff
            log::debug!(
                "Marking bucket for deletion - start: {}, end: {}",
                bucket_start,
                bucket_end
            );

            // Aggiungi il bucket alla list di quelli da eliminare
            keys_to_delete.push(key.to_vec());

            // Non ci fermiamo qui, continuiamo a cercare altri bucket da eliminare
        }

        log::debug!(
            "Processed {} buckets, marking {} for deletion",
            processed_count,
            keys_to_delete.len()
        );

        for key in &keys_to_delete {
            self.delete_cf(CF_FEE_METRICS, key)?;
            deleted_count += 1;

            // Log the timestamp of the deleted metric for debugging
            if let Ok(timestamp_bytes) = key[..].try_into() {
                let timestamp = u64::from_le_bytes(timestamp_bytes);
                log::info!("Deleted metric with timestamp: {}", timestamp);
            }
        }

        log::info!(
            "cleanup_old_metrics completed - deleted {} metrics",
            deleted_count
        );
        Ok(deleted_count)
    }

    pub fn get_metrics_24h(
        &self,
        current_timestamp: u64,
    ) -> anyhow::Result<Vec<(u64, FeeMetrics)>> {
        let cutoff_timestamp = current_timestamp.saturating_sub(ROLLING_WINDOW_SECONDS);

        // Arrotonda il cutoff all'intervallo di aggregazione
        let cutoff_aggregated = Self::round_timestamp_to_interval(cutoff_timestamp);
        let current_aggregated = Self::round_timestamp_to_interval(current_timestamp);

        let cf = self.cf(CF_FEE_METRICS)?;

        let cutoff_key = cutoff_aggregated.to_le_bytes();
        let iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&cutoff_key, rocksdb::Direction::Forward),
        );

        let mut metrics = Vec::new();

        for entry in iter {
            let (key, value): (Box<[u8]>, Box<[u8]>) = entry?;
            let timestamp = u64::from_le_bytes(
                key.as_ref()
                    .try_into()
                    .map_err(|_| anyhow::anyhow!("invalid timestamp key length"))?,
            );

            if timestamp > current_aggregated {
                break;
            }

            if timestamp >= cutoff_aggregated {
                let fee_metrics: FeeMetrics = crate::safe_deserialize(&value)?;
                metrics.push((timestamp, fee_metrics));
            }
        }

        Ok(metrics)
    }
}
