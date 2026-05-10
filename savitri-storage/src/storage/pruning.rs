//! Storage pruning for Savitri Network
//!
//! Multi-layer pruning system to keep node storage requirements bounded:
//! - Block & TX pruning: keep full data for recent blocks, receipts-only for older
//! - Certificate pruning: remove certificates older than retention window
//! - Metadata cleanup: remove height mappings and orphan data
//! - RocksDB compaction: reclaim disk space after deletion
//!
//! Designed for full nodes (default ~500 GB max). Archive nodes disable pruning.

use std::sync::Arc;
use std::time::Instant;
use tracing::{debug, info, warn};

#[cfg(feature = "rocksdb")]
use rocksdb::DB;

use super::{Storage, CF_ACCOUNTS, CF_BLOCKS, CF_METADATA, CF_STATE, CF_TRANSACTIONS};

/// Column family names for prunable data (may not all be active on every node)
pub const CF_CERTIFICATES: &str = "certificates";
pub const CF_RECEIPTS: &str = "receipts";
pub const CF_POU_SCORES: &str = "pou_scores";
pub const CF_POU_HISTORY: &str = "pou_history";
pub const CF_FEE_METRICS: &str = "fee_metrics";
pub const CF_ORPHANS: &str = "orphans";
pub const CF_MISSING: &str = "missing";

/// Pruning configuration
#[derive(Debug, Clone)]
pub struct PruningConfig {
    /// Master switch — if false, no pruning occurs
    pub enabled: bool,

    /// Archive mode — overrides enabled, keeps everything
    pub archive_mode: bool,

    /// Block retention: keep full blocks for this many blocks from head
    /// Default: 86400 (~30 days at 0.03 blk/s sustained)
    pub block_retention_blocks: u64,

    /// Certificate retention: keep certificates for this many blocks
    /// Default: same as block_retention
    pub certificate_retention_blocks: u64,

    /// TX receipt retention: keep receipts even after full TX pruned
    /// Default: 172800 (~60 days) — receipts are small
    pub receipt_retention_blocks: u64,

    /// PoU score history retention
    /// Default: 43200 (~15 days)
    pub pou_retention_blocks: u64,

    /// Fee metrics retention
    /// Default: 43200 (~15 days)
    pub fee_metrics_retention_blocks: u64,

    /// Orphan/missing block cleanup: remove entries older than this
    /// Default: 1000 blocks
    pub orphan_retention_blocks: u64,

    /// Run RocksDB compaction after pruning cycle
    pub compact_after_prune: bool,

    /// Minimum blocks between pruning cycles (avoid pruning too often)
    /// Default: 100
    pub min_prune_interval_blocks: u64,
}

impl Default for PruningConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            archive_mode: false,
            block_retention_blocks: 86400,
            certificate_retention_blocks: 86400,
            receipt_retention_blocks: 172800,
            pou_retention_blocks: 43200,
            fee_metrics_retention_blocks: 43200,
            orphan_retention_blocks: 1000,
            compact_after_prune: true,
            min_prune_interval_blocks: 100,
        }
    }
}

/// Result of a pruning cycle
#[derive(Debug, Default)]
pub struct PruneResult {
    pub blocks_pruned: u64,
    pub txs_pruned: u64,
    pub certificates_pruned: u64,
    pub metadata_pruned: u64,
    pub pou_pruned: u64,
    pub fee_metrics_pruned: u64,
    pub orphans_pruned: u64,
    pub duration_ms: u64,
    pub compact_duration_ms: u64,
}

impl std::fmt::Display for PruneResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "blocks={} txs={} certs={} meta={} pou={} fees={} orphans={} ({}ms, compact={}ms)",
            self.blocks_pruned,
            self.txs_pruned,
            self.certificates_pruned,
            self.metadata_pruned,
            self.pou_pruned,
            self.fee_metrics_pruned,
            self.orphans_pruned,
            self.duration_ms,
            self.compact_duration_ms,
        )
    }
}

/// Pruner operates on a Storage instance to remove old data
pub struct Pruner {
    config: PruningConfig,
    last_pruned_height: u64,
}

impl Pruner {
    pub fn new(config: PruningConfig) -> Self {
        Self {
            config,
            last_pruned_height: 0,
        }
    }

    /// Check if pruning should run at the given height
    pub fn should_prune(&self, current_height: u64) -> bool {
        if !self.config.enabled || self.config.archive_mode {
            return false;
        }
        current_height > self.last_pruned_height + self.config.min_prune_interval_blocks
    }

    /// Run a pruning cycle. Call periodically (e.g., every 100 blocks or every hour).
    ///
    /// This is safe to call from a background task — it uses only delete operations
    /// and does not interfere with concurrent reads/writes.
    #[cfg(feature = "rocksdb")]
    pub fn prune(&mut self, storage: &Storage, current_height: u64) -> PruneResult {
        if !self.should_prune(current_height) {
            return PruneResult::default();
        }

        let start = Instant::now();
        let mut result = PruneResult::default();

        // Block pruning
        let block_cutoff = current_height.saturating_sub(self.config.block_retention_blocks);
        if block_cutoff > 0 {
            result.blocks_pruned = self.prune_height_keyed_cf(storage, CF_BLOCKS, block_cutoff);
            result.txs_pruned = self.prune_height_keyed_cf(storage, CF_TRANSACTIONS, block_cutoff);
        }

        // Certificate pruning (dual-indexed: height:: and block:: prefixes)
        let cert_cutoff = current_height.saturating_sub(self.config.certificate_retention_blocks);
        if cert_cutoff > 0 {
            result.certificates_pruned =
                self.prune_prefixed_height_cf(storage, CF_CERTIFICATES, "height::", cert_cutoff);
        }

        // Metadata height mappings
        if block_cutoff > 0 {
            result.metadata_pruned =
                self.prune_prefixed_height_cf(storage, CF_METADATA, "height::", block_cutoff);
        }

        // PoU scores/history
        let pou_cutoff = current_height.saturating_sub(self.config.pou_retention_blocks);
        if pou_cutoff > 0 {
            result.pou_pruned += self.prune_height_keyed_cf(storage, CF_POU_SCORES, pou_cutoff);
            result.pou_pruned += self.prune_height_keyed_cf(storage, CF_POU_HISTORY, pou_cutoff);
        }

        // Fee metrics
        let fee_cutoff = current_height.saturating_sub(self.config.fee_metrics_retention_blocks);
        if fee_cutoff > 0 {
            result.fee_metrics_pruned =
                self.prune_height_keyed_cf(storage, CF_FEE_METRICS, fee_cutoff);
        }

        // Orphans and missing blocks (aggressive cleanup)
        let orphan_cutoff = current_height.saturating_sub(self.config.orphan_retention_blocks);
        if orphan_cutoff > 0 {
            result.orphans_pruned += self.prune_height_keyed_cf(storage, CF_ORPHANS, orphan_cutoff);
            result.orphans_pruned += self.prune_height_keyed_cf(storage, CF_MISSING, orphan_cutoff);
        }

        result.duration_ms = start.elapsed().as_millis() as u64;

        // Compaction
        if self.config.compact_after_prune && result.total_pruned() > 0 {
            let compact_start = Instant::now();
            self.compact_pruned_cfs(storage, block_cutoff);
            result.compact_duration_ms = compact_start.elapsed().as_millis() as u64;
        }

        self.last_pruned_height = current_height;

        if result.total_pruned() > 0 {
            info!(
                height = current_height,
                result = %result,
                "Pruning cycle completed"
            );
        }

        result
    }

    /// Prune entries from a CF where keys are height encoded as u64 big-endian bytes.
    /// Deletes all entries with key < cutoff_height.
    #[cfg(feature = "rocksdb")]
    fn prune_height_keyed_cf(&self, storage: &Storage, cf_name: &str, cutoff_height: u64) -> u64 {
        let db = match storage.get_db() {
            Some(db) => db,
            None => return 0,
        };
        let cf = match db.cf_handle(cf_name) {
            Some(cf) => cf,
            None => {
                debug!(cf = cf_name, "Column family not found, skipping prune");
                return 0;
            }
        };

        let mut count = 0u64;
        let cutoff_bytes = cutoff_height.to_be_bytes();
        let iter = db.iterator_cf(&cf, rocksdb::IteratorMode::Start);

        for item in iter {
            match item {
                Ok((key, _)) => {
                    // Keys that are 8 bytes are height-encoded
                    if key.len() == 8 && key.as_ref() < cutoff_bytes.as_slice() {
                        if db.delete_cf(&cf, &key).is_ok() {
                            count += 1;
                        }
                    } else if key.len() == 8 {
                        // Past cutoff, stop iterating (keys are sorted)
                        break;
                    }
                    // Non-height keys (e.g., hash-keyed) are skipped
                }
                Err(_) => break,
            }
        }

        if count > 0 {
            debug!(
                cf = cf_name,
                pruned = count,
                cutoff = cutoff_height,
                "Pruned height-keyed CF"
            );
        }
        count
    }

    /// Prune entries from a CF where keys have a prefix like "height::" followed by
    /// big-endian height bytes.
    #[cfg(feature = "rocksdb")]
    fn prune_prefixed_height_cf(
        &self,
        storage: &Storage,
        cf_name: &str,
        prefix: &str,
        cutoff_height: u64,
    ) -> u64 {
        let db = match storage.get_db() {
            Some(db) => db,
            None => return 0,
        };
        let cf = match db.cf_handle(cf_name) {
            Some(cf) => cf,
            None => {
                debug!(cf = cf_name, "Column family not found, skipping prune");
                return 0;
            }
        };

        let mut count = 0u64;
        let prefix_bytes = prefix.as_bytes();
        let iter = db.prefix_iterator_cf(&cf, prefix_bytes);

        for item in iter {
            match item {
                Ok((key, _)) => {
                    if !key.starts_with(prefix_bytes) {
                        break; // Past prefix range
                    }
                    // Extract height from key after prefix
                    let height_part = &key[prefix_bytes.len()..];
                    let height = if height_part.len() == 8 {
                        u64::from_be_bytes(height_part.try_into().unwrap_or([0; 8]))
                    } else {
                        // Try parsing as decimal string
                        std::str::from_utf8(height_part)
                            .ok()
                            .and_then(|s| s.parse::<u64>().ok())
                            .unwrap_or(u64::MAX)
                    };

                    if height < cutoff_height {
                        if db.delete_cf(&cf, &key).is_ok() {
                            count += 1;
                        }
                    }
                }
                Err(_) => break,
            }
        }

        if count > 0 {
            debug!(
                cf = cf_name,
                prefix,
                pruned = count,
                cutoff = cutoff_height,
                "Pruned prefixed CF"
            );
        }
        count
    }

    /// Compact the CFs that were pruned to reclaim disk space
    #[cfg(feature = "rocksdb")]
    fn compact_pruned_cfs(&self, storage: &Storage, _cutoff: u64) {
        let db = match storage.get_db() {
            Some(db) => db,
            None => return,
        };

        for cf_name in &[CF_BLOCKS, CF_TRANSACTIONS, CF_CERTIFICATES, CF_METADATA] {
            if let Some(cf) = db.cf_handle(cf_name) {
                db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
                debug!(cf = cf_name, "Compacted CF after pruning");
            }
        }
    }

    /// No-op for non-RocksDB builds
    #[cfg(not(feature = "rocksdb"))]
    pub fn prune(&mut self, _storage: &Storage, _current_height: u64) -> PruneResult {
        PruneResult::default()
    }
}

impl PruneResult {
    pub fn total_pruned(&self) -> u64 {
        self.blocks_pruned
            + self.txs_pruned
            + self.certificates_pruned
            + self.metadata_pruned
            + self.pou_pruned
            + self.fee_metrics_pruned
            + self.orphans_pruned
    }
}
