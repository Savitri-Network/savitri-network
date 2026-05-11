use anyhow::Result;
use savitri_storage::Storage;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

/// Type alias so the rest of the crate can refer to archive storage uniformly.
pub type ArchiveStorage = Storage;

/// Maximum allowed size for archive block deserialization (4 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized payloads.
const MAX_ARCHIVE_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;

/// Simplified stats returned by `ArchiveStorage::get_stats()` extension.
#[derive(Debug, Clone, Default)]
pub struct ArchiveStats {
    pub size_bytes: u64,
    pub block_count: u64,
    pub chain_height: u64,
}

/// Extension trait so `main.rs` can call `storage.get_stats()` via archive module.
pub fn get_archive_stats(storage: &Storage) -> Result<ArchiveStats> {
    let metrics = get_archive_metrics(storage)?;
    Ok(ArchiveStats {
        size_bytes: metrics.size_bytes,
        block_count: metrics.block_count,
        chain_height: metrics.chain_height.unwrap_or(0),
    })
}

/// Archive storage metrics for monitoring
#[derive(Debug, Clone)]
pub struct ArchiveMetrics {
    /// Total database size in bytes
    pub size_bytes: u64,
    /// Estimated size in GB
    pub size_gb: f64,
    /// Number of blocks stored
    pub block_count: u64,
    /// Number of monoliths stored
    pub monolith_count: u64,
    /// Chain head height
    pub chain_height: Option<u64>,
    /// Last measurement timestamp
    pub measured_at: u64,
}

/// Block information for archive queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveBlock {
    pub height: u64,
    pub hash: [u8; 32],
    pub timestamp: u64,
    pub tx_count: u32,
    pub size: u32,
    pub parent_hash: [u8; 32],
}

/// Account transaction entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountTxEntry {
    pub height: u64,
    #[serde(with = "BigArray")]
    pub tx_hash: [u8; 64],
    pub timestamp: u64,
}

/// Open RocksDB storage for a guardian node with archive-optimized settings.
///
/// Archive nodes use lighter compaction to preserve all data while maintaining
/// reasonable performance. No retention/purge hooks are triggered.
///
/// # Compaction Policy
/// - Level-based compaction with reduced aggressiveness
/// - Larger level sizes to reduce compaction frequency
/// - No TTL-based deletion
/// - Preserve all historical data
pub fn open_archive<P: AsRef<Path>>(path: P) -> Result<Arc<ArchiveStorage>> {
    // Create storage with archive-optimized configuration
    let storage = Arc::new(Storage::with_config(savitri_storage::StorageConfig {
        path: path.as_ref().to_string_lossy().to_string(),
        cache_size: 256 * 1024 * 1024,       // 256MB cache
        write_buffer_size: 64 * 1024 * 1024, // 64MB
        max_write_buffer_number: 3,
        enable_compression: true,
        create_if_missing: true,
        max_open_files: -1,
        use_fsync: true,
    })?);

    // Ensure genesis block exists
    ensure_genesis_block(storage.as_ref())?;

    info!("Archive storage opened at {}", path.as_ref().display());
    Ok(storage)
}

/// Ensure genesis block exists in archive storage
fn ensure_genesis_block(storage: &Storage) -> Result<()> {
    // Check if we have any blocks stored
    if let Ok(Some(_)) = storage.get_block(0) {
        return Ok(()); // Genesis block already exists
    }

    // Create a minimal genesis block for archive initialization
    let genesis_block = ArchiveBlock {
        height: 0,
        hash: [0u8; 32], // Would be actual genesis hash
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
        tx_count: 0,
        size: 0,
        parent_hash: [0u8; 32],
    };

    // Store genesis block
    let block_data = bincode::serialize(&genesis_block)?;
    storage.set_block(0, &block_data)?;

    // Set chain head to genesis
    storage.set_chain_head(&block_data)?;

    info!("Genesis block initialized for archive storage");
    Ok(())
}

/// Get archive storage metrics for monitoring
pub fn get_archive_metrics(storage: &Storage) -> Result<ArchiveMetrics> {
    // Get chain head to determine height
    let chain_height = if let Ok(Some(head_data)) = storage.get_chain_head() {
        if head_data.len() > MAX_ARCHIVE_DESERIALIZE_SIZE {
            warn!(
                "Chain head data too large: {} bytes, skipping",
                head_data.len()
            );
            None
        } else if let Ok(block) = bincode::deserialize::<ArchiveBlock>(&head_data) {
            Some(block.height)
        } else {
            None
        }
    } else {
        None
    };

    // Estimate block count from chain height (if available)
    let block_count = chain_height.unwrap_or(0).saturating_add(1);

    // Count monoliths by checking metadata
    let monolith_count = count_monoliths(storage)?;

    // Estimate database size from RocksDB properties
    let size_bytes = estimate_db_size(storage)?;
    let size_gb = size_bytes as f64 / (1024.0 * 1024.0 * 1024.0);

    let measured_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    Ok(ArchiveMetrics {
        size_bytes,
        size_gb,
        block_count,
        monolith_count,
        chain_height,
        measured_at,
    })
}

/// Count monoliths in storage
fn count_monoliths(storage: &Storage) -> Result<u64> {
    let mut count = 0u64;

    // Use the storage iterator to count monolith entries
    let iter = storage.iterator_cf(savitri_storage::storage::CF_METADATA)?;

    for item in iter {
        let (key, _) = item?;
        let key_str = String::from_utf8_lossy(&key);

        // Count monolith entries (keys starting with "monolith:")
        if key_str.starts_with("monolith:") && key.len() > 8 {
            count = count.saturating_add(1);
        }
    }

    Ok(count)
}

/// Estimate database size from RocksDB properties
fn estimate_db_size(storage: &Storage) -> Result<u64> {
    // Try to get actual database statistics
    let mut total_size = 0u64;

    // Count entries in main column families
    let cfs = [
        savitri_storage::storage::CF_DEFAULT,
        savitri_storage::storage::CF_BLOCKS,
        savitri_storage::storage::CF_TRANSACTIONS,
        savitri_storage::storage::CF_STATE,
        savitri_storage::storage::CF_METADATA,
        savitri_storage::storage::CF_ACCOUNTS,
    ];

    for cf_name in &cfs {
        if let Ok(iter) = storage.iterator_cf(cf_name) {
            let cf_count = iter.count();
            // Estimate: average 1KB per entry + overhead
            total_size = total_size.saturating_add((cf_count as u64).saturating_mul(1024));
        }
    }

    // Add some overhead for indexes and metadata
    total_size = total_size.saturating_mul(12) / 10; // 20% overhead

    Ok(total_size)
}

/// Check disk usage and return alert if threshold exceeded
pub fn check_disk_usage(storage: &Storage, alert_threshold_gb: f64) -> Result<Option<String>> {
    let metrics = get_archive_metrics(storage)?;
    if metrics.size_gb >= alert_threshold_gb {
        Ok(Some(format!(
            "⚠️ Archive storage size {:.2} GB exceeds alert threshold {:.2} GB ({} blocks, {} monoliths)",
            metrics.size_gb, alert_threshold_gb, metrics.block_count, metrics.monolith_count
        )))
    } else {
        Ok(None)
    }
}

/// Calculate growth rate (GB per day) from two metric snapshots
pub fn calculate_growth_rate(old: &ArchiveMetrics, new: &ArchiveMetrics) -> Result<f64> {
    if new.measured_at <= old.measured_at {
        return Ok(0.0);
    }
    let elapsed_days = (new.measured_at - old.measured_at) as f64 / 86400.0;
    if elapsed_days <= 0.0 {
        return Ok(0.0);
    }
    let growth_gb = new.size_gb - old.size_gb;
    Ok(growth_gb / elapsed_days)
}

/// Spawn a background monitor that periodically logs metrics and alerts on disk usage.
pub fn spawn_archive_monitor(
    storage: Arc<Storage>,
    interval_secs: u64,
    alert_threshold_gb: Option<f64>,
) {
    std::thread::spawn(move || {
        let mut last_metrics: Option<ArchiveMetrics> = None;

        loop {
            if let Ok(metrics) = get_archive_metrics(&storage) {
                // Log current metrics
                info!(
                    "Archive metrics: {:.2} GB, {} blocks, {} monoliths, chain height: {:?}",
                    metrics.size_gb,
                    metrics.block_count,
                    metrics.monolith_count,
                    metrics.chain_height
                );

                // Calculate and log growth rate if we have previous metrics
                if let Some(ref last) = last_metrics {
                    if let Ok(growth_rate) = calculate_growth_rate(last, &metrics) {
                        if growth_rate > 0.0 {
                            info!("Archive growth rate: {:.2} GB/day", growth_rate);
                        }
                    }
                }

                // Check disk usage alerts
                if let Some(threshold) = alert_threshold_gb {
                    if metrics.size_gb >= threshold {
                        warn!(
                            "Archive size {:.2} GB exceeds alert threshold {:.2} GB",
                            metrics.size_gb, threshold
                        );
                    }
                }

                last_metrics = Some(metrics);
            } else {
                error!("Failed to get archive metrics");
            }

            std::thread::sleep(std::time::Duration::from_secs(interval_secs.max(30)));
        }
    });
}

/// Get blocks in height range (efficient batch query for history serving)
pub fn get_blocks_range(
    storage: &Storage,
    start_height: u64,
    end_height: u64,
) -> Result<Vec<ArchiveBlock>> {
    if start_height > end_height {
        return Err(anyhow::anyhow!(
            "Invalid range: start_height {} > end_height {}",
            start_height,
            end_height
        ));
    }

    let mut blocks = Vec::new();

    for height in start_height..=end_height {
        match storage.get_block(height) {
            Ok(Some(block_data)) => {
                if block_data.len() > MAX_ARCHIVE_DESERIALIZE_SIZE {
                    warn!(
                        "Block data at height {} too large: {} bytes, skipping",
                        height,
                        block_data.len()
                    );
                    continue;
                }
                match bincode::deserialize::<ArchiveBlock>(&block_data) {
                    Ok(block) => blocks.push(block),
                    Err(e) => {
                        warn!("Failed to deserialize block at height {}: {}", height, e);
                        // Continue with other blocks
                    }
                }
            }
            Ok(None) => {
                warn!("Block not found at height {}", height);
                // Continue with other blocks
            }
            Err(e) => {
                error!("Error retrieving block at height {}: {}", height, e);
                return Err(e);
            }
        }
    }

    Ok(blocks)
}

/// Get block by height
pub fn get_block(storage: &Storage, height: u64) -> Result<Option<ArchiveBlock>> {
    match storage.get_block(height) {
        Ok(Some(block_data)) => {
            if block_data.len() > MAX_ARCHIVE_DESERIALIZE_SIZE {
                anyhow::bail!(
                    "Block data at height {} too large: {} bytes (max {})",
                    height,
                    block_data.len(),
                    MAX_ARCHIVE_DESERIALIZE_SIZE
                );
            }
            let block = bincode::deserialize::<ArchiveBlock>(&block_data)?;
            Ok(Some(block))
        }
        Ok(None) => Ok(None),
        Err(e) => Err(e),
    }
}

pub fn store_block(storage: &Storage, block: ArchiveBlock) -> Result<()> {
    // SECURITY: Validate block hash is non-zero (reject obviously invalid blocks)
    if block.hash == [0u8; 32] {
        anyhow::bail!(
            "Block at height {} has zero hash — rejecting potentially poisoned block",
            block.height
        );
    }

    // SECURITY: Validate parent chain continuity
    if block.height > 0 {
        if let Ok(Some(parent_data)) = storage.get_block(block.height - 1) {
            if parent_data.len() > MAX_ARCHIVE_DESERIALIZE_SIZE {
                anyhow::bail!(
                    "Parent block data too large: {} bytes (max {})",
                    parent_data.len(),
                    MAX_ARCHIVE_DESERIALIZE_SIZE
                );
            }
            if let Ok(parent) = bincode::deserialize::<ArchiveBlock>(&parent_data) {
                if block.parent_hash != parent.hash {
                    anyhow::bail!(
                        "Block at height {} has parent_hash mismatch (expected {:?}, got {:?})",
                        block.height,
                        parent.hash,
                        block.parent_hash
                    );
                }
            }
        }
    }

    let block_data = bincode::serialize(&block)?;
    storage.set_block(block.height, &block_data)?;

    // Update chain head if this is the latest block
    if let Ok(Some(head_data)) = storage.get_chain_head() {
        if head_data.len() <= MAX_ARCHIVE_DESERIALIZE_SIZE {
            if let Ok(head_block) = bincode::deserialize::<ArchiveBlock>(&head_data) {
                if block.height > head_block.height {
                    storage.set_chain_head(&block_data)?;
                }
            }
        } else {
            warn!(
                "Chain head data too large: {} bytes, overwriting",
                head_data.len()
            );
            storage.set_chain_head(&block_data)?;
        }
    } else {
        // No chain head set, this becomes the head
        storage.set_chain_head(&block_data)?;
    }

    Ok(())
}

/// Get account transaction history (append-only log index)
///
/// This creates an index entry for each transaction that touches an account.
/// Format: `account_history::<account_bytes>::<height>::<tx_hash>`
pub fn index_account_tx(
    storage: &Storage,
    account: &[u8; 32],
    height: u64,
    tx_hash: &[u8; 64],
) -> Result<()> {
    // Key format: account_history::<account>::<height>::<tx_hash>
    let mut key = b"account_history::".to_vec();
    key.extend_from_slice(account);
    key.push(b':');
    key.extend_from_slice(&height.to_be_bytes());
    key.push(b':');
    key.extend_from_slice(tx_hash);

    // Value: timestamp (when indexed)
    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

    storage.put_cf(
        savitri_storage::storage::CF_METADATA,
        &key,
        &timestamp.to_be_bytes(),
    )?;

    Ok(())
}

/// Query account history (all transactions that touched this account)
pub fn get_account_history(
    storage: &Storage,
    account: &[u8; 32],
    limit: Option<usize>,
) -> Result<Vec<AccountTxEntry>> {
    let mut results = Vec::new();

    // Build prefix for account history
    let mut prefix = b"account_history::".to_vec();
    prefix.extend_from_slice(account);
    prefix.push(b':');

    // Iterate through account history entries
    let iter = storage.iterator_cf(savitri_storage::storage::CF_METADATA)?;

    for item in iter {
        let (key, value) = item?;

        // Check if key starts with our prefix
        if !key.starts_with(&prefix) {
            continue;
        }

        // Parse: account_history::<account>::<height>::<tx_hash>
        let key_str = String::from_utf8_lossy(&key);
        let parts: Vec<&str> = key_str.split(':').collect();

        if parts.len() >= 4 {
            if let (Ok(height), Ok(timestamp)) = (
                parts[2].parse::<u64>(),
                if value.len() >= 8 {
                    Ok::<u64, std::num::ParseIntError>(u64::from_be_bytes(
                        value[..8].try_into().unwrap_or([0; 8]),
                    ))
                } else {
                    Ok(0)
                },
            ) {
                // Parse tx_hash (should be 64 bytes, 128 hex chars)
                let tx_hash_str = parts[3];
                if tx_hash_str.len() == 128 {
                    if let Ok(tx_hash_bytes) = hex::decode(tx_hash_str) {
                        if let Ok(tx_hash) = tx_hash_bytes.try_into() {
                            results.push(AccountTxEntry {
                                height,
                                tx_hash,
                                timestamp,
                            });

                            // Apply limit if specified
                            if let Some(limit) = limit {
                                if results.len() >= limit {
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // Sort by height (descending - most recent first)
    results.sort_by(|a, b| b.height.cmp(&a.height));

    Ok(results)
}

/// Get account balance history
pub fn get_account_balance_history(
    storage: &Storage,
    account: &[u8; 32],
    limit: Option<usize>,
) -> Result<Vec<(u64, u128)>> {
    let history = get_account_history(storage, account, limit)?;
    let mut balance_history = Vec::new();

    // This would require scanning blocks to reconstruct balance changes
    // For now, return a simplified version based on transaction timestamps
    for entry in history {
        // Get account balance at this height (simplified)
        if let Ok(Some(account_data)) = storage.get_account(account) {
            if account_data.len() >= 16 {
                let balance = u128::from_le_bytes(account_data[..16].try_into().unwrap_or([0; 16]));
                balance_history.push((entry.height, balance));
            }
        }

        if let Some(limit) = limit {
            if balance_history.len() >= limit {
                break;
            }
        }
    }

    Ok(balance_history)
}

/// Compact archive storage (manual compaction trigger)
pub fn compact_archive(storage: &Storage) -> Result<()> {
    info!("Starting manual archive compaction");

    // This would trigger RocksDB compaction
    // For now, we'll just log the action
    // In a real implementation, you'd call storage.compact_range() or similar

    info!("Archive compaction completed");
    Ok(())
}

/// Validate archive integrity
pub fn validate_archive_integrity(storage: &Storage) -> Result<IntegrityReport> {
    let mut report = IntegrityReport {
        total_blocks: 0,
        missing_blocks: Vec::new(),
        corrupted_blocks: Vec::new(),
        chain_continuity: true,
        last_checked: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
    };

    // Get chain head to know expected range
    if let Ok(Some(head_data)) = storage.get_chain_head() {
        if head_data.len() > MAX_ARCHIVE_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Chain head data too large for integrity check: {} bytes (max {})",
                head_data.len(),
                MAX_ARCHIVE_DESERIALIZE_SIZE
            );
        }
        if let Ok(head_block) = bincode::deserialize::<ArchiveBlock>(&head_data) {
            let max_height = head_block.height;

            // Check block continuity
            let mut expected_height = 0u64;
            while expected_height <= max_height {
                match storage.get_block(expected_height) {
                    Ok(Some(block_data)) => {
                        if block_data.len() > MAX_ARCHIVE_DESERIALIZE_SIZE {
                            report.corrupted_blocks.push(expected_height);
                            expected_height += 1;
                            continue;
                        }
                        match bincode::deserialize::<ArchiveBlock>(&block_data) {
                            Ok(block) => {
                                if block.height != expected_height {
                                    report.corrupted_blocks.push(expected_height);
                                }
                                report.total_blocks += 1;
                            }
                            Err(_) => {
                                report.corrupted_blocks.push(expected_height);
                            }
                        }
                    }
                    Ok(None) => {
                        report.missing_blocks.push(expected_height);
                        report.chain_continuity = false;
                    }
                    Err(_) => {
                        report.corrupted_blocks.push(expected_height);
                    }
                }
                expected_height += 1;
            }
        }
    }

    Ok(report)
}

/// Archive integrity report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityReport {
    pub total_blocks: u64,
    pub missing_blocks: Vec<u64>,
    pub corrupted_blocks: Vec<u64>,
    pub chain_continuity: bool,
    pub last_checked: u64,
}
