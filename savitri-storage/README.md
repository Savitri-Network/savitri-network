# Savitri Storage

A high-performance storage layer for the Savitri blockchain, providing RocksDB-based persistent storage with optimized column families, caching, and backup/restore capabilities.

## Features

- **RocksDB Integration**: Production-grade persistent storage with optimized column families
- **FlatFile Storage**: Specialized storage for monoliths and large data
- **In-Memory Storage**: Lightweight in-memory storage for testing
- **Thread-Safe Operations**: Full concurrent access support
- **Intelligent Caching**: LRU cache with configurable capacity
- **Backup & Restore**: Comprehensive backup and restore functionality
- **Metrics Collection**: Detailed performance metrics and monitoring
- **Migration Support**: Database schema migration management
- **FL Storage**: Specialized storage for Federated Learning operations

## Quick Start

### Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
savitri-storage = "0.1.0"
```

### Basic Usage

```rust
use savitri_storage::Storage;

// Create new storage instance
let storage = Storage::new("path/to/database")?;

// Store and retrieve data
storage.put("key", b"value")?;
let value = storage.get("key")?;

// Use column families
storage.put_cf("blocks", "block_1", b"block_data")?;
let block_data = storage.get_cf("blocks", "block_1")?;

// Batch operations
let mut batch = storage.begin_batch();
batch.put("key1", b"value1")?;
batch.put_cf("blocks", "block_1", b"block_data")?;
batch.commit()?;
```

### FL Storage

```rust
use savitri_storage::{Storage, FlStorage};

// Create FL storage
let storage = Storage::new("path/to/database")?;
let fl_storage = FlStorage::new(storage);

// Store FL model
let model = FlModelData {
    model_id: 1,
    creator: [1u8; 32],
    // ... other fields
};
fl_storage.put_model(&model)?;

// Store FL round
let round = FlRoundData {
    round_id: 1,
    model_id: 1,
    status: FlRoundStatus::Open,
    // ... other fields
};
fl_storage.put_round(&round)?;
```

## Configuration

### Storage Configuration

```rust
use savitri_storage::{Storage, StorageConfig};

let config = StorageConfig {
    path: "path/to/database".into(),
    cache_capacity: 10000,
    write_buffer_size: 64 * 1024 * 1024, // 64MB
    max_write_buffer_number: 4,
    enable_compression: true,
    create_if_missing: true,
    enable_statistics: true,
};

let storage = Storage::with_config(config)?;
```

### FL Storage Configuration

```rust
use savitri_storage::fl::{FlRetentionConfig, FlStorage};

let retention_config = FlRetentionConfig {
    max_rounds: 100,
    purge_finalized_updates: true,
    archive_updates_before_purge: true,
};

let fl_storage = FlStorage::new(storage);
fl_storage.apply_retention(model_id, retention_config)?;
```

## Column Families

The storage layer uses optimized column families for different data types:

### Core Column Families
- `default` - General key-value storage
- `blocks` - Blockchain blocks
- `tx` - Transactions
- `accounts` - Account data
- `receipts` - Transaction receipts
- `meta` - Metadata
- `orphans` - Orphan blocks
- `missing` - Missing data tracking
- `monoliths` - Monolith data

### Tokenomics & Governance
- `fee_metrics` - Fee statistics
- `vote_tokens` - Vote token balances
- `treasury` - Treasury data
- `governance` - Governance proposals
- `vesting` - Vesting schedules
- `supply_metrics` - Supply statistics

### Smart Contracts
- `contracts` - Contract data
- `identity` - Identity data
- `bonds` - Bond data

### PoU Scoring
- `pou_scores` - Proof of Unity scores
- `pou_history` - Historical scores
- `certificates` - Certificate data

### Oracle
- `oracle` - Oracle data

### Federated Learning
- `fl_models` - FL models
- `fl_rounds` - FL rounds
- `fl_updates` - FL updates
- `fl_contributions` - FL contributions
- `fl_rewards` - FL rewards

### Sharding
- `account_to_shard` - Account shard mapping
- `account_locks` - Account locks
- `accounts_shard_0` through `accounts_shard_7` - Account shards
- `contracts_shard_0` through `contracts_shard_7` - Contract shards

## Performance Optimization

### Caching

```rust
use savitri_storage::Storage;

let storage = Storage::new("path/to/database")?;

// Get cache statistics
let stats = storage.cache_stats();
println!("Cache hit rate: {:.2}%", stats.hit_rate() * 100.0);

// Clear cache
storage.clear_cache();

// Resize cache
storage.cache().resize(20000);
```

### Batch Operations

```rust
// Use batch operations for better performance
let mut batch = storage.begin_batch();

// Add multiple operations
for i in 0..1000 {
    batch.put(format!("key_{}", i), format!("value_{}", i))?;
}

// Commit all operations atomically
batch.commit()?;
```

### Snapshots

```rust
// Create snapshot for consistent reads
let snapshot = storage.create_snapshot();

// Read from snapshot (isolated from concurrent writes)
let value = snapshot.get("key")?;

// Use snapshot for iterators
let iter = snapshot.iterator_cf("blocks")?;
for item in iter {
    let (key, value) = item?;
    // Process item
}
```

## Backup & Restore

### Creating Backups

```rust
use savitri_storage::{Storage, BackupConfig, BackupType};

let storage = Storage::new("path/to/database")?;

// Create backup with default configuration
let metadata = storage.create_backup("backup.tar.gz")?;

// Create backup with custom configuration
let config = BackupConfig {
    backup_type: BackupType::Full,
    compression: BackupCompression::Gzip,
    encryption: BackupEncryption::None,
    verify_backup: true,
    // ... other options
};

let backup_manager = BackupManager::with_config(storage, config);
let metadata = backup_manager.create_backup("backup.tar.gz")?;
```

### Restoring from Backup

```rust
use savitri_storage::{Storage, RestoreConfig};

let storage = Storage::new("path/to/database")?;

// Restore from backup
let config = RestoreConfig {
    backup_path: "backup.tar.gz".into(),
    target_path: "restored_db".into(),
    verify_restore: true,
    force_overwrite: false,
    // ... other options
};

let backup_manager = BackupManager::new(storage);
backup_manager.restore_from_backup(config)?;
```

## Metrics & Monitoring

```rust
use savitri_storage::{Storage, MetricsCollector};

let storage = Storage::new("path/to/database")?;
let metrics = MetricsCollector::new();

// Record operations
let timer = PerformanceTimer::new(metrics.clone(), OperationType::Read, 100);
// ... perform operation
timer.finish();

// Get metrics
let metrics_data = metrics.get_metrics();
println!("Total operations: {}", metrics_data.total_operations);
println!("Cache hit rate: {:.2}%", metrics_data.cache_hit_rate() * 100.0);
println!("Read throughput: {:.2} B/s", metrics_data.read_throughput_bps());

// Export metrics as JSON
let json = metrics.export_json()?;
println!("Metrics: {}", json);
```

## Migration Support

```rust
use savitri_storage::{Storage, MigrationManager};

let storage = Storage::new("path/to/database")?;
let mut migration_manager = MigrationManager::new();

// Add migrations
migration_manager.add_migration(Migration {
    version: 1,
    name: "add_new_column_family".to_string(),
    description: "Add new column family for feature X".to_string(),
    migration_type: MigrationType::AddColumnFamily,
    migrate_fn: Box::new(|storage| {
        // Migration logic
        Ok(())
    }),
    rollback_fn: None,
    dependencies: vec![],
    estimated_duration_secs: 10,
});

// Execute migrations
migration_manager.set_target_version(1);
let records = migration_manager.migrate(&mut storage)?;

// Get migration history
let history = migration_manager.get_migration_history(&storage)?;
```

## Testing

### In-Memory Storage for Testing

```rust
#[cfg(feature = "memory")]
use savitri_storage::MemoryStorage;

#[cfg(feature = "memory")]
#[test]
fn test_memory_storage() -> anyhow::Result<()> {
    let storage = MemoryStorage::new();
    
    storage.put("key", b"value")?;
    let value = storage.get("key")?;
    assert_eq!(value, Some(b"value".to_vec()));
    
    Ok(())
}
```

### Integration Tests

```rust
use savitri_storage::Storage;
use tempfile::TempDir;

#[test]
fn test_storage_operations() -> anyhow::Result<()> {
    let temp_dir = TempDir::new()?;
    let storage = Storage::new(temp_dir.path())?;
    
    // Test operations
    storage.put("test_key", b"test_value")?;
    let value = storage.get("test_key")?;
    assert_eq!(value, Some(b"test_value".to_vec()));
    
    Ok(())
}
```

## Benchmarks

Run benchmarks with:

```bash
cargo bench
```

## Features Flags

- `rocksdb` (default): Enable RocksDB storage
- `memory`: Enable in-memory storage for testing

## Platform Support

### Windows

Requires Visual Studio Build Tools 2019/2022:

```powershell
# Add C++ compiler to PATH
$env:PATH = "C:\Program Files (x86)\Microsoft Visual Studio\18\BuildTools\VC\Tools\MSVC\14.29.30133\bin\HostX64\x64;" + $env:PATH

# Build
cargo build --release
```

### Linux/macOS

Works out of the box:

```bash
cargo build --release
```

## Performance Tips

1. **Use Batch Operations**: Combine multiple writes into a single batch
2. **Configure Cache**: Adjust cache size based on your memory constraints
3. **Use Snapshots**: For consistent reads during heavy write periods
4. **Enable Compression**: Use Snappy compression for better space efficiency
5. **Monitor Metrics**: Track performance metrics to identify bottlenecks
6. **Regular Compaction**: Schedule regular database compaction

## Error Handling

The storage layer uses `anyhow::Result` for error handling:

```rust
use savitri_storage::Storage;

fn storage_example() -> anyhow::Result<()> {
    let storage = Storage::new("path/to/database")?;
    
    storage.put("key", b"value")?;
    let value = storage.get("key")?;
    
    match value {
        Some(data) => println!("Found data: {:?}", data),
        None => println!("Key not found"),
    }
    
    Ok(())
}
```

## Thread Safety

The storage layer is fully thread-safe:

```rust
use savitri_storage::Storage;
use std::sync::Arc;
use std::thread;

let storage = Arc::new(Storage::new("path/to/database")?);
let mut handles = vec![];

// Spawn multiple threads
for i in 0..10 {
    let storage_clone = Arc::clone(&storage);
    let handle = thread::spawn(move || {
        storage_clone.put(format!("key_{}", i), format!("value_{}", i)).unwrap();
    });
    handles.push(handle);
}

// Wait for all threads to complete
for handle in handles {
    handle.join().unwrap();
}
```

## License

Apache 2.0 - see LICENSE file for details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For support and questions:

- Create an issue on GitHub
- Check the documentation
- Review the examples in the `examples/` directory
