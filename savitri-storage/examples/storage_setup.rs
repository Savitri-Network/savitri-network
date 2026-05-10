//! Esempio: Storage Setup
//!

use anyhow::Result;
use savitri_storage::{Storage, StorageConfig};
use std::time::Instant;

fn main() -> Result<()> {
    println!("=== Storage Setup Example ===\n");

    // 1. Basic Setup
    println!("1. Basic storage setup...");
    let tmp_dir = tempfile::TempDir::new()?;
    let storage = Storage::new(tmp_dir.path())?;
    println!("   ✓ Storage created");

    // 2. Put/Get operations
    println!("\n2. Basic operations...");
    storage.put(b"key1", b"value1")?;
    let value = storage.get(b"key1")?;
    assert_eq!(value, Some(b"value1".to_vec()));
    println!("   ✓ Put/Get operations successful");

    // 3. Column families
    println!("\n3. Column families...");
    storage.put_cf("blocks", b"block_1", b"block_data")?;
    let block_data = storage.get_cf("blocks", b"block_1")?;
    assert_eq!(block_data, Some(b"block_data".to_vec()));
    println!("   ✓ Column family operations successful");

    // 4. Batch operations
    println!("\n4. Batch operations...");
    let mut batch = storage.begin_batch();
    batch.put(b"key2", b"value2")?;
    batch.put(b"key3", b"value3")?;
    batch.put_cf("blocks", b"block_2", b"block_data_2")?;
    batch.commit()?;
    println!("   ✓ Batch operations successful");

    // 5. Performance test
    println!("\n5. Performance test...");
    let start = Instant::now();
    for i in 0..1000 {
        storage.put(format!("key{}", i).as_bytes(), b"value")?;
    }
    let duration = start.elapsed();
    println!(
        "   ✓ 1000 writes in {:?} ({:.0} ops/sec)",
        duration,
        1000.0 / duration.as_secs_f64()
    );

    // 6. Configuration example
    println!("\n6. Advanced configuration...");
    let config = StorageConfig {
        path: tmp_dir.path().to_string_lossy().to_string(),
        cache_size: 10000,
        write_buffer_size: 64 * 1024 * 1024,
        max_write_buffer_number: 4,
        enable_compression: true,
        create_if_missing: true,
        max_open_files: 1000,
        use_fsync: true,
    };
    let _storage_config = Storage::with_config(config)?;
    println!("   ✓ Advanced configuration applied");

    println!("\n=== Example completed successfully ===");

    Ok(())
}
