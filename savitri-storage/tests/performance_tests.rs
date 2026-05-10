//! Performance Tests for Savitri Storage Layer
//!
//! This module contains performance benchmarks and tests for the storage layer.

use savitri_storage::{storage::Storage, FlRetentionConfig, FlStorage};
use std::time::Instant;
use tempfile::TempDir;

#[test]
fn test_storage_write_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing storage write performance...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    let start = Instant::now();
    let num_operations = 10_000;

    for i in 0..num_operations {
        let key = format!("perf_key_{}", i);
        let value = format!("perf_value_{}", i);
        storage.put(key.as_bytes(), value.as_bytes())?;
    }

    let duration = start.elapsed();
    let ops_per_sec = num_operations as f64 / duration.as_secs_f64();

    println!("  Write performance: {:.2} ops/sec", ops_per_sec);
    println!(
        "  Average write time: {:.2} μs",
        duration.as_micros() as f64 / num_operations as f64
    );

    // Performance should be reasonable (> 1000 ops/sec)
    assert!(
        ops_per_sec > 1000.0,
        "Write performance too low: {:.2} ops/sec",
        ops_per_sec
    );

    println!("✓ Storage write performance is acceptable");
    Ok(())
}

#[test]
fn test_storage_read_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing storage read performance...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Pre-populate data
    let num_entries = 10_000;
    for i in 0..num_entries {
        let key = format!("perf_key_{}", i);
        let value = format!("perf_value_{}", i);
        storage.put(key.as_bytes(), value.as_bytes())?;
    }

    let start = Instant::now();

    for i in 0..num_entries {
        let key = format!("perf_key_{}", i);
        let _value = storage.get(key.as_bytes())?;
    }

    let duration = start.elapsed();
    let ops_per_sec = num_entries as f64 / duration.as_secs_f64();

    println!("  Read performance: {:.2} ops/sec", ops_per_sec);
    println!(
        "  Average read time: {:.2} μs",
        duration.as_micros() as f64 / num_entries as f64
    );

    // Read performance should be better than write (> 5000 ops/sec)
    assert!(
        ops_per_sec > 5000.0,
        "Read performance too low: {:.2} ops/sec",
        ops_per_sec
    );

    println!("✓ Storage read performance is acceptable");
    Ok(())
}

#[test]
fn test_storage_memory_usage() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing storage memory usage...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Add data with different sizes
    let sizes = vec![100, 1000, 10000, 100000];

    for size in sizes {
        let data = vec![42u8; size];
        let key = format!("mem_test_{}", size);

        let start = Instant::now();
        storage.put(key.as_bytes(), &data)?;
        let put_time = start.elapsed();

        let start = Instant::now();
        let _retrieved = storage.get(key.as_bytes())?;
        let get_time = start.elapsed();

        println!("  Size {}: put={:?}, get={:?}", size, put_time, get_time);

        // Operations should complete in reasonable time
        assert!(
            put_time.as_millis() < 100,
            "Put operation too slow for size {}",
            size
        );
        assert!(
            get_time.as_millis() < 50,
            "Get operation too slow for size {}",
            size
        );
    }

    println!("✓ Storage memory usage is acceptable");
    Ok(())
}

#[test]
fn test_fl_storage_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FL storage performance...");

    let mut fl_storage = FlStorage::new()?;

    // Test model storage performance
    let start = Instant::now();
    let num_models = 1_000;

    for i in 0..num_models {
        let model = savitri_storage::fl::ModelData {
            model_id: i,
            version: 1,
            data: vec![i as u8; 1000],
        };
        fl_storage.put_model(model)?;
    }

    let model_put_duration = start.elapsed();
    let model_ops_per_sec = num_models as f64 / model_put_duration.as_secs_f64();

    println!("  Model put performance: {:.2} ops/sec", model_ops_per_sec);

    // Test round storage performance
    let start = Instant::now();
    let num_rounds = 500;

    for i in 0..num_rounds {
        let round = savitri_storage::fl::RoundState {
            round_id: i,
            status: "active".to_string(),
            participants: vec![[i as u8; 32]],
        };
        fl_storage.put_round(round)?;
    }

    let round_put_duration = start.elapsed();
    let round_ops_per_sec = num_rounds as f64 / round_put_duration.as_secs_f64();

    println!("  Round put performance: {:.2} ops/sec", round_ops_per_sec);

    // Performance should be reasonable
    assert!(model_ops_per_sec > 1000.0, "Model put performance too low");
    assert!(round_ops_per_sec > 1000.0, "Round put performance too low");

    println!("✓ FL storage performance is acceptable");
    Ok(())
}

#[test]
fn test_fl_retention_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FL retention performance...");

    let mut fl_storage = FlStorage::new()?;

    // Add a large dataset
    let num_items = 10_000;
    for i in 0..num_items {
        let model = savitri_storage::fl::ModelData {
            model_id: i,
            version: 1,
            data: vec![i as u8; 100],
        };
        fl_storage.put_model(model)?;

        let round = savitri_storage::fl::RoundState {
            round_id: i,
            status: "completed".to_string(),
            participants: vec![[i as u8; 32]],
        };
        fl_storage.put_round(round)?;
    }

    // Test retention performance with different limits
    let retention_configs = vec![
        (100, 50),    // Small retention
        (1000, 500),  // Medium retention
        (5000, 2500), // Large retention
    ];

    for (max_models, max_rounds) in retention_configs {
        let start = Instant::now();

        let config = FlRetentionConfig {
            max_models,
            max_rounds,
        };
        let outcome = fl_storage.apply_retention(config)?;

        let duration = start.elapsed();

        println!(
            "  Retention ({}, {}): {:?} (removed {} models, {} rounds)",
            max_models, max_rounds, duration, outcome.models_removed, outcome.rounds_removed
        );

        // Retention should complete in reasonable time
        assert!(duration.as_secs() < 5, "Retention operation too slow");

        // Re-populate for next test
        for i in num_items..num_items + outcome.models_removed as u64 {
            let model = savitri_storage::fl::ModelData {
                model_id: i,
                version: 1,
                data: vec![i as u8; 100],
            };
            fl_storage.put_model(model)?;
        }

        for i in num_items..num_items + outcome.rounds_removed as u64 {
            let round = savitri_storage::fl::RoundState {
                round_id: i,
                status: "completed".to_string(),
                participants: vec![[i as u8; 32]],
            };
            fl_storage.put_round(round)?;
        }
    }

    println!("✓ FL retention performance is acceptable");
    Ok(())
}

#[test]
fn test_concurrent_performance() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing concurrent performance...");

    let temp_dir = TempDir::new()?;
    let temp_dir_path = temp_dir.path().to_path_buf();

    let start = Instant::now();
    let num_threads = 4;
    let ops_per_thread = 1000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let path = temp_dir_path.clone();
            std::thread::spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let mut storage = Storage::new(&path)?;

                    for i in 0..ops_per_thread {
                        let key = format!("thread_{}_key_{}", thread_id, i);
                        let value = format!("thread_{}_value_{}", thread_id, i);
                        storage.put(key.as_bytes(), value.as_bytes())?;
                    }

                    Ok(())
                },
            )
        })
        .collect();

    // Wait for all threads to complete
    for handle in handles {
        handle
            .join()
            .unwrap()
            .map_err(|e| format!("Thread panic: {:?}", e))?;
    }

    let total_duration = start.elapsed();
    let total_ops = (num_threads * ops_per_thread) as f64;
    let ops_per_sec = total_ops / total_duration.as_secs_f64();

    println!("  Concurrent performance: {:.2} ops/sec", ops_per_sec);
    println!("  Total operations: {}", total_ops);
    println!("  Total time: {:?}", total_duration);

    // Concurrent performance should be reasonable
    assert!(
        ops_per_sec > 500.0,
        "Concurrent performance too low: {:.2} ops/sec",
        ops_per_sec
    );

    println!("✓ Concurrent performance is acceptable");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Savitri Storage Performance Tests ===\n");

    // Run all performance tests
    test_storage_write_performance()?;
    test_storage_read_performance()?;
    test_storage_memory_usage()?;
    test_fl_storage_performance()?;
    test_fl_retention_performance()?;
    test_concurrent_performance()?;

    println!("\n=== All Performance Tests Passed! ===");
    println!("✅ Savitri Storage performance tests completed successfully");

    Ok(())
}
