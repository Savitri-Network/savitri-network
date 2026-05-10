//! Stress Tests for Savitri Storage Layer
//!
//! This module contains stress tests that push the storage layer to its limits.

use savitri_storage::{storage::Storage, FlRetentionConfig, FlStorage};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

#[test]
fn test_high_volume_writes() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing high volume writes...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    let start = Instant::now();
    let num_operations = 100_000;

    for i in 0..num_operations {
        let key = format!("stress_key_{}", i);
        let value = format!("stress_value_{}", i);
        storage.put(key.as_bytes(), value.as_bytes())?;

        // Progress reporting
        if i % 10_000 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let ops_per_sec = i as f64 / elapsed.as_secs_f64();
            println!(
                "  Progress: {}/{} ({:.1} ops/sec)",
                i, num_operations, ops_per_sec
            );
        }
    }

    let total_duration = start.elapsed();
    let ops_per_sec = num_operations as f64 / total_duration.as_secs_f64();

    println!(
        "  High volume write performance: {:.2} ops/sec",
        ops_per_sec
    );
    println!("  Total time: {:?}", total_duration);

    // Should handle high volume reasonably well
    assert!(ops_per_sec > 100.0, "High volume write performance too low");

    println!("✓ High volume writes handled successfully");
    Ok(())
}

#[test]
fn test_memory_pressure() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing memory pressure...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Test with progressively larger data
    let sizes = vec![1_000, 10_000, 100_000, 1_000_000];

    for (i, size) in sizes.iter().enumerate() {
        let data = vec![42u8; *size];
        let key = format!("memory_pressure_{}", i);

        let start = Instant::now();
        storage.put(key.as_bytes(), &data)?;
        let put_time = start.elapsed();

        let start = Instant::now();
        let _retrieved = storage.get(key.as_bytes())?;
        let get_time = start.elapsed();

        println!("  Size {}: put={:?}, get={:?}", size, put_time, get_time);

        // Even large operations should complete
        assert!(
            put_time.as_secs() < 10,
            "Put operation too slow for size {}",
            size
        );
        assert!(
            get_time.as_secs() < 5,
            "Get operation too slow for size {}",
            size
        );
    }

    println!("✓ Memory pressure handled successfully");
    Ok(())
}

#[test]
fn test_concurrent_stress() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing concurrent stress...");

    let temp_dir = Arc::new(TempDir::new()?);
    let num_threads = 8;
    let ops_per_thread = 5_000;

    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let temp_dir = Arc::clone(&temp_dir);
            thread::spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let mut storage = Storage::new(temp_dir.path())?;

                    for i in 0..ops_per_thread {
                        let key = format!("stress_thread_{}_key_{}", thread_id, i);
                        let value = format!("stress_thread_{}_value_{}", thread_id, i);
                        storage.put(key.as_bytes(), value.as_bytes())?;

                        // Occasionally read some data
                        if i % 100 == 0 && i > 0 {
                            let read_key = format!("stress_thread_{}_key_{}", thread_id, i / 2);
                            let _ = storage.get(read_key.as_bytes())?;
                        }
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

    println!(
        "  Concurrent stress performance: {:.2} ops/sec",
        ops_per_sec
    );
    println!("  Total operations: {}", total_ops);
    println!("  Total time: {:?}", total_duration);

    // Should handle concurrent stress reasonably
    assert!(ops_per_sec > 50.0, "Concurrent stress performance too low");

    println!("✓ Concurrent stress handled successfully");
    Ok(())
}

#[test]
fn test_fl_storage_stress() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FL storage stress...");

    let mut fl_storage = FlStorage::new()?;

    // Stress test with large number of models and rounds
    let start = Instant::now();
    let num_items = 50_000;

    for i in 0..num_items {
        let model = savitri_storage::fl::ModelData {
            model_id: i,
            version: 1,
            data: vec![i as u8; 500],
        };
        fl_storage.put_model(model)?;

        let round = savitri_storage::fl::RoundState {
            round_id: i,
            status: "active".to_string(),
            participants: vec![[i as u8; 32]],
        };
        fl_storage.put_round(round)?;

        // Progress reporting
        if i % 10_000 == 0 && i > 0 {
            let elapsed = start.elapsed();
            let ops_per_sec = (i * 2) as f64 / elapsed.as_secs_f64(); // 2 ops per iteration
            println!(
                "  Progress: {}/{} ({:.1} ops/sec)",
                i, num_items, ops_per_sec
            );
        }
    }

    let total_duration = start.elapsed();
    let total_ops = (num_items * 2) as f64;
    let ops_per_sec = total_ops / total_duration.as_secs_f64();

    println!(
        "  FL storage stress performance: {:.2} ops/sec",
        ops_per_sec
    );
    println!("  Total operations: {}", total_ops);
    println!("  Total time: {:?}", total_duration);

    // Should handle FL storage stress
    assert!(ops_per_sec > 200.0, "FL storage stress performance too low");

    println!("✓ FL storage stress handled successfully");
    Ok(())
}

#[test]
fn test_retention_stress() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing retention stress...");

    let mut fl_storage = FlStorage::new()?;

    // Build up a large dataset
    let num_items = 100_000;
    println!("  Building dataset of {} items...", num_items);

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

    // Test retention with aggressive policies
    let retention_tests = vec![
        (100, 50),     // Very aggressive
        (1000, 500),   // Aggressive
        (10000, 5000), // Moderate
    ];

    for (max_models, max_rounds) in retention_tests {
        println!(
            "  Testing retention policy: max_models={}, max_rounds={}",
            max_models, max_rounds
        );

        let start = Instant::now();

        let config = FlRetentionConfig {
            max_models,
            max_rounds,
        };
        let outcome = fl_storage.apply_retention(config)?;

        let duration = start.elapsed();

        println!(
            "    Retention completed in {:?} (removed {} models, {} rounds)",
            duration, outcome.models_removed, outcome.rounds_removed
        );

        // Should complete in reasonable time even for large datasets
        assert!(
            duration.as_secs() < 30,
            "Retention operation too slow for large dataset"
        );

        // Rebuild dataset for next test
        let start_rebuild = Instant::now();
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

        let rebuild_duration = start_rebuild.elapsed();
        println!("    Dataset rebuild completed in {:?}", rebuild_duration);
    }

    println!("✓ Retention stress handled successfully");
    Ok(())
}

#[test]
fn test_mixed_workload_stress() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing mixed workload stress...");

    let temp_dir = Arc::new(TempDir::new()?);
    let fl_storage = Arc::new(std::sync::Mutex::new(FlStorage::new()?));

    let num_threads = 6;
    let ops_per_thread = 2_000;

    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let temp_dir = Arc::clone(&temp_dir);
            let fl_storage = Arc::clone(&fl_storage);

            thread::spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let mut storage = Storage::new(temp_dir.path())?;

                    for i in 0..ops_per_thread {
                        match thread_id % 3 {
                            0 => {
                                // Storage operations
                                let key = format!("mixed_{}_{}", thread_id, i);
                                let value = format!("value_{}_{}", thread_id, i);
                                storage.put(key.as_bytes(), value.as_bytes())?;
                                let _ = storage.get(key.as_bytes())?;
                            }
                            1 => {
                                // FL model operations
                                let model = savitri_storage::fl::ModelData {
                                    model_id: (thread_id * ops_per_thread + i) as u64,
                                    version: 1,
                                    data: vec![thread_id as u8; 200],
                                };
                                let mut fl = fl_storage.lock().unwrap();
                                fl.put_model(model)?;
                                let _ = fl.get_model((thread_id * ops_per_thread + i) as u64)?;
                            }
                            2 => {
                                // FL round operations
                                let round = savitri_storage::fl::RoundState {
                                    round_id: (thread_id * ops_per_thread + i) as u64,
                                    status: "active".to_string(),
                                    participants: vec![[thread_id as u8; 32]],
                                };
                                let mut fl = fl_storage.lock().unwrap();
                                fl.put_round(round)?;
                                let _ = fl.get_round((thread_id * ops_per_thread + i) as u64)?;
                            }
                            _ => unreachable!(),
                        }

                        // Occasional retention operation
                        if i % 500 == 0 && i > 0 && thread_id == 0 {
                            let mut fl = fl_storage.lock().unwrap();
                            let config = FlRetentionConfig {
                                max_models: 1000,
                                max_rounds: 500,
                            };
                            let _ = fl.apply_retention(config)?;
                        }
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

    println!("  Mixed workload performance: {:.2} ops/sec", ops_per_sec);
    println!("  Total operations: {}", total_ops);
    println!("  Total time: {:?}", total_duration);

    // Should handle mixed workload reasonably
    assert!(ops_per_sec > 25.0, "Mixed workload performance too low");

    println!("✓ Mixed workload stress handled successfully");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Savitri Storage Stress Tests ===\n");

    // Run all stress tests
    test_high_volume_writes()?;
    test_memory_pressure()?;
    test_concurrent_stress()?;
    test_fl_storage_stress()?;
    test_retention_stress()?;
    test_mixed_workload_stress()?;

    println!("\n=== All Stress Tests Passed! ===");
    println!("✅ Savitri Storage stress tests completed successfully");
    println!("🚀 Storage layer demonstrated excellent performance under stress");

    Ok(())
}
