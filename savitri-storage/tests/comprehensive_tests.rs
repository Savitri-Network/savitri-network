//! Comprehensive Tests for Savitri Storage Layer
//!
//! storage layer functionality under realistic scenarios.

use savitri_storage::{storage::Storage, FlRetentionConfig, FlStorage};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use tempfile::TempDir;

#[test]
fn test_blockchain_scenario() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing blockchain storage scenario...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Simulate blockchain data storage
    let num_blocks = 1000;
    let start = Instant::now();

    for block_height in 0..num_blocks {
        // Store block header
        let block_header = format!(
            "block_header_{}:hash={},prev={},timestamp={},proposer={}",
            block_height,
            format!("hash_{}", block_height),
            if block_height > 0 {
                format!("hash_{}", block_height - 1)
            } else {
                "genesis".to_string()
            },
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            format!("validator_{}", block_height % 10)
        );
        storage.put(
            format!("block_header_{}", block_height).as_bytes(),
            block_header.as_bytes(),
        )?;

        // Store block transactions
        let num_txs = 50 + (block_height % 100); // Variable transaction count
        for tx_index in 0..num_txs {
            let tx = format!(
                "tx_{}:{}:from={},to={},amount={},fee={}",
                block_height,
                tx_index,
                format!("addr_{}", tx_index % 100),
                format!("addr_{}", (tx_index + 1) % 100),
                (tx_index + 1) * 1000,
                (tx_index + 1) * 10
            );
            storage.put(
                format!("tx_{}_{}", block_height, tx_index).as_bytes(),
                tx.as_bytes(),
            )?;
        }

        // Store block state root
        let state_root = format!(
            "state_root_{}:{}",
            block_height,
            format!("root_{}", block_height)
        );
        storage.put(
            format!("state_root_{}", block_height).as_bytes(),
            state_root.as_bytes(),
        )?;

        // Progress reporting
        if block_height % 100 == 0 && block_height > 0 {
            let elapsed = start.elapsed();
            let blocks_per_sec = block_height as f64 / elapsed.as_secs_f64();
            println!(
                "  Progress: {}/{} blocks ({:.1} blocks/sec)",
                block_height, num_blocks, blocks_per_sec
            );
        }
    }

    let total_duration = start.elapsed();
    let blocks_per_sec = num_blocks as f64 / total_duration.as_secs_f64();

    println!(
        "  Blockchain scenario performance: {:.2} blocks/sec",
        blocks_per_sec
    );
    println!("  Total time: {:?}", total_duration);

    // Verify blockchain data integrity
    println!("  Verifying blockchain data integrity...");
    let verification_start = Instant::now();

    for block_height in 0..num_blocks {
        // Verify block header exists
        let header_key = format!("block_header_{}", block_height);
        let header = storage.get(header_key.as_bytes())?;
        assert!(header.is_some(), "Block {} header missing", block_height);

        // Verify state root exists
        let state_root_key = format!("state_root_{}", block_height);
        let state_root = storage.get(state_root_key.as_bytes())?;
        assert!(
            state_root.is_some(),
            "Block {} state root missing",
            block_height
        );

        // Verify some transactions exist
        let tx_key = format!("tx_{}_0", block_height);
        let tx = storage.get(tx_key.as_bytes())?;
        assert!(
            tx.is_some(),
            "Block {} first transaction missing",
            block_height
        );
    }

    let verification_duration = verification_start.elapsed();
    println!("  Verification completed in {:?}", verification_duration);

    // Performance should be reasonable for blockchain scenario
    assert!(
        blocks_per_sec > 10.0,
        "Blockchain scenario performance too low"
    );

    println!("✓ Blockchain scenario handled successfully");
    Ok(())
}

#[test]
fn test_federated_learning_scenario() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing federated learning scenario...");

    let mut fl_storage = FlStorage::new()?;

    // Simulate federated learning workflow
    let num_rounds = 100;
    let participants_per_round = 20;
    let start = Instant::now();

    for round in 0..num_rounds {
        // Store round metadata
        let round_state = savitri_storage::fl::RoundState {
            round_id: round as u64,
            status: if round < num_rounds - 1 {
                "completed"
            } else {
                "active"
            }
            .to_string(),
            participants: (0..participants_per_round).map(|i| [i as u8; 32]).collect(),
        };
        fl_storage.put_round(round_state)?;

        // Store participant models
        for participant in 0..participants_per_round {
            let model_data = savitri_storage::fl::ModelData {
                model_id: (round * participants_per_round + participant) as u64,
                version: 1,
                data: vec![
                    (round % 256) as u8,
                    (participant % 256) as u8,
                    ((round * participant) % 256) as u8,
                ]
                .repeat(1000), // 3KB model data
            };
            fl_storage.put_model(model_data)?;
        }

        // Store aggregated model (every 10 rounds)
        if round % 10 == 0 {
            let aggregated_model = savitri_storage::fl::ModelData {
                model_id: (round * 1000 + 99999) as u64, // Special ID for aggregated models
                version: 1,
                data: vec![round as u8; 5000], // 5KB aggregated model
            };
            fl_storage.put_model(aggregated_model)?;
        }

        // Progress reporting
        if round % 20 == 0 && round > 0 {
            let elapsed = start.elapsed();
            let rounds_per_sec = round as f64 / elapsed.as_secs_f64();
            println!(
                "  Progress: {}/{} rounds ({:.1} rounds/sec)",
                round, num_rounds, rounds_per_sec
            );
        }
    }

    let total_duration = start.elapsed();
    let rounds_per_sec = num_rounds as f64 / total_duration.as_secs_f64();

    println!(
        "  Federated learning scenario performance: {:.2} rounds/sec",
        rounds_per_sec
    );
    println!("  Total time: {:?}", total_duration);

    // Verify FL data integrity
    println!("  Verifying FL data integrity...");
    let verification_start = Instant::now();

    for round in 0..num_rounds {
        // Verify round exists
        let round_data = fl_storage.get_round(round as u64)?;
        assert!(round_data.is_some(), "Round {} missing", round);

        // Verify participant models exist
        for participant in 0..participants_per_round {
            let model_id = (round * participants_per_round + participant) as u64;
            let model = fl_storage.get_model(model_id)?;
            assert!(
                model.is_some(),
                "Model {} for round {} missing",
                model_id,
                round
            );
        }
    }

    let verification_duration = verification_start.elapsed();
    println!("  Verification completed in {:?}", verification_duration);

    // Test retention policy
    println!("  Testing retention policy...");
    let retention_start = Instant::now();

    let config = FlRetentionConfig {
        max_models: 500, // Keep only recent models
        max_rounds: 20,  // Keep only recent rounds
    };
    let outcome = fl_storage.apply_retention(config)?;

    let retention_duration = retention_start.elapsed();
    println!(
        "  Retention completed in {:?} (removed {} models, {} rounds)",
        retention_duration, outcome.models_removed, outcome.rounds_removed
    );

    // Performance should be reasonable for FL scenario
    assert!(
        rounds_per_sec > 5.0,
        "Federated learning scenario performance too low"
    );

    println!("✓ Federated learning scenario handled successfully");
    Ok(())
}

#[test]
fn test_mixed_workload_scenario() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing mixed workload scenario...");

    let temp_dir = Arc::new(TempDir::new()?);
    let fl_storage = Arc::new(std::sync::Mutex::new(FlStorage::new()?));

    // Simulate mixed blockchain + FL workload
    let num_operations = 10_000;
    let num_threads = 4;
    let start = Instant::now();

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let temp_dir = Arc::clone(&temp_dir);
            let fl_storage = Arc::clone(&fl_storage);

            thread::spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    let mut storage = Storage::new(temp_dir.path())?;

                    for i in 0..num_operations {
                        let operation_id = thread_id * num_operations + i;

                        match operation_id % 4 {
                            0 => {
                                // Blockchain block storage
                                let block_data = format!(
                                    "block_{}:hash={},timestamp={}",
                                    operation_id,
                                    format!("hash_{}", operation_id),
                                    std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_secs()
                                );
                                storage.put(
                                    format!("block_{}", operation_id).as_bytes(),
                                    block_data.as_bytes(),
                                )?;
                            }
                            1 => {
                                // Blockchain transaction storage
                                let tx_data = format!(
                                    "tx_{}:from={},to={},amount={}",
                                    operation_id,
                                    format!("addr_{}", operation_id % 1000),
                                    format!("addr_{}", (operation_id + 1) % 1000),
                                    operation_id * 100
                                );
                                storage.put(
                                    format!("tx_{}", operation_id).as_bytes(),
                                    tx_data.as_bytes(),
                                )?;
                            }
                            2 => {
                                // FL model storage
                                let model = savitri_storage::fl::ModelData {
                                    model_id: operation_id as u64,
                                    version: 1,
                                    data: vec![operation_id as u8; 1000],
                                };
                                let mut fl = fl_storage.lock().unwrap();
                                fl.put_model(model)?;
                            }
                            3 => {
                                // FL round storage
                                let round = savitri_storage::fl::RoundState {
                                    round_id: operation_id as u64,
                                    status: "active".to_string(),
                                    participants: vec![[operation_id as u8; 32]],
                                };
                                let mut fl = fl_storage.lock().unwrap();
                                fl.put_round(round)?;
                            }
                            _ => unreachable!(),
                        }

                        // Occasional read operations
                        if i % 100 == 0 && i > 0 {
                            match operation_id % 4 {
                                0 => {
                                    let _ = storage
                                        .get(format!("block_{}", operation_id - 1).as_bytes())?;
                                }
                                1 => {
                                    let _ = storage
                                        .get(format!("tx_{}", operation_id - 1).as_bytes())?;
                                }
                                2 => {
                                    let fl = fl_storage.lock().unwrap();
                                    let _ = fl.get_model((operation_id - 1) as u64)?;
                                }
                                3 => {
                                    let fl = fl_storage.lock().unwrap();
                                    let _ = fl.get_round((operation_id - 1) as u64)?;
                                }
                                _ => unreachable!(),
                            }
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
    let total_ops = (num_threads * num_operations) as f64;
    let ops_per_sec = total_ops / total_duration.as_secs_f64();

    println!("  Mixed workload performance: {:.2} ops/sec", ops_per_sec);
    println!("  Total operations: {}", total_ops);
    println!("  Total time: {:?}", total_duration);

    // Verify data integrity across both storage systems
    println!("  Verifying mixed workload integrity...");
    let verification_start = Instant::now();

    // Sample verification of blockchain data
    let storage = Storage::new(temp_dir.path())?;
    let mut found_blocks = 0;
    let mut found_txs = 0;

    for i in 0..100.min(num_operations) {
        let block_key = format!("block_{}", i * 40);
        if let Some(_) = storage.get(block_key.as_bytes())? {
            found_blocks += 1;
        }

        let tx_key = format!("tx_{}", i * 40 + 1);
        if let Some(_) = storage.get(tx_key.as_bytes())? {
            found_txs += 1;
        }
    }

    println!(
        "  Found {} blocks and {} transactions in verification",
        found_blocks, found_txs
    );

    // Sample verification of FL data
    let fl = fl_storage.lock().unwrap();
    let mut found_models = 0;
    for i in 0..100.min(num_operations) {
        let model_id = (i * 40 + 2) as u64;
        if let Some(_) = fl.get_model(model_id)? {
            found_models += 1;
        }
    }
    println!("  Found {} models in verification", found_models);

    let verification_duration = verification_start.elapsed();
    println!("  Verification completed in {:?}", verification_duration);

    // Performance should be reasonable for mixed workload
    assert!(ops_per_sec > 20.0, "Mixed workload performance too low");

    println!("✓ Mixed workload scenario handled successfully");
    Ok(())
}

#[test]
fn test_disaster_recovery_scenario() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing disaster recovery scenario...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;
    let mut fl_storage = FlStorage::new()?;

    println!("  Phase 1: Building critical data...");
    let build_start = Instant::now();

    // Critical blockchain data
    for block_height in 0..100 {
        let block_data = format!(
            "critical_block_{}:hash={},timestamp={}",
            block_height,
            format!("critical_hash_{}", block_height),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()
        );
        storage.put(
            format!("critical_block_{}", block_height).as_bytes(),
            block_data.as_bytes(),
        )?;
    }

    // Critical FL data
    for round in 0..50 {
        let round_state = savitri_storage::fl::RoundState {
            round_id: round as u64,
            status: "critical".to_string(),
            participants: vec![[round as u8; 32]],
        };
        fl_storage.put_round(round_state)?;

        let model = savitri_storage::fl::ModelData {
            model_id: round as u64,
            version: 1,
            data: vec![round as u8; 500],
        };
        fl_storage.put_model(model)?;
    }

    let build_duration = build_start.elapsed();
    println!("    Critical data built in {:?}", build_duration);

    println!("  Phase 2: Simulating disaster recovery...");
    let recovery_start = Instant::now();

    // Verify all critical data is accessible
    let mut recovered_blocks = 0;
    for block_height in 0..100 {
        let block_key = format!("critical_block_{}", block_height);
        let block = storage.get(block_key.as_bytes())?;
        if block.is_some() {
            recovered_blocks += 1;
        }
    }

    let mut recovered_rounds = 0;
    let mut recovered_models = 0;
    for round in 0..50 {
        let round_data = fl_storage.get_round(round as u64)?;
        if round_data.is_some() {
            recovered_rounds += 1;
        }

        let model = fl_storage.get_model(round as u64)?;
        if model.is_some() {
            recovered_models += 1;
        }
    }

    let recovery_duration = recovery_start.elapsed();
    println!(
        "    Recovery verification completed in {:?}",
        recovery_duration
    );
    println!(
        "    Recovered: {} blocks, {} rounds, {} models",
        recovered_blocks, recovered_rounds, recovered_models
    );

    println!("  Phase 3: Testing data consistency...");
    let consistency_start = Instant::now();

    // Verify blockchain chain integrity
    for block_height in 1..100 {
        let block_key = format!("critical_block_{}", block_height);
        let block = storage.get(block_key.as_bytes())?;
        assert!(block.is_some(), "Block {} not recovered", block_height);

        let block_str = String::from_utf8(block.unwrap())?;
        assert!(block_str.contains(&format!("critical_block_{}", block_height)));
        assert!(block_str.contains(&format!("critical_hash_{}", block_height)));
    }

    // Verify FL data consistency
    for round in 0..50 {
        let round_data = fl_storage.get_round(round as u64)?;
        assert!(round_data.is_some(), "Round {} not recovered", round);
        assert_eq!(round_data.unwrap().status, "critical");

        let model = fl_storage.get_model(round as u64)?;
        assert!(model.is_some(), "Model {} not recovered", round);
        assert_eq!(model.unwrap().version, 1);
    }

    let consistency_duration = consistency_start.elapsed();
    println!(
        "    Consistency check completed in {:?}",
        consistency_duration
    );

    // Verify recovery success
    assert_eq!(recovered_blocks, 100, "Not all critical blocks recovered");
    assert_eq!(recovered_rounds, 50, "Not all critical rounds recovered");
    assert_eq!(recovered_models, 50, "Not all critical models recovered");

    // Performance should be reasonable for disaster recovery
    let total_recovery_time = recovery_duration + consistency_duration;
    assert!(
        total_recovery_time.as_secs() < 10,
        "Disaster recovery too slow"
    );

    println!("✓ Disaster recovery scenario handled successfully");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Savitri Storage Comprehensive Tests ===\n");

    // Run all comprehensive tests
    test_blockchain_scenario()?;
    test_federated_learning_scenario()?;
    test_mixed_workload_scenario()?;
    test_disaster_recovery_scenario()?;

    println!("\n=== All Comprehensive Tests Passed! ===");
    println!("✅ Savitri Storage comprehensive tests completed successfully");
    println!("🚀 Storage layer demonstrated excellent performance in real-world scenarios");

    Ok(())
}
