//! Integration Tests for Savitri Storage Layer
//!
//! This module contains integration tests that verify the interaction
//! between different storage components.

use savitri_storage::{storage::Storage, FlRetentionConfig, FlStorage};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

#[test]
fn test_storage_fl_integration() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing Storage-FL integration...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;
    let mut fl_storage = FlStorage::new()?;

    // Store data in main storage
    storage.put(b"fl_config", b"max_models=1000")?;

    // Store FL data
    let model = savitri_storage::fl::ModelData {
        model_id: 1,
        version: 1,
        data: vec![1, 2, 3, 4, 5],
    };
    fl_storage.put_model(model)?;

    // Verify both storages work independently
    let config = storage.get(b"fl_config")?;
    assert_eq!(config, Some(b"max_models=1000".to_vec()));

    let retrieved_model = fl_storage.get_model(1)?;
    assert!(retrieved_model.is_some());

    println!("✓ Storage-FL integration works correctly");
    Ok(())
}

#[test]
fn test_multi_threaded_access() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing multi-threaded access...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Pre-populate some data
    for i in 0..10 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        storage.put(key.as_bytes(), value.as_bytes())?;
    }

    // Create multiple threads for concurrent access to shared storage
    let storage = Arc::new(std::sync::Mutex::new(storage));
    let handles: Vec<_> = (0..5)
        .map(|thread_id| {
            let storage = Arc::clone(&storage);
            thread::spawn(
                move || -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
                    for i in 0..10 {
                        let key = format!("key_{}", i);
                        let value = {
                            let storage = storage.lock().unwrap();
                            storage.get(key.as_bytes())?
                        };
                        assert!(
                            value.is_some(),
                            "Thread {} could not read key {}",
                            thread_id,
                            i
                        );
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

    println!("✓ Multi-threaded access works correctly");
    Ok(())
}

#[test]
fn test_large_data_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing large data operations...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Test with large data (1MB)
    let large_data = vec![42u8; 1_048_576]; // 1MB of data
    storage.put(b"large_key", &large_data)?;

    let retrieved = storage.get(b"large_key")?;
    assert_eq!(retrieved, Some(large_data));

    // Test with multiple large entries
    for i in 0..10 {
        let data = vec![i as u8; 100_000]; // 100KB each
        let key = format!("large_{}", i);
        storage.put(key.as_bytes(), &data)?;
    }

    // Verify all entries
    for i in 0..10 {
        let key = format!("large_{}", i);
        let retrieved = storage.get(key.as_bytes())?;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().len(), 100_000);
    }

    println!("✓ Large data operations work correctly");
    Ok(())
}

#[test]
fn test_fl_retention_integration() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FL retention integration...");

    let mut fl_storage = FlStorage::new()?;

    // Add a large number of models and rounds
    for i in 1..=100 {
        let model = savitri_storage::fl::ModelData {
            model_id: i,
            version: 1,
            data: vec![i as u8; 1000],
        };
        fl_storage.put_model(model)?;

        let round = savitri_storage::fl::RoundState {
            round_id: i,
            status: "completed".to_string(),
            participants: vec![[i as u8; 32]],
        };
        fl_storage.put_round(round)?;
    }

    // Apply aggressive retention policy
    let config = FlRetentionConfig {
        max_models: 10,
        max_rounds: 5,
    };
    let outcome = fl_storage.apply_retention(config)?;

    assert_eq!(outcome.models_removed, 90); // 100 - 10 = 90
    assert_eq!(outcome.rounds_removed, 95); // 100 - 5 = 95

    // Verify remaining data
    let remaining_models = (1..=100)
        .filter(|&i| fl_storage.get_model(i).unwrap().is_some())
        .count();
    let remaining_rounds = (1..=100)
        .filter(|&i| fl_storage.get_round(i).unwrap().is_some())
        .count();

    assert_eq!(remaining_models, 10);
    assert_eq!(remaining_rounds, 5);

    println!("✓ FL retention integration works correctly");
    Ok(())
}

#[test]
fn test_error_recovery() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing error recovery...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Normal operations
    storage.put(b"key1", b"value1")?;
    storage.put(b"key2", b"value2")?;

    // Simulate error conditions
    let _ = storage.get(b"non_existent")?; // Should return None
    storage.delete(b"non_existent")?; // Should not error

    // Verify storage is still functional
    let value1 = storage.get(b"key1")?;
    let value2 = storage.get(b"key2")?;

    assert_eq!(value1, Some(b"value1".to_vec()));
    assert_eq!(value2, Some(b"value2".to_vec()));

    println!("✓ Error recovery works correctly");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Savitri Storage Integration Tests ===\n");

    // Run all integration tests
    test_storage_fl_integration()?;
    test_multi_threaded_access()?;
    test_large_data_operations()?;
    test_fl_retention_integration()?;
    test_error_recovery()?;

    println!("\n=== All Integration Tests Passed! ===");
    println!("✅ Savitri Storage integration tests completed successfully");

    Ok(())
}
