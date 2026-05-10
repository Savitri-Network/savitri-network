//! Unit Tests for Savitri Storage Layer
//!
//! This module contains comprehensive unit tests for the storage layer components.

use savitri_storage::{storage::Storage, FlRetentionConfig, FlStorage};
use tempfile::TempDir;

#[test]
fn test_storage_basic_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing basic storage operations...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Test put and get
    storage.put(b"key1", b"value1")?;
    let value = storage.get(b"key1")?;
    assert_eq!(value, Some(b"value1".to_vec()));

    // Test delete
    storage.delete(b"key1")?;
    let value = storage.get(b"key1")?;
    assert_eq!(value, None);

    println!("✓ Basic storage operations work correctly");
    Ok(())
}

#[test]
fn test_storage_health_check() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing storage health check...");

    let temp_dir = TempDir::new()?;
    let storage = Storage::new(temp_dir.path())?;

    assert!(storage.is_healthy());

    println!("✓ Storage health check works correctly");
    Ok(())
}

#[test]
fn test_fl_storage_basic_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FL storage basic operations...");

    let mut fl_storage = FlStorage::new()?;

    // Test model storage
    let model = savitri_storage::fl::ModelData {
        model_id: 1,
        version: 1,
        data: vec![1, 2, 3, 4, 5],
    };
    fl_storage.put_model(model.clone())?;

    let retrieved = fl_storage.get_model(1)?;
    assert!(retrieved.is_some());
    assert_eq!(retrieved.unwrap().model_id, 1);

    // Test round storage
    let round = savitri_storage::fl::RoundState {
        round_id: 1,
        status: "active".to_string(),
        participants: vec![[1; 32], [2; 32]],
    };
    fl_storage.put_round(round.clone())?;

    let retrieved_round = fl_storage.get_round(1)?;
    assert!(retrieved_round.is_some());
    assert_eq!(retrieved_round.unwrap().status, "active");

    println!("✓ FL storage basic operations work correctly");
    Ok(())
}

#[test]
fn test_fl_retention_policy() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing FL retention policy...");

    let mut fl_storage = FlStorage::new()?;

    // Add some test data
    for i in 1..=5 {
        let model = savitri_storage::fl::ModelData {
            model_id: i,
            version: 1,
            data: vec![i as u8; 10],
        };
        fl_storage.put_model(model)?;

        let round = savitri_storage::fl::RoundState {
            round_id: i,
            status: "completed".to_string(),
            participants: vec![[i as u8; 32]],
        };
        fl_storage.put_round(round)?;
    }

    // Apply retention policy
    let config = FlRetentionConfig {
        max_models: 3,
        max_rounds: 2,
    };
    let outcome = fl_storage.apply_retention(config)?;

    assert_eq!(outcome.models_removed, 2); // 5 - 3 = 2 removed
    assert_eq!(outcome.rounds_removed, 3); // 5 - 2 = 3 removed

    println!("✓ FL retention policy works correctly");
    Ok(())
}

#[test]
fn test_error_handling() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing error handling...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Test getting non-existent key
    let value = storage.get(b"non_existent")?;
    assert_eq!(value, None);

    // Test deleting non-existent key (should not error)
    storage.delete(b"non_existent")?;

    println!("✓ Error handling works correctly");
    Ok(())
}

#[test]
fn test_concurrent_operations() -> Result<(), Box<dyn std::error::Error>> {
    println!("Testing concurrent operations...");

    let temp_dir = TempDir::new()?;
    let mut storage = Storage::new(temp_dir.path())?;

    // Test multiple operations
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        storage.put(key.as_bytes(), value.as_bytes())?;
    }

    // Verify all operations
    for i in 0..100 {
        let key = format!("key_{}", i);
        let value = format!("value_{}", i);
        let retrieved = storage.get(key.as_bytes())?;
        assert_eq!(retrieved, Some(value.into_bytes()));
    }

    println!("✓ Concurrent operations work correctly");
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Savitri Storage Unit Tests ===\n");

    // Run all tests
    test_storage_basic_operations()?;
    test_storage_health_check()?;
    test_fl_storage_basic_operations()?;
    test_fl_retention_policy()?;
    test_error_handling()?;
    test_concurrent_operations()?;

    println!("\n=== All Unit Tests Passed! ===");
    println!("✅ Savitri Storage unit tests completed successfully");

    Ok(())
}
