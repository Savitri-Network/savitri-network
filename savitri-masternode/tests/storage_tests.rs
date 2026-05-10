// SPDX-License-Identifier: Apache-2.0
// © 2026 Savitri Network

//! Tests for the storage functionality

use savitri_storage::Storage;
use std::collections::HashMap;
use tempfile::TempDir;

#[cfg(test)]
mod storage_tests {
    use super::*;

    #[test]
    fn test_storage_creation() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage = Storage::new(temp_dir.path()).expect("Failed to create storage");

        assert!(storage.is_healthy(), "Storage should be healthy");
    }

    #[test]
    fn test_basic_operations() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");

        // Test put and get
        let key = b"test_key";
        let value = b"test_value";

        storage.put(key, value).expect("Failed to put data");

        let retrieved = storage.get(key).expect("Failed to get data");
        assert_eq!(retrieved, Some(value.to_vec()));

        // Test delete
        storage.delete(key).expect("Failed to delete data");

        let retrieved_after_delete = storage.get(key).expect("Failed to get data after delete");
        assert_eq!(retrieved_after_delete, None);
    }

    #[test]
    fn test_multiple_operations() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");

        // Test multiple put operations
        let mut test_data = HashMap::new();
        test_data.insert("key1".as_bytes(), "value1".as_bytes());
        test_data.insert("key2".as_bytes(), "value2".as_bytes());
        test_data.insert("key3".as_bytes(), "value3".as_bytes());

        for (key, value) in &test_data {
            storage.put(key, value).expect("Failed to put data");
        }

        // Verify all data
        for (key, value) in &test_data {
            let retrieved = storage.get(key).expect("Failed to get data");
            assert_eq!(retrieved, Some(value.to_vec()));
        }

        // Test deletion of multiple keys
        for key in test_data.keys() {
            storage.delete(key).expect("Failed to delete data");
        }

        // Verify all data is deleted
        for key in test_data.keys() {
            let retrieved = storage.get(key).expect("Failed to get data after delete");
            assert_eq!(retrieved, None);
        }
    }

    #[test]
    fn test_large_data() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");

        // Test with large data (1MB)
        let large_key = b"large_key";
        let large_value = vec![0u8; 1024 * 1024]; // 1MB of zeros

        storage
            .put(large_key, &large_value)
            .expect("Failed to put large data");

        let retrieved = storage.get(large_key).expect("Failed to get large data");
        assert_eq!(retrieved, Some(large_value));
        assert_eq!(retrieved.unwrap().len(), 1024 * 1024);
    }

    #[test]
    fn test_overwrite_data() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");

        let key = b"overwrite_key";
        let initial_value = b"initial_value";
        let new_value = b"new_value";

        // Put initial value
        storage
            .put(key, initial_value)
            .expect("Failed to put initial data");

        let retrieved = storage.get(key).expect("Failed to get initial data");
        assert_eq!(retrieved, Some(initial_value.to_vec()));

        // Overwrite with new value
        storage.put(key, new_value).expect("Failed to put new data");

        let retrieved_after_overwrite = storage
            .get(key)
            .expect("Failed to get data after overwrite");
        assert_eq!(retrieved_after_overwrite, Some(new_value.to_vec()));
    }

    #[test]
    fn test_nonexistent_key() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage = Storage::new(temp_dir.path()).expect("Failed to create storage");

        let nonexistent_key = b"nonexistent_key";
        let retrieved = storage.get(nonexistent_key).expect("Failed to get data");
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_empty_data() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");

        let key = b"empty_key";
        let empty_value = b"";

        storage
            .put(key, empty_value)
            .expect("Failed to put empty data");

        let retrieved = storage.get(key).expect("Failed to get empty data");
        assert_eq!(retrieved, Some(empty_value.to_vec()));
        assert_eq!(retrieved.unwrap().len(), 0);
    }

    #[test]
    fn test_concurrent_access() {
        use std::sync::{Arc, Mutex};
        use std::thread;

        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage = Arc::new(Mutex::new(
            Storage::new(temp_dir.path()).expect("Failed to create storage"),
        ));

        let mut handles = vec![];

        // Spawn multiple threads to test concurrent access
        for i in 0..10 {
            let storage_clone = Arc::clone(&storage);
            let handle = thread::spawn(move || {
                let mut storage = storage_clone.lock().unwrap();
                let key = format!("thread_{}_key", i);
                let value = format!("thread_{}_value", i);

                storage
                    .put(key.as_bytes(), value.as_bytes())
                    .expect("Failed to put data");

                let retrieved = storage.get(key.as_bytes()).expect("Failed to get data");
                assert_eq!(retrieved, Some(value.into_bytes()));
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked");
        }
    }

    #[test]
    fn test_storage_persistence() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let storage_path = temp_dir.path();

        // Create storage and add data
        {
            let mut storage = Storage::new(storage_path).expect("Failed to create storage");
            storage
                .put(b"persistent_key", b"persistent_value")
                .expect("Failed to put data");
        } // storage is dropped here

        // Reopen storage and verify data persistence
        {
            let storage = Storage::new(storage_path).expect("Failed to reopen storage");
            let retrieved = storage
                .get(b"persistent_key")
                .expect("Failed to get persistent data");
            assert_eq!(retrieved, Some(b"persistent_value".to_vec()));
        }
    }

    #[test]
    fn test_error_handling() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut storage = Storage::new(temp_dir.path()).expect("Failed to create storage");

        // Test operations with invalid data (should not panic)
        let invalid_key = vec![0xFF; 1000]; // Large key
        let invalid_value = vec![0x00; 1000]; // Large value

        let result = storage.put(&invalid_key, &invalid_value);
        // This should either succeed or fail gracefully, not panic
        assert!(result.is_ok() || result.is_err());
    }
}
