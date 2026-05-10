// tests/contracts/upgrade_test.rs
use anyhow::{Context, Result};
use hex;
use savitri_node::{
    contracts::{
        base::BaseContract, events::EventSystem, gas::GasMeter, runtime::Runtime,
        storage::ContractStorage,
    },
    storage::Storage,
};
use std::path::PathBuf;
use tempfile::TempDir;

// Helper function to create a test environment
fn setup_test_env() -> (Storage, ContractStorage, Runtime, GasMeter, TempDir) {
    let (storage, tmp_dir) = create_test_storage("upgrade-test");
    let contract_address = [0xAB; 32];
    let contract_storage =
        ContractStorage::new(contract_address.to_vec()).expect("Failed to create contract storage");
    let gas_meter = GasMeter::new(1_000_000);
    let runtime = Runtime::default();

    (storage, contract_storage, runtime, gas_meter, tmp_dir)
}

// Test contract version 1
mod v1 {
    use super::*;

    pub const VERSION: u32 = 1;

    pub fn initialize(
        storage: &mut ContractStorage,
        _storage_backend: &Storage,
        _gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        storage.set(b"version", &VERSION.to_be_bytes())?;
        storage.set(b"value", &42u64.to_be_bytes())?;
        Ok(())
    }

    pub fn get_value(storage: &ContractStorage) -> Result<u64> {
        let value_bytes = storage.get(b"value").context("Value not found")?;
        Ok(u64::from_be_bytes(
            value_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid value format"))?,
        ))
    }
}

// Test contract version 2
mod v2 {
    use super::*;

    pub const VERSION: u32 = 2;

    pub fn migrate(
        storage: &mut ContractStorage,
        _storage_backend: &Storage,
        _gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Get old value
        let old_value = v1::get_value(storage)?;

        // Update to new storage layout
        storage.set(b"version", &VERSION.to_be_bytes())?;
        storage.set(b"new_value", &(old_value * 2).to_be_bytes())?;

        Ok(())
    }

    pub fn get_upgraded_value(storage: &ContractStorage) -> Result<u64> {
        let value_bytes = storage.get(b"new_value").context("New value not found")?;
        Ok(u64::from_be_bytes(
            value_bytes
                .try_into()
                .map_err(|_| anyhow::anyhow!("Invalid value format"))?,
        ))
    }
}

#[test]
fn test_contract_upgrade() -> Result<()> {
    // Setup test environment
    let (storage, mut contract_storage, _runtime, mut gas_meter, _tmp_dir) = setup_test_env();

    // 1. Deploy and initialize v1
    v1::initialize(&mut contract_storage, &storage, Some(&mut gas_meter))?;

    // Verify initial state
    let initial_value = v1::get_value(&contract_storage)?;
    assert_eq!(initial_value, 42, "Initial value should be 42");

    // 2. Perform upgrade to v2
    v2::migrate(&mut contract_storage, &storage, Some(&mut gas_meter))?;

    // 3. Verify upgrade
    // Check version was updated
    let version_bytes = contract_storage
        .get(b"version")
        .context("Version not found")?;
    let version = u32::from_be_bytes(
        version_bytes
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid version format"))?,
    );
    assert_eq!(version, v2::VERSION, "Version should be upgraded to v2");

    // Check value was migrated correctly
    let upgraded_value = v2::get_upgraded_value(&contract_storage)?;
    assert_eq!(upgraded_value, 84, "Value should be doubled after upgrade");

    // 4. Verify old value is no longer directly accessible
    assert!(
        v1::get_value(&contract_storage).is_err(),
        "Old value should not be directly accessible after upgrade"
    );

    Ok(())
}

#[test]
fn test_upgrade_access_control() -> Result<()> {
    let (storage, mut contract_storage, mut runtime, mut gas_meter, _tmp_dir) = setup_test_env();

    // Set up admin and non-admin accounts
    let admin = [0x01; 32];
    let non_admin = [0x02; 32];

    // Set admin in contract storage
    contract_storage.set(b"admin", &admin)?;

    // Test 1: Admin can upgrade
    runtime.set_caller(admin);
    let result = v1::initialize(&mut contract_storage, &storage, Some(&mut gas_meter));
    assert!(result.is_ok(), "Admin should be able to initialize");

    // Test 2: Non-admin cannot upgrade
    runtime.set_caller(non_admin);
    let result = v2::migrate(&mut contract_storage, &storage, Some(&mut gas_meter));
    assert!(
        result.is_err(),
        "Non-admin should not be able to upgrade contract"
    );

    Ok(())
}

#[test]
fn test_upgrade_rollback() -> Result<()> {
    let (storage, mut contract_storage, _runtime, mut gas_meter, _tmp_dir) = setup_test_env();

    // 1. Create a snapshot of the initial state
    let snapshot = contract_storage.create_snapshot();

    // 2. Perform upgrade
    v1::initialize(&mut contract_storage, &storage, Some(&mut gas_meter))?;
    v2::migrate(&mut contract_storage, &storage, Some(&mut gas_meter))?;

    // 3. Verify upgrade
    let upgraded_value = v2::get_upgraded_value(&contract_storage)?;
    assert_eq!(upgraded_value, 84, "Upgrade should be successful");

    // 4. Rollback to snapshot
    contract_storage.restore_snapshot(snapshot);

    // 5. Verify rollback
    assert!(
        contract_storage.get(b"version").is_err(),
        "Version should not exist after rollback"
    );

    Ok(())
}

// Helper function to create test storage
fn create_test_storage(prefix: &str) -> (Storage, TempDir) {
    let temp_dir = tempfile::tempdir().expect("Failed to create temp dir");
    let db_path = temp_dir.path().join(format!("{}-db", prefix));
    let storage = Storage::open(&db_path).expect("Failed to open storage");
    (storage, temp_dir)
}
