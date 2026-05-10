//! BaseContract: Base contract with standard functions
//!
//! This module implements BaseContract that all contracts must extend:
//! - Storage layout: slots 0-99 reserved
//! - Functions: owner(), transfer_ownership(), version(), upgrade(), pause(), unpause()
//! - Governance hooks
//! - Fee hooks
//!
//! # Storage Layout
//! Slots 0-99 are reserved for BaseContract:
//! - Slot 0: owner_address (32 bytes)
//! - Slot 1: version (u64, stored as 32 bytes)
//! - Slot 2: governance_hook_enabled (bool, stored as 32 bytes)
//! - Slot 3: fee_hook_enabled (bool, stored as 32 bytes)
//! - Slot 4: paused (bool, stored as 32 bytes)
//! - Slot 5-99: Reserved for future use

use crate::contracts::events::StandardEvent;
use crate::contracts::gas::GasMeter;
use crate::contracts::runtime::Runtime;
use crate::contracts::storage::ContractStorage;
use crate::governance::proposals::ProposalAction;
use anyhow::{Context, Result};
use hex;
use savitri_storage::storage::contracts::ContractInfo;
use savitri_storage::Storage;

/// Reserved slots for BaseContract (0-99)
pub const BASE_CONTRACT_SLOT_START: u64 = 0;
pub const BASE_CONTRACT_SLOT_END: u64 = 99;

/// Specific slots for BaseContract fields
pub const SLOT_OWNER: u64 = 0;
pub const SLOT_VERSION: u64 = 1;
pub const SLOT_GOVERNANCE_HOOK_ENABLED: u64 = 2;
pub const SLOT_FEE_HOOK_ENABLED: u64 = 3;
pub const SLOT_PAUSED: u64 = 4;

/// Base contract
///
/// All contracts must extend BaseContract.
/// Slots 0-99 are reserved for BaseContract.
///
/// # Storage Layout
/// BaseContract fields are stored in contract storage
/// using reserved slots 0-99. Functions in this module
/// provide type-safe access to these slots.
pub struct BaseContract;

impl BaseContract {
    /// Checks if a slot is reserved for BaseContract
    ///
    /// # Arguments
    /// * `slot` - Slot to check
    ///
    /// # Returns
    /// `true` if the slot is reserved (0-99), `false` otherwise
    pub fn is_reserved_slot(slot: u64) -> bool {
        (BASE_CONTRACT_SLOT_START..=BASE_CONTRACT_SLOT_END).contains(&slot)
    }

    /// Validates that a slot is not reserved for BaseContract
    ///
    /// # Arguments
    ///
    /// # Returns
    /// `Ok(())` if the slot is not reserved, `Err` if it is reserved
    pub fn validate_slot_not_reserved(slot: u64) -> Result<()> {
        if Self::is_reserved_slot(slot) {
            anyhow::bail!(
                "Slot {} is reserved for BaseContract (slots 0-99 are reserved)",
                slot
            );
        }
        Ok(())
    }

    /// Initializes BaseContract in contract storage
    ///
    /// Writes initial BaseContract values to reserved slots.
    /// Must be called during contract deployment.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `owner_address` - Owner address (must be 32 bytes)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Error if owner_address is not 32 bytes or other errors
    pub fn initialize(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner_address: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if owner_address.len() != 32 {
            anyhow::bail!(
                "owner_address must be exactly 32 bytes, got {}",
                owner_address.len()
            );
        }

        // Initialize owner (slot 0)
        let mut owner_value = vec![0u8; 32];
        owner_value.copy_from_slice(owner_address);
        contract_storage
            .sstore_reserved(storage, SLOT_OWNER, owner_value, gas_meter.as_deref_mut())
            .with_context(|| "Failed to initialize owner in BaseContract storage")?;

        // Initialize version (slot 1) - initial version is 1
        let version_value = Self::u64_to_storage_value(1);
        contract_storage
            .sstore_reserved(
                storage,
                SLOT_VERSION,
                version_value,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to initialize version in BaseContract storage")?;

        // Initialize governance_hook_enabled (slot 2) - default false
        let governance_hook_value = Self::bool_to_storage_value(false);
        contract_storage
            .sstore_reserved(
                storage,
                SLOT_GOVERNANCE_HOOK_ENABLED,
                governance_hook_value,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| {
                "Failed to initialize governance_hook_enabled in BaseContract storage"
            })?;

        // Initialize fee_hook_enabled (slot 3) - default false
        let fee_hook_value = Self::bool_to_storage_value(false);
        contract_storage
            .sstore_reserved(
                storage,
                SLOT_FEE_HOOK_ENABLED,
                fee_hook_value,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to initialize fee_hook_enabled in BaseContract storage")?;

        // Initialize paused (slot 4) - default false
        let paused_value = Self::bool_to_storage_value(false);
        contract_storage
            .sstore_reserved(storage, SLOT_PAUSED, paused_value, gas_meter.as_deref_mut())
            .with_context(|| "Failed to initialize paused in BaseContract storage")?;

        Ok(())
    }

    /// Reads the owner address from storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Owner address (32 bytes) or error
    pub fn get_owner(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<[u8; 32]> {
        let value = contract_storage
            .sload(storage, SLOT_OWNER, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read owner from BaseContract storage")?;

        if value.len() != 32 {
            anyhow::bail!(
                "Invalid owner value length: expected 32, got {}",
                value.len()
            );
        }

        let mut owner = [0u8; 32];
        owner.copy_from_slice(&value);
        Ok(owner)
    }

    /// Writes the owner address to storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `owner_address` - New owner address (must be 32 bytes)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Error if owner_address is not 32 bytes or other errors
    pub fn set_owner(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner_address: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if owner_address.len() != 32 {
            anyhow::bail!(
                "owner_address must be exactly 32 bytes, got {}",
                owner_address.len()
            );
        }

        let mut owner_value = vec![0u8; 32];
        owner_value.copy_from_slice(owner_address);
        contract_storage
            .sstore_reserved(storage, SLOT_OWNER, owner_value, gas_meter.as_deref_mut())
            .with_context(|| "Failed to write owner to BaseContract storage")?;

        Ok(())
    }

    /// Reads the version from storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Version (u64) or error
    pub fn get_version(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        let value = contract_storage
            .sload(storage, SLOT_VERSION, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read version from BaseContract storage")?;

        Self::storage_value_to_u64(&value)
    }

    /// Writes the version to storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `version` - New version (u64)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Error if the write fails
    pub fn set_version(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        version: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let version_value = Self::u64_to_storage_value(version);
        contract_storage
            .sstore_reserved(
                storage,
                SLOT_VERSION,
                version_value,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write version to BaseContract storage")?;

        Ok(())
    }

    /// Reads the paused state from storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `true` if the contract is paused, `false` otherwise
    pub fn is_paused(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        let value = contract_storage
            .sload(storage, SLOT_PAUSED, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read paused from BaseContract storage")?;

        Self::storage_value_to_bool(&value)
    }

    /// Sets the paused state in storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `paused` - New paused state (bool)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Error if the write fails
    pub fn set_paused(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        paused: bool,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let paused_value = Self::bool_to_storage_value(paused);
        contract_storage
            .sstore_reserved(storage, SLOT_PAUSED, paused_value, gas_meter.as_deref_mut())
            .with_context(|| "Failed to write paused to BaseContract storage")?;

        Ok(())
    }

    /// Reads the governance_hook_enabled state from storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `true` if the governance hook is enabled, `false` otherwise
    pub fn is_governance_hook_enabled(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        let value = contract_storage
            .sload(
                storage,
                SLOT_GOVERNANCE_HOOK_ENABLED,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to read governance_hook_enabled from BaseContract storage")?;

        Self::storage_value_to_bool(&value)
    }

    /// Sets the governance_hook_enabled state in storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `enabled` - New enabled state (bool)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Error if the write fails
    pub fn set_governance_hook_enabled(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        enabled: bool,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let enabled_value = Self::bool_to_storage_value(enabled);
        contract_storage
            .sstore_reserved(
                storage,
                SLOT_GOVERNANCE_HOOK_ENABLED,
                enabled_value,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write governance_hook_enabled to BaseContract storage")?;

        Ok(())
    }

    /// Reads the fee_hook_enabled state from storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `true` if the fee hook is enabled, `false` otherwise
    pub fn is_fee_hook_enabled(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        let value = contract_storage
            .sload(storage, SLOT_FEE_HOOK_ENABLED, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read fee_hook_enabled from BaseContract storage")?;

        Self::storage_value_to_bool(&value)
    }

    /// Sets the fee_hook_enabled state in storage
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `enabled` - New enabled state (bool)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Error if the write fails
    pub fn set_fee_hook_enabled(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        enabled: bool,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let enabled_value = Self::bool_to_storage_value(enabled);
        contract_storage
            .sstore_reserved(
                storage,
                SLOT_FEE_HOOK_ENABLED,
                enabled_value,
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write fee_hook_enabled to BaseContract storage")?;

        Ok(())
    }

    // ============================================
    // Helper functions for value conversion
    // ============================================

    /// Converts a u64 to a storage value (32 bytes, little-endian)
    fn u64_to_storage_value(value: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[0..8].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    /// Converts a storage value (32 bytes) to u64 (little-endian)
    fn storage_value_to_u64(value: &[u8]) -> Result<u64> {
        if value.len() < 8 {
            anyhow::bail!(
                "Invalid storage value length for u64: expected at least 8 bytes, got {}",
                value.len()
            );
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&value[0..8]);
        Ok(u64::from_le_bytes(bytes))
    }

    /// Converts a bool to a storage value (32 bytes)
    /// true = [1, 0, 0, ...], false = [0, 0, 0, ...]
    fn bool_to_storage_value(value: bool) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        if value {
            bytes[0] = 1;
        }
        bytes
    }

    /// Converts a storage value (32 bytes) to bool
    /// true if the first byte is non-zero, false otherwise
    fn storage_value_to_bool(value: &[u8]) -> Result<bool> {
        if value.is_empty() {
            return Ok(false);
        }
        Ok(value[0] != 0)
    }

    // ============================================
    // Mandatory public BaseContract functions
    // ============================================

    /// Returns the contract owner
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Owner address (32 bytes) or error
    pub fn owner(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<[u8; 32]> {
        Self::get_owner(contract_storage, storage, gas_meter)
    }

    /// Transfers contract ownership to a new owner
    ///
    /// # Requirements
    /// - The caller must be the current owner (onlyOwner)
    /// - The new owner must not be address zero
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current caller
    /// * `new_owner` - New owner address (must be 32 bytes)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the transfer succeeded, error otherwise
    pub fn transfer_ownership(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        new_owner: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Validation: new owner must be 32 bytes
        if new_owner.len() != 32 {
            anyhow::bail!(
                "new_owner must be exactly 32 bytes, got {}",
                new_owner.len()
            );
        }

        // Validation: new owner must not be address zero
        if new_owner.iter().all(|&b| b == 0) {
            anyhow::bail!("new_owner cannot be address zero");
        }

        // Get current caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Get current owner
        let current_owner = Self::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read current owner")?;

        // Validation: caller must be current owner (onlyOwner)
        if caller != current_owner {
            anyhow::bail!(
                "Only owner can transfer ownership. Current owner: {}, caller: {}",
                hex::encode(current_owner),
                hex::encode(caller)
            );
        }

        // Validation: new owner must be different from current owner
        if new_owner == current_owner.as_slice() {
            anyhow::bail!("new_owner must be different from current owner");
        }

        // Transfer ownership
        Self::set_owner(
            contract_storage,
            storage,
            new_owner,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to transfer ownership")?;

        // Emit OwnershipTransferred event
        // Use EventSystem from runtime if available, otherwise create new one
        let event_system = runtime.event_system();
        event_system.emit_standard_event(
            StandardEvent::OwnershipTransferred {
                previous_owner: hex::encode(current_owner),
                new_owner: hex::encode(new_owner),
            },
            gas_meter.as_deref_mut(),
        );

        Ok(true)
    }

    /// Returns the contract version
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// Version (u64) or error
    pub fn version(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        Self::get_version(contract_storage, storage, gas_meter)
    }

    /// Upgrades the contract
    ///
    /// # Requirements
    /// - Must be controlled by governance (valid proposal_id)
    /// - Contract must not be in paused state
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current contract
    /// * `new_code_hash` - Hash of new bytecode (64 bytes)
    /// * `proposal_id` - ID of governance proposal authorizing upgrade (32 bytes)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if upgrade succeeded, error otherwise
    ///
    /// # Note
    /// Complete upgrade implementation requires integration with governance system
    /// and contract upgrade system. This is a basic implementation.
    pub fn upgrade(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        new_code_hash: &[u8],
        proposal_id: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Validation: new_code_hash must be 64 bytes (or 32 bytes for simplicity)
        if new_code_hash.len() != 32 && new_code_hash.len() != 64 {
            anyhow::bail!(
                "new_code_hash must be 32 or 64 bytes, got {}",
                new_code_hash.len()
            );
        }

        if proposal_id.len() != 32 {
            anyhow::bail!(
                "proposal_id must be exactly 32 bytes, got {}",
                proposal_id.len()
            );
        }

        // Validation: contract must not be paused
        if Self::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Cannot upgrade contract while paused");
        }

        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Validate that governance proposal is approved
        // Convert proposal_id from bytes (32 bytes) to u64 (little-endian)
        if proposal_id.len() < 8 {
            anyhow::bail!("proposal_id must be at least 8 bytes to convert to u64");
        }
        let proposal_id_u64 = u64::from_le_bytes(
            proposal_id[0..8]
                .try_into()
                .map_err(|_| anyhow::anyhow!("Failed to convert proposal_id to u64"))?,
        );

        // Verify that proposal_id is not zero
        if proposal_id_u64 == 0 {
            anyhow::bail!("proposal_id cannot be zero");
        }

        // Validate the governance proposal using UpgradeSystem
        use crate::contracts::upgrade::UpgradeSystem;
        let upgrade_system = UpgradeSystem::new();

        // Convert new_code_hash from bytes to [u8; 32]
        let new_code_hash_array: [u8; 32] = if new_code_hash.len() == 32 {
            new_code_hash
                .try_into()
                .map_err(|_| anyhow::anyhow!("Failed to convert new_code_hash to [u8; 32]"))?
        } else if new_code_hash.len() == 64 {
            // If it's 64 bytes, take the first 32 (or the last 32, depends on convention)
            // For now we assume it's the first 32 bytes
            new_code_hash[0..32]
                .try_into()
                .map_err(|_| anyhow::anyhow!("Failed to convert new_code_hash to [u8; 32]"))?
        } else {
            anyhow::bail!(
                "new_code_hash must be 32 or 64 bytes, got {}",
                new_code_hash.len()
            );
        };

        // Get current timestamp from runtime (deterministic)
        let current_timestamp = runtime.block_timestamp();

        upgrade_system
            .validate_governance_proposal(
                storage,
                proposal_id_u64,
                &contract_address,
                &new_code_hash_array,
                current_timestamp,
            )
            .with_context(|| {
                format!("Governance proposal {} validation failed", proposal_id_u64)
            })?;

        // Get current version
        let current_version =
            Self::get_version(contract_storage, storage, gas_meter.as_deref_mut())?;
        let new_version = current_version
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Version overflow"))?;

        // Update the version
        Self::set_version(
            contract_storage,
            storage,
            new_version,
            gas_meter.as_deref_mut(),
        )
        .with_context(|| "Failed to update version during upgrade")?;

        // Emit Upgraded event
        // Use EventSystem from runtime if available
        let event_system = runtime.event_system();
        event_system.emit_standard_event(
            StandardEvent::Upgraded {
                contract_address: hex::encode(contract_address),
                new_version,
            },
            gas_meter.as_deref_mut(),
        );

        // Integrate with upgrade system to update bytecode
        // The upgrade system handles bytecode update while preserving storage
        let upgrade_system = UpgradeSystem::new();

        // Get the current contract info to extract the bytecode
        let contract_info = storage
            .get_contract(&contract_address)
            .with_context(|| "Failed to get contract info for upgrade")?
            .ok_or_else(|| anyhow::anyhow!("Contract not found for upgrade"))
            .and_then(|raw| {
                const MAX_CONTRACT_INFO_SIZE: usize = 4 * 1024 * 1024;
                if raw.len() > MAX_CONTRACT_INFO_SIZE {
                    return Err(anyhow::anyhow!(
                        "Contract info data too large: {} bytes (max {})",
                        raw.len(),
                        MAX_CONTRACT_INFO_SIZE
                    ));
                }
                bincode::deserialize::<ContractInfo>(&raw)
                    .map_err(|e| anyhow::anyhow!("Failed to decode contract info: {}", e))
            })?;

        // Execute the actual upgrade through the upgrade system
        // This updates the bytecode while preserving all storage (slots 0-99 BaseContract + custom slots)
        upgrade_system
            .upgrade_contract_governance_controlled(
                storage,
                &hex::encode(contract_address),
                contract_info.code.clone(), // Current bytecode will be updated by upgrade system
                proposal_id_u64,
                current_timestamp,
                gas_meter,
            )
            .with_context(|| {
                format!(
                    "Failed to execute contract upgrade through governance proposal {}",
                    proposal_id_u64
                )
            })?;

        Ok(true)
    }

    /// Pauses the contract
    ///
    /// # Requirements
    /// - The caller must be the current owner (onlyOwner)
    /// - The contract must not already be paused
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current caller
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the contract was paused, error otherwise
    pub fn pause(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Get current caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Get current owner
        let owner = Self::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read owner")?;

        // Validation: caller must be current owner (onlyOwner)
        if caller != owner {
            anyhow::bail!(
                "Only owner can pause contract. Owner: {}, caller: {}",
                hex::encode(owner),
                hex::encode(caller)
            );
        }

        // Validation: contract must not already be paused
        if Self::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is already paused");
        }

        // Pause the contract
        Self::set_paused(contract_storage, storage, true, gas_meter.as_deref_mut())
            .with_context(|| "Failed to pause contract")?;

        // Emit Paused event
        // Use EventSystem from runtime if available
        let event_system = runtime.event_system();
        event_system.emit_standard_event(
            StandardEvent::Paused {
                account: hex::encode(caller),
            },
            gas_meter.as_deref_mut(),
        );

        Ok(true)
    }

    /// Unpauses the contract
    ///
    /// # Requirements
    /// - The caller must be the current owner (onlyOwner)
    /// - The contract must be paused
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current caller
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the contract was unpaused, error otherwise
    pub fn unpause(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Get current caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Get current owner
        let owner = Self::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read owner")?;

        // Validation: caller must be current owner (onlyOwner)
        if caller != owner {
            anyhow::bail!(
                "Only owner can unpause contract. Owner: {}, caller: {}",
                hex::encode(owner),
                hex::encode(caller)
            );
        }

        // Validation: contract must be paused
        if !Self::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is not paused");
        }

        // Unpause the contract
        Self::set_paused(contract_storage, storage, false, gas_meter.as_deref_mut())
            .with_context(|| "Failed to unpause contract")?;

        // Emit Unpaused event
        // Use EventSystem from runtime if available
        let event_system = runtime.event_system();
        event_system.emit_standard_event(
            StandardEvent::Unpaused {
                account: hex::encode(caller),
            },
            gas_meter.as_deref_mut(),
        );

        Ok(true)
    }

    // ============================================
    // Governance Hooks
    // ============================================

    /// Enables or disables the governance hook
    ///
    /// # Requirements
    /// - The caller must be the current owner (onlyOwner)
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current caller
    /// * `enabled` - `true` to enable the hook, `false` to disable it
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the operation succeeded, error otherwise
    pub fn set_governance_hook(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        enabled: bool,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Get current caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Get current owner
        let owner = Self::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read owner")?;

        // Validation: caller must be current owner (onlyOwner)
        if caller != owner {
            anyhow::bail!(
                "Only owner can set governance hook. Owner: {}, caller: {}",
                hex::encode(owner),
                hex::encode(caller)
            );
        }

        // Set the governance_hook_enabled state
        Self::set_governance_hook_enabled(contract_storage, storage, enabled, gas_meter)
            .with_context(|| "Failed to set governance hook enabled")?;

        Ok(true)
    }

    /// Hook called when a governance proposal is approved
    ///
    /// This function is automatically called by the governance system
    /// when a proposal is approved. Contracts can implement
    /// custom logic to react to governance proposals.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current contract
    /// * `proposal_id` - ID of the governance proposal (32 bytes)
    /// * `action` - Action of the governance proposal
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the hook was processed successfully, error otherwise
    ///
    /// # Note
    /// - The hook is only called if `governance_hook_enabled` is `true`
    /// - If the hook reverts, the entire proposal execution reverts
    /// - Contracts can implement custom logic to react to proposals
    pub fn on_governance_proposal(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        proposal_id: &[u8],
        action: &ProposalAction,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        if proposal_id.len() != 32 {
            anyhow::bail!(
                "proposal_id must be exactly 32 bytes, got {}",
                proposal_id.len()
            );
        }

        // Check if the governance hook is enabled
        let hook_enabled =
            Self::is_governance_hook_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to read governance_hook_enabled")?;

        // If the hook is not enabled, return success without doing anything
        // (the contract is not interested in governance proposals)
        if !hook_enabled {
            return Ok(true);
        }

        // Get current contract
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Emit GovernanceHookTriggered event
        // Use EventSystem from runtime if available
        let event_system = runtime.event_system();
        let action_type = match action {
            ProposalAction::FeeVariation { .. } => "FeeVariation",
            ProposalAction::ProjectSelection { .. } => "ProjectSelection",
            ProposalAction::Standards { .. } => "Standards",
            ProposalAction::NonCore { .. } => "NonCore",
            ProposalAction::ContractUpgrade { .. } => "ContractUpgrade",
            ProposalAction::SlashingParamsUpdate { .. } => "SlashingParamsUpdate",
            ProposalAction::SetFlPolicy { .. } => "SetFlPolicy",
            ProposalAction::ApproveFlModel { .. } => "ApproveFlModel",
            ProposalAction::AbortFlRound { .. } => "AbortFlRound",
            ProposalAction::AddConnector { .. } => "AddConnector",
            ProposalAction::RemoveConnector { .. } => "RemoveConnector",
        };
        event_system.emit_standard_event(
            StandardEvent::GovernanceHookTriggered {
                contract_address: hex::encode(contract_address),
                proposal_id: hex::encode(proposal_id),
                action_type: action_type.to_string(),
            },
            gas_meter,
        );

        // Nota: I contratti possono implementare logica custom qui
        // Per ora, l'hook base emette solo l'evento

        Ok(true)
    }

    /// Automatically calls the governance hook for a contract
    ///
    /// This function is called by the governance system when a proposal
    /// is approved. It checks if the contract has the governance hook enabled
    /// and calls `on_governance_proposal` if necessary.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current contract
    /// * `proposal_id` - ID of the governance proposal (32 bytes)
    /// * `action` - Action of the governance proposal
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the hook was called successfully or was not needed, error otherwise
    pub fn trigger_governance_hook(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        proposal_id: &[u8],
        action: &ProposalAction,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Check if the governance hook is enabled
        let hook_enabled =
            Self::is_governance_hook_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to read governance_hook_enabled")?;

        // If the hook is not enabled, return success without doing anything
        if !hook_enabled {
            return Ok(true);
        }

        // Call the hook
        Self::on_governance_proposal(
            contract_storage,
            storage,
            runtime,
            proposal_id,
            action,
            gas_meter,
        )
    }

    // ============================================
    // Fee Hooks
    // ============================================

    /// Enables or disables the fee hook
    ///
    /// # Requirements
    /// - The caller must be the current owner (onlyOwner)
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current caller
    /// * `enabled` - `true` to enable the hook, `false` to disable it
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the operation succeeded, error otherwise
    pub fn set_fee_hook(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        enabled: bool,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Get current caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Get current owner
        let owner = Self::get_owner(contract_storage, storage, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read owner")?;

        // Validation: caller must be current owner (onlyOwner)
        if caller != owner {
            anyhow::bail!(
                "Only owner can set fee hook. Owner: {}, caller: {}",
                hex::encode(owner),
                hex::encode(caller)
            );
        }

        // Set the fee_hook_enabled state
        Self::set_fee_hook_enabled(contract_storage, storage, enabled, gas_meter)
            .with_context(|| "Failed to set fee hook enabled")?;

        Ok(true)
    }

    /// Hook called when a fee is paid for a contract call
    ///
    /// This function is automatically called by the fee system
    /// when a fee is paid for a contract call. Contracts
    /// can implement custom logic to react to paid fees.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current contract and caller
    /// * `amount` - Amount of the fee paid (u128)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the hook was processed successfully, error otherwise
    ///
    /// # Note
    /// - The hook is only called if `fee_hook_enabled` is `true`
    /// - The hook is called AFTER the fee deduction from the caller, BEFORE function execution
    /// - If the hook reverts, the entire call reverts
    /// - Contracts can implement custom logic to react to fees (e.g. tracking, reward distribution, fee sharing)
    pub fn on_fee_paid(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        amount: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Check if the fee hook is enabled
        let hook_enabled =
            Self::is_fee_hook_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to read fee_hook_enabled")?;

        // If the hook is not enabled, return success without doing anything
        // (the contract is not interested in paid fees)
        if !hook_enabled {
            return Ok(true);
        }

        // Get current caller from runtime
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Get current contract
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Emit FeeHookTriggered event
        // Use EventSystem from runtime if available
        let event_system = runtime.event_system();
        event_system.emit_standard_event(
            StandardEvent::FeeHookTriggered {
                contract_address: hex::encode(contract_address),
                caller: hex::encode(caller),
                amount,
            },
            gas_meter,
        );

        // Note: Contracts can implement custom logic here
        // For now, the base hook only emits the event
        // Derived contracts can override this function to add custom logic
        // Examples of custom logic:
        // - Tracking of paid fees
        // - Reward distribution based on fees
        // - Fee sharing with other contracts

        Ok(true)
    }

    /// Automatically calls the fee hook for a contract
    ///
    /// This function is called by the fee system when a fee is paid
    /// for a contract call. It checks if the contract has the
    /// fee hook enabled and calls `on_fee_paid` if necessary.
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer to access RocksDB
    /// * `runtime` - Runtime to get current contract and caller
    /// * `amount` - Amount of the fee paid (u128)
    /// * `gas_meter` - Optional gas meter to consume gas
    ///
    /// # Returns
    /// `Ok(true)` if the hook was called successfully or was not needed, error otherwise
    ///
    /// # Note
    /// This function must be called AFTER the fee deduction from the caller
    /// and BEFORE the contract function execution. If the hook reverts,
    /// the entire call must revert.
    pub fn trigger_fee_hook(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        amount: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        // Check if the fee hook is enabled
        let hook_enabled =
            Self::is_fee_hook_enabled(contract_storage, storage, gas_meter.as_deref_mut())
                .with_context(|| "Failed to read fee_hook_enabled")?;

        // If the hook is not enabled, return success without doing anything
        if !hook_enabled {
            return Ok(true);
        }

        // Call the hook
        Self::on_fee_paid(contract_storage, storage, runtime, amount, gas_meter)
    }
}
