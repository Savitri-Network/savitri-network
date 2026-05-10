//! SAVITRI-20: Fungible Token Standard
//!
//! Implementation of SAVITRI-20 standard (similar to ERC20):
//! - totalSupply(), balanceOf(address)
//! - transfer(to, amount), approve(spender, amount)
//! - transferFrom(from, to, amount), allowance(owner, spender)
//! - mint/burn (optional)
//!
//! # Storage Layout
//! - Slot 0-99: BaseContract (reserved)
//! - Slot 100+: SAVITRI-20 specific storage with mapping for balances and allowances

use crate::contracts::base::BaseContract;
use crate::contracts::events::{CustomEvent, EventSystem};
use crate::contracts::gas::GasMeter;
use crate::contracts::runtime::Runtime;
use crate::contracts::storage::ContractStorage;
use crate::storage::Storage;
use anyhow::{Context, Result};
use hex;
use sha3::{Digest, Keccak256};

/// Slot for total_supply
const SLOT_TOTAL_SUPPLY: u64 = 100;

/// Base slot for balances mapping
const SLOT_BALANCES_BASE: u64 = 101;

/// Base slot for allowances mapping
const SLOT_ALLOWANCES_BASE: u64 = 200;

/// Base slot for token name string storage
const SLOT_NAME_BASE: u64 = 300;

/// Base slot for token symbol string storage
const SLOT_SYMBOL_BASE: u64 = 400;

/// SAVITRI-20 Contract
///
/// Implements the fungible token standard.
/// All contracts must extend BaseContract (slots 0-99 reserved).
pub struct SAVITRI20;

impl SAVITRI20 {
    fn ensure_non_zero_address(address: &[u8; 32], message: &str) -> Result<()> {
        if *address == [0u8; 32] {
            anyhow::bail!(message.to_string());
        }
        Ok(())
    }

    /// Initializes token metadata and initial supply during deployment.
    pub fn initialize(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &[u8; 32],
        name: &str,
        symbol: &str,
        initial_supply: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        Self::set_name(contract_storage, storage, name, gas_meter.as_deref_mut())
            .with_context(|| "Failed to initialize token name")?;
        Self::set_symbol(contract_storage, storage, symbol, gas_meter.as_deref_mut())
            .with_context(|| "Failed to initialize token symbol")?;

        contract_storage
            .sstore(
                storage,
                SLOT_TOTAL_SUPPLY,
                Self::u128_to_storage_value(initial_supply),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to initialize total_supply")?;

        if initial_supply > 0 {
            let owner_slot = Self::calculate_balance_slot(owner);
            contract_storage
                .sstore(
                    storage,
                    owner_slot,
                    Self::u128_to_storage_value(initial_supply),
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| "Failed to initialize owner balance")?;
        }

        Ok(())
    }

    /// Calculates the slot for a balance of an address
    ///
    /// Formula: slot = keccak256(address || slot_base)
    /// Where address is 32 bytes and slot_base is 101 (SLOT_BALANCES_BASE)
    fn calculate_balance_slot(address: &[u8; 32]) -> u64 {
        let mut hasher = Keccak256::new();
        hasher.update(address);
        hasher.update(&SLOT_BALANCES_BASE.to_le_bytes());
        let hash = hasher.finalize();

        // Take first 8 bytes of hash as u64
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        u64::from_le_bytes(slot_bytes)
    }

    /// Calculates the slot for an allowance of owner -> spender
    ///
    /// Formula: slot = keccak256(spender || keccak256(owner || slot_base))
    /// Where owner and spender are 32 bytes and slot_base is 200 (SLOT_ALLOWANCES_BASE)
    fn calculate_allowance_slot(owner: &[u8; 32], spender: &[u8; 32]) -> u64 {
        // First hash: keccak256(owner || slot_base)
        let mut hasher1 = Keccak256::new();
        hasher1.update(owner);
        hasher1.update(&SLOT_ALLOWANCES_BASE.to_le_bytes());
        let hash1 = hasher1.finalize();

        // Second hash: keccak256(spender || hash1)
        let mut hasher2 = Keccak256::new();
        hasher2.update(spender);
        hasher2.update(&hash1);
        let hash2 = hasher2.finalize();

        // Take first 8 bytes of hash as u64
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash2[0..8]);
        u64::from_le_bytes(slot_bytes)
    }

    /// Converts u128 to storage value (32 bytes, little-endian)
    fn u128_to_storage_value(value: u128) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[0..16].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    /// Converts storage value (32 bytes) to u128
    fn storage_value_to_u128(value: &[u8]) -> Result<u128> {
        if value.len() < 16 {
            anyhow::bail!("Storage value too short for u128");
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&value[0..16]);
        Ok(u128::from_le_bytes(bytes))
    }

    fn write_string_to_storage(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        base_slot: u64,
        value: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let bytes = value.as_bytes();
        let len = bytes.len() as u64;

        let mut len_storage = vec![0u8; 32];
        len_storage[..8].copy_from_slice(&len.to_le_bytes());
        contract_storage
            .sstore(storage, base_slot, len_storage, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to store string length at slot {}", base_slot))?;

        if bytes.is_empty() {
            return Ok(());
        }

        for (index, chunk) in bytes.chunks(32).enumerate() {
            let mut slot_value = vec![0u8; 32];
            slot_value[..chunk.len()].copy_from_slice(chunk);
            contract_storage
                .sstore(
                    storage,
                    base_slot + 1 + index as u64,
                    slot_value,
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| {
                    format!(
                        "Failed to store string chunk {} at slot {}",
                        index,
                        base_slot + 1 + index as u64
                    )
                })?;
        }

        Ok(())
    }

    fn read_string_from_storage(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        base_slot: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        let len_value = contract_storage
            .sload(storage, base_slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to load string length at slot {}", base_slot))?;

        if len_value.len() < 8 {
            anyhow::bail!("Invalid string length storage at slot {}", base_slot);
        }

        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&len_value[..8]);
        let len = u64::from_le_bytes(len_bytes) as usize;

        if len == 0 {
            return Ok(String::new());
        }

        let mut bytes = Vec::with_capacity(len);
        let chunks = len.div_ceil(32);
        for index in 0..chunks {
            let chunk = contract_storage
                .sload(
                    storage,
                    base_slot + 1 + index as u64,
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| {
                    format!(
                        "Failed to load string chunk {} from slot {}",
                        index,
                        base_slot + 1 + index as u64
                    )
                })?;
            bytes.extend_from_slice(&chunk);
        }

        bytes.truncate(len);
        String::from_utf8(bytes).with_context(|| "Stored string is not valid UTF-8")
    }

    fn set_name(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        name: &str,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        Self::write_string_to_storage(contract_storage, storage, SLOT_NAME_BASE, name, gas_meter)
            .with_context(|| "Failed to store token name")
    }

    pub fn name(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        Self::read_string_from_storage(contract_storage, storage, SLOT_NAME_BASE, gas_meter)
            .with_context(|| "Failed to read token name")
    }

    fn set_symbol(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        symbol: &str,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        Self::write_string_to_storage(
            contract_storage,
            storage,
            SLOT_SYMBOL_BASE,
            symbol,
            gas_meter,
        )
        .with_context(|| "Failed to store token symbol")
    }

    pub fn symbol(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        Self::read_string_from_storage(contract_storage, storage, SLOT_SYMBOL_BASE, gas_meter)
            .with_context(|| "Failed to read token symbol")
    }

    /// Decodifica address da stringa hex a bytes (32 bytes)
    fn decode_address(address_str: &str) -> Result<[u8; 32]> {
        let address_hex = address_str.strip_prefix("0x").unwrap_or(address_str);
        let address_bytes = hex::decode(address_hex).with_context(|| "Failed to decode address")?;

        if address_bytes.len() != 32 {
            anyhow::bail!("Address must be 32 bytes, got {}", address_bytes.len());
        }

        let mut address = [0u8; 32];
        address.copy_from_slice(&address_bytes);
        Ok(address)
    }

    /// Emette evento Transfer
    fn emit_transfer_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        from: &[u8; 32],
        to: &[u8; 32],
        amount: u128,
        gas_meter: Option<&mut GasMeter>,
    ) {
        // Topic 0: keccak256("Transfer(address,address,uint256)")
        let transfer_signature = b"Transfer(address,address,uint256)";
        let mut hasher = Keccak256::new();
        hasher.update(transfer_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        // Topic 1: from (padded to 32 bytes)
        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(from);

        // Topic 2: to (padded to 32 bytes)
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(to);

        // Data: amount (u128 encoded as 32 bytes)
        let mut data = vec![0u8; 32];
        data[0..16].copy_from_slice(&amount.to_le_bytes());

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "Transfer".to_string(),
            topics: vec![topic0_bytes, topic1, topic2],
            data,
        };

        event_system.emit_custom_event(event, gas_meter);
    }

    /// Emette evento Approval
    fn emit_approval_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        owner: &[u8; 32],
        spender: &[u8; 32],
        amount: u128,
        gas_meter: Option<&mut GasMeter>,
    ) {
        // Topic 0: keccak256("Approval(address,address,uint256)")
        let approval_signature = b"Approval(address,address,uint256)";
        let mut hasher = Keccak256::new();
        hasher.update(approval_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        // Topic 1: owner (padded to 32 bytes)
        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(owner);

        // Topic 2: spender (padded to 32 bytes)
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(spender);

        // Data: amount (u128 encoded as 32 bytes)
        let mut data = vec![0u8; 32];
        data[0..16].copy_from_slice(&amount.to_le_bytes());

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "Approval".to_string(),
            topics: vec![topic0_bytes, topic1, topic2],
            data,
        };

        event_system.emit_custom_event(event, gas_meter);
    }

    /// Ottiene la supply totale
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Supply totale (u128) o errore
    pub fn total_supply(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u128> {
        let value = contract_storage
            .sload(storage, SLOT_TOTAL_SUPPLY, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read total_supply from storage")?;
        Self::storage_value_to_u128(&value)
    }

    /// Ottiene il balance di un address
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `address` - Address di cui leggere il balance (stringa hex, 32 bytes)
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Balance (u128) o errore
    pub fn balance_of(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        address: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u128> {
        let address_bytes = Self::decode_address(address)?;
        let slot = Self::calculate_balance_slot(&address_bytes);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .with_context(|| format!("Failed to read balance for address {}", address))?;
        Self::storage_value_to_u128(&value)
    }

    /// Transfers token da sender a receiver
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per ottenere caller e contract address
    /// * `amount` - Amount da trasferire
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    pub fn transfer(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        to: &str,
        amount: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        // Ottieni il caller (sender)
        let from = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Ottieni il contract address
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Decodifica address receiver
        let to_bytes = Self::decode_address(to)?;
        Self::ensure_non_zero_address(&to_bytes, "Cannot transfer to zero address")?;

        // Leggi balance sender
        let from_slot = Self::calculate_balance_slot(&from);
        let from_balance_value = contract_storage
            .sload(storage, from_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read sender balance")?;
        let from_balance = Self::storage_value_to_u128(&from_balance_value)?;

        // Check balance sufficiente
        if from_balance < amount {
            anyhow::bail!(
                "Insufficient balance: have {}, need {}",
                from_balance,
                amount
            );
        }

        // Transfer verso se stessi: evitare doppia scrittura on the stesso slot (from_slot == to_slot).
        if from == to_bytes {
            let event_system = runtime.event_system();
            Self::emit_transfer_event(
                &event_system,
                &contract_address,
                &from,
                &to_bytes,
                amount,
                gas_meter,
            );
            return Ok(());
        }

        // Compute nuovo balance sender (checked arithmetic)
        let new_from_balance = from_balance
            .checked_sub(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance underflow"))?;

        // Leggi balance receiver
        let to_slot = Self::calculate_balance_slot(&to_bytes);
        let to_balance_value = contract_storage
            .sload(storage, to_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read receiver balance")?;
        let to_balance = Self::storage_value_to_u128(&to_balance_value)?;

        // Compute nuovo balance receiver (checked arithmetic)
        let new_to_balance = to_balance
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;

        // Scrivi nuovi balance
        contract_storage
            .sstore(
                storage,
                from_slot,
                Self::u128_to_storage_value(new_from_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update sender balance")?;
        contract_storage
            .sstore(
                storage,
                to_slot,
                Self::u128_to_storage_value(new_to_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update receiver balance")?;

        // Emetti evento Transfer
        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &from,
            &to_bytes,
            amount,
            gas_meter,
        );

        Ok(())
    }

    /// Approva uno spender per spendere token
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per ottenere caller e contract address
    /// * `amount` - Amount da approvare
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Errore se l'approval fallisce
    pub fn approve(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        spender: &str,
        amount: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        // Ottieni il caller (owner)
        let owner = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Ottieni il contract address
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Decodifica address spender
        let spender_bytes = Self::decode_address(spender)?;
        Self::ensure_non_zero_address(&spender_bytes, "Cannot approve zero address")?;

        // Compute slot allowance
        let allowance_slot = Self::calculate_allowance_slot(&owner, &spender_bytes);

        // Scrivi allowance
        contract_storage
            .sstore(
                storage,
                allowance_slot,
                Self::u128_to_storage_value(amount),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write allowance")?;

        // Emetti evento Approval
        let event_system = runtime.event_system();
        Self::emit_approval_event(
            &event_system,
            &contract_address,
            &owner,
            &spender_bytes,
            amount,
            gas_meter,
        );

        Ok(())
    }

    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per ottenere caller e contract address
    /// * `amount` - Amount da trasferire
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    pub fn transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        amount: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        // Ottieni il caller (spender)
        let spender = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        // Decodifica address
        let from_bytes = Self::decode_address(from)?;
        let to_bytes = Self::decode_address(to)?;
        Self::ensure_non_zero_address(&from_bytes, "Cannot transfer from zero address")?;
        Self::ensure_non_zero_address(&to_bytes, "Cannot transfer to zero address")?;

        // Leggi allowance
        let allowance_slot = Self::calculate_allowance_slot(&from_bytes, &spender);
        let allowance_value = contract_storage
            .sload(storage, allowance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read allowance")?;
        let allowance = Self::storage_value_to_u128(&allowance_value)?;

        // Check allowance sufficiente
        if allowance < amount {
            anyhow::bail!(
                "Insufficient allowance: have {}, need {}",
                allowance,
                amount
            );
        }

        // Sottrai allowance (checked arithmetic)
        let new_allowance = allowance
            .checked_sub(amount)
            .ok_or_else(|| anyhow::anyhow!("Allowance underflow"))?;

        contract_storage
            .sstore(
                storage,
                allowance_slot,
                Self::u128_to_storage_value(new_allowance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update allowance")?;

        // Esegui transfer (riusa logica transfer)
        // Leggi balance sender
        let from_slot = Self::calculate_balance_slot(&from_bytes);
        let from_balance_value = contract_storage
            .sload(storage, from_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read sender balance")?;
        let from_balance = Self::storage_value_to_u128(&from_balance_value)?;

        // Check balance sufficiente
        if from_balance < amount {
            anyhow::bail!(
                "Insufficient balance: have {}, need {}",
                from_balance,
                amount
            );
        }

        // Compute nuovo balance sender
        let new_from_balance = from_balance
            .checked_sub(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance underflow"))?;

        // Leggi balance receiver
        let to_slot = Self::calculate_balance_slot(&to_bytes);
        let to_balance_value = contract_storage
            .sload(storage, to_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read receiver balance")?;
        let to_balance = Self::storage_value_to_u128(&to_balance_value)?;

        // Compute nuovo balance receiver
        let new_to_balance = to_balance
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;

        // Scrivi nuovi balance
        contract_storage
            .sstore(
                storage,
                from_slot,
                Self::u128_to_storage_value(new_from_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update sender balance")?;
        contract_storage
            .sstore(
                storage,
                to_slot,
                Self::u128_to_storage_value(new_to_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update receiver balance")?;

        // Emetti evento Transfer
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;
        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &from_bytes,
            &to_bytes,
            amount,
            gas_meter,
        );

        Ok(())
    }

    /// Ottiene l'allowance di owner -> spender
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Allowance (u128) o errore
    pub fn allowance(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        spender: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u128> {
        let owner_bytes = Self::decode_address(owner)?;
        let spender_bytes = Self::decode_address(spender)?;
        let slot = Self::calculate_allowance_slot(&owner_bytes, &spender_bytes);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .with_context(|| {
                format!(
                    "Failed to read allowance for owner {} and spender {}",
                    owner, spender
                )
            })?;
        Self::storage_value_to_u128(&value)
    }

    /// Mint token (opzionale)
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per ottenere caller e contract address
    /// * `to` - Address a cui mintare (stringa hex, 32 bytes)
    /// * `amount` - Amount da mintare
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    pub fn mint(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        to: &str,
        amount: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        // Check permessi: solo owner può mintare
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;

        if caller != owner {
            anyhow::bail!("Only owner can mint tokens");
        }

        // Ottieni il contract address
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Decodifica address receiver
        let to_bytes = Self::decode_address(to)?;
        Self::ensure_non_zero_address(&to_bytes, "Cannot mint to zero address")?;

        // Leggi total_supply
        let current_supply =
            Self::total_supply(contract_storage, storage, gas_meter.as_deref_mut())?;

        // Compute nuovo total_supply (checked arithmetic)
        let new_supply = current_supply
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("Total supply overflow"))?;

        contract_storage
            .sstore(
                storage,
                SLOT_TOTAL_SUPPLY,
                Self::u128_to_storage_value(new_supply),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update total_supply")?;

        // Leggi balance receiver
        let to_slot = Self::calculate_balance_slot(&to_bytes);
        let to_balance_value = contract_storage
            .sload(storage, to_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read receiver balance")?;
        let to_balance = Self::storage_value_to_u128(&to_balance_value)?;

        // Compute nuovo balance receiver
        let new_to_balance = to_balance
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;

        contract_storage
            .sstore(
                storage,
                to_slot,
                Self::u128_to_storage_value(new_to_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update receiver balance")?;

        // Emetti evento Transfer (from = zero address per mint)
        let zero_address = [0u8; 32];
        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &zero_address,
            &to_bytes,
            amount,
            gas_meter,
        );

        Ok(())
    }

    /// Burn token (opzionale)
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per ottenere caller e contract address
    /// * `from` - Address da cui bruciare (stringa hex, 32 bytes)
    /// * `amount` - Amount da bruciare
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    pub fn burn(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        amount: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        // SECURITY: Verify caller is token owner or has sufficient allowance
        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;
        let from_bytes = Self::decode_address(from)?;
        if caller != from_bytes {
            let caller_hex = hex::encode(&caller);
            let allowance = Self::allowance(
                contract_storage,
                storage,
                from,
                &caller_hex,
                gas_meter.as_deref_mut(),
            )?;
            if allowance < amount {
                anyhow::bail!(
                    "Burn not authorized: caller is not token owner and insufficient allowance"
                );
            }
            // Deduct allowance
            let new_allowance = allowance
                .checked_sub(amount)
                .ok_or_else(|| anyhow::anyhow!("Allowance underflow"))?;
            let allowance_slot = Self::calculate_allowance_slot(&from_bytes, &caller);
            contract_storage
                .sstore(
                    storage,
                    allowance_slot,
                    Self::u128_to_storage_value(new_allowance),
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| "Failed to update allowance after burn")?;
        }

        // Ottieni il contract address
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Leggi balance sender
        let from_slot = Self::calculate_balance_slot(&from_bytes);
        let from_balance_value = contract_storage
            .sload(storage, from_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read sender balance")?;
        let from_balance = Self::storage_value_to_u128(&from_balance_value)?;

        // Check balance sufficiente
        if from_balance < amount {
            anyhow::bail!(
                "Insufficient balance: have {}, need {}",
                from_balance,
                amount
            );
        }

        // Compute nuovo balance sender
        let new_from_balance = from_balance
            .checked_sub(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance underflow"))?;

        // Leggi total_supply
        let current_supply =
            Self::total_supply(contract_storage, storage, gas_meter.as_deref_mut())?;

        // Compute nuovo total_supply
        let new_supply = current_supply
            .checked_sub(amount)
            .ok_or_else(|| anyhow::anyhow!("Total supply underflow"))?;

        contract_storage
            .sstore(
                storage,
                from_slot,
                Self::u128_to_storage_value(new_from_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update sender balance")?;

        contract_storage
            .sstore(
                storage,
                SLOT_TOTAL_SUPPLY,
                Self::u128_to_storage_value(new_supply),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update total_supply")?;

        // Emetti evento Transfer (to = zero address per burn)
        let zero_address = [0u8; 32];
        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &from_bytes,
            &zero_address,
            amount,
            gas_meter,
        );

        Ok(())
    }
}
