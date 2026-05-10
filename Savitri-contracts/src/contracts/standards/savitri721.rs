//! SAVITRI-721: NFT Standard
//!
//! Implementation of SNT1 (SAVITRI Non Fungible Token) standard:
//! - balanceOf(owner), ownerOf(tokenId)
//! - transferFrom(from, to, tokenId), approve(to, tokenId)
//! - safeTransferFrom(from, to, tokenId), tokenURI(tokenId)

#![allow(dead_code)]
#![allow(clippy::needless_option_as_deref)]
#![allow(clippy::too_many_arguments)]
#![allow(clippy::needless_borrows_for_generic_args)]

use crate::contracts::base::BaseContract;
use crate::contracts::events::{CustomEvent, EventSystem};
use crate::contracts::gas::GasMeter;
use crate::contracts::runtime::Runtime;
use crate::contracts::storage::ContractStorage;
use crate::storage::Storage;
use anyhow::{Context, Result};
use hex;
use sha3::{Digest, Keccak256};

pub struct SAVITRI721;

const SLOT_TOKEN_OWNERS_BASE: u64 = 100;
const SLOT_TOKEN_BALANCES_BASE: u64 = 200;
const SLOT_TOKEN_APPROVALS_BASE: u64 = 300;
const SLOT_TOKEN_URIS_BASE: u64 = 400;

impl SAVITRI721 {
    fn u64_to_storage_value(value: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[0..8].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    fn storage_value_to_u64(value: &[u8]) -> Result<u64> {
        if value.len() < 8 {
            anyhow::bail!("Storage value too short for u64");
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&value[0..8]);
        Ok(u64::from_le_bytes(bytes))
    }

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

    fn encode_address(address: &[u8; 32]) -> String {
        format!("0x{}", hex::encode(address))
    }

    /// SECURITY FIX: Use Keccak256 hashing to derive slots, preventing
    /// collision between token categories. Previously, simple addition
    /// (base + token_id) meant token_id >= 100 would collide with
    /// SLOT_TOKEN_BALANCES_BASE, etc.
    fn token_owner_slot(token_id: u64) -> u64 {
        let mut hasher = Keccak256::new();
        hasher.update(&SLOT_TOKEN_OWNERS_BASE.to_le_bytes());
        hasher.update(&token_id.to_le_bytes());
        let hash = hasher.finalize();
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        u64::from_le_bytes(slot_bytes)
    }

    fn token_approval_slot(token_id: u64) -> u64 {
        let mut hasher = Keccak256::new();
        hasher.update(&SLOT_TOKEN_APPROVALS_BASE.to_le_bytes());
        hasher.update(&token_id.to_le_bytes());
        let hash = hasher.finalize();
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        u64::from_le_bytes(slot_bytes)
    }

    fn token_uri_slot(token_id: u64) -> u64 {
        let mut hasher = Keccak256::new();
        hasher.update(&SLOT_TOKEN_URIS_BASE.to_le_bytes());
        hasher.update(&token_id.to_le_bytes());
        let hash = hasher.finalize();
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        u64::from_le_bytes(slot_bytes)
    }

    fn owner_balance_slot(owner: &[u8; 32]) -> u64 {
        let mut hasher = Keccak256::new();
        hasher.update(owner);
        hasher.update(&SLOT_TOKEN_BALANCES_BASE.to_le_bytes());
        let hash = hasher.finalize();
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash[0..8]);
        u64::from_le_bytes(slot_bytes)
    }

    fn storage_value_to_address(value: &[u8]) -> Result<[u8; 32]> {
        if value.len() != 32 {
            anyhow::bail!(
                "Invalid address value length: expected 32, got {}",
                value.len()
            );
        }
        let mut out = [0u8; 32];
        out.copy_from_slice(value);
        Ok(out)
    }

    fn emit_transfer_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        from: &[u8; 32],
        to: &[u8; 32],
        token_id: u64,
        gas_meter: Option<&mut GasMeter>,
    ) {
        let transfer_signature = b"Transfer(address,address,uint256)";
        let mut hasher = Keccak256::new();
        hasher.update(transfer_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(from);
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(to);

        let mut data = vec![0u8; 32];
        data[0..8].copy_from_slice(&token_id.to_le_bytes());

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "Transfer".to_string(),
            topics: vec![topic0_bytes, topic1, topic2],
            data,
        };
        event_system.emit_custom_event(event, gas_meter);
    }

    fn emit_approval_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        owner: &[u8; 32],
        approved: &[u8; 32],
        token_id: u64,
        gas_meter: Option<&mut GasMeter>,
    ) {
        let approval_signature = b"Approval(address,address,uint256)";
        let mut hasher = Keccak256::new();
        hasher.update(approval_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(owner);
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(approved);

        let mut data = vec![0u8; 32];
        data[0..8].copy_from_slice(&token_id.to_le_bytes());

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "Approval".to_string(),
            topics: vec![topic0_bytes, topic1, topic2],
            data,
        };
        event_system.emit_custom_event(event, gas_meter);
    }

    pub fn mint(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        to: &str,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        if caller != owner {
            anyhow::bail!("Only owner can mint tokens");
        }

        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        let to_bytes = Self::decode_address(to)?;
        if to_bytes == [0u8; 32] {
            anyhow::bail!("Address cannot be zero");
        }

        let owner_slot = Self::token_owner_slot(token_id);
        let current_owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner")?;
        if !current_owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token already minted");
        }

        contract_storage
            .sstore(
                storage,
                owner_slot,
                to_bytes.to_vec(),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write token owner")?;

        let balance_slot = Self::owner_balance_slot(&to_bytes);
        let balance_value = contract_storage
            .sload(storage, balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read owner balance")?;
        let balance = Self::storage_value_to_u64(&balance_value)?;
        let new_balance = balance
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;
        contract_storage
            .sstore(
                storage,
                balance_slot,
                Self::u64_to_storage_value(new_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update owner balance")?;

        let zero_address = [0u8; 32];
        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &zero_address,
            &to_bytes,
            token_id,
            gas_meter,
        );

        Ok(())
    }

    pub fn balance_of(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u64> {
        let owner_bytes = Self::decode_address(owner)?;
        let slot = Self::owner_balance_slot(&owner_bytes);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read balance")?;
        Self::storage_value_to_u64(&value)
    }

    pub fn owner_of(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        let slot = Self::token_owner_slot(token_id);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner")?;
        if value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }
        let owner = Self::storage_value_to_address(&value)?;
        Ok(Self::encode_address(&owner))
    }

    pub fn get_approved(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<Option<String>> {
        let owner_slot = Self::token_owner_slot(token_id);
        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner")?;
        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        let slot = Self::token_approval_slot(token_id);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token approval")?;
        if value.iter().all(|&b| b == 0) {
            return Ok(None);
        }
        let approved = Self::storage_value_to_address(&value)?;
        Ok(Some(Self::encode_address(&approved)))
    }

    pub fn approve(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        to: &str,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        let owner_slot = Self::token_owner_slot(token_id);
        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner")?;
        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }
        let token_owner = Self::storage_value_to_address(&owner_value)?;
        if caller != token_owner {
            anyhow::bail!("Only token owner can approve");
        }

        let approved_bytes = Self::decode_address(to)?;
        if approved_bytes == token_owner {
            anyhow::bail!("Cannot approve owner");
        }
        let approval_slot = Self::token_approval_slot(token_id);
        contract_storage
            .sstore(
                storage,
                approval_slot,
                approved_bytes.to_vec(),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write token approval")?;

        let event_system = runtime.event_system();
        Self::emit_approval_event(
            &event_system,
            &contract_address,
            &token_owner,
            &approved_bytes,
            token_id,
            gas_meter,
        );

        Ok(())
    }

    pub fn transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;
        let contract_address = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        let from_bytes = Self::decode_address(from)?;
        let to_bytes = Self::decode_address(to)?;
        if to_bytes == [0u8; 32] {
            anyhow::bail!("Address cannot be zero");
        }

        let owner_slot = Self::token_owner_slot(token_id);
        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner")?;
        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }
        let token_owner = Self::storage_value_to_address(&owner_value)?;
        if token_owner != from_bytes {
            anyhow::bail!("From is not token owner");
        }

        let approval_slot = Self::token_approval_slot(token_id);
        let approval_value = contract_storage
            .sload(storage, approval_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token approval")?;
        let approved = if approval_value.iter().all(|&b| b == 0) {
            None
        } else {
            Some(Self::storage_value_to_address(&approval_value)?)
        };

        let caller_is_owner = caller == token_owner;
        let caller_is_approved = approved.map(|a| a == caller).unwrap_or(false);
        if !caller_is_owner && !caller_is_approved {
            anyhow::bail!("Caller is not owner nor approved");
        }

        if from_bytes == to_bytes {
            let event_system = runtime.event_system();
            Self::emit_transfer_event(
                &event_system,
                &contract_address,
                &from_bytes,
                &to_bytes,
                token_id,
                gas_meter,
            );
            return Ok(());
        }

        contract_storage
            .sstore(
                storage,
                owner_slot,
                to_bytes.to_vec(),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update token owner")?;

        contract_storage
            .sstore(
                storage,
                approval_slot,
                vec![0u8; 32],
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to clear approval")?;

        let from_balance_slot = Self::owner_balance_slot(&from_bytes);
        let from_balance_value = contract_storage
            .sload(storage, from_balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read from balance")?;
        let from_balance = Self::storage_value_to_u64(&from_balance_value)?;
        let new_from_balance = from_balance
            .checked_sub(1)
            .ok_or_else(|| anyhow::anyhow!("Balance underflow"))?;
        contract_storage
            .sstore(
                storage,
                from_balance_slot,
                Self::u64_to_storage_value(new_from_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update from balance")?;

        let to_balance_slot = Self::owner_balance_slot(&to_bytes);
        let to_balance_value = contract_storage
            .sload(storage, to_balance_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read to balance")?;
        let to_balance = Self::storage_value_to_u64(&to_balance_value)?;
        let new_to_balance = to_balance
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow"))?;
        contract_storage
            .sstore(
                storage,
                to_balance_slot,
                Self::u64_to_storage_value(new_to_balance),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to update to balance")?;

        let event_system = runtime.event_system();
        Self::emit_transfer_event(
            &event_system,
            &contract_address,
            &from_bytes,
            &to_bytes,
            token_id,
            gas_meter,
        );

        Ok(())
    }

    /// Compute il magic value per onSNT1Received
    /// bytes4(keccak256("onSNT1Received(address,address,uint256,bytes)"))
    fn on_snt1_received_magic_value() -> [u8; 4] {
        use crate::contracts::call::CallTransaction;
        CallTransaction::calculate_selector("onSNT1Received(address,address,uint256,bytes)")
    }

    /// Compute il function selector per onSNT1Received
    fn on_snt1_received_selector() -> [u8; 4] {
        Self::on_snt1_received_magic_value()
    }

    ///
    fn validate_receiver(
        storage: &Storage,
        runtime: &Runtime,
        receiver: &[u8; 32],
        operator: &[u8; 32],
        from: &[u8; 32],
        token_id: u64,
        data: &[u8],
        gas_meter: &mut Option<&mut GasMeter>,
    ) -> Result<()> {
        let is_contract = storage
            .contract_exists(receiver)
            .with_context(|| "Failed to check if receiver is a contract")?;

        if !is_contract {
            return Ok(());
        }

        let gas_meter_mut = gas_meter
            .as_deref_mut()
            .ok_or_else(|| anyhow::anyhow!("Gas meter required for receiver validation"))?;

        // Prepara calldata per onSNT1Received(address operator, address from, uint256 tokenId, bytes data)
        // Format: operator (32 bytes) + from (32 bytes) + tokenId (32 bytes) + data offset (32 bytes) + data length (32 bytes) + data (padded to 32 bytes)
        let mut calldata = Vec::new();

        // operator (32 bytes, padded)
        calldata.extend_from_slice(operator);

        // from (32 bytes, padded)
        calldata.extend_from_slice(from);

        // tokenId (32 bytes, padded) - ABI encoding usa big-endian
        let mut token_id_bytes = vec![0u8; 32];
        token_id_bytes[24..32].copy_from_slice(&token_id.to_be_bytes());
        calldata.extend_from_slice(&token_id_bytes);

        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&128u32.to_be_bytes());

        // data length (32 bytes)
        let data_len = data.len() as u32;
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&data_len.to_be_bytes());

        // data (padded to multiple of 32 bytes)
        if !data.is_empty() {
            calldata.extend_from_slice(data);
            let padding = (32 - (data.len() % 32)) % 32;
            calldata.extend(vec![0u8; padding]);
        }

        use crate::contracts::call::CallTransaction;
        let selector = Self::on_snt1_received_selector();
        let return_data = CallTransaction::call_contract(
            *receiver,
            selector,
            calldata,
            Some(50000), // Gas limit per onSNT1Received (standard SNT1)
            storage,
            runtime,
            gas_meter_mut,
        )
        .map_err(|e| {
            anyhow::anyhow!("Failed to call onSNT1Received on receiver contract: {}", e)
        })?;

        // Check che il return data sia esattamente il magic value (4 bytes)
        if return_data.len() < 4 {
            anyhow::bail!(
                "onSNT1Received returned invalid data: expected at least 4 bytes, got {}",
                return_data.len()
            );
        }

        let returned_magic = [
            return_data[0],
            return_data[1],
            return_data[2],
            return_data[3],
        ];
        let expected_magic = Self::on_snt1_received_magic_value();
        if returned_magic != expected_magic {
            anyhow::bail!(
                "onSNT1Received returned invalid magic value: expected {:?}, got {:?}",
                hex::encode(expected_magic),
                hex::encode(returned_magic)
            );
        }

        Ok(())
    }

    pub fn safe_transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        token_id: u64,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        Self::safe_transfer_from_with_data(
            contract_storage,
            storage,
            runtime,
            from,
            to,
            token_id,
            &[],
            gas_meter,
        )
    }

    pub fn safe_transfer_from_with_data(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        token_id: u64,
        data: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Decodifica gli address
        let from_bytes = Self::decode_address(from)?;
        let to_bytes = Self::decode_address(to)?;

        // Check che il receiver non sia l'address zero
        let zero_address = [0u8; 32];
        if to_bytes == zero_address {
            anyhow::bail!("Cannot transfer to zero address");
        }

        // Ottieni il caller (operator) dal runtime
        let operator = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        Self::validate_receiver(
            storage,
            runtime,
            &to_bytes,
            &operator,
            &from_bytes,
            token_id,
            data,
            &mut gas_meter,
        )?;

        Self::transfer_from(
            contract_storage,
            storage,
            runtime,
            from,
            to,
            token_id,
            gas_meter,
        )
    }

    ///
    /// Schema storage:
    /// - Slot base: [length (8 bytes, little-endian) | first 24 bytes of URI]
    /// - Slot base+1, base+2, ...: [next 32 bytes chunks] se URI > 24 bytes
    fn write_uri_to_storage(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        base_slot: u64,
        uri_bytes: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let uri_len = uri_bytes.len();

        // Slot base: [length (8 bytes) | first 24 bytes]
        let mut first_slot = vec![0u8; 32];
        first_slot[0..8].copy_from_slice(&uri_len.to_le_bytes());

        if uri_len <= 24 {
            // URI completo nel primo slot
            first_slot[8..8 + uri_len].copy_from_slice(uri_bytes);
            contract_storage
                .sstore(storage, base_slot, first_slot, gas_meter.as_deref_mut())
                .with_context(|| "Failed to write URI first slot")?;
        } else {
            // URI parziale nel primo slot + slot aggiuntivi
            first_slot[8..32].copy_from_slice(&uri_bytes[0..24]);
            contract_storage
                .sstore(storage, base_slot, first_slot, gas_meter.as_deref_mut())
                .with_context(|| "Failed to write URI first slot")?;

            // Scrive i chunk rimanenti (32 bytes per slot)
            let mut offset = 24;
            let mut slot_offset = 1u64;
            while offset < uri_len {
                let chunk_end = std::cmp::min(offset + 32, uri_len);
                let chunk_size = chunk_end - offset;

                let mut chunk = vec![0u8; 32];
                chunk[0..chunk_size].copy_from_slice(&uri_bytes[offset..chunk_end]);

                let chunk_slot = base_slot
                    .checked_add(slot_offset)
                    .ok_or_else(|| anyhow::anyhow!("Slot overflow for URI storage"))?;

                contract_storage
                    .sstore(storage, chunk_slot, chunk, gas_meter.as_deref_mut())
                    .with_context(|| format!("Failed to write URI chunk at slot {}", chunk_slot))?;

                offset = chunk_end;
                slot_offset = slot_offset
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("Too many slots for URI"))?;
            }
        }

        Ok(())
    }

    /// Legge un URI dallo storage (supporta multi-slot)
    fn read_uri_from_storage(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        base_slot: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        // Legge il primo slot: [length | first 24 bytes]
        let first_slot_value = contract_storage
            .sload(storage, base_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read URI first slot")?;

        // Estrae la lunghezza (primi 8 bytes)
        if first_slot_value.len() < 8 {
            anyhow::bail!("Invalid URI storage: first slot too short");
        }
        let mut len_bytes = [0u8; 8];
        len_bytes.copy_from_slice(&first_slot_value[0..8]);
        let uri_len = u64::from_le_bytes(len_bytes);

        if uri_len == 0 {
            return Ok(String::new());
        }

        // Costruisce l'URI
        let mut uri_bytes = Vec::with_capacity(uri_len as usize);

        if uri_len <= 24 {
            // URI completo nel primo slot
            uri_bytes.extend_from_slice(&first_slot_value[8..8 + uri_len as usize]);
        } else {
            // URI parziale nel primo slot + slot aggiuntivi
            uri_bytes.extend_from_slice(&first_slot_value[8..32]);

            // Legge i chunk rimanenti
            let mut offset = 24;
            let mut slot_offset = 1u64;
            while offset < uri_len as usize {
                let chunk_slot = base_slot
                    .checked_add(slot_offset)
                    .ok_or_else(|| anyhow::anyhow!("Slot overflow reading URI"))?;

                let chunk_value = contract_storage
                    .sload(storage, chunk_slot, gas_meter.as_deref_mut())
                    .with_context(|| format!("Failed to read URI chunk at slot {}", chunk_slot))?;

                let remaining = uri_len as usize - offset;
                let chunk_size = std::cmp::min(32, remaining);
                uri_bytes.extend_from_slice(&chunk_value[0..chunk_size]);

                offset += chunk_size;
                slot_offset = slot_offset
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("Too many slots reading URI"))?;
            }
        }

        // Converte in stringa UTF-8
        let uri = std::str::from_utf8(&uri_bytes).with_context(|| "Invalid UTF-8 in token URI")?;
        Ok(uri.to_string())
    }

    pub fn set_token_uri(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        token_id: u64,
        uri: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        let caller = runtime
            .current_frame()
            .ok_or_else(|| anyhow::anyhow!("No caller in execution context"))?
            .caller;

        let owner_slot = Self::token_owner_slot(token_id);
        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner")?;
        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }
        let token_owner = Self::storage_value_to_address(&owner_value)?;
        if caller != token_owner {
            anyhow::bail!("Only token owner can set token URI");
        }

        let uri_bytes = uri.as_bytes();
        let base_slot = Self::token_uri_slot(token_id);

        Self::write_uri_to_storage(
            contract_storage,
            storage,
            base_slot,
            uri_bytes,
            gas_meter.as_deref_mut(),
        )?;

        Ok(())
    }

    pub fn token_uri(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        token_id: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<String> {
        let owner_slot = Self::token_owner_slot(token_id);
        let owner_value = contract_storage
            .sload(storage, owner_slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read token owner")?;
        if owner_value.iter().all(|&b| b == 0) {
            anyhow::bail!("Token does not exist");
        }

        let base_slot = Self::token_uri_slot(token_id);
        Self::read_uri_from_storage(
            contract_storage,
            storage,
            base_slot,
            gas_meter.as_deref_mut(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::SAVITRI721;
    use crate::contracts::base::BaseContract;
    use crate::contracts::gas::GasMeter;
    use crate::contracts::runtime::{CallFrame, Runtime};
    use crate::contracts::storage::ContractStorage;
    use crate::storage::Storage;
    use anyhow::{Context, Result};
    use hex;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_tmp_dir(prefix: &str) -> Result<PathBuf> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut path = std::env::temp_dir();
        path.push(format!("{}-{}", prefix, nanos));
        fs::create_dir_all(&path)?;
        Ok(path)
    }

    fn create_test_storage(prefix: &str) -> Result<(Storage, PathBuf)> {
        let path = unique_tmp_dir(prefix)?;
        let storage = Storage::new(&path).context("Failed to create test storage")?;
        Ok((storage, path))
    }

    fn create_test_runtime(contract_address: [u8; 32], caller: [u8; 32]) -> Runtime {
        let runtime = Runtime::with_empty_overlay(1_000_000, 1_000);
        let frame = CallFrame {
            contract_address,
            caller,
            value: 0,
            calldata: Vec::new(),
            return_data: Vec::new(),
            gas_remaining: 1_000_000,
            depth: 0,
            storage_snapshot: [0u8; 64],
        };
        runtime.push_frame(frame).expect("Failed to push frame");
        runtime
    }

    fn encode_address(address: &[u8; 32]) -> String {
        format!("0x{}", hex::encode(address))
    }

    struct TestEnv {
        storage: Storage,
        contract_storage: ContractStorage,
        contract_address: [u8; 32],
        contract_owner: [u8; 32],
        alice: [u8; 32],
        bob: [u8; 32],
        carol: [u8; 32],
        gas_meter: GasMeter,
    }

    impl TestEnv {
        fn new(prefix: &str) -> Result<Self> {
            let (storage, _tmp_dir) = create_test_storage(prefix)?;
            let contract_address = [0xAB; 32];
            let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;
            let contract_owner = [0x11; 32];
            let gas_meter = GasMeter::new(1_000_000);

            BaseContract::initialize(&mut contract_storage, &storage, &contract_owner, None)?;

            Ok(Self {
                storage,
                contract_storage,
                contract_address,
                contract_owner,
                alice: [0x22; 32],
                bob: [0x33; 32],
                carol: [0x44; 32],
                gas_meter,
            })
        }

        fn runtime(&self, caller: [u8; 32]) -> Runtime {
            create_test_runtime(self.contract_address, caller)
        }

        fn mint(&mut self, caller: [u8; 32], to: [u8; 32], token_id: u64) -> Result<Runtime> {
            let runtime = self.runtime(caller);
            SAVITRI721::mint(
                &mut self.contract_storage,
                &self.storage,
                &runtime,
                &encode_address(&to),
                token_id,
                Some(&mut self.gas_meter),
            )?;
            Ok(runtime)
        }

        fn approve(
            &mut self,
            caller: [u8; 32],
            approved: [u8; 32],
            token_id: u64,
        ) -> Result<Runtime> {
            let runtime = self.runtime(caller);
            SAVITRI721::approve(
                &mut self.contract_storage,
                &self.storage,
                &runtime,
                &encode_address(&approved),
                token_id,
                Some(&mut self.gas_meter),
            )?;
            Ok(runtime)
        }

        fn transfer(
            &mut self,
            caller: [u8; 32],
            from: [u8; 32],
            to: [u8; 32],
            token_id: u64,
        ) -> Result<Runtime> {
            let runtime = self.runtime(caller);
            SAVITRI721::transfer_from(
                &mut self.contract_storage,
                &self.storage,
                &runtime,
                &encode_address(&from),
                &encode_address(&to),
                token_id,
                Some(&mut self.gas_meter),
            )?;
            Ok(runtime)
        }

        fn safe_transfer(
            &mut self,
            caller: [u8; 32],
            from: [u8; 32],
            to: [u8; 32],
            token_id: u64,
        ) -> Result<Runtime> {
            let runtime = self.runtime(caller);
            SAVITRI721::safe_transfer_from(
                &mut self.contract_storage,
                &self.storage,
                &runtime,
                &encode_address(&from),
                &encode_address(&to),
                token_id,
                Some(&mut self.gas_meter),
            )?;
            Ok(runtime)
        }

        fn set_token_uri(&mut self, caller: [u8; 32], token_id: u64, uri: &str) -> Result<()> {
            let runtime = self.runtime(caller);
            SAVITRI721::set_token_uri(
                &mut self.contract_storage,
                &self.storage,
                &runtime,
                token_id,
                uri,
                Some(&mut self.gas_meter),
            )
        }

        fn owner_of(&mut self, token_id: u64) -> Result<String> {
            SAVITRI721::owner_of(
                &mut self.contract_storage,
                &self.storage,
                token_id,
                Some(&mut self.gas_meter),
            )
        }

        fn balance_of(&mut self, owner: [u8; 32]) -> Result<u64> {
            SAVITRI721::balance_of(
                &mut self.contract_storage,
                &self.storage,
                &encode_address(&owner),
                Some(&mut self.gas_meter),
            )
        }

        fn approved(&mut self, token_id: u64) -> Result<Option<String>> {
            SAVITRI721::get_approved(
                &mut self.contract_storage,
                &self.storage,
                token_id,
                Some(&mut self.gas_meter),
            )
        }

        fn token_uri(&mut self, token_id: u64) -> Result<String> {
            SAVITRI721::token_uri(
                &mut self.contract_storage,
                &self.storage,
                token_id,
                Some(&mut self.gas_meter),
            )
        }
    }

    #[test]
    fn mint_tracks_owner_balance_and_runtime_events() -> Result<()> {
        let mut env = TestEnv::new("savitri721-mint")?;

        let runtime = env.mint(env.contract_owner, env.alice, 1)?;

        assert_eq!(env.owner_of(1)?, encode_address(&env.alice));
        assert_eq!(env.balance_of(env.alice)?, 1);

        let events = runtime.event_system().get_custom_events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_name, "Transfer");

        Ok(())
    }

    #[test]
    fn mint_rejects_zero_address_and_non_owner() -> Result<()> {
        let mut env = TestEnv::new("savitri721-mint-guards")?;

        let zero_address = [0u8; 32];
        let owner_runtime = env.runtime(env.contract_owner);
        let zero_result = SAVITRI721::mint(
            &mut env.contract_storage,
            &env.storage,
            &owner_runtime,
            &encode_address(&zero_address),
            1,
            Some(&mut env.gas_meter),
        );
        assert!(zero_result.is_err());
        assert!(zero_result
            .unwrap_err()
            .to_string()
            .contains("Address cannot be zero"));

        let non_owner_runtime = env.runtime(env.alice);
        let non_owner_result = SAVITRI721::mint(
            &mut env.contract_storage,
            &env.storage,
            &non_owner_runtime,
            &encode_address(&env.alice),
            2,
            Some(&mut env.gas_meter),
        );
        assert!(non_owner_result.is_err());
        assert!(non_owner_result
            .unwrap_err()
            .to_string()
            .contains("Only owner can mint tokens"));

        Ok(())
    }

    #[test]
    fn approve_and_transfer_by_approved_clears_approval() -> Result<()> {
        let mut env = TestEnv::new("savitri721-approved-transfer")?;

        env.mint(env.contract_owner, env.alice, 7)?;
        let approval_runtime = env.approve(env.alice, env.bob, 7)?;
        assert_eq!(env.approved(7)?, Some(encode_address(&env.bob)));
        assert_eq!(approval_runtime.event_system().get_custom_events().len(), 1);

        let transfer_runtime = env.transfer(env.bob, env.alice, env.carol, 7)?;

        assert_eq!(env.owner_of(7)?, encode_address(&env.carol));
        assert_eq!(env.balance_of(env.alice)?, 0);
        assert_eq!(env.balance_of(env.carol)?, 1);
        assert_eq!(env.approved(7)?, None);
        assert_eq!(transfer_runtime.event_system().get_custom_events().len(), 1);

        Ok(())
    }

    #[test]
    fn approve_rejects_token_owner_address() -> Result<()> {
        let mut env = TestEnv::new("savitri721-approve-owner")?;

        env.mint(env.contract_owner, env.alice, 3)?;
        let runtime = env.runtime(env.alice);
        let result = SAVITRI721::approve(
            &mut env.contract_storage,
            &env.storage,
            &runtime,
            &encode_address(&env.alice),
            3,
            Some(&mut env.gas_meter),
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot approve owner"));

        Ok(())
    }

    #[test]
    fn transfer_rejects_zero_address() -> Result<()> {
        let mut env = TestEnv::new("savitri721-zero-transfer")?;

        env.mint(env.contract_owner, env.alice, 9)?;
        let zero_address = [0u8; 32];
        let runtime = env.runtime(env.alice);
        let result = SAVITRI721::transfer_from(
            &mut env.contract_storage,
            &env.storage,
            &runtime,
            &encode_address(&env.alice),
            &encode_address(&zero_address),
            9,
            Some(&mut env.gas_meter),
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Address cannot be zero"));

        Ok(())
    }

    #[test]
    fn safe_transfer_rejects_zero_address() -> Result<()> {
        let mut env = TestEnv::new("savitri721-zero-safe-transfer")?;

        env.mint(env.contract_owner, env.alice, 11)?;
        let zero_address = [0u8; 32];
        let runtime = env.runtime(env.alice);
        let result = SAVITRI721::safe_transfer_from(
            &mut env.contract_storage,
            &env.storage,
            &runtime,
            &encode_address(&env.alice),
            &encode_address(&zero_address),
            11,
            Some(&mut env.gas_meter),
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Cannot transfer to zero address"));

        Ok(())
    }

    #[test]
    fn long_token_uri_round_trips_across_multiple_slots() -> Result<()> {
        let mut env = TestEnv::new("savitri721-uri")?;

        env.mint(env.contract_owner, env.alice, 42)?;
        let long_uri = "https://example.com/api/v1/metadata/tokens/1234567890123456789012345678901234567890123456789012345678901234567890";
        env.set_token_uri(env.alice, 42, long_uri)?;

        assert_eq!(env.token_uri(42)?, long_uri);

        Ok(())
    }

    #[test]
    fn token_ids_above_reserved_ranges_do_not_collide() -> Result<()> {
        let mut env = TestEnv::new("savitri721-slot-collision")?;

        env.mint(env.contract_owner, env.alice, 100)?;
        env.mint(env.contract_owner, env.bob, 200)?;

        assert_eq!(env.owner_of(100)?, encode_address(&env.alice));
        assert_eq!(env.owner_of(200)?, encode_address(&env.bob));
        assert_eq!(env.balance_of(env.alice)?, 1);
        assert_eq!(env.balance_of(env.bob)?, 1);

        Ok(())
    }

    #[test]
    fn paused_contract_blocks_mint_and_transfer() -> Result<()> {
        let mut env = TestEnv::new("savitri721-paused")?;

        let owner_runtime = env.runtime(env.contract_owner);
        BaseContract::pause(
            &mut env.contract_storage,
            &env.storage,
            &owner_runtime,
            Some(&mut env.gas_meter),
        )?;

        let mint_result = SAVITRI721::mint(
            &mut env.contract_storage,
            &env.storage,
            &owner_runtime,
            &encode_address(&env.alice),
            1,
            Some(&mut env.gas_meter),
        );
        assert!(mint_result.is_err());
        assert!(mint_result
            .unwrap_err()
            .to_string()
            .contains("Contract is paused"));

        BaseContract::unpause(
            &mut env.contract_storage,
            &env.storage,
            &owner_runtime,
            Some(&mut env.gas_meter),
        )?;
        env.mint(env.contract_owner, env.alice, 1)?;
        BaseContract::pause(
            &mut env.contract_storage,
            &env.storage,
            &owner_runtime,
            Some(&mut env.gas_meter),
        )?;

        let alice_runtime = env.runtime(env.alice);
        let transfer_result = SAVITRI721::transfer_from(
            &mut env.contract_storage,
            &env.storage,
            &alice_runtime,
            &encode_address(&env.alice),
            &encode_address(&env.bob),
            1,
            Some(&mut env.gas_meter),
        );
        assert!(transfer_result.is_err());
        assert!(transfer_result
            .unwrap_err()
            .to_string()
            .contains("Contract is paused"));

        Ok(())
    }

    #[test]
    fn transfer_to_self_keeps_balances_and_owner() -> Result<()> {
        let mut env = TestEnv::new("savitri721-self-transfer")?;

        env.mint(env.contract_owner, env.alice, 55)?;
        let before = env.balance_of(env.alice)?;
        let runtime = env.transfer(env.alice, env.alice, env.alice, 55)?;

        assert_eq!(env.balance_of(env.alice)?, before);
        assert_eq!(env.owner_of(55)?, encode_address(&env.alice));
        assert_eq!(runtime.event_system().get_custom_events().len(), 1);

        Ok(())
    }

    #[test]
    fn safe_transfer_to_eoa_succeeds() -> Result<()> {
        let mut env = TestEnv::new("savitri721-safe-transfer")?;

        env.mint(env.contract_owner, env.alice, 77)?;
        let runtime = env.safe_transfer(env.alice, env.alice, env.bob, 77)?;

        assert_eq!(env.owner_of(77)?, encode_address(&env.bob));
        assert_eq!(env.balance_of(env.alice)?, 0);
        assert_eq!(env.balance_of(env.bob)?, 1);
        assert_eq!(runtime.event_system().get_custom_events().len(), 1);

        Ok(())
    }
}
