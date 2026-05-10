//! SAVITRI-1155: Multi-Asset Token Standard
//!
//! Implementation of SMA (SAVITRI Multi Asset) standard:
//! - balanceOf(owner, id), balanceOfBatch(owners, ids)
//! - safeTransferFrom(from, to, id, amount, data)
//! - safeBatchTransferFrom(from, to, ids, amounts, data)
//! - setApprovalForAll(operator, approved)
//!
//! # Storage Layout
//!
//! Storage layout is optimized to support efficient batch queries:
//!
//! ## Slot Allocation
//! - **Slot 0-99**: BaseContract (reserved for base contract functionality)
//! - **Slot 100+**: `balances[owner][id]` - Nested mapping for balances of each owner for each token id
//! - **Slot 200+**: `operator_approvals[owner][operator]` - Nested mapping for operator approvals
//!
//! ## Balance Storage (Slot 100+)
//! Formula: `slot = keccak256(id || keccak256(owner || 100))`
//!
//! - First hash: `keccak256(owner || SLOT_BALANCES_BASE)` where owner is 32 bytes and SLOT_BALANCES_BASE = 100
//! - Second hash: `keccak256(id || hash1)` where id is encoded as 32 bytes (big-endian)
//! - Final slot: first 8 bytes of second hash as u64
//!
//! This layout ensures:
//! - **Uniform distribution**: keccak256 ensures uniform slot distribution
//! - **No collisions**: collision probability is negligible
//! - **Efficient batch queries**: each slot is calculated deterministically and independently
//! - **Cache-friendly**: ContractStorage caches reads to optimize repeated queries
//!
//! ## Operator Approval Storage (Slot 200+)
//! Formula: `slot = keccak256(operator || keccak256(owner || 200))`
//!
//! - Primo hash: `keccak256(owner || SLOT_OPERATOR_APPROVALS_BASE)` dove SLOT_OPERATOR_APPROVALS_BASE = 200
//! - Secondo hash: `keccak256(operator || hash1)` dove operator è 32 bytes
//! - Slot finale: primi 8 bytes of the secondo hash come u64
//!
//! ## Batch Query Optimization
//!

use crate::contracts::base::BaseContract;
use crate::contracts::events::{CustomEvent, EventSystem};
use crate::contracts::gas::GasMeter;
use crate::contracts::runtime::Runtime;
use crate::contracts::storage::ContractStorage;
use crate::storage::Storage;
use anyhow::{Context, Result};
use hex;
use sha3::{Digest, Keccak256};

pub struct SAVITRI1155;

const SLOT_BALANCES_BASE: u64 = 100;
const SLOT_OPERATOR_APPROVALS_BASE: u64 = 200;

impl SAVITRI1155 {
    /// Converte u128 a storage value (32 bytes, little-endian)
    pub fn u128_to_storage_value(value: u128) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[0..16].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    /// Converte storage value (32 bytes) a u128
    fn storage_value_to_u128(value: &[u8]) -> Result<u128> {
        if value.len() < 16 {
            anyhow::bail!("Storage value too short for u128");
        }
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&value[0..16]);
        Ok(u128::from_le_bytes(bytes))
    }

    /// Converte u64 a storage value (32 bytes, little-endian)
    fn u64_to_storage_value(value: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[0..8].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    /// Converte storage value (32 bytes) a u64
    fn storage_value_to_u64(value: &[u8]) -> Result<u64> {
        if value.len() < 8 {
            anyhow::bail!("Storage value too short for u64");
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&value[0..8]);
        Ok(u64::from_le_bytes(bytes))
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

    /// Codifica address da bytes a stringa hex
    fn encode_address(address: &[u8; 32]) -> String {
        format!("0x{}", hex::encode(address))
    }

    ///
    /// Formula: slot = keccak256(id || keccak256(owner || slot_base))
    pub fn calculate_balance_slot(owner: &[u8; 32], id: u128) -> u64 {
        // Primo hash: keccak256(owner || slot_base)
        let mut hasher1 = Keccak256::new();
        hasher1.update(owner);
        hasher1.update(&SLOT_BALANCES_BASE.to_le_bytes());
        let hash1 = hasher1.finalize();

        let mut id_bytes = vec![0u8; 32];
        id_bytes[16..32].copy_from_slice(&id.to_be_bytes());

        // Secondo hash: keccak256(id || hash1)
        let mut hasher2 = Keccak256::new();
        hasher2.update(&id_bytes);
        hasher2.update(&hash1);
        let hash2 = hasher2.finalize();

        // Prendi i primi 8 bytes of the hash come u64
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash2[0..8]);
        u64::from_le_bytes(slot_bytes)
    }

    ///
    /// Formula: slot = keccak256(operator || keccak256(owner || slot_base))
    fn calculate_operator_approval_slot(owner: &[u8; 32], operator: &[u8; 32]) -> u64 {
        // Primo hash: keccak256(owner || slot_base)
        let mut hasher1 = Keccak256::new();
        hasher1.update(owner);
        hasher1.update(&SLOT_OPERATOR_APPROVALS_BASE.to_le_bytes());
        let hash1 = hasher1.finalize();

        // Secondo hash: keccak256(operator || hash1)
        let mut hasher2 = Keccak256::new();
        hasher2.update(operator);
        hasher2.update(&hash1);
        let hash2 = hasher2.finalize();

        // Prendi i primi 8 bytes of the hash come u64
        let mut slot_bytes = [0u8; 8];
        slot_bytes.copy_from_slice(&hash2[0..8]);
        u64::from_le_bytes(slot_bytes)
    }

    /// Emette evento TransferSingle
    pub(crate) fn emit_transfer_single_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        operator: &[u8; 32],
        from: &[u8; 32],
        to: &[u8; 32],
        id: u128,
        value: u128,
        gas_meter: Option<&mut GasMeter>,
    ) {
        // Topic 0: keccak256("TransferSingle(address,address,address,uint256,uint256)")
        let transfer_signature = b"TransferSingle(address,address,address,uint256,uint256)";
        let mut hasher = Keccak256::new();
        hasher.update(transfer_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        // Topic 1: operator (padded to 32 bytes)
        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(operator);

        // Topic 2: from (padded to 32 bytes)
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(from);

        // Topic 3: to (padded to 32 bytes)
        let mut topic3 = [0u8; 32];
        topic3.copy_from_slice(to);

        // Data: id (u128, 32 bytes) + value (u128, 32 bytes)
        let mut data = vec![0u8; 64];
        data[16..32].copy_from_slice(&id.to_be_bytes());
        data[48..64].copy_from_slice(&value.to_be_bytes());

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "TransferSingle".to_string(),
            topics: vec![topic0_bytes, topic1, topic2, topic3],
            data,
        };

        event_system.emit_custom_event(event, gas_meter);
    }

    /// Emette evento TransferBatch
    fn emit_transfer_batch_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        operator: &[u8; 32],
        from: &[u8; 32],
        to: &[u8; 32],
        ids: &[u128],
        values: &[u128],
        gas_meter: Option<&mut GasMeter>,
    ) {
        // Topic 0: keccak256("TransferBatch(address,address,address,uint256[],uint256[])")
        let transfer_signature = b"TransferBatch(address,address,address,uint256[],uint256[])";
        let mut hasher = Keccak256::new();
        hasher.update(transfer_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        // Topic 1: operator (padded to 32 bytes)
        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(operator);

        // Topic 2: from (padded to 32 bytes)
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(from);

        // Topic 3: to (padded to 32 bytes)
        let mut topic3 = [0u8; 32];
        topic3.copy_from_slice(to);

        // Data: ids offset (32 bytes) + values offset (32 bytes) + ids length (32 bytes) + ids data + values length (32 bytes) + values data
        // Per semplicità, codifichiamo ids e values come array ABI-encoded
        let mut data = Vec::new();

        // ids offset: 0x80 (128) - dopo operator, from, to
        data.extend_from_slice(&[0u8; 28]);
        data.extend_from_slice(&128u32.to_be_bytes());

        // values offset: 0x80 + 32 + 32 + (ids.len() * 32) = 128 + 64 + (ids.len() * 32)
        let values_offset = 128u32 + 64 + (ids.len() as u32 * 32);
        data.extend_from_slice(&[0u8; 28]);
        data.extend_from_slice(&values_offset.to_be_bytes());

        // ids length
        data.extend_from_slice(&[0u8; 28]);
        data.extend_from_slice(&(ids.len() as u32).to_be_bytes());

        // ids data (padded to 32 bytes each)
        for id in ids {
            let mut id_bytes = vec![0u8; 32];
            id_bytes[16..32].copy_from_slice(&id.to_be_bytes());
            data.extend_from_slice(&id_bytes);
        }

        // values length
        data.extend_from_slice(&[0u8; 28]);
        data.extend_from_slice(&(values.len() as u32).to_be_bytes());

        // values data (padded to 32 bytes each)
        for value in values {
            let mut value_bytes = vec![0u8; 32];
            value_bytes[16..32].copy_from_slice(&value.to_be_bytes());
            data.extend_from_slice(&value_bytes);
        }

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "TransferBatch".to_string(),
            topics: vec![topic0_bytes, topic1, topic2, topic3],
            data,
        };

        event_system.emit_custom_event(event, gas_meter);
    }

    /// Emette evento ApprovalForAll
    fn emit_approval_for_all_event(
        event_system: &EventSystem,
        contract_address: &[u8; 32],
        owner: &[u8; 32],
        operator: &[u8; 32],
        approved: bool,
        gas_meter: Option<&mut GasMeter>,
    ) {
        // Topic 0: keccak256("ApprovalForAll(address,address,bool)")
        let approval_signature = b"ApprovalForAll(address,address,bool)";
        let mut hasher = Keccak256::new();
        hasher.update(approval_signature);
        let topic0 = hasher.finalize();
        let mut topic0_bytes = [0u8; 32];
        topic0_bytes.copy_from_slice(&topic0);

        // Topic 1: owner (padded to 32 bytes)
        let mut topic1 = [0u8; 32];
        topic1.copy_from_slice(owner);

        // Topic 2: operator (padded to 32 bytes)
        let mut topic2 = [0u8; 32];
        topic2.copy_from_slice(operator);

        // Data: approved (bool, encoded as 32 bytes: 0x00...00 or 0x00...01)
        let mut data = vec![0u8; 32];
        if approved {
            data[31] = 1;
        }

        let event = CustomEvent {
            contract_address: hex::encode(contract_address),
            event_name: "ApprovalForAll".to_string(),
            topics: vec![topic0_bytes, topic1, topic2],
            data,
        };

        event_system.emit_custom_event(event, gas_meter);
    }

    /// Compute il magic value per onSMAReceived
    /// bytes4(keccak256("onSMAReceived(address,address,uint256,uint256,bytes)"))
    fn on_sma_received_magic_value() -> [u8; 4] {
        use crate::contracts::call::CallTransaction;
        CallTransaction::calculate_selector("onSMAReceived(address,address,uint256,uint256,bytes)")
    }

    /// Compute il magic value per onSMABatchReceived
    /// bytes4(keccak256("onSMABatchReceived(address,address,uint256[],uint256[],bytes)"))
    fn on_sma_batch_received_magic_value() -> [u8; 4] {
        use crate::contracts::call::CallTransaction;
        CallTransaction::calculate_selector(
            "onSMABatchReceived(address,address,uint256[],uint256[],bytes)",
        )
    }

    ///
    fn validate_receiver(
        storage: &Storage,
        runtime: &Runtime,
        receiver: &[u8; 32],
        operator: &[u8; 32],
        from: &[u8; 32],
        id: u128,
        value: u128,
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

        // Prepara calldata per onSMAReceived(address operator, address from, uint256 id, uint256 value, bytes data)
        let mut calldata = Vec::new();

        // operator (32 bytes, padded)
        calldata.extend_from_slice(operator);

        // from (32 bytes, padded)
        calldata.extend_from_slice(from);

        // id (32 bytes, padded) - ABI encoding usa big-endian
        let mut id_bytes = vec![0u8; 32];
        id_bytes[16..32].copy_from_slice(&id.to_be_bytes());
        calldata.extend_from_slice(&id_bytes);

        // value (32 bytes, padded) - ABI encoding usa big-endian
        let mut value_bytes = vec![0u8; 32];
        value_bytes[16..32].copy_from_slice(&value.to_be_bytes());
        calldata.extend_from_slice(&value_bytes);

        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&160u32.to_be_bytes());

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
        let selector = Self::on_sma_received_magic_value();
        let return_data = CallTransaction::call_contract(
            *receiver,
            selector,
            calldata,
            Some(50000), // Gas limit per onSMAReceived (standard SMA)
            storage,
            runtime,
            gas_meter_mut,
        )
        .map_err(|e| anyhow::anyhow!("Failed to call onSMAReceived on receiver contract: {}", e))?;

        // Check che il return data sia esattamente il magic value (4 bytes)
        if return_data.len() < 4 {
            anyhow::bail!(
                "onSMAReceived returned invalid data: expected at least 4 bytes, got {}",
                return_data.len()
            );
        }

        let returned_magic = [
            return_data[0],
            return_data[1],
            return_data[2],
            return_data[3],
        ];
        let expected_magic = Self::on_sma_received_magic_value();
        if returned_magic != expected_magic {
            anyhow::bail!(
                "onSMAReceived returned invalid magic value: expected {:?}, got {:?}",
                hex::encode(expected_magic),
                hex::encode(returned_magic)
            );
        }

        Ok(())
    }

    ///
    fn validate_batch_receiver(
        storage: &Storage,
        runtime: &Runtime,
        receiver: &[u8; 32],
        operator: &[u8; 32],
        from: &[u8; 32],
        ids: &[u128],
        values: &[u128],
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
            .ok_or_else(|| anyhow::anyhow!("Gas meter required for batch receiver validation"))?;

        // Prepara calldata per onSMABatchReceived(address operator, address from, uint256[] ids, uint256[] values, bytes data)
        let mut calldata = Vec::new();

        // operator (32 bytes, padded)
        calldata.extend_from_slice(operator);

        // from (32 bytes, padded)
        calldata.extend_from_slice(from);

        // ids offset (32 bytes) - offset to start of ids array data
        // Layout: operator(32) + from(32) + ids_offset(32) + values_offset(32) + data_offset(32) = 160 bytes of head
        // ids array starts at offset 160
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&160u32.to_be_bytes());

        // values offset (32 bytes) - offset to start of values array data
        // values array starts after ids: 160 + 32 (ids length) + ids.len() * 32
        let values_offset = 160u32 + 32 + (ids.len() as u32 * 32);
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&values_offset.to_be_bytes());

        // data offset (32 bytes) - offset to start of bytes data
        // data starts after values: values_offset + 32 (values length) + values.len() * 32
        let data_offset = values_offset + 32 + (values.len() as u32 * 32);
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&data_offset.to_be_bytes());

        // ids array: length (32 bytes) + each id (32 bytes)
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&(ids.len() as u32).to_be_bytes());
        for id in ids {
            let mut id_bytes = vec![0u8; 32];
            id_bytes[16..32].copy_from_slice(&id.to_be_bytes());
            calldata.extend_from_slice(&id_bytes);
        }

        // values array: length (32 bytes) + each value (32 bytes)
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&(values.len() as u32).to_be_bytes());
        for value in values {
            let mut value_bytes = vec![0u8; 32];
            value_bytes[16..32].copy_from_slice(&value.to_be_bytes());
            calldata.extend_from_slice(&value_bytes);
        }

        // data: length (32 bytes) + data (padded to multiple of 32 bytes)
        let data_len = data.len() as u32;
        calldata.extend_from_slice(&[0u8; 28]);
        calldata.extend_from_slice(&data_len.to_be_bytes());
        if !data.is_empty() {
            calldata.extend_from_slice(data);
            let padding = (32 - (data.len() % 32)) % 32;
            calldata.extend(vec![0u8; padding]);
        }

        use crate::contracts::call::CallTransaction;
        let selector = Self::on_sma_batch_received_magic_value();
        let return_data = CallTransaction::call_contract(
            *receiver,
            selector,
            calldata,
            Some(100_000), // Gas limit per onSMABatchReceived (higher than single due to batch)
            storage,
            runtime,
            gas_meter_mut,
        )
        .map_err(|e| {
            anyhow::anyhow!(
                "Failed to call onSMABatchReceived on receiver contract: {}",
                e
            )
        })?;

        // Check che il return data sia esattamente il magic value (4 bytes)
        if return_data.len() < 4 {
            anyhow::bail!(
                "onSMABatchReceived returned invalid data: expected at least 4 bytes, got {}",
                return_data.len()
            );
        }

        let returned_magic = [
            return_data[0],
            return_data[1],
            return_data[2],
            return_data[3],
        ];
        let expected_magic = Self::on_sma_batch_received_magic_value();
        if returned_magic != expected_magic {
            anyhow::bail!(
                "onSMABatchReceived returned invalid magic value: expected {:?}, got {:?}",
                hex::encode(expected_magic),
                hex::encode(returned_magic)
            );
        }

        Ok(())
    }

    /// Ottiene il balance di un owner per un id
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `id` - ID of the token
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Balance (u128) o errore
    pub fn balance_of(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        id: u128,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<u128> {
        let owner_bytes = Self::decode_address(owner)?;
        let slot = Self::calculate_balance_slot(&owner_bytes, id);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read balance")?;

        if value.iter().all(|&b| b == 0) {
            Ok(0)
        } else {
            Self::storage_value_to_u128(&value)
        }
    }

    /// Ottiene i balance di più owner per più id (batch)
    ///
    /// - Sfrutta la cache of the ContractStorage per letture multiple
    ///
    /// # Query Batch Optimization
    ///
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `owners` - Array di address degli owner (hex strings)
    /// * `ids` - Array di ID dei token
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Array di balance (u128) o errore
    ///
    /// # Note
    ///   in modo deterministico e indipendente, permettendo letture parallele
    pub fn balance_of_batch(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owners: &[String],
        ids: &[u128],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<Vec<u128>> {
        if owners.len() != ids.len() {
            anyhow::bail!("owners and ids arrays must have the same length");
        }

        let mut slots_and_ids: Vec<(u64, u128)> = Vec::with_capacity(owners.len());
        for (owner, id) in owners.iter().zip(ids.iter()) {
            let owner_bytes = Self::decode_address(owner)?;
            let slot = Self::calculate_balance_slot(&owner_bytes, *id);
            slots_and_ids.push((slot, *id));
        }

        // La cache of the ContractStorage ottimizza le letture ripetute
        let mut balances = Vec::with_capacity(slots_and_ids.len());
        for (slot, _id) in slots_and_ids.iter() {
            let value = contract_storage
                .sload(storage, *slot, gas_meter.as_deref_mut())
                .with_context(|| format!("Failed to read balance at slot {}", slot))?;

            if value.iter().all(|&b| b == 0) {
                balances.push(0);
            } else {
                let balance = Self::storage_value_to_u128(&value)?;
                balances.push(balance);
            }
        }

        Ok(balances)
    }

    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// true se approvato, false altrimenti
    pub fn is_approved_for_all(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner: &str,
        operator: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        let owner_bytes = Self::decode_address(owner)?;
        let operator_bytes = Self::decode_address(operator)?;
        let slot = Self::calculate_operator_approval_slot(&owner_bytes, &operator_bytes);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .with_context(|| "Failed to read operator approval")?;

        if value.iter().all(|&b| b == 0) {
            Ok(false)
        } else {
            // Leggi il valore come u64 (1 = true, 0 = false)
            let approval_value = Self::storage_value_to_u64(&value)?;
            Ok(approval_value != 0)
        }
    }

    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per ottenere caller e contract address
    /// * `approved` - true per approvare, false per revocare
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Ok(()) se successo, errore altrimenti
    pub fn set_approval_for_all(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        operator: &str,
        approved: bool,
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

        let operator_bytes = Self::decode_address(operator)?;
        let slot = Self::calculate_operator_approval_slot(&caller, &operator_bytes);

        // Salva approval (1 = true, 0 = false)
        let approval_value = if approved { 1u64 } else { 0u64 };
        contract_storage
            .sstore(
                storage,
                slot,
                Self::u64_to_storage_value(approval_value),
                gas_meter.as_deref_mut(),
            )
            .with_context(|| "Failed to write operator approval")?;

        // Emetti evento ApprovalForAll
        let event_system = EventSystem::new();
        Self::emit_approval_for_all_event(
            &event_system,
            &contract_address,
            &caller,
            &operator_bytes,
            approved,
            gas_meter,
        );

        Ok(())
    }

    /// Safe transfer di un singolo token
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per ottenere caller e contract address
    /// * `id` - ID of the token
    /// * `amount` - Quantità da trasferire
    /// * `data` - Dati aggiuntivi da passare al receiver
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Ok(()) se successo, errore altrimenti
    pub fn safe_transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        id: u128,
        amount: u128,
        data: &[u8],
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

        // Check che il receiver non sia l'address zero
        let zero_address = [0u8; 32];
        if to_bytes == zero_address {
            anyhow::bail!("Cannot transfer to zero address");
        }

        // Check che il caller sia owner o approvato
        let caller_is_owner = caller == from_bytes;
        let caller_is_approved = if !caller_is_owner {
            Self::is_approved_for_all(
                contract_storage,
                storage,
                from,
                &Self::encode_address(&caller),
                gas_meter.as_deref_mut(),
            )?
        } else {
            false
        };

        if !caller_is_owner && !caller_is_approved {
            anyhow::bail!("Caller is not owner nor approved");
        }

        // Check balance sufficiente
        let from_balance = Self::balance_of(
            contract_storage,
            storage,
            from,
            id,
            gas_meter.as_deref_mut(),
        )?;
        if from_balance < amount {
            anyhow::bail!(
                "Insufficient balance: have {}, need {}",
                from_balance,
                amount
            );
        }

        Self::validate_receiver(
            storage,
            runtime,
            &to_bytes,
            &caller,
            &from_bytes,
            id,
            amount,
            data,
            &mut gas_meter,
        )?;

        // Esegui transfer
        if from_bytes != to_bytes {
            let from_slot = Self::calculate_balance_slot(&from_bytes, id);
            let new_from_balance = from_balance
                .checked_sub(amount)
                .ok_or_else(|| anyhow::anyhow!("Balance underflow"))?;
            contract_storage
                .sstore(
                    storage,
                    from_slot,
                    Self::u128_to_storage_value(new_from_balance),
                    gas_meter.as_deref_mut(),
                )
                .with_context(|| "Failed to update from balance")?;

            let to_balance = Self::balance_of(
                contract_storage,
                storage,
                &Self::encode_address(&to_bytes),
                id,
                gas_meter.as_deref_mut(),
            )?;
            let to_slot = Self::calculate_balance_slot(&to_bytes, id);
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
                .with_context(|| "Failed to update to balance")?;
        }

        // Emetti evento TransferSingle
        let event_system = EventSystem::new();
        Self::emit_transfer_single_event(
            &event_system,
            &contract_address,
            &caller,
            &from_bytes,
            &to_bytes,
            id,
            amount,
            gas_meter,
        );

        Ok(())
    }

    /// Safe batch transfer di più token
    ///
    /// or none. Atomicity is guaranteed by the ContractStorage overlay:
    /// - If any part fails the overlay is discarded and no modification is applied
    ///
    /// # Atomicità
    ///
    ///    riletture durante il loop che potrebbero leggere valori modificati
    ///    fallisce immediatamente e l'overlay viene scartato
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per ottenere caller e contract address
    /// * `ids` - Array di ID dei token
    /// * `amounts` - Array di quantità da trasferire
    /// * `data` - Dati aggiuntivi da passare al receiver
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Ok(()) se successo, errore altrimenti
    ///
    /// # Note
    pub fn safe_batch_transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        from: &str,
        to: &str,
        ids: &[u128],
        amounts: &[u128],
        data: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if BaseContract::is_paused(contract_storage, storage, gas_meter.as_deref_mut())? {
            anyhow::bail!("Contract is paused");
        }

        if ids.len() != amounts.len() {
            anyhow::bail!("ids and amounts arrays must have the same length");
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

        // Check che il receiver non sia l'address zero
        let zero_address = [0u8; 32];
        if to_bytes == zero_address {
            anyhow::bail!("Cannot transfer to zero address");
        }

        // Check che il caller sia owner o approvato
        let caller_is_owner = caller == from_bytes;
        let caller_is_approved = if !caller_is_owner {
            Self::is_approved_for_all(
                contract_storage,
                storage,
                from,
                &Self::encode_address(&caller),
                gas_meter.as_deref_mut(),
            )?
        } else {
            false
        };

        if !caller_is_owner && !caller_is_approved {
            anyhow::bail!("Caller is not owner nor approved");
        }

        let mut from_balances: Vec<u128> = Vec::with_capacity(ids.len());
        let mut to_balances: Vec<u128> = Vec::with_capacity(ids.len());

        for (id, amount) in ids.iter().zip(amounts.iter()) {
            // Check balance from sufficiente
            let from_balance = Self::balance_of(
                contract_storage,
                storage,
                from,
                *id,
                gas_meter.as_deref_mut(),
            )?;
            if from_balance < *amount {
                anyhow::bail!(
                    "Insufficient balance for token {}: have {}, need {}",
                    id,
                    from_balance,
                    amount
                );
            }
            from_balances.push(from_balance);

            // Pre-compute balance to per verificare overflow
            let to_balance = Self::balance_of(
                contract_storage,
                storage,
                &Self::encode_address(&to_bytes),
                *id,
                gas_meter.as_deref_mut(),
            )?;
            // Check overflow (il risultato non viene used qui, ma check che non ci sia overflow)
            let _ = to_balance.checked_add(*amount).ok_or_else(|| {
                anyhow::anyhow!("Balance overflow for token {}: would exceed u128::MAX", id)
            })?;
            to_balances.push(to_balance);
        }

        // Per batch, SMA standard richiede onSMABatchReceived
        Self::validate_batch_receiver(
            storage,
            runtime,
            &to_bytes,
            &caller,
            &from_bytes,
            ids,
            amounts,
            data,
            &mut gas_meter,
        )?;

        if from_bytes != to_bytes {
            for ((id, amount), (from_balance, to_balance)) in ids
                .iter()
                .zip(amounts.iter())
                .zip(from_balances.iter().zip(to_balances.iter()))
            {
                // Compute nuovi balance (già verificati in the fase 1)
                let new_from_balance = from_balance.checked_sub(*amount).ok_or_else(|| {
                    anyhow::anyhow!("Balance underflow for token {} (should not happen)", id)
                })?;

                let new_to_balance = to_balance.checked_add(*amount).ok_or_else(|| {
                    anyhow::anyhow!("Balance overflow for token {} (should not happen)", id)
                })?;

                let from_slot = Self::calculate_balance_slot(&from_bytes, *id);
                contract_storage
                    .sstore(
                        storage,
                        from_slot,
                        Self::u128_to_storage_value(new_from_balance),
                        gas_meter.as_deref_mut(),
                    )
                    .with_context(|| format!("Failed to update from balance for token {}", id))?;

                let to_slot = Self::calculate_balance_slot(&to_bytes, *id);
                contract_storage
                    .sstore(
                        storage,
                        to_slot,
                        Self::u128_to_storage_value(new_to_balance),
                        gas_meter.as_deref_mut(),
                    )
                    .with_context(|| format!("Failed to update to balance for token {}", id))?;
            }
        }

        // Emetti evento TransferBatch
        let event_system = EventSystem::new();
        Self::emit_transfer_batch_event(
            &event_system,
            &contract_address,
            &caller,
            &from_bytes,
            &to_bytes,
            ids,
            amounts,
            gas_meter,
        );

        Ok(())
    }

    // Public wrappers for testing - these are needed by external test modules
    #[doc(hidden)]
    pub fn test_calculate_balance_slot(owner: &[u8; 32], id: u128) -> u64 {
        Self::calculate_balance_slot(owner, id)
    }

    #[doc(hidden)]
    pub fn test_u128_to_storage_value(value: u128) -> Vec<u8> {
        Self::u128_to_storage_value(value)
    }
}
