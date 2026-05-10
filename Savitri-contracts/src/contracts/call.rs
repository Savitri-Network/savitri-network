//! Contract Calls: Chiamate a contratti
//!
//! - Function selector
//! - Esecuzione funzioni
//! - Return data handling

use crate::contracts::base::BaseContract;
use crate::contracts::evm_interpreter;
use crate::contracts::fee::ContractFee;
use crate::contracts::gas::GasMeter;
use crate::contracts::runtime::{CallFrame, Runtime};
use crate::contracts::storage::ContractStorage;
use anyhow::Result;
use hex;
use savitri_core::core::types::Account;
use savitri_storage::storage::contracts::ContractInfo;
use savitri_storage::storage::Storage;
use sha3::{Digest, Keccak256};
use std::collections::BTreeMap;

/// Errore di esecuzione of the contract
///
/// Supporta revert con messaggio personalizzato.
#[derive(Debug, Clone)]
pub enum ContractError {
    /// Revert without messaggio (revert semplice)
    Revert,
    /// Revert con messaggio personalizzato
    RevertWithMessage(String),
    /// Errore generico di esecuzione
    ExecutionError(String),
}

impl ContractError {
    /// Creates un errore di revert without messaggio
    pub fn revert() -> Self {
        ContractError::Revert
    }

    /// Creates un errore di revert con messaggio personalizzato
    pub fn revert_with_message(message: String) -> Self {
        ContractError::RevertWithMessage(message)
    }

    /// Creates un errore di esecuzione generico
    pub fn execution_error(message: String) -> Self {
        ContractError::ExecutionError(message)
    }

    /// Ottiene il messaggio dell'errore (se disponibile)
    pub fn message(&self) -> Option<&str> {
        match self {
            ContractError::Revert => None,
            ContractError::RevertWithMessage(msg) => Some(msg),
            ContractError::ExecutionError(msg) => Some(msg),
        }
    }
}

impl std::fmt::Display for ContractError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContractError::Revert => write!(f, "Contract execution reverted"),
            ContractError::RevertWithMessage(msg) => {
                write!(f, "Contract execution reverted: {}", msg)
            }
            ContractError::ExecutionError(msg) => write!(f, "Contract execution error: {}", msg),
        }
    }
}

impl std::error::Error for ContractError {}

/// Transazione di chiamata
pub struct CallTransaction {
    pub contract_address: String,
    pub function_selector: [u8; 4],
    pub calldata: Vec<u8>,
    pub caller: String,
    pub value: u128, // Amount of tokens to transfer with the call (for payable functions)
}

impl CallTransaction {
    pub fn new(
        contract_address: String,
        function_signature: &str,
        calldata: Vec<u8>,
        caller: String,
    ) -> Self {
        Self::new_with_value(contract_address, function_signature, calldata, caller, 0)
    }

    pub fn new_with_value(
        contract_address: String,
        function_signature: &str,
        calldata: Vec<u8>,
        caller: String,
        value: u128,
    ) -> Self {
        let function_selector = Self::calculate_selector(function_signature);
        Self {
            contract_address,
            function_selector,
            calldata,
            caller,
            value,
        }
    }

    /// Compute il function selector
    /// Formula: keccak256(function_signature)[0:4]
    ///
    /// es. "transfer(address,uint256)" o "balanceOf(address)"
    ///
    /// # Arguments
    ///
    /// # Returns
    ///
    /// # Example
    /// ```
    /// use savitri_node::contracts::call::CallTransaction;
    ///
    /// let selector = CallTransaction::calculate_selector("transfer(address,uint256)");
    /// // selector sarà i primi 4 bytes di keccak256("transfer(address,uint256)")
    /// ```
    pub fn calculate_selector(function_signature: &str) -> [u8; 4] {
        let mut hasher = Keccak256::new();
        hasher.update(function_signature.as_bytes());
        let hash = hasher.finalize();

        // Prendi i primi 4 bytes of the hash
        let mut selector = [0u8; 4];
        selector.copy_from_slice(&hash[0..4]);
        selector
    }

    pub fn execute(
        &self,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        self.execute_with_overlay(storage, runtime, gas_meter, None)
    }

    pub fn execute_with_overlay(
        &self,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
        mut overlay: Option<&mut BTreeMap<Vec<u8>, Account>>,
    ) -> Result<Vec<u8>, String> {
        // Salva il gas iniziale per calcolare il gas consumato anche in caso di errore
        let gas_before = gas_meter.gas_used();
        // 1. Decodifica contract address da stringa hex a bytes (32 bytes)
        let contract_address = self
            .decode_contract_address()
            .map_err(|e| format!("Invalid contract address: {}", e))?;

        const MAX_CONTRACT_INFO_SIZE: usize = 4 * 1024 * 1024;
        let contract_info = storage
            .get_contract(&contract_address)
            .map_err(|e| format!("Failed to load contract from storage: {}", e))?
            .ok_or_else(|| format!("Contract not found at address {}", self.contract_address))
            .and_then(|raw| {
                if raw.len() > MAX_CONTRACT_INFO_SIZE {
                    return Err(format!(
                        "Contract info data too large: {} bytes (max {})",
                        raw.len(),
                        MAX_CONTRACT_INFO_SIZE
                    ));
                }
                bincode::deserialize::<ContractInfo>(&raw)
                    .map_err(|e| format!("Failed to decode contract info: {}", e))
            })?;

        if contract_info.code.is_empty() {
            return Err("Contract has no bytecode".to_string());
        }

        // 4. Decodifica caller address da stringa hex a bytes (32 bytes)
        let caller_address = self
            .decode_caller_address()
            .map_err(|e| format!("Invalid caller address: {}", e))?;

        // 5. Prepara calldata completo (function selector + args)
        let mut full_calldata = Vec::with_capacity(4 + self.calldata.len());
        full_calldata.extend_from_slice(&self.function_selector);
        full_calldata.extend_from_slice(&self.calldata);

        // 6. Ottieni gas rimanente dal gas meter
        let gas_remaining = gas_meter.gas_remaining();

        let mut contract_storage = ContractStorage::new(contract_address.to_vec())
            .map_err(|e| format!("Failed to create contract storage: {}", e))?;

        // Per ora usiamo uno snapshot vuoto - sarà implementato quando avremo storage root
        let storage_snapshot = [0u8; 64];

        if runtime.call_depth() > 0 {
            runtime
                .check_reentrancy(&contract_address)
                .map_err(|e| format!("Re-entrancy protection: {}", e))?;
        }

        // 10. Creates call frame
        let current_depth = runtime.call_depth();
        let call_frame = CallFrame {
            contract_address,
            caller: caller_address,
            value: self.value, // Value transfer supported
            calldata: full_calldata,
            return_data: vec![],
            gas_remaining,
            depth: current_depth as u8,
            storage_snapshot,
        };

        // 11. Push call frame nel runtime
        // La check re-entrancy è già fatta sopra e anche in push_frame() come doppio controllo
        runtime
            .push_frame(call_frame)
            .map_err(|e| format!("Failed to push call frame: {}", e))?;

        // 11a. Execute value transfer if value > 0
        if self.value > 0 {
            // Get caller account from overlay or storage
            let caller_key = caller_address.as_slice();
            let mut caller_account = if let Some(overlay_mut) = overlay.as_ref() {
                overlay_mut.get(caller_key).cloned().unwrap_or_else(|| {
                    storage
                        .get_account(caller_key)
                        .ok()
                        .flatten()
                        .and_then(|raw| Account::decode(&raw).ok())
                        .unwrap_or_default()
                })
            } else {
                storage
                    .get_account(caller_key)
                    .ok()
                    .flatten()
                    .and_then(|raw| Account::decode(&raw).ok())
                    .unwrap_or_default()
            };

            // Check if caller has sufficient balance
            if caller_account.balance < self.value {
                // Pop frame and return error
                let _ = runtime.pop_frame();
                return Err(format!(
                    "Insufficient balance for value transfer: caller has {}, needs {}",
                    caller_account.balance, self.value
                ));
            }

            // Deduct value from caller
            caller_account
                .debit(self.value)
                .map_err(|e| format!("Failed to deduct value from caller: {}", e))?;

            // Get contract account
            let contract_key = contract_address.as_slice();
            let mut contract_account = storage
                .get_account(contract_key)
                .ok()
                .flatten()
                .and_then(|raw| Account::decode(&raw).ok())
                .unwrap_or_default();

            // Add value to contract
            contract_account
                .credit(self.value)
                .map_err(|e| format!("Failed to add value to contract: {}", e))?;

            // Update accounts in overlay or storage
            if let Some(overlay_mut) = overlay.as_mut() {
                overlay_mut.insert(caller_key.to_vec(), caller_account);
                overlay_mut.insert(contract_key.to_vec(), contract_account);
            } else {
                storage
                    .put_account(caller_key, &caller_account.encode())
                    .map_err(|e| format!("Failed to update caller account: {}", e))?;
                storage
                    .put_account(contract_key, &contract_account.encode())
                    .map_err(|e| format!("Failed to update contract account: {}", e))?;
            }
        }

        // 12. Match function selector e esegui funzione con gestione errori
        // Se l'esecuzione fallisce, l'errore viene propagato e lo stato viene ripristinato
        let execution_result = Self::execute_function_with_selector(
            &contract_info,
            &self.function_selector,
            &self.calldata,
            &mut contract_storage,
            storage,
            runtime,
            gas_meter,
            &caller_address,
            self.value,
        );

        // 12a. Compute il gas consumato (anche se l'esecuzione è fallita)
        let gas_after = gas_meter.gas_used();
        let gas_consumed = gas_after.saturating_sub(gas_before);

        // 12a.1. T4.9.2: Commit contract storage overlay after execution
        // Commit the contract storage overlay to the database so it's included in state root
        let storage_overlay = contract_storage.overlay();
        if !storage_overlay.is_empty() {
            storage
                .commit_contract_storage_overlay(&contract_address, storage_overlay)
                .map_err(|e| format!("Failed to commit contract storage overlay: {}", e))?;

            // Update contract storage root in ContractInfo
            let new_storage_root = contract_storage
                .compute_storage_root(storage)
                .map_err(|e| format!("Failed to compute storage root after execution: {}", e))?;

            // Update ContractInfo with new storage root
            let mut updated_contract_info = contract_info;
            updated_contract_info.storage_root = new_storage_root.to_vec();
            let encoded_contract = bincode::serialize(&updated_contract_info)
                .map_err(|e| format!("Failed to encode updated contract info: {}", e))?;
            storage
                .put_contract(&contract_address, &encoded_contract)
                .map_err(|e| {
                    format!(
                        "Failed to update contract info with new storage root: {}",
                        e
                    )
                })?;
        }

        // 12b. Compute e deduce il fee dal caller basandosi sul gas consumato (T4.8.1)
        // Il fee viene dedotto anche se l'esecuzione fallisce (prevenzione DoS, come in Ethereum)
        if let Some(overlay_mut) = overlay.as_mut() {
            if gas_consumed > 0 {
                let contract_fee = ContractFee::default();
                let caller_key = caller_address.as_slice();

                // Usa la funzione completa per calcolare e dedurre il fee
                match contract_fee.calculate_and_deduct_fee_from_caller_with_gas(
                    gas_consumed,
                    caller_key,
                    storage,
                    overlay_mut,
                    None, // Usa il gas_price di default
                ) {
                    Ok(_fee_amount) => {
                        // Fee dedotto con successo
                    }
                    Err(e) => {
                        // Pop il frame se necessario prima di ritornare l'errore
                        let _ = runtime.pop_frame();
                        return Err(e);
                    }
                }
            }
        }

        // 12c. Gestisci il risultato dell'esecuzione
        let return_data = match execution_result {
            Ok(data) => data,
            Err(err) => {
                // Error propagation: l'errore viene propagato al caller
                // Il call frame viene rimosso automaticamente dal call stack
                // Lo stato viene ripristinato tramite storage snapshot (quando implementato)
                // NOTA: Il fee è già stato dedotto sopra anche in caso di errore

                // Pop il frame fallito dal call stack
                let _ = runtime.pop_frame();

                return Err(format!(
                    "Contract {} execution failed: {}",
                    hex::encode(contract_address),
                    err
                ));
            }
        };

        // Il return data viene già salvato nel frame durante l'esecuzione
        if let Some(frame) = runtime.pop_frame() {
            // Check che il return data corrisponda (per debugging)
            if frame.return_data != return_data {
                // Log warning ma non fallisce (il return_data è già corretto)
                eprintln!(
                    "Warning: return_data mismatch in call frame (expected: {:?}, got: {:?})",
                    hex::encode(&return_data),
                    hex::encode(&frame.return_data)
                );
            }
        }

        Self::validate_return_data(&return_data)?;
        Ok(return_data)
    }

    ///
    /// Gestisce:
    ///
    /// # Arguments
    /// * `calldata` - Dati di chiamata (args, without selector)
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per l'esecuzione
    /// * `gas_meter` - Gas meter per tracciare il consumo di gas
    ///
    /// # Returns
    ///
    /// # Errors
    /// - Se il max call depth è stato superato
    /// - Se il gas è insufficiente
    pub fn call_contract(
        target_contract: [u8; 32],
        function_selector: [u8; 4],
        calldata: Vec<u8>,
        gas_limit: Option<u64>,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        let caller_contract = runtime.current_contract_address().ok_or_else(|| {
            "Cross-contract call can only be made from a contract execution context".to_string()
        })?;

        runtime
            .check_reentrancy(&target_contract)
            .map_err(|e| format!("Re-entrancy protection: {}", e))?;

        let target_info = storage
            .get_contract(&target_contract)
            .map_err(|e| format!("Failed to load target contract from storage: {}", e))?
            .ok_or_else(|| {
                format!(
                    "Target contract not found at address {}",
                    hex::encode(target_contract)
                )
            })
            .and_then(|raw| {
                const MAX_CONTRACT_INFO_SIZE: usize = 4 * 1024 * 1024;
                if raw.len() > MAX_CONTRACT_INFO_SIZE {
                    return Err(format!(
                        "Target contract info data too large: {} bytes (max {})",
                        raw.len(),
                        MAX_CONTRACT_INFO_SIZE
                    ));
                }
                bincode::deserialize::<ContractInfo>(&raw)
                    .map_err(|e| format!("Failed to decode target contract info: {}", e))
            })?;

        if target_info.code.is_empty() {
            return Err(format!(
                "Target contract has no bytecode at address {}",
                hex::encode(target_contract)
            ));
        }

        // 5. Prepara calldata completo (function selector + args)
        let mut full_calldata = Vec::with_capacity(4 + calldata.len());
        full_calldata.extend_from_slice(&function_selector);
        full_calldata.extend_from_slice(&calldata);

        let available_gas = gas_meter.gas_remaining();
        let gas_to_forward = match gas_limit {
            Some(limit) => {
                // Check che il limit richiesto non superi il gas disponibile
                if limit > available_gas {
                    return Err(format!(
                        "Insufficient gas: requested {} but only {} available",
                        limit, available_gas
                    ));
                }
                limit
            }
            None => {
                // Se non specificato, passa tutto il gas disponibile
                available_gas
            }
        };

        // 7. Consuma gas per CALL (base cost)
        gas_meter
            .consume_call(Some(full_calldata.len()))
            .map_err(|e| format!("Failed to consume CALL gas: {}", e))?;

        let mut target_storage = ContractStorage::new(target_contract.to_vec())
            .map_err(|e| format!("Failed to create target contract storage: {}", e))?;

        // Per ora usiamo uno snapshot vuoto - sarà implementato quando avremo storage root
        let storage_snapshot = [0u8; 64];

        // 10. Creates call frame per la cross-contract call
        let call_frame = CallFrame {
            contract_address: target_contract,
            caller: caller_contract,
            value: 0, // Cross-contract calls don't support value transfer in this implementation
            calldata: full_calldata.clone(),
            return_data: vec![],
            gas_remaining: gas_to_forward,
            depth: 0, // Sarà impostato automaticamente da push_frame
            storage_snapshot,
        };

        // 11. Context switching: push of the nuovo frame nel call stack
        // La check re-entrancy è già fatta in push_frame()
        runtime
            .push_frame(call_frame)
            .map_err(|e| format!("Failed to push call frame for cross-contract call: {}", e))?;

        // Il gas meter principale continua a tracciare il gas totale
        let mut target_gas_meter = GasMeter::new(gas_to_forward);

        // Se l'esecuzione fallisce, l'errore viene propagato e lo stato viene ripristinato
        let return_data = match Self::execute_function_with_selector(
            &target_info,
            &function_selector,
            &calldata,
            &mut target_storage,
            storage,
            runtime,
            &mut target_gas_meter,
            &caller_contract,
            0,
        ) {
            Ok(data) => data,
            Err(err) => {
                // Error propagation: l'errore viene propagato al caller
                // Il call frame viene rimosso automaticamente dal call stack
                // Lo stato viene ripristinato tramite storage snapshot (quando implementato)

                // Pop il frame fallito dal call stack
                let _ = runtime.pop_frame();

                return Err(format!(
                    "Contract {} execution failed: {}",
                    hex::encode(target_contract),
                    err
                ));
            }
        };

        let gas_consumed_by_target = target_gas_meter.gas_used();
        if gas_consumed_by_target > 0 {
            gas_meter
                .consume(gas_consumed_by_target)
                .map_err(|e| format!("Failed to consume gas used by target contract: {}", e))?;
        }

        // 15. Pop call frame e ottieni il frame aggiornato
        let popped_frame = runtime.pop_frame().ok_or_else(|| {
            "Call stack corrupted: frame not found after cross-contract call".to_string()
        })?;

        if popped_frame.contract_address != target_contract {
            return Err(format!(
                "Call stack corrupted: expected frame for contract {}, got {}",
                hex::encode(target_contract),
                hex::encode(popped_frame.contract_address)
            ));
        }

        Self::validate_return_data(&return_data)?;

        // 18. Return data propagation: il return data viene automaticamente propagato
        Ok(return_data)
    }

    /// Runs una cross-contract call con address come stringa hex (helper function)
    ///
    ///
    /// # Arguments
    /// * `calldata` - Dati di chiamata (args, without selector)
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per l'esecuzione
    /// * `gas_meter` - Gas meter per tracciare il consumo di gas
    ///
    /// # Returns
    pub fn call_contract_hex(
        target_contract_hex: &str,
        function_signature: &str,
        calldata: Vec<u8>,
        gas_limit: Option<u64>,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        // Decodifica l'address da hex
        let address_hex = target_contract_hex
            .strip_prefix("0x")
            .unwrap_or(target_contract_hex);
        let address_bytes = hex::decode(address_hex)
            .map_err(|e| format!("Failed to decode target contract address: {}", e))?;

        if address_bytes.len() != 32 {
            return Err(format!(
                "Target contract address must be 32 bytes, got {}",
                address_bytes.len()
            ));
        }

        let mut target_address = [0u8; 32];
        target_address.copy_from_slice(&address_bytes);

        // Compute il function selector
        let function_selector = Self::calculate_selector(function_signature);

        // Chiama la funzione principale
        Self::call_contract(
            target_address,
            function_selector,
            calldata,
            gas_limit,
            storage,
            runtime,
            gas_meter,
        )
    }

    /// Runs un revert con messaggio personalizzato
    ///
    /// con un messaggio personalizzato. L'errore viene propagato al caller.
    ///
    /// # Arguments
    /// * `message` - Messaggio di errore da propagare
    ///
    /// # Returns
    pub fn revert_with_message(message: &str) -> Result<Vec<u8>, String> {
        Err(ContractError::revert_with_message(message.to_string()).to_string())
    }

    /// Runs un revert semplice (without messaggio)
    ///
    /// without un messaggio personalizzato. L'errore viene propagato al caller.
    ///
    /// # Returns
    pub fn revert() -> Result<Vec<u8>, String> {
        Err(ContractError::revert().to_string())
    }

    /// Runs una funzione con il selector specificato
    ///
    ///
    /// **Error Propagation**: Se l'esecuzione fallisce, l'errore viene propagato al caller
    /// e lo stato viene ripristinato (tramite storage snapshot quando implementato).
    ///
    /// # Arguments
    /// * `calldata` - Dati di chiamata (args, without selector)
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `runtime` - Runtime per l'esecuzione
    /// * `gas_meter` - Gas meter per tracciare il consumo di gas
    ///
    /// # Returns
    fn execute_function_with_selector(
        contract_info: &ContractInfo,
        function_selector: &[u8; 4],
        calldata: &[u8],
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
        caller: &[u8; 32],
        value: u128,
    ) -> Result<Vec<u8>, String> {
        if function_selector == &[0; 4] {
            return Err("Invalid function selector: cannot be zero".to_string());
        }

        // Compute selector per funzioni BaseContract note
        let owner_selector = Self::calculate_selector("owner()");
        let version_selector = Self::calculate_selector("version()");
        let paused_selector = Self::calculate_selector("paused()");
        let transfer_ownership_selector = Self::calculate_selector("transfer_ownership(address)");
        let pause_selector = Self::calculate_selector("pause()");
        let unpause_selector = Self::calculate_selector("unpause()");

        // T5.1.1: Compute selector per funzioni SAVITRI-20
        let total_supply_selector = Self::calculate_selector("totalSupply()");
        let balance_of_selector = Self::calculate_selector("balanceOf(address)");
        let balance_of_bytes32_selector = Self::calculate_selector("balanceOf(bytes32)");
        let transfer_selector = Self::calculate_selector("transfer(address,uint256)");
        let transfer_coin_bytes32_selector =
            Self::calculate_selector("transferCoin(bytes32,uint256)");
        let faucet_mint_bytes32_selector = Self::calculate_selector("faucetMint(bytes32,uint256)");
        let approve_selector = Self::calculate_selector("approve(address,uint256)");
        let transfer_from_selector =
            Self::calculate_selector("transferFrom(address,address,uint256)");
        let allowance_selector = Self::calculate_selector("allowance(address,address)");
        let name_selector = Self::calculate_selector("name()");
        let symbol_selector = Self::calculate_selector("symbol()");

        // T5.2.1: Compute selector per funzioni SAVITRI-721
        let balance_of_721_selector = Self::calculate_selector("balanceOf(address)");
        let owner_of_selector = Self::calculate_selector("ownerOf(uint256)");
        let transfer_from_721_selector =
            Self::calculate_selector("transferFrom(address,address,uint256)");
        let approve_721_selector = Self::calculate_selector("approve(address,uint256)");
        let safe_transfer_from_selector =
            Self::calculate_selector("safeTransferFrom(address,address,uint256)");
        let safe_transfer_from_with_data_selector =
            Self::calculate_selector("safeTransferFrom(address,address,uint256,bytes)");
        let token_uri_selector = Self::calculate_selector("tokenURI(uint256)");
        let set_token_uri_selector = Self::calculate_selector("setTokenURI(uint256,string)");

        // Match function selector e esegui funzione
        let return_data = if *function_selector == owner_selector {
            Self::execute_owner(contract_storage, storage, gas_meter)
                .map_err(|e| format!("owner() execution failed: {}", e))?
        } else if *function_selector == version_selector {
            Self::execute_version(contract_storage, storage, gas_meter)
                .map_err(|e| format!("version() execution failed: {}", e))?
        } else if *function_selector == paused_selector {
            Self::execute_paused(contract_storage, storage, gas_meter)
                .map_err(|e| format!("paused() execution failed: {}", e))?
        } else if *function_selector == transfer_ownership_selector {
            // transfer_ownership(address) - state-changing function
            Self::execute_transfer_ownership(
                contract_storage,
                storage,
                runtime,
                calldata,
                gas_meter,
            )
            .map_err(|e| format!("transfer_ownership(address) execution failed: {}", e))?
        } else if *function_selector == pause_selector {
            // pause() - state-changing function
            Self::execute_pause(contract_storage, storage, runtime, gas_meter)
                .map_err(|e| format!("pause() execution failed: {}", e))?
        } else if *function_selector == unpause_selector {
            // unpause() - state-changing function
            Self::execute_unpause(contract_storage, storage, runtime, gas_meter)
                .map_err(|e| format!("unpause() execution failed: {}", e))?
        } else if *function_selector == total_supply_selector {
            Self::execute_total_supply(contract_storage, storage, gas_meter)
                .map_err(|e| format!("totalSupply() execution failed: {}", e))?
        } else if *function_selector == name_selector {
            Self::execute_name(contract_storage, storage, gas_meter)
                .map_err(|e| format!("name() execution failed: {}", e))?
        } else if *function_selector == symbol_selector {
            Self::execute_symbol(contract_storage, storage, gas_meter)
                .map_err(|e| format!("symbol() execution failed: {}", e))?
        } else if *function_selector == balance_of_selector
            || *function_selector == balance_of_bytes32_selector
        {
            Self::execute_balance_of(contract_storage, storage, calldata, gas_meter)
                .map_err(|e| format!("balanceOf(address|bytes32) execution failed: {}", e))?
        } else if *function_selector == transfer_selector
            || *function_selector == transfer_coin_bytes32_selector
        {
            // transfer(address,uint256) / transferCoin(bytes32,uint256) - state-changing function
            Self::execute_transfer(contract_storage, storage, runtime, calldata, gas_meter)
                .map_err(|e| {
                    format!(
                        "transfer(address,uint256)|transferCoin(bytes32,uint256) execution failed: {}",
                        e
                    )
                })?
        } else if *function_selector == faucet_mint_bytes32_selector {
            // faucetMint(bytes32,uint256) - state-changing helper for testnet/devnet
            Self::execute_faucet_mint(contract_storage, storage, runtime, calldata, gas_meter)
                .map_err(|e| format!("faucetMint(bytes32,uint256) execution failed: {}", e))?
        } else if *function_selector == approve_selector {
            // approve(address,uint256) - state-changing function
            Self::execute_approve(contract_storage, storage, runtime, calldata, gas_meter)
                .map_err(|e| format!("approve(address,uint256) execution failed: {}", e))?
        } else if *function_selector == transfer_from_selector {
            // transferFrom(address,address,uint256) - state-changing function
            Self::execute_transfer_from(contract_storage, storage, runtime, calldata, gas_meter)
                .map_err(|e| {
                    format!(
                        "transferFrom(address,address,uint256) execution failed: {}",
                        e
                    )
                })?
        } else if *function_selector == allowance_selector {
            Self::execute_allowance(contract_storage, storage, calldata, gas_meter)
                .map_err(|e| format!("allowance(address,address) execution failed: {}", e))?
        } else if *function_selector == balance_of_721_selector {
            // Nota: balanceOf ha lo stesso selector per SAVITRI-20 e SAVITRI-721
            Self::execute_balance_of_721(contract_storage, storage, calldata, gas_meter)
                .map_err(|e| format!("balanceOf(address) SAVITRI-721 execution failed: {}", e))?
        } else if *function_selector == owner_of_selector {
            Self::execute_owner_of(contract_storage, storage, calldata, gas_meter)
                .map_err(|e| format!("ownerOf(uint256) execution failed: {}", e))?
        } else if *function_selector == transfer_from_721_selector {
            // transferFrom(address,address,uint256) - state-changing function SAVITRI-721
            // Nota: transferFrom ha lo stesso selector per SAVITRI-20 e SAVITRI-721
            // La differenza è nel comportamento (token vs NFT)
            Self::execute_transfer_from_721(contract_storage, storage, runtime, calldata, gas_meter)
                .map_err(|e| {
                    format!(
                        "transferFrom(address,address,uint256) SAVITRI-721 execution failed: {}",
                        e
                    )
                })?
        } else if *function_selector == approve_721_selector {
            // approve(address,uint256) - state-changing function SAVITRI-721
            // Nota: approve ha lo stesso selector per SAVITRI-20 e SAVITRI-721
            Self::execute_approve_721(contract_storage, storage, runtime, calldata, gas_meter)
                .map_err(|e| {
                    format!(
                        "approve(address,uint256) SAVITRI-721 execution failed: {}",
                        e
                    )
                })?
        } else if *function_selector == safe_transfer_from_selector {
            // safeTransferFrom(address,address,uint256) - state-changing function SAVITRI-721
            Self::execute_safe_transfer_from(
                contract_storage,
                storage,
                runtime,
                calldata,
                gas_meter,
            )
            .map_err(|e| {
                format!(
                    "safeTransferFrom(address,address,uint256) execution failed: {}",
                    e
                )
            })?
        } else if *function_selector == safe_transfer_from_with_data_selector {
            // safeTransferFrom(address,address,uint256,bytes) - state-changing function SAVITRI-721
            Self::execute_safe_transfer_from_with_data(
                contract_storage,
                storage,
                runtime,
                calldata,
                gas_meter,
            )
            .map_err(|e| {
                format!(
                    "safeTransferFrom(address,address,uint256,bytes) execution failed: {}",
                    e
                )
            })?
        } else if *function_selector == token_uri_selector {
            Self::execute_token_uri(contract_storage, storage, calldata, gas_meter)
                .map_err(|e| format!("tokenURI(uint256) execution failed: {}", e))?
        } else if *function_selector == set_token_uri_selector {
            // setTokenURI(uint256,string) - state-changing function SAVITRI-721
            Self::execute_set_token_uri(contract_storage, storage, runtime, calldata, gas_meter)
                .map_err(|e| format!("setTokenURI(uint256,string) execution failed: {}", e))?
        } else {
            let mut full_calldata = Vec::with_capacity(4 + calldata.len());
            full_calldata.extend_from_slice(function_selector);
            full_calldata.extend_from_slice(calldata);
            return evm_interpreter::execute(
                contract_info,
                contract_storage,
                storage,
                runtime,
                gas_meter,
                *caller,
                value,
                &full_calldata,
            )
            .map_err(|e| {
                format!(
                    "Function selector 0x{} native-dispatch miss, VM path failed: {}",
                    hex::encode(function_selector),
                    e
                )
            });
        };

        Ok(return_data)
    }

    /// Runs owner() - view function
    ///
    fn execute_owner(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        let owner = BaseContract::owner(contract_storage, storage, Some(gas_meter))
            .map_err(|e| format!("Failed to get owner: {}", e))?;
        Ok(owner.to_vec())
    }

    /// Runs version() - view function
    ///
    fn execute_version(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        let version = BaseContract::version(contract_storage, storage, Some(gas_meter))
            .map_err(|e| format!("Failed to get version: {}", e))?;
        Ok(Self::encode_uint256(version as u128))
    }

    /// Runs paused() - view function
    ///
    fn execute_paused(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        // BaseContract non ha una funzione paused() pubblica, usiamo is_paused()
        let paused = BaseContract::is_paused(contract_storage, storage, Some(gas_meter))
            .map_err(|e| format!("Failed to check paused status: {}", e))?;
        Ok(Self::encode_bool(paused))
    }

    /// Runs transfer_ownership(address) - state-changing function
    ///
    fn execute_transfer_ownership(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        // Decodifica nuovo owner da calldata (primi 32 bytes)
        let new_owner = Self::decode_address(calldata)
            .map_err(|e| format!("Failed to decode new_owner from calldata: {}", e))?;

        // Esegui transfer_ownership
        let success = BaseContract::transfer_ownership(
            contract_storage,
            storage,
            runtime,
            &new_owner,
            Some(gas_meter),
        )
        .map_err(|e| format!("transfer_ownership failed: {}", e))?;

        Ok(Self::encode_bool(success))
    }

    /// Runs pause() - state-changing function
    ///
    fn execute_pause(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        let success = BaseContract::pause(contract_storage, storage, runtime, Some(gas_meter))
            .map_err(|e| format!("pause() failed: {}", e))?;
        Ok(Self::encode_bool(success))
    }

    /// Runs unpause() - state-changing function
    ///
    fn execute_unpause(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        let success = BaseContract::unpause(contract_storage, storage, runtime, Some(gas_meter))
            .map_err(|e| format!("unpause() failed: {}", e))?;
        Ok(Self::encode_bool(success))
    }

    // ============================================
    // T5.1.1: SAVITRI-20 Function Wrappers
    // ============================================

    /// Runs totalSupply() - view function
    ///
    fn execute_total_supply(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;
        let total_supply = SAVITRI20::total_supply(contract_storage, storage, Some(gas_meter))
            .map_err(|e| format!("totalSupply() failed: {}", e))?;
        Ok(Self::encode_uint256(total_supply))
    }

    fn execute_name(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;
        let name = SAVITRI20::name(contract_storage, storage, Some(gas_meter))
            .map_err(|e| format!("name() failed: {}", e))?;
        Ok(Self::encode_string(&name))
    }

    fn execute_symbol(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;
        let symbol = SAVITRI20::symbol(contract_storage, storage, Some(gas_meter))
            .map_err(|e| format!("symbol() failed: {}", e))?;
        Ok(Self::encode_string(&symbol))
    }

    /// Runs balanceOf(address) - view function
    ///
    fn execute_balance_of(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;

        // Decodifica address (primi 32 bytes)
        let address = Self::decode_address(calldata)?;
        let address_hex = Self::address_to_hex(&address);

        let balance =
            SAVITRI20::balance_of(contract_storage, storage, &address_hex, Some(gas_meter))
                .map_err(|e| format!("balanceOf() failed: {}", e))?;
        Ok(Self::encode_uint256(balance))
    }

    /// Runs transfer(address,uint256) - state-changing function
    ///
    fn execute_transfer(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;

        // Decodifica parametri: address (32 bytes) + uint256 (32 bytes)
        if calldata.len() < 64 {
            return Err(
                "Calldata too short for transfer(address,uint256): expected at least 64 bytes"
                    .to_string(),
            );
        }

        let to_address = Self::decode_address(&calldata[0..32])?;
        let amount = Self::decode_uint256(&calldata[32..64])?;

        let to_hex = Self::address_to_hex(&to_address);

        SAVITRI20::transfer(
            contract_storage,
            storage,
            runtime,
            &to_hex,
            amount,
            Some(gas_meter),
        )
        .map_err(|e| format!("transfer() failed: {}", e))?;

        Ok(Self::encode_bool(true))
    }

    /// Runs faucetMint(bytes32,uint256) - state-changing function (dev/test helper)
    ///
    fn execute_faucet_mint(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;

        // Decodifica parametri: bytes32 (32 bytes) + uint256 (32 bytes)
        if calldata.len() < 64 {
            return Err(
                "Calldata too short for faucetMint(bytes32,uint256): expected at least 64 bytes"
                    .to_string(),
            );
        }

        let to_address = Self::decode_address(&calldata[0..32])?;
        let amount = Self::decode_uint256(&calldata[32..64])?;
        let to_hex = Self::address_to_hex(&to_address);

        SAVITRI20::mint(
            contract_storage,
            storage,
            runtime,
            &to_hex,
            amount,
            Some(gas_meter),
        )
        .map_err(|e| format!("faucetMint() failed: {}", e))?;

        Ok(Vec::new())
    }

    /// Runs approve(address,uint256) - state-changing function
    ///
    fn execute_approve(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;

        // Decodifica parametri: address (32 bytes) + uint256 (32 bytes)
        if calldata.len() < 64 {
            return Err(
                "Calldata too short for approve(address,uint256): expected at least 64 bytes"
                    .to_string(),
            );
        }

        let spender_address = Self::decode_address(&calldata[0..32])?;
        let amount = Self::decode_uint256(&calldata[32..64])?;

        let spender_hex = Self::address_to_hex(&spender_address);

        SAVITRI20::approve(
            contract_storage,
            storage,
            runtime,
            &spender_hex,
            amount,
            Some(gas_meter),
        )
        .map_err(|e| format!("approve() failed: {}", e))?;

        Ok(Self::encode_bool(true))
    }

    /// Runs transferFrom(address,address,uint256) - state-changing function
    ///
    fn execute_transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;

        // Decodifica parametri: address (32 bytes) + address (32 bytes) + uint256 (32 bytes)
        if calldata.len() < 96 {
            return Err("Calldata too short for transferFrom(address,address,uint256): expected at least 96 bytes".to_string());
        }

        let from_address = Self::decode_address(&calldata[0..32])?;
        let to_address = Self::decode_address(&calldata[32..64])?;
        let amount = Self::decode_uint256(&calldata[64..96])?;

        let from_hex = Self::address_to_hex(&from_address);
        let to_hex = Self::address_to_hex(&to_address);

        SAVITRI20::transfer_from(
            contract_storage,
            storage,
            runtime,
            &from_hex,
            &to_hex,
            amount,
            Some(gas_meter),
        )
        .map_err(|e| format!("transferFrom() failed: {}", e))?;

        Ok(Self::encode_bool(true))
    }

    /// Runs allowance(address,address) - view function
    ///
    fn execute_allowance(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI20;

        // Decodifica parametri: address (32 bytes) + address (32 bytes)
        if calldata.len() < 64 {
            return Err(
                "Calldata too short for allowance(address,address): expected at least 64 bytes"
                    .to_string(),
            );
        }

        let owner_address = Self::decode_address(&calldata[0..32])?;
        let spender_address = Self::decode_address(&calldata[32..64])?;

        let owner_hex = Self::address_to_hex(&owner_address);
        let spender_hex = Self::address_to_hex(&spender_address);

        let allowance = SAVITRI20::allowance(
            contract_storage,
            storage,
            &owner_hex,
            &spender_hex,
            Some(gas_meter),
        )
        .map_err(|e| format!("allowance() failed: {}", e))?;
        Ok(Self::encode_uint256(allowance))
    }

    // ============================================
    // T5.2.1: SAVITRI-721 Function Wrappers
    // ============================================

    /// Runs balanceOf(address) - view function SAVITRI-721
    ///
    /// Nota: balanceOf ha lo stesso selector per SAVITRI-20 e SAVITRI-721.
    fn execute_balance_of_721(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI721;

        // Decodifica address (primi 32 bytes)
        let address = Self::decode_address(calldata)?;
        let address_hex = Self::address_to_hex(&address);

        let balance =
            SAVITRI721::balance_of(contract_storage, storage, &address_hex, Some(gas_meter))
                .map_err(|e| format!("balanceOf() SAVITRI-721 failed: {}", e))?;
        // Codifica u64 come uint256
        Ok(Self::encode_uint256(balance as u128))
    }

    /// Runs ownerOf(uint256) - view function SAVITRI-721
    ///
    fn execute_owner_of(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI721;

        // Decodifica tokenId (primi 32 bytes come uint256)
        let token_id = Self::decode_uint256_to_u64(calldata)?;

        let owner_hex = SAVITRI721::owner_of(contract_storage, storage, token_id, Some(gas_meter))
            .map_err(|e| format!("ownerOf() failed: {}", e))?;

        // Decodifica address da hex string
        let owner_address = Self::decode_address_from_hex(&owner_hex)?;
        Ok(owner_address.to_vec())
    }

    /// Runs transferFrom(address,address,uint256) - state-changing function SAVITRI-721
    ///
    /// Nota: transferFrom ha lo stesso selector per SAVITRI-20 e SAVITRI-721.
    fn execute_transfer_from_721(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI721;

        // Decodifica parametri: address (32 bytes) + address (32 bytes) + uint256 (32 bytes)
        if calldata.len() < 96 {
            return Err("Calldata too short for transferFrom(address,address,uint256) SAVITRI-721: expected at least 96 bytes".to_string());
        }

        let from_address = Self::decode_address(&calldata[0..32])?;
        let to_address = Self::decode_address(&calldata[32..64])?;
        let token_id = Self::decode_uint256_to_u64(&calldata[64..96])?;

        let from_hex = Self::address_to_hex(&from_address);
        let to_hex = Self::address_to_hex(&to_address);

        SAVITRI721::transfer_from(
            contract_storage,
            storage,
            runtime,
            &from_hex,
            &to_hex,
            token_id,
            Some(gas_meter),
        )
        .map_err(|e| format!("transferFrom() SAVITRI-721 failed: {}", e))?;

        Ok(Self::encode_bool(true))
    }

    /// Runs approve(address,uint256) - state-changing function SAVITRI-721
    ///
    /// Nota: approve ha lo stesso selector per SAVITRI-20 e SAVITRI-721.
    fn execute_approve_721(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI721;

        // Decodifica parametri: address (32 bytes) + uint256 (32 bytes)
        if calldata.len() < 64 {
            return Err("Calldata too short for approve(address,uint256) SAVITRI-721: expected at least 64 bytes".to_string());
        }

        let approved_address = Self::decode_address(&calldata[0..32])?;
        let token_id = Self::decode_uint256_to_u64(&calldata[32..64])?;

        let approved_hex = Self::address_to_hex(&approved_address);

        SAVITRI721::approve(
            contract_storage,
            storage,
            runtime,
            &approved_hex,
            token_id,
            Some(gas_meter),
        )
        .map_err(|e| format!("approve() SAVITRI-721 failed: {}", e))?;

        Ok(Self::encode_bool(true))
    }

    /// Runs safeTransferFrom(address,address,uint256) - state-changing function SAVITRI-721
    ///
    fn execute_safe_transfer_from(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI721;

        // Decodifica parametri: address (32 bytes) + address (32 bytes) + uint256 (32 bytes)
        if calldata.len() < 96 {
            return Err("Calldata too short for safeTransferFrom(address,address,uint256): expected at least 96 bytes".to_string());
        }

        let from_address = Self::decode_address(&calldata[0..32])?;
        let to_address = Self::decode_address(&calldata[32..64])?;
        let token_id = Self::decode_uint256_to_u64(&calldata[64..96])?;

        let from_hex = Self::address_to_hex(&from_address);
        let to_hex = Self::address_to_hex(&to_address);

        SAVITRI721::safe_transfer_from(
            contract_storage,
            storage,
            runtime,
            &from_hex,
            &to_hex,
            token_id,
            Some(gas_meter),
        )
        .map_err(|e| format!("safeTransferFrom() failed: {}", e))?;

        Ok(Self::encode_bool(true))
    }

    /// Decodifica bytes da calldata ABI-encoded
    ///
    /// In formato ABI, bytes è codificato come:
    /// - Offset (32 bytes) che punta ai dati
    /// - Length (32 bytes) che indica la lunghezza
    /// - data (padded a multipli di 32 bytes)
    fn decode_bytes(calldata: &[u8], offset_bytes: &[u8]) -> Result<Vec<u8>, String> {
        // Decodifica offset (32 bytes, big-endian)
        if offset_bytes.len() < 32 {
            return Err("Offset bytes too short for bytes offset".to_string());
        }
        let mut offset_bytes_array = [0u8; 32];
        offset_bytes_array.copy_from_slice(&offset_bytes[0..32]);
        let offset = u64::from_be_bytes(offset_bytes_array[24..32].try_into().unwrap()) as usize;

        // Check che l'offset sia valido
        if offset >= calldata.len() {
            return Err(format!(
                "Bytes offset {} exceeds calldata length {}",
                offset,
                calldata.len()
            ));
        }

        // Leggi length (32 bytes all'offset)
        if calldata.len() < offset + 32 {
            return Err(format!(
                "Calldata too short for bytes length at offset {}",
                offset
            ));
        }
        let mut length_bytes = [0u8; 32];
        length_bytes.copy_from_slice(&calldata[offset..offset + 32]);
        let length = u64::from_be_bytes(length_bytes[24..32].try_into().unwrap()) as usize;

        // Leggi data (all'offset + 32)
        let data_offset = offset + 32;
        if calldata.len() < data_offset + length {
            return Err(format!(
                "Calldata too short for bytes data: need {}, have {}",
                data_offset + length,
                calldata.len()
            ));
        }

        let bytes_data = &calldata[data_offset..data_offset + length];
        Ok(bytes_data.to_vec())
    }

    /// Runs safeTransferFrom(address,address,uint256,bytes) - state-changing function SAVITRI-721
    ///
    fn execute_safe_transfer_from_with_data(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI721;

        // Decodifica parametri: address (32 bytes) + address (32 bytes) + uint256 (32 bytes) + bytes offset (32 bytes)
        // bytes è codificato come offset che punta ai dati
        if calldata.len() < 128 {
            return Err("Calldata too short for safeTransferFrom(address,address,uint256,bytes): expected at least 128 bytes".to_string());
        }

        let from_address = Self::decode_address(&calldata[0..32])?;
        let to_address = Self::decode_address(&calldata[32..64])?;
        let token_id = Self::decode_uint256_to_u64(&calldata[64..96])?;

        // Decodifica bytes (offset è all'offset 96)
        let data = Self::decode_bytes(calldata, &calldata[96..128])?;

        let from_hex = Self::address_to_hex(&from_address);
        let to_hex = Self::address_to_hex(&to_address);

        SAVITRI721::safe_transfer_from_with_data(
            contract_storage,
            storage,
            runtime,
            &from_hex,
            &to_hex,
            token_id,
            &data,
            Some(gas_meter),
        )
        .map_err(|e| {
            format!(
                "safeTransferFrom(address,address,uint256,bytes) failed: {}",
                e
            )
        })?;

        Ok(Self::encode_bool(true))
    }

    /// Runs tokenURI(uint256) - view function SAVITRI-721
    ///
    fn execute_token_uri(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI721;

        // Decodifica tokenId (primi 32 bytes come uint256)
        let token_id = Self::decode_uint256_to_u64(calldata)?;

        let uri = SAVITRI721::token_uri(contract_storage, storage, token_id, Some(gas_meter))
            .map_err(|e| format!("tokenURI() failed: {}", e))?;

        // Codifica stringa in formato ABI
        Ok(Self::encode_string(&uri))
    }

    /// Decodifica una stringa da calldata ABI-encoded
    ///
    /// In formato ABI, una stringa è codificata come:
    /// - offset (32 bytes) che punta alla posizione dei dati
    /// - length (32 bytes) alla posizione indicata dall'offset
    /// - data (padded a multipli di 32 bytes)
    fn decode_string(calldata: &[u8], offset_bytes: &[u8]) -> Result<String, String> {
        // Decodifica offset (32 bytes, big-endian)
        if offset_bytes.len() < 32 {
            return Err("Offset bytes too short for string offset".to_string());
        }
        let mut offset_bytes_array = [0u8; 32];
        offset_bytes_array.copy_from_slice(&offset_bytes[0..32]);
        let offset = u64::from_be_bytes(offset_bytes_array[24..32].try_into().unwrap()) as usize;

        // Check che l'offset sia valido
        if offset >= calldata.len() {
            return Err(format!(
                "String offset {} exceeds calldata length {}",
                offset,
                calldata.len()
            ));
        }

        // Leggi length (32 bytes all'offset)
        if calldata.len() < offset + 32 {
            return Err(format!(
                "Calldata too short for string length at offset {}",
                offset
            ));
        }
        let mut length_bytes = [0u8; 32];
        length_bytes.copy_from_slice(&calldata[offset..offset + 32]);
        let length = u64::from_be_bytes(length_bytes[24..32].try_into().unwrap()) as usize;

        // Leggi data (all'offset + 32)
        let data_offset = offset + 32;
        if calldata.len() < data_offset + length {
            return Err(format!(
                "Calldata too short for string data: need {}, have {}",
                data_offset + length,
                calldata.len()
            ));
        }

        let string_bytes = &calldata[data_offset..data_offset + length];
        let string = std::str::from_utf8(string_bytes)
            .map_err(|e| format!("Invalid UTF-8 in string: {}", e))?;

        Ok(string.to_string())
    }

    /// Runs setTokenURI(uint256,string) - state-changing function SAVITRI-721
    ///
    fn execute_set_token_uri(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        calldata: &[u8],
        gas_meter: &mut GasMeter,
    ) -> Result<Vec<u8>, String> {
        use crate::contracts::standards::SAVITRI721;

        // Decodifica parametri: uint256 (32 bytes) + string offset (32 bytes)
        if calldata.len() < 64 {
            return Err(
                "Calldata too short for setTokenURI(uint256,string): expected at least 64 bytes"
                    .to_string(),
            );
        }

        // tokenId (primi 32 bytes)
        let token_id = Self::decode_uint256_to_u64(&calldata[0..32])?;

        // URI offset (secondi 32 bytes)
        let uri = Self::decode_string(calldata, &calldata[32..64])?;

        SAVITRI721::set_token_uri(
            contract_storage,
            storage,
            runtime,
            token_id,
            &uri,
            Some(gas_meter),
        )
        .map_err(|e| format!("setTokenURI() failed: {}", e))?;

        Ok(Self::encode_bool(true))
    }

    /// Decodifica un address da stringa hex
    ///
    /// Converte una stringa hex (es. "0x1234...") in [u8; 32].
    fn decode_address_from_hex(hex_str: &str) -> Result<[u8; 32], String> {
        let hex_clean = hex_str.strip_prefix("0x").unwrap_or(hex_str);
        let bytes =
            hex::decode(hex_clean).map_err(|e| format!("Failed to decode hex address: {}", e))?;

        if bytes.len() != 32 {
            return Err(format!(
                "Invalid address length: expected 32 bytes, got {}",
                bytes.len()
            ));
        }

        let mut address = [0u8; 32];
        address.copy_from_slice(&bytes);
        Ok(address)
    }

    /// Decodifica un address da calldata ABI-encoded
    ///
    /// In formato ABI, un address è codificato come 32 bytes (left-padded con zeri).
    ///
    /// # Arguments
    ///
    /// # Returns
    /// Address decodificato (32 bytes) o errore
    fn decode_address(calldata: &[u8]) -> Result<[u8; 32], String> {
        if calldata.len() < 32 {
            return Err(format!(
                "Calldata too short for address: expected at least 32 bytes, got {}",
                calldata.len()
            ));
        }

        let mut address = [0u8; 32];
        address.copy_from_slice(&calldata[0..32]);
        Ok(address)
    }

    /// Decodifica un uint256 da calldata ABI-encoded
    ///
    /// In formato ABI, un uint256 è codificato come 32 bytes in big-endian.
    ///
    /// # Arguments
    ///
    /// # Returns
    /// Uint256 decodificato (u128) o errore
    fn decode_uint256(calldata: &[u8]) -> Result<u128, String> {
        if calldata.len() < 32 {
            return Err(format!(
                "Calldata too short for uint256: expected at least 32 bytes, got {}",
                calldata.len()
            ));
        }

        // Estrai i 32 bytes (big-endian)
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&calldata[0..32]);

        // Prendi gli ultimi 16 bytes significativi
        let mut u128_bytes = [0u8; 16];
        u128_bytes.copy_from_slice(&bytes[16..32]);
        Ok(u128::from_be_bytes(u128_bytes))
    }

    /// Converte un address [u8; 32] in stringa hex
    fn address_to_hex(address: &[u8; 32]) -> String {
        format!("0x{}", hex::encode(address))
    }

    /// Codifica un bool in formato ABI (32 bytes)
    ///
    /// In formato ABI, un bool è codificato come 32 bytes:
    /// - true = 0x00...01 (ultimo byte è 1)
    ///
    /// # Arguments
    /// * `value` - Valore bool da codificare
    ///
    /// # Returns
    /// Bool codificato come 32 bytes
    fn encode_bool(value: bool) -> Vec<u8> {
        let mut encoded = vec![0u8; 32];
        if value {
            encoded[31] = 1;
        }
        encoded
    }

    /// Codifica un uint256 in formato ABI (32 bytes, big-endian)
    ///
    /// In formato ABI, un uint256 è codificato come 32 bytes in big-endian.
    ///
    /// # Arguments
    /// * `value` - Valore u128 da codificare (trattato come uint256)
    ///
    /// # Returns
    /// Uint256 codificato come 32 bytes (big-endian, left-padded con zeri)
    fn encode_uint256(value: u128) -> Vec<u8> {
        let mut encoded = vec![0u8; 32];
        let bytes = value.to_be_bytes();
        // Copia i 16 bytes di u128 negli ultimi 16 bytes of the risultato (left-padding)
        encoded[16..32].copy_from_slice(&bytes);
        encoded
    }

    /// Decodifica un uint256 in u64 (per tokenId)
    ///
    /// Estrae un u64 da un uint256 ABI-encoded.
    ///
    /// # Arguments
    ///
    /// # Returns
    /// Uint256 decodificato come u64 o errore
    fn decode_uint256_to_u64(calldata: &[u8]) -> Result<u64, String> {
        if calldata.len() < 32 {
            return Err(format!(
                "Calldata too short for uint256: expected at least 32 bytes, got {}",
                calldata.len()
            ));
        }

        // Estrai i 32 bytes (big-endian)
        let mut bytes = [0u8; 32];
        bytes.copy_from_slice(&calldata[0..32]);

        // Converti in u64 (gli ultimi 8 bytes significativi)
        let mut u64_bytes = [0u8; 8];
        u64_bytes.copy_from_slice(&bytes[24..32]);
        Ok(u64::from_be_bytes(u64_bytes))
    }

    /// Codifica una stringa in formato ABI
    ///
    /// In formato ABI, una stringa è codificata come:
    /// - Offset (32 bytes) che punta ai dati
    /// - Length (32 bytes) che indica la lunghezza
    /// - Data (padded a multipli di 32 bytes)
    ///
    /// Per semplicità, per return data usiamo un formato più semplice:
    /// - Length (32 bytes) + Data (padded)
    ///
    /// # Arguments
    /// * `value` - Stringa da codificare
    ///
    /// # Returns
    /// Stringa codificata in formato ABI
    fn encode_string(value: &str) -> Vec<u8> {
        let bytes = value.as_bytes();
        let len = bytes.len();

        // Compute padding per allineare a 32 bytes
        let padded_len = ((len + 31) / 32) * 32;

        let mut encoded = Vec::with_capacity(32 + padded_len);

        // Aggiungi length (32 bytes, big-endian)
        let mut len_bytes = vec![0u8; 32];
        let len_u256 = len as u128;
        len_bytes[16..32].copy_from_slice(&len_u256.to_be_bytes());
        encoded.extend_from_slice(&len_bytes);

        // Aggiungi data con padding
        encoded.extend_from_slice(bytes);
        encoded.resize(32 + padded_len, 0);

        encoded
    }

    ///
    /// - Vuoto (per funzioni che non ritornano valori)
    /// - 32 bytes (per valori singoli: address, uint256, bool)
    /// - Multiplo di 32 bytes (per valori multipli o bytes dinamici)
    ///
    /// # Arguments
    ///
    /// # Returns
    fn validate_return_data(return_data: &[u8]) -> Result<(), String> {
        // Return data vuoto è valido (funzioni che non ritornano valori)
        if return_data.is_empty() {
            return Ok(());
        }

        if return_data.len() % 32 != 0 {
            return Err(format!(
                "Invalid return data length: {} bytes (must be multiple of 32)",
                return_data.len()
            ));
        }

        // Limit massimo per return data (prevent DoS)
        const MAX_RETURN_DATA_SIZE: usize = 1024 * 1024; // 1 MB
        if return_data.len() > MAX_RETURN_DATA_SIZE {
            return Err(format!(
                "Return data too large: {} bytes (max: {} bytes)",
                return_data.len(),
                MAX_RETURN_DATA_SIZE
            ));
        }

        Ok(())
    }

    /// Codifica return data per diversi tipi
    ///
    /// per diversi tipi di valori. Supporta:
    /// - `ReturnType::Void` - Nessun valore (return data vuoto)
    /// - `ReturnType::Address` - Address (32 bytes)
    /// - `ReturnType::Uint256` - Uint256 (32 bytes)
    /// - `ReturnType::Bool` - Bool (32 bytes)
    /// - `ReturnType::Bytes` - Bytes (dynamic, multiplo di 32 bytes)
    ///
    /// # Arguments
    /// * `return_type` - Tipo di return data
    /// * `value` - Valore da codificare (opzionale, dipende dal tipo)
    ///
    /// # Returns
    /// Return data codificato o errore
    pub fn encode_return_data(
        return_type: ReturnType,
        value: Option<&[u8]>,
    ) -> Result<Vec<u8>, String> {
        match return_type {
            ReturnType::Void => Ok(vec![]),
            ReturnType::Address => {
                if let Some(addr) = value {
                    if addr.len() != 32 {
                        return Err(format!("Address must be 32 bytes, got {}", addr.len()));
                    }
                    Ok(addr.to_vec())
                } else {
                    Err("Address value required for ReturnType::Address".to_string())
                }
            }
            ReturnType::Uint256 => {
                if let Some(val_bytes) = value {
                    if val_bytes.len() <= 16 {
                        // Tratta come u128 e codifica
                        let mut val_u128 = [0u8; 16];
                        val_u128[16 - val_bytes.len()..].copy_from_slice(val_bytes);
                        let val = u128::from_be_bytes(val_u128);
                        Ok(Self::encode_uint256(val))
                    } else {
                        Err("Uint256 value too large (max 16 bytes for u128)".to_string())
                    }
                } else {
                    Err("Uint256 value required for ReturnType::Uint256".to_string())
                }
            }
            ReturnType::Bool => {
                if let Some(val_bytes) = value {
                    if val_bytes.len() == 1 {
                        let bool_val = val_bytes[0] != 0;
                        Ok(Self::encode_bool(bool_val))
                    } else {
                        Err("Bool value must be 1 byte".to_string())
                    }
                } else {
                    Err("Bool value required for ReturnType::Bool".to_string())
                }
            }
            ReturnType::Bytes => {
                if let Some(bytes) = value {
                    // Padding a multiplo di 32 bytes
                    let mut encoded = bytes.to_vec();
                    let padding = (32 - (encoded.len() % 32)) % 32;
                    encoded.extend(vec![0u8; padding]);
                    Ok(encoded)
                } else {
                    Ok(vec![]) // Bytes vuoto
                }
            }
        }
    }

    /// Decodifica contract address da stringa hex a bytes (32 bytes)
    fn decode_contract_address(&self) -> Result<[u8; 32], String> {
        let address_hex = self
            .contract_address
            .strip_prefix("0x")
            .unwrap_or(&self.contract_address);
        let address_bytes = hex::decode(address_hex)
            .map_err(|e| format!("Failed to decode contract address: {}", e))?;

        if address_bytes.len() != 32 {
            return Err(format!(
                "Contract address must be 32 bytes, got {}",
                address_bytes.len()
            ));
        }

        let mut address = [0u8; 32];
        address.copy_from_slice(&address_bytes);
        Ok(address)
    }

    /// Decodifica caller address da stringa hex a bytes (32 bytes)
    fn decode_caller_address(&self) -> Result<[u8; 32], String> {
        let caller_hex = self.caller.strip_prefix("0x").unwrap_or(&self.caller);
        let caller_bytes = hex::decode(caller_hex)
            .map_err(|e| format!("Failed to decode caller address: {}", e))?;

        if caller_bytes.len() != 32 {
            return Err(format!(
                "Caller address must be 32 bytes, got {}",
                caller_bytes.len()
            ));
        }

        let mut caller = [0u8; 32];
        caller.copy_from_slice(&caller_bytes);
        Ok(caller)
    }

    /// Check se è una view function (non modifica stato)
    ///
    ///
    /// # Note
    ///
    /// # Returns
    pub fn is_view_function(&self) -> bool {
        // List di function selector comuni per view functions (ERC20, ERC721, etc.)

        // balanceOf(address) - ERC20/ERC721
        let balance_of_selector = Self::calculate_selector("balanceOf(address)");
        // balanceOf(bytes32) - RealCoin (bytes32-account token)
        let balance_of_bytes32_selector = Self::calculate_selector("balanceOf(bytes32)");

        // totalSupply() - ERC20/ERC721
        let total_supply_selector = Self::calculate_selector("totalSupply()");

        // owner() - BaseContract/ERC721
        let owner_selector = Self::calculate_selector("owner()");

        // version() - BaseContract
        let version_selector = Self::calculate_selector("version()");

        // paused() - BaseContract
        let paused_selector = Self::calculate_selector("paused()");

        // allowance(address,address) - ERC20
        let allowance_selector = Self::calculate_selector("allowance(address,address)");

        // name() - ERC20/ERC721
        let name_selector = Self::calculate_selector("name()");

        // symbol() - ERC20/ERC721
        let symbol_selector = Self::calculate_selector("symbol()");

        // decimals() - ERC20
        let decimals_selector = Self::calculate_selector("decimals()");

        // tokenURI(uint256) - ERC721
        let token_uri_selector = Self::calculate_selector("tokenURI(uint256)");

        self.function_selector == balance_of_selector
            || self.function_selector == balance_of_bytes32_selector
            || self.function_selector == total_supply_selector
            || self.function_selector == owner_selector
            || self.function_selector == version_selector
            || self.function_selector == paused_selector
            || self.function_selector == allowance_selector
            || self.function_selector == name_selector
            || self.function_selector == symbol_selector
            || self.function_selector == decimals_selector
            || self.function_selector == token_uri_selector

        // Se non contiene SSTORE, è una view function.
    }
}

/// Tipo di return data per funzioni
///
/// Definisce i tipi di return supportati per le funzioni of contracts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReturnType {
    /// Nessun valore di ritorno (void)
    Void,
    /// Address (32 bytes)
    Address,
    /// Uint256 (32 bytes)
    Uint256,
    /// Bool (32 bytes, ABI-encoded)
    Bool,
    /// Bytes (dynamic, multiplo di 32 bytes)
    Bytes,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_selector() {
        // Test con signature comune: transfer(address,uint256)
        let selector1 = CallTransaction::calculate_selector("transfer(address,uint256)");

        let selector2 = CallTransaction::calculate_selector("transfer(address,uint256)");
        assert_eq!(selector1, selector2, "Selector deve essere deterministico");

        let selector3 = CallTransaction::calculate_selector("balanceOf(address)");
        assert_ne!(
            selector1, selector3,
            "Signature diverse devono produrre selector diversi"
        );

        assert_eq!(
            selector1.len(),
            4,
            "Selector deve essere esattamente 4 bytes"
        );

        // Check che il selector non sia tutto zero (probabilità molto bassa)
        assert_ne!(selector1, [0; 4], "Selector must not be all zero");
    }

    #[test]
    fn test_calculate_selector_known_values() {
        // Test con alcune signature comuni per verificare che il calcolo sia corretto

        // transfer(address,uint256) - selector comune ERC20
        let transfer_selector = CallTransaction::calculate_selector("transfer(address,uint256)");

        // balanceOf(address) - selector comune ERC20
        let balance_selector = CallTransaction::calculate_selector("balanceOf(address)");

        // approve(address,uint256) - selector comune ERC20
        let approve_selector = CallTransaction::calculate_selector("approve(address,uint256)");

        assert_ne!(transfer_selector, balance_selector);
        assert_ne!(transfer_selector, approve_selector);
        assert_ne!(balance_selector, approve_selector);

        assert_ne!(transfer_selector, [0; 4]);
        assert_ne!(balance_selector, [0; 4]);
        assert_ne!(approve_selector, [0; 4]);
    }

    #[test]
    fn test_call_transaction_new() {
        // Test che CallTransaction::new calcoli correttamente il selector
        let call = CallTransaction::new(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "transfer(address,uint256)",
            vec![1, 2, 3, 4],
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
        );

        // Check che il selector sia stato calcolato
        assert_ne!(
            call.function_selector, [0; 4],
            "Selector deve essere calcolato"
        );

        // Check che il selector corrisponda a quello calcolato manualmente
        let expected_selector = CallTransaction::calculate_selector("transfer(address,uint256)");
        assert_eq!(
            call.function_selector, expected_selector,
            "Selector deve corrispondere"
        );

        // Check che il value sia 0 di default
        assert_eq!(call.value, 0, "Value di default deve essere 0");
    }

    #[test]
    fn test_call_transaction_new_with_value() {
        // Test che CallTransaction::new_with_value imposti correttamente il value
        let call = CallTransaction::new_with_value(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "transfer(address,uint256)",
            vec![1, 2, 3, 4],
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
            1000000, // 1 million tokens
        );

        // Check che il selector sia stato calcolato
        assert_ne!(
            call.function_selector, [0; 4],
            "Selector deve essere calcolato"
        );

        // Check che il value sia impostato correttamente
        assert_eq!(
            call.value, 1000000,
            "Value deve essere impostato correttamente"
        );
    }

    #[test]
    fn test_is_view_function() {
        // Test che is_view_function() identifichi correttamente le view functions
        let balance_call = CallTransaction::new(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "balanceOf(address)",
            vec![],
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
        );
        assert!(
            balance_call.is_view_function(),
            "balanceOf deve essere view function"
        );

        let owner_call = CallTransaction::new(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "owner()",
            vec![],
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
        );
        assert!(
            owner_call.is_view_function(),
            "owner deve essere view function"
        );

        // Test che is_view_function() identifichi correttamente le state-changing functions
        let transfer_call = CallTransaction::new(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "transfer(address,uint256)",
            vec![],
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
        );
        assert!(
            !transfer_call.is_view_function(),
            "transfer NON deve essere view function"
        );

        let approve_call = CallTransaction::new(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "approve(address,uint256)",
            vec![],
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
        );
        assert!(
            !approve_call.is_view_function(),
            "approve NON deve essere view function"
        );
    }

    #[test]
    fn test_decode_addresses() {
        let call = CallTransaction::new(
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "transfer(address,uint256)",
            vec![],
            "0xabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
        );

        let contract_addr = call.decode_contract_address().unwrap();
        assert_eq!(contract_addr.len(), 32);

        let caller_addr = call.decode_caller_address().unwrap();
        assert_eq!(caller_addr.len(), 32);

        let call_no_prefix = CallTransaction::new(
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string(),
            "transfer(address,uint256)",
            vec![],
            "abcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcdefabcd".to_string(),
        );

        let contract_addr2 = call_no_prefix.decode_contract_address().unwrap();
        assert_eq!(contract_addr2.len(), 32);

        let caller_addr2 = call_no_prefix.decode_caller_address().unwrap();
        assert_eq!(caller_addr2.len(), 32);
    }

    #[test]
    fn test_decode_addresses_invalid() {
        // Test con indirizzo invalido (lunghezza sbagliata)
        let call = CallTransaction::new(
            "0x1234".to_string(), // Troppo corto
            "transfer(address,uint256)",
            vec![],
            "0xabcdef".to_string(), // Troppo corto
        );

        assert!(call.decode_contract_address().is_err());
        assert!(call.decode_caller_address().is_err());
    }
}
