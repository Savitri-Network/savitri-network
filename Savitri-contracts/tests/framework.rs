//! Contract Testing Framework
//!
//! Framework completo per testing smart contracts con:
//! - TestEnvironment per gestire setup/teardown
//! - Helpers per deployment
//! - Helpers per chiamate a contratti
//! - Snapshot/restore per isolamento test
//! - Check stato contratti

use anyhow::{Context, Result};
use savitri_contracts::{
    contracts::{
        base::{BaseContract, SLOT_OWNER, SLOT_VERSION},
        call::{CallTransaction, ContractError},
        deploy::DeployTransaction,
        gas::GasMeter,
        runtime::Runtime,
        storage::ContractStorage,
    },
    storage::Storage,
};
use std::path::PathBuf;

fn create_test_storage(prefix: &str) -> Result<(Storage, PathBuf)> {
    use tempfile::TempDir;

    let tmp_dir = TempDir::new().context("Failed to create temp directory")?;
    let path = tmp_dir.path().join(prefix);
    std::fs::create_dir_all(&path).context("Failed to create test directory")?;

    let storage = Storage::new(path.clone()).context("Failed to create storage")?;
    let path_buf = path.to_path_buf();
    std::mem::forget(tmp_dir);

    Ok((storage, path_buf))
}

#[derive(Clone, Debug)]
pub struct StorageSnapshot {
    pub contract_address: [u8; 32],
    pub storage_root: Vec<u8>,
    pub storage_slots: std::collections::BTreeMap<u64, Vec<u8>>,
    pub overlay: std::collections::BTreeMap<u64, Vec<u8>>,
    pub account_balance: Option<u128>,
    pub account_nonce: Option<u64>,
}

/// Ambiente di test per contratti
///
/// necessarie per testare smart contracts. Fornisce metodi per:
/// - Deploy di contratti
/// - Chiamate a contratti
/// - Snapshot/restore per isolamento test
/// - Check stato contratti
pub struct TestEnvironment {
    /// Storage layer per persistenza
    pub storage: Storage,
    /// Directory temporanea (mantenuta per cleanup)
    _tmp_dir: PathBuf,
    /// Runtime per esecuzione contratti
    pub runtime: Runtime,
    /// Gas meter per tracking gas consumption
    pub gas_meter: GasMeter,
    /// Gas limit di default
    pub gas_limit: u64,
    /// Block timestamp deterministico
    pub block_timestamp: u64,
    /// Nonce corrente per deployment
    pub deploy_nonce: u64,
}

impl TestEnvironment {
    ///
    /// # Arguments
    /// * `prefix` - Prefisso per directory temporanea
    /// * `gas_limit` - Gas limit di default (default: 10_000_000)
    /// * `block_timestamp` - Timestamp of the blocco (default: 0)
    ///
    /// # Returns
    /// Nuovo TestEnvironment configurato
    pub fn new(prefix: &str, gas_limit: Option<u64>, block_timestamp: Option<u64>) -> Result<Self> {
        let (storage, tmp_dir) =
            create_test_storage(prefix).context("Failed to create test storage")?;

        let gas_limit = gas_limit.unwrap_or(10_000_000);
        let block_timestamp = block_timestamp.unwrap_or(0);

        let runtime = Runtime::new(
            std::collections::BTreeMap::new(),
            gas_limit,
            64, // max_call_depth
            block_timestamp,
        );

        let gas_meter = GasMeter::new(gas_limit);

        Ok(Self {
            storage,
            _tmp_dir: tmp_dir,
            runtime,
            gas_meter,
            gas_limit,
            block_timestamp,
            deploy_nonce: 0,
        })
    }

    ///
    /// # Arguments
    /// * `prefix` - Prefisso per directory temporanea
    /// * `deployer_balance` - Balance iniziale of the deployer
    /// * `gas_limit` - Gas limit di default
    /// * `block_timestamp` - Timestamp of the blocco
    ///
    /// # Returns
    /// Nuovo TestEnvironment con account deployer inizializzato
    pub fn with_deployer(
        prefix: &str,
        deployer_address: [u8; 32],
        deployer_balance: u128,
        gas_limit: Option<u64>,
        block_timestamp: Option<u64>,
    ) -> Result<Self> {
        let mut env = Self::new(prefix, gas_limit, block_timestamp)?;

        // Inizializza account deployer
        let mut deployer_account = Vec::with_capacity(24);
        deployer_account.extend_from_slice(&deployer_balance.to_le_bytes());
        deployer_account.extend_from_slice(&0u64.to_le_bytes());
        env.storage
            .put_account(&deployer_address, &deployer_account)
            .context("Failed to initialize deployer account")?;

        // Set deployer nel runtime
        env.runtime.set_caller(deployer_address);

        Ok(env)
    }

    /// Deploya a contract
    ///
    /// # Arguments
    /// * `bytecode` - Bytecode of the contract
    /// * `constructor_args` - Argomenti per il constructor
    ///
    /// # Returns
    pub fn deploy_contract(
        &mut self,
        deployer_address_hex: &str,
        bytecode: Vec<u8>,
        constructor_args: Vec<u8>,
    ) -> Result<[u8; 32]> {
        let deploy_tx = DeployTransaction::new(
            deployer_address_hex.to_string(),
            bytecode,
            constructor_args,
            self.deploy_nonce,
        );

        // Incrementa nonce per prossimo deployment
        self.deploy_nonce += 1;

        let mut gas_meter = GasMeter::new(self.gas_limit);

        // Esegui deployment
        let contract_address =
            deploy_tx.execute_constructor(&self.storage, &self.runtime, &mut gas_meter)?;

        // Consuma gas dal gas meter principale
        let gas_used = gas_meter.gas_used();
        self.gas_meter
            .consume_gas(gas_used)
            .map_err(|e| anyhow::anyhow!("Gas limit exceeded: {}", e))?;

        Ok(contract_address)
    }

    ///
    /// # Arguments
    /// * `constructor_args` - Argomenti per il constructor
    ///
    /// # Returns
    pub fn deploy_contract_hex(
        &mut self,
        deployer_address_hex: &str,
        bytecode_hex: &str,
        constructor_args: Vec<u8>,
    ) -> Result<[u8; 32]> {
        use hex;
        let bytecode = hex::decode(bytecode_hex.strip_prefix("0x").unwrap_or(bytecode_hex))
            .context("Failed to decode bytecode hex")?;

        self.deploy_contract(deployer_address_hex, bytecode, constructor_args)
    }

    /// Chiama una funzione view (read-only) di a contract
    ///
    /// # Arguments
    /// * `calldata` - Dati di chiamata
    ///
    /// # Returns
    pub fn call_view_function(
        &mut self,
        contract_address: [u8; 32],
        function_signature: &str,
        calldata: Vec<u8>,
        caller_address_hex: &str,
    ) -> Result<Vec<u8>> {
        let contract_address_hex = format!("0x{}", hex::encode(contract_address));

        let call_tx = CallTransaction::new(
            contract_address_hex,
            function_signature,
            calldata,
            caller_address_hex.to_string(),
        );

        // Creates storage per the contract
        let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;

        // Set caller nel runtime
        let caller_bytes = hex::decode(
            caller_address_hex
                .strip_prefix("0x")
                .unwrap_or(caller_address_hex),
        )?;
        if caller_bytes.len() != 32 {
            anyhow::bail!("Caller address must be 32 bytes");
        }
        let mut caller_array = [0u8; 32];
        caller_array.copy_from_slice(&caller_bytes);
        self.runtime.set_caller(caller_array);

        // Esegui chiamata
        let mut gas_meter = GasMeter::new(self.gas_limit);
        let result = call_tx.execute(
            &mut contract_storage,
            &self.storage,
            &self.runtime,
            &mut gas_meter,
        )?;

        // Consuma gas
        let gas_used = gas_meter.gas_used();
        self.gas_meter
            .consume_gas(gas_used)
            .map_err(|e| anyhow::anyhow!("Gas limit exceeded: {}", e))?;

        Ok(result)
    }

    /// Chiama una funzione che modifica lo stato
    ///
    /// # Arguments
    /// * `calldata` - Dati di chiamata
    /// * `value` - Valore in token trasferiti (0 per chiamate non-payable)
    ///
    /// # Returns
    pub fn call_state_function(
        &mut self,
        contract_address: [u8; 32],
        function_signature: &str,
        calldata: Vec<u8>,
        caller_address_hex: &str,
        value: u128,
    ) -> Result<Vec<u8>> {
        self.call_view_function(
            contract_address,
            function_signature,
            calldata,
            caller_address_hex,
        )
    }

    ///
    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn snapshot_contract_state(&self, contract_address: [u8; 32]) -> Result<StorageSnapshot> {
        let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;

        // Compute storage root
        let storage_root = contract_storage.compute_storage_root(&self.storage)?;

        // Salva overlay corrente
        let overlay = contract_storage.overlay().clone();

        // Leggiamo gli slot più comuni (0-1000) e quelli nell'overlay
        let mut storage_slots = std::collections::BTreeMap::new();

        for (slot, value) in overlay.iter() {
            storage_slots.insert(*slot, value.clone());
        }

        // Leggi slot comuni dal database (0-1000 per test)
        for slot in 0..1000u64 {
            if let Ok(Some(value)) = self
                .storage
                .get_contract_storage_slot(&contract_address, slot)
            {
                if !storage_slots.contains_key(&slot) {
                    storage_slots.insert(slot, value);
                }
            }
        }

        // Salva account state
        let account = self.storage.get_account(&contract_address)?;
        let account_balance = account.as_ref().map(|a| a.balance);
        let account_nonce = account.as_ref().map(|a| a.nonce);

        Ok(StorageSnapshot {
            contract_address,
            storage_root: storage_root.to_vec(),
            storage_slots,
            overlay,
            account_balance,
            account_nonce,
        })
    }

    ///
    ///
    /// # Arguments
    /// * `snapshot` - Snapshot da ripristinare
    ///
    /// # Note
    pub fn restore_contract_state(&mut self, snapshot: &StorageSnapshot) -> Result<()> {
        if !self.storage.contract_exists(&snapshot.contract_address)? {
            anyhow::bail!("Contract does not exist at address");
        }

        // Poi ripristina gli slot dallo snapshot
        for (slot, value) in snapshot.storage_slots.iter() {
            self.storage
                .put_contract_storage_slot(&snapshot.contract_address, *slot, value)?;
        }

        self.storage
            .update_contract_storage_root(&snapshot.contract_address, &snapshot.storage_root)?;

        // Ripristina account state se presente
        if let Some(balance) = snapshot.account_balance {
            if let Some(nonce) = snapshot.account_nonce {
                let mut account = Vec::with_capacity(24);
                account.extend_from_slice(&balance.to_le_bytes());
                account.extend_from_slice(&nonce.to_le_bytes());
                self.storage
                    .put_account(&snapshot.contract_address, &account)?;
            }
        }

        // Reset gas meter per isolamento test
        self.reset_gas_meter();

        Ok(())
    }

    ///
    /// # Arguments
    /// * `slot` - Slot da leggere
    ///
    /// # Returns
    pub fn get_contract_storage_slot(
        &mut self,
        contract_address: [u8; 32],
        slot: u64,
    ) -> Result<Vec<u8>> {
        let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;
        contract_storage.sload(&self.storage, slot, None)
    }

    ///
    /// # Arguments
    /// * `slot` - Slot da scrivere
    /// * `value` - Valore da scrivere (32 bytes)
    ///
    /// # Note
    pub fn set_contract_storage_slot(
        &mut self,
        contract_address: [u8; 32],
        slot: u64,
        value: Vec<u8>,
    ) -> Result<()> {
        if value.len() != 32 {
            anyhow::bail!("Storage value must be 32 bytes");
        }

        let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;

        // Leggi il vecchio valore per calcolare gas correctly
        let _old_value = contract_storage.sload(&self.storage, slot, None)?;

        // Scrivi il nuovo valore
        contract_storage.sstore(&self.storage, slot, value, None)?;

        // Committa l'overlay
        self.storage
            .commit_contract_storage_overlay(&contract_address, contract_storage.overlay())?;

        Ok(())
    }

    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn contract_exists(&self, contract_address: &[u8; 32]) -> Result<bool> {
        self.storage.contract_exists(contract_address)
    }

    /// Ottiene l'owner di a contract
    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn get_contract_owner(&mut self, contract_address: [u8; 32]) -> Result<[u8; 32]> {
        let owner_slot = SLOT_OWNER;
        let owner_bytes = self.get_contract_storage_slot(contract_address, owner_slot)?;

        if owner_bytes.len() != 32 {
            anyhow::bail!("Invalid owner address length");
        }

        let mut owner = [0u8; 32];
        owner.copy_from_slice(&owner_bytes);
        Ok(owner)
    }

    /// Ottiene la versione di a contract
    ///
    /// # Arguments
    ///
    /// # Returns
    /// Versione of the contract
    pub fn get_contract_version(&mut self, contract_address: [u8; 32]) -> Result<u64> {
        let version_slot = SLOT_VERSION;
        let version_bytes = self.get_contract_storage_slot(contract_address, version_slot)?;

        // Leggi u64 da 32 bytes (primi 8 bytes)
        let mut version_array = [0u8; 8];
        version_array.copy_from_slice(&version_bytes[0..8]);
        Ok(u64::from_be_bytes(version_array))
    }

    pub fn reset_gas_meter(&mut self) {
        self.gas_meter = GasMeter::new(self.gas_limit);
    }

    /// Ottiene il gas utilizzato
    pub fn gas_used(&self) -> u64 {
        self.gas_meter.gas_used()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_test_environment() -> Result<()> {
        let env = TestEnvironment::new("test-env", None, None)?;
        assert_eq!(env.gas_limit, 10_000_000);
        assert_eq!(env.block_timestamp, 0);
        assert_eq!(env.deploy_nonce, 0);
        Ok(())
    }

    #[test]
    fn test_create_test_environment_with_deployer() -> Result<()> {
        let deployer = [0xAA; 32];
        let env = TestEnvironment::with_deployer(
            "test-env-deployer",
            deployer,
            1_000_000_000_000_000,
            None,
            None,
        )?;

        // Check che l'account esista
        let account = env.storage.get_account(&deployer)?;
        assert!(account.is_some());
        assert_eq!(account.unwrap().balance, 1_000_000_000_000_000);

        Ok(())
    }
}
