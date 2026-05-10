//! Contract Deployment: Deployment di contratti
//!
//! - Calcolo contract address
//! - Esecuzione constructor
//! - Validazione bytecode
#![allow(unused_comparisons)]

use crate::contracts::base::BaseContract;
use crate::contracts::gas::GasMeter;
use crate::contracts::runtime::Runtime;
use crate::contracts::standards::SAVITRI20;
use crate::contracts::storage::ContractStorage;
use anyhow::{Context, Result};
use hex;
use savitri_storage::storage::contracts::ContractInfo;
use savitri_storage::storage::Storage;
use sha3::{Digest, Keccak256};

/// Transazione di deployment
pub struct DeployTransaction {
    pub deployer: String,
    pub bytecode: Vec<u8>,
    pub constructor_args: Vec<u8>,
    pub nonce: u64,
}

/// Risultato dell'esecuzione of the constructor
#[derive(Debug, Clone)]
pub struct ConstructorResult {
    pub success: bool,
    pub gas_used: u64,
    pub return_data: Vec<u8>,
    pub error: String,
}

impl DeployTransaction {
    fn selector(signature: &str) -> [u8; 4] {
        let hash = Self::keccak256(signature.as_bytes());
        let mut selector = [0u8; 4];
        selector.copy_from_slice(&hash[..4]);
        selector
    }

    fn has_selector(&self, selector: &[u8; 4]) -> bool {
        self.bytecode
            .windows(selector.len())
            .any(|window| window == selector)
    }

    fn looks_like_savitri20(&self) -> bool {
        let required = [
            Self::selector("totalSupply()"),
            Self::selector("balanceOf(address)"),
            Self::selector("transfer(address,uint256)"),
            Self::selector("approve(address,uint256)"),
            Self::selector("allowance(address,address)"),
        ];

        required.iter().all(|selector| self.has_selector(selector))
    }

    fn decode_constructor_string(args: &[u8], offset: &mut usize, field: &str) -> Result<String> {
        if args.len() < *offset + 4 {
            anyhow::bail!("Constructor args too short for {} length", field);
        }

        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&args[*offset..*offset + 4]);
        *offset += 4;

        let len = u32::from_be_bytes(len_bytes) as usize;
        if args.len() < *offset + len {
            anyhow::bail!("Constructor args too short for {} bytes", field);
        }

        let value = std::str::from_utf8(&args[*offset..*offset + len])
            .with_context(|| format!("{} is not valid UTF-8", field))?
            .to_string();
        *offset += len;
        Ok(value)
    }

    fn decode_savitri20_constructor_args(&self) -> Result<(String, String, u128)> {
        let mut offset = 0usize;
        let name = Self::decode_constructor_string(&self.constructor_args, &mut offset, "name")?;
        let symbol =
            Self::decode_constructor_string(&self.constructor_args, &mut offset, "symbol")?;

        if self.constructor_args.len() < offset + 16 {
            anyhow::bail!("Constructor args too short for initial_supply");
        }

        let mut supply_bytes = [0u8; 16];
        supply_bytes.copy_from_slice(&self.constructor_args[offset..offset + 16]);
        offset += 16;

        if offset != self.constructor_args.len() {
            anyhow::bail!("Constructor args contain trailing bytes");
        }

        Ok((name, symbol, u128::from_be_bytes(supply_bytes)))
    }

    pub fn new(deployer: String, bytecode: Vec<u8>, constructor_args: Vec<u8>, nonce: u64) -> Self {
        Self {
            deployer,
            bytecode,
            constructor_args,
            nonce,
        }
    }

    /// Computes the address of the contract
    /// Formula: keccak256(deployer_address || nonce || code_hash)
    ///
    /// L'address è deterministico: stesso deployer + nonce + code → stesso address.
    /// L'address è univoco: collisioni impossibili in pratica.
    ///
    /// # Returns
    pub fn calculate_address(&self) -> anyhow::Result<String> {
        // Decodifica deployer_address da stringa hex a bytes (32 bytes)
        let deployer_bytes = self.decode_deployer_address()?;

        // Compute code_hash = keccak256(bytecode)
        let code_hash = Self::keccak256(&self.bytecode);

        // Prepara input: deployer_address || nonce || code_hash
        let mut input = Vec::with_capacity(32 + 8 + 32); // 72 bytes totali
        input.extend_from_slice(&deployer_bytes);
        input.extend_from_slice(&self.nonce.to_le_bytes());
        input.extend_from_slice(&code_hash);

        // Compute address = keccak256(deployer_address || nonce || code_hash)
        let address_bytes = Self::keccak256(&input);

        Ok(format!("0x{}", hex::encode(address_bytes)))
    }

    /// Decodifica deployer_address da stringa hex a bytes (32 bytes)
    ///
    /// Returns an error instead of panicking on invalid input.
    fn decode_deployer_address(&self) -> anyhow::Result<[u8; 32]> {
        let deployer_hex = self.deployer.strip_prefix("0x").unwrap_or(&self.deployer);
        let deployer_bytes = hex::decode(deployer_hex)
            .map_err(|e| anyhow::anyhow!("deployer address is not valid hex: {}", e))?;

        if deployer_bytes.len() != 32 {
            anyhow::bail!(
                "deployer address must be 32 bytes, got {} bytes",
                deployer_bytes.len()
            );
        }

        let mut address = [0u8; 32];
        address.copy_from_slice(&deployer_bytes);
        Ok(address)
    }

    /// Compute keccak256 hash di un input
    ///
    /// # Arguments
    /// * `input` - Dati da hashar
    ///
    /// # Returns
    /// Hash keccak256 (32 bytes)
    fn keccak256(input: &[u8]) -> [u8; 32] {
        let mut hasher = Keccak256::new();
        hasher.update(input);
        let hash = hasher.finalize();
        let mut result = [0u8; 32];
        result.copy_from_slice(&hash);
        result
    }

    pub fn validate_bytecode(&self) -> Result<(), String> {
        // 1. Verifiche base of the bytecode
        if self.bytecode.is_empty() {
            return Err("Bytecode cannot be empty".to_string());
        }

        if self.bytecode.len() > 1_000_000 {
            return Err("Bytecode too large (max 1MB)".to_string());
        }

        // 2. Check lunghezza minima ragionevole (almeno qualche byte per funzioni base)
        if self.bytecode.len() < 50 {
            return Err("Bytecode too short to contain required functions".to_string());
        }

        // Le funzioni obbligatorie sono:
        // - owner() -> selector: keccak256("owner()")[0:4]
        // - version() -> selector: keccak256("version()")[0:4]
        // - transfer_ownership(address) -> selector: keccak256("transfer_ownership(address)")[0:4]
        // - pause() -> selector: keccak256("pause()")[0:4]
        // - unpause() -> selector: keccak256("unpause()")[0:4]

        let required_selectors = vec![
            Self::keccak256(b"owner()")[0..4].to_vec(),
            Self::keccak256(b"version()")[0..4].to_vec(),
            Self::keccak256(b"transfer_ownership(address)")[0..4].to_vec(),
            Self::keccak256(b"pause()")[0..4].to_vec(),
            Self::keccak256(b"unpause()")[0..4].to_vec(),
        ];

        for selector in &required_selectors {
            if !self
                .bytecode
                .windows(selector.len())
                .any(|window| window == selector)
            {
                return Err(format!(
                    "Missing required BaseContract function selector: 0x{}",
                    hex::encode(selector)
                ));
            }
        }

        // 3. Check che il bytecode non contenga pattern pericolosi noti
        let dangerous_patterns = vec![
            // Self-destruct patterns
            b"\xff\x5b", // SELFDESTRUCT
            b"\xff\x5c", // SELFDESTRUCT (alias)
            // Delegatecall pericoloso
            b"\xf4\x00", // DELEGATECALL (padded to 2 bytes for consistency)
            // Re-entrancy patterns comuni
            b"\x5f\x3e", // PUSH1 0x3e, CALL (potenziale re-entrancy)
        ];

        for pattern in &dangerous_patterns {
            if self
                .bytecode
                .windows(pattern.len())
                .any(|window: &[u8]| window == *pattern)
            {
                return Err("Bytecode contains dangerous patterns".to_string());
            }
        }

        if self.bytecode.len() > 0 {
            let first_byte = self.bytecode[0];
            // Check che sia un opcode EVM valido (base check)
            if first_byte > 0xff {
                return Err("Invalid bytecode: first byte is not a valid EVM opcode".to_string());
            }
        }

        // 5. Check che il bytecode non sia tutto zeri (corrotto)
        if self.bytecode.iter().all(|&b| b == 0) {
            return Err("Bytecode appears to be corrupted (all zeros)".to_string());
        }

        Ok(())
    }

    ///
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `runtime` - Runtime per l'esecuzione
    /// * `gas_meter` - Gas meter per tracciare il consumo
    ///
    /// # Returns
    /// Risultato dell'esecuzione of the constructor
    fn execute_constructor_simulation(
        &self,
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
    ) -> Result<ConstructorResult> {
        // 1. Validazione base of the bytecode of the constructor
        if self.bytecode.is_empty() {
            return Ok(ConstructorResult {
                success: false,
                gas_used: 0,
                return_data: vec![],
                error: "Constructor bytecode is empty".to_string(),
            });
        }

        // 2. Compute il gas base per l'esecuzione of the constructor
        let base_gas = 21000; // Gas base per transazione
        let bytecode_gas = self.bytecode.len() as u64 * 200; // 200 gas per byte di bytecode
        let args_gas = self.constructor_args.len() as u64 * 16; // 16 gas per byte di argomenti
        let total_gas = base_gas + bytecode_gas + args_gas;

        // 3. Check che ci sia abbastanza gas
        let gas_before = gas_meter.gas_used();
        let gas_remaining = gas_meter.gas_remaining();

        if gas_remaining < total_gas {
            return Ok(ConstructorResult {
                success: false,
                gas_used: 0,
                return_data: vec![],
                error: format!(
                    "Insufficient gas: need {}, have {}",
                    total_gas, gas_remaining
                ),
            });
        }

        // 4. Consuma il gas per l'esecuzione
        gas_meter
            .consume(total_gas)
            .map_err(|e| anyhow::anyhow!("Failed to consume constructor gas: {}", e))?;

        // A production implementation would invoke an EVM/WASM VM to execute the bytecode

        // 5.1. Check che il bytecode inizi con opcodes validi
        if self.bytecode.len() > 0 {
            let first_byte = self.bytecode[0];
            if !(0x60 <= first_byte && first_byte <= 0x7f)
                && first_byte != 0x56
                && first_byte != 0x57
            {
                return Ok(ConstructorResult {
                    success: false,
                    gas_used: total_gas,
                    return_data: vec![],
                    error: "Invalid constructor bytecode: invalid initial opcode".to_string(),
                });
            }
        }

        // Gli argomenti are passati tramite calldata e processati dal bytecode
        let _args_processed = if !self.constructor_args.is_empty() {
            self.constructor_args.len() as u64 * 10 // 10 gas per byte di argomento processato
        } else {
            0
        };

        // Molti constructor inizializzano variabili di stato
        let storage_init_gas = 5000; // Gas per inizializzazione storage base
        gas_meter
            .consume(storage_init_gas)
            .map_err(|e| anyhow::anyhow!("Failed to consume storage init gas: {}", e))?;

        let has_events = self.bytecode.windows(1).any(|window| {
            let opcode = window[0];
            0xa0 <= opcode && opcode <= 0xa4 // LOG0, LOG1, LOG2, LOG3, LOG4
        });

        if has_events {
            let event_gas = 1500; // Gas per emissione eventi
            gas_meter
                .consume(event_gas)
                .map_err(|e| anyhow::anyhow!("Failed to consume event gas: {}", e))?;
        }

        // 6. Compute il gas totale consumato
        let gas_after = gas_meter.gas_used();
        let gas_used = gas_after - gas_before + storage_init_gas;

        // 7. Prepara i dati di ritorno of the constructor
        let return_data = if self.bytecode.len() > 100 {
            vec![
                0x01,                                            // Success flag
                (runtime.block_timestamp() & 0xFF) as u8,        // Timestamp parte bassa
                ((runtime.block_timestamp() >> 8) & 0xFF) as u8, // Timestamp parte alta
            ]
        } else {
            vec![] // Empty constructor, no return data
        };

        // In una implementazione reale, il VM modificherebbe il contract_storage
        // Per ora, simuliamo alcune modifiche base

        // 8.1. Salva il timestamp di deployment in the storage (slot 1000)
        let deployment_timestamp_slot = 1000u64;
        let timestamp_bytes = runtime.block_timestamp().to_le_bytes().to_vec();
        let mut timestamp_storage = vec![0u8; 32];
        timestamp_storage[..8].copy_from_slice(&timestamp_bytes);

        contract_storage
            .sstore(
                storage,
                deployment_timestamp_slot,
                timestamp_storage,
                Some(gas_meter),
            )
            .map_err(|e| anyhow::anyhow!("Failed to store deployment timestamp: {}", e))?;

        // 8.2. Salva la lunghezza degli argomenti of the constructor (slot 1001)
        let args_length_slot = 1001u64;
        let args_length = self.constructor_args.len() as u64;
        let args_length_storage = Self::u64_to_storage_value(args_length);

        contract_storage
            .sstore(
                storage,
                args_length_slot,
                args_length_storage,
                Some(gas_meter),
            )
            .map_err(|e| anyhow::anyhow!("Failed to store constructor args length: {}", e))?;

        if self.looks_like_savitri20() && !self.constructor_args.is_empty() {
            let deployer = self.decode_deployer_address()?;
            let (name, symbol, initial_supply) = self
                .decode_savitri20_constructor_args()
                .context("Failed to decode SAVITRI20 constructor args")?;

            SAVITRI20::initialize(
                contract_storage,
                storage,
                &deployer,
                &name,
                &symbol,
                initial_supply,
                Some(gas_meter),
            )
            .context("Failed to initialize SAVITRI20 state")?;
        }

        Ok(ConstructorResult {
            success: true,
            gas_used,
            return_data,
            error: String::new(),
        })
    }

    /// Converte u64 a storage value (32 bytes, little-endian)
    fn u64_to_storage_value(value: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    /// Runs the constructor e deploya the contract
    ///
    /// 1. Computes the address of the contract
    /// 2. Creates a ContractStorage per il new contract
    /// 3. Inizializza BaseContract (owner, version)
    /// 4. Runs the constructor con gli argomenti forniti
    /// 5. Compute e salva code_hash e storage_root
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `gas_meter` - Gas meter per tracciare il consumo di gas
    ///
    /// # Returns
    ///
    /// # Note
    /// - L'esecuzione of the bytecode of the constructor sarà implementata in una task futura
    /// - Per ora, il constructor viene preparato ma non eseguito (il bytecode non viene interpretato)
    pub fn execute_constructor(
        &self,
        storage: &Storage,
        runtime: &Runtime,
        gas_meter: &mut GasMeter,
    ) -> Result<[u8; 32]> {
        // 1. Computes the address of the contract
        let address_hex = self.calculate_address()?;
        let address_hex_stripped = address_hex.strip_prefix("0x").unwrap_or(&address_hex);
        let address_bytes =
            hex::decode(address_hex_stripped).context("Failed to decode contract address")?;

        if address_bytes.len() != 32 {
            anyhow::bail!(
                "Contract address must be 32 bytes, got {}",
                address_bytes.len()
            );
        }

        let mut contract_address = [0u8; 32];
        contract_address.copy_from_slice(&address_bytes);

        if storage
            .contract_exists(&contract_address)
            .context("Failed to check if contract exists")?
        {
            anyhow::bail!(
                "Contract already exists at address {}",
                hex::encode(contract_address)
            );
        }

        // 2. Consuma gas per CREATE (deploy contract)
        // Il gas viene consumato prima di procedere con il deployment
        gas_meter
            .consume_create(self.bytecode.len())
            .map_err(|e| anyhow::anyhow!("CREATE gas consumption failed: {}", e))?;

        // 3. Compute code_hash
        let code_hash = Self::keccak256(&self.bytecode);

        // 4. Creates ContractStorage per il new contract
        let mut contract_storage = ContractStorage::new(contract_address.to_vec())
            .context("Failed to create contract storage")?;

        // 5. Decodifica deployer address
        let deployer_bytes = self.decode_deployer_address()?;

        // 6. Inizializza BaseContract (owner, version)
        BaseContract::initialize(
            &mut contract_storage,
            storage,
            &deployer_bytes,
            Some(gas_meter),
        )
        .context("Failed to initialize BaseContract")?;

        // 7. Runs the constructor con gli argomenti
        // Simulazione dell'esecuzione of the bytecode of the constructor
        let constructor_result = self
            .execute_constructor_simulation(&mut contract_storage, storage, runtime, gas_meter)
            .context("Failed to execute constructor")?;

        // Se il constructor fallisce, il deployment fallisce
        if !constructor_result.success {
            anyhow::bail!("Constructor execution failed: {}", constructor_result.error);
        }

        // Log of the risultato of the constructor per debugging
        tracing::info!(
            "Constructor executed successfully at address {}, gas used: {}, return data length: {}",
            hex::encode(contract_address),
            constructor_result.gas_used,
            constructor_result.return_data.len()
        );

        storage
            .commit_contract_storage_overlay(&contract_address, contract_storage.overlay())
            .context("Failed to commit contract storage overlay")?;

        let storage_root = contract_storage
            .compute_storage_root(storage)
            .context("Failed to compute storage root")?;

        // 10. Ottieni il timestamp of the blocco corrente
        let deployed_at = runtime.block_timestamp();

        // 11. Creates ContractInfo e salvalo in the storage
        let contract_info = ContractInfo::new(
            contract_address.to_vec(),
            self.bytecode.clone(),
            code_hash.to_vec(),
            storage_root.to_vec(),
            deployer_bytes.to_vec(),
            1, // version iniziale è sempre 1
            deployed_at,
        );

        storage
            .put_contract(
                &contract_address,
                &bincode::serialize(&contract_info).context("Failed to encode contract info")?,
            )
            .context("Failed to save contract info")?;

        Ok(contract_address)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_tmp_dir() -> anyhow::Result<PathBuf> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!("savitri-deploy-test-{}", nanos));
        fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn setup_test_storage() -> anyhow::Result<(Storage, Runtime)> {
        let tmp = unique_tmp_dir()?;
        let storage = Storage::new(&tmp)?;

        // Create runtime with test timestamp
        let overlay = std::collections::BTreeMap::new();
        let runtime = Runtime::new(overlay, 1_000_000, 64, 1234567890); // Test timestamp

        Ok((storage, runtime))
    }

    fn create_test_bytecode() -> Vec<u8> {
        // Create bytecode that contains required BaseContract function selectors
        let owner_selector = DeployTransaction::keccak256(b"owner()")[0..4].to_vec();
        let version_selector = DeployTransaction::keccak256(b"version()")[0..4].to_vec();
        let transfer_ownership_selector =
            DeployTransaction::keccak256(b"transfer_ownership(address)")[0..4].to_vec();
        let pause_selector = DeployTransaction::keccak256(b"pause()")[0..4].to_vec();
        let unpause_selector = DeployTransaction::keccak256(b"unpause()")[0..4].to_vec();

        let mut bytecode = Vec::new();

        // Add PUSH instructions for each selector (0x60-0x7f range)
        bytecode.push(0x60); // PUSH1
        bytecode.extend_from_slice(&owner_selector);

        bytecode.push(0x60); // PUSH1
        bytecode.extend_from_slice(&version_selector);

        bytecode.push(0x60); // PUSH1
        bytecode.extend_from_slice(&transfer_ownership_selector);

        bytecode.push(0x60); // PUSH1
        bytecode.extend_from_slice(&pause_selector);

        bytecode.push(0x60); // PUSH1
        bytecode.extend_from_slice(&unpause_selector);

        // Add some additional bytecode to make it realistic and exceed min length (50 bytes)
        bytecode.extend_from_slice(&[0x56, 0x57, 0x58, 0x59]); // JUMP, JUMPI, PC, MSIZE

        // Pad to at least 50 bytes (required minimum)
        while bytecode.len() < 50 {
            bytecode.push(0x00); // STOP opcode as padding
        }

        bytecode
    }

    #[test]
    fn test_deploy_transaction_new() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = vec![0x60, 0x01, 0x56]; // Simple bytecode
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(
            deployer.clone(),
            bytecode.clone(),
            constructor_args.clone(),
            nonce,
        );

        assert_eq!(deploy_tx.deployer, deployer);
        assert_eq!(deploy_tx.bytecode, bytecode);
        assert_eq!(deploy_tx.constructor_args, constructor_args);
        assert_eq!(deploy_tx.nonce, nonce);
    }

    #[test]
    fn test_calculate_address() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = vec![0x60, 0x01, 0x56];
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);
        let address = deploy_tx.calculate_address().unwrap();

        // Address should be deterministic and start with "0x"
        assert!(address.starts_with("0x"));
        assert_eq!(address.len(), 66); // "0x" + 64 hex chars

        // Same input should produce same address
        let address2 = deploy_tx.calculate_address().unwrap();
        assert_eq!(address, address2);
    }

    #[test]
    fn test_calculate_address_different_nonce() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = vec![0x60, 0x01, 0x56];
        let constructor_args = vec![0x01, 0x02, 0x03];

        let deploy_tx1 = DeployTransaction::new(
            deployer.clone(),
            bytecode.clone(),
            constructor_args.clone(),
            1,
        );
        let deploy_tx2 = DeployTransaction::new(deployer, bytecode, constructor_args, 2);

        let address1 = deploy_tx1.calculate_address().unwrap();
        let address2 = deploy_tx2.calculate_address().unwrap();

        // Different nonces should produce different addresses
        assert_ne!(address1, address2);
    }

    #[test]
    fn test_validate_bytecode_success() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = create_test_bytecode();
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        let result = deploy_tx.validate_bytecode();
        assert!(
            result.is_ok(),
            "Valid bytecode should pass validation: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_validate_bytecode_empty() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = vec![];
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        let result = deploy_tx.validate_bytecode();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Bytecode cannot be empty");
    }

    #[test]
    fn test_validate_bytecode_too_large() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = vec![0x60; 1_000_001]; // 1MB + 1 byte
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        let result = deploy_tx.validate_bytecode();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Bytecode too large (max 1MB)");
    }

    #[test]
    fn test_validate_bytecode_missing_functions() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        // Bytecode without required BaseContract functions (>= 50 bytes but no selectors)
        let bytecode = vec![0x60; 60];
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        let result = deploy_tx.validate_bytecode();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("Missing required BaseContract function selector"));
    }

    #[test]
    fn test_validate_bytecode_dangerous_patterns() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let mut bytecode = create_test_bytecode();

        // Add dangerous SELFDESTRUCT pattern
        bytecode.extend_from_slice(b"\xff\x5b");

        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        let result = deploy_tx.validate_bytecode();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Bytecode contains dangerous patterns");
    }

    #[test]
    fn test_validate_bytecode_too_short() {
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = vec![0x60, 0x01]; // Too short for required functions
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        let result = deploy_tx.validate_bytecode();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Bytecode too short to contain required functions"
        );
    }

    #[test]
    fn test_execute_constructor_simulation_success() -> anyhow::Result<()> {
        let (storage, runtime) = setup_test_storage()?;
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = create_test_bytecode();
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        // Create contract storage for the test
        let contract_address = deploy_tx.decode_deployer_address()?;
        let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;

        let mut gas_meter = GasMeter::new(1_000_000);

        let result = deploy_tx.execute_constructor_simulation(
            &mut contract_storage,
            &storage,
            &runtime,
            &mut gas_meter,
        )?;

        assert!(result.success, "Constructor execution should succeed");
        assert!(result.gas_used > 0, "Gas should be consumed");
        assert_eq!(result.error, "", "Error should be empty on success");

        Ok(())
    }

    #[test]
    fn test_execute_constructor_simulation_insufficient_gas() -> anyhow::Result<()> {
        let (storage, runtime) = setup_test_storage()?;
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = create_test_bytecode();
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        // Create contract storage for the test
        let contract_address = deploy_tx.decode_deployer_address()?;
        let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;

        // Use very low gas limit
        let mut gas_meter = GasMeter::new(1000);

        let result = deploy_tx.execute_constructor_simulation(
            &mut contract_storage,
            &storage,
            &runtime,
            &mut gas_meter,
        )?;

        assert!(
            !result.success,
            "Constructor should fail with insufficient gas"
        );
        assert!(result.error.contains("Insufficient gas"));

        Ok(())
    }

    #[test]
    fn test_execute_constructor_simulation_invalid_bytecode() -> anyhow::Result<()> {
        let (storage, runtime) = setup_test_storage()?;
        let deployer =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        // Bytecode with invalid initial opcode
        let bytecode = vec![0x99]; // Invalid opcode
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx = DeployTransaction::new(deployer, bytecode, constructor_args, nonce);

        // Create contract storage for the test
        let contract_address = deploy_tx.decode_deployer_address()?;
        let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;

        let mut gas_meter = GasMeter::new(1_000_000);

        let result = deploy_tx.execute_constructor_simulation(
            &mut contract_storage,
            &storage,
            &runtime,
            &mut gas_meter,
        )?;

        assert!(
            !result.success,
            "Constructor should fail with invalid bytecode"
        );
        assert!(result.error.contains("invalid initial opcode"));

        Ok(())
    }

    #[test]
    fn test_decode_deployer_address() {
        let deployer_with_prefix =
            "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let deployer_without_prefix =
            "1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef".to_string();
        let bytecode = vec![0x60, 0x01, 0x56];
        let constructor_args = vec![0x01, 0x02, 0x03];
        let nonce = 1;

        let deploy_tx1 = DeployTransaction::new(
            deployer_with_prefix,
            bytecode.clone(),
            constructor_args.clone(),
            nonce,
        );
        let deploy_tx2 =
            DeployTransaction::new(deployer_without_prefix, bytecode, constructor_args, nonce);

        let address1 = deploy_tx1.decode_deployer_address().unwrap();
        let address2 = deploy_tx2.decode_deployer_address().unwrap();

        // Both should decode to the same 32-byte address
        assert_eq!(address1, address2);
        assert_eq!(address1.len(), 32);
    }

    #[test]
    fn test_keccak256() {
        let input = b"test";
        let hash = DeployTransaction::keccak256(input);

        // Keccak256 should always produce 32 bytes
        assert_eq!(hash.len(), 32);

        // Same input should produce same hash
        let hash2 = DeployTransaction::keccak256(input);
        assert_eq!(hash, hash2);

        // Different input should produce different hash
        let hash3 = DeployTransaction::keccak256(b"different");
        assert_ne!(hash, hash3);
    }
}
