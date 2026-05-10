//! Core Module for Savitri Contracts
//!
//! This module provides the fundamental types and utilities needed for smart contract
//! development on the Savitri blockchain. It includes transaction handling, type definitions,
//! and core contract execution primitives.

use serde::{Deserialize, Serialize};
use anyhow::Result;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub mod tx;
pub mod types;

// Re-export commonly used types for easier access
pub use tx::{Transaction, TransactionType, TransactionResult};
pub use types::{ContractAddress, ContractResult, ContractError};

/// Core contract execution engine
pub struct ContractEngine {
    /// Gas meter for execution tracking
    gas_meter: GasMeter,
    /// Execution context
    context: ExecutionContext,
    /// Contract registry
    registry: ContractRegistry,
}

/// Gas meter for tracking contract execution costs
#[derive(Debug, Clone)]
pub struct GasMeter {
    /// Current gas used
    pub current_gas: u64,
    /// Gas limit for execution
    gas_limit: u64,
    /// Total gas consumed
    total_consumed: u64,
    /// Refundable gas
    refundable_gas: u64,
}

impl GasMeter {
    /// Create new gas meter with limit
    pub fn new(gas_limit: u64) -> Self {
        Self {
            current_gas: 0,
            gas_limit,
            total_consumed: 0,
            refundable_gas: 0,
        }
    }

    /// Consume gas for operation
    pub fn consume_gas(&mut self, amount: u64) -> Result<(), ContractError> {
        if self.current_gas + amount > self.gas_limit {
            return Err(ContractError::OutOfGas);
        }
        
        self.current_gas += amount;
        self.total_consumed += amount;
        Ok(())
    }

    /// Refund gas (for unused operations)
    pub fn refund_gas(&mut self, amount: u64) {
        if amount <= self.current_gas {
            self.current_gas -= amount;
            self.refundable_gas += amount;
        }
    }

    /// Reset gas meter for new execution
    pub fn reset(&mut self) {
        self.current_gas = 0;
        self.refundable_gas = 0;
    }

    /// Get remaining gas
    pub fn remaining_gas(&self) -> u64 {
        self.gas_limit.saturating_sub(self.current_gas)
    }

    /// Check if operation can be afforded
    pub fn can_afford(&self, gas_cost: u64) -> bool {
        self.current_gas + gas_cost <= self.gas_limit
    }

    /// Get gas efficiency percentage
    pub fn efficiency(&self) -> f64 {
        if self.total_consumed == 0 {
            1.0
        } else {
            self.current_gas as f64 / self.total_consumed as f64
        }
    }
}

/// Contract execution context
#[derive(Debug, Clone)]
pub struct ExecutionContext {
    /// Contract address
    pub contract_address: ContractAddress,
    /// Caller address
    pub caller: Vec<u8>,
    /// Transaction hash
    pub tx_hash: Vec<u8>,
    /// Block number
    pub block_number: u64,
    /// Block timestamp
    pub timestamp: u64,
    /// Value sent with transaction
    pub value: u128,
    /// Gas limit
    pub gas_limit: u64,
    /// Contract storage state
    storage_state: ContractStorageState,
}

/// Contract storage state
#[derive(Debug, Clone, Default)]
pub struct ContractStorageState {
    /// Storage root hash
    pub storage_root: Vec<u8>,
    /// Modified storage keys
    pub modified_keys: Vec<Vec<u8>>,
    /// Storage size in bytes
    pub storage_size: u64,
}

impl ExecutionContext {
    /// Create new execution context
    pub fn new(
        contract_address: ContractAddress,
        caller: Vec<u8>,
        tx_hash: Vec<u8>,
        block_number: u64,
        value: u128,
        gas_limit: u64,
    ) -> Self {
        Self {
            contract_address,
            caller,
            tx_hash,
            block_number,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            value,
            gas_limit,
            storage_state: ContractStorageState::default(),
        }
    }

    /// Get contract age in blocks
    pub fn age(&self) -> u64 {
        let current_block = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() / 10; // Assuming 10s per block
            .saturating_sub(self.block_number as u64)
    }

    /// Check if transaction is recent (within last 100 blocks)
    pub fn is_recent(&self) -> bool {
        self.age() <= 100
    }

    /// Update storage root
    pub fn update_storage_root(&mut self, root: Vec<u8>) {
        self.storage_state.storage_root = root;
        self.storage_state.modified_keys.clear();
    }

    /// Mark storage key as modified
    pub fn mark_storage_modified(&mut self, key: Vec<u8>) {
        if !self.storage_state.modified_keys.contains(&key) {
            self.storage_state.modified_keys.push(key);
        }
    }
}

/// Contract registry for managing deployed contracts
#[derive(Debug, Clone, Default)]
pub struct ContractRegistry {
    /// Registered contracts by address
    contracts: HashMap<ContractAddress, ContractInfo>,
    /// Contract bytecode cache
    bytecode_cache: HashMap<Vec<u8>, Vec<u8>>,
    /// Contract metadata
    metadata: ContractMetadata,
}

impl ContractRegistry {
    /// Create new contract registry
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new contract
    pub fn register_contract(&mut self, contract: ContractInfo) -> Result<ContractAddress, ContractError> {
        let address = contract.address.clone();
        
        // Validate contract
        self.validate_contract(&contract)?;
        
        // Store contract
        self.contracts.insert(address.clone(), contract);
        
        tracing::info!(
            address = hex::encode(&address),
            name = contract.metadata.name,
            "Contract registered successfully"
        );
        
        Ok(address)
    }

    /// Get contract by address
    pub fn get_contract(&self, address: &ContractAddress) -> Option<&ContractInfo> {
        self.contracts.get(address)
    }

    /// Update contract information
    pub fn update_contract(&mut self, address: &ContractAddress, contract: ContractInfo) -> Result<(), ContractError> {
        if !self.contracts.contains_key(address) {
            return Err(ContractError::ContractNotFound);
        }
        
        self.contracts.insert(address.clone(), contract);
        tracing::info!(
            address = hex::encode(address),
            "Contract updated successfully"
        );
        Ok(())
    }

    /// Deregister contract
    pub fn deregister_contract(&mut self, address: &ContractAddress) -> Result<(), ContractError> {
        if self.contracts.remove(address).is_some() {
            tracing::info!(
                address = hex::encode(address),
                "Contract deregistered successfully"
            );
            Ok(())
        } else {
            Err(ContractError::ContractNotFound)
        }
    }

    /// List all contracts
    pub fn list_contracts(&self) -> Vec<&ContractInfo> {
        self.contracts.values().collect()
    }

    /// Get contracts by type
    pub fn get_contracts_by_type(&self, contract_type: &str) -> Vec<&ContractInfo> {
        self.contracts
            .values()
            .filter(|c| c.metadata.contract_type == contract_type)
            .collect()
    }

    /// Search contracts by name
    pub fn search_contracts(&self, query: &str) -> Vec<&ContractInfo> {
        let query_lower = query.to_lowercase();
        self.contracts
            .values()
            .filter(|c| {
                c.metadata.name.to_lowercase().contains(&query_lower) ||
                c.metadata.description.to_lowercase().contains(&query_lower)
            })
            .collect()
    }

    /// Validate contract before registration
    fn validate_contract(&self, contract: &ContractInfo) -> Result<(), ContractError> {
        // Check contract bytecode
        if contract.code.is_empty() {
            return Err(ContractError::EmptyBytecode);
        }

        // Check contract address format
        if contract.address.len() != 20 {
            return Err(ContractError::InvalidAddress);
        }

        // Verify code hash
        let calculated_hash = self.calculate_code_hash(&contract.code);
        if calculated_hash != contract.code_hash {
            return Err(ContractError::InvalidHash);
        }

        // Check contract name
        if contract.metadata.name.is_empty() {
            return Err(ContractError::InvalidName);
        }

        Ok(())
    }

    /// Calculate SHA-256 hash of contract bytecode
    fn calculate_code_hash(&self, code: &[u8]) -> Vec<u8> {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(code);
        hasher.finalize().to_vec()
    }
}

impl ContractEngine {
    /// Create new contract engine
    pub fn new(gas_limit: u64) -> Self {
        Self {
            gas_meter: GasMeter::new(gas_limit),
            context: ExecutionContext::new(
                ContractAddress::default(),
                vec![0u8; 20], // Default caller
                vec![0u8; 32], // Default tx hash
                0, // Default block number
                0, // Default value
                gas_limit,
            ),
            registry: ContractRegistry::new(),
        }
    }

    /// Set execution context
    pub fn set_context(&mut self, context: ExecutionContext) {
        self.context = context;
        self.gas_meter = GasMeter::new(context.gas_limit);
    }

    /// Execute contract call
    pub fn execute_call(
        &mut self,
        contract_address: &ContractAddress,
        function: &str,
        args: &[Vec<u8>],
        value: u128,
    ) -> Result<Vec<u8>, ContractError> {
        // Get contract
        let contract = self.registry.get_contract(contract_address)
            .ok_or_else(|| Err(ContractError::ContractNotFound))?;

        // Check if contract can execute
        if !contract.can_execute() {
            return Err(contract.status.into());
        }

        // Check if caller has sufficient balance
        if value > 0 && self.context.value < value {
            return Err(ContractError::InsufficientBalance);
        }

        // Estimate gas cost
        let estimated_gas = self.estimate_gas_cost(function, args);
        if !self.gas_meter.can_afford(estimated_gas) {
            return Err(ContractError::OutOfGas);
        }

        // Consume gas for execution
        self.gas_meter.consume_gas(estimated_gas)?;

        // In a real implementation, this would:
        // 1. Load contract bytecode
        // 2. Execute the function
        // 3. Handle results
        // For now, return mock result
        let result = format!("{}({:?})", function, args);
        Ok(result.into_bytes())
    }

    /// Execute contract deployment
    pub fn deploy_contract(
        &mut self,
        bytecode: Vec<u8>,
        metadata: ContractMetadata,
        value: u128,
    ) -> Result<ContractAddress, ContractError> {
        // Calculate deployment cost
        let deployment_cost = bytecode.len() as u64 * 200; // 200 gas per byte
        if !self.gas_meter.can_afford(deployment_cost) {
            return Err(ContractError::OutOfGas);
        }

        // Generate contract address
        let address = self.generate_contract_address(&bytecode);

        // Create contract info
        let contract = ContractInfo::with_metadata(
            address.clone(),
            bytecode,
            self.calculate_code_hash(&bytecode),
            vec![0u8; 32], // Initial storage root
            self.context.caller.clone(),
            1, // Initial version
            self.context.timestamp,
            metadata,
        );

        // Consume gas for deployment
        self.gas_meter.consume_gas(deployment_cost)?;

        // Register contract
        let registered_address = self.registry.register_contract(contract)?;

        tracing::info!(
            address = hex::encode(&registered_address),
            name = metadata.name,
            "Contract deployed successfully"
        );

        Ok(registered_address)
    }

    /// Generate unique contract address
    fn generate_contract_address(&self, bytecode: &[u8]) -> ContractAddress {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(bytecode);
        hasher.update(&self.context.caller);
        hasher.update(&self.context.tx_hash);
        let hash = hasher.finalize();
        
        // Take first 20 bytes as address
        ContractAddress::from_slice(&hash[..20])
    }

    /// Estimate gas cost for function execution
    fn estimate_gas_cost(&self, function: &str, args: &[Vec<u8>]) -> u64 {
        // Base cost for function call
        let base_cost = 21000; // Standard function call cost
        
        // Add cost for arguments
        let args_cost = args.iter().map(|arg| arg.len() as u64 * 200).sum();
        
        base_cost + args_cost
    }

    /// Get current gas usage
    pub fn gas_used(&self) -> u64 {
        self.gas_meter.current_gas
    }

    /// Get gas limit
    pub fn gas_limit(&self) -> u64 {
        self.gas_meter.gas_limit
    }

    /// Get total gas consumed
    pub fn total_gas_consumed(&self) -> u64 {
        self.gas_meter.total_consumed
    }

    /// Get contract registry
    pub fn registry(&self) -> &ContractRegistry {
        &self.registry
    }

    /// Get execution context
    pub fn context(&self) -> &ExecutionContext {
        &self.context
    }
}

/// Contract address type (20 bytes)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct ContractAddress(pub [u8; 20]);

impl ContractAddress {
    /// Create contract address from bytes
    pub fn from_slice(bytes: &[u8]) -> Self {
        let mut address = [0u8; 20];
        let len = bytes.len().min(20);
        address[..len].copy_from_slice(bytes);
        ContractAddress(address)
    }

    /// Create contract address from hex string
    pub fn from_hex(hex_str: &str) -> Result<Self, ContractError> {
        let bytes = hex::decode(hex_str)
            .map_err(|_| ContractError::InvalidAddress)?;
        Ok(ContractAddress::from_slice(&bytes))
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    /// Check if address is zero address
    pub fn is_zero(&self) -> bool {
        self.iter().all(|&b| b == 0)
    }
}

/// Contract result type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContractResult<T> {
    Success(T),
    Error(ContractError),
}

impl<T> ContractResult<T> {
    /// Map successful result
    pub fn map<U, F>(self, f: F) -> ContractResult<U> where
        F: FnOnce(T) -> U,
    {
        match self {
            ContractResult::Success(value) => ContractResult::Success(f(value)),
            ContractResult::Error(err) => ContractResult::Error(err),
        }
    }

    /// Convert Result to ContractResult
    pub fn from_result(result: Result<T, ContractError>) -> ContractResult<T> {
        match result {
            Ok(value) => ContractResult::Success(value),
            Err(err) => ContractResult::Error(err),
        }
    }
}

/// Contract error types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ContractError {
    /// Out of gas error
    OutOfGas,
    /// Contract not found
    ContractNotFound,
    /// Invalid address format
    InvalidAddress,
    /// Empty bytecode
    EmptyBytecode,
    /// Invalid bytecode hash
    InvalidHash,
    /// Invalid contract name
    InvalidName,
    /// Insufficient balance
    InsufficientBalance,
    /// Contract execution error
    ExecutionError(String),
    /// Invalid function signature
    InvalidFunction,
    /// Storage error
    StorageError(String),
    /// Revert error
    Revert(String),
    /// Panic error
    Panic(String),
}

impl From<ContractStatus> for ContractError {
    fn from(status: ContractStatus) -> Self {
        match status {
            ContractStatus::Deploying => ContractError::ExecutionError("Contract is still deploying".to_string()),
            ContractStatus::Paused => ContractError::ExecutionError("Contract is paused".to_string()),
            ContractStatus::Terminated => ContractError::ExecutionError("Contract is terminated".to_string()),
            ContractStatus::Maintenance => ContractError::ExecutionError("Contract is under maintenance".to_string()),
            ContractStatus::Active => ContractError::ExecutionError("Contract is active".to_string()),
        }
    }
}

/// Contract metadata for additional information
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ContractMetadata {
    /// Contract name
    pub name: String,
    /// Contract description
    pub description: String,
    /// Contract version
    pub version: String,
    /// Contract type (ERC20, ERC721, Custom, etc.)
    pub contract_type: String,
    /// Creator address
    pub creator: Vec<u8>,
    /// Deployment timestamp
    pub deployed_at: u64,
    /// Last updated timestamp
    pub updated_at: u64,
    /// Contract tags
    pub tags: Vec<String>,
    /// Contract ABI
    pub abi: Vec<u8>,
    pub verified: bool,
    /// Audit status
    pub audited: bool,
    /// Source code hash
    pub source_hash: Vec<u8>,
    /// Compiler version
    pub compiler_version: String,
    /// Optimization settings
    pub optimization: String,
}

impl ContractMetadata {
    /// Create new contract metadata
    pub fn new(name: String, contract_type: String, creator: Vec<u8>) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        Self {
            name,
            description: String::new(),
            version: "1.0.0".to_string(),
            contract_type,
            creator,
            deployed_at: timestamp,
            updated_at: timestamp,
            tags: Vec::new(),
            abi: Vec::new(),
            verified: false,
            audited: false,
            source_hash: Vec::new(),
            compiler_version: "savitri-contracts 0.1.0".to_string(),
            optimization: "opt".to_string(),
        }
    }

    /// Update timestamp
    pub fn update_timestamp(&mut self) {
        self.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    /// Add tag
    pub fn add_tag(&mut self, tag: String) {
        if !self.tags.contains(&tag) {
            self.tags.push(tag);
            self.update_timestamp();
        }
    }

    /// Remove tag
    pub fn remove_tag(&mut self, tag: &str) -> bool {
        if let Some(pos) = self.tags.iter().position(|t| t == tag) {
            self.tags.remove(pos);
            self.update_timestamp();
            true
        } else {
            false
        }
    }

    /// Check if contract is verified
    pub fn is_verified(&self) -> bool {
        self.verified
    }

    /// Set verification status
    pub fn set_verified(&mut self, verified: bool) {
        self.verified = verified;
        self.update_timestamp();
    }

    /// Check if contract is audited
    pub fn is_audited(&self) -> bool {
        self.audited
    }

    /// Set audit status
    pub fn set_audited(&mut self, audited: bool) {
        self.audited = audited;
        self.update_timestamp();
    }
}

/// Transaction type enumeration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionType {
    /// Regular transaction
    Regular,
    /// Contract deployment
    ContractDeploy,
    /// Contract call
    ContractCall,
    /// Contract creation
    ContractCreation,
    /// Contract upgrade
    ContractUpgrade,
}

/// Transaction information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    /// Transaction type
    pub tx_type: TransactionType,
    /// Sender address
    pub sender: Vec<u8>,
    /// Receiver address
    pub receiver: Vec<u8>,
    /// Amount
    pub amount: u128,
    /// Nonce
    pub nonce: u64,
    /// Gas limit
    pub gas_limit: u64,
    /// Gas price
    pub gas_price: u64,
    /// Transaction data
    pub data: Vec<u8>,
    /// Signature
    pub signature: Vec<u8>,
    /// Timestamp
    pub timestamp: u64,
    /// Block number
    pub block_number: u64,
    /// Transaction hash
    pub hash: Vec<u8>,
}

impl Transaction {
    /// Create new transaction
    pub fn new(
        tx_type: TransactionType,
        sender: Vec<u8>,
        receiver: Vec<u8>,
        amount: u128,
        nonce: u64,
        gas_limit: u64,
        gas_price: u64,
        data: Vec<u8>,
    ) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        
        let hash = Self::calculate_hash(&sender, &receiver, amount, nonce, &data);
        
        Self {
            tx_type,
            sender,
            receiver,
            amount,
            nonce,
            gas_limit,
            gas_price,
            data,
            signature: Vec::new(), // To be added later
            timestamp,
            block_number: 0, // To be set by blockchain
            hash,
        }
    }

    /// Add signature to transaction
    pub fn sign(&mut self, signature: Vec<u8>) {
        self.signature = signature;
        self.hash = Self::calculate_hash(
            &self.sender,
            &self.receiver,
            self.amount,
            self.nonce,
            &self.data,
        );
    }

    /// Calculate transaction hash
    fn calculate_hash(
        sender: &[u8],
        receiver: &[u8],
        amount: u128,
        nonce: u64,
        data: &[u8],
    ) -> Vec<u8> {
        use sha2::Digest;
        let mut hasher = sha2::Sha256::new();
        hasher.update(sender);
        hasher.update(receiver);
        hasher.update(&amount.to_le_bytes());
        hasher.update(&nonce.to_le_bytes());
        hasher.update(data);
        hasher.finalize().to_vec()
    }

    /// Get transaction size in bytes
    pub fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.data.len() + self.signature.len()
    }

    /// Check if transaction is valid
    pub fn is_valid(&self) -> bool {
        !self.signature.is_empty() &&
        self.nonce >= 0 &&
        self.gas_limit > 0 &&
        self.gas_price > 0
    }

    /// Get transaction fee
    pub fn fee(&self) -> u128 {
        self.gas_limit as u128 * self.gas_price as u128
    }

    /// Check if transaction is contract-related
    pub fn is_contract_transaction(&self) -> bool {
        matches!(self.tx_type, TransactionType::ContractCall | TransactionType::ContractDeploy | TransactionType::ContractCreation)
    }
}

/// Transaction result type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionResult {
    /// Success flag
    pub success: bool,
    /// Return data
    pub return_data: Vec<u8>,
    /// Error message (if any)
    pub error: Option<String>,
    /// Gas used
    pub gas_used: u64,
    /// Logs generated
    pub logs: Vec<String>,
}

impl TransactionResult {
    /// Create successful result
    pub fn success(return_data: Vec<u8>, gas_used: u64) -> Self {
        Self {
            success: true,
            return_data,
            error: None,
            gas_used,
            logs: Vec::new(),
        }
    }

    /// Create error result
    pub fn error(error: String, gas_used: u64) -> Self {
        Self {
            success: false,
            return_data: Vec::new(),
            error: Some(error),
            gas_used,
            logs: Vec::new(),
        }
    }

    /// Add log entry
    pub fn add_log(&mut self, log: String) {
        self.logs.push(log);
    }

    /// Get total logs
    pub fn logs(&self) -> &[String] {
        &self.logs
    }
}

/// Contract execution engine factory
pub struct ContractEngineFactory;

impl ContractEngineFactory {
    /// Create contract engine with default configuration
    pub fn create_default() -> ContractEngine {
        ContractEngine::new(1_000_000) // Default 1M gas limit
    }

    /// Create contract engine with custom gas limit
    pub fn create_with_gas_limit(gas_limit: u64) -> ContractEngine {
        ContractEngine::new(gas_limit)
    }

    /// Create contract engine with full configuration
    pub fn create_with_config(
        gas_limit: u64,
        context: ExecutionContext,
    ) -> ContractEngine {
        let mut engine = ContractEngine::new(gas_limit);
        engine.set_context(context);
        engine
    }
}

/// Contract deployment helper
pub struct ContractDeployer {
    engine: ContractEngine,
}

impl ContractDeployer {
    /// Create new contract deployer
    pub fn new() -> Self {
        Self {
            engine: ContractEngine::create_default(),
        }
    }

    /// Deploy contract with metadata
    pub fn deploy(
        &mut self,
        bytecode: Vec<u8>,
        name: String,
        contract_type: String,
        creator: Vec<u8>,
        value: u128,
    ) -> Result<ContractAddress, ContractError> {
        let metadata = ContractMetadata::new(name, contract_type, creator);
        self.engine.deploy_contract(bytecode, metadata, value)
    }

    /// Get engine reference
    pub fn engine(&self) -> &ContractEngine {
        &self.engine
    }
}

/// Contract caller interface
pub trait ContractCaller {
    /// Call contract function
    fn call(
        &self,
        contract_address: &ContractAddress,
        function: &str,
        args: &[Vec<u8>],
        value: u128,
    ) -> Result<Vec<u8>, ContractError>;
}

/// Contract caller implementation using ContractEngine
impl ContractCaller for ContractDeployer {
    fn call(
        &self,
        contract_address: &ContractAddress,
        function: &str,
        args: &[Vec<u8>],
        value: u128,
    ) -> Result<Vec<u8>, ContractError> {
        self.engine.execute_call(contract_address, function, args, value)
    }
}

/// Contract viewer for inspection
pub struct ContractViewer {
    registry: ContractRegistry,
}

impl ContractViewer {
    /// Create new contract viewer
    pub fn new() -> Self {
        Self {
            registry: ContractRegistry::new(),
        }
    }

    /// Get all contracts
    pub fn get_all_contracts(&self) -> Vec<&ContractInfo> {
        self.registry.list_contracts()
    }

    /// Get contract by address
    pub fn get_contract(&self, address: &ContractAddress) -> Option<&ContractInfo> {
        self.registry.get_contract(address)
    }

    /// Search contracts
    pub fn search(&self, query: &str) -> Vec<&ContractInfo> {
        self.registry.search_contracts(query)
    }

    /// Get contracts by type
    pub fn get_contracts_by_type(&self, contract_type: &str) -> Vec<&ContractInfo> {
        self.registry.get_contracts_by_type(contract_type)
    }
}

/// Contract state inspector
pub struct ContractStateInspector {
    storage: ContractStorageState,
}

impl ContractStateInspector {
    /// Create new state inspector
    pub fn new() -> Self {
        Self {
            storage: ContractStorageState::default(),
        }
    }

    /// Get storage root
    pub fn storage_root(&self) -> &[u8] {
        &self.storage.storage_root
    }

    /// Get modified keys
    pub fn modified_keys(&self) -> &[Vec<u8>] {
        &self.storage.modified_keys
    }

    /// Get storage size
    pub fn storage_size(&self) -> u64 {
        self.storage_state.storage_size
    }

    /// Check if storage was modified
    pub fn is_modified(&self) -> bool {
            !self.storage.modified_keys.is_empty()
    }
}

/// Contract event emitter
pub struct ContractEventEmitter {
    events: Vec<ContractEvent>,
}

impl ContractEventEmitter {
    /// Create new event emitter
    pub fn new() -> Self {
        Self {
            events: Vec::new(),
        }
    }

    /// Emit event
    pub fn emit(&mut self, event: ContractEvent) {
        self.events.push(event);
    }

    /// Get all events
    pub fn events(&self) -> &[ContractEvent] {
        &self.events
    }

    /// Clear all events
    pub fn clear_events(&mut self) {
        self.events.clear();
    }
}

/// Contract event structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ContractEvent {
    /// Event signature
    pub signature: String,
    /// Contract address
    pub contract_address: ContractAddress,
    /// Event data
    pub data: Vec<u8>,
    /// Emitter address
    pub emitter: Vec<u8>,
    /// Event index
    pub index: u32,
    /// Block number
    pub block_number: u64,
    /// Timestamp
    pub timestamp: u64,
}

/// Contract event factory
pub struct ContractEventFactory;

impl ContractEventFactory {
    /// Create transfer event
    pub fn create_transfer_event(
        from: Vec<u8>,
        to: Vec<u8>,
        amount: u128,
        token_id: Vec<u8>,
    ) -> ContractEvent {
        let signature = format!("Transfer(from={}, to={}, amount={}, token_id={})", 
            hex::encode(&from),
            hex::encode(&to),
            amount,
            hex::encode(&token_id)
        );

        ContractEvent {
            signature,
            contract_address: ContractAddress::default(),
            data: vec![],
            emitter: from,
            index: 0,
            block_number: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Create approval event
    pub fn create_approval_event(
        owner: Vec<u8>,
        spender: Vec<u8>,
        value: u128,
        token_id: Vec<u8>,
    ) -> ContractEvent {
        let signature = format!("Approval(owner={}, spender={}, value={}, token_id={})", 
            hex::encode(&owner),
            hex::encode(&spender),
            value,
            hex::encode(&token_id)
        );

        ContractEvent {
            signature,
            contract_address: ContractAddress::default(),
            data: vec![],
            emitter: owner,
            index: 0,
            block_number: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Create custom event
    pub fn create_custom_event(
        signature: String,
        contract_address: ContractAddress,
        data: Vec<u8>,
        emitter: Vec<u8>,
        index: u32,
    ) -> ContractEvent {
        ContractEvent {
            signature,
            contract_address,
            data,
            emitter,
            index,
            block_number: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}
