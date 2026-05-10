use hex;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tracing::info;

use super::executor::cache::ExecutorAccountCache;
use super::types::FeeLimits;

// Transaction types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transaction {
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub amount: u128,
    pub nonce: u64,
    pub fee: Option<u128>,
    pub pubkey: Vec<u8>,
    pub sig: ByteBuf,
    pub pre_verified: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedTx {
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub amount: u128,
    pub nonce: u64,
    pub fee: Option<u128>,
    pub pubkey: Vec<u8>,
    pub sig: ByteBuf,
    pub pre_verified: bool,
}

impl SignedTx {
    pub fn to_transaction(&self) -> Transaction {
        Transaction {
            from: self.from.clone(),
            to: self.to.clone(),
            amount: self.amount,
            nonce: self.nonce,
            fee: self.fee,
            pubkey: self.pubkey.clone(),
            sig: self.sig.clone(),
            pre_verified: self.pre_verified,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployTransaction {
    pub from: Vec<u8>,
    pub bytecode: Vec<u8>,
    pub nonce: u64,
    pub fee: Option<u128>,
    pub pubkey: Vec<u8>,
    pub pre_verified: bool,
    // Note: signature is handled separately in the deployment process
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallTransaction {
    pub from: Vec<u8>,
    /// Target contract address
    pub contract_address: Vec<u8>,
    /// Function selector (4 bytes)
    pub function_selector: Vec<u8>,
    /// Function call data
    pub calldata: Vec<u8>,
    /// Transaction nonce
    pub nonce: u64,
    /// Optional transaction fee
    pub fee: Option<u128>,
    /// Public key for signature verification
    pub pubkey: Vec<u8>,
    /// Transaction signature
    pub sig: ByteBuf,
    /// Whether transaction is pre-verified
    pub pre_verified: bool,
}

/// Transaction receipt structure
///
/// Represents the result of a transaction execution,
/// including success status, gas consumption, return data, and events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Receipt {
    /// Whether the transaction succeeded
    pub success: bool,
    /// Gas consumed by the transaction
    pub gas_used: u64,
    /// Return data from the transaction
    pub return_data: Vec<u8>,
    /// Events emitted during transaction execution
    pub events: Vec<ReceiptEvent>,
}

/// Event structure for transaction receipts
///
/// Represents an event emitted during smart contract execution
/// with topics for indexing and data payload.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceiptEvent {
    /// Event topics for indexing and filtering
    pub topics: Vec<Vec<u8>>,
    /// Event data payload
    pub data: Vec<u8>,
}

// Contract execution result structures
/// Contract constructor execution result
///
/// Represents the result of contract deployment/constructor execution.
pub struct ConstructorResult {
    /// Initial contract state after constructor execution
    pub initial_state: Vec<u8>,
    /// Gas consumed by constructor execution
    pub gas_used: u64,
}

/// Contract call execution result
///
/// Represents the result of a contract call execution.
pub struct CallResult {
    /// New contract state after call execution
    pub new_state: Vec<u8>,
    /// Return data from the contract call
    pub return_data: Vec<u8>,
    /// Gas consumed by the call execution
    pub gas_used: u64,
}

/// Maximum transaction size limit
///
/// Hard cap for serialized transaction size to prevent
/// excessively large transactions that could impact network performance.
pub const MAX_TX_SIZE: usize = 1024 * 1024; // 1 MiB hard cap for serialized tx

/// Limiti fee di default (configurabili)
pub const DEFAULT_FEE_LIMITS: FeeLimits = FeeLimits {
    min_fee: 100_000_000_000_000,       // 0.0001 token
    max_fee: 1_000_000_000_000_000_000, // 1.0 token
};

/// Validate transaction signature
pub fn validate_transaction_signature(tx: &SignedTx) -> anyhow::Result<bool> {
    use ed25519_dalek::Verifier;
    use sha2::{Digest, Sha256};

    // Create message from transaction data
    let message = format!(
        "{}:{}:{}:{}:{}",
        hex::encode(&tx.from),
        hex::encode(&tx.to),
        tx.amount,
        tx.nonce,
        tx.fee.unwrap_or(0)
    );

    let message_hash = Sha256::digest(message.as_bytes());

    // Parse signature
    let signature = ed25519_dalek::Signature::try_from(tx.sig.as_ref())
        .map_err(|e| anyhow::anyhow!("Invalid signature: {}", e))?;

    // Parse public key
    let public_key = ed25519_dalek::VerifyingKey::from_bytes(
        &tx.pubkey
            .clone()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid public key length"))?,
    )
    .map_err(|e| anyhow::anyhow!("Invalid public key: {}", e))?;

    // Verify signature
    Ok(public_key.verify(&message_hash, &signature).is_ok())
}

/// Create a new transaction
pub fn create_transaction(
    from: &[u8; 32],
    to: &[u8; 32],
    amount: u128,
    nonce: u64,
    fee: u128,
) -> anyhow::Result<Transaction> {
    if amount == 0 {
        anyhow::bail!("Amount must be greater than zero");
    }
    if fee < DEFAULT_FEE_LIMITS.min_fee {
        anyhow::bail!("Fee below minimum: {}", DEFAULT_FEE_LIMITS.min_fee);
    }
    if fee > DEFAULT_FEE_LIMITS.max_fee {
        anyhow::bail!("Fee above maximum: {}", DEFAULT_FEE_LIMITS.max_fee);
    }

    Ok(Transaction {
        from: from.to_vec(),
        to: to.to_vec(),
        amount,
        nonce,
        fee: Some(fee),
        pubkey: vec![],                    // Will be filled during signing
        sig: ByteBuf::from(vec![0u8; 64]), // Signature will be added during signing process
        pre_verified: false,
    })
}

/// Generate transaction hash
pub fn generate_tx_hash(tx: &Transaction) -> anyhow::Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    // Create canonical transaction data for hashing
    let tx_data = format!(
        "{}:{}:{}:{}:{}:{}:{}",
        hex::encode(&tx.from),
        hex::encode(&tx.to),
        tx.amount,
        tx.nonce,
        tx.fee.unwrap_or(0),
        hex::encode(&tx.pubkey),
        hex::encode(tx.sig.as_ref())
    );

    let hash = Sha256::digest(tx_data.as_bytes());
    let mut result = [0u8; 32];
    result.copy_from_slice(&hash);
    Ok(result)
}

/// Get transaction fee
pub fn get_tx_fee(tx: &Transaction) -> u128 {
    tx.fee.unwrap_or(0)
}

/// Estimate transaction gas
pub fn estimate_tx_gas(tx: &Transaction) -> anyhow::Result<u64> {
    // Base gas for standard transaction
    let mut gas = 21000;

    // Additional gas for amount (0 if zero)
    if tx.amount > 0 {
        gas += 9000; // Transfer gas
    }

    // Additional gas for data size
    gas += tx.from.len() as u64 * 68; // 68 gas per byte for non-zero data
    gas += tx.to.len() as u64 * 68;

    // Additional gas for larger amounts
    if tx.amount > 1000000_000_000_000_000 {
        // > 1 token
        gas += 10000;
    }

    Ok(gas)
}

/// Check if transaction is contract deployment
pub fn is_contract_deployment(tx: &Transaction) -> bool {
    // Contract deployment if recipient is zero address (0x000...000)
    tx.to == vec![0u8; 20] || tx.to == vec![0u8; 32] // Check both 20 and 32 byte zero addresses
}

/// Get transaction recipient
pub fn get_recipient(tx: &Transaction) -> &[u8] {
    &tx.to
}

/// Get transaction sender
pub fn get_sender(tx: &Transaction) -> &[u8] {
    &tx.from
}

/// Get transaction value
pub fn get_value(tx: &Transaction) -> u128 {
    tx.amount
}

/// Get transaction nonce
pub fn get_nonce(tx: &Transaction) -> u64 {
    tx.nonce
}

/// Set transaction nonce
pub fn set_nonce(tx: &mut Transaction, nonce: u64) {
    tx.nonce = nonce;
}

/// Set transaction fee
pub fn set_fee(tx: &mut Transaction, fee: u128) {
    tx.fee = Some(fee);
}

/// Mark transaction as pre-verified
pub fn mark_pre_verified(tx: &mut Transaction) {
    tx.pre_verified = true;
}

/// Check if transaction is pre-verified
pub fn is_pre_verified(tx: &Transaction) -> bool {
    tx.pre_verified
}

/// Create deployment transaction
pub fn create_deployment_transaction(
    from: &[u8; 32],
    bytecode: Vec<u8>,
    nonce: u64,
    fee: u128,
) -> anyhow::Result<DeployTransaction> {
    if bytecode.is_empty() {
        anyhow::bail!("Bytecode cannot be empty for deployment");
    }
    if bytecode.len() > 24576 {
        // 24KB limit
        anyhow::bail!("Bytecode too large: {} bytes (max 24576)", bytecode.len());
    }
    if fee < DEFAULT_FEE_LIMITS.min_fee * 10 {
        // Higher fee for deployment
        anyhow::bail!(
            "Deployment fee too low: minimum {}",
            DEFAULT_FEE_LIMITS.min_fee * 10
        );
    }

    Ok(DeployTransaction {
        from: from.to_vec(),
        bytecode,
        nonce,
        fee: Some(fee),
        pubkey: vec![], // Will be filled during signing
        pre_verified: false,
    })
}

/// Convert signed transaction to deployment
pub fn signed_tx_to_deployment(tx: &SignedTx) -> anyhow::Result<DeployTransaction> {
    if !is_contract_deployment(&tx.to_transaction()) {
        anyhow::bail!("Not a contract deployment transaction");
    }

    Ok(DeployTransaction {
        from: tx.from.clone(),
        bytecode: vec![], // Bytecode extraction would need additional transaction fields
        nonce: tx.nonce,
        fee: tx.fee,
        pubkey: tx.pubkey.clone(),
        pre_verified: tx.pre_verified,
    })
}

/// Convert deployment to signed transaction
pub fn deployment_to_signed_tx(deployment: &DeployTransaction) -> anyhow::Result<SignedTx> {
    Ok(SignedTx {
        from: deployment.from.clone(),
        to: vec![0u8; 32], // Zero address for contract deployment
        amount: 0,         // No value for deployment
        nonce: deployment.nonce,
        fee: deployment.fee,
        pubkey: deployment.pubkey.clone(),
        sig: ByteBuf::from(vec![0u8; 64]), // Signature will be added during signing process
        pre_verified: deployment.pre_verified,
    })
}

/// Calculate contract address from deployment transaction
pub fn calculate_contract_address(deployer: &[u8], nonce: u64) -> [u8; 32] {
    use sha3::{Digest, Keccak256};

    let input = [deployer, &nonce.to_le_bytes()].concat();
    let hash = Keccak256::digest(&input);
    let mut address = [0u8; 32];
    address.copy_from_slice(&hash);
    address
}

/// Execute contract constructor
///
/// This function executes a smart contract constructor with the given bytecode and arguments.
/// In a production environment, this would use a proper EVM or WASM interpreter.
/// For now, it simulates execution with basic gas calculation and state initialization.
pub fn execute_contract_constructor(
    bytecode: &[u8],
    args: &[u8],
    deployer: &[u8],
    value: u128,
    gas_limit: u64,
    cache: &mut ExecutorAccountCache,
) -> Result<ConstructorResult, anyhow::Error> {
    // Validate inputs
    if bytecode.is_empty() {
        anyhow::bail!("Bytecode cannot be empty");
    }
    if gas_limit < 21000 {
        anyhow::bail!("Gas limit too low: minimum 21000");
    }

    // Calculate gas cost
    let mut gas_used = 21000; // Base deployment cost
    gas_used += bytecode.len() as u64 * 200; // 200 gas per byte of bytecode
    gas_used += args.len() as u64 * 68; // 68 gas per byte of arguments

    if gas_used > gas_limit {
        anyhow::bail!("Insufficient gas: needed {}, limit {}", gas_used, gas_limit);
    }

    // Simulate contract state initialization
    let mut initial_state = Vec::new();

    // Add contract metadata to state
    initial_state.extend_from_slice(&(bytecode.len() as u32).to_le_bytes());
    initial_state.extend_from_slice(&(args.len() as u32).to_le_bytes());
    initial_state.extend_from_slice(&(value as u128).to_le_bytes());
    initial_state.extend_from_slice(&(gas_used as u64).to_le_bytes());

    // Add deployer address to state
    initial_state.extend_from_slice(deployer);

    // Add bytecode hash for integrity verification
    use sha2::{Digest, Sha256};
    let bytecode_hash = Sha256::digest(bytecode);
    initial_state.extend_from_slice(&bytecode_hash);

    // Cache the initial contract state
    let contract_address = calculate_contract_address(deployer, 0); // Use nonce 0 for deployment
    cache
        .insert_contract_state(&contract_address, initial_state.clone())
        .map_err(|e| anyhow::anyhow!("Failed to cache contract state: {}", e))?;

    info!(
        "Contract constructor executed: address={}, gas_used={}, bytecode_size={}",
        hex::encode(contract_address),
        gas_used,
        bytecode.len()
    );

    Ok(ConstructorResult {
        initial_state,
        gas_used,
    })
}

/// Execute contract call
///
/// This function executes a smart contract call with the given calldata and parameters.
/// In a production environment, this would use a proper EVM or WASM interpreter.
/// For now, it simulates execution with basic gas calculation and state updates.
pub fn execute_contract_call(
    contract_address: &[u8],
    calldata: &[u8],
    _sender: &[u8],
    value: u128,
    gas_limit: u64,
    cache: &mut ExecutorAccountCache,
) -> Result<CallResult, anyhow::Error> {
    // Validate inputs
    if contract_address.is_empty() {
        anyhow::bail!("Contract address cannot be empty");
    }
    if gas_limit < 21000 {
        anyhow::bail!("Gas limit too low: minimum 21000");
    }

    // Retrieve contract state from cache
    let current_state = cache
        .get_contract_state(contract_address)
        .map_err(|e| anyhow::anyhow!("Failed to retrieve contract state: {}", e))?;

    if current_state.is_empty() {
        anyhow::bail!(
            "Contract not found at address: {}",
            hex::encode(contract_address)
        );
    }

    // Calculate gas cost
    let mut gas_used = 21000; // Base call cost
    gas_used += calldata.len() as u64 * 68; // 68 gas per byte of calldata
    gas_used += value.checked_mul(10).unwrap_or(0) as u64; // Additional gas for value transfer

    if gas_used > gas_limit {
        anyhow::bail!("Insufficient gas: needed {}, limit {}", gas_used, gas_limit);
    }

    // Simulate contract execution
    let mut new_state = current_state.clone();
    let mut return_data = Vec::new();

    // Parse function selector (first 4 bytes of calldata)
    if calldata.len() >= 4 {
        let function_selector = &calldata[0..4];

        // Simulate different function calls based on selector
        match function_selector {
            [0x60, 0x61, 0x62, 0x63] => {
                // Example: "abc" selector
                // Simulate getter function - return contract metadata
                return_data.extend_from_slice(&new_state[0..32]); // Return first 32 bytes
            }
            [0x64, 0x65, 0x66, 0x67] => {
                // Example: "def" selector
                // Simulate setter function - update state
                if calldata.len() >= 36 {
                    let new_value = &calldata[4..36]; // Next 32 bytes
                    if new_state.len() >= 68 {
                        new_state[64..96].copy_from_slice(new_value);
                    }
                }
                return_data.extend_from_slice(&[1u8]); // Success indicator
            }
            _ => {
                // Unknown function selector - return error
                return_data.extend_from_slice(&[0u8]); // Error indicator
            }
        }
    }

    // Update contract state in cache
    cache
        .insert_contract_state(contract_address, new_state.clone())
        .map_err(|e| anyhow::anyhow!("Failed to update contract state: {}", e))?;

    info!(
        "Contract call executed: address={}, gas_used={}, calldata_size={}, value={}",
        hex::encode(contract_address),
        gas_used,
        calldata.len(),
        value
    );

    Ok(CallResult {
        new_state,
        return_data,
        gas_used,
    })
}
