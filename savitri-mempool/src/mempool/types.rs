use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use savitri_core::core::types::Account;
use savitri_storage::{Storage, StorageTrait};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Transaction classification for class-aware processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TxClass {
    /// Financial transactions (transfers, payments)
    Financial,
    /// IoT data submissions (sensor readings, telemetry)
    IoTData,
    /// Federated learning updates (model gradients, aggregations)
    FederatedUpdate,
    /// System transactions (governance, upgrades, maintenance)
    System,
}

impl TxClass {
    /// Classify transaction based on content/pattern
    /// Analyzes transaction bytes to determine the appropriate class
    pub fn from_tx_bytes(bytes: &[u8]) -> Self {
        if bytes.len() < 32 {
            return TxClass::Financial; // Default for invalid/short transactions
        }

        // Analyze transaction patterns for classification
        let mut class_scores = std::collections::HashMap::new();

        // Pattern 1: Check for IoT data patterns
        // IoT transactions typically have small amounts, specific data patterns
        if bytes.len() >= 40 {
            let amount_bytes = &bytes[32..40]; // Assuming amount is at offset 32
            let amount = u64::from_le_bytes([
                amount_bytes[0],
                amount_bytes[1],
                amount_bytes[2],
                amount_bytes[3],
                amount_bytes[4],
                amount_bytes[5],
                amount_bytes[6],
                amount_bytes[7],
            ]);

            // IoT transactions typically have very small amounts (sensor data fees)
            if amount < 1000 && bytes.len() > 64 {
                class_scores.insert(TxClass::IoTData, 3);
            }
        }

        // Pattern 2: Check for federated learning patterns
        // Federated updates typically have larger data payloads and specific patterns
        if bytes.len() > 1024 {
            // Large transactions with data payloads are likely federated updates
            class_scores.insert(TxClass::FederatedUpdate, 2);
        }

        // Pattern 3: Check for system transaction patterns
        // System transactions often have specific addresses or patterns
        if bytes.len() >= 64 {
            let to_address = &bytes[16..48]; // Assuming to address is at offset 16

            // Check for system addresses (e.g., zero address, governance addresses)
            if to_address.iter().all(|&b| b == 0) {
                class_scores.insert(TxClass::System, 5);
            }

            // Check for known governance patterns
            if bytes.len() >= 68 {
                let data_field = &bytes[64..68];
                if data_field == [0xFF, 0xFF, 0xFF, 0xFF] {
                    class_scores.insert(TxClass::System, 4);
                }
            }
        }

        // Pattern 4: Check for smart contract calls (could be federated)
        if bytes.len() > 100 {
            // Look for function selector patterns (first 4 bytes of data)
            if bytes.len() >= 100 {
                let selector_slice = &bytes[96..100];
                let potential_selector: [u8; 4] = [
                    selector_slice[0],
                    selector_slice[1],
                    selector_slice[2],
                    selector_slice[3],
                ];
                // Common function selectors for federated learning
                let federated_selectors: [[u8; 4]; 3] = [
                    [0x12, 0x34, 0x56, 0x78], // Example: submit_gradient
                    [0x87, 0x65, 0x43, 0x21], // Example: aggregate_model
                    [0xAB, 0xCD, 0xEF, 0x01], // Example: update_weights
                ];

                if federated_selectors.contains(&potential_selector) {
                    class_scores.insert(TxClass::FederatedUpdate, 4);
                }
            }
        }

        // Determine the class with highest score
        if let Some((&best_class, &score)) = class_scores.iter().max_by_key(|(_, &score)| score) {
            if score >= 2 {
                return best_class;
            }
        }

        // Default to Financial for regular transactions
        TxClass::Financial
    }

    /// Get priority score for the class (higher = higher priority)
    pub fn priority_score(&self) -> u8 {
        match self {
            TxClass::System => 4, // Highest priority
            TxClass::FederatedUpdate => 3,
            TxClass::IoTData => 2,
            TxClass::Financial => 1, // Lowest priority
        }
    }

    /// Get description of the transaction class
    pub fn description(&self) -> &'static str {
        match self {
            TxClass::Financial => "Financial transaction (transfers, payments)",
            TxClass::IoTData => "IoT data submission (sensor readings, telemetry)",
            TxClass::FederatedUpdate => "Federated learning update (model gradients, aggregations)",
            TxClass::System => "System transaction (governance, upgrades, maintenance)",
        }
    }
}

/// Compact sender identifier (u32) instead of Vec<u8> address
/// This reduces memory footprint and improves cache locality
pub type SenderId = u32;

/// Handle to transaction bytes stored externally (not in mempool)
/// This allows mempool to avoid storing heavy data (bytes, signatures)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TxHandle(pub u64);

/// Contains only hot-path data needed for scheduling
#[derive(Debug, Clone)]
pub struct PrevalidatedTx {
    /// Compact sender identifier
    pub sender_id: SenderId,
    pub sender_address: [u8; 32],
    /// Transaction nonce
    pub nonce: u64,
    /// Maximum fee (for prioritization)
    pub max_fee: u64,
    /// Transfer amount carried by the TX (used by admission so its balance
    /// blocked_senders bug where admission accepted a TX on `balance >= fee`
    /// blacklist every later TX from the sender in the same drain round.
    pub amount: u64,
    /// Handle to actual transaction bytes (stored externally)
    pub tx_handle: TxHandle,
    /// Transaction class for class-aware scheduling
    pub class: TxClass,
    /// Stream nonce for IoT/FederatedUpdate (allows out-of-order for same stream)
    pub stream_nonce: Option<u64>,
}

/// Minimal mempool transaction (hot-path only)
/// Contains only data needed for enqueueing and round-robin scheduling
/// Enhanced with security fields for transaction ordering and replay prevention
#[derive(Debug, Clone)]
pub struct MempoolTx {
    /// Sender identifier
    pub sender_id: SenderId,
    /// Transaction nonce
    pub nonce: u64,
    /// Fee for prioritization
    pub fee: u64,
    /// Handle to transaction bytes
    pub tx_handle: TxHandle,
    /// Transaction class
    pub class: TxClass,
    /// Stream nonce (if applicable)
    pub stream_nonce: Option<u64>,
    /// Insertion timestamp (for TTL purge)
    pub inserted: std::time::Instant,
    /// Transaction hash for duplicate detection (optional, for cleanup)
    pub tx_hash: Option<[u8; 32]>,

    // ⭐ NUOVI CAMPI CRITICI PER SECURITY (FASE 3)
    pub sender_address: Vec<u8>,
    /// Signature hash for replay detection and deduplication
    pub signature_hash: [u8; 32],
    /// Gas limit for accurate fee calculation
    pub gas_limit: u64,
    pub max_fee: u64,
    /// Reception timestamp for temporal ordering and priority
    pub received_at: std::time::Instant,
    /// (path locale, non gossip cross-LN). Quando true, drain_for_block_production
    /// la include nel blocco anche se shard_filter la classificherebbe "remote",
    /// permettendo al LN-RPC di chiudere il commit pipeline con TX accepted da
    /// utenti diretti (in attesa of the fix architetturale TxFetch path → Opzione C).
    pub rpc_accepted: bool,
}

impl From<PrevalidatedTx> for MempoolTx {
    fn from(pv: PrevalidatedTx) -> Self {
        MempoolTx {
            sender_id: pv.sender_id,
            nonce: pv.nonce,
            fee: pv.max_fee,
            tx_handle: pv.tx_handle,
            class: pv.class,
            stream_nonce: pv.stream_nonce,
            inserted: std::time::Instant::now(),
            tx_hash: None, // Will be set when hash is available

            sender_address: pv.sender_address.to_vec(),
            signature_hash: [0u8; 32], // Will be computed from full transaction
            gas_limit: 0,              // Will be extracted from transaction bytes
            max_fee: pv.max_fee,       // Already available in PrevalidatedTx
            received_at: std::time::Instant::now(),
            rpc_accepted: false, // default: gossip-received; sovrascritto in process_single_raw_transaction
        }
    }
}

impl MempoolTx {
    /// Create a new MempoolTx with all security fields (FASE 3)
    ///
    /// # Arguments
    /// * `sender_id` - Compact sender identifier
    /// * `nonce` - Transaction nonce
    /// * `fee` - Transaction fee
    /// * `max_fee` - Maximum fee willing to pay
    /// * `gas_limit` - Gas limit for accurate fee calculation
    /// * `tx_handle` - Handle to transaction bytes
    /// * `class` - Transaction class
    /// * `signature_hash` - Hash of signature for replay detection
    /// * `stream_nonce` - Optional stream nonce
    pub fn new_with_security_fields(
        sender_id: SenderId,
        sender_address: Vec<u8>,
        nonce: u64,
        fee: u64,
        max_fee: u64,
        gas_limit: u64,
        tx_handle: TxHandle,
        class: TxClass,
        signature_hash: [u8; 32],
        stream_nonce: Option<u64>,
    ) -> Self {
        let now = std::time::Instant::now();
        Self {
            sender_id,
            nonce,
            fee,
            tx_handle,
            class,
            stream_nonce,
            inserted: now,
            tx_hash: None,

            // ⭐ NUOVI CAMPI CRITICI
            sender_address,
            signature_hash,
            gas_limit,
            max_fee,
            received_at: now,
            rpc_accepted: false,
        }
    }

    /// Get transaction priority score for fee-based conflict resolution
    /// Higher score = higher priority
    pub fn priority_score(&self) -> f64 {
        // Priority based on fee/gas_limit ratio + time bonus
        let fee_ratio = if self.gas_limit > 0 {
            self.fee as f64 / self.gas_limit as f64
        } else {
            0.0
        };

        // Add small time bonus for earlier transactions (0.01 per second)
        let time_bonus = self.received_at.elapsed().as_secs_f64() * 0.01;

        fee_ratio + time_bonus
    }

    /// Check if this transaction conflicts with another (same sender + nonce)
    pub fn conflicts_with(&self, other: &MempoolTx) -> bool {
        self.sender_address == other.sender_address && self.nonce == other.nonce
    }

    /// Check if this transaction has higher priority than a conflicting one
    pub fn has_higher_priority_than(&self, other: &MempoolTx) -> bool {
        // First compare fee, then reception time if tie
        if self.fee != other.fee {
            self.fee > other.fee
        } else {
            self.received_at < other.received_at
        }
    }
}

/// Raw transaction from network/RPC layer
#[derive(Debug, Clone)]
pub struct RawTx {
    /// Transaction bytes
    pub bytes: Vec<u8>,
    /// Peer ID (for rate limiting)
    pub peer_id: Option<u64>,
    /// Reception timestamp
    pub recv_ts: std::time::Instant,
}

// Missing type definitions
#[derive(Debug, Clone)]
pub struct CallTransaction {
    pub caller: Vec<u8>,
    pub pubkey: Vec<u8>,
    pub calldata: Vec<u8>,
    pub nonce: u64,
    pub fee: u64,
    pub sig: Vec<u8>,
    pub pre_verified: bool,
    pub function_selector: Vec<u8>,
    pub contract_address: Vec<u8>,
    pub call_data: Vec<u8>,
    pub value: u128,
    pub gas_limit: u64,
    pub timestamp: u64,
}

impl CallTransaction {
    /// Verify the call transaction structure and content
    pub fn verify(&self) -> anyhow::Result<()> {
        // Verify contract address is not zero
        if self.contract_address.iter().all(|&b| b == 0) {
            return Err(anyhow::anyhow!("Contract address cannot be zero"));
        }

        // Verify contract address length
        if self.contract_address.len() != 20 && self.contract_address.len() != 32 {
            return Err(anyhow::anyhow!("Invalid contract address length"));
        }

        // Verify function selector
        if self.function_selector.len() != 4 {
            return Err(anyhow::anyhow!("Function selector must be 4 bytes"));
        }

        // Verify call data is reasonable size
        if self.call_data.len() > 100_000 {
            // 100KB limit
            return Err(anyhow::anyhow!("Call data too large"));
        }

        // Verify value is reasonable
        if self.value > u128::MAX / 2 {
            // Prevent overflow issues
            return Err(anyhow::anyhow!("Transaction value too large"));
        }

        // Verify gas limit is reasonable
        if self.gas_limit == 0 {
            return Err(anyhow::anyhow!("Gas limit cannot be zero"));
        }

        if self.gas_limit > 10_000_000 {
            // 10 million gas limit
            return Err(anyhow::anyhow!("Gas limit too high"));
        }

        // Verify timestamp is not too old or in future
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if self.timestamp > now + 300 {
            // 5 minutes in future
            return Err(anyhow::anyhow!(
                "Transaction timestamp is too far in future"
            ));
        }

        if self.timestamp < now.saturating_sub(3600) {
            // 1 hour ago
            return Err(anyhow::anyhow!("Transaction timestamp is too old"));
        }

        Ok(())
    }

    /// Get the gas cost estimate for this call
    pub fn estimate_gas_cost(&self) -> u64 {
        let base_cost = 21_000; // Base transaction cost
        let call_data_cost = self.call_data.len() as u64 * 16; // 16 gas per byte
        let contract_cost = if self.contract_address.len() == 20 {
            6_000
        } else {
            0
        }; // Contract creation cost

        base_cost + call_data_cost + contract_cost
    }

    /// Get the total cost (gas * gas_price + value)
    pub fn get_total_cost(&self, gas_price: u64) -> u128 {
        let gas_cost = self.gas_limit as u128 * gas_price as u128;
        gas_cost + self.value
    }
}

#[derive(Debug, Clone)]
pub struct PipelinePrefetcher;

impl PipelinePrefetcher {
    pub fn new(_storage: Arc<dyn StorageTrait>) -> Self {
        Self
    }
}
// Missing transaction types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignedTx {
    pub from: Vec<u8>,
    pub to: Vec<u8>,
    pub amount: u64,
    pub nonce: u64,
    pub fee: u64,
    pub pubkey: Vec<u8>,
    pub sig: Vec<u8>,
    pub pre_verified: bool,
}

impl SignedTx {
    /// Verify the signed transaction structure and signature
    pub fn verify(&self) -> anyhow::Result<()> {
        // Verify addresses are not zero
        if self.from.iter().all(|&b| b == 0) {
            return Err(anyhow::anyhow!("From address cannot be zero"));
        }

        if self.to.iter().all(|&b| b == 0) {
            return Err(anyhow::anyhow!("To address cannot be zero"));
        }

        // Verify address lengths
        if self.from.len() != 32 {
            return Err(anyhow::anyhow!("Invalid from address length"));
        }

        if self.to.len() != 32 {
            return Err(anyhow::anyhow!("Invalid to address length"));
        }

        // Verify public key length
        if self.pubkey.len() != 32 {
            return Err(anyhow::anyhow!("Invalid public key length"));
        }

        // Verify signature length
        if self.sig.len() != 64 {
            return Err(anyhow::anyhow!("Invalid signature length"));
        }

        // Verify amounts are reasonable
        if self.amount == 0 {
            return Err(anyhow::anyhow!("Amount cannot be zero"));
        }

        if self.amount > u64::MAX / 2 {
            return Err(anyhow::anyhow!("Amount too large"));
        }

        // Verify fee is reasonable
        if self.fee == 0 {
            return Err(anyhow::anyhow!("Fee cannot be zero"));
        }

        if self.fee > 1_000_000_000 {
            // 1 SAVT max fee
            return Err(anyhow::anyhow!("Fee too high"));
        }

        // Verify nonce is reasonable
        if self.nonce > u64::MAX / 2 {
            return Err(anyhow::anyhow!("Nonce too large"));
        }

        // Verify signature matches public key
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        use sha2::{Digest, Sha256};

        // Create message for signature verification
        let mut message = Vec::new();
        message.extend_from_slice(&self.from);
        message.extend_from_slice(&self.to);
        message.extend_from_slice(&self.amount.to_le_bytes());
        message.extend_from_slice(&self.nonce.to_le_bytes());
        message.extend_from_slice(&self.fee.to_le_bytes());

        // Hash the message
        let message_hash = Sha256::digest(&message);

        // Verify signature - convert Vec<u8> to fixed arrays
        let pubkey_array: [u8; 32] = self
            .pubkey
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid public key length"))?;
        let public_key = VerifyingKey::from_bytes(&pubkey_array)
            .map_err(|_| anyhow::anyhow!("Invalid public key"))?;

        let sig_array: [u8; 64] = self
            .sig
            .as_slice()
            .try_into()
            .map_err(|_| anyhow::anyhow!("Invalid signature length"))?;
        let signature = Signature::from_bytes(&sig_array);

        public_key
            .verify(&message_hash, &signature)
            .map_err(|_| anyhow::anyhow!("Signature verification failed"))?;

        Ok(())
    }

    /// Get the transaction hash
    pub fn hash(&self) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();
        hasher.update(&self.from);
        hasher.update(&self.to);
        hasher.update(&self.amount.to_le_bytes());
        hasher.update(&self.nonce.to_le_bytes());
        hasher.update(&self.fee.to_le_bytes());
        hasher.update(&self.pubkey);
        hasher.update(&self.sig);

        let mut result = [0u8; 32];
        result.copy_from_slice(&hasher.finalize());
        result
    }

    /// Get the total cost (amount + fee)
    pub fn get_total_cost(&self) -> u128 {
        self.amount as u128 + self.fee as u128
    }

    /// Check if this is a contract creation transaction
    pub fn is_contract_creation(&self) -> bool {
        self.to.iter().all(|&b| b == 0)
    }

    /// Get the effective fee rate (fee / data_size)
    pub fn get_fee_rate(&self) -> f64 {
        let data_size = 32 + 32 + 8 + 8 + 8 + 32 + 64; // from + to + amount + nonce + fee + pubkey + sig
        self.fee as f64 / data_size as f64
    }
}

// Helper function for creating default SignedTx
pub fn create_default_signed_tx() -> SignedTx {
    SignedTx {
        from: vec![0u8; 32],
        to: vec![0u8; 32],
        amount: 0,
        nonce: 0,
        fee: 0,
        pubkey: vec![0u8; 32],
        sig: vec![0u8; 32],
        pre_verified: true,
    }
}
// Missing system types
#[derive(Debug, Clone)]
pub struct SigVerifyStage;

impl SigVerifyStage {
    pub fn new() -> Self {
        Self
    }
}

#[derive(Debug, Clone)]
pub struct OracleValidator {
    config: OracleConfig,
}

#[derive(Debug, Clone)]
pub struct OracleConfig {
    pub timeout_ms: u64,
    pub max_retries: u32,
}

impl OracleConfig {
    pub fn default() -> Self {
        Self {
            timeout_ms: 5000,
            max_retries: 3,
        }
    }
}

impl OracleValidator {
    pub fn new(config: OracleConfig) -> Self {
        Self { config }
    }
}
// Storage trait extensions for missing methods
pub trait MemoryStorageExt {
    fn get_account(&self, address: &[u8]) -> anyhow::Result<Account>;
    fn get_accounts_batch(&self, addresses: &[&[u8]]) -> anyhow::Result<Vec<Account>>;
}

impl MemoryStorageExt for Arc<dyn StorageTrait> {
    fn get_account(&self, address: &[u8]) -> anyhow::Result<Account> {
        Ok(Account::default())
    }

    fn get_accounts_batch(&self, addresses: &[&[u8]]) -> anyhow::Result<Vec<Account>> {
        Ok(addresses.iter().map(|_| Account::default()).collect())
    }
}
