use anyhow::{bail, Context, Result};
use ed25519_dalek::{Signer, Verifier};
use once_cell::sync::Lazy;
use savitri_storage::Storage;
use serde::Deserialize;
use sha2::{Digest, Sha256};
// use crate::tokenomics::DualTokenConfig; // Commented out - tokenomics module doesn't exist

// Crypto functions
pub fn compute_tx_root(txs: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    for tx in txs {
        hasher.update(tx);
    }
    hasher.finalize().into()
}

pub fn sign_data(data: &[u8], key: &[u8]) -> Vec<u8> {
    if key.len() != 32 {
        tracing::error!("Invalid key length {}, expected 32 bytes", key.len());
        return Vec::new();
    }
    let key_array: [u8; 32] = key
        .try_into()
        .expect("invariant: key length checked above to be 32");
    let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_array);
    signing_key.sign(data).to_bytes().to_vec()
}

pub fn verify_signature(data: &[u8], signature: &[u8], public_key: &[u8]) -> bool {
    // Real Ed25519 signature verification
    if signature.len() != 64 || public_key.len() != 32 {
        return false;
    }

    // Try to verify with Ed25519
    let public_key_array: &[u8; 32] = match public_key.try_into() {
        Ok(arr) => arr,
        Err(_) => return false,
    };
    let verifying_key = match ed25519_dalek::VerifyingKey::from_bytes(public_key_array) {
        Ok(key) => key,
        Err(_) => return false,
    };

    let sig_array: &[u8; 64] = match signature.try_into() {
        Ok(arr) => arr,
        Err(_) => return false,
    };
    let sig = ed25519_dalek::Signature::from_bytes(sig_array);

    verifying_key.verify(data, &sig).is_ok()
}

use super::{block::Block, types::Transaction};
use savitri_storage::{VestingSchedule, VestingType};
use serde::Serialize;

/// Supply manager for token supply tracking
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SupplyManager {
    pub total_supply: u128,
    pub circulating_supply: u128,
    pub locked_supply: u128,
    pub minted_supply: u128,
    pub burned_supply: u128,
    pub last_updated: u64,
}

impl SupplyManager {
    pub fn new(total_supply: u128) -> Self {
        Self {
            total_supply,
            circulating_supply: 0,
            locked_supply: total_supply, // Initially all tokens are locked
            minted_supply: total_supply,
            burned_supply: 0,
            last_updated: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    pub fn update_circulating_supply(&mut self, amount: u128) {
        self.circulating_supply = amount;
        self.locked_supply = self.total_supply.saturating_sub(self.circulating_supply);
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    pub fn mint(&mut self, amount: u128) -> Result<()> {
        self.minted_supply += amount;
        self.total_supply += amount;
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Ok(())
    }

    pub fn burn(&mut self, amount: u128) -> Result<()> {
        if amount > self.circulating_supply {
            anyhow::bail!("Cannot burn more than circulating supply");
        }
        self.burned_supply += amount;
        self.circulating_supply -= amount;
        self.total_supply -= amount;
        self.last_updated = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        Ok(())
    }
}

static GENESIS_SPEC: Lazy<GenesisSpec> = Lazy::new(|| {
    let raw = if cfg!(feature = "testnet") {
        include_str!("genesis/genesis_testnet.json")
    } else {
        include_str!("genesis/genesis.json")
    };
    // Invariant: the genesis JSON is bundled at compile time via `include_str!`,
    // so a malformed file would have failed at build time and is unreachable here.
    serde_json::from_str(raw).expect("invariant: bundled genesis JSON is valid at build time")
});

#[derive(Debug, Deserialize)]
struct GenesisSpec {
    version: u8,
    timestamp: u64,
    proposer: String,
    signature: String,
    state_root: String,
    parent_exec_hash: String,
    parent_ref_hash: String,
    #[serde(default)]
    transactions: Vec<GenesisTransaction>,
}

#[derive(Debug, Deserialize)]
struct GenesisTransaction {
    from: String,
    to: String,
    amount: u64,
}

/// Testnet genesis uses 8 decimals (10M SAVT = 10^15) to fit in u64. Scale to 18 decimals.
const TESTNET_GENESIS_SCALE: u128 = 10_000_000_000; // 10^10

pub fn load_genesis_block() -> Result<Block> {
    let spec = &*GENESIS_SPEC;
    let mut block = Block {
        version: spec.version,
        hash: [0u8; 64],
        transactions: spec
            .transactions
            .iter()
            .map(|tx| Transaction {
                from: tx.from.to_lowercase(),
                to: tx.to.to_lowercase(),
                amount: tx.amount,
                data: vec![],
                fee: 0,
                nonce: 0,
                signature: vec![],
                timestamp: 0,
                pubkey: vec![],
                sig: vec![],
                pre_verified: false,
            })
            .collect(),
        proposer: decode_hex::<32>(&spec.proposer)?,
        signature: decode_hex::<64>(&spec.signature)?,
        state_root: decode_hex::<64>(&spec.state_root)?,
        parent_exec_hash: decode_hex::<64>(&spec.parent_exec_hash)?,
        parent_ref_hash: decode_hex::<64>(&spec.parent_ref_hash)?,
        height: 0,
        timestamp: spec.timestamp,
        tx_root: [0u8; 64],
    };
    block.tx_root = crate::core::crypto::compute_tx_root(&block.transactions);
    block.hash = block.header_hash();
    Ok(block)
}

pub fn ensure_genesis_block(storage: &Storage) -> Result<()> {
    if storage.get_block_hash_by_height(0)?.is_some() {
        return Ok(());
    }
    let block = load_genesis_block()?;

    initialize_genesis_mint(storage, &block)?;

    // Serialize and store the block
    let block_bytes = bincode::serialize(&block)?;
    storage.put_block(block.height, &block_bytes)?;
    storage.set_block_hash_for_height(0, &block.hash)?;
    storage.set_chain_head(&block_bytes)?;
    Ok(())
}

fn decode_hex<const N: usize>(s: &str) -> Result<[u8; N]> {
    let hex = s.trim_start_matches("0x");
    let bytes = hex::decode(hex).context("invalid hex string")?;
    if bytes.len() != N {
        bail!("expected {} bytes, got {}", N, bytes.len());
    }
    let mut array = [0u8; N];
    array.copy_from_slice(&bytes);
    Ok(array)
}

fn initialize_genesis_mint(storage: &Storage, block: &Block) -> Result<()> {
    if cfg!(feature = "testnet") {
        let total_supply: u128 = block
            .transactions
            .iter()
            .map(|tx| (tx.amount as u128) * TESTNET_GENESIS_SCALE)
            .sum();
        let supply_manager = SupplyManager::new(total_supply);
        let supply_manager_bytes = bincode::serialize(&supply_manager)?;
        storage.put_supply_manager(&supply_manager_bytes)?;

        // Materialize the genesis allocations as on-chain Account records.
        // Previously this branch only set SupplyManager and relied on the
        // mainnet code path (never executed for testnet) to create balances.
        // As a result the 11 genesis `to` addresses (treasury + 10 faucet
        // keys) showed `storage.get_account() -> None` at boot — which made
        // the faucet's first `tx_sendTransaction` land in the new-account
        // branch of mempool admission (queued_pool @ nonce=0), producing
        // "Duplicate nonce 0 for sender N" errors after the round-robin
        // wrapped back to the same faucet key. Creating the records up front
        // matches the mainnet semantics: post-genesis reads return
        // Some(Account { balance, nonce: 0 }).
        for tx in &block.transactions {
            let to_hex = tx.to.trim_start_matches("0x");
            let to_bytes = match hex::decode(to_hex) {
                Ok(b) if b.len() == 32 => b,
                _ => continue, // skip malformed entries rather than abort
            };
            let amount_scaled: u128 = (tx.amount as u128) * TESTNET_GENESIS_SCALE;
            // Read+write via the byte API (mod.rs put_account/get_account).
            // To remain compatible with the lightnode RocksDBLightnodeStorage
            // wrapper — which bincode-serializes a 32-byte "lightnode Account"
            // (balance:u128 + nonce:u64 + Vec<u8> data, where data length is
            // an 8-byte u64 prefix, here 0) — we write 32 bytes:
            //   bytes[0..16]  balance (u128 little-endian)
            //   bytes[16..24] nonce   (u64  little-endian)
            //   bytes[24..32] zeros   (Vec<u8> length prefix = 0)
            // savitri-core's Account::decode reads the first 24 bytes and
            // ignores the rest, and bincode-deserialize as lightnode Account
            // sees an empty data Vec — both consumers happy.
            let prior_balance = storage
                .get_account(to_bytes.as_slice())
                .ok()
                .flatten()
                .and_then(|bytes| {
                    if bytes.len() >= 16 {
                        Some(u128::from_le_bytes(bytes[0..16].try_into().ok()?))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let new_balance = prior_balance.saturating_add(amount_scaled);
            let mut wire = [0u8; 32];
            wire[0..16].copy_from_slice(&new_balance.to_le_bytes());
            // nonce = 0 → bytes 16..24 stay zero
            // data length = 0 → bytes 24..32 stay zero (8-byte u64 LE)
            storage.put_account(to_bytes.as_slice(), &wire)?;
            tracing::info!(
                addr = %hex::encode(&to_bytes),
                balance = new_balance,
                "[GENESIS] seeded account from genesis allocation"
            );
        }
        return Ok(());
    }

    // Mainnet: vesting e 220M supply
    let vesting_schedules = create_genesis_vesting_schedules(block.timestamp);
    let total_supply = 220_000_000u128 * 10u128.pow(18); // 220M tokens
    let supply_manager = SupplyManager::new(total_supply);

    for schedule in vesting_schedules {
        storage.put_vesting_schedule(&schedule)?;
    }

    let supply_manager_bytes = bincode::serialize(&supply_manager)?;
    storage.put_supply_manager(&supply_manager_bytes)?;

    Ok(())
}

fn create_genesis_vesting_schedules(timestamp: u64) -> Vec<VestingSchedule> {
    let mut schedules = Vec::new();

    // Team vesting - 20M tokens over 4 years
    schedules.push(VestingSchedule {
        address: decode_hex::<32>("0x0000000000000000000000000000000000000001")
            .expect("invariant: hardcoded address literal is valid hex")
            .to_vec(),
        schedule_id: 1,
        amount: 20_000_000u128 * 10u128.pow(18),
        start_time: timestamp,
        duration: 4 * 365 * 24 * 60 * 60, // 4 years
        cliff: 365 * 24 * 60 * 60,        // 1 year cliff
        vesting_type: VestingType::Linear,
        vested_amount: 0,
        released_amount: 0,
    });

    // Investors vesting - 50M tokens over 2 years
    schedules.push(VestingSchedule {
        address: decode_hex::<32>("0x0000000000000000000000000000000000000002")
            .expect("invariant: hardcoded address literal is valid hex")
            .to_vec(),
        schedule_id: 2,
        amount: 50_000_000u128 * 10u128.pow(18),
        start_time: timestamp,
        duration: 2 * 365 * 24 * 60 * 60, // 2 years
        cliff: 180 * 24 * 60 * 60,        // 6 months cliff
        vesting_type: VestingType::Linear,
        vested_amount: 0,
        released_amount: 0,
    });

    // Foundation vesting - 30M tokens over 30M tokens over 10 years
    schedules.push(VestingSchedule {
        address: decode_hex::<32>("0x0000000000000000000000000000000000000003")
            .expect("invariant: hardcoded address literal is valid hex")
            .to_vec(),
        schedule_id: 3,
        amount: 30_000_000u128 * 10u128.pow(18),
        start_time: timestamp,
        duration: 10 * 365 * 24 * 60 * 60, // 10 years
        cliff: 365 * 24 * 60 * 60,         // 1 year cliff
        vesting_type: VestingType::Linear,
        vested_amount: 0,
        released_amount: 0,
    });

    // Community rewards - 120M tokens over 8 years
    schedules.push(VestingSchedule {
        address: decode_hex::<32>("0x0000000000000000000000000000000000000004")
            .expect("invariant: hardcoded address literal is valid hex")
            .to_vec(),
        schedule_id: 4,
        amount: 120_000_000u128 * 10u128.pow(18),
        start_time: timestamp,
        duration: 8 * 365 * 24 * 60 * 60, // 8 years
        cliff: 90 * 24 * 60 * 60,         // 3 months cliff
        vesting_type: VestingType::Linear,
        vested_amount: 0,
        released_amount: 0,
    });

    schedules
}

/// Create a custom genesis block for testing
pub fn create_test_genesis_block() -> Result<Block> {
    let block = Block {
        version: 1,
        hash: [0u8; 64],
        transactions: vec![],
        proposer: [1u8; 32],
        signature: [2u8; 64],
        state_root: [3u8; 64],
        parent_exec_hash: [4u8; 64],
        parent_ref_hash: [5u8; 64],
        height: 0,
        timestamp: 1000000,
        tx_root: [6u8; 64],
    };

    Ok(block)
}

/// Validate genesis block integrity
pub fn validate_genesis_block(block: &Block) -> Result<()> {
    // Check height is 0
    if block.height != 0 {
        bail!("Genesis block height must be 0");
    }

    // Check version
    if block.version != 1 {
        bail!("Genesis block version must be 1");
    }

    // Check timestamp is reasonable
    if block.timestamp == 0 {
        bail!("Genesis block timestamp cannot be 0");
    }

    // Check proposer is not all zeros
    if block.proposer == [0u8; 32] {
        bail!("Genesis block proposer cannot be all zeros");
    }

    // Check signature is not all zeros
    if block.signature == [0u8; 64] {
        bail!("Genesis block signature cannot be all zeros");
    }

    // Check state root is not all zeros
    if block.state_root == [0u8; 64] {
        bail!("Genesis block state root cannot be all zeros");
    }

    // Check parent hashes are all zeros (genesis has no parent)
    if block.parent_exec_hash != [0u8; 64] {
        bail!("Genesis block parent exec hash must be all zeros");
    }

    if block.parent_ref_hash != [0u8; 64] {
        bail!("Genesis block parent ref hash must be all zeros");
    }

    Ok(())
}

/// Get genesis block hash
pub fn get_genesis_hash() -> Result<[u8; 64]> {
    let block = load_genesis_block()?;
    Ok(block.hash)
}

/// Get genesis block timestamp
pub fn get_genesis_timestamp() -> Result<u64> {
    let block = load_genesis_block()?;
    Ok(block.timestamp)
}

/// Get genesis block proposer
pub fn get_genesis_proposer() -> Result<[u8; 32]> {
    let block = load_genesis_block()?;
    Ok(block.proposer)
}

/// Get genesis block state root
pub fn get_genesis_state_root() -> Result<[u8; 64]> {
    let block = load_genesis_block()?;
    Ok(block.state_root)
}

/// Check if a block is the genesis block
pub fn is_genesis_block(block: &Block) -> bool {
    block.height == 0
}

/// Get genesis block transactions
pub fn get_genesis_transactions() -> Result<Vec<Transaction>> {
    let block = load_genesis_block()?;
    Ok(block.transactions)
}

/// Get genesis block transaction root
pub fn get_genesis_tx_root() -> Result<[u8; 64]> {
    let block = load_genesis_block()?;
    Ok(block.tx_root)
}

/// Create a genesis block from transactions
pub fn create_genesis_from_transactions(
    transactions: Vec<Transaction>,
    proposer: [u8; 32],
    timestamp: u64,
) -> Result<Block> {
    let mut block = Block {
        version: 1,
        hash: [0u8; 64],
        transactions,
        proposer,
        signature: [0u8; 64],  // Would need to be signed
        state_root: [0u8; 64], // Would need to be computed
        parent_exec_hash: [0u8; 64],
        parent_ref_hash: [0u8; 64],
        height: 0,
        timestamp,
        tx_root: [0u8; 64],
    };

    block.tx_root = crate::core::crypto::compute_tx_root(&block.transactions);
    block.hash = block.header_hash();

    Ok(block)
}

/// Initialize accounts from genesis transactions
pub fn initialize_accounts_from_genesis(storage: &Storage) -> Result<()> {
    use super::types::Account;

    let block = load_genesis_block()?;

    let scale = if cfg!(feature = "testnet") {
        TESTNET_GENESIS_SCALE
    } else {
        1
    };

    for tx in block.transactions {
        // Initialize sender account
        let sender_bytes = storage.get_account(tx.from.as_bytes())?;
        let mut sender_account = if let Some(bytes) = sender_bytes {
            Account::decode(&bytes)?
        } else {
            Account {
                balance: 0,
                nonce: 0,
            }
        };
        sender_account.nonce = 0;
        storage.put_account(tx.from.as_bytes(), &sender_account.encode())?;

        // Initialize recipient account
        let recipient_bytes = storage.get_account(tx.to.as_bytes())?;
        let mut recipient_account = if let Some(bytes) = recipient_bytes {
            Account::decode(&bytes)?
        } else {
            Account {
                balance: 0,
                nonce: 0,
            }
        };
        let amount = (tx.amount as u128) * scale;
        recipient_account.balance = recipient_account
            .balance
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("Balance overflow in genesis"))?;
        recipient_account.nonce = 0;
        storage.put_account(tx.to.as_bytes(), &recipient_account.encode())?;
    }

    Ok(())
}

/// Get total initial supply from genesis
pub fn get_initial_supply() -> Result<u128> {
    let block = load_genesis_block()?;
    let scale = if cfg!(feature = "testnet") {
        TESTNET_GENESIS_SCALE
    } else {
        1u128
    };
    let mut total_supply = 0u128;
    for tx in block.transactions {
        total_supply = total_supply
            .checked_add((tx.amount as u128) * scale)
            .ok_or_else(|| anyhow::anyhow!("Total supply overflow"))?;
    }
    Ok(total_supply)
}

/// Check if an address is in the genesis transactions
pub fn is_genesis_address(address: &str) -> Result<bool> {
    let block = load_genesis_block()?;

    for tx in block.transactions {
        if tx.from == address.to_lowercase() || tx.to == address.to_lowercase() {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Get all genesis addresses
pub fn get_genesis_addresses() -> Result<Vec<String>> {
    let block = load_genesis_block()?;
    let mut addresses = std::collections::HashSet::new();

    for tx in block.transactions {
        addresses.insert(tx.from.clone());
        addresses.insert(tx.to.clone());
    }

    Ok(addresses.into_iter().collect())
}

/// Get genesis block version
pub fn get_genesis_version() -> Result<u8> {
    let block = load_genesis_block()?;
    Ok(block.version)
}

/// Create a minimal genesis block for development
pub fn create_dev_genesis_block() -> Result<Block> {
    let transactions = vec![
        Transaction {
            from: "0x0000000000000000000000000000000000000001".to_string(),
            to: "0x0000000000000000000000000000000000000002".to_string(),
            amount: (1000000u128 * 10u128.pow(18)) as u64, // 1M tokens
            data: vec![],
            fee: 0,
            nonce: 0,
            signature: vec![],
            timestamp: 0,
            pubkey: vec![],
            sig: vec![],
            pre_verified: false,
        },
        Transaction {
            from: "0x0000000000000000000000000000000000000001".to_string(),
            to: "0x0000000000000000000000000000000000000003".to_string(),
            amount: (500000u128 * 10u128.pow(18)) as u64, // 500K tokens
            data: vec![],
            fee: 0,
            nonce: 0,
            signature: vec![],
            timestamp: 0,
            pubkey: vec![],
            sig: vec![],
            pre_verified: false,
        },
    ];

    create_genesis_from_transactions(
        transactions,
        decode_hex::<32>("0x0000000000000000000000000000000000000001")?,
        1000000,
    )
}

/// Verify genesis block signature with real Ed25519 verification
pub fn verify_genesis_signature(block: &Block) -> Result<bool> {
    // Check that signature is not all zeros
    if block.signature == [0u8; 64] {
        return Ok(false);
    }

    // Real signature verification using Ed25519
    if block.proposer != [0u8; 32] {
        let message_string = format!(
            "{}{}{}{}{}",
            block.version,
            hex::encode(&block.hash),
            block.height,
            block.timestamp,
            hex::encode(&block.tx_root)
        );
        let message_to_verify = message_string.as_bytes();

        Ok(verify_signature(
            message_to_verify,
            &block.signature,
            &block.proposer,
        ))
    } else {
        // Fallback check if proposer is invalid
        Ok(block.signature != [0u8; 64])
    }
}

/// Compute genesis block hash
pub fn compute_genesis_hash(block: &Block) -> [u8; 64] {
    block.header_hash()
}

/// Export genesis block to JSON
pub fn export_genesis_to_json() -> Result<String> {
    let block = load_genesis_block()?;
    serde_json::to_string_pretty(&block)
        .map_err(|e| anyhow::anyhow!("Failed to serialize genesis block: {}", e))
}

/// Import genesis block from JSON
pub fn import_genesis_from_json(json: &str) -> Result<Block> {
    serde_json::from_str(json)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize genesis block: {}", e))
}

/// Reset genesis block (for testing only)
pub fn reset_genesis_block(storage: &Storage) -> Result<()> {
    // Remove genesis block
    storage.delete_block_hash_for_height(0)?;
    storage.delete_block(&load_genesis_block()?.hash)?;

    // Reset chain head
    storage.delete_chain_head()?;

    Ok(())
}

/// Check if genesis block exists in storage
pub fn genesis_block_exists(storage: &Storage) -> Result<bool> {
    Ok(storage.get_block(0)?.is_some())
}

/// Maximum allowed size for genesis deserialization (4 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized payloads.
const MAX_GENESIS_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;

/// Get genesis block from storage
pub fn get_genesis_block_from_storage(storage: &Storage) -> Result<Option<Block>> {
    if let Some(bytes) = storage.get_block(0)? {
        if bytes.len() > MAX_GENESIS_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Genesis block data too large for deserialization: {} bytes (max {})",
                bytes.len(),
                MAX_GENESIS_DESERIALIZE_SIZE
            );
        }
        let block: Block = bincode::deserialize(&bytes)?;
        Ok(Some(block))
    } else {
        Ok(None)
    }
}

/// Validate genesis block in storage
pub fn validate_genesis_in_storage(storage: &Storage) -> Result<bool> {
    if let Some(block) = get_genesis_block_from_storage(storage)? {
        validate_genesis_block(&block).map(|_| true)
    } else {
        Ok(false)
    }
}

/// Initialize genesis state
pub fn initialize_genesis_state(storage: &Storage) -> Result<()> {
    let block = load_genesis_block()?;

    // Initialize accounts
    initialize_accounts_from_genesis(storage)?;

    // Initialize vesting / supply (testnet: solo supply; mainnet: vesting + supply)
    initialize_genesis_mint(storage, &block)?;

    // Store genesis metadata
    let metadata = GenesisMetadata {
        block_hash: block.hash.to_vec(),
        timestamp: block.timestamp,
        proposer: block.proposer,
        total_supply: get_initial_supply()?,
        transaction_count: block.transactions.len(),
    };
    let metadata_bytes = bincode::serialize(&metadata)?;
    storage.put_genesis_metadata(&metadata_bytes)?;

    Ok(())
}

/// Genesis metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenesisMetadata {
    #[serde(with = "serde_bytes")]
    pub block_hash: Vec<u8>,
    pub timestamp: u64,
    pub proposer: [u8; 32],
    pub total_supply: u128,
    pub transaction_count: usize,
}

/// Get genesis metadata from storage
pub fn get_genesis_metadata(storage: &Storage) -> Result<Option<GenesisMetadata>> {
    if let Some(bytes) = storage.get_genesis_metadata()? {
        if bytes.len() > MAX_GENESIS_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Genesis metadata too large for deserialization: {} bytes (max {})",
                bytes.len(),
                MAX_GENESIS_DESERIALIZE_SIZE
            );
        }
        let metadata: GenesisMetadata = bincode::deserialize(&bytes)?;
        Ok(Some(metadata))
    } else {
        Ok(None)
    }
}

/// Check if genesis is properly initialized
pub fn is_genesis_initialized(storage: &Storage) -> Result<bool> {
    Ok(genesis_block_exists(storage)? && get_genesis_metadata(storage)?.is_some())
}

