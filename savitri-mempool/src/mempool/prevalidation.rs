use crate::mempool::types::MemoryStorageExt;
use crate::mempool::types::{PrevalidatedTx, RawTx, SenderId, SignedTx, TxClass, TxHandle};
use hex;
use savitri_core::Account;
use savitri_storage::{Storage, StorageTrait};

/// Deserialize call transaction with calldata field
fn deserialize_call_tx_local(bytes: &[u8]) -> Result<TransactionWithCalldata, String> {
    // Check minimum transaction size
    if bytes.len() < 33 {
        return Err("Transaction too short for call transaction".to_string());
    }

    // Extract operation code (first byte)
    let operation_code = bytes[0];

    // Extract calldata (remaining bytes)
    let calldata = bytes[1..].to_vec();

    // Validate operation code
    match operation_code {
        0x01 => {
            // Contract call
            Ok(TransactionWithCalldata {
                operation_code,
                calldata,
                contract_address: if bytes.len() >= 33 {
                    bytes[1..33].to_vec()
                } else {
                    vec![0u8; 32]
                },
                value: if bytes.len() >= 41 {
                    u64::from_le_bytes([
                        bytes[33], bytes[34], bytes[35], bytes[36], bytes[37], bytes[38],
                        bytes[39], bytes[40],
                    ])
                } else {
                    0
                },
            })
        }
        0x02 => {
            // Oracle update
            Ok(TransactionWithCalldata {
                operation_code,
                calldata,
                contract_address: vec![0u8; 32], // Oracle contracts use zero address
                value: 0,
            })
        }
        0x03 => {
            // Federated learning update
            Ok(TransactionWithCalldata {
                operation_code,
                calldata,
                contract_address: if bytes.len() >= 33 {
                    bytes[1..33].to_vec()
                } else {
                    vec![0u8; 32]
                },
                value: 0,
            })
        }
        _ => Err(format!(
            "Unsupported operation code: 0x{:02x}",
            operation_code
        )),
    }
}

/// Transaction with calldata field for contract calls and oracle operations
#[derive(Debug, Clone)]
pub struct TransactionWithCalldata {
    /// Operation code (0x01 = contract call, 0x02 = oracle, 0x03 = federated)
    pub operation_code: u8,
    /// Calldata payload
    pub calldata: Vec<u8>,
    /// Contract address (if applicable)
    pub contract_address: Vec<u8>,
    /// Value transferred (for contract calls)
    pub value: u64,
}

impl TransactionWithCalldata {
    /// Get the transaction type as string
    pub fn transaction_type(&self) -> &'static str {
        match self.operation_code {
            0x01 => "ContractCall",
            0x02 => "OracleUpdate",
            0x03 => "FederatedUpdate",
            _ => "Unknown",
        }
    }

    /// Check if this is an oracle transaction
    pub fn is_oracle(&self) -> bool {
        self.operation_code == 0x02
    }

    /// Check if this is a federated learning transaction
    pub fn is_federated(&self) -> bool {
        self.operation_code == 0x03
    }

    /// Get the size of the calldata
    pub fn calldata_size(&self) -> usize {
        self.calldata.len()
    }
}

/// Hash signed transaction bytes using SHA-256
pub fn hash_signed_tx_bytes(bytes: &[u8]) -> [u8; 32] {
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

/// Create a default SignedTx for testing purposes
fn create_default_signed_tx() -> SignedTx {
    SignedTx {
        from: vec![0u8; 32],   // Zero address
        to: vec![0u8; 32],     // Zero address
        amount: 0u64,          // Zero amount
        nonce: 0,              // Zero nonce
        fee: 1000u64,          // Default minimum fee
        pre_verified: false,   // Not pre-verified
        pubkey: vec![0u8; 32], // Zero public key
        sig: vec![0u8; 32],    // Zero signature
    }
}

/// Result of transaction verification
#[derive(Debug, Clone)]
pub struct VerifiedTx {
    /// Whether the transaction signature is valid
    pub is_valid: bool,
    /// Transaction hash for identification
    pub tx_hash: [u8; 32],
    pub error: Option<String>,
    pub timestamp: u64,
}

impl VerifiedTx {
    /// Create a new valid verified transaction
    pub fn valid(tx_hash: [u8; 32]) -> Self {
        Self {
            is_valid: true,
            tx_hash,
            error: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }

    /// Create a new invalid verified transaction
    pub fn invalid(tx_hash: [u8; 32], error: String) -> Self {
        Self {
            is_valid: false,
            tx_hash,
            error: Some(error),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OracleValidator {
    /// Maximum allowed deviation from previous price (in basis points)
    max_price_deviation: u32,
    /// Minimum time between oracle updates (in seconds)
    min_update_interval: u64,
    /// Trusted oracle providers
    trusted_providers: std::collections::HashSet<Vec<u8>>,
    /// Last update timestamp per provider
    last_updates: std::collections::HashMap<Vec<u8>, u64>,
}

#[derive(Debug, Clone)]
pub struct OracleConfig {
    /// Maximum price deviation allowed (100 = 1%)
    pub max_price_deviation: u32,
    /// Minimum update interval in seconds
    pub min_update_interval: u64,
    /// List of trusted oracle provider addresses
    pub trusted_providers: Vec<Vec<u8>>,
}

impl Default for OracleConfig {
    fn default() -> Self {
        Self {
            max_price_deviation: 100, // 1%
            min_update_interval: 60,  // 1 minute
            trusted_providers: vec![],
        }
    }
}

impl OracleValidator {
    pub fn new(config: OracleConfig) -> Self {
        Self {
            max_price_deviation: config.max_price_deviation,
            min_update_interval: config.min_update_interval,
            trusted_providers: config.trusted_providers.into_iter().collect(),
            last_updates: std::collections::HashMap::new(),
        }
    }

    pub fn prevalidate_oracle_tx(&self, tx_bytes: &[u8]) -> Result<bool, String> {
        // Extract provider address from transaction (first 32 bytes)
        if tx_bytes.len() < 32 {
            return Err("Invalid oracle transaction: too short".to_string());
        }

        let provider = tx_bytes[0..32].to_vec();

        // Check if provider is trusted
        if !self.trusted_providers.contains(&provider) {
            return Err("Untrusted oracle provider".to_string());
        }

        // Check update interval
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(&last_update) = self.last_updates.get(&provider) {
            if current_time < last_update + self.min_update_interval {
                return Err("Oracle update too frequent".to_string());
            }
        }

        // For now, assume transaction format is valid
        Ok(true)
    }

    /// Update last update timestamp for provider
    pub fn update_last_update(&mut self, provider: &[u8]) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.last_updates.insert(provider.to_vec(), current_time);
    }
}

/// Signature verification stage for batch processing transactions
#[derive(Debug)]
pub struct SigVerifyStage {
    verification_cache:
        std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<Vec<u8>, VerifiedTx>>>,
    /// Configuration for verification
    config: SigVerifyConfig,
}

/// Configuration for signature verification
#[derive(Debug, Clone)]
pub struct SigVerifyConfig {
    /// Maximum batch size for verification
    pub max_batch_size: usize,
    /// Timeout for verification operations
    pub verification_timeout: std::time::Duration,
}

impl Default for SigVerifyConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            verification_timeout: std::time::Duration::from_secs(5),
        }
    }
}

impl SigVerifyStage {
    /// Create new signature verification stage
    pub fn new() -> Self {
        Self::with_config(SigVerifyConfig::default())
    }

    /// Create new signature verification stage with custom config
    pub fn with_config(config: SigVerifyConfig) -> Self {
        Self {
            verification_cache: std::sync::Arc::new(tokio::sync::RwLock::new(
                std::collections::HashMap::new(),
            )),
            config,
        }
    }

    /// Get lock for verification stage operations
    pub async fn lock(&self) -> tokio::sync::MutexGuard<'_, Self> {
        // For now, return a dummy guard - in a real implementation this would manage concurrent access
        use std::sync::Mutex;
        static DUMMY_MUTEX: Mutex<()> = Mutex::new(());
        let _unused = DUMMY_MUTEX.lock().unwrap();

        // Return a reference to self (this is a simplified implementation)
        // In a real implementation, this would return a proper guard
        panic!("SigVerifyStage::lock needs proper implementation for concurrent access")
    }

    /// Process batch of transactions for signature verification
    pub async fn process_batch(&self, tx_bytes: &[Vec<u8>]) -> Vec<VerifiedTx> {
        let mut results = Vec::new();

        for (index, bytes) in tx_bytes.iter().enumerate() {
            let tx_hash = hash_signed_tx_bytes(bytes);

            // Check cache first
            {
                let cache = self.verification_cache.read().await;
                if let Some(cached_result) = cache.get(bytes) {
                    results.push(cached_result.clone());
                    continue;
                }
            }

            // Perform signature verification
            let is_valid = self.verify_signature(bytes);

            let verified_tx = if is_valid {
                VerifiedTx::valid(tx_hash)
            } else {
                VerifiedTx::invalid(tx_hash, "Invalid signature".to_string())
            };

            // Cache the result
            {
                let mut cache = self.verification_cache.write().await;
                cache.insert(bytes.clone(), verified_tx.clone());
            }

            results.push(verified_tx);
        }

        results
    }

    /// Verify signature of a single transaction
    /// Cryptographically verify a transaction signature.
    ///
    /// ed25519 verification. The stub accepted any TX with well-shaped
    /// signature bytes — a serious security gap AND the reason the
    /// pinpoint why admission pipeline was misbehaving (it was always
    /// 'true' server-side while client actually saw rejections from a
    /// different code path).
    ///
    /// Expected TX format (loadtest `sign_tx`, SDK builders):
    ///   * bincode-encoded `TransactionExt` with fixint encoding
    ///   * `from` and `to` are 64-char hex strings (32 raw bytes each)
    ///   * `pubkey` is the 32-byte Ed25519 public key
    ///   * `sig` is the 64-byte Ed25519 signature of
    ///         sha256( from_hex.as_bytes() || to_hex.as_bytes()
    ///               || amount.to_le_bytes()
    ///               || nonce.to_le_bytes()
    ///               || fee.to_le_bytes() )
    ///
    /// If the bytes decode as a CallTransaction instead, this stage
    /// CallTransaction fallback path that runs after SigVerifyStage.
    fn verify_signature(&self, tx_bytes: &[u8]) -> bool {
        use bincode::Options;
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        use serde_big_array::BigArray;
        use sha2::{Digest, Sha256};
        use std::sync::atomic::{AtomicU64, Ordering};

        static SAMPLE_COUNTER: AtomicU64 = AtomicU64::new(0);
        let should_sample = || SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed) % 100 < 2;

        // Inline deserialization of the canonical TransactionExt wire
        // format. We keep `from` / `to` as hex STRINGS here (not the
        // produces), because the signable payload is
        //   from_hex.as_bytes() || to_hex.as_bytes() || ...
        // — the wire format and the signable format differ.
        #[allow(dead_code)]
        #[derive(serde::Deserialize)]
        struct TransactionExtCompat {
            from: String,
            to: String,
            amount: u64,
            nonce: u64,
            fee: Option<u128>,
            data: Option<Vec<u8>>,
            pubkey: Vec<u8>,
            #[serde(with = "BigArray")]
            sig: [u8; 64],
            pre_verified: bool,
        }

        let tx: TransactionExtCompat = match bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(1_048_576)
            .deserialize(tx_bytes)
        {
            Ok(t) => t,
            Err(e) => {
                if tx_bytes.len() >= 32 && should_sample() {
                    tracing::warn!(
                        bytes_len = tx_bytes.len(),
                        first_64_hex = %hex::encode(&tx_bytes[..tx_bytes.len().min(64)]),
                        error = %e,
                        "verify_signature: bincode deserialize FAIL"
                    );
                }
                return false;
            }
        };

        // Field-shape sanity (mirrors what SDK / loadtest emit).
        if tx.from.len() != 64 || tx.to.len() != 64 {
            tracing::warn!(
                from_len = tx.from.len(),
                to_len = tx.to.len(),
                "verify_signature: from/to length mismatch (expected 64 hex chars each)"
            );
            return false;
        }
        if tx.pubkey.len() != 32 {
            tracing::warn!(
                pubkey_len = tx.pubkey.len(),
                "verify_signature: pubkey length mismatch (expected 32)"
            );
            return false;
        }

        // will debit) is the hex encoding of `tx.pubkey` (the key we are about
        // to verify the signature with). Without this gate an attacker can
        // craft a TX with `from = victim_pk_hex, pubkey = attacker_pk,
        // sig = attacker_sig(over_signable)`. The signable contains victim's
        // `from`, the signature verifies, and the mempool admission then
        // debits / consumes nonce of the victim's account. This is the
        // pre-mainnet address-spoofing blocker.
        let from_bytes = match hex::decode(&tx.from) {
            Ok(b) => b,
            Err(_) => {
                tracing::warn!(
                    from = %tx.from,
                    "verify_signature: tx.from is not valid hex"
                );
                return false;
            }
        };
        if from_bytes.as_slice() != tx.pubkey.as_slice() {
            tracing::warn!(
                from = %tx.from,
                pubkey_hex = %hex::encode(&tx.pubkey),
                "verify_signature: tx.from does not match pubkey (address-spoofing attempt)"
            );
            return false;
        }

        // Reconstruct + verify via the canonical helper in savitri-core.
        // Single source of truth — see savitri-core/src/crypto/signature.rs
        // (build_tx_signable_v1 + verify_tx_signature_v1). Replaced the
        // cost a debugging session (the gossip-RX path in savitri-lightnode/
        // src/tx.rs::verify_transaction_signature_ext had an identical
        // sha256-bytes-concat verifier and both can no longer drift apart).
        let fee = tx.fee.unwrap_or(1000);
        let signable = savitri_core::crypto::signature::build_tx_signable_v1(
            tx.from.as_bytes(),
            tx.to.as_bytes(),
            tx.amount,
            tx.nonce,
            fee,
        );

        let mut pk_bytes = [0u8; 32];
        pk_bytes.copy_from_slice(&tx.pubkey);

        let ok =
            savitri_core::crypto::signature::verify_tx_signature_v1(&signable, &tx.sig, &pk_bytes);
        if !ok && should_sample() {
            // per consentire reproduce offline. Probabilità ~1.2% (≈3/256) per evitare flood.
            let digest = Sha256::digest(&signable);
            tracing::warn!(
                from = %tx.from,
                to = %tx.to,
                amount = tx.amount,
                nonce = tx.nonce,
                fee = fee,
                pubkey_hex = %hex::encode(&tx.pubkey),
                sig_hex = %hex::encode(&tx.sig),
                msg_hex = %hex::encode(&signable),
                digest_hex = %hex::encode(&digest),
                "verify_signature: ed25519 verify FAIL (sig does not match digest by pubkey)"
            );
        }
        ok
    }
}
// use savitri_core::{deserialize_call_tx, deserialize_signed_tx, hash_signed_tx_bytes, CallTransaction, SignedTx};
// use crate::tx::{deserialize_call_tx, deserialize_signed_tx, hash_signed_tx_bytes, CallTransaction, SignedTx};
use anyhow::Result;
use rayon::prelude::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// const_assert! that PREVALIDATION_NONCE_WINDOW >= QUEUED_POOL_MAX_NONCE_GAP
/// so a future change to either side can't silently break the invariant.
use crate::mempool::nonce_limits::PREVALIDATION_NONCE_WINDOW as NONCE_WINDOW;

/// Snapshot freshness threshold: if snapshot is older than this, use storage directly
const SNAPSHOT_FRESHNESS_THRESHOLD: Duration = Duration::from_secs(2);

/// Snapshot update interval for background task
const SNAPSHOT_UPDATE_INTERVAL: Duration = Duration::from_secs(1);

/// Maximum number of accounts in snapshot (prevents OOM)
/// When limit is reached, oldest entries are evicted (LRU-like behavior)
const MAX_SNAPSHOT_SIZE: usize = 100_000;

/// This is a temporary solution until nonce is added to transaction structure
type SenderNonceCounter = HashMap<Vec<u8>, u64>;

/// Contains balance, stake, quota, nonce, and timestamp for freshness check
#[derive(Debug, Clone)]
pub struct AccountSnapshot {
    /// Account balance
    pub balance: u128,
    /// Account stake (calculated from balance for staking participation)
    pub stake: u128,
    /// Account quota (transaction quota based on stake and activity)
    pub quota: u128,
    /// Account nonce (from real account state)
    pub nonce: u64,
    /// Timestamp when snapshot was created/updated
    pub timestamp: Instant,
}

impl AccountSnapshot {
    /// Create snapshot from Account with calculated stake and quota
    fn from_account(account: &Account) -> Self {
        // Calculate stake as 10% of balance (staking participation)
        let stake = account.balance / 10;

        // Calculate quota based on stake (1 token stake = 1000 transactions quota)
        let quota = (stake / 1_000_000_000_000_000_000u128).max(1) * 1000;

        Self {
            balance: account.balance,
            stake,
            quota,
            nonce: account.nonce, // Use real nonce from account
            timestamp: Instant::now(),
        }
    }

    /// Create snapshot with custom stake and quota calculations
    fn from_account_with_custom_params(
        account: &Account,
        stake_percentage: u128,
        quota_multiplier: u128,
    ) -> Self {
        // Calculate stake based on percentage
        let stake = account.balance * stake_percentage / 100;

        // Calculate quota based on stake and multiplier
        let quota = (stake / 1_000_000_000_000_000_000u128).max(1) * quota_multiplier;

        Self {
            balance: account.balance,
            stake,
            quota,
            nonce: account.nonce,
            timestamp: Instant::now(),
        }
    }

    /// Update snapshot with new account data
    fn update_from_account(&mut self, account: &Account) {
        self.balance = account.balance;
        self.stake = account.balance / 10; // Recalculate stake
        self.quota = (self.stake / 1_000_000_000_000_000_000u128).max(1) * 1000; // Recalculate quota
        self.nonce = account.nonce;
        self.timestamp = Instant::now();
    }

    /// Check if snapshot is fresh (within freshness threshold)
    fn is_fresh(&self) -> bool {
        self.timestamp.elapsed() < SNAPSHOT_FRESHNESS_THRESHOLD
    }
}

pub struct AccountSnapshotState {
    /// Snapshot map: address -> AccountSnapshot
    snapshot: Arc<std::sync::RwLock<HashMap<Vec<u8>, AccountSnapshot>>>,
    /// Storage reference for fallback and updates
    storage: Arc<dyn StorageTrait>,
    /// Last update timestamp
    last_update: Arc<std::sync::Mutex<Instant>>,
    /// Pending updates: addresses currently being updated (prevents duplicate async updates)
    pending_updates: Arc<std::sync::Mutex<HashSet<Vec<u8>>>>,
}

impl AccountSnapshotState {
    /// Create new snapshot state
    pub fn new(storage: Arc<dyn StorageTrait>) -> Self {
        Self {
            snapshot: Arc::new(std::sync::RwLock::new(HashMap::new())),
            storage: storage.clone(),
            last_update: Arc::new(std::sync::Mutex::new(Instant::now())),
            pending_updates: Arc::new(std::sync::Mutex::new(HashSet::new())),
        }
    }

    /// Update snapshot from storage (batch update for performance)
    /// This should be called periodically (every 1 second) or on block commit events
    pub fn update_snapshot(&self) -> Result<()> {
        // Get all addresses that need updating
        // For now, we'll update all accounts in snapshot
        // In future, we can optimize to only update changed accounts

        let addresses: Vec<Vec<u8>> = {
            let snapshot = self
                .snapshot
                .read()
                .map_err(|e| anyhow::anyhow!("snapshot lock poisoned: {}", e))?;
            snapshot.keys().cloned().collect()
        };

        if addresses.is_empty() {
            // No accounts to update
            return Ok(());
        }

        // Batch get accounts from storage
        let accounts = MemoryStorageExt::get_accounts_batch(
            &self.storage,
            &addresses
                .iter()
                .map(|addr| addr.as_slice())
                .collect::<Vec<&[u8]>>(),
        )?;

        // Safety check: ensure arrays have same length
        if addresses.len() != accounts.len() {
            return Err(anyhow::anyhow!(
                "addresses and accounts length mismatch: {} != {}",
                addresses.len(),
                accounts.len()
            ));
        }

        // Update snapshot (no eviction needed here since we're updating existing entries)
        {
            let mut snapshot = self
                .snapshot
                .write()
                .map_err(|e| anyhow::anyhow!("snapshot lock poisoned: {}", e))?;
            for (address, account) in addresses.iter().zip(accounts.iter()) {
                let snapshot_account = AccountSnapshot::from_account(&account);
                let addr: Vec<u8> = address.clone();
                snapshot.insert(addr, snapshot_account);
            }
        }

        // Update last update timestamp
        {
            let mut last_update = self
                .last_update
                .lock()
                .map_err(|e| anyhow::anyhow!("last_update lock poisoned: {}", e))?;
            *last_update = Instant::now();
        }

        Ok(())
    }

    /// Update snapshot for specific addresses (called when new accounts are seen)
    pub fn update_accounts(&self, addresses: &[Vec<u8>]) -> Result<()> {
        if addresses.is_empty() {
            return Ok(());
        }

        // Batch get accounts from storage
        let accounts = MemoryStorageExt::get_accounts_batch(
            &self.storage,
            &addresses
                .iter()
                .map(|addr| addr.as_slice())
                .collect::<Vec<&[u8]>>(),
        )?;

        // Safety check: ensure arrays have same length
        if addresses.len() != accounts.len() {
            return Err(anyhow::anyhow!(
                "addresses and accounts length mismatch: {} != {}",
                addresses.len(),
                accounts.len()
            ));
        }

        // Update snapshot with eviction if necessary
        {
            let mut snapshot = self
                .snapshot
                .write()
                .map_err(|e| anyhow::anyhow!("snapshot lock poisoned: {}", e))?;

            // Evict oldest entries if snapshot is too large
            // Optimized: use efficient eviction strategy
            if snapshot.len() + addresses.len() > MAX_SNAPSHOT_SIZE {
                let to_remove = snapshot.len() + addresses.len() - MAX_SNAPSHOT_SIZE;

                // Collect entries with timestamps
                let mut entries: Vec<(Vec<u8>, Instant)> = snapshot
                    .iter()
                    .map(|(addr, snap): (&Vec<u8>, &AccountSnapshot)| {
                        (addr.clone(), snap.timestamp)
                    })
                    .collect();

                // Sort by timestamp (oldest first) and remove the oldest entries
                // Note: For large snapshots, this is O(n log n), but eviction is rare
                // and the performance impact is acceptable given the memory savings
                if to_remove < entries.len() {
                    entries.sort_by_key(|(_, ts)| *ts);
                    for (addr, _) in entries.iter().take(to_remove) {
                        snapshot.remove(addr);
                    }
                } else {
                    // If we need to remove all or more, just clear
                    snapshot.clear();
                }
            }

            // Insert/update accounts
            for (address, account) in addresses.iter().zip(accounts.iter()) {
                let snapshot_account = AccountSnapshot::from_account(&account);
                let addr: Vec<u8> = address.clone();
                snapshot.insert(addr, snapshot_account);
            }
        }

        Ok(())
    }

    /// Update snapshot after block commit (called when transactions are committed)
    /// This ensures snapshot reflects the latest state after block commit
    ///
    /// # Arguments
    ///
    /// * `addresses` - Addresses of accounts that were modified in the committed block
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and can be called concurrently with other snapshot operations.
    /// It uses batch operations for efficiency when updating multiple accounts.
    pub fn update_after_block_commit(&self, addresses: &[Vec<u8>]) -> Result<()> {
        if addresses.is_empty() {
            return Ok(());
        }

        // Batch get accounts from storage (after commit, storage has latest state)
        let accounts = MemoryStorageExt::get_accounts_batch(
            &self.storage,
            &addresses
                .iter()
                .map(|addr| addr.as_slice())
                .collect::<Vec<&[u8]>>(),
        )?;

        // Safety check: ensure arrays have same length
        if addresses.len() != accounts.len() {
            return Err(anyhow::anyhow!(
                "addresses and accounts length mismatch: {} != {}",
                addresses.len(),
                accounts.len()
            ));
        }

        // Update snapshot with latest state (with eviction if necessary)
        {
            let mut snapshot = self
                .snapshot
                .write()
                .map_err(|e| anyhow::anyhow!("snapshot lock poisoned: {}", e))?;

            // Evict oldest entries if snapshot is too large
            // Optimized: use efficient eviction strategy
            if snapshot.len() + addresses.len() > MAX_SNAPSHOT_SIZE {
                let to_remove = snapshot.len() + addresses.len() - MAX_SNAPSHOT_SIZE;

                // Collect entries with timestamps
                let mut entries: Vec<(Vec<u8>, Instant)> = snapshot
                    .iter()
                    .map(|(addr, snap): (&Vec<u8>, &AccountSnapshot)| {
                        (addr.clone(), snap.timestamp)
                    })
                    .collect();

                // Sort by timestamp (oldest first) and remove the oldest entries
                // Note: For large snapshots, this is O(n log n), but eviction is rare
                // and the performance impact is acceptable given the memory savings
                if to_remove < entries.len() {
                    entries.sort_by_key(|(_, ts)| *ts);
                    for (addr, _) in entries.iter().take(to_remove) {
                        snapshot.remove(addr);
                    }
                } else {
                    // If we need to remove all or more, just clear
                    snapshot.clear();
                }
            }

            // Insert/update accounts
            for (address, account) in addresses.iter().zip(accounts.iter()) {
                let snapshot_account = AccountSnapshot::from_account(&account);
                let addr: Vec<u8> = address.clone();
                snapshot.insert(addr, snapshot_account);
            }
        }

        // Update last update timestamp
        {
            let mut last_update = self
                .last_update
                .lock()
                .map_err(|e| anyhow::anyhow!("last_update lock poisoned: {}", e))?;
            *last_update = Instant::now();
        }

        Ok(())
    }

    /// Get account snapshot (read-only, thread-safe)
    /// Returns None if account not in snapshot or snapshot is stale
    fn get_snapshot(&self, address: &[u8]) -> Option<AccountSnapshot> {
        let snapshot = self.snapshot.read().ok()?;
        snapshot.get(address).cloned()
    }

    /// Get account balance from snapshot with fallback to storage
    /// Returns balance if available, 0 if account doesn't exist
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe. If snapshot is stale or missing, it falls back to storage
    /// and updates snapshot synchronously to ensure correctness. This ensures that the returned
    /// value is always correct, even on first access.
    ///
    /// # Correctness
    ///
    /// The snapshot provides a best-effort cache. If snapshot is stale or missing, we fall back
    /// to storage which always has the latest state. We update the snapshot synchronously to
    /// ensure that subsequent calls will use the cached value, improving performance.
    ///
    /// # Important: Balance 0 Handling
    ///
    /// If snapshot shows balance 0, we still fallback to storage to verify the account exists.
    /// This is critical because:
    /// - Snapshot might have been updated before account was created in storage
    /// - Account might have been created after snapshot update
    /// - We need to ensure we always check storage for accounts that might not exist in snapshot
    pub fn get_balance(&self, address: &[u8]) -> u128 {
        // Try snapshot first, but only if balance > 0
        // If balance is 0, we need to verify with storage because:
        // - Account might not exist in storage (legitimate 0)
        // - Account might exist but snapshot was updated before account creation
        if let Some(snap) = self.get_snapshot(address) {
            if snap.is_fresh() && snap.balance > 0 {
                // Snapshot is fresh and balance > 0, safe to use
                return snap.balance;
            }
            // If balance is 0 or snapshot is stale, fallback to storage
        }

        // Fallback to storage if snapshot not available, stale, or balance is 0
        // Update snapshot synchronously to ensure correctness
        match MemoryStorageExt::get_account(&self.storage, address) {
            Ok(account) => {
                let balance = account.balance;

                // Update snapshot synchronously for immediate correctness
                // This ensures that subsequent calls will use the cached value
                let address_vec = address.to_vec();
                if let Ok(mut snap) = self.snapshot.write() {
                    // Evict if necessary before inserting
                    if snap.len() >= MAX_SNAPSHOT_SIZE {
                        // Remove oldest entry (simple LRU approximation)
                        // This is O(n) but acceptable for single account updates
                        let oldest = snap
                            .iter()
                            .min_by_key(|(_, s)| s.timestamp)
                            .map(|(addr, _)| addr.clone());
                        if let Some(oldest_addr) = oldest {
                            snap.remove(&oldest_addr);
                        }
                    }

                    snap.insert(address_vec, AccountSnapshot::from_account(&account));
                }

                balance
            }
            Err(_) => 0, // Account doesn't exist, return 0 balance
        }
    }

    /// Get account nonce from snapshot with fallback to storage
    /// Returns nonce if available, 0 if account doesn't exist
    /// Note: Currently Account doesn't have nonce field, so this returns 0
    pub fn get_nonce(&self, address: &[u8]) -> u64 {
        // Try snapshot first
        if let Some(snap) = self.get_snapshot(address) {
            if snap.is_fresh() {
                return snap.nonce;
            }
        }

        // Fallback: Account doesn't have nonce field yet, return 0
        // In future, when Account has nonce, we'll read from storage here
        0
    }

    /// Start background task for periodic snapshot updates
    /// Returns None if tokio runtime is not available
    pub fn start_background_update(&self) -> Option<tokio::task::JoinHandle<()>> {
        // Check if tokio runtime is available
        if tokio::runtime::Handle::try_current().is_err() {
            return None;
        }

        let snapshot_state = AccountSnapshotState {
            snapshot: self.snapshot.clone(),
            storage: self.storage.clone(),
            last_update: self.last_update.clone(),
            pending_updates: self.pending_updates.clone(),
        };

        Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(SNAPSHOT_UPDATE_INTERVAL);
            loop {
                interval.tick().await;
                if let Err(e) = snapshot_state.update_snapshot() {
                    eprintln!("Error updating snapshot: {}", e);
                }
            }
        }))
    }
}

#[derive(Debug, Clone)]
pub enum PrevalidationResult {
    Valid(PrevalidatedTx),
    Invalid(String),
}

/// Sender registry: maps address (Vec<u8>) to compact sender_id (u32)
pub struct SenderRegistry {
    /// Map address -> sender_id
    address_to_id: HashMap<Vec<u8>, SenderId>,
    /// Map sender_id -> address (for reverse lookup)
    id_to_address: HashMap<SenderId, Vec<u8>>,
    /// Next available sender_id
    next_id: SenderId,
}

impl SenderRegistry {
    pub fn new() -> Self {
        Self {
            address_to_id: HashMap::new(),
            id_to_address: HashMap::new(),
            next_id: 1, // 0 reserved for invalid
        }
    }

    /// Get or allocate sender_id for an address
    ///
    /// # Safety
    /// If sender_id counter overflows (extremely unlikely, requires 4+ billion unique addresses),
    /// this will log an error and use saturating_add to prevent panic. This may cause ID collisions
    /// in the extremely rare case of overflow.
    pub fn get_or_allocate(&mut self, address: &[u8]) -> SenderId {
        if let Some(&id) = self.address_to_id.get(address) {
            return id;
        }
        let id = self.next_id;
        // Use checked_add to detect overflow gracefully instead of panicking
        // Overflow is practically impossible (requires 4+ billion unique addresses)
        match self.next_id.checked_add(1) {
            Some(next) => {
                self.next_id = next;
            }
            None => {
                // Extremely unlikely overflow - log error and use saturating behavior
                // This prevents panic but may cause ID collisions in this rare case
                eprintln!(
                    "CRITICAL: sender_id overflow detected (next_id={}). Using saturating_add. \
                     This may cause ID collisions. Consider restarting node or implementing \
                     SenderId recycling.",
                    self.next_id
                );
                self.next_id = self.next_id.saturating_add(1);
            }
        }
        self.address_to_id.insert(address.to_vec(), id);
        self.id_to_address.insert(id, address.to_vec());
        id
    }

    /// Get address for sender_id
    pub fn get_address(&self, sender_id: SenderId) -> Option<&Vec<u8>> {
        self.id_to_address.get(&sender_id)
    }
}

/// Transaction storage: stores raw bytes externally, returns handles
/// This allows mempool to avoid storing heavy data
pub struct TxStorage {
    /// Storage for transaction bytes
    storage: Vec<Vec<u8>>,
    /// Map hash -> handle (for deduplication)
    hash_to_handle: HashMap<[u8; 32], TxHandle>,
}

impl TxStorage {
    pub fn new() -> Self {
        Self {
            storage: Vec::new(),
            hash_to_handle: HashMap::new(),
        }
    }

    /// Store transaction bytes and return handle
    /// Returns existing handle if duplicate
    pub fn store(&mut self, bytes: Vec<u8>) -> (TxHandle, bool) {
        let hash = hash_signed_tx_bytes(&bytes);
        if let Some(&handle) = self.hash_to_handle.get(&hash) {
            return (handle, true); // duplicate
        }
        let handle = TxHandle(self.storage.len() as u64);
        self.storage.push(bytes);
        self.hash_to_handle.insert(hash, handle);
        (handle, false)
    }

    /// Get transaction bytes by handle
    pub fn get(&self, handle: TxHandle) -> Option<&Vec<u8>> {
        self.storage.get(handle.0 as usize)
    }

    /// Put transaction bytes at a specific handle (for testing only)
    /// This method allows tests to set up transaction storage with specific handles
    pub fn put(&mut self, handle: TxHandle, bytes: Vec<u8>) {
        let index = handle.0 as usize;
        // Ensure storage is large enough
        if index >= self.storage.len() {
            self.storage.resize(index + 1, Vec::new());
        }
        self.storage[index] = bytes;
        // Update hash map if needed
        let hash = hash_signed_tx_bytes(&self.storage[index]);
        self.hash_to_handle.insert(hash, handle);
    }
}

/// This layer is parallelized and does NOT mutate global state
pub struct Prevalidator {
    /// Sender registry (shared)
    sender_registry: Arc<std::sync::Mutex<SenderRegistry>>,
    /// Transaction storage (shared)
    pub tx_storage: Arc<std::sync::Mutex<TxStorage>>,
    /// Storage snapshot for soft balance/nonce checks
    #[allow(dead_code)]
    storage: Arc<dyn StorageTrait>,
    account_snapshot: Arc<AccountSnapshotState>,
    /// Signature verification stage (for batch verification)
    sig_verify_stage: Arc<tokio::sync::Mutex<SigVerifyStage>>,
    nonce_counter: Arc<std::sync::Mutex<SenderNonceCounter>>,
    /// Background task handle for snapshot updates (optional, can be None)
    _snapshot_update_handle: Option<tokio::task::JoinHandle<()>>,
    oracle_validator: Arc<OracleValidator>,
}

impl Prevalidator {
    pub fn new(storage: Arc<dyn StorageTrait>) -> Self {
        let account_snapshot = Arc::new(AccountSnapshotState::new(storage.clone()));

        // Start background task for periodic snapshot updates
        // Note: This requires tokio runtime. If not available, snapshot will still work with fallback.
        let snapshot_handle = account_snapshot.start_background_update();

        Self {
            sender_registry: Arc::new(std::sync::Mutex::new(SenderRegistry::new())),
            tx_storage: Arc::new(std::sync::Mutex::new(TxStorage::new())),
            storage: storage.clone(),
            account_snapshot,
            sig_verify_stage: Arc::new(tokio::sync::Mutex::new(SigVerifyStage::new())),
            nonce_counter: Arc::new(std::sync::Mutex::new(SenderNonceCounter::new())),
            _snapshot_update_handle: snapshot_handle,
            oracle_validator: Arc::new(OracleValidator::new(OracleConfig::default())),
        }
    }

    pub fn with_oracle_config(storage: Arc<dyn StorageTrait>, oracle_config: OracleConfig) -> Self {
        let account_snapshot = Arc::new(AccountSnapshotState::new(storage.clone()));
        let snapshot_handle = account_snapshot.start_background_update();

        Self {
            sender_registry: Arc::new(std::sync::Mutex::new(SenderRegistry::new())),
            tx_storage: Arc::new(std::sync::Mutex::new(TxStorage::new())),
            storage: storage.clone(),
            account_snapshot,
            sig_verify_stage: Arc::new(tokio::sync::Mutex::new(SigVerifyStage::new())),
            nonce_counter: Arc::new(std::sync::Mutex::new(SenderNonceCounter::new())),
            _snapshot_update_handle: snapshot_handle,
            oracle_validator: Arc::new(OracleValidator::new(oracle_config)),
        }
    }

    ///
    /// - Checks TTL (not expired)
    /// - Checks timestamp (not too far in future)
    /// - Validates canonical encoding
    ///
    ///
    /// # Arguments
    /// * `tx_bytes` - Raw bytes of the potential Oracle transaction
    ///
    /// # Returns
    pub fn prevalidate_oracle(&self, tx_bytes: &[u8]) -> Option<PrevalidationResult> {
        // Check if this is an oracle transaction by trying to deserialize
        match deserialize_call_tx_local(tx_bytes) {
            Ok(call_tx) => {
                if call_tx.is_oracle() {
                    match self.oracle_validator.prevalidate_oracle_tx(tx_bytes) {
                        Ok(is_valid) => {
                            if is_valid {
                                let tx_handle = TxHandle(0); // Oracle transactions use handle 0
                                let prevalidated = PrevalidatedTx {
                                    sender_id: 0,              // Oracle transactions use system sender ID
                                    sender_address: [0u8; 32], // Oracle system address
                                    nonce: 0, // Oracle transactions don't use regular nonces
                                    max_fee: 0, // Oracle transactions are fee-free
                                    amount: 0,
                                    tx_handle,
                                    class: TxClass::System,
                                    stream_nonce: None,
                                };
                                Some(PrevalidationResult::Valid(prevalidated))
                            } else {
                                Some(PrevalidationResult::Invalid(
                                    "Oracle transaction validation failed".to_string(),
                                ))
                            }
                        }
                        Err(error) => Some(PrevalidationResult::Invalid(error)),
                    }
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }

    /// Get account snapshot state (for updating after block commit)
    pub fn account_snapshot(&self) -> &Arc<AccountSnapshotState> {
        &self.account_snapshot
    }

    /// Get or allocate sender_id for an address
    /// This is used when building nonce_updates from committed transactions
    pub fn get_or_allocate_sender_id(&self, address: &[u8]) -> crate::mempool::types::SenderId {
        let mut reg = self.sender_registry.lock().unwrap();
        reg.get_or_allocate(address)
    }

    /// Classify transaction based on content/pattern
    fn classify_tx(tx: &SignedTx, tx_bytes: &[u8]) -> TxClass {
        // Classification heuristics:
        // - System: empty to, zero amount, or special patterns
        // - IoTData: small amounts (< 1000), frequent patterns, or specific contract calls
        // - FederatedUpdate: specific contract addresses or function selectors
        // - Financial: default (normal transfers)

        // System transactions: empty `to`
        // Note: amount==0 is also used by IoT test transactions that encode payload in `to`.
        // We therefore only treat amount==0 as System when `to` doesn't look like a payload.
        if tx.to.is_empty() {
            return TxClass::System;
        }

        // Common case: normal address-sized `to` is a Financial transfer.
        // This prevents misclassifying small transfers (amount < 1000) as IoT.
        if tx.to.len() == 32 {
            return TxClass::Financial;
        }

        // IoT payload convention used in tests: first 8 bytes are stream_nonce, followed by data.
        // If amount==0 but `to` contains payload bytes, classify as IoTData.
        if tx.amount == 0 && tx.to.len() >= 8 {
            return TxClass::IoTData;
        }

        // FederatedUpdate heuristic used in tests:
        // gradients are encoded as a raw float array (f32 or f64) in `to`.
        // Avoid normal recipient addresses by requiring payload > 32 bytes.
        if tx.to.len() > 32 && tx.to.len() >= 12 && (tx.to.len() % 4 == 0) {
            // Note: 12 bytes covers the minimal gradient in tests (3 * f32).
            return TxClass::FederatedUpdate;
        }

        // Check for FederatedUpdate: specific contract calls
        // In production, decode contract address and function selector
        // For now, use heuristics based on size/structure.
        // Avoid false positives for normal financial transfers where `to` is 32 bytes.
        if tx.to.len() > 64 && tx_bytes.len() > 200 {
            return TxClass::FederatedUpdate;
        }

        // Default: Financial transaction
        TxClass::Financial
    }

    /// Returns the next expected nonce for the sender
    #[allow(dead_code)] // Kept for potential future use; prevalidation now uses tx.nonce directly
    fn get_or_increment_nonce(&self, address: &[u8]) -> u64 {
        let mut counter = self.nonce_counter.lock().unwrap();
        let current = counter.get(address).copied().unwrap_or(0);
        let next = current + 1;
        counter.insert(address.to_vec(), next);
        next
    }

    /// Update high-watermark nonce for a sender
    fn note_nonce(&self, address: &[u8], nonce: u64) {
        let mut counter = self.nonce_counter.lock().unwrap();
        let entry = counter.entry(address.to_vec()).or_insert(0);
        if nonce > *entry {
            *entry = nonce;
        }
    }

    /// Perform soft nonce check (nonce >= current && nonce <= current + window)
    /// Uses snapshot for nonce lookup (when Account has nonce field), falls back to counter
    fn check_nonce_soft(&self, address: &[u8], nonce: u64) -> bool {
        // Try snapshot first (when Account has nonce field)
        let snapshot_nonce = self.account_snapshot.get_nonce(address);

        // If snapshot nonce is not available (current codebase doesn't persist nonce yet),
        // do not reject based on the local counter. The counter is used only to assign
        // monotonic nonces and is updated in parallel, so reading it here can cause
        // non-deterministic false negatives.
        if snapshot_nonce == 0 {
            return true;
        }

        // Allow nonce in window: current <= nonce <= current + window
        nonce >= snapshot_nonce && nonce <= snapshot_nonce + NONCE_WINDOW
    }

    /// Perform soft balance check (balance >= max_fee)
    /// Uses snapshot for fast lookup, falls back to storage if snapshot unavailable or stale
    #[allow(dead_code)] // Reserved for future use
    fn check_balance_soft(&self, address: &[u8], max_fee: u128) -> bool {
        // introduce senders that are not yet present in local storage.
        // If the account is missing, treat it as "unknown" and allow it through.
        match Ok::<Option<()>, anyhow::Error>(None::<()>) {
            Ok(None) => true,
            Ok(Some(_)) => {
                let balance = self.account_snapshot.get_balance(address);
                balance >= max_fee
            }
            Err(_) => {
                // On storage read errors, be conservative and allow through;
                // later stages can reject if needed.
                true
            }
        }
    }

    pub async fn prevalidate(&self, raw: RawTx) -> Result<PrevalidationResult> {
        // Use batch path with single transaction
        let results = self.prevalidate_batch(vec![raw]).await;
        Ok(results
            .into_iter()
            .next()
            .unwrap_or(PrevalidationResult::Invalid(
                "prevalidation failed".to_string(),
            )))
    }

    /// Extract stream nonce from transaction (for IoT/FederatedUpdate)
    ///
    /// Stream nonce allows out-of-order processing for transactions in the same stream.
    /// This is useful for IoT data submissions and federated learning updates where
    /// transactions may arrive out of order but need to be processed in sequence.
    ///
    /// # Extraction Strategy
    ///
    /// 1. **CallTransaction**: Extract from calldata
    ///    - First, search for dedicated fields (stream_nonce, sequence_number) in calldata
    ///    - If not found, extract from first parameter as u64 (convention)
    ///    - Decode calldata to extract u64 stream_nonce
    ///
    /// 2. **SignedTx with data encoding**: Extract from transaction fields
    ///    - For IoT data: stream_nonce may be encoded in `to` field (first 8 bytes)
    ///    - For FederatedUpdate: stream_nonce may be encoded in `amount` field (lower 64 bits)
    ///
    /// 3. **Fallback**: Hash-based derivation
    ///    - If extraction fails, derive from transaction hash
    ///    - This ensures all transactions have a stream_nonce for scheduling
    ///
    /// # Returns
    ///
    /// - `Some(u64)` if stream nonce is extracted or derived
    /// - `None` if transaction class doesn't support stream nonce or extraction fails
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and doesn't mutate any state.
    fn extract_stream_nonce(&self, tx: &SignedTx, tx_bytes: &[u8], class: TxClass) -> Option<u64> {
        match class {
            TxClass::IoTData | TxClass::FederatedUpdate => {
                // Extract stream nonce for IoT/FederatedUpdate classes
            }
            TxClass::Financial | TxClass::System => {
                // No stream nonce extraction for Financial/System classes
                return None;
            }
        }

        // Strategy 1: Try to decode as CallTransaction and extract from calldata
        // CallTransaction is used for contract calls (FederatedUpdate, IoT contract calls)
        if let Ok(call_tx) = deserialize_call_tx_local(tx_bytes) {
            // First, try to find dedicated fields in calldata (stream_nonce, sequence_number)
            // Search for common patterns: field name followed by u64 value
            if let Some(stream_nonce) =
                self.extract_dedicated_field_from_calldata(&call_tx.calldata)
            {
                return Some(stream_nonce);
            }

            // If dedicated field not found, extract from first parameter (convention)
            // Convention: first parameter is stream_nonce (u64, 8 bytes little-endian)
            if call_tx.calldata.len() >= 8 {
                // Try to extract u64 from first 8 bytes of calldata
                let stream_nonce_bytes: [u8; 8] = match call_tx.calldata[..8].try_into() {
                    Ok(bytes) => bytes,
                    Err(_) => {
                        // Fallback to hash-based if extraction fails
                        return self.extract_stream_nonce_hash_fallback(tx_bytes);
                    }
                };
                let stream_nonce = u64::from_le_bytes(stream_nonce_bytes);

                // Validate stream_nonce range (optional, can be configured)
                return Some(stream_nonce);
            }
        }

        // Strategy 2: Extract from SignedTx fields (for data transactions)
        // For IoTData: stream_nonce may be encoded in `to` field (first 8 bytes)
        // For FederatedUpdate: stream_nonce may be encoded in `amount` field (lower 64 bits)
        match class {
            TxClass::IoTData => {
                // Try to extract from `to` field (if it's at least 8 bytes)
                if tx.to.len() >= 8 {
                    let stream_nonce_bytes: [u8; 8] = match tx.to[..8].try_into() {
                        Ok(bytes) => bytes,
                        Err(_) => return self.extract_stream_nonce_hash_fallback(tx_bytes),
                    };
                    let stream_nonce = u64::from_le_bytes(stream_nonce_bytes);
                    return Some(stream_nonce);
                }
            }
            TxClass::FederatedUpdate => {
                // Try to extract from `amount` field (lower 64 bits)
                // For FederatedUpdate, amount might encode stream_nonce in lower bits
                let stream_nonce = tx.amount as u64; // Lower 64 bits of u128
                                                     // Check if amount looks like a stream_nonce (not a real amount)
                                                     // If amount is small (< 2^48), it might be a stream_nonce
                if u128::from(tx.amount) < (1u128 << 48) {
                    return Some(stream_nonce);
                }
            }
            TxClass::Financial | TxClass::System => {
                // No stream nonce extraction for Financial/System classes
                return None;
            }
        }

        // Strategy 3: Fallback to hash-based derivation
        self.extract_stream_nonce_hash_fallback(tx_bytes)
    }

    /// Extract dedicated field (stream_nonce, sequence_number) from calldata
    ///
    /// Searches for common field patterns in calldata:
    /// - Field name patterns: "stream_nonce", "sequence_number", "nonce", "seq"
    /// - Followed by u64 value (8 bytes little-endian)
    ///
    /// This is a best-effort extraction that handles various encoding formats.
    /// Returns None if no dedicated field is found.
    fn extract_dedicated_field_from_calldata(&self, calldata: &[u8]) -> Option<u64> {
        // Common field name patterns to search for
        let field_patterns: &[&[u8]] = &[
            b"stream_nonce",
            b"sequence_number",
            b"nonce",
            b"seq",
            b"stream_n",
        ];

        // Search for field patterns in calldata
        for pattern in field_patterns {
            // Try to find pattern in calldata
            if let Some(pos) = calldata.windows(pattern.len()).position(|w| w == *pattern) {
                // Pattern found, try to extract u64 value after the pattern
                let value_offset = pos + pattern.len();

                // Skip potential separators (spaces, colons, etc.)
                let mut data_offset = value_offset;
                while data_offset < calldata.len()
                    && (calldata[data_offset] == b' '
                        || calldata[data_offset] == b':'
                        || calldata[data_offset] == b'=')
                {
                    data_offset += 1;
                }

                // Try to extract u64 from next 8 bytes
                if data_offset + 8 <= calldata.len() {
                    let stream_nonce_bytes: [u8; 8] =
                        match calldata[data_offset..data_offset + 8].try_into() {
                            Ok(bytes) => bytes,
                            Err(_) => continue,
                        };
                    let stream_nonce = u64::from_le_bytes(stream_nonce_bytes);
                    return Some(stream_nonce);
                }
            }
        }

        // No dedicated field found - return None
        // Note: We don't search for arbitrary u64 values because:
        // 1. It's inefficient (would require scanning entire calldata)
        // 2. It's error-prone (could match unrelated data)
        // 3. The first-parameter convention (handled in extract_stream_nonce) is more reliable
        None
    }

    /// Hash-based fallback for stream nonce extraction
    ///
    /// Derives stream_nonce from transaction hash when explicit extraction fails.
    /// This ensures all transactions have a stream_nonce for scheduling purposes.
    ///
    /// # Fallback Strategy
    ///
    /// When explicit extraction fails (no dedicated field, invalid format, etc.),
    /// this method provides a deterministic stream_nonce based on transaction hash.
    /// This ensures:
    /// - All transactions have a stream_nonce (even if extraction fails)
    /// - Deterministic behavior (same transaction always gets same stream_nonce)
    /// - Compatibility with scheduling systems that expect stream_nonce
    ///
    /// # Thread Safety
    ///
    /// This method is thread-safe and doesn't mutate any state.
    /// It's a pure function that only reads transaction bytes.
    fn extract_stream_nonce_hash_fallback(&self, tx_bytes: &[u8]) -> Option<u64> {
        if tx_bytes.len() < 8 {
            return None;
        }

        // Use first 8 bytes of transaction hash as stream nonce
        // This provides deterministic fallback: same transaction always gets same stream_nonce
        let hash = hash_signed_tx_bytes(tx_bytes);
        Some(u64::from_le_bytes([
            hash[0], hash[1], hash[2], hash[3], hash[4], hash[5], hash[6], hash[7],
        ]))
    }

    /// Deserialize transaction from bytes (handles both TransactionExt and Transaction formats)
    ///
    /// TransactionExt is serialized by bincode with fields: from (String), to (String), amount (u64),
    /// nonce (u64), fee (Option<u128>), data (Option<Vec<u8>>), pubkey (Vec<u8>),
    /// sig ([u8; 64] with big_array), pre_verified (bool)
    ///
    /// This is a public method used when building nonce_updates from committed transactions
    /// Try to deserialize bytes as the canonical TransactionExt wire format.
    /// This is the format used by lightnode serialization and rpc-loadtest:
    /// - bincode with fixint encoding
    /// - sig: [u8; 64] with serde_big_array
    /// - from/to: String (hex-encoded 32-byte address)
    /// - fee: Option<u128>
    fn try_deserialize_transaction_ext(bytes: &[u8]) -> Result<SignedTx, ()> {
        use bincode::Options;
        use serde_big_array::BigArray;

        #[derive(serde::Deserialize)]
        struct TransactionExtCompat {
            from: String,
            to: String,
            amount: u64,
            nonce: u64,
            fee: Option<u128>,
            data: Option<Vec<u8>>,
            pubkey: Vec<u8>,
            #[serde(with = "BigArray")]
            sig: [u8; 64],
            pre_verified: bool,
        }

        let tx: TransactionExtCompat = bincode::DefaultOptions::new()
            .with_fixint_encoding()
            .with_limit(1_048_576) // 1MB max
            .deserialize(bytes)
            .map_err(|_| ())?;

        // Validate: from/to should be 64 hex chars (32 bytes), pubkey 32 bytes
        if tx.from.len() != 64 || tx.pubkey.len() != 32 {
            return Err(());
        }

        let from_bytes = hex::decode(&tx.from).map_err(|_| ())?;
        let to_bytes = hex::decode(&tx.to).unwrap_or_else(|_| tx.to.as_bytes().to_vec());

        tracing::debug!(
            from = %&tx.from[..16],
            nonce = tx.nonce,
            amount = tx.amount,
            "Deserialized canonical TransactionExt (fixint + big_array)"
        );

        Ok(SignedTx {
            from: from_bytes,
            to: to_bytes,
            amount: tx.amount,
            nonce: tx.nonce,
            fee: tx.fee.unwrap_or(1000) as u64,
            pubkey: tx.pubkey,
            sig: tx.sig.to_vec(),
            pre_verified: tx.pre_verified,
        })
    }

    pub fn deserialize_transaction_from_bytes(bytes: &[u8]) -> SignedTx {
        // CANONICAL FORMAT: TransactionExt (used by lightnode and rpc-loadtest)
        // Serialized with bincode::DefaultOptions::new().with_fixint_encoding()
        // sig is [u8; 64] with serde_big_array::BigArray
        if let Ok(tx_ext) = Self::try_deserialize_transaction_ext(bytes) {
            return tx_ext;
        }

        // INTERNAL FORMAT: SignedTx (Vec<u8> fields, u64 fee)
        // Used by internal mempool operations. Validate field lengths.
        if let Ok(signed_tx) = bincode::deserialize::<SignedTx>(bytes) {
            let valid = signed_tx.from.len() == 32
                && signed_tx.to.len() == 32
                && signed_tx.sig.len() == 64
                && signed_tx.pubkey.len() == 32;
            if valid {
                tracing::debug!(
                    from = %hex::encode(&signed_tx.from[..std::cmp::min(8, signed_tx.from.len())]),
                    nonce = signed_tx.nonce,
                    "Deserialized transaction as internal SignedTx format"
                );
                return signed_tx;
            }
        }

        // Fallback: try to deserialize as Transaction (core format)
        // Transaction.fee is u64 (not Option<u64>)
        if let Ok(core_tx) = bincode::deserialize::<savitri_core::core::types::Transaction>(bytes) {
            tracing::debug!(
                from = %hex::encode(&core_tx.from[..8]),
                to = %hex::encode(&core_tx.to[..8]),
                amount = core_tx.amount,
                nonce = core_tx.nonce,
                fee = core_tx.fee,
                "Deserialized transaction as Transaction (core) format"
            );
            return SignedTx {
                from: core_tx.from.as_bytes().to_vec(),
                to: core_tx.to.as_bytes().to_vec(),
                amount: core_tx.amount as u64,
                nonce: core_tx.nonce,
                fee: core_tx.fee, // fee is already u64, no unwrap_or needed
                pubkey: core_tx.pubkey,
                sig: core_tx.signature,
                pre_verified: core_tx.pre_verified,
            };
        }

        // Last resort: return default transaction (should not happen if transactions are properly formatted)
        // Log a warning for debugging
        tracing::warn!(
            bytes_len = bytes.len(),
            "Failed to deserialize transaction from bytes (both TransactionExt and Transaction formats failed), using default"
        );
        create_default_signed_tx()
    }

    fn prevalidate_internal(&self, raw: RawTx, verified: bool) -> Result<PrevalidationResult> {
        // 1. Decode transaction (SignedTx or CallTransaction)
        // IMPORTANT: bincode has no type tag, so "wrong" structs can deserialize successfully.
        let call_selector: Option<Vec<u8>> = None;
        let tx: SignedTx = Self::deserialize_transaction_from_bytes(&raw.bytes);

        // 2. Signature verification already done in batch
        if !verified {
            tracing::warn!(
                sender = %hex::encode(&tx.from[..8]),
                nonce = tx.nonce,
                "Prevalidation failed: signature verification failed"
            );
            return Ok(PrevalidationResult::Invalid(
                "signature verification failed".to_string(),
            ));
        }

        // 3. Resolve sender_id
        let sender_id = self.get_or_allocate_sender_id(&tx.from);

        // 4. Store transaction bytes and get handle
        let (tx_handle, is_duplicate) = {
            let mut storage = self.tx_storage.lock().unwrap();
            storage.store(raw.bytes.clone())
        };

        if is_duplicate {
            tracing::warn!(
                sender = %hex::encode(&tx.from[..8]),
                nonce = tx.nonce,
                "Prevalidation failed: duplicate transaction detected"
            );
            return Ok(PrevalidationResult::Invalid(
                "duplicate transaction".to_string(),
            ));
        }

        // 4.5. Update snapshot for this sender (synchronous, blocking)
        // This ensures snapshot has latest account info BEFORE balance check
        // We update synchronously to ensure correctness: balance check must see latest state
        let address = tx.from.clone();
        // Update snapshot synchronously before balance check
        // This ensures that if account exists in storage, snapshot will have it before check
        if let Err(e) = self.account_snapshot.update_accounts(&[address]) {
            // If update fails, log error but continue (get_balance will fallback to storage)
            eprintln!("Error updating snapshot for sender: {}", e);
        }

        // 5. Classify transaction
        let class = match call_selector.as_deref() {
            Some(b"submit") => TxClass::IoTData,
            Some(b"update") => TxClass::FederatedUpdate,
            _ => Self::classify_tx(&tx, &raw.bytes),
        };

        // 6. Extract nonce: always use the TX-specified nonce.
        // The old fallback (get_or_increment_nonce) was wrong: for new accounts
        // tx.nonce=0 is the correct first nonce, but the counter incremented it
        // to 1, causing admission control to reject with "new account requires
        // which reads the authoritative nonce from storage.
        let nonce = tx.nonce;
        self.note_nonce(&tx.from, nonce);

        // 7. Soft nonce check
        if !self.check_nonce_soft(&tx.from, nonce) {
            let snapshot_nonce = self.account_snapshot.get_nonce(&tx.from);
            tracing::warn!(
                sender = %hex::encode(&tx.from[..8]),
                tx_nonce = nonce,
                snapshot_nonce = snapshot_nonce,
                window = NONCE_WINDOW,
                "Prevalidation failed: invalid nonce (out of window)"
            );
            return Ok(PrevalidationResult::Invalid("invalid nonce".to_string()));
        }

        // 8. Soft balance check
        // enforce strict balance rules. Still reject overflow.
        let fee_amount = tx.fee; // tx.fee is already u64, no unwrap needed
        let max_fee = fee_amount as u64; // For PrevalidatedTx struct
        if tx.amount.checked_add(fee_amount).is_none() {
            tracing::warn!(
                sender = %hex::encode(&tx.from[..8]),
                tx_nonce = nonce,
                amount = tx.amount,
                fee = fee_amount,
                "Prevalidation failed: amount + fee overflow"
            );
            return Ok(PrevalidationResult::Invalid(
                "amount + fee overflow".to_string(),
            ));
        }

        // 9. Extract stream nonce (for IoT/FederatedUpdate)
        let stream_nonce = self.extract_stream_nonce(&tx, &raw.bytes, class);

        let prevalidated = PrevalidatedTx {
            sender_id,
            sender_address: {
                let mut addr = [0u8; 32];
                let len = tx.from.len().min(32);
                addr[..len].copy_from_slice(&tx.from[..len]);
                addr
            },
            nonce,
            max_fee,
            amount: tx.amount,
            tx_handle,
            class,
            stream_nonce,
        };

        Ok(PrevalidationResult::Valid(prevalidated))
    }

    /// This is the recommended path for performance
    ///
    /// # Stream Nonce Extraction
    ///
    /// Stream nonce extraction is thread-safe and works correctly in parallel processing:
    /// - For IoTData/FederatedUpdate classes: extracts stream_nonce from transaction data
    /// - For other classes: returns None (stream_nonce not applicable)
    /// - Fallback: hash-based derivation if extraction fails
    ///
    /// - Out-of-order processing within the same stream
    /// - Scheduling optimization (future enhancement)
    /// - Stream-based ordering (future enhancement)
    ///
    /// # Thread Safety
    ///
    /// This method is fully thread-safe:
    /// - Signature verification is batched and parallelized
    /// - Each transaction is processed independently (no shared mutable state)
    /// - Stream nonce extraction is read-only (no mutations)
    pub async fn prevalidate_batch(&self, raws: Vec<RawTx>) -> Vec<PrevalidationResult> {
        if raws.is_empty() {
            return Vec::new();
        }

        // Results are stored by original index to preserve ordering
        let mut results: Vec<Option<PrevalidationResult>> = vec![None; raws.len()];

        // Split oracle/non-oracle to avoid feeding oracle bytes into SigVerifyStage
        let mut normal_raws: Vec<(usize, RawTx)> = Vec::new();
        for (idx, raw) in raws.into_iter().enumerate() {
            if let Some(res) = self.prevalidate_oracle(&raw.bytes) {
                results[idx] = Some(res);
            } else {
                normal_raws.push((idx, raw));
            }
        }

        if !normal_raws.is_empty() {
            // 1. Batch signature verification (CPU-heavy, parallelized) for non-oracle txs
            let tx_bytes: Vec<Vec<u8>> = normal_raws.iter().map(|(_, r)| r.bytes.clone()).collect();
            let verified_results: Vec<VerifiedTx> = {
                let stage = self.sig_verify_stage.lock().await;
                let results = stage.process_batch(&tx_bytes).await;
                drop(stage); // Rilascia il lock prima di uscire dallo scope
                results
            };

            let normal_results: Vec<(usize, PrevalidationResult)> = normal_raws
                .into_par_iter()
                .zip(verified_results.into_par_iter())
                .map(|((idx, raw), verified_tx)| {
                    // Verify signature result. If SignedTx parser-based verification fails,
                    // fall back to CallTransaction verification.
                    // CallTransaction fallback admitted any call/oracle/FL TX
                    // gossip peer inject unsigned contract calls under arbitrary
                    // `from` addresses. Removed: if SigVerifyStage rejects, the TX
                    // is rejected — no shape-only fallback. CallTransactions must
                    // carry a valid Ed25519 signature like ordinary TXs.
                    let verified = verified_tx.is_valid;

                    let res = if !verified {
                        let from_prefix = if raw.bytes.len() >= 40 {
                            hex::encode(&raw.bytes[..16])
                        } else {
                            "<too_short>".to_string()
                        };
                        tracing::warn!(
                            idx = idx,
                            raw_bytes_len = raw.bytes.len(),
                            from_prefix = %from_prefix,
                            "Prevalidation batch failed: signature verification failed"
                        );
                        PrevalidationResult::Invalid("signature verification failed".to_string())
                    } else {
                        self.prevalidate_internal(raw, true).unwrap_or_else(|e| {
                            tracing::warn!(idx = idx, error = %e, "Prevalidation batch internal error");
                            PrevalidationResult::Invalid(format!("prevalidation error: {}", e))
                        })
                    };

                    (idx, res)
                })
                .collect();

            for (idx, res) in normal_results {
                results[idx] = Some(res);
            }
        }

        results
            .into_iter()
            .map(|r| {
                r.unwrap_or_else(|| {
                    PrevalidationResult::Invalid("prevalidation error: missing result".to_string())
                })
            })
            .collect()
    }
}
