// use savitri_core::crypto::signature::Keypair; // Implemented using sha2 for signing
#![allow(dead_code)]
use crate::storage::{BlockAndAccountStorage, BlockAndAccountStorageTrait, Storage};
use anyhow;
use ed25519_dalek::Signer;
use rand::{rngs::StdRng, RngCore, SeedableRng};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::{sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc::Sender, RwLock},
    time,
};
use tracing::warn;
use tracing::{error, info};

// Local implementations for types that were in savitri_node
#[allow(unused_imports)]
pub use savitri_core::FeeLimits;

// Helper module for serializing large arrays.
//
// serde's macro-based Serialize/Deserialize only covers [T; 0..=32].
// For N > 32 (e.g. [u8; 64]), `data.serialize(s)` auto-derefs to `&[u8]`
// and calls `serialize_bytes`, adding a u64 length prefix that
// `deserialize_tuple` doesn't expect.  We avoid this by explicitly
// using `serialize_tuple` / `deserialize_tuple` for ALL sizes.
pub mod big_array {
    use serde::{Deserializer, Serializer};

    pub fn serialize<S, const N: usize>(data: &[u8; N], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeTuple;
        let mut tuple = serializer.serialize_tuple(N)?;
        for byte in data {
            tuple.serialize_element(byte)?;
        }
        tuple.end()
    }

    pub fn deserialize<'de, D, const N: usize>(deserializer: D) -> Result<[u8; N], D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{SeqAccess, Visitor};
        struct ArrayVisitor<const M: usize>;
        impl<'de, const M: usize> Visitor<'de> for ArrayVisitor<M> {
            type Value = [u8; M];
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                write!(f, "a byte array of length {}", M)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
                let mut arr = [0u8; M];
                for i in 0..M {
                    arr[i] = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                }
                Ok(arr)
            }
        }
        deserializer.deserialize_tuple(N, ArrayVisitor::<N>)
    }
}

// Use our own Transaction type instead of savitri_core::Transaction
pub type SignedTx = TransactionExt;

// Extend Transaction with additional fields needed
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TransactionExt {
    pub from: String,
    pub to: String,
    pub amount: u64,
    pub nonce: u64,
    pub fee: Option<u128>,
    pub data: Option<Vec<u8>>,
    pub pubkey: Vec<u8>,
    #[serde(with = "big_array")]
    pub sig: [u8; 64],
    pub pre_verified: bool,
}

impl Default for TransactionExt {
    fn default() -> Self {
        Self {
            from: String::new(),
            to: String::new(),
            amount: 0,
            nonce: 0,
            fee: None,
            data: None,
            pubkey: Vec::new(),
            sig: [0u8; 64],
            pre_verified: false,
        }
    }
}

// Type alias for compatibility
pub type CallTransaction = TransactionExt;
pub type Transaction = TransactionExt;

/// Maximum allowed size for deserialized transaction data (1 MB).
/// SECURITY: Prevents memory exhaustion from crafted messages with huge length fields.
const MAX_DESERIALIZE_SIZE: u64 = 1_048_576;

/// Maximum length for address strings (hex-encoded 32-byte key = 64 chars)
const MAX_ADDRESS_LEN: usize = 128;

/// Maximum size for optional transaction data field (64 KB)
const MAX_TX_DATA_SIZE: usize = 65_536;

/// SECURITY: Validate transaction field sizes to prevent memory abuse from network data.
fn validate_tx_field_sizes(tx: &TransactionExt) -> Result<(), anyhow::Error> {
    if tx.from.len() > MAX_ADDRESS_LEN {
        anyhow::bail!(
            "'from' field too long: {} bytes (max {})",
            tx.from.len(),
            MAX_ADDRESS_LEN
        );
    }
    if tx.to.len() > MAX_ADDRESS_LEN {
        anyhow::bail!(
            "'to' field too long: {} bytes (max {})",
            tx.to.len(),
            MAX_ADDRESS_LEN
        );
    }
    if tx.pubkey.len() != 32 {
        anyhow::bail!("Invalid pubkey length: {} (expected 32)", tx.pubkey.len());
    }
    if let Some(ref data) = tx.data {
        if data.len() > MAX_TX_DATA_SIZE {
            anyhow::bail!(
                "'data' field too large: {} bytes (max {})",
                data.len(),
                MAX_TX_DATA_SIZE
            );
        }
    }
    Ok(())
}

pub fn deserialize_call_tx(bytes: &[u8]) -> Result<CallTransaction, anyhow::Error> {
    use bincode::Options;
    // SECURITY: enforce size limit to prevent memory exhaustion from crafted messages
    // Use legacy() encoding to match bincode::serialize() which uses fixed-width integers
    let tx: CallTransaction = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_DESERIALIZE_SIZE)
        .deserialize(bytes)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize call transaction: {}", e))?;
    validate_tx_field_sizes(&tx)?;
    Ok(tx)
}

/// Build and sign a TransactionExt using the same logic as lightnode transaction generation.
///
/// **This is a production function** - it's used internally by lightnodes and is available
/// for external clients (wallets, SDKs, transaction generators) to create valid transactions
/// that will pass `verify_transaction_signature_ext` verification.
///
/// This function ensures compatibility with the network's transaction verification logic.
///
/// # Arguments
/// * `keypair` - Ed25519 signing keypair
/// * `from` - Sender address (hex-encoded string)
/// * `to` - Recipient address (hex-encoded string)
/// * `amount` - Transaction amount (u64)
/// * `nonce` - Transaction nonce (u64)
/// * `fee` - Optional transaction fee (u128)
/// * `data` - Optional transaction data (Vec<u8>)
///
/// # Returns
/// A fully signed TransactionExt with pre_verified=false (will be verified by SigVerifyStage)
///
/// # Example
/// ```no_run
/// use savitri_lightnode::build_and_sign_transaction_ext;
/// use ed25519_dalek::SigningKey;
/// use rand::rngs::OsRng;
///
/// let keypair = SigningKey::generate(&mut OsRng);
/// let from_addr = hex::encode(keypair.verifying_key().to_bytes());
///
/// let tx = build_and_sign_transaction_ext(
///     &keypair,
///     from_addr,
///     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
///     100,
///     0,
///     Some(1000),
///     None,
/// );
/// ```
pub fn build_and_sign_transaction_ext(
    keypair: &ed25519_dalek::SigningKey,
    from: String,
    to: String,
    amount: u64,
    nonce: u64,
    fee: Option<u128>,
    data: Option<Vec<u8>>,
) -> TransactionExt {
    let from_addr: Vec<u8> = keypair.verifying_key().to_bytes().to_vec();

    let mut tx = TransactionExt {
        from,
        to,
        amount,
        nonce,
        fee,
        data,
        pubkey: from_addr.clone(),
        sig: [0u8; 64],
        pre_verified: false,
    };

    // (savitri_core::crypto::signature::build_tx_signable_v1) when fee is
    // Some and data is None — the production path. Both this builder and the
    // matching verifier (verify_transaction_signature_ext) delegate to the
    // canonical so they cannot drift apart.
    let signable: Vec<u8> = if tx.data.is_none() && tx.fee.is_some() {
        savitri_core::crypto::signature::build_tx_signable_v1(
            tx.from.as_bytes(),
            tx.to.as_bytes(),
            tx.amount,
            tx.nonce,
            tx.fee.unwrap(),
        )
    } else {
        // Legacy: contract-call (data Some) or fee=None builders. Mirrors the
        // legacy fallback in verify_transaction_signature_ext.
        let mut m = Vec::new();
        m.extend_from_slice(tx.from.as_bytes());
        m.extend_from_slice(tx.to.as_bytes());
        m.extend_from_slice(&tx.amount.to_le_bytes());
        m.extend_from_slice(&tx.nonce.to_le_bytes());
        if let Some(fee) = tx.fee {
            m.extend_from_slice(&fee.to_le_bytes());
        }
        if let Some(ref data) = tx.data {
            m.extend_from_slice(data);
        }
        m
    };
    let message_hash = sha2::Sha256::digest(&signable);
    let signature = keypair.sign(message_hash.as_slice());
    tx.sig = signature.to_bytes();

    tx
}

/// Verify signature for TransactionExt (gossip-RX path).
///
/// `savitri_core::crypto::signature::verify_tx_signature_v1`. Single source
/// of truth shared with the RPC-submit verifier in
/// the unification both implementations existed and could drift silently.
///
/// Two minor wire-format quirks were preserved for backward compatibility
/// with TX produced by older builders:
///   * `fee = None` → omitted from the signable bytes (canonical v1 would
///     use `unwrap_or(1000)` and always include 16 bytes). All current
///     producers (rpc-loadtest, build_and_sign_transaction_ext) set
///     `Some(fee)`, so this branch is dead in practice but kept defensive.
///   * `data = Some(d)` → appended to signable. No producer currently sets
///     `data` for plain transfers, but the field exists in TransactionExt
///     for future contract calls. When data IS present we cannot use the
///     canonical helper — fall back to the inline path.
pub fn verify_transaction_signature_ext(tx: &TransactionExt) -> bool {
    if tx.pubkey.len() != 32 {
        tracing::debug!(pubkey_len = tx.pubkey.len(), "Invalid pubkey length");
        return false;
    }
    if tx.sig.len() != 64 {
        tracing::debug!(sig_len = tx.sig.len(), "Invalid sig length");
        return false;
    }
    let pk_bytes: [u8; 32] = match tx.pubkey.as_slice().try_into() {
        Ok(b) => b,
        Err(_) => return false,
    };
    let sig_arr: [u8; 64] = tx.sig;

    // verify the signature. Without this, an attacker can forge `from` to point
    // at any victim address while signing with their own key — the signable
    // contains the victim's `from`, so ed25519::verify(signable, atk_sig, atk_pk)
    // succeeds and the mempool admits a TX that debits the victim. Mirrors the
    let from_bytes = match hex::decode(&tx.from) {
        Ok(b) => b,
        Err(_) => {
            tracing::debug!(from = %tx.from, "verify_transaction_signature_ext: from is not valid hex");
            return false;
        }
    };
    if from_bytes.as_slice() != pk_bytes.as_slice() {
        tracing::warn!(
            from = %tx.from,
            pubkey_hex = %hex::encode(pk_bytes),
            "verify_transaction_signature_ext: tx.from does not match pubkey (spoof)"
        );
        return false;
    }

    // Canonical v1 path (covers all production TX: fee=Some, data=None).
    if tx.data.is_none() && tx.fee.is_some() {
        let signable = savitri_core::crypto::signature::build_tx_signable_v1(
            tx.from.as_bytes(),
            tx.to.as_bytes(),
            tx.amount,
            tx.nonce,
            tx.fee.unwrap(),
        );
        return savitri_core::crypto::signature::verify_tx_signature_v1(
            &signable, &sig_arr, &pk_bytes,
        );
    }

    // Legacy fallback — only reachable when `data: Some(_)` (contract calls)
    // or `fee: None` (deprecated builders). Kept until those code paths are
    // either removed or migrated to a versioned canonical (v2) format.
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    let verifying_key = match VerifyingKey::from_bytes(&pk_bytes) {
        Ok(key) => key,
        Err(_) => return false,
    };
    let mut message = Vec::new();
    message.extend_from_slice(tx.from.as_bytes());
    message.extend_from_slice(tx.to.as_bytes());
    message.extend_from_slice(&tx.amount.to_le_bytes());
    message.extend_from_slice(&tx.nonce.to_le_bytes());
    if let Some(fee) = tx.fee {
        message.extend_from_slice(&fee.to_le_bytes());
    }
    if let Some(ref data) = tx.data {
        message.extend_from_slice(data);
    }
    let mut hasher = sha2::Sha256::new();
    hasher.update(&message);
    let message_hash = hasher.finalize();
    let signature = Signature::from_bytes(&sig_arr);
    verifying_key.verify(&message_hash, &signature).is_ok()
}

/// Serialize a TransactionExt using the same bincode options as deserialize_signed_tx.
/// CRITICAL: Always use this instead of bincode::serialize() for SignedTx/TransactionExt
/// to guarantee roundtrip compatibility (same fixint encoding + limit config).
pub fn serialize_signed_tx(tx: &TransactionExt) -> Result<Vec<u8>, anyhow::Error> {
    use bincode::Options;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_DESERIALIZE_SIZE)
        .serialize(tx)
        .map_err(|e| anyhow::anyhow!("Failed to serialize signed transaction: {}", e))
}

pub fn deserialize_signed_tx(bytes: &[u8]) -> Result<TransactionExt, anyhow::Error> {
    use bincode::Options;
    // SECURITY: enforce size limit to prevent memory exhaustion from crafted messages
    // Use with_fixint_encoding() to match bincode::serialize() which uses fixed-width integers
    let mut tx: TransactionExt = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_limit(MAX_DESERIALIZE_SIZE)
        .deserialize(bytes)
        .map_err(|e| anyhow::anyhow!("Failed to deserialize signed transaction: {}", e))?;

    validate_tx_field_sizes(&tx)?;

    // pre_verified as a public field so a malicious peer can craft a TX with
    // pre_verified=true and bypass signature verification entirely. Force the
    // flag back to false BEFORE the verification gate so the wire-controlled
    // value is never trusted. After local verification we may set it true to
    // skip duplicate work in the same process — that local cache is safe.
    tx.pre_verified = false;
    let verified = verify_transaction_signature_ext(&tx);
    if verified {
        tx.pre_verified = true;
    }
    // Note: we do NOT bail on verification failure here — the caller is the
    // and they inspect `tx.pre_verified` before admitting the TX. Returning
    // Err would be a behavioural change for non-malicious paths that decode
    // never make us return Ok with pre_verified=true unless verification
    // actually succeeded locally.

    Ok(tx)
}

pub fn hash_signed_tx_bytes(tx_bytes: &[u8]) -> [u8; 32] {
    // Compute SHA-256 hash of transaction bytes
    use sha2::Digest;
    let mut hasher = sha2::Sha256::new();
    hasher.update(tx_bytes);
    hasher.finalize().into()
}

// Helper trait to extend Storage with typed block methods
pub trait StorageBlockExt {
    fn get_block_typed(&self, height: u64) -> Result<Option<Block>, anyhow::Error>;
    fn set_block_typed(&self, height: u64, block: &Block) -> Result<(), anyhow::Error>;
    fn get_chain_head_typed(&self) -> Result<Option<Block>, anyhow::Error>;
    fn set_chain_head_typed(&self, block: &Block) -> Result<(), anyhow::Error>;
}

impl StorageBlockExt for Storage {
    fn get_block_typed(&self, height: u64) -> Result<Option<Block>, anyhow::Error> {
        self.get_block(height)
    }

    fn set_block_typed(&self, height: u64, block: &Block) -> Result<(), anyhow::Error> {
        self.set_block(height, block.clone())
    }

    fn get_chain_head_typed(&self) -> Result<Option<Block>, anyhow::Error> {
        self.get_chain_head()
    }

    fn set_chain_head_typed(&self, block: &Block) -> Result<(), anyhow::Error> {
        self.set_chain_head(block)
    }
}

/// Re-seed deterministic test senders (SAVITRI_TEST_PREFUND_*) regardless of
/// whether genesis already exists. `put_account` is idempotent and resets
/// nonce=0/balance=AMOUNT for the derived addresses; safe to call on every
/// boot. Without this, restart with persistent DB silently skips prefund and
/// the loadtest ends up with 100% rejected ("new account queued pool").
fn seed_test_prefund_senders(storage: &dyn crate::storage::BlockAndAccountStorage) {
    let count_str = match std::env::var("SAVITRI_TEST_PREFUND_SENDERS") {
        Ok(s) => s,
        Err(_) => return,
    };
    let count: usize = match count_str.parse() {
        Ok(n) if n > 0 => n,
        _ => return,
    };
    let seed_u64: u64 = std::env::var("SAVITRI_TEST_PREFUND_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    let amount: u128 = std::env::var("SAVITRI_TEST_PREFUND_AMOUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000_000_000_000_000_000u128);
    let t0 = std::time::Instant::now();
    let mut seeded = 0usize;
    for i in 0..count {
        let mut hasher = sha2::Sha256::new();
        sha2::Digest::update(&mut hasher, b"savitri-tx-gen-sender-");
        sha2::Digest::update(&mut hasher, seed_u64.to_le_bytes());
        sha2::Digest::update(&mut hasher, (i as u32).to_le_bytes());
        let seed_bytes: [u8; 32] = sha2::Digest::finalize(hasher).into();
        let sk = ed25519_dalek::SigningKey::from_bytes(&seed_bytes);
        let pk_bytes: [u8; 32] = sk.verifying_key().to_bytes();
        // Read-then-write to preserve nonce on restart while topping up balance
        // if it dropped below AMOUNT (prevents nonce reset breaking pending TX).
        let prior = storage.get_account(&pk_bytes).ok().flatten();
        let acc = crate::storage::Account {
            balance: amount,
            nonce: prior.as_ref().map(|a| a.nonce).unwrap_or(0),
            data: Vec::new(),
        };
        if storage.put_account(&pk_bytes, &acc).is_ok() {
            seeded += 1;
        }
    }
    tracing::info!(
        seeded,
        count,
        seed = seed_u64,
        amount = amount,
        elapsed_ms = t0.elapsed().as_millis() as u64,
        "Seeded pre-funded test senders (idempotent on restart)"
    );
}

pub fn ensure_genesis_block(
    storage: &dyn crate::storage::BlockAndAccountStorage,
) -> Result<(), anyhow::Error> {
    // Test-sender prefund is idempotent and runs every boot — see
    // seed_test_prefund_senders. Previously it was nested inside the
    // Ok(None) branch and silently skipped on restart with a persistent DB.
    seed_test_prefund_senders(storage);
    // Check if genesis block exists
    match storage.get_block(0) {
        Ok(Some(_)) => {
            // Genesis block already exists
            Ok(())
        }
        Ok(None) => {
            // Create genesis block with a deterministic non-zero hash so that
            // the first real block (height=1) will have a non-zero parent_hash
            // and pass LN's "Regular block must have non-zero parent hash" check.
            //
            // Hash formula matches compute_block_hash(): SHA256(parent_hash || state_root_64 || tx_root_64 || height_le).
            // With parent_hash=0, roots=0, height=0 the SHA256 digest is deterministic and non-zero.
            use sha2::{Digest, Sha256};
            let state_root = [0u8; 32];
            let tx_root = [0u8; 32];
            let parent_hash = [0u8; 64];
            let height: u64 = 0;
            let mut hasher = Sha256::new();
            hasher.update(&parent_hash);
            let mut state_root_64 = [0u8; 64];
            state_root_64[..32].copy_from_slice(&state_root);
            hasher.update(&state_root_64);
            let mut tx_root_64 = [0u8; 64];
            tx_root_64[..32].copy_from_slice(&tx_root);
            hasher.update(&tx_root_64);
            hasher.update(&height.to_le_bytes());
            let digest = hasher.finalize();
            let mut genesis_hash = [0u8; 64];
            genesis_hash[..32].copy_from_slice(digest.as_slice());

            let genesis_block = Block {
                hash: genesis_hash, // deterministic non-zero digest
                height: 0,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                parent_hash, // [0;64] — genesis has no parent
                state_root,
                tx_root,
                proposer: [0u8; 32],
                signature: [0u8; 64],
                parent_exec_hash: [0u8; 64],
                parent_ref_hash: [0u8; 64],
                version: 1,
            };

            // Store genesis block
            storage.set_block(0, genesis_block.clone())?;

            // Update chain head
            storage.set_chain_head(&genesis_block)?;

            // Seed genesis allocations into CF_ACCOUNTS. Lightnode's
            // `ensure_genesis_block` previously created the block header
            // only — no Account records were materialized for the addresses
            // in genesis_testnet.json (treasury + 10 faucet keys). As a
            // result `storage.get_account(faucet_key)` returned None, and
            // the very first `savitri_faucetClaim` hit the new-account
            // branch of mempool admission which routed the tx to
            // queued_pool@nonce=0. The round-robin then wrapped back to
            // the same faucet key and tried nonce=0 again → "Duplicate
            // nonce 0 for sender N" cascade. Here we mirror savitri-core's
            // initialize_genesis_mint in the path that lightnode actually
            // invokes.
            let genesis_spec = savitri_core::load_genesis_block();
            if let Ok(core_block) = genesis_spec {
                // Testnet JSON amounts are stored at 8 decimals (u64);
                // scale to 18 decimals (u128) for on-chain balance.
                const TESTNET_GENESIS_SCALE: u128 = 10_000_000_000;
                let mut seeded = 0usize;
                for tx in &core_block.transactions {
                    let to_hex = tx.to.trim_start_matches("0x");
                    let to_bytes = match hex::decode(to_hex) {
                        Ok(b) if b.len() == 32 => b,
                        _ => continue,
                    };
                    let amount_scaled: u128 = (tx.amount as u128) * TESTNET_GENESIS_SCALE;
                    let prior_balance = storage
                        .get_account(&to_bytes)
                        .ok()
                        .flatten()
                        .map(|a| a.balance)
                        .unwrap_or(0);
                    let acc = crate::storage::Account {
                        balance: prior_balance.saturating_add(amount_scaled),
                        nonce: 0,
                        data: Vec::new(),
                    };
                    if storage.put_account(&to_bytes, &acc).is_ok() {
                        seeded += 1;
                    }
                }
                tracing::info!(seeded, "Seeded genesis allocations into CF_ACCOUNTS");
            } else {
                tracing::warn!(
                    "ensure_genesis_block: failed to load savitri-core genesis spec; \
                     genesis allocations NOT materialized — faucet keys will have balance=0"
                );
            }

            // Test-sender prefund moved to seed_test_prefund_senders (called
            // unconditionally above) so it runs on every boot, not just at
            // cold-start.

            tracing::info!("Genesis block created and stored");
            Ok(())
        }
        Err(e) => Err(anyhow::anyhow!("Failed to check genesis block: {}", e)),
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Block {
    #[serde(with = "big_array")]
    pub hash: [u8; 64],
    pub height: u64,
    pub timestamp: u64,
    #[serde(with = "big_array")]
    pub parent_hash: [u8; 64],
    #[serde(with = "big_array")]
    pub state_root: [u8; 32],
    #[serde(with = "big_array")]
    pub tx_root: [u8; 32],
    #[serde(with = "big_array")]
    pub proposer: [u8; 32],
    #[serde(with = "big_array")]
    pub signature: [u8; 64],
    #[serde(with = "big_array")]
    pub parent_exec_hash: [u8; 64],
    #[serde(with = "big_array")]
    pub parent_ref_hash: [u8; 64],
    pub version: u32,
}

impl Default for Block {
    fn default() -> Self {
        Self {
            hash: [0u8; 64],
            height: 0,
            timestamp: 0,
            parent_hash: [0u8; 64],
            state_root: [0u8; 32],
            tx_root: [0u8; 32],
            proposer: [0u8; 32],
            signature: [0u8; 64],
            parent_exec_hash: [0u8; 64],
            parent_ref_hash: [0u8; 64],
            version: 0,
        }
    }
}

impl Block {
    pub fn new(_txs: Vec<Transaction>, proposer: [u8; 32]) -> Self {
        Self {
            hash: [0u8; 64],
            height: 0,
            timestamp: 0,
            parent_hash: [0u8; 64],
            state_root: [0u8; 32],
            tx_root: [0u8; 32],
            proposer,
            signature: [0u8; 64],
            parent_exec_hash: [0u8; 64],
            parent_ref_hash: [0u8; 64],
            version: 0,
        }
    }
}

#[derive(Clone)]
pub enum RecipientStrategy {
    Static(Vec<[u8; 32]>),
    Shared(Arc<RwLock<Vec<[u8; 32]>>>),
}

/// Generate deterministic sender keypairs for the tx generator.
/// Uses SHA-256("savitri-tx-gen-sender-" || index_le32) as the ed25519 seed.
/// The same derivation is used by `derive-pubkey --tx-gen-keys` so test scripts
/// can pre-compute genesis accounts.
/// `offset` shifts the starting index so multiple generator processes use
/// non-overlapping sender pools (e.g. offset=0 → indices 0..49, offset=50 → 50..99).
pub fn generate_tx_gen_sender_keys(count: usize, offset: usize) -> Vec<ed25519_dalek::SigningKey> {
    use sha2::Digest as _;
    (offset..offset + count)
        .map(|i| {
            let seed: [u8; 32] = sha2::Sha256::new()
                .chain_update(b"savitri-tx-gen-sender-")
                .chain_update((i as u32).to_le_bytes())
                .finalize()
                .into();
            ed25519_dalek::SigningKey::from_bytes(&seed)
        })
        .collect()
}

/// Pre-seed the mempool with a batch of pre-signed transactions for capacity testing.
/// Generates `total_tx` transactions with consecutive nonces per sender and injects
/// them directly into the mempool (bypassing gossipsub). This removes the TX generator
/// bottleneck and measures pure consensus + block production throughput.
pub async fn preseed_mempool(
    storage: Arc<dyn BlockAndAccountStorageTrait>,
    keypair: Arc<ed25519_dalek::SigningKey>,
    total_tx: usize,
    extra_sender_keys: Vec<Arc<ed25519_dalek::SigningKey>>,
    genesis_accounts: Vec<[u8; 32]>,
    mempool: &crate::p2p::block::LightnodeMempoolHandle,
) -> usize {
    const TX_FEE: u128 = 1_000;
    const TX_AMOUNT: u64 = 1;

    // Build sender list (same as run_tx_generator)
    let mut all_keys: Vec<Arc<ed25519_dalek::SigningKey>> = vec![keypair.clone()];
    all_keys.extend(extra_sender_keys.iter().cloned());
    let deterministic = generate_tx_gen_sender_keys(50, 0);
    all_keys.extend(deterministic.into_iter().map(Arc::new));

    // Map address → key
    let sender_by_addr: std::collections::HashMap<[u8; 32], Arc<ed25519_dalek::SigningKey>> =
        all_keys
            .iter()
            .map(|k| {
                let pub_bytes: [u8; 32] = k.verifying_key().to_bytes();
                (pub_bytes, k.clone())
            })
            .collect();

    // Select funded genesis senders
    let mut sender_addrs: Vec<[u8; 32]> = Vec::new();
    if !genesis_accounts.is_empty() {
        for addr in &genesis_accounts {
            if sender_by_addr.contains_key(addr) {
                if let Ok(Some(account)) =
                    BlockAndAccountStorage::get_account(storage.as_ref(), addr)
                {
                    if account.balance > 0 {
                        sender_addrs.push(*addr);
                    }
                }
            }
        }
    }
    if sender_addrs.is_empty() {
        warn!("preseed_mempool: no funded genesis senders available");
        return 0;
    }
    sender_addrs.sort_unstable();

    let senders: Vec<Arc<ed25519_dalek::SigningKey>> = sender_addrs
        .iter()
        .filter_map(|addr| sender_by_addr.get(addr).cloned())
        .collect();
    let sender_hexes: Vec<String> = sender_addrs.iter().map(hex::encode).collect();

    let num_senders = senders.len();
    let tx_per_sender = total_tx / num_senders;
    let recipient = "0000000000000000000000000000000000000000000000000000000000000001".to_string();

    info!(
        num_senders,
        tx_per_sender,
        total_tx = num_senders * tx_per_sender,
        "preseed_mempool: generating pre-signed transactions"
    );

    let start = std::time::Instant::now();
    let mut injected = 0usize;
    let mut failed = 0usize;

    for (idx, sender_key) in senders.iter().enumerate() {
        let from_hex = &sender_hexes[idx];
        // Get current storage nonce for this sender
        let base_nonce = BlockAndAccountStorage::get_account(storage.as_ref(), &sender_addrs[idx])
            .ok()
            .flatten()
            .map(|a| a.nonce)
            .unwrap_or(0);

        for n in 0..tx_per_sender as u64 {
            let mut tx = build_and_sign_transaction_ext(
                sender_key,
                from_hex.clone(),
                recipient.clone(),
                TX_AMOUNT,
                base_nonce + n,
                Some(TX_FEE),
                None,
            );
            // Even though we just produced the signature inline via
            // build_and_sign_transaction_ext, marking the TX pre-verified
            // bypasses the canonical-v1 verifier downstream. If a future
            // refactor changes the signable bytes in the builder but not in
            // the verifier (or vice versa), the pre_verified=true short-
            // circuits the safety net. The cost of leaving the flag false
            // is one ed25519 verify per TX in the consumer (~25us), which
            // is dwarfed by the mempool admit path itself.
            // tx.pre_verified intentionally left at the default `false`.

            // inner `Mempool` `std::sync::Mutex` provides the only sync
            // needed (drain and submit operate on disjoint sections).
            match mempool.add_transaction(tx) {
                Ok(_) => injected += 1,
                Err(_) => failed += 1,
            }

            // Log progress every 10K
            if (injected + failed) % 10_000 == 0 {
                info!(
                    injected,
                    failed,
                    elapsed_ms = start.elapsed().as_millis(),
                    "preseed_mempool: progress"
                );
            }
        }
    }

    let elapsed = start.elapsed();
    info!(
        injected,
        failed,
        elapsed_ms = elapsed.as_millis(),
        rate = if elapsed.as_secs() > 0 {
            injected as u64 / elapsed.as_secs()
        } else {
            injected as u64
        },
        "preseed_mempool: completed"
    );

    injected
}

pub async fn run_tx_generator(
    storage: Arc<dyn BlockAndAccountStorageTrait>,
    keypair: Arc<ed25519_dalek::SigningKey>,
    interval: Duration,
    _min_tx_per_second_per_recipient: u32,
    tx_tx: Sender<SignedTx>,
    recipients: RecipientStrategy,
    sender_offset: usize,
    extra_sender_keys: Vec<Arc<ed25519_dalek::SigningKey>>,
    genesis_accounts: Vec<[u8; 32]>,
) {
    /// Number of additional deterministic senders (+ the original keypair = NUM_SENDERS+1 total).
    const NUM_SENDERS: usize = 50;
    /// Fee: 0.00001 SAVT (8 decimals: 1 SAVT = 10^8)
    const TX_FEE: u128 = 1_000;
    /// Minimum transfer amount (1 base unit = 0.00000001 SAVT)
    const TX_AMOUNT: u64 = 1;
    /// Number of senders processed per tick (aggressive: 100 senders per tick).
    const SENDERS_PER_TICK: usize = 100;

    // Wait for gossipsub mesh formation before generating transactions.
    // With heartbeat_interval=7s, the mesh needs 2-3 heartbeats to GRAFT
    // new peers into topic meshes. TX-only generators that start before mesh
    // formation waste all TX as InsufficientPeers errors.
    info!("TX generator: waiting 20s for gossipsub mesh formation...");
    tokio::time::sleep(Duration::from_secs(20)).await;
    info!("TX generator: starting transaction generation");

    // Aggressive tick rate: 50ms when tx_interval_secs=0 (20 ticks/s)
    let actual_interval = if interval.as_secs() == 0 && interval.subsec_nanos() == 0 {
        Duration::from_millis(50)
    } else {
        interval
    };
    let mut ticker = time::interval(actual_interval);
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);

    // Build sender pool from:
    // - local tx key
    // - extra configured sender keys
    // - deterministic tx-gen keys
    let mut sender_by_addr: std::collections::HashMap<[u8; 32], Arc<ed25519_dalek::SigningKey>> =
        std::collections::HashMap::new();
    let mut insert_sender = |kp: Arc<ed25519_dalek::SigningKey>| {
        let addr = kp.verifying_key().to_bytes();
        sender_by_addr.entry(addr).or_insert(kp);
    };
    insert_sender(keypair.clone());
    for kp in extra_sender_keys {
        insert_sender(kp);
    }
    for kp in generate_tx_gen_sender_keys(NUM_SENDERS, sender_offset)
        .into_iter()
        .map(Arc::new)
    {
        insert_sender(kp);
    }

    // Select funded senders from genesis_accounts.
    // If genesis_accounts is empty, fallback to all known funded keys.
    let mut sender_addrs: Vec<[u8; 32]> = Vec::new();
    if !genesis_accounts.is_empty() {
        let mut seen = std::collections::HashSet::<[u8; 32]>::new();
        let mut missing_genesis_keys = 0usize;
        let mut zero_balance_genesis = 0usize;
        for addr in genesis_accounts {
            if !seen.insert(addr) {
                continue;
            }
            match BlockAndAccountStorage::get_account(storage.as_ref(), &addr) {
                Ok(Some(account)) if account.balance > 0 => {
                    if sender_by_addr.contains_key(&addr) {
                        sender_addrs.push(addr);
                    } else {
                        missing_genesis_keys += 1;
                    }
                }
                Ok(Some(_)) => {
                    zero_balance_genesis += 1;
                }
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        sender = %hex::encode(addr),
                        error = %e,
                        "TX generator: failed to read sender account while filtering genesis senders"
                    );
                }
            }
        }

        if missing_genesis_keys > 0 {
            warn!(
                missing_genesis_keys,
                funded_with_keys = sender_addrs.len(),
                "TX generator: some funded genesis accounts have no private key on this node (expected in multi-node setup); skipping those senders"
            );
        }

        if sender_addrs.is_empty() {
            warn!("TX generator: no funded genesis senders with private keys available; no transactions will be generated");
            return;
        }

        info!(
            funded_genesis_senders = sender_addrs.len(),
            zero_balance_genesis,
            skipped_no_key = missing_genesis_keys,
            "TX generator sender selection from genesis completed"
        );
    } else {
        for addr in sender_by_addr.keys() {
            if let Ok(Some(account)) = BlockAndAccountStorage::get_account(storage.as_ref(), addr) {
                if account.balance > 0 {
                    sender_addrs.push(*addr);
                }
            }
        }
        info!(
            funded_known_senders = sender_addrs.len(),
            "TX generator sender selection completed without genesis filter"
        );
    }

    if sender_addrs.is_empty() {
        warn!("TX generator: no funded sender available, stopping generator");
        return;
    }

    sender_addrs.sort_unstable();

    let senders: Vec<Arc<ed25519_dalek::SigningKey>> = sender_addrs
        .iter()
        .filter_map(|addr| sender_by_addr.get(addr).cloned())
        .collect();
    let sender_hexes: Vec<String> = sender_addrs.iter().map(hex::encode).collect();

    // Track next nonce per sender.
    let mut nonces: Vec<u64> = vec![0; senders.len()];
    // Cached storage nonces — refreshed every NONCE_REFRESH_TICKS to avoid
    // hammering RocksDB on every tick (was #1 perf bottleneck).
    let mut cached_storage_nonces: Vec<u64> = vec![0; senders.len()];

    // Initialize nonces from storage
    for (i, addr) in sender_addrs.iter().enumerate() {
        if let Ok(Some(account)) = BlockAndAccountStorage::get_account(storage.as_ref(), addr) {
            nonces[i] = account.nonce;
            cached_storage_nonces[i] = account.nonce;
        }
    }

    let mut rng = StdRng::seed_from_u64(0xDEADBEEF ^ sender_offset as u64);
    let mut sender_idx: usize = 0;
    let mut tick_count: u64 = 0;

    // Tuned settings:
    // - MAX_NONCE_AHEAD=1000: allows 50 senders × 1000 = 50K TX in-flight per generator.
    //   With max_block_txs=1000 and ~2s blocks, the network consumes ~500 TX/s,
    //   so 50K gives ~100s of headroom before nonce reset kicks in.
    // - NONCE_REFRESH_TICKS=100: refresh storage nonces every 5s (100 × 50ms)
    //   so the generator stays closer to committed state.
    const MAX_NONCE_AHEAD: u64 = 1000;
    const NONCE_REFRESH_TICKS: u64 = 100;

    info!(
        senders = senders.len(),
        senders_per_tick = SENDERS_PER_TICK,
        max_nonce_ahead = MAX_NONCE_AHEAD,
        tick_ms = actual_interval.as_millis(),
        "TX generator: aggressive mode started"
    );

    loop {
        ticker.tick().await;
        tick_count += 1;

        // Periodic nonce refresh from storage (every ~10s instead of every tick).
        // This is the key perf optimization: avoids 100 RocksDB reads per 50ms tick.
        if tick_count % NONCE_REFRESH_TICKS == 0 {
            for (i, addr) in sender_addrs.iter().enumerate() {
                if let Ok(Some(account)) =
                    BlockAndAccountStorage::get_account(storage.as_ref(), addr)
                {
                    cached_storage_nonces[i] = account.nonce;
                    // Catch up if storage advanced past us (block committed)
                    if nonces[i] < account.nonce {
                        nonces[i] = account.nonce;
                    }
                }
            }
        }

        let recipients_this_tick: Vec<[u8; 32]> = match &recipients {
            RecipientStrategy::Static(list) => {
                if list.is_empty() {
                    vec![random_recipient(&mut rng)]
                } else {
                    list.clone()
                }
            }
            RecipientStrategy::Shared(shared) => {
                let snapshot = shared.read().await;
                if snapshot.is_empty() {
                    vec![random_recipient(&mut rng)]
                } else {
                    snapshot.clone()
                }
            }
        };

        for _ in 0..SENDERS_PER_TICK {
            let idx = sender_idx % senders.len();
            sender_idx = sender_idx.wrapping_add(1);

            // Reset sender nonce when too far ahead of committed state.
            // The previous `continue` approach permanently blocked senders
            // on TX-only nodes where storage_nonce never advances (no local
            // block production). Resetting to storage_nonce allows the sender
            // to re-generate from the committed point; receiving nodes'
            // mempools deduplicate any already-seen nonces.
            if nonces[idx] > cached_storage_nonces[idx] + MAX_NONCE_AHEAD {
                nonces[idx] = cached_storage_nonces[idx];
            }

            for to_bytes in &recipients_this_tick {
                let tx_nonce = nonces[idx];
                nonces[idx] += 1;

                let tx = build_and_sign_transaction_ext(
                    &senders[idx],
                    sender_hexes[idx].clone(),
                    hex::encode(to_bytes),
                    TX_AMOUNT,
                    tx_nonce,
                    Some(TX_FEE),
                    None,
                );

                if tx_tx.send(tx).await.is_err() {
                    warn!("tx channel closed; stopping generator");
                    return;
                }
            }
        }
    }
}

fn random_recipient(rng: &mut StdRng) -> [u8; 32] {
    let mut to = [0u8; 32];
    rng.fill_bytes(&mut to);
    to
}
