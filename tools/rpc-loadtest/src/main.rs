//! RPC Continuous Load Test — Sustained TPS via JSON-RPC
//!
//! Simulates real wallets: each worker manages sender accounts, queries nonce
//! from RPC, signs TX, submits via HTTP POST, and repeats continuously.
//!
//! Usage:
//!   rpc-loadtest --rpc-url http://127.0.0.1:8545 --duration 300 --senders 200 --concurrency 50
//!
//! Modes:
//!   --duration N              Continuous mode: run for N seconds (default)
//!   --total-tx N              Batch mode: submit exactly N TX then stop
//!   --recipient-mode fixed    All TX → 0x...01 (legacy, one shard).
//!   --recipient-mode random   P2P uniform: recipient picked from prefund pool.
//!   --recipient-mode hot      DeFi: recipient from --hot-count (default 10) addresses.
//!   --recipient-mode intra    Wallet-internal: sender == recipient.
//!   --hot-count N             Size of hot-address subset for --recipient-mode hot (default 10).
//!   --discover-proposer       Probe consensus_getProposer on each --urls, keep
//!                             only those where is_proposer=true (fallback: all).
//!
//! Cross-group traffic report:
//!   When the target cluster exposes consensus_getShardMap (savitri-rpc
//!   per TX and reports: intra/cross-group %, Gini coefficient, top-10 pair
//!   concentration. Feed into Layer-1 mempool routing design decision:
//!   Gini ≥ 0.6  → hierarchical bridge with per-hot-pair sub-topic
//!   Gini ≤ 0.3  → end-to-end forward (no bridge)
//!   otherwise   → end-to-end with hot-pair cache

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

// ─── Shard/group helpers (matching server-side ShardFilter::is_local) ────

/// Compute the shard id for an address using the same hashing scheme as
/// `savitri_mempool::ShardFilter::is_local`: std DefaultHasher (SipHash-1-3
/// with zero key, stable within a rustc release) over the raw address bytes,
/// then mod num_shards. The address is the 32-byte ed25519 verifying key,
/// NOT the hex encoding — hash must see the same bytes the server sees.
fn shard_of(address_bytes: &[u8], num_shards: u32) -> u32 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut hasher = DefaultHasher::new();
    address_bytes.hash(&mut hasher);
    (hasher.finish() as u32) % num_shards
}

/// Workload pattern for recipient selection.
#[derive(Debug, Clone, Copy)]
enum RecipientMode {
    /// Recipient = `0x...01` (legacy default, all TX cross to 1 shard).
    Fixed,
    /// Recipient = random element of the prefunded sender pool — simulates
    /// P2P transfers between uniformly-distributed wallets.
    Random,
    /// Recipient ∈ small set of N "hot" addresses — simulates DeFi traffic
    /// where many senders interact with a few contract addresses.
    Hot,
    /// Recipient = sender — simulates wallet-internal consolidation.
    Intra,
    /// Weighted combination of Random/Hot/Intra — mainnet-realistic
    /// synthesized from --mix "random:W1,hot:W2,intra:W3" percentages.
    Mixed,
}

impl RecipientMode {
    fn parse(s: &str) -> Option<Self> {
        match s {
            "fixed" => Some(RecipientMode::Fixed),
            "random" => Some(RecipientMode::Random),
            "hot" => Some(RecipientMode::Hot),
            "intra" => Some(RecipientMode::Intra),
            "mixed" => Some(RecipientMode::Mixed),
            _ => None,
        }
    }
}

/// Weights for the Mixed mode. Parsed from `--mix "random:60,hot:30,intra:10"`.
/// Stored as cumulative thresholds over 100 so a single `x % 100` lookup picks.
#[derive(Debug, Clone, Copy)]
struct MixWeights {
    /// Cumulative threshold for Random — picks < t_random use Random.
    t_random: u8,
    /// Cumulative threshold for Hot — t_random ≤ x < t_hot uses Hot.
    t_hot: u8,
    /// Cumulative threshold for Intra — t_hot ≤ x < t_intra uses Intra.
    /// Missing weight (100 - t_intra) falls through to Random as default.
    t_intra: u8,
}

impl MixWeights {
    fn parse(s: &str) -> Option<Self> {
        let mut r = 0u8;
        let mut h = 0u8;
        let mut i = 0u8;
        for part in s.split(',') {
            let (name, val) = part.trim().split_once(':')?;
            let v: u8 = val.trim().parse().ok()?;
            match name.trim() {
                "random" => r = v,
                "hot" => h = v,
                "intra" => i = v,
                _ => return None,
            }
        }
        // Build cumulative thresholds; total capped at 100 for stability.
        let total = r.saturating_add(h).saturating_add(i);
        if total == 0 {
            return None;
        }
        let t_random = r;
        let t_hot = t_random.saturating_add(h);
        let t_intra = t_hot.saturating_add(i);
        Some(MixWeights {
            t_random,
            t_hot,
            t_intra,
        })
    }
    /// Pick a concrete mode from a pseudo-random byte (0..=99).
    fn pick(&self, rand_byte: u8) -> RecipientMode {
        let x = rand_byte % 100;
        if x < self.t_random {
            RecipientMode::Random
        } else if x < self.t_hot {
            RecipientMode::Hot
        } else if x < self.t_intra {
            RecipientMode::Intra
        } else {
            RecipientMode::Random
        }
    }
}

/// Group id index: dense u16 per distinct group_id seen in shard_map, for
/// use as offset into the NxN pair-counter matrix.
#[derive(Clone)]
struct GroupIndex {
    /// Ordered group ids (sorted for deterministic output).
    ids: Arc<Vec<String>>,
    /// Reverse lookup: group_id → index.
    lookup: Arc<std::collections::HashMap<String, u16>>,
    /// shard_id → group index. Array form for O(1) lookup per TX.
    shard_to_idx: Arc<Vec<u16>>,
    /// Total shards (used when shard_map is missing shards — treat as sentinel UNKNOWN).
    num_shards: u32,
    /// Sentinel index reserved for unknown shards.
    unknown_idx: u16,
}

impl GroupIndex {
    /// Build from the `consensus_getShardMap` response. Unknown shards map to
    /// a sentinel "unknown" bucket so TX whose shards haven't been announced
    /// yet are countable separately (pre-announcement grace window).
    fn build(num_shards: u32, shard_map: std::collections::HashMap<u32, String>) -> Self {
        let mut distinct: std::collections::BTreeSet<String> =
            shard_map.values().cloned().collect();
        distinct.insert("<unknown>".to_string()); // sentinel
        let ids: Vec<String> = distinct.into_iter().collect();
        let lookup: std::collections::HashMap<String, u16> = ids
            .iter()
            .enumerate()
            .map(|(i, s)| (s.clone(), i as u16))
            .collect();
        let unknown_idx = *lookup.get("<unknown>").unwrap();
        let mut shard_to_idx = vec![unknown_idx; num_shards as usize];
        for (shard, gid) in shard_map.iter() {
            if (*shard as usize) < shard_to_idx.len() {
                if let Some(&idx) = lookup.get(gid) {
                    shard_to_idx[*shard as usize] = idx;
                }
            }
        }
        Self {
            ids: Arc::new(ids),
            lookup: Arc::new(lookup),
            shard_to_idx: Arc::new(shard_to_idx),
            num_shards,
            unknown_idx,
        }
    }
    fn n(&self) -> usize {
        self.ids.len()
    }
    fn group_idx_for_address(&self, address_bytes: &[u8]) -> u16 {
        let shard = shard_of(address_bytes, self.num_shards);
        self.shard_to_idx[shard as usize]
    }
}

/// Atomic NxN traffic matrix: `counts[src * N + dst]`.
struct TrafficMatrix {
    counts: Vec<AtomicU64>,
    n: usize,
}

impl TrafficMatrix {
    fn new(n: usize) -> Self {
        let mut counts = Vec::with_capacity(n * n);
        for _ in 0..(n * n) {
            counts.push(AtomicU64::new(0));
        }
        Self { counts, n }
    }
    fn record(&self, src: u16, dst: u16) {
        let i = (src as usize) * self.n + (dst as usize);
        if i < self.counts.len() {
            self.counts[i].fetch_add(1, Ordering::Relaxed);
        }
    }
    fn snapshot(&self) -> Vec<u64> {
        self.counts
            .iter()
            .map(|a| a.load(Ordering::Relaxed))
            .collect()
    }
}

/// e bucket-izza la latenza di inclusion in slot (0, 1, 2, 3, 4+, timeout).
/// Bucket legend:
///   slot_0    included in the slot whose block_height == submit_height + 1
///   slot_1    block_height == submit_height + 2
///   slot_2    block_height == submit_height + 3
///   slot_3    block_height == submit_height + 4
///   slot_4_plus  block_height > submit_height + 4
///   timeout   receipt non osservata entro 10 s
struct InclusionTracker {
    /// tx_hash → (submit_height, submit_instant)
    pending: Arc<std::sync::Mutex<std::collections::HashMap<[u8; 32], (u64, Instant)>>>,
    /// Current chain height — refreshed by a background task every 1s so
    /// the worker does not have to issue an RPC query per TX.
    current_height: AtomicU64,
    bucket_slot_0: AtomicU64,
    bucket_slot_1: AtomicU64,
    bucket_slot_2: AtomicU64,
    bucket_slot_3: AtomicU64,
    bucket_slot_4_plus: AtomicU64,
    bucket_timeout: AtomicU64,
}

impl InclusionTracker {
    fn new() -> Self {
        Self {
            pending: Arc::new(std::sync::Mutex::new(
                std::collections::HashMap::with_capacity(100_000),
            )),
            current_height: AtomicU64::new(0),
            bucket_slot_0: AtomicU64::new(0),
            bucket_slot_1: AtomicU64::new(0),
            bucket_slot_2: AtomicU64::new(0),
            bucket_slot_3: AtomicU64::new(0),
            bucket_slot_4_plus: AtomicU64::new(0),
            bucket_timeout: AtomicU64::new(0),
        }
    }
    fn record_submit(&self, hash: [u8; 32]) {
        let submit_height = self.current_height.load(Ordering::Relaxed);
        if let Ok(mut p) = self.pending.lock() {
            p.insert(hash, (submit_height, Instant::now()));
        }
    }
    fn bucket_inclusion(&self, submit_height: u64, block_height: u64) {
        let delta = block_height.saturating_sub(submit_height);
        match delta {
            0 | 1 => self.bucket_slot_0.fetch_add(1, Ordering::Relaxed),
            2 => self.bucket_slot_1.fetch_add(1, Ordering::Relaxed),
            3 => self.bucket_slot_2.fetch_add(1, Ordering::Relaxed),
            4 => self.bucket_slot_3.fetch_add(1, Ordering::Relaxed),
            _ => self.bucket_slot_4_plus.fetch_add(1, Ordering::Relaxed),
        };
    }
    fn bucket_timeout_inc(&self) {
        self.bucket_timeout.fetch_add(1, Ordering::Relaxed);
    }
}

/// Gini coefficient over a non-negative distribution. Range [0, 1]:
/// 0 = perfectly uniform (all buckets equal), 1 = fully concentrated (one bucket).
/// Classic formula: G = (2·Σ i·xᵢ − (n+1)·Σ xᵢ) / (n · Σ xᵢ) after sort ascending.
fn gini(values: &[u64]) -> f64 {
    let n = values.len();
    if n == 0 {
        return 0.0;
    }
    let mut v: Vec<u64> = values.iter().copied().collect();
    v.sort_unstable();
    let total: u128 = v.iter().map(|&x| x as u128).sum();
    if total == 0 {
        return 0.0;
    }
    let mut weighted: u128 = 0;
    for (i, &x) in v.iter().enumerate() {
        weighted += (i as u128 + 1) * (x as u128);
    }
    let num = 2.0 * weighted as f64 - (n as f64 + 1.0) * total as f64;
    let den = n as f64 * total as f64;
    num / den
}

// ─── JSON-RPC types ──────────────────────────────────────────────────────

#[derive(Serialize)]
struct JsonRpcRequest<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    params: serde_json::Value,
    id: u64,
}

#[derive(Deserialize)]
struct JsonRpcResponse {
    result: Option<serde_json::Value>,
    error: Option<serde_json::Value>,
}

// ─── TX wire format (matches savitri-lightnode TransactionExt) ───────────

use serde_big_array::BigArray;

#[derive(Serialize)]
struct TransactionExtWire {
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

fn sign_tx(
    keypair: &ed25519_dalek::SigningKey,
    from_hex: &str,
    to_hex: &str,
    amount: u64,
    nonce: u64,
    fee: u128,
) -> String {
    // (the path used by RPC submit). Signable bytes:
    //   sha256( from_hex.as_bytes() || to_hex.as_bytes()
    //         || amount_u64_le || nonce_u64_le || fee_u128_le )
    // Then ed25519.sign(digest). DO NOT use the format!("{}:...") path —
    // that is core/tx.rs::Transaction::message(), used by a different verify
    use sha2::Digest;
    let mut msg = Vec::with_capacity(64 + 64 + 8 + 8 + 16);
    msg.extend_from_slice(from_hex.as_bytes());
    msg.extend_from_slice(to_hex.as_bytes());
    msg.extend_from_slice(&amount.to_le_bytes());
    msg.extend_from_slice(&nonce.to_le_bytes());
    msg.extend_from_slice(&fee.to_le_bytes());
    let hash = sha2::Sha256::digest(&msg);
    let sig = ed25519_dalek::Signer::sign(keypair, hash.as_slice());

    let tx = TransactionExtWire {
        from: from_hex.to_string(),
        to: to_hex.to_string(),
        amount,
        nonce,
        fee: Some(fee),
        data: None,
        pubkey: keypair.verifying_key().to_bytes().to_vec(),
        sig: sig.to_bytes(),
        pre_verified: false,
    };
    use bincode::Options;
    let bytes = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .serialize(&tx)
        .expect("serialize TX");
    hex::encode(bytes)
}

fn generate_sender_keys(count: usize, run_id: u64) -> Vec<ed25519_dalek::SigningKey> {
    use sha2::Digest;
    (0..count)
        .map(|i| {
            let seed: [u8; 32] = sha2::Sha256::new()
                .chain_update(b"savitri-tx-gen-sender-")
                .chain_update(run_id.to_le_bytes())
                .chain_update((i as u32).to_le_bytes())
                .finalize()
                .into();
            ed25519_dalek::SigningKey::from_bytes(&seed)
        })
        .collect()
}

/// Fund sender accounts via faucet claims. Each claim gives 5 SAVT.
/// Repeats `claims_per_sender` times per sender to reach desired balance.
async fn fund_senders(
    client: &reqwest::Client,
    url: &str,
    senders: &[String],
    claims_per_sender: usize,
) {
    println!(
        "Funding {} sender accounts ({} claims each)...",
        senders.len(),
        claims_per_sender
    );
    let mut funded = 0usize;
    let mut failed = 0usize;
    for (i, addr) in senders.iter().enumerate() {
        for c in 0..claims_per_sender {
            let req = JsonRpcRequest {
                jsonrpc: "2.0",
                method: "savitri_faucetClaim",
                params: serde_json::json!([addr]),
                id: (i * 1000 + c) as u64,
            };
            match client.post(url).json(&req).send().await {
                Ok(resp) => {
                    if let Ok(rpc) = resp.json::<JsonRpcResponse>().await {
                        if rpc.error.is_none() {
                            funded += 1;
                        } else {
                            failed += 1;
                        }
                    }
                }
                Err(_) => {
                    failed += 1;
                }
            }
        }
    }
    println!("  Funded: {} claims OK, {} failed", funded, failed);
    // Wait for funding TX to be included in blocks
    println!("  Waiting 15s for funding TX to be committed...");
    tokio::time::sleep(Duration::from_secs(15)).await;
}

/// Query account balance from RPC
async fn get_balance(client: &reqwest::Client, url: &str, address: &str) -> Option<String> {
    let req = JsonRpcRequest {
        jsonrpc: "2.0",
        method: "account_getBalance",
        params: serde_json::json!([address]),
        id: 999999,
    };
    let resp = client.post(url).json(&req).send().await.ok()?;
    let rpc: JsonRpcResponse = resp.json().await.ok()?;
    let result = rpc.result?;
    result
        .get("balance")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
        .or_else(|| result.as_str().map(|s| s.to_string()))
}

/// Query account nonce from RPC
async fn get_nonce(client: &reqwest::Client, url: &str, address: &str, id: u64) -> Option<u64> {
    let req = JsonRpcRequest {
        jsonrpc: "2.0",
        method: "account_getNonce",
        params: serde_json::json!([address]),
        id,
    };
    let resp = client.post(url).json(&req).send().await.ok()?;
    let rpc: JsonRpcResponse = resp.json().await.ok()?;
    // Response format: {"result": {"nonce": N}} or {"result": N}
    let result = rpc.result?;
    result
        .get("nonce")
        .and_then(|v| v.as_u64())
        .or_else(|| result.as_u64())
}

// ─── Shared counters ─────────────────────────────────────────────────────

struct Stats {
    submitted: AtomicU64,
    accepted: AtomicU64,
    rejected: AtomicU64,
    errors: AtomicU64,
}

// ─── Adaptive per-account state ──────────────────────────────────────────
//
// Tier-1 adaptive nonce manager. Per-sender we track:
//   storage_nonce         = last known committed nonce (from RPC `account_getNonce`)
//   pending_count         = # of TX we've successfully submitted but not yet
//                           observed as committed (storage advances)
//   consecutive_duplicates = counter for exponential backoff when the RPC
//                           keeps returning "duplicate transaction"
//
// The attempt nonce for a new submission is `storage_nonce + pending_count`
// — a best-effort estimate of the first free nonce accounting for our own
// in-flight submissions. The background refresher periodically re-reads
// `storage_nonce` from RPC and decreases `pending_count` by however many
// TX committed since the last check, so the estimate self-heals.
//
// This avoids the two failure modes seen in earlier iterations:
//   * "blind bump on duplicate" → cache ran ahead of chain, TX trapped in
//     mempool with nonce gaps, acceptance → 0% in stages 4+.
//   * "resync-to-storage on duplicate" → reset nonce to last committed,
//     but our prior submission of that nonce was still in mempool → same
//     tx bytes re-submitted → same "duplicate transaction" cascade.
#[derive(Clone, Copy, Debug)]
struct AccountState {
    storage_nonce: u64,
    pending_count: u64,
    consecutive_duplicates: u32,
}

impl Default for AccountState {
    fn default() -> Self {
        Self {
            storage_nonce: 0,
            pending_count: 0,
            consecutive_duplicates: 0,
        }
    }
}

// No per-sender cap on in-flight submissions — the mempool's own per-sender
// limit acts as natural backpressure. The parallel nonce refresher and
// capacity-error hard-resync keep the nonce window self-healing under load.
const MAX_PENDING_PER_ACCOUNT: u64 = u64::MAX;

// Backoff schedule (ms) keyed on consecutive_duplicates. Chain block time
// is ~1s under load, so 1–2s lets a block commit before retry.
fn duplicate_backoff_ms(consecutive: u32) -> u64 {
    match consecutive {
        0..=2 => 100,
        3..=5 => 300,
        6..=9 => 800,
        10..=14 => 1_500,
        _ => 2_500,
    }
}

// ─── Worker: manages a set of senders, continuous TX submission ──────────

async fn worker(
    worker_id: usize,
    senders: Vec<(ed25519_dalek::SigningKey, String)>,
    client: reqwest::Client,
    urls: Vec<String>,
    recipient_pool: Arc<Vec<(String, [u8; 32])>>, // (hex, raw_bytes) for all recipients
    recipient_mode: RecipientMode,
    mix_weights: Option<MixWeights>,
    hot_pool: Arc<Vec<(String, [u8; 32])>>, // hot addresses (subset for Hot mode)
    stats: Arc<Stats>,
    running: Arc<AtomicBool>,
    states: Arc<RwLock<Vec<AccountState>>>,
    global_start_idx: usize,
    group_index: Option<Arc<GroupIndex>>,
    traffic: Option<Arc<TrafficMatrix>>,
    inclusion: Option<Arc<InclusionTracker>>,
    // 0 — every submission uses storage_nonce alone. This eliminates the
    // optimistic-nonce drift that fills the mempool with un-includable
    // ahead of confirmed state.
    wait_nonce: bool,
) -> Vec<u64> {
    let mut local_id: u64 = (worker_id as u64) * 10_000_000;
    let mut url_idx: usize = worker_id % urls.len();
    let mut recip_idx: usize = worker_id; // rotate per worker to avoid all workers picking recipient 0
    let mut latencies_us: Vec<u64> = Vec::with_capacity(16_384);

    while running.load(Ordering::Relaxed) {
        for (sender_idx, (key, from_hex)) in senders.iter().enumerate() {
            if !running.load(Ordering::Relaxed) {
                break;
            }

            let url = &urls[url_idx % urls.len()];
            url_idx += 1;

            let global_idx = global_start_idx + sender_idx;

            // Pick recipient per workload mode. For Mixed, draw a fresh mode
            // per TX from the weighted distribution using a cheap pseudo-random
            // byte derived from (local_id, sender_idx) — no extra crate, zero
            // overhead, uniform enough for sub-100 % bucketing.
            let sender_bytes: [u8; 32] = key.verifying_key().to_bytes();
            let effective_mode = match recipient_mode {
                RecipientMode::Mixed => {
                    let rb =
                        (local_id ^ (sender_idx as u64).wrapping_mul(0x9e3779b97f4a7c15)) as u8;
                    mix_weights
                        .map(|w| w.pick(rb))
                        .unwrap_or(RecipientMode::Random)
                }
                m => m,
            };
            let (recipient_hex, recipient_bytes): (String, [u8; 32]) = match effective_mode {
                RecipientMode::Fixed => {
                    // All TX go to address 0x...01 (legacy).
                    let mut b = [0u8; 32];
                    b[31] = 1;
                    (
                        "0000000000000000000000000000000000000000000000000000000000000001"
                            .to_string(),
                        b,
                    )
                }
                RecipientMode::Random => {
                    // Pick any prefunded address — uniformly distributed P2P.
                    recip_idx = recip_idx.wrapping_add(1);
                    let pool = &recipient_pool;
                    if pool.is_empty() {
                        continue;
                    }
                    let (h, b) = &pool[recip_idx % pool.len()];
                    (h.clone(), *b)
                }
                RecipientMode::Hot => {
                    // Pick from the hot subset — skewed DeFi-style traffic.
                    recip_idx = recip_idx.wrapping_add(1);
                    let pool = &hot_pool;
                    if pool.is_empty() {
                        continue;
                    }
                    let (h, b) = &pool[recip_idx % pool.len()];
                    (h.clone(), *b)
                }
                RecipientMode::Intra => {
                    // Sender transfers to self — wallet consolidation.
                    (from_hex.clone(), sender_bytes)
                }
                RecipientMode::Mixed => {
                    // Unreachable: Mixed is resolved to a concrete mode in
                    // effective_mode just above. Defensive fallback: random.
                    recip_idx = recip_idx.wrapping_add(1);
                    let pool = &recipient_pool;
                    if pool.is_empty() {
                        continue;
                    }
                    let (h, b) = &pool[recip_idx % pool.len()];
                    (h.clone(), *b)
                }
            };

            // Read a snapshot of this account's adaptive state. Any sender at
            // the pending-cap is skipped for this cycle to let the mempool
            // drain, with a brief sleep on the way out so the scheduler can
            // run the refresher and other workers.
            let (attempt_nonce, consecutive) = {
                let guard = states.read().await;
                match guard.get(global_idx) {
                    Some(s) if s.pending_count >= MAX_PENDING_PER_ACCOUNT => {
                        drop(guard);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                    Some(s) => (s.storage_nonce + s.pending_count, s.consecutive_duplicates),
                    None => (0, 0),
                }
            };

            // Back off exponentially before submitting when this account has
            // been producing duplicate errors. Scales with consecutive count
            // so a stuck sender doesn't spin the worker uselessly.
            if consecutive > 0 {
                tokio::time::sleep(Duration::from_millis(duplicate_backoff_ms(consecutive))).await;
            }

            let tx_hex = sign_tx(key, from_hex, &recipient_hex, 5, attempt_nonce, 1000);
            local_id += 1;

            // Record (src_group, dst_group) pair in the NxN traffic matrix. Done
            // before submit so even rejected TX count toward the distribution —
            // rejection doesn't retract intent to route.
            if let (Some(gi), Some(tm)) = (group_index.as_ref(), traffic.as_ref()) {
                let src = gi.group_idx_for_address(&sender_bytes);
                let dst = gi.group_idx_for_address(&recipient_bytes);
                tm.record(src, dst);
            }

            let req = JsonRpcRequest {
                jsonrpc: "2.0",
                method: "tx_sendTransaction",
                params: serde_json::json!([tx_hex]),
                id: local_id,
            };

            let t0 = Instant::now();
            match client.post(url).json(&req).send().await {
                Ok(resp) => {
                    let status = resp.status();
                    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
                        stats.rejected.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(10)).await;
                        continue;
                    }
                    stats.submitted.fetch_add(1, Ordering::Relaxed);
                    match resp.json::<JsonRpcResponse>().await {
                        Ok(rpc) => {
                            let elapsed_us = t0.elapsed().as_micros() as u64;
                            if let Some(ref err) = rpc.error {
                                stats.rejected.fetch_add(1, Ordering::Relaxed);
                                let msg = err.get("message").and_then(|v| v.as_str()).unwrap_or("");
                                let is_duplicate = msg.contains("duplicate transaction")
                                    || msg.contains("Duplicate nonce");
                                let is_nonce_low = msg.contains("nonce too low");
                                // Mempool capacity exceeded — actual error strings from
                                // savitri-mempool admission.rs and queued_pool.rs:
                                //   "per-sender cap exceeded"  → AdmissionConfig.max_per_sender
                                //   "global cap exceeded"      → AdmissionConfig.global_cap
                                //   "new account queued pool"  → queued pool for unfunded account
                                //   "queued pool:"             → any queued pool error
                                //   "PoolFull"                 → QueuedPoolError::PoolFull
                                //   "AccountQueueFull"         → per-account queued pool cap
                                //   "NonceGapTooLarge"         → nonce too far ahead of chain
                                let is_capacity = msg.contains("per-sender cap exceeded")
                                    || msg.contains("global cap exceeded")
                                    || msg.contains("new account queued pool")
                                    || msg.contains("queued pool:")
                                    || msg.contains("PoolFull")
                                    || msg.contains("AccountQueueFull")
                                    || msg.contains("NonceGapTooLarge")
                                    // Legacy strings kept for safety:
                                    || msg.contains("too many pending")
                                    || msg.contains("per-sender limit")
                                    || msg.contains("mempool full")
                                    || msg.contains("sender capacity");

                                if is_capacity {
                                    if let Some(fresh) = get_nonce(
                                        &client,
                                        url,
                                        from_hex,
                                        local_id.wrapping_add(600_000),
                                    )
                                    .await
                                    {
                                        let mut guard = states.write().await;
                                        if let Some(s) = guard.get_mut(global_idx) {
                                            s.storage_nonce = fresh;
                                            s.pending_count = 0;
                                            s.consecutive_duplicates = 0;
                                        }
                                    }
                                } else if is_duplicate {
                                    // Account has an in-flight submission at
                                    // this nonce that hasn't committed yet.
                                    // Probe storage on every duplicate: if
                                    // `fresh == attempt_nonce`, the chain has
                                    // NOT moved past our nonce — the server
                                    // already has this exact tx bytes in its
                                    // mempool or seen_transactions cache.
                                    // Bumping pending_count here moves the
                                    // client to nonce+1 on the next iteration,
                                    // avoiding the cascade where the same
                                    // bytes are re-submitted forever (1937/1937
                                    // duplicate rejects observed on testnet
                                    // because the first submit failed for a
                                    // server-side reason, the hash was cached,
                                    // and every subsequent retry at nonce=0
                                    // produced the same duplicate bytes).
                                    let mut guard = states.write().await;
                                    if let Some(s) = guard.get_mut(global_idx) {
                                        s.consecutive_duplicates =
                                            s.consecutive_duplicates.saturating_add(1);
                                    }
                                    drop(guard);
                                    if let Some(fresh) = get_nonce(
                                        &client,
                                        url,
                                        from_hex,
                                        local_id.wrapping_add(500_000),
                                    )
                                    .await
                                    {
                                        let mut guard = states.write().await;
                                        if let Some(s) = guard.get_mut(global_idx) {
                                            if fresh > s.storage_nonce {
                                                // Committed in the meantime;
                                                // account for those as drained
                                                // from our pending window.
                                                let advance = fresh - s.storage_nonce;
                                                s.pending_count =
                                                    s.pending_count.saturating_sub(advance);
                                                s.storage_nonce = fresh;
                                            } else if fresh == attempt_nonce {
                                                // Chain hasn't moved. Our tx
                                                // bytes are already cached
                                                // server-side; advance to
                                                // nonce+1 so we don't resubmit
                                                // the exact same bytes.
                                                if !wait_nonce {
                                                    s.pending_count =
                                                        s.pending_count.saturating_add(1);
                                                }
                                                s.consecutive_duplicates = 0;
                                            }
                                        }
                                    }
                                } else if is_nonce_low {
                                    // Chain moved past the nonce we tried —
                                    // the whole pending window is committed
                                    // (or earlier TX dropped). Hard-resync
                                    // storage_nonce and zero pending_count.
                                    if let Some(fresh) = get_nonce(
                                        &client,
                                        url,
                                        from_hex,
                                        local_id.wrapping_add(700_000),
                                    )
                                    .await
                                    {
                                        let mut guard = states.write().await;
                                        if let Some(s) = guard.get_mut(global_idx) {
                                            s.storage_nonce = fresh;
                                            s.pending_count = 0;
                                            s.consecutive_duplicates = 0;
                                        }
                                    }
                                }
                                // For other errors (insufficient balance,
                                // rate limit, etc.) don't touch state —
                                // the refresher will resync if needed.
                            } else {
                                stats.accepted.fetch_add(1, Ordering::Relaxed);
                                latencies_us.push(elapsed_us);
                                // Successful submission: bump pending_count,
                                // clear the consecutive counter so the next
                                // attempt doesn't wait.
                                let mut guard = states.write().await;
                                if let Some(s) = guard.get_mut(global_idx) {
                                    if !wait_nonce {
                                        s.pending_count = s.pending_count.saturating_add(1);
                                    }
                                    s.consecutive_duplicates = 0;
                                }
                                drop(guard);
                                // Inclusion tracking: grab tx_hash from the RPC
                                // response (result field = hex string) and stash
                                // it against the current block height so the
                                // poller can bucket the inclusion latency.
                                if let Some(ref tracker) = inclusion {
                                    if let Some(ref result) = rpc.result {
                                        if let Some(s) = result.as_str() {
                                            let s = s.trim_start_matches("0x");
                                            if let Ok(bytes) = hex::decode(s) {
                                                if bytes.len() == 32 {
                                                    let mut h = [0u8; 32];
                                                    h.copy_from_slice(&bytes);
                                                    tracker.record_submit(h);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let prev = stats.errors.fetch_add(1, Ordering::Relaxed);
                            if prev < 5 {
                                eprintln!(
                                    "  [worker-{}] response parse error (HTTP {}): {}",
                                    worker_id, status, e
                                );
                            }
                        }
                    }
                }
                Err(e) => {
                    let prev = stats.errors.fetch_add(1, Ordering::Relaxed);
                    if prev < 5 {
                        eprintln!("  [worker-{}] HTTP error: {}", worker_id, e);
                    }
                }
            }
        }
    }
    latencies_us
}

/// da 2s troppo lento: sotto loadtest intenso il chain avanza di ~100
/// nonce/sender/sec, il client tracking va out-of-sync rapidamente e
/// genera 30-80% di duplicate-reject residuali) queries ALL senders'
/// storage nonces in parallel (batched) and advances storage_nonce +
/// drains pending_count. Parallelism prevents the refresher from becoming
/// a bottleneck at high sender counts (sequential queries at 600 senders
/// × ~5ms each = 3s per cycle, longer than the interval itself).
async fn nonce_refresher(
    senders: Vec<String>,
    client: reqwest::Client,
    urls: Vec<String>,
    states: Arc<RwLock<Vec<AccountState>>>,
    running: Arc<AtomicBool>,
) {
    let mut interval = tokio::time::interval(Duration::from_millis(500));
    let client = Arc::new(client);
    let urls = Arc::new(urls);
    while running.load(Ordering::Relaxed) {
        interval.tick().await;
        // Spawn all nonce queries in parallel
        let mut handles = Vec::with_capacity(senders.len());
        for (i, addr) in senders.iter().enumerate() {
            let client = client.clone();
            let urls = urls.clone();
            let addr = addr.clone();
            handles.push(tokio::spawn(async move {
                let url = &urls[i % urls.len()];
                let nonce = get_nonce(&client, url, &addr, i as u64 + 900_000).await;
                (i, nonce)
            }));
        }
        // Collect results and apply updates
        let mut updates: Vec<(usize, u64)> = Vec::with_capacity(senders.len());
        for h in handles {
            if let Ok((i, Some(nonce))) = h.await {
                updates.push((i, nonce));
            }
        }
        if !updates.is_empty() {
            let mut guard = states.write().await;
            for (i, nonce) in updates {
                if let Some(s) = guard.get_mut(i) {
                    if nonce > s.storage_nonce {
                        let advance = nonce - s.storage_nonce;
                        s.storage_nonce = nonce;
                        s.pending_count = s.pending_count.saturating_sub(advance);
                    }
                    // If storage advanced past our estimate entirely, reset
                    if nonce >= s.storage_nonce + s.pending_count {
                        s.pending_count = 0;
                        s.consecutive_duplicates = 0;
                    }
                }
            }
        }
    }
}

// ─── Main ────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();

    // Accept --urls as alias for --rpc-url, --workers as alias for --concurrency
    let rpc_url_str = args
        .iter()
        .position(|a| a == "--urls" || a == "--rpc-url")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("http://127.0.0.1:8545");
    let mut rpc_urls: Vec<String> = rpc_url_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    // gli endpoint a quelli con is_proposer=true. Previene il caso in cui le TX
    // finiscono in un LN che non produce blocchi e are evitte per TTL prima
    // che HaveTx/TxFetch le consegni al proposer of the gruppo (delivery < 1%
    // misurato sotto c=50 loadtest). Nessun flag = comportamento legacy.
    let discover_proposer = args.iter().any(|a| a == "--discover-proposer");
    // last-known committed `storage_nonce` instead of the optimistic
    // `storage_nonce + pending_count`. Lets us measure end-to-end TPS
    // confirmed without the client racing ahead of chain state and
    // poisoning the mempool with high-nonce TXs.
    let wait_nonce = args.iter().any(|a| a == "--wait-nonce");
    let duration_secs: u64 = args
        .iter()
        .position(|a| a == "--duration")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);
    let concurrency: usize = args
        .iter()
        .position(|a| a == "--concurrency" || a == "--workers")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(50);
    let num_senders: usize = args
        .iter()
        .position(|a| a == "--senders")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    // --claims N: how many faucet claims per sender (each claim = 5 SAVT).
    // Default 1 (= 5 SAVT) — sufficient for trillions of TX at amount=5+fee=1000.
    // Set higher for psychological confidence or very long runs: --claims 2000 = 10,000 SAVT.
    let claims_per_sender: usize = args
        .iter()
        .position(|a| a == "--claims")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);
    // Per-run seed so every benchmark invocation derives a fresh set of sender
    // addresses. Priority: --seed flag > SAVITRI_TEST_PREFUND_SEED env > time.now().
    // The env-var fallback aligns loadtest senders with the LN prefund block
    // (ensure_genesis_block reads SAVITRI_TEST_PREFUND_SEED) so accounts exist
    // in storage when the loadtest starts — without this, 100% of TX land in
    // the queued_pool under Ok(None) "new account" path and never promote.
    let run_seed: u64 = args
        .iter()
        .position(|a| a == "--seed")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .or_else(|| {
            std::env::var("SAVITRI_TEST_PREFUND_SEED")
                .ok()
                .and_then(|s| s.parse().ok())
        })
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
        });

    // Proposer discovery: probe every URL with consensus_getProposer BEFORE
    // start. Keep only the endpoints that answer is_proposer=true. If no
    // endpoint is a proposer (very fresh cluster or rotation in progress),
    // keep all URLs so the test is not blocked.
    if discover_proposer && rpc_urls.len() > 1 {
        let probe_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(3))
            .build()?;
        let mut proposers: Vec<String> = Vec::new();
        println!(
            "Discovering proposer across {} endpoints...",
            rpc_urls.len()
        );
        for url in &rpc_urls {
            let body = serde_json::json!({
                "jsonrpc": "2.0",
                "method": "consensus_getProposer",
                "params": [],
                "id": 1,
            });
            match probe_client.post(url).json(&body).send().await {
                Ok(resp) => {
                    if let Ok(v) = resp.json::<serde_json::Value>().await {
                        let is_p = v
                            .pointer("/result/is_proposer")
                            .and_then(|x| x.as_bool())
                            .unwrap_or(false);
                        let gid = v
                            .pointer("/result/group_id")
                            .and_then(|x| x.as_str())
                            .unwrap_or("?");
                        println!("  {} → is_proposer={} group={}", url, is_p, gid);
                        if is_p {
                            proposers.push(url.clone());
                        }
                    }
                }
                Err(e) => {
                    println!("  {} → probe error: {}", url, e);
                }
            }
        }
        if proposers.is_empty() {
            println!(
                "  WARN: no proposer currently active — falling back to all {} URLs",
                rpc_urls.len()
            );
        } else {
            println!("  Using {} proposer endpoint(s)", proposers.len());
            rpc_urls = proposers;
        }
    }

    println!("================================================================");
    println!(" Savitri RPC Continuous Load Test");
    println!(
        " URLs: {} endpoint(s): {}",
        rpc_urls.len(),
        rpc_urls.join(", ")
    );
    println!(" Duration: {}s", duration_secs);
    println!(" Concurrency: {} workers", concurrency);
    println!(" Senders: {} accounts (seed={})", num_senders, run_seed);
    println!("================================================================");
    println!();

    let fund = args.iter().any(|a| a == "--fund");

    // Generate sender keys
    let senders = generate_sender_keys(num_senders, run_seed);
    let sender_hexes: Vec<String> = senders
        .iter()
        .map(|k| hex::encode(k.verifying_key().to_bytes()))
        .collect();
    let _recipient_legacy =
        "0000000000000000000000000000000000000000000000000000000000000001".to_string();

    // Determines how the worker picks a recipient for each TX:
    //   fixed    legacy, all TX → 0x...01 (no realistic distribution)
    //   random   uniform P2P — recipient is a random prefunded sender
    //   hot      DeFi-like — recipient from a small hot set (--hot-count N)
    //   intra    wallet-internal — sender == recipient
    let recipient_mode = args
        .iter()
        .position(|a| a == "--recipient-mode")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| RecipientMode::parse(s))
        .unwrap_or(RecipientMode::Fixed);
    let hot_count: usize = args
        .iter()
        .position(|a| a == "--hot-count")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    // --mix "random:60,hot:30,intra:10" — required for --recipient-mode mixed,
    // ignored otherwise. Default 60/30/10 seems to match casual observations
    // of crypto workloads (majority P2P, DeFi minority, some consolidation).
    let mix_weights: Option<MixWeights> = args
        .iter()
        .position(|a| a == "--mix")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| MixWeights::parse(s))
        .or_else(|| MixWeights::parse("random:60,hot:30,intra:10"));
    // --track-inclusion: enable per-TX inclusion-latency histogram via
    // periodic polling of tx_getTransaction. Adds ~1 RPC round-trip per
    // pending TX per poll_interval — use with moderate load.
    let track_inclusion = args.iter().any(|a| a == "--track-inclusion");

    // Recipient pool for Random / Hot modes = prefunded senders. This is safe
    // even if the recipient account is unknown to the LN because prefund seeds
    // nonce=0 + balance=1e24 for all `num_senders` addresses.
    let recipient_pool: Arc<Vec<(String, [u8; 32])>> = Arc::new(
        senders
            .iter()
            .map(|k| {
                let b = k.verifying_key().to_bytes();
                (hex::encode(&b), b)
            })
            .collect(),
    );
    let hot_pool: Arc<Vec<(String, [u8; 32])>> = Arc::new(
        recipient_pool
            .iter()
            .take(hot_count.min(num_senders))
            .cloned()
            .collect(),
    );
    println!(
        " Recipient mode: {:?} (hot_count={})",
        recipient_mode, hot_count
    );

    // ─── Cross-group traffic matrix setup ────────────────────────────────
    // Query consensus_getShardMap on the first endpoint to learn the
    // shard→group_id assignment, then build the NxN traffic matrix that
    // workers will increment on every TX submission.
    let (group_index, traffic_matrix) = {
        // Timeout 60s: shardMap may contain 65k entries (multi-MB JSON) under
        // num_shards=65536 production config — 5s default times out the decode.
        let probe_client = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "method": "consensus_getShardMap",
            "params": [],
            "id": 1,
        });
        match probe_client.post(&rpc_urls[0]).json(&body).send().await {
            Ok(resp) => match resp.json::<serde_json::Value>().await {
                Ok(v) => {
                    let n = v
                        .pointer("/result/num_shards")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0) as u32;
                    let map_val = v
                        .pointer("/result/shard_map")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null);
                    let mut map: std::collections::HashMap<u32, String> =
                        std::collections::HashMap::new();
                    if let Some(obj) = map_val.as_object() {
                        for (k, val) in obj.iter() {
                            if let (Ok(k_u32), Some(gid)) = (k.parse::<u32>(), val.as_str()) {
                                map.insert(k_u32, gid.to_string());
                            }
                        }
                    }
                    if n == 0 || map.is_empty() {
                        println!(" Traffic matrix: disabled (consensus_getShardMap returned empty — group announcements not yet processed?)");
                        (None, None)
                    } else {
                        let gi = Arc::new(GroupIndex::build(n, map));
                        let n_groups = gi.n();
                        println!(
                            " Traffic matrix: {} distinct groups × {} shards",
                            n_groups, n
                        );
                        let tm = Arc::new(TrafficMatrix::new(n_groups));
                        (Some(gi), Some(tm))
                    }
                }
                Err(e) => {
                    println!(" Traffic matrix: disabled (shardMap decode error: {})", e);
                    (None, None)
                }
            },
            Err(e) => {
                println!(" Traffic matrix: disabled (shardMap probe error: {})", e);
                (None, None)
            }
        }
    };

    // bucket-izzando la latenza in slot_0, slot_1, slot_2, slot_3, slot_4+,
    // timeout. Bucket disegnato per isolare pattern bimodali (es. nonce
    // desync) che un singolo P99 numero nasconde.
    let inclusion_tracker: Option<Arc<InclusionTracker>> = if track_inclusion {
        Some(Arc::new(InclusionTracker::new()))
    } else {
        None
    };
    // Initialize per-account adaptive state (all zeros — storage_nonce will
    // be filled in by the initial RPC query after funding, pending_count
    // grows on successful submissions and shrinks via the refresher task).
    let states = Arc::new(RwLock::new(vec![AccountState::default(); num_senders]));

    // Create HTTP client
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency + 10)
        .timeout(Duration::from_secs(10))
        .build()?;

    // Fund sender accounts if --fund flag is set.
    // Each faucet claim gives 5 SAVT (5e18 base units). At amount=5+fee=1000
    // per TX, even 1 claim lasts ~5e15 TX; set --claims 2000 for 10,000 SAVT.
    // Claims are spread round-robin across all RPC endpoints.
    if fund {
        println!(
            "Funding {} sender accounts via faucet ({} claim(s) each = {} SAVT)...",
            num_senders,
            claims_per_sender,
            5 * claims_per_sender
        );
        let mut ok = 0usize;
        let mut fail = 0usize;
        let mut url_idx = 0usize;
        for (i, addr) in sender_hexes.iter().enumerate() {
            for c in 0..claims_per_sender {
                let url = &rpc_urls[url_idx % rpc_urls.len()];
                url_idx += 1;
                let req = JsonRpcRequest {
                    jsonrpc: "2.0",
                    method: "savitri_faucetClaim",
                    params: serde_json::json!([addr]),
                    id: (i * 10_000 + c + 800_000) as u64,
                };
                match client.post(url).json(&req).send().await {
                    Ok(resp) => {
                        if let Ok(rpc) = resp.json::<JsonRpcResponse>().await {
                            if rpc.error.is_none() {
                                ok += 1;
                            } else {
                                fail += 1;
                            }
                        } else {
                            fail += 1;
                        }
                    }
                    Err(_) => {
                        fail += 1;
                    }
                }
            }
            if (i + 1) % 100 == 0 {
                eprint!("  {}/{} funded  \r", i + 1, num_senders);
            }
        }
        println!("  Funded: {} OK, {} failed", ok, fail);
        // Poll for funding TX to commit. We query sender 0's balance every 5s
        // for up to 120s. A balance > 0 means at least the first funding TX
        // committed. If still 0 after 120s the chain is likely stalled — we warn
        // and continue anyway (TX may be in queued pool waiting for promotion).
        println!("  Polling for funding TX commit (up to 120s)...");
        let poll_start = Instant::now();
        let funded = loop {
            tokio::time::sleep(Duration::from_secs(5)).await;
            let bal = get_balance(&client, &rpc_urls[0], &sender_hexes[0]).await;
            let balance_ok = bal
                .as_deref()
                .map(|s| s != "0" && !s.is_empty())
                .unwrap_or(false);
            if balance_ok {
                println!(
                    "  Sample balance (sender 0): {} ✓ (committed in {:.0}s)",
                    bal.unwrap(),
                    poll_start.elapsed().as_secs_f64()
                );
                break true;
            }
            if poll_start.elapsed().as_secs() >= 120 {
                println!("  WARNING: Sample balance (sender 0) still 0 after 120s — chain may be stalled.");
                println!("  Continuing anyway; TX will execute when blocks commit.");
                break false;
            }
            eprint!(
                "  Waiting for funding… {:.0}s elapsed, balance={}\r",
                poll_start.elapsed().as_secs_f64(),
                bal.as_deref().unwrap_or("?")
            );
        };
        let _ = funded; // suppress unused warning
    }

    // Query initial nonces, parallelized with a bounded worker pool. The
    // previous sequential loop at ~200ms RTT/query × 12000 senders took
    // ~40min and ate the entire test duration. Fan out up to 50 concurrent
    // queries across the 5 RPC endpoints; each worker pulls from a shared
    // index so we don't re-query the same address. Senders with no account
    // yet (e.g. unfunded) return None → left at storage_nonce=0.
    println!("Querying initial nonces from RPC (parallel)...");
    let init_t0 = Instant::now();
    let query_concurrency: usize = 50;
    let next_idx = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let done_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let sender_hexes_arc = Arc::new(sender_hexes.clone());
    let mut query_handles = Vec::with_capacity(query_concurrency);
    for w in 0..query_concurrency {
        let client_c = client.clone();
        let urls_c = rpc_urls.clone();
        let next_c = next_idx.clone();
        let done_c = done_count.clone();
        let hexes_c = sender_hexes_arc.clone();
        let states_c = states.clone();
        let total = num_senders;
        query_handles.push(tokio::spawn(async move {
            loop {
                let i = next_c.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if i >= total {
                    break;
                }
                let addr = &hexes_c[i];
                let url = &urls_c[(w * 17 + i) % urls_c.len()];
                if let Some(nonce) = get_nonce(&client_c, url, addr, i as u64).await {
                    let mut guard = states_c.write().await;
                    if let Some(s) = guard.get_mut(i) {
                        s.storage_nonce = nonce;
                    }
                }
                let done = done_c.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                if done % 500 == 0 {
                    eprint!("  {}/{} nonces queried\r", done, total);
                }
            }
        }));
    }
    for h in query_handles {
        let _ = h.await;
    }
    println!(
        "  Initial nonces loaded for {} accounts in {:.1}s",
        num_senders,
        init_t0.elapsed().as_secs_f64()
    );

    // Shared state
    let stats = Arc::new(Stats {
        submitted: AtomicU64::new(0),
        accepted: AtomicU64::new(0),
        rejected: AtomicU64::new(0),
        errors: AtomicU64::new(0),
    });
    let running = Arc::new(AtomicBool::new(true));

    // Split senders across workers
    let senders_per_worker = (num_senders + concurrency - 1) / concurrency;

    println!();
    println!(
        "Starting {} workers ({} senders each)...",
        concurrency, senders_per_worker
    );
    let start = Instant::now();

    // Start nonce refresher
    let refresher = tokio::spawn(nonce_refresher(
        sender_hexes.clone(),
        client.clone(),
        rpc_urls.clone(),
        states.clone(),
        running.clone(),
    ));

    // Start workers
    let mut handles = Vec::new();
    for w in 0..concurrency {
        let start_idx = w * senders_per_worker;
        let end_idx = ((w + 1) * senders_per_worker).min(num_senders);
        if start_idx >= num_senders {
            break;
        }

        let worker_senders: Vec<(ed25519_dalek::SigningKey, String)> = (start_idx..end_idx)
            .map(|i| {
                let seed = senders[i].to_bytes();
                (
                    ed25519_dalek::SigningKey::from_bytes(&seed),
                    sender_hexes[i].clone(),
                )
            })
            .collect();

        handles.push(tokio::spawn(worker(
            w,
            worker_senders,
            client.clone(),
            rpc_urls.clone(),
            recipient_pool.clone(),
            recipient_mode,
            mix_weights,
            hot_pool.clone(),
            stats.clone(),
            running.clone(),
            states.clone(),
            start_idx,
            group_index.clone(),
            traffic_matrix.clone(),
            inclusion_tracker.clone(),
            wait_nonce,
        )));
    }

    // ─── Inclusion tracker background tasks ─────────────────────────────
    if let Some(ref tracker) = inclusion_tracker {
        let t_height = Arc::clone(tracker);
        let client_h = client.clone();
        let url_h = rpc_urls[0].clone();
        let running_h = running.clone();
        tokio::spawn(async move {
            let body = serde_json::json!({
                "jsonrpc":"2.0","method":"chain_getBlockHeight","params":[],"id":1
            });
            while running_h.load(Ordering::Relaxed) {
                if let Ok(resp) = client_h.post(&url_h).json(&body).send().await {
                    if let Ok(v) = resp.json::<serde_json::Value>().await {
                        if let Some(h) = v.pointer("/result").and_then(|x| x.as_u64()) {
                            t_height.current_height.store(h, Ordering::Relaxed);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        let t_poll = Arc::clone(tracker);
        let client_p = client.clone();
        let urls_p = rpc_urls.clone();
        let running_p = running.clone();
        const POLL_TIMEOUT_SECS: u64 = 10;
        tokio::spawn(async move {
            use futures::stream::StreamExt;
            let mut poll_tick = tokio::time::interval(Duration::from_millis(500));
            let mut poll_id: u64 = 900_000_000;
            while running_p.load(Ordering::Relaxed) {
                poll_tick.tick().await;
                let snapshot: Vec<([u8; 32], u64, Instant)> = {
                    let guard = match t_poll.pending.lock() {
                        Ok(g) => g,
                        Err(_) => continue,
                    };
                    guard
                        .iter()
                        .map(|(k, v)| (*k, v.0, v.1))
                        .take(500)
                        .collect()
                };
                let (live, expired): (Vec<_>, Vec<_>) = snapshot
                    .into_iter()
                    .partition(|(_, _, t)| t.elapsed() <= Duration::from_secs(POLL_TIMEOUT_SECS));
                if !expired.is_empty() {
                    if let Ok(mut p) = t_poll.pending.lock() {
                        for (h, _, _) in &expired {
                            p.remove(h);
                        }
                    }
                    for _ in 0..expired.len() {
                        t_poll.bucket_timeout_inc();
                    }
                }
                let requests = live.into_iter().map(|(hash, submit_h, _)| {
                    poll_id = poll_id.wrapping_add(1);
                    let id = poll_id;
                    let url = urls_p[(id as usize) % urls_p.len()].clone();
                    let client = client_p.clone();
                    async move {
                        let body = serde_json::json!({
                            "jsonrpc":"2.0",
                            "method":"tx_getTransaction",
                            "params":[hex::encode(&hash)],
                            "id": id,
                        });
                        let resp = client.post(&url).json(&body).send().await.ok()?;
                        let v = resp.json::<serde_json::Value>().await.ok()?;
                        let bh = v
                            .pointer("/result/block_height")
                            .or_else(|| v.pointer("/result/blockHeight"))
                            .and_then(|x| x.as_u64())?;
                        Some((hash, submit_h, bh))
                    }
                });
                let results: Vec<_> = futures::stream::iter(requests)
                    .buffer_unordered(50)
                    .collect()
                    .await;
                let included: Vec<([u8; 32], u64, u64)> = results.into_iter().flatten().collect();
                if !included.is_empty() {
                    if let Ok(mut p) = t_poll.pending.lock() {
                        for (h, _, _) in &included {
                            p.remove(h);
                        }
                    }
                    for (_, submit_h, block_h) in included {
                        t_poll.bucket_inclusion(submit_h, block_h);
                    }
                }
            }
        });
    }

    // Progress reporter
    let stats_clone = stats.clone();
    let running_clone = running.clone();
    let reporter = tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        while running_clone.load(Ordering::Relaxed) {
            interval.tick().await;
            let elapsed = start.elapsed().as_secs_f64();
            let sub = stats_clone.submitted.load(Ordering::Relaxed);
            let acc = stats_clone.accepted.load(Ordering::Relaxed);
            let rej = stats_clone.rejected.load(Ordering::Relaxed);
            let err = stats_clone.errors.load(Ordering::Relaxed);
            if sub > 0 || err > 0 {
                println!(
                    "  [{:.0}s] submitted={} accepted={} rejected={} errors={} | TPS: {:.0} submit, {:.0} accept",
                    elapsed, sub, acc, rej, err,
                    sub as f64 / elapsed,
                    acc as f64 / elapsed,
                );
            }
        }
    });

    // Wait for duration
    tokio::time::sleep(Duration::from_secs(duration_secs)).await;
    running.store(false, Ordering::Relaxed);

    // Collect per-worker latency samples
    let mut all_latencies: Vec<u64> = Vec::new();
    for h in handles {
        if let Ok(worker_latencies) = h.await {
            all_latencies.extend(worker_latencies);
        }
    }
    refresher.abort();
    reporter.abort();

    let elapsed = start.elapsed();
    let sub = stats.submitted.load(Ordering::Relaxed);
    let acc = stats.accepted.load(Ordering::Relaxed);
    let rej = stats.rejected.load(Ordering::Relaxed);
    let err = stats.errors.load(Ordering::Relaxed);

    // Compute latency percentiles (microseconds)
    all_latencies.sort_unstable();
    let n = all_latencies.len();
    let pct = |p: f64| -> f64 {
        if n == 0 {
            return 0.0;
        }
        let idx = ((p * (n.saturating_sub(1)) as f64).round() as usize).min(n - 1);
        all_latencies[idx] as f64 / 1000.0 // convert to ms
    };
    let (min_ms, max_ms) = if n > 0 {
        (
            all_latencies[0] as f64 / 1000.0,
            all_latencies[n - 1] as f64 / 1000.0,
        )
    } else {
        (0.0, 0.0)
    };
    let mean_ms = if n > 0 {
        all_latencies.iter().sum::<u64>() as f64 / n as f64 / 1000.0
    } else {
        0.0
    };

    println!();
    println!("================================================================");
    println!(" RPC CONTINUOUS LOAD TEST RESULTS");
    println!("================================================================");
    println!();
    println!("  Duration:        {:.1}s", elapsed.as_secs_f64());
    println!("  Total submitted: {}", sub);
    println!(
        "  Accepted:        {} ({:.1}%)",
        acc,
        if sub > 0 {
            acc as f64 / sub as f64 * 100.0
        } else {
            0.0
        }
    );
    println!(
        "  Rejected:        {} ({:.1}%)",
        rej,
        if sub > 0 {
            rej as f64 / sub as f64 * 100.0
        } else {
            0.0
        }
    );
    println!("  Errors:          {}", err);
    println!();
    println!(
        "  Submission TPS:  {:.0}",
        sub as f64 / elapsed.as_secs_f64()
    );
    println!(
        "  Acceptance TPS:  {:.0}",
        acc as f64 / elapsed.as_secs_f64()
    );
    println!();
    println!("  RPC Latency (accepted TX, {} samples):", n);
    println!("    min    : {:>8.2} ms", min_ms);
    println!("    p50    : {:>8.2} ms", pct(0.50));
    println!("    p90    : {:>8.2} ms", pct(0.90));
    println!("    p95    : {:>8.2} ms", pct(0.95));
    println!("    p99    : {:>8.2} ms", pct(0.99));
    println!("    p99.9  : {:>8.2} ms", pct(0.999));
    println!("    max    : {:>8.2} ms", max_ms);
    println!("    mean   : {:>8.2} ms", mean_ms);
    println!();
    // ─── Cross-group traffic distribution report ─────────────────────────
    if let (Some(gi), Some(tm)) = (group_index.as_ref(), traffic_matrix.as_ref()) {
        let snap = tm.snapshot();
        let n = gi.n();
        let total: u64 = snap.iter().sum();
        if total == 0 {
            println!("  Traffic matrix: 0 samples recorded (was the test exercised?)");
        } else {
            // Diagonal = intra-group TX (src_group == dst_group). Off-diagonal = cross-group.
            let mut intra: u64 = 0;
            for i in 0..n {
                intra += snap[i * n + i];
            }
            let cross = total.saturating_sub(intra);
            let pct_cross = cross as f64 * 100.0 / total as f64;

            println!(
                "  ─── Traffic distribution (recipient_mode={:?}) ──────────────",
                recipient_mode
            );
            println!("  Total TX sampled: {}", total);
            println!(
                "  Intra-group:      {} ({:.2}%)",
                intra,
                intra as f64 * 100.0 / total as f64
            );
            println!("  Cross-group:      {} ({:.2}%)", cross, pct_cross);
            let g = gini(&snap);
            println!(
                "  Gini (NxN):       {:.4}  ({} — {})",
                g,
                if g >= 0.6 {
                    "HIGH concentration"
                } else if g <= 0.3 {
                    "LOW concentration"
                } else {
                    "medium"
                },
                if g >= 0.6 {
                    "→ hierarchical bridge (sub-topic per hot pair) is the right Layer-1 design"
                } else if g <= 0.3 {
                    "→ end-to-end forward is the right Layer-1 design"
                } else {
                    "→ intermediate zone — likely end-to-end forward with hot-pair caching"
                }
            );
            println!();
            println!(
                "  Matrix (src → dst counts); rows=src, cols=dst, groups sorted alphabetically:"
            );
            // Print header
            print!("    {:<24}", "src\\dst");
            for (j, gj) in gi.ids.iter().enumerate() {
                let short: String = gj.chars().take(16).collect();
                print!(" {:>10}", short);
                if j >= 7 {
                    print!(" ...");
                    break;
                }
            }
            println!();
            for (i, gi_name) in gi.ids.iter().enumerate() {
                let row_total: u64 = (0..n).map(|j| snap[i * n + j]).sum();
                let short: String = gi_name.chars().take(22).collect();
                print!("    {:<24}", short);
                for j in 0..n {
                    print!(" {:>10}", snap[i * n + j]);
                    if j >= 7 {
                        print!(" ...");
                        break;
                    }
                }
                println!("  (row_total={})", row_total);
                if i >= 7 {
                    println!("    ... ({} more rows)", n - 8);
                    break;
                }
            }
            println!();
            // Top-10 hot pairs (for gerarchico sub-topic design).
            let snap_ref: &[u64] = &snap;
            let mut pairs: Vec<(u16, u16, u64)> = (0..n)
                .flat_map(|i| (0..n).map(move |j| (i as u16, j as u16, snap_ref[i * n + j])))
                .filter(|(_, _, c)| *c > 0)
                .collect();
            pairs.sort_unstable_by(|a, b| b.2.cmp(&a.2));
            println!("  Top-10 hot (src,dst) pairs:");
            for (k, (si, di, c)) in pairs.iter().take(10).enumerate() {
                let s_short: String = gi.ids[*si as usize].chars().take(22).collect();
                let d_short: String = gi.ids[*di as usize].chars().take(22).collect();
                println!(
                    "    {:>2}.  {} → {}  count={} ({:.2}%)",
                    k + 1,
                    s_short,
                    d_short,
                    c,
                    *c as f64 * 100.0 / total as f64
                );
            }
            // Concentration of top-k pairs (design hint).
            for &topk in &[5usize, 10, 20] {
                let sum_top: u64 = pairs.iter().take(topk).map(|(_, _, c)| c).sum();
                println!(
                    "  Top-{} pairs capture {:.1}% of traffic",
                    topk,
                    sum_top as f64 * 100.0 / total as f64
                );
            }
        }
        println!();
    }
    // ─── Inclusion-latency histogram ─────────────────────────────────────
    if let Some(ref tracker) = inclusion_tracker {
        let b0 = tracker.bucket_slot_0.load(Ordering::Relaxed);
        let b1 = tracker.bucket_slot_1.load(Ordering::Relaxed);
        let b2 = tracker.bucket_slot_2.load(Ordering::Relaxed);
        let b3 = tracker.bucket_slot_3.load(Ordering::Relaxed);
        let b4p = tracker.bucket_slot_4_plus.load(Ordering::Relaxed);
        let bt = tracker.bucket_timeout.load(Ordering::Relaxed);
        let pending = tracker.pending.lock().map(|p| p.len() as u64).unwrap_or(0);
        let total = b0 + b1 + b2 + b3 + b4p + bt;
        println!("  ─── Inclusion latency histogram ─────────────────────────────");
        if total == 0 && pending == 0 {
            println!("    (no samples — is --track-inclusion enabled on a productive cluster?)");
        } else if total == 0 && pending > 0 {
            println!(
                "    {} TX still pending, 0 committed — cluster is producing empty blocks",
                pending
            );
            println!("    (proposer drain returning staged_txs=0 — check task #31 status)");
        } else {
            let pct = |n: u64| {
                if total > 0 {
                    n as f64 * 100.0 / total as f64
                } else {
                    0.0
                }
            };
            println!(
                "    slot_0 (= submit_height+1):    {:>8} ({:>5.2}%)",
                b0,
                pct(b0)
            );
            println!(
                "    slot_1 (= submit_height+2):    {:>8} ({:>5.2}%)",
                b1,
                pct(b1)
            );
            println!(
                "    slot_2 (= submit_height+3):    {:>8} ({:>5.2}%)",
                b2,
                pct(b2)
            );
            println!(
                "    slot_3 (= submit_height+4):    {:>8} ({:>5.2}%)",
                b3,
                pct(b3)
            );
            println!(
                "    slot_4+:                       {:>8} ({:>5.2}%)",
                b4p,
                pct(b4p)
            );
            println!(
                "    timeout (10s):                 {:>8} ({:>5.2}%)",
                bt,
                pct(bt)
            );
            println!("    pending (not yet resolved):    {:>8}", pending);
            let bimodal_flag = b0 > 0 && b4p > 0 && (b4p as f64 / total as f64) > 0.15;
            if bimodal_flag {
                println!(
                    "    ⚠ BIMODAL pattern detected: {:.1}% in slot_4+ while slot_0 active",
                    b4p as f64 * 100.0 / total as f64
                );
                println!("      → suggests tail TX are being fetched/re-propagated after delay");
            }
        }
        println!();
    }
    println!("================================================================");

    Ok(())
}
