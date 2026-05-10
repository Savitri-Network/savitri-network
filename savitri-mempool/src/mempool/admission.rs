use crate::mempool::nonce_limits::{
    ADMISSION_MAX_MAIN_POOL_NONCE_GAP, ADMISSION_MAX_MAIN_POOL_NONCE_GAP_RPC,
};
use crate::mempool::queued_pool::{QueuedPool, QueuedPoolConfig, QueuedPoolError};
use crate::mempool::types::TxClass;
use crate::mempool::PrevalidatedTx;
use savitri_core::Account;
use savitri_storage::{Storage, StorageTrait};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing;

/// Admission control policies
#[derive(Debug, Clone)]
pub struct AdmissionConfig {
    /// Max transactions per sender
    pub max_per_sender: usize,
    /// Max transactions per device (for IoT)
    pub max_per_device: usize,
    /// Global cap
    pub global_cap: usize,
    /// Fee floor (minimum fee to be admitted) - used when fee_floor_per_class is None
    pub fee_floor: u64,
    /// Per-class fee floors for testnet (Normal 1 SAVT, Contract 5 SAVT, IoT 0.05 SAVT).
    /// When Some, overrides fee_floor for the fee check.
    pub fee_floor_per_class: Option<HashMap<TxClass, u64>>,
    /// Quota per classe (max transactions per class)
    pub quota_per_class: HashMap<TxClass, usize>,
    /// Rate limit window (for IoT/FederatedUpdate)
    pub rate_limit_window: Duration,
    /// Max transactions per sender per window
    pub rate_limit_per_sender: usize,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        let mut quota = HashMap::new();
        // With 2000 TX/block and 1+ blk/s, the mempool must hold enough TX
        // to keep the consensus pipeline fed continuously.
        quota.insert(TxClass::Financial, 500_000);
        quota.insert(TxClass::IoTData, 200_000);
        quota.insert(TxClass::FederatedUpdate, 50_000);
        quota.insert(TxClass::System, 20_000);

        Self {
            // Per-sender cap: 2000 allows burst submission while consensus catches up.
            max_per_sender: 2000,
            max_per_device: 512,
            global_cap: 1_000_000,
            fee_floor: 1_000, // 0.00001 SAVT (8 decimals: 1 SAVT = 10^8)
            fee_floor_per_class: None,
            quota_per_class: quota,
            rate_limit_window: Duration::from_secs(60),
            rate_limit_per_sender: 5000, // 5K TX/sender/min allows sustained high throughput
        }
    }
}

impl AdmissionConfig {
    /// Testnet fee schedule (SAVT, 8 decimals: 1 SAVT = 10^8)
    /// Normal 0.00001 SAVT | Contract 0.0001 SAVT | IoT 0.000001 SAVT
    pub fn testnet_fees() -> Self {
        let mut fee_per_class = HashMap::new();
        fee_per_class.insert(TxClass::Financial, 1_000); // 0.00001 SAVT
        fee_per_class.insert(TxClass::IoTData, 100); // 0.000001 SAVT
        fee_per_class.insert(TxClass::FederatedUpdate, 10_000); // 0.0001 SAVT
        fee_per_class.insert(TxClass::System, 0); // free (system)
        let mut config = Self::default();
        config.fee_floor_per_class = Some(fee_per_class);
        config.fee_floor = 1_000; // fallback 0.00001 SAVT
        config
    }
}

/// Rate limiter state per sender
struct RateLimiter {
    /// Timestamps of recent transactions
    recent_txs: Vec<Instant>,
    /// Window duration
    window: Duration,
    /// Max transactions per window
    limit: usize,
}

/// Round contribution tracker (for IoT/FederatedUpdate)
/// Tracks contributions per round to ensure fairness
struct RoundContribution {
    /// Current round ID
    round_id: u64,
    /// Contributions per sender in current round
    contributions: HashMap<u32, usize>,
    /// Max contributions per sender per round
    max_per_sender: usize,
}

impl RateLimiter {
    fn new(window: Duration, limit: usize) -> Self {
        Self {
            recent_txs: Vec::new(),
            window,
            limit,
        }
    }

    fn check(&mut self) -> bool {
        let now = Instant::now();
        // Remove expired entries
        self.recent_txs
            .retain(|&ts| now.duration_since(ts) < self.window);
        // Check if under limit
        if self.recent_txs.len() >= self.limit {
            return false;
        }
        // Add current transaction
        self.recent_txs.push(now);
        true
    }
}

/// Result of admission check: tells caller what to do with the transaction
#[derive(Debug)]
pub enum AdmissionResult {
    /// Transaction admitted to main mempool (nonce is ready)
    Admitted,
    /// Transaction queued for future execution (nonce gap)
    Queued,
    /// Transaction rejected (with reason for logging)
    Rejected(String),
}

/// Admission control layer: enforces anti-spam and fairness policies
pub struct AdmissionControl {
    config: AdmissionConfig,
    /// Per-sender counts
    sender_counts: HashMap<u32, usize>,
    /// Per-device counts (for IoT) - device_id -> count
    device_counts: HashMap<u64, usize>,
    /// Per-class counts
    class_counts: HashMap<TxClass, usize>,
    /// Global count
    global_count: usize,
    /// Rate limiters per sender
    rate_limiters: HashMap<u32, RateLimiter>,
    /// Round contribution tracker (for IoT/FederatedUpdate)
    round_contribution: RoundContribution,
    /// Transaction hash cache for duplicate detection.
    /// Maps transaction hash -> (sender_id, nonce, inserted_at) for fast
    /// duplicate checking and time-based eviction.
    /// committed (gossip-only TX whose proposer never sees them, expired
    /// queued TX, etc.) can be aged out instead of accumulating forever and
    /// triggering false-positive "duplicate" rejections on retried TX.
    seen_transactions: HashMap<[u8; 32], (u32, u64, Instant)>,
    /// Maximum size of seen_transactions cache (to prevent memory bloat)
    max_seen_transactions: usize,
    /// Per-sender map of admitted nonce -> insertion timestamp.
    ///
    /// admission. The seen_transactions cache keys by tx_hash, so two
    /// differently-signed TX with the same (sender, nonce) — produced by
    /// client retries crossed with gossip re-broadcast — both pass the
    /// hash check and both land in the ready queue. When the proposer
    /// now mismatches → blocked_senders cascade, valid=0. Rejecting the
    /// duplicate at admission keeps the ready queue clean.
    /// `HashMap<u64, Instant>` so entries can be aged out when commit
    /// never fires (memory leak + false-positive duplicate rejects).
    admitted_nonces: HashMap<u32, HashMap<u64, Instant>>,
    /// TTL after which admitted_nonces / seen_transactions entries are
    /// considered stale and evictable. Should be ≥ queued_pool TTL so a
    /// queued-then-promoted TX never finds its dedup entry already gone.
    admitted_ttl: Duration,
    /// Counter to amortize eviction sweeps across admissions instead of
    /// running them on every call.
    admit_calls_since_evict: u32,
    storage: Option<Arc<dyn StorageTrait>>,
    /// Replay prevention system for comprehensive replay detection
    replay_prevention: Option<Arc<crate::mempool::replay_prevention::ReplayPrevention>>,
    /// Queued pool for transactions with future nonces
    queued_pool: QueuedPool,
}

impl RoundContribution {
    fn new(max_per_sender: usize) -> Self {
        Self {
            round_id: 0,
            contributions: HashMap::new(),
            max_per_sender,
        }
    }

    /// Check if sender can contribute to current round
    fn can_contribute(&self, sender_id: u32) -> bool {
        let count = self.contributions.get(&sender_id).copied().unwrap_or(0);
        count < self.max_per_sender
    }

    /// Record contribution
    fn record_contribution(&mut self, sender_id: u32) {
        *self.contributions.entry(sender_id).or_insert(0) += 1;
    }

    /// Start new round (called periodically or on block commit)
    fn new_round(&mut self) {
        self.round_id += 1;
        self.contributions.clear();
    }
}

impl AdmissionControl {
    /// Create new AdmissionControl with default configuration.
    ///
    /// `storage` and `replay_prevention` unset. With no `replay_prevention`
    /// the comprehensive replay/nonce-too-old/hash-replay checks at
    /// `check_admission_ext_with_source` step 0 are silently bypassed. The
    /// only remaining nonce gate is the storage one — and `storage` is also
    /// `None` here, so even that's skipped, leaving admission with no replay
    /// protection at all. **Production callers must use
    /// `with_storage_and_replay_prevention`**. This `new` is preserved only
    /// for unit tests; a runtime warn fires below to surface accidental
    /// production use.
    #[doc(hidden)]
    pub fn new(config: AdmissionConfig) -> Self {
        tracing::warn!(
            "AdmissionControl::new constructed without storage/replay_prevention — \
             replay & nonce checks DISABLED. Call site should be a unit test only; \
             production must use with_storage_and_replay_prevention (audit §2.6)."
        );
        Self {
            config,
            sender_counts: HashMap::new(),
            device_counts: HashMap::new(),
            class_counts: HashMap::new(),
            global_count: 0,
            rate_limiters: HashMap::new(),
            round_contribution: RoundContribution::new(100), // Default: 100 per sender per round
            seen_transactions: HashMap::new(),
            max_seen_transactions: 100_000, // Cache up to 100K transaction hashes
            storage: None,
            replay_prevention: None,
            queued_pool: QueuedPool::new(QueuedPoolConfig::default()),
            admitted_nonces: HashMap::new(),
            admitted_ttl: Duration::from_secs(300),
            admit_calls_since_evict: 0,
        }
    }

    ///
    /// `storage` (so the storage-nonce gate at step 1 is active) but leaves
    /// `replay_prevention=None`, silently disabling the comprehensive
    /// replay-attack and hash-replay checks at step 0. Callers must follow
    /// up with `set_replay_prevention` before serving traffic, or — better —
    /// use `with_storage_and_replay_prevention` which makes the dependency
    /// explicit at the type level. A runtime warn fires below to surface
    /// the gap.
    pub fn with_storage(config: AdmissionConfig, storage: Arc<dyn StorageTrait>) -> Self {
        tracing::warn!(
            "AdmissionControl::with_storage constructed without replay_prevention — \
             replay-attack and hash-replay checks DISABLED until set_replay_prevention is called \
             (audit §2.6)."
        );
        Self {
            config,
            sender_counts: HashMap::new(),
            device_counts: HashMap::new(),
            class_counts: HashMap::new(),
            global_count: 0,
            rate_limiters: HashMap::new(),
            round_contribution: RoundContribution::new(100),
            seen_transactions: HashMap::new(),
            max_seen_transactions: 100_000,
            storage: Some(storage),
            replay_prevention: None,
            queued_pool: QueuedPool::new(QueuedPoolConfig::default()),
            admitted_nonces: HashMap::new(),
            admitted_ttl: Duration::from_secs(300),
            admit_calls_since_evict: 0,
        }
    }

    /// Create AdmissionControl with storage and replay prevention
    pub fn with_storage_and_replay_prevention(
        config: AdmissionConfig,
        storage: Arc<dyn StorageTrait>,
        replay_prevention: Arc<crate::mempool::replay_prevention::ReplayPrevention>,
    ) -> Self {
        Self {
            config,
            sender_counts: HashMap::new(),
            device_counts: HashMap::new(),
            class_counts: HashMap::new(),
            global_count: 0,
            rate_limiters: HashMap::new(),
            round_contribution: RoundContribution::new(100),
            seen_transactions: HashMap::new(),
            max_seen_transactions: 100_000,
            storage: Some(storage),
            replay_prevention: Some(replay_prevention),
            queued_pool: QueuedPool::new(QueuedPoolConfig::default()),
            admitted_nonces: HashMap::new(),
            admitted_ttl: Duration::from_secs(300),
            admit_calls_since_evict: 0,
        }
    }

    /// Create AdmissionControl with custom queued pool configuration
    pub fn with_queued_pool_config(
        config: AdmissionConfig,
        queued_pool_config: QueuedPoolConfig,
    ) -> Self {
        let mut ac = Self::new(config);
        ac.queued_pool = QueuedPool::new(queued_pool_config);
        ac
    }

    /// Set replay prevention system
    pub fn set_replay_prevention(
        &mut self,
        replay_prevention: Arc<crate::mempool::replay_prevention::ReplayPrevention>,
    ) {
        self.replay_prevention = Some(replay_prevention);
    }

    /// Extract device ID from transaction (for IoT)
    /// Returns None if not applicable
    fn extract_device_id(pv: &PrevalidatedTx) -> Option<u64> {
        // In production, extract device ID from transaction data
        // For now, use sender_id as device_id for IoT transactions
        if matches!(pv.class, TxClass::IoTData) {
            Some(pv.sender_id as u64)
        } else {
            None
        }
    }

    /// Check if transaction should be admitted to the main mempool.
    ///
    /// Returns `AdmissionResult::Admitted` if ready for main mempool,
    /// `AdmissionResult::Queued` if parked in the queued pool for future nonce,
    /// or `AdmissionResult::Rejected` if invalid.
    ///
    /// The legacy `check_admission` method (returning bool) delegates here.
    pub fn check_admission_ext(
        &mut self,
        pv: &PrevalidatedTx,
        tx_hash: Option<[u8; 32]>,
    ) -> AdmissionResult {
        self.check_admission_ext_with_source(pv, tx_hash, false)
    }

    /// the caller signal that the TX arrived via local RPC. RPC-accepted TXs
    /// from senders without an on-chain account (e.g., loadtest fresh-seed not
    /// covered by prefund) used to be force-queued forever waiting for a funding
    /// commit, producing the queued-pool trap (50800/50800 admission outcomes
    /// "Queued"). With from_rpc=true we admit the TX into the main pool with an
    /// effective account.nonce=0 — matching the genesis-account semantics — so
    /// the proposer can include it in the next block.
    pub fn check_admission_ext_with_source(
        &mut self,
        pv: &PrevalidatedTx,
        tx_hash: Option<[u8; 32]>,
        from_rpc: bool,
    ) -> AdmissionResult {
        // with this exact (sender, nonce) pair. Two paths routinely produce
        // duplicates: (1) RPC retry from a client that didn't see the ACK,
        // (2) gossip re-broadcast of a TX we also received via RPC.
        // Without this check both copies are admitted to the ready queue;
        // "tx_nonce != expected_nonce" guard and triggers the
        // sender in the current drain round.
        // that never get committed (gossip-only TX whose proposer never sees
        // them, expired queued TX) don't pile up forever. Every 1024 calls we
        // evict entries older than `admitted_ttl`. The dedup window remains
        // ≥ TTL wide, so legitimate retries within the protection window are
        // still caught.
        self.admit_calls_since_evict = self.admit_calls_since_evict.saturating_add(1);
        if self.admit_calls_since_evict >= 1024 {
            self.admit_calls_since_evict = 0;
            self.evict_aged_entries(Instant::now());
        }
        if let Some(map) = self.admitted_nonces.get(&pv.sender_id) {
            if map.contains_key(&pv.nonce) {
                return AdmissionResult::Rejected(format!(
                    "duplicate (sender_id={}, nonce={}) already admitted",
                    pv.sender_id, pv.nonce
                ));
            }
        }

        // 0. CRITICAL: Replay prevention check (comprehensive)
        if let Some(replay_prevention) = &self.replay_prevention {
            if let Some(hash) = tx_hash {
                if replay_prevention.is_replay_transaction(&hash, &pv.sender_address, pv.nonce) {
                    return AdmissionResult::Rejected("replay attack detected".into());
                }

                // Only reject for nonce-too-old via replay prevention;
                // nonce-too-far is now handled by the queued pool below
                if let Err(e) =
                    replay_prevention.validate_transaction_nonce(&pv.sender_address, pv.nonce)
                {
                    match &e {
                        crate::mempool::replay_prevention::ReplayPreventionError::NonceTooOld {
                            ..
                        } => {
                            return AdmissionResult::Rejected(format!("replay prevention: {}", e));
                        }
                        crate::mempool::replay_prevention::ReplayPreventionError::NonceTooFar {
                            ..
                        } => {
                            // Don't reject here; let the queued pool handle it below
                        }
                        _ => {
                            return AdmissionResult::Rejected(format!("replay prevention: {}", e));
                        }
                    }
                }
            }
        }

        if let Some(storage) = &self.storage {
            match storage.get_account(&pv.sender_address) {
                Ok(Some(account_bytes)) => {
                    let account = match bincode::deserialize::<Account>(&account_bytes) {
                        Ok(acc) => acc,
                        Err(_) => match Account::decode(&account_bytes) {
                            Ok(acc) => acc,
                            Err(e) => {
                                tracing::warn!(
                                        "Admission rejected: failed to decode account (bincode and raw decode both failed, error={})",
                                        e
                                    );
                                return AdmissionResult::Rejected("account decode failed".into());
                            }
                        },
                    };

                    if pv.nonce < account.nonce {
                        tracing::warn!(
                            sender_address = %hex::encode(&pv.sender_address[..8]),
                            tx_nonce = pv.nonce,
                            account_nonce = account.nonce,
                            "Admission rejected: nonce too low (replay attack detected)"
                        );
                        return AdmissionResult::Rejected("nonce too low".into());
                    }

                    // Allow nonces up to account.nonce + GAP directly into the main pool.
                    // Must exceed the TX generator's MAX_NONCE_GAP (512) with headroom so all
                    // txs go directly to main pool instead of queued. drain_fair_batch handles
                    // fair allocation across senders.
                    //
                    // (e.g. consensus stalled, BFT cert mismatch), `account.nonce` stays
                    // pinned at the last committed value while RPC clients keep
                    // submitting fresh TX. With a 3000 gap the queued-pool trap fires:
                    // every TX past nonce 3000 lands in queued_pool waiting for a
                    // commit-driven promote() that never comes. We extend the trusted
                    // path's gap to 100K so the proposer's drain has ammo to break the
                    // stall — gossip path and untrusted senders keep the original 3000
                    // ceiling against memory-blowup spam.
                    // mempool::nonce_limits with const_assert! ordering checks.
                    let gap_ceiling = if from_rpc {
                        ADMISSION_MAX_MAIN_POOL_NONCE_GAP_RPC
                    } else {
                        ADMISSION_MAX_MAIN_POOL_NONCE_GAP
                    };
                    if pv.nonce > account.nonce + gap_ceiling {
                        // Future nonce: try to queue instead of rejecting
                        match self
                            .queued_pool
                            .try_queue(pv.clone(), tx_hash, account.nonce)
                        {
                            Ok(()) => {
                                tracing::info!(
                                    sender_address = %hex::encode(&pv.sender_address[..8]),
                                    tx_nonce = pv.nonce,
                                    account_nonce = account.nonce,
                                    "Transaction queued for future nonce (gap={})",
                                    pv.nonce - account.nonce
                                );
                                return AdmissionResult::Queued;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    sender_address = %hex::encode(&pv.sender_address[..8]),
                                    tx_nonce = pv.nonce,
                                    account_nonce = account.nonce,
                                    error = %e,
                                    "Admission rejected: could not queue future nonce transaction"
                                );
                                return AdmissionResult::Rejected(format!("queued pool: {}", e));
                            }
                        }
                    }

                    // which checks `account.balance >= fee + amount`. Previously
                    // admission checked only `fee`, so a TX with sufficient fee but
                    // insufficient `fee + amount` was admitted, took a slot, then
                    // rejected at drain time — triggering the blocked_senders
                    // sender in the same drain round.
                    let total_required = (pv.max_fee as u128).saturating_add(pv.amount as u128);
                    if account.balance < total_required {
                        tracing::warn!(
                            sender_address = %hex::encode(&pv.sender_address[..8]),
                            balance = account.balance,
                            required = total_required,
                            fee = pv.max_fee,
                            amount = pv.amount,
                            "Admission rejected: insufficient balance for fee + amount"
                        );
                        return AdmissionResult::Rejected("insufficient balance".into());
                    }
                }
                Ok(None) => {
                    // we trust the local node and admit it as if account.nonce=0,
                    // skipping the queued_pool detour. Without this, RPC-accepted TXs
                    // for unfunded senders (loadtest fresh-seed, new wallets) get
                    // permanently parked in queued_pool because the funding-commit-
                    // triggered promote() never fires for accounts that never get
                    // funded. The block proposer's drain then sees an empty main
                    // pool and produces tx_count=0 blocks — the queued-pool trap.
                    if from_rpc {
                        // Treat as account.nonce=0 and re-run nonce/balance gates
                        // below. We synthesize the gap check inline: if the TX
                        // nonce is within the trusted-path cap of 0 it goes to
                        // main pool; otherwise we still queue it, mirroring the
                        // funded-account branch above.
                        // trusted path so RPC-accepted TX from fresh senders
                        // don't fall into the queued-pool trap when consensus
                        // stalls and nonces accumulate beyond the legacy 3000
                        // cap. Constant centralized in mempool::nonce_limits.
                        if pv.nonce > ADMISSION_MAX_MAIN_POOL_NONCE_GAP_RPC {
                            match self.queued_pool.try_queue(pv.clone(), tx_hash, 0) {
                                Ok(()) => return AdmissionResult::Queued,
                                Err(e) => {
                                    return AdmissionResult::Rejected(format!(
                                        "queued pool (rpc, no acct): {}",
                                        e
                                    ))
                                }
                            }
                        }
                        // Skip the balance check — the prefund/funding pipeline will
                        // catch insufficient balance at execution time. This trades
                        // a possible execution-time reject for unblocking sustained
                        // throughput on RPC-accepted TXs.
                    } else {
                        // Gossip path or untrusted source: route ALL TXs to queued_pool
                        // (waiting for funding commit). Original behaviour preserved.
                        match self.queued_pool.try_queue(pv.clone(), tx_hash, 0) {
                            Ok(()) => {
                                tracing::info!(
                                    sender_address = %hex::encode(&pv.sender_address[..8]),
                                    tx_nonce = pv.nonce,
                                    "Transaction queued for new account (waiting for account creation)"
                                );
                                return AdmissionResult::Queued;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    sender_address = %hex::encode(&pv.sender_address[..8]),
                                    tx_nonce = pv.nonce,
                                    error = %e,
                                    "Admission rejected: new account, could not queue"
                                );
                                return AdmissionResult::Rejected(format!(
                                    "new account queued pool: {}",
                                    e
                                ));
                            }
                        }
                    } // end else gossip branch
                }
                Err(e) => {
                    tracing::warn!(
                        "Admission rejected: storage error when fetching account: {}",
                        e
                    );
                    return AdmissionResult::Rejected(format!("storage error: {}", e));
                }
            }
        }

        // 2. Duplicate transaction check (CRITICAL for consensus integrity)
        if let Some(hash) = tx_hash {
            if let Some((existing_sender, existing_nonce, _ts)) = self.seen_transactions.get(&hash)
            {
                if *existing_sender == pv.sender_id && *existing_nonce == pv.nonce {
                    tracing::warn!(
                        tx_hash = %hex::encode(&hash[..8]),
                        sender_id = pv.sender_id,
                        nonce = pv.nonce,
                        "Admission rejected: duplicate transaction detected"
                    );
                    return AdmissionResult::Rejected("duplicate transaction".into());
                }
                tracing::warn!(
                    tx_hash = %hex::encode(&hash[..8]),
                    "Admission rejected: hash collision detected"
                );
                return AdmissionResult::Rejected("hash collision".into());
            }
        }

        // 3. Fee floor check (per-class for testnet)
        let fee_floor = self
            .config
            .fee_floor_per_class
            .as_ref()
            .and_then(|m| m.get(&pv.class))
            .copied()
            .unwrap_or(self.config.fee_floor);
        if !matches!(pv.class, TxClass::System) && pv.max_fee < fee_floor {
            tracing::warn!(
                sender_id = pv.sender_id,
                fee = pv.max_fee,
                fee_floor = fee_floor,
                class = ?pv.class,
                "Admission rejected: fee too low"
            );
            return AdmissionResult::Rejected("fee too low".into());
        }

        // 4. Per-sender cap
        let sender_count = *self.sender_counts.get(&pv.sender_id).unwrap_or(&0);
        if sender_count >= self.config.max_per_sender {
            tracing::warn!(
                sender_id = pv.sender_id,
                count = sender_count,
                cap = self.config.max_per_sender,
                "Admission rejected: per-sender cap exceeded"
            );
            return AdmissionResult::Rejected("per-sender cap exceeded".into());
        }

        // 5. Global cap
        if self.global_count >= self.config.global_cap {
            tracing::warn!(
                global_count = self.global_count,
                global_cap = self.config.global_cap,
                "Admission rejected: global cap exceeded"
            );
            return AdmissionResult::Rejected("global cap exceeded".into());
        }

        // 6. Per-class quota
        let class_count = *self.class_counts.get(&pv.class).unwrap_or(&0);
        if let Some(&quota) = self.config.quota_per_class.get(&pv.class) {
            if class_count >= quota {
                tracing::warn!(
                    class = ?pv.class,
                    count = class_count,
                    quota = quota,
                    "Admission rejected: per-class quota exceeded"
                );
                return AdmissionResult::Rejected("per-class quota exceeded".into());
            }
        }

        // 7. Per-device cap (for IoT)
        if let Some(device_id) = Self::extract_device_id(pv) {
            let device_count = *self.device_counts.get(&device_id).unwrap_or(&0);
            if device_count >= self.config.max_per_device {
                tracing::warn!(
                    device_id = device_id,
                    count = device_count,
                    cap = self.config.max_per_device,
                    "Admission rejected: per-device cap exceeded"
                );
                return AdmissionResult::Rejected("per-device cap exceeded".into());
            }
        }

        // 8. Rate limiting (for IoT/FederatedUpdate)
        if matches!(pv.class, TxClass::IoTData | TxClass::FederatedUpdate) {
            let limiter = self.rate_limiters.entry(pv.sender_id).or_insert_with(|| {
                RateLimiter::new(
                    self.config.rate_limit_window,
                    self.config.rate_limit_per_sender,
                )
            });
            if !limiter.check() {
                tracing::warn!(
                    sender_id = pv.sender_id,
                    class = ?pv.class,
                    "Admission rejected: rate limit exceeded"
                );
                return AdmissionResult::Rejected("rate limit exceeded".into());
            }

            // 9. Round contribution check (for IoT/FederatedUpdate)
            if !self.round_contribution.can_contribute(pv.sender_id) {
                tracing::warn!(
                    sender_id = pv.sender_id,
                    class = ?pv.class,
                    "Admission rejected: round contribution limit exceeded"
                );
                return AdmissionResult::Rejected("round contribution limit exceeded".into());
            }
        }

        // Transaction accepted
        tracing::info!(
            sender_id = pv.sender_id,
            sender_address = %hex::encode(&pv.sender_address[..8]),
            nonce = pv.nonce,
            fee = pv.max_fee,
            class = ?pv.class,
            "Transaction admitted to mempool"
        );

        AdmissionResult::Admitted
    }

    /// Backward-compatible admission check (returns bool).
    /// Returns true only for `Admitted` (not for `Queued`).
    pub fn check_admission(&mut self, pv: &PrevalidatedTx, tx_hash: Option<[u8; 32]>) -> bool {
        matches!(
            self.check_admission_ext(pv, tx_hash),
            AdmissionResult::Admitted
        )
    }

    /// Evict entries from `seen_transactions` and `admitted_nonces` whose
    /// `inserted_at` is older than `self.admitted_ttl`.
    ///
    /// never committed (typical pattern: gossip-only TX whose proposer never
    /// receives it, or expired queued TX) leaves a permanent entry in both
    /// maps. After enough churn, retried TX from the same (sender, nonce)
    /// hit the dedup gate and are rejected as "duplicate" even though no
    /// active TX with that pair exists. This is also a slow memory leak.
    ///
    /// Called from `check_admission_ext_with_source` every 1024 admissions.
    pub fn evict_aged_entries(&mut self, now: Instant) {
        let ttl = self.admitted_ttl;
        let mut evicted_seen: usize = 0;
        let mut evicted_nonces: usize = 0;

        // seen_transactions: drop entries older than TTL.
        let stale: Vec<[u8; 32]> = self
            .seen_transactions
            .iter()
            .filter_map(|(k, (_, _, ts))| {
                if now.duration_since(*ts) > ttl {
                    Some(*k)
                } else {
                    None
                }
            })
            .collect();
        for k in stale {
            self.seen_transactions.remove(&k);
            evicted_seen += 1;
        }

        // admitted_nonces: drop per-sender entries older than TTL, then drop
        // empty sender buckets.
        let mut empty_senders: Vec<u32> = Vec::new();
        for (sid, nonce_map) in self.admitted_nonces.iter_mut() {
            let stale_nonces: Vec<u64> = nonce_map
                .iter()
                .filter_map(|(n, ts)| {
                    if now.duration_since(*ts) > ttl {
                        Some(*n)
                    } else {
                        None
                    }
                })
                .collect();
            for n in stale_nonces {
                nonce_map.remove(&n);
                evicted_nonces += 1;
            }
            if nonce_map.is_empty() {
                empty_senders.push(*sid);
            }
        }
        for sid in empty_senders {
            self.admitted_nonces.remove(&sid);
        }

        if evicted_seen > 0 || evicted_nonces > 0 {
            tracing::debug!(
                evicted_seen,
                evicted_nonces,
                ttl_secs = ttl.as_secs(),
                "AdmissionControl: aged-out dedup entries"
            );
        }
    }

    /// Record admitted transaction
    pub fn record_admission(&mut self, pv: &PrevalidatedTx, tx_hash: Option<[u8; 32]>) {
        let now = Instant::now();
        // Record transaction hash for duplicate detection
        if let Some(hash) = tx_hash {
            // entries by `inserted_at` instead of arbitrary HashMap iteration
            // order. Without this the FIFO degenerated to "drop random 1000",
            // which under stale-account churn could drop fresh entries while
            // keeping ancient never-committed TX hashes around.
            if self.seen_transactions.len() >= self.max_seen_transactions {
                let mut entries: Vec<([u8; 32], Instant)> = self
                    .seen_transactions
                    .iter()
                    .map(|(k, v)| (*k, v.2))
                    .collect();
                entries.sort_by_key(|(_, ts)| *ts);
                for (k, _) in entries.into_iter().take(1000) {
                    self.seen_transactions.remove(&k);
                }
            }
            self.seen_transactions
                .insert(hash, (pv.sender_id, pv.nonce, now));
        }

        // Track (sender, nonce) for dedup on later admission attempts.
        // See check_admission_ext header note on fix #a.
        self.admitted_nonces
            .entry(pv.sender_id)
            .or_default()
            .insert(pv.nonce, now);

        *self.sender_counts.entry(pv.sender_id).or_insert(0) += 1;
        *self.class_counts.entry(pv.class).or_insert(0) += 1;
        self.global_count += 1;

        // Record device count (for IoT)
        if let Some(device_id) = Self::extract_device_id(pv) {
            *self.device_counts.entry(device_id).or_insert(0) += 1;
        }

        // Record round contribution (for IoT/FederatedUpdate)
        if matches!(pv.class, TxClass::IoTData | TxClass::FederatedUpdate) {
            self.round_contribution.record_contribution(pv.sender_id);
        }
    }

    /// Record removal (when transaction is committed or purged)
    pub fn record_removal(&mut self, sender_id: u32, class: TxClass, tx_hash: Option<[u8; 32]>) {
        // Remove from seen transactions cache and from admitted_nonces dedup
        // set. We pull the (sender, nonce) tuple out of seen_transactions
        // before deleting so the dedup clearing uses the authoritative pair
        // (matches what check_admission_ext will check on re-submission).
        if let Some(hash) = tx_hash {
            if let Some((sid, nonce, _ts)) = self.seen_transactions.remove(&hash) {
                if let Some(map) = self.admitted_nonces.get_mut(&sid) {
                    map.remove(&nonce);
                    if map.is_empty() {
                        self.admitted_nonces.remove(&sid);
                    }
                }
            }
        }

        if let Some(count) = self.sender_counts.get_mut(&sender_id) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.sender_counts.remove(&sender_id);
            }
        }
        if let Some(count) = self.class_counts.get_mut(&class) {
            *count = count.saturating_sub(1);
        }
        self.global_count = self.global_count.saturating_sub(1);

        // Remove device count (for IoT)
        if matches!(class, TxClass::IoTData) {
            let device_id = sender_id as u64;
            if let Some(count) = self.device_counts.get_mut(&device_id) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    self.device_counts.remove(&device_id);
                }
            }
        }
    }

    /// ROUND 13: Record a TX being restored to the mempool (reverse of record_removal).
    /// Re-increments sender and class counts so quotas remain consistent.
    pub fn record_restoration(&mut self, sender_id: u32, class: TxClass) {
        *self.sender_counts.entry(sender_id).or_insert(0) += 1;
        *self.class_counts.entry(class).or_insert(0) += 1;
        self.global_count += 1;

        if matches!(class, TxClass::IoTData) {
            let device_id = sender_id as u64;
            *self.device_counts.entry(device_id).or_insert(0) += 1;
        }
    }

    /// Start new round (called on block commit or periodically)
    pub fn new_round(&mut self) {
        self.round_contribution.new_round();
    }

    /// Promote queued transactions after account nonce advances.
    /// Returns transactions ready to be added to the main mempool.
    pub fn promote_queued(
        &mut self,
        sender_id: u32,
        new_account_nonce: u64,
    ) -> Vec<(PrevalidatedTx, Option<[u8; 32]>)> {
        self.queued_pool.promote(sender_id, new_account_nonce)
    }

    /// Promote queued transactions for multiple accounts (batch, after block commit).
    pub fn promote_queued_batch(
        &mut self,
        nonce_updates: &HashMap<u32, u64>,
    ) -> Vec<(PrevalidatedTx, Option<[u8; 32]>)> {
        self.queued_pool.promote_batch(nonce_updates)
    }

    /// Run periodic cleanup on the queued pool (remove expired entries)
    pub fn cleanup_queued_pool(&mut self) {
        self.queued_pool.cleanup_expired();
    }

    /// Get queued pool statistics
    pub fn queued_pool_stats(&self) -> crate::mempool::queued_pool::QueuedPoolStats {
        self.queued_pool.get_stats()
    }
}
