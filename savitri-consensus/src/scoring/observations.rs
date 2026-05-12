//! Per-peer observation store backing real PoU scoring.
//!
//! The store collects measurements produced during normal P2P/consensus
//! `PouCalculator` can compute real component scores instead of returning
//! stub values. Step 1 covers latency; additional metrics are added in
//! subsequent commits following the same pattern.
//!
//! ## Design notes
//!
//! * **Bounded memory.** Each peer keeps a rolling window (default 1h) and a
//!   hard cap on samples per metric. Old samples are pruned lazily on insert
//!   and on read, so there is no background task to manage.
//! * **Deterministic outputs.** The store only records observations; the
//!   derived scores remain integer (`u64` permille) per AUDIT-003.
//! * **Sync API.** `std::sync::RwLock` keeps the critical section trivial
//!   and lets any caller — async or sync — record without `.await`.
//! * **No self-reporting.** Every measurement is supplied by the local node
//!   from its own observation of the peer; peers cannot inflate their own
//!   score through this API.

use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::types::{
    bucket_from_rtt_ms, DefaultScoreCalculator, IntegrityMeasurement, LatencyMeasurement,
    LatencyType, PeerLatencyObservation, ScoreCalculator,
};

/// Default rolling window for samples, in seconds (1 hour).
pub const DEFAULT_WINDOW_SECS: u64 = 3600;

/// Hard cap on samples kept per peer per metric, regardless of window.
/// Protects against memory exhaustion if a peer floods with measurements.
pub const MAX_SAMPLES_PER_METRIC: usize = 1024;

/// A single RTT sample tagged with its source.
#[derive(Debug, Clone)]
pub struct LatencySample {
    /// Unix timestamp (seconds) when the sample was taken.
    pub ts_secs: u64,
    /// Round-trip time in milliseconds.
    pub rtt_ms: u64,
    /// How the RTT was measured (ping, block propagation, ...).
    pub kind: LatencyType,
}

#[derive(Debug, Clone, Copy)]
pub struct ValidationSample {
    pub ts_secs: u64,
    pub valid: bool,
}

/// Why a peer was slashed. Only `DoubleVote` and `Equivocation` are expected
/// to map to score = 0; lesser offences apply a proportional penalty. The
/// scorer in `types/score.rs` currently treats every slash as 100 permille.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlashReason {
    DoubleVote,
    Equivocation,
    InvalidBlock,
    LateVote,
    /// Repeated Byzantine gradient updates: the peer's FL contribution
    /// score stayed below the configured threshold for a sustained streak
    /// (see `fl_robust::MALICIOUS_GRADIENT_STREAK`).
    MaliciousGradient,
    Other,
}

#[derive(Debug, Clone, Copy)]
pub struct SlashEvent {
    pub ts_secs: u64,
    pub reason: SlashReason,
}

/// Score assigned to a single peer's gradient update in an FL round.
/// Value is permille (0-1000): higher means closer to the round's robust
#[derive(Debug, Clone, Copy)]
pub struct FlContributionSample {
    pub ts_secs: u64,
    pub round_id: u64,
    pub score_permille: u16,
}

/// All observations accumulated for a single peer.
///
/// Fields are `pub` to allow richer reads from consumers, but writes must go
/// through `ObservationStore` so pruning and caps are enforced.
#[derive(Debug, Default)]
pub struct PeerObservations {
    pub latency: VecDeque<LatencySample>,
    pub block_validations: VecDeque<ValidationSample>,
    pub tx_validations: VecDeque<ValidationSample>,
    pub slash_events: VecDeque<SlashEvent>,
    pub fl_contributions: VecDeque<FlContributionSample>,
}

impl PeerObservations {
    fn push_latency(&mut self, sample: LatencySample, now: u64, window: u64) {
        self.latency.push_back(sample);
        Self::prune_deque(&mut self.latency, |s| s.ts_secs, now, window);
    }

    fn push_block_validation(&mut self, sample: ValidationSample, now: u64, window: u64) {
        self.block_validations.push_back(sample);
        Self::prune_deque(&mut self.block_validations, |s| s.ts_secs, now, window);
    }

    fn push_tx_validation(&mut self, sample: ValidationSample, now: u64, window: u64) {
        self.tx_validations.push_back(sample);
        Self::prune_deque(&mut self.tx_validations, |s| s.ts_secs, now, window);
    }

    fn push_slash(&mut self, event: SlashEvent, now: u64, window: u64) {
        self.slash_events.push_back(event);
        Self::prune_deque(&mut self.slash_events, |e| e.ts_secs, now, window);
    }

    fn push_fl_contribution(&mut self, sample: FlContributionSample, now: u64, window: u64) {
        self.fl_contributions.push_back(sample);
        Self::prune_deque(&mut self.fl_contributions, |s| s.ts_secs, now, window);
    }

    fn prune_deque<T, F: Fn(&T) -> u64>(buf: &mut VecDeque<T>, ts: F, now: u64, window: u64) {
        let cutoff = now.saturating_sub(window);
        while let Some(front) = buf.front() {
            if ts(front) < cutoff {
                buf.pop_front();
            } else {
                break;
            }
        }
        while buf.len() > MAX_SAMPLES_PER_METRIC {
            buf.pop_front();
        }
    }
}

/// Thread-safe store of `PeerObservations` keyed by peer id.
///
/// Clone-cheap: the inner state lives behind an `Arc`, so callers typically
/// hold a `Arc<ObservationStore>` and share it between the P2P layer (writers)
/// and the scoring pipeline (readers).
pub struct ObservationStore {
    inner: RwLock<HashMap<String, PeerObservations>>,
    window_secs: u64,
}

impl ObservationStore {
    pub fn new() -> Self {
        Self::with_window(DEFAULT_WINDOW_SECS)
    }

    pub fn with_window(window_secs: u64) -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            window_secs,
        }
    }

    /// Record an RTT observation for `peer_id`.
    ///
    /// Silently drops the sample if the internal lock is poisoned — scoring
    /// is best-effort and must never panic the P2P event loop.
    pub fn record_latency(&self, peer_id: &str, rtt_ms: u64, kind: LatencyType) {
        let now = now_secs();
        let Ok(mut map) = self.inner.write() else {
            return;
        };
        let entry = map.entry(peer_id.to_string()).or_default();
        entry.push_latency(
            LatencySample {
                ts_secs: now,
                rtt_ms,
                kind,
            },
            now,
            self.window_secs,
        );
    }

    pub fn record_block_validation(&self, peer_id: &str, valid: bool) {
        let now = now_secs();
        let Ok(mut map) = self.inner.write() else {
            return;
        };
        let entry = map.entry(peer_id.to_string()).or_default();
        entry.push_block_validation(
            ValidationSample {
                ts_secs: now,
                valid,
            },
            now,
            self.window_secs,
        );
    }

    /// Feed from mempool admission checks.
    pub fn record_tx_validation(&self, peer_id: &str, valid: bool) {
        let now = now_secs();
        let Ok(mut map) = self.inner.write() else {
            return;
        };
        let entry = map.entry(peer_id.to_string()).or_default();
        entry.push_tx_validation(
            ValidationSample {
                ts_secs: now,
                valid,
            },
            now,
            self.window_secs,
        );
    }

    /// Record a slash event for `peer_id`. The current scorer applies a flat
    /// 100 permille (10%) penalty per slash regardless of reason; the reason
    /// is kept for future-proofing and audit logging.
    pub fn record_slash(&self, peer_id: &str, reason: SlashReason) {
        let now = now_secs();
        let Ok(mut map) = self.inner.write() else {
            return;
        };
        let entry = map.entry(peer_id.to_string()).or_default();
        entry.push_slash(
            SlashEvent {
                ts_secs: now,
                reason,
            },
            now,
            self.window_secs,
        );
    }

    /// Record the robust per-client FL score computed by `fl_robust` at
    /// the end of a round. Callers should publish exactly one sample per
    /// (peer, round) — repeat calls are tolerated but only waste window
    /// capacity.
    pub fn record_fl_contribution(&self, peer_id: &str, round_id: u64, score_permille: u16) {
        let now = now_secs();
        let Ok(mut map) = self.inner.write() else {
            return;
        };
        let entry = map.entry(peer_id.to_string()).or_default();
        entry.push_fl_contribution(
            FlContributionSample {
                ts_secs: now,
                round_id,
                score_permille: score_permille.min(1000),
            },
            now,
            self.window_secs,
        );
    }

    /// Summarise recent FL contributions for `peer_id` as a single
    /// permille score consumable by `ScoreComponents::fl_integrity_score`.
    ///
    /// Policy (matching `integrity_score` semantics): a peer with no
    /// recorded contributions inside the window returns `1000` (perfect)
    /// so non-participating peers are not punished.
    pub fn build_fl_integrity_score(&self, peer_id: &str) -> u16 {
        let now = now_secs();
        let cutoff = now.saturating_sub(self.window_secs);
        let Ok(map) = self.inner.read() else {
            return 1000;
        };
        let Some(obs) = map.get(peer_id) else {
            return 1000;
        };
        let recent: Vec<u64> = obs
            .fl_contributions
            .iter()
            .filter(|s| s.ts_secs >= cutoff)
            .map(|s| s.score_permille as u64)
            .collect();
        if recent.is_empty() {
            return 1000;
        }
        // Simple arithmetic mean over the window. Equal weight per round is
        // the natural choice: a peer who was malicious last round and honest
        // this round should recover monotonically, not jump.
        let sum: u64 = recent.iter().sum();
        let avg = sum / recent.len() as u64;
        avg.min(1000) as u16
    }

    /// Count consecutive recent FL contributions below `threshold` for
    /// `peer_id`. The caller (typically the FL aggregator pipeline) uses
    /// this to decide whether to trigger a `MaliciousGradient` slash.
    ///
    /// Streak is computed from the most recent sample backwards, stopping
    /// at the first sample at or above `threshold`.
    pub fn bad_fl_streak(&self, peer_id: &str, threshold_permille: u16) -> usize {
        let Ok(map) = self.inner.read() else { return 0 };
        let Some(obs) = map.get(peer_id) else {
            return 0;
        };
        let mut streak = 0;
        for sample in obs.fl_contributions.iter().rev() {
            if sample.score_permille < threshold_permille {
                streak += 1;
            } else {
                break;
            }
        }
        streak
    }

    /// Number of FL contribution samples currently retained for `peer_id`.
    pub fn fl_contribution_count(&self, peer_id: &str) -> usize {
        self.inner
            .read()
            .ok()
            .and_then(|m| m.get(peer_id).map(|o| o.fl_contributions.len()))
            .unwrap_or(0)
    }

    /// Snapshot of every peer currently tracked in the store.
    ///
    /// Used by the streak daemon to enumerate FL participants for the
    /// `bad_fl_streak` check. Returns `Vec<String>` (peer hex ids) for
    /// stability — the daemon only needs read-only access.
    pub fn known_peers(&self) -> Vec<String> {
        self.inner
            .read()
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// Derive a single 0-1000 trust score from the components this store
    /// can observe — latency, integrity, and FL contribution — using the
    /// same weight ratios as `ScoreConfig::default()` (250 / 150 / 200,
    /// renormalised to 600 = 1000). Used as the FL aggregation weight
    /// modulator so the same observation surface that gates consensus
    /// eligibility (via `PouCalculator`) also weights FL contributions.
    ///
    /// Excludes availability / geographic / performance / reputation
    /// store does not own; the result is therefore a *behaviour-only*
    /// PoU view, deterministic and synchronous.
    ///
    /// Per existing semantics, peers with no recorded data on a
    /// component default to MAX (1000) for that component (no-data ≠
    /// penalty), so a fresh peer scores 1000 across the board. Note we
    /// override `calculate_latency_score`'s neutral-500 default for
    /// empty input to keep the no-data convention consistent.
    pub fn derive_observation_score(&self, peer_id: &str) -> u16 {
        const W_LATENCY: u64 = 250;
        const W_INTEGRITY: u64 = 150;
        const W_FL: u64 = 200;
        const W_TOTAL: u64 = W_LATENCY + W_INTEGRITY + W_FL; // 600

        let calc = DefaultScoreCalculator;
        let latency_samples = self.latency_measurements(peer_id);
        let lat = if latency_samples.is_empty() {
            1000u64
        } else {
            calc.calculate_latency_score(&latency_samples) as u64
        };
        let int_meas = self.build_integrity_measurement(peer_id, /* epoch hint */ 0);
        let integrity = calc.calculate_integrity_score(&int_meas) as u64;
        let fl = self.build_fl_integrity_score(peer_id) as u64;

        // Round-half-up division.
        let weighted = W_LATENCY * lat + W_INTEGRITY * integrity + W_FL * fl;
        let permille = (weighted + W_TOTAL / 2) / W_TOTAL;
        permille.min(1000) as u16
    }

    /// Build an `IntegrityMeasurement` snapshot for `peer_id` within the
    /// active window, directly consumable by
    /// `DefaultScoreCalculator::calculate_integrity_score`.
    ///
    /// Returns a measurement with all zeros for an unknown peer — the scorer
    /// treats zero-denominators as "no data, perfect integrity" (1000).
    pub fn build_integrity_measurement(&self, peer_id: &str, epoch: u64) -> IntegrityMeasurement {
        let now = now_secs();
        let cutoff = now.saturating_sub(self.window_secs);
        let Ok(map) = self.inner.read() else {
            return empty_integrity(peer_id, epoch);
        };
        let Some(obs) = map.get(peer_id) else {
            return empty_integrity(peer_id, epoch);
        };

        let (valid_blocks, total_blocks) = count_validations(&obs.block_validations, cutoff);
        let (valid_transactions, total_transactions) =
            count_validations(&obs.tx_validations, cutoff);
        let slash_events = obs
            .slash_events
            .iter()
            .filter(|e| e.ts_secs >= cutoff)
            .count() as u32;

        IntegrityMeasurement {
            node_id: peer_id.to_string(),
            valid_blocks,
            total_blocks,
            valid_transactions,
            total_transactions,
            slash_events,
            epoch,
        }
    }

    /// Build `LatencyMeasurement`s for `peer_id` within the active window.
    ///
    /// Consumed directly by `DefaultScoreCalculator::calculate_latency_score`
    /// in `types/score.rs`. Returns an empty vector if the peer is unknown.
    pub fn latency_measurements(&self, peer_id: &str) -> Vec<LatencyMeasurement> {
        let now = now_secs();
        let cutoff = now.saturating_sub(self.window_secs);
        let Ok(map) = self.inner.read() else {
            return Vec::new();
        };
        let Some(obs) = map.get(peer_id) else {
            return Vec::new();
        };
        obs.latency
            .iter()
            .filter(|s| s.ts_secs >= cutoff)
            .map(|s| LatencyMeasurement {
                peer_id: peer_id.to_string(),
                rtt_ms: s.rtt_ms,
                timestamp: s.ts_secs,
                measurement_type: s.kind.clone(),
            })
            .collect()
    }

    /// Drop all observations for a peer (e.g. on disconnect / eviction).
    pub fn forget_peer(&self, peer_id: &str) {
        if let Ok(mut map) = self.inner.write() {
            map.remove(peer_id);
        }
    }

    /// Current number of tracked peers. Primarily useful for metrics/tests.
    pub fn peer_count(&self) -> usize {
        self.inner.read().map(|m| m.len()).unwrap_or(0)
    }

    /// Number of latency samples currently retained for `peer_id`.
    pub fn latency_sample_count(&self, peer_id: &str) -> usize {
        self.inner
            .read()
            .ok()
            .and_then(|m| m.get(peer_id).map(|o| o.latency.len()))
            .unwrap_or(0)
    }

    pub fn window_secs(&self) -> u64 {
        self.window_secs
    }

    /// V0.2 Phase 1 (Score Canonicity, issue #31): build the set of
    /// `PeerLatencyObservation` rows for the current observation window,
    /// ready to be included in a `LatencyReport` and gossipped to peers.
    ///
    /// `exclude_self` is the local node's peer_id; the returned vector
    /// never includes an observation of self.
    pub fn build_canon_observations(&self, exclude_self: &str) -> Vec<PeerLatencyObservation> {
        let now = now_secs();
        let cutoff = now.saturating_sub(self.window_secs);
        let Ok(map) = self.inner.read() else {
            return Vec::new();
        };
        let mut out: Vec<PeerLatencyObservation> = Vec::with_capacity(map.len());
        for (peer_id, obs) in map.iter() {
            if peer_id == exclude_self {
                continue;
            }
            let mut rtts: Vec<u64> = obs
                .latency
                .iter()
                .filter(|s| s.ts_secs >= cutoff)
                .map(|s| s.rtt_ms)
                .collect();
            if rtts.is_empty() {
                continue;
            }
            rtts.sort_unstable();
            let median_rtt = rtts[(rtts.len() - 1) / 2];
            let samples = if rtts.len() > u8::MAX as usize {
                u8::MAX
            } else {
                rtts.len() as u8
            };
            out.push(PeerLatencyObservation {
                peer_id: peer_id.clone(),
                rtt_ms_bucket: bucket_from_rtt_ms(median_rtt),
                samples,
            });
        }
        out.sort_by(|a, b| a.peer_id.cmp(&b.peer_id));
        out
    }
}

impl Default for ObservationStore {
    fn default() -> Self {
        Self::new()
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn count_validations(buf: &VecDeque<ValidationSample>, cutoff: u64) -> (u32, u32) {
    let mut total = 0u32;
    let mut valid = 0u32;
    for s in buf.iter().filter(|s| s.ts_secs >= cutoff) {
        total = total.saturating_add(1);
        if s.valid {
            valid = valid.saturating_add(1);
        }
    }
    (valid, total)
}

fn empty_integrity(peer_id: &str, epoch: u64) -> IntegrityMeasurement {
    IntegrityMeasurement {
        node_id: peer_id.to_string(),
        valid_blocks: 0,
        total_blocks: 0,
        valid_transactions: 0,
        total_transactions: 0,
        slash_events: 0,
        epoch,
    }
}
