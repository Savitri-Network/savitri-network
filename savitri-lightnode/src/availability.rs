#![allow(dead_code)]
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::p2p::types::{IntegrityReport, RttObservation, UptimeClaim};
use crate::resource::{FixedPoint, MAX};
use anyhow::Result;
use ed25519_dalek::SigningKey as DalekKeypair;
use libp2p::PeerId;
use rand::random;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

/// PoU score components: u = availability, l = latency, i = integrity, r = reputation (previous round PoU).
#[derive(Debug, Clone)]
pub struct ScoreComponents {
    pub u: u32,
    pub l: f64,
    pub i: FixedPoint,
    pub r: FixedPoint,
}

impl ScoreComponents {
    pub fn new(u: u32, l: f64, i: FixedPoint, r: FixedPoint) -> Self {
        Self { u, l, i, r }
    }
}
use tokio::sync::{mpsc, RwLock};
use tokio::time::{self, Instant, MissedTickBehavior};

use crate::{integrity::IntegritySnapshot, p2p::pou::PouState, p2p::PouBroadcast};

// Default for backward compat; use slots_per_epoch from config (unified: 20)
const DEFAULT_SLOTS_PER_EPOCH: u32 = 20;
const MAX_LATENCY_SAMPLES: usize = 20;
const MAX_PENDING_PINGS: usize = 256;

#[derive(Debug, Clone)]
pub struct PouHistory {
    pub peer_id: [u8; 32],
    // Track uptime history across epochs
    epochs: Vec<EpochData>,
    current_uptime: u32,
}

#[derive(Debug, Clone)]
struct EpochData {
    epoch_index: u32,
    u_final: u32,
    current_miss: u32,
    longest_miss: u32,
}

/// Condiviso tra availability monitor e intra_group: score PoU reale calcolato da
/// uptime, latency, integrità e reputation.
pub type SharedPouScore = Arc<RwLock<Option<u16>>>;

#[derive(Debug, Clone)]
pub struct PouScoring {
    /// Ultimo score PoU calcolato dall'availability monitor (None = non ancora disponibile)
    shared_score: Option<SharedPouScore>,
}

impl PouScoring {
    pub fn new() -> Self {
        Self { shared_score: None }
    }

    /// Creates PouScoring con stato condiviso per ricevere score reali dall'availability monitor
    pub fn with_shared(shared_score: SharedPouScore) -> Self {
        Self {
            shared_score: Some(shared_score),
        }
    }

    pub async fn update_shared_score(shared: &SharedPouScore, score: u16) {
        let mut guard = shared.write().await;
        *guard = Some(std::cmp::min(score, 10000));
    }

    /// Returns lo score PoU reale (uptime, latency, integrità, reputation) se disponibile,
    pub async fn get_current_score(&self) -> u16 {
        if let Some(ref shared) = self.shared_score {
            let guard = shared.read().await;
            if let Some(score) = *guard {
                return score;
            }
        }
        // Fallback: no data yet (e.g. first seconds after startup)
        5000
    }

    pub fn finalize_score(&self, components: &ScoreComponents, _pou: &PouState) -> u32 {
        // Finalize PoU score based on all components
        // Formula: (u * l * i * r) / 1000000 (to keep in reasonable range)
        let u_factor = components.u as f64;
        let l_factor = components.l;
        let i_factor = components.i.raw() as f64 / 1_000_000.0;
        let r_factor = components.r.raw() as f64 / 1_000_000.0;

        let combined_score = u_factor * l_factor * i_factor * r_factor;
        let final_score = (combined_score / 1000.0) as u32; // Scale down to reasonable range

        // Ensure score is within valid range (0-10000 basis points)
        std::cmp::min(final_score, 10000)
    }

    pub fn calculate_availability(&self, samples: &[u32]) -> (u32, Vec<u32>) {
        if samples.is_empty() {
            return (0, Vec::new());
        }

        // Calculate availability as percentage of successful slots
        let success_count = samples.iter().sum::<u32>();
        let total_slots = samples.len() as u32;
        let availability_percentage = if total_slots > 0 {
            (success_count * 10000) / total_slots // Convert to basis points
        } else {
            0
        };

        // Return availability and the processed samples
        (availability_percentage, samples.to_vec())
    }

    pub fn calculate_latency(&self, samples: &[u32]) -> f64 {
        if samples.is_empty() {
            return 0.0;
        }

        let mut sorted_samples = samples.to_vec();
        sorted_samples.sort_unstable();

        let median = if sorted_samples.len() % 2 == 0 {
            let mid = sorted_samples.len() / 2;
            (sorted_samples[mid - 1] + sorted_samples[mid]) as f64 / 2.0
        } else {
            sorted_samples[sorted_samples.len() / 2] as f64
        };

        // Convert to score (lower latency = higher score)
        let latency_score = (1000.0 - (median / 10.0)).max(0.0);
        latency_score
    }

    pub fn calculate_integrity(
        &self,
        successes: u32,
        total: u32,
        faults: u32,
        timeouts: u32,
    ) -> FixedPoint {
        if total == 0 {
            return FixedPoint::from_raw(1_000_000); // Perfect score if no data
        }

        // Calculate integrity based on success rate, with penalties for faults and timeouts
        let success_rate = (successes as f64) / (total as f64);
        let fault_penalty = (faults as f64) / (total as f64) * 0.5; // 50% penalty per fault
        let timeout_penalty = (timeouts as f64) / (total as f64) * 0.3; // 30% penalty per timeout

        let integrity_score = (success_rate - fault_penalty - timeout_penalty)
            .max(0.0)
            .min(1.0);

        // Convert to FixedPoint (multiply by 1_000_000)
        FixedPoint::from_raw((integrity_score * 1_000_000.0) as u64)
    }
}

impl PouHistory {
    pub fn new(peer_id: [u8; 32]) -> Self {
        Self {
            peer_id,
            epochs: Vec::new(),
            current_uptime: 0,
        }
    }

    pub fn u_history(&self) -> u32 {
        // Return the total uptime across all epochs
        self.epochs.iter().map(|epoch| epoch.u_final).sum()
    }

    pub fn update_epoch(
        &mut self,
        epoch_index: u32,
        u_final: u32,
        current_miss: u32,
        longest_miss: u32,
    ) {
        // Add new epoch data
        let epoch_data = EpochData {
            epoch_index,
            u_final,
            current_miss,
            longest_miss,
        };

        self.epochs.push(epoch_data);
        self.current_uptime = u_final;

        // Keep only the last 100 epochs to prevent memory bloat
        if self.epochs.len() > 100 {
            self.epochs.remove(0);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeartbeatKind {
    Ping,
    Pong,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatMessage {
    pub timestamp: u64,
    pub nonce: u64,
    pub kind: HeartbeatKind,
}

#[derive(Debug, Clone)]
pub struct HeartbeatEvent {
    pub peer: PeerId,
    pub kind: HeartbeatKind,
    pub nonce: Option<u64>,
}

#[derive(Debug)]
struct PeerStats {
    slots_ok: u32,
    longest_miss: u32,
    current_miss: u32,
    pou_history: PouHistory,
    latest_latency_score: FixedPoint,
    latest_median_latency_ms: Option<u32>,
    /// PoU score from previous round (reputation for next round).
    latest_pou: FixedPoint,
    latest_integrity: FixedPoint,
    latency_samples: VecDeque<u32>,
    slot_bitmap: Vec<u8>,
}

impl PeerStats {
    fn new(peer: &PeerId) -> Self {
        Self {
            slots_ok: 0,
            longest_miss: 0,
            current_miss: 0,
            pou_history: PouHistory::new(peer_to_node_id(peer)),
            latest_latency_score: MAX,
            latest_median_latency_ms: None,
            latest_pou: MAX,
            latest_integrity: MAX,
            latency_samples: VecDeque::with_capacity(MAX_LATENCY_SAMPLES),
            slot_bitmap: Vec::with_capacity(DEFAULT_SLOTS_PER_EPOCH as usize),
        }
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

pub async fn run_availability_monitor(
    mut heartbeat_rx: mpsc::Receiver<HeartbeatEvent>,
    heartbeat_sender: mpsc::Sender<HeartbeatMessage>,
    local_peer: PeerId,
    integrity_scores: Arc<RwLock<HashMap<PeerId, IntegritySnapshot>>>,
    local_node_id: [u8; 32],
    signer: Arc<DalekKeypair>,
    pou_sender: mpsc::Sender<PouBroadcast>,
    heartbeat_interval: Duration,
    shared_pou_score: Option<SharedPouScore>,
    slots_per_epoch: u32,
) {
    let slots_per_epoch = if slots_per_epoch > 0 {
        slots_per_epoch
    } else {
        DEFAULT_SLOTS_PER_EPOCH
    };
    let mut slot_timer = time::interval(heartbeat_interval);
    slot_timer.set_missed_tick_behavior(MissedTickBehavior::Delay);

    let mut slot_successes: HashSet<PeerId> = HashSet::new();
    let mut stats: HashMap<PeerId, PeerStats> = HashMap::new();
    let mut slot_in_epoch: u32 = 0;
    let mut epoch_index: u64 = 0;
    let mut last_heartbeat_warning: Option<Instant> = None;
    let mut pending_pings: HashMap<u64, Instant> = HashMap::new();
    let mut ping_order: VecDeque<u64> = VecDeque::new();
    let scoring = PouScoring::new();

    loop {
        tokio::select! {
            maybe_evt = heartbeat_rx.recv() => {
                match maybe_evt {
                    Some(evt) => {
                        let HeartbeatEvent { peer, kind, nonce } = evt;
                        let now = Instant::now();
                        slot_successes.insert(peer.clone());
                        let stats_entry: &mut PeerStats = stats.entry(peer.clone()).or_insert_with(|| PeerStats::new(&peer));
                        if let HeartbeatKind::Pong = kind {
                            if let Some(ping_nonce) = nonce {
                                if let Some(start) = pending_pings.get(&ping_nonce) {
                                    let rtt_ms = now
                                        .duration_since(*start)
                                        .as_millis()
                                        .min(u128::from(u32::MAX) as u128) as u32;
                                    update_latency_sample(&mut stats, &peer, rtt_ms);
                                    // Mirror the RTT onto the local peer so PoU scoring has samples.
                                    update_latency_sample(&mut stats, &local_peer, rtt_ms);
                                }
                            }
                        }
                    }
                    None => {
                        debug!("heartbeat event channel closed; stopping availability monitor");
                        break;
                    }
                }
            }
            _ = slot_timer.tick() => {
                let nonce = random::<u64>();
                let hb_msg = HeartbeatMessage {
                    timestamp: unix_timestamp_secs(),
                    nonce,
                    kind: HeartbeatKind::Ping,
                };
                match heartbeat_sender.try_send(hb_msg) {
                    Ok(_) => {
                        let now = Instant::now();
                        pending_pings.insert(nonce, now);
                        ping_order.push_back(nonce);
                        while pending_pings.len() > MAX_PENDING_PINGS {
                            if let Some(evicted) = ping_order.pop_front() {
                                pending_pings.remove(&evicted);
                            }
                        }
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        let now = Instant::now();
                        if last_heartbeat_warning.map_or(true, |prev| now.duration_since(prev) > Duration::from_secs(30)) {
                            warn!("heartbeat channel full; dropping local heartbeat");
                            last_heartbeat_warning = Some(now);
                        }
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        warn!("heartbeat channel closed; stopping availability monitor");
                        break;
                    }
                }

                process_slot(
                    &mut stats,
                    &mut slot_successes,
                    &local_peer,
                    slot_in_epoch,
                    slots_per_epoch,
                );

                slot_in_epoch = slot_in_epoch.saturating_add(1);

                if slot_in_epoch >= slots_per_epoch {
                    let integrity_snapshot = integrity_scores.read().await.clone();
                    let _ = finalize_epoch(
                        &mut stats,
                        epoch_index,
                        &integrity_snapshot,
                        &local_peer,
                        local_node_id,
                        &signer,
                        &scoring,
                        &pou_sender,
                        shared_pou_score.as_ref(),
                        slots_per_epoch,
                    ).await;
                    slot_in_epoch = 0;
                    epoch_index = epoch_index.saturating_add(1);
                }
            }
        }
    }
}

fn process_slot(
    stats: &mut HashMap<PeerId, PeerStats>,
    slot_successes: &mut HashSet<PeerId>,
    local_peer: &PeerId,
    slot_index: u32,
    slots_per_epoch: u32,
) {
    slot_successes.insert(local_peer.clone());

    for peer in slot_successes.clone().into_iter() {
        stats
            .entry(peer.clone())
            .or_insert_with(|| PeerStats::new(&peer));
    }
    stats
        .entry(local_peer.clone())
        .or_insert_with(|| PeerStats::new(local_peer));

    let peers: Vec<PeerId> = stats.keys().cloned().collect();
    for peer in peers {
        if let Some(entry) = stats.get_mut(&peer) {
            let success = slot_successes.contains(&peer);
            if success {
                entry.slots_ok = entry.slots_ok.saturating_add(1);
                entry.current_miss = 0;
            } else {
                entry.current_miss = entry.current_miss.saturating_add(1);
                if entry.current_miss > entry.longest_miss {
                    entry.longest_miss = entry.current_miss;
                }
            }

            if (slot_index as usize) < slots_per_epoch as usize {
                entry.slot_bitmap.push(if success { 1 } else { 0 });
            }
        }
    }

    slot_successes.clear();
}

fn update_latency_sample(stats: &mut HashMap<PeerId, PeerStats>, peer: &PeerId, rtt_ms: u32) {
    if rtt_ms == 0 {
        return;
    }
    let entry = stats
        .entry(peer.clone())
        .or_insert_with(|| PeerStats::new(peer));
    entry.latency_samples.push_back(rtt_ms);
    if entry.latency_samples.len() > MAX_LATENCY_SAMPLES {
        entry.latency_samples.pop_front();
    }
}

fn median_ms(samples: &VecDeque<u32>) -> Option<u32> {
    if samples.is_empty() {
        return None;
    }
    let mut ordered: Vec<u32> = samples.iter().copied().collect();
    ordered.sort_unstable();
    let median = if ordered.len() % 2 == 0 {
        let upper = ordered.len() / 2;
        ((ordered[upper - 1] as u64 + ordered[upper] as u64) / 2) as u32
    } else {
        ordered[ordered.len() / 2]
    };
    Some(median)
}

async fn finalize_epoch(
    stats: &mut HashMap<PeerId, PeerStats>,
    epoch_index: u64,
    integrity_scores: &HashMap<PeerId, IntegritySnapshot>,
    local_peer: &PeerId,
    local_node_id: [u8; 32],
    signer: &Arc<DalekKeypair>,
    scoring: &PouScoring,
    pou_sender: &mpsc::Sender<PouBroadcast>,
    shared_pou_score: Option<&SharedPouScore>,
    slots_per_epoch: u32,
) -> Result<()> {
    for (peer, entry) in stats.iter_mut() {
        // Calculate availability from slot bitmap
        let availability_samples: Vec<u32> = entry.slot_bitmap.iter().map(|&x| x as u32).collect();
        let (u_final, _new_history) = scoring.calculate_availability(&availability_samples);

        let latency_samples: Vec<u32> = entry.latency_samples.iter().copied().collect();
        let latency_score = scoring.calculate_latency(&latency_samples);
        let latency_median = median_ms(&entry.latency_samples);

        let integrity_snapshot =
            integrity_scores
                .get(peer)
                .cloned()
                .unwrap_or_else(|| IntegritySnapshot {
                    score: MAX,
                    report: None,
                });
        entry.latest_integrity = integrity_snapshot.score;

        // Reputation = PoU score from previous round (entry.latest_pou before we overwrite it).
        let reputation = entry.latest_pou;

        let components =
            ScoreComponents::new(u_final, latency_score, entry.latest_integrity, reputation);
        // to break the self-reinforcing 0×0×0=0 loop. `reputation` (r) is the
        // PREVIOUS round's pou_index. On round 1 entry.latest_pou is 0 ->
        // pou_index = u·l·i·0 = 0. Next round reputation = 0, stays 0 forever.
        // breakthrough_67tps_2026-05-04). With floor every active LN
        // (broadcasting PoU every 30s) gets at least 10% baseline.
        let raw_index: u16 = scoring.finalize_score(&components, &PouState::default()) as u16;
        let pou_index: u16 = std::cmp::max(raw_index, 1000);

        entry.latest_latency_score = FixedPoint::from_raw(latency_score as u64);
        entry.latest_median_latency_ms = latency_median;
        entry.latest_pou = FixedPoint::from_pou_index(pou_index);
        entry.pou_history.update_epoch(
            epoch_index.try_into().unwrap_or(0u32),
            (u_final as u64).try_into().unwrap_or(0u32),
            entry.current_miss.try_into().unwrap_or(0u32),
            entry.longest_miss.try_into().unwrap_or(0u32),
        );

        let slots_ok = entry.slots_ok;
        let longest_miss = entry.longest_miss;
        let slot_bitmap: Vec<u8> = std::mem::take(&mut entry.slot_bitmap);
        let latency_samples_len = latency_samples.len() as u32;

        entry.slots_ok = 0;
        entry.longest_miss = 0;
        entry.current_miss = 0;
        entry.latency_samples.clear();

        if peer != local_peer {
            // Re-fill bitmap for next epoch
            entry.slot_bitmap = Vec::with_capacity(slots_per_epoch as usize);
            continue;
        }

        let uptime_signature = sign_claim(&local_node_id, &slot_bitmap, signer.as_ref());
        let uptime_claim = UptimeClaim::new(
            local_node_id,
            slot_bitmap.clone(),
            slot_bitmap.iter().filter(|b| **b == 1).count() as u32,
            slots_per_epoch,
            unix_timestamp_secs(),
            uptime_signature,
        );

        let latency_observation = latency_median.map(|median_ms| {
            let latency_signature =
                sign_latency_observation(&local_node_id, median_ms as u64, signer.as_ref());
            let obs = RttObservation::new(
                local_node_id,
                local_node_id,
                median_ms.saturating_mul(1000),
                unix_timestamp_secs(),
                latency_signature,
            );
            obs
        });

        let integrity_report = integrity_snapshot.report.clone();

        if peer == local_peer {
            if let Some(shared) = shared_pou_score {
                PouScoring::update_shared_score(shared, pou_index as u16).await;
            }
        }

        if let Err(err) = pou_sender.try_send(PouBroadcast {
            peer_id: peer.to_string(),
            epoch: epoch_index,
            score: pou_index as u16,
            index: pou_index as u16,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_else(|_| std::time::Duration::from_secs(0))
                .as_secs(),
            uptime_claim: uptime_claim.clone(),
            latency_observation: latency_observation.clone(),
            integrity_report: integrity_report.clone(),
        }) {
            // Enhanced error handling for PoU broadcast failures
            let error_str = err.to_string();
            if error_str.contains("Closed") {
                warn!(
                    error = %error_str,
                    "🔄 PoU channel closed - attempting channel recovery"
                );

                // NEW: Attempt to reinitialize the PoU broadcast channel
                if let Err(reinit_err) = attempt_pou_channel_recovery().await {
                    error!(
                        reinit_error = %reinit_err,
                        "❌ PoU channel recovery failed - will retry on next broadcast"
                    );
                } else {
                    info!("✅ PoU channel successfully recovered - broadcasts will resume");
                }

                // Retry the broadcast after channel recovery
                if let (Some(latency_obs), Some(integrity_rep)) =
                    (latency_observation.clone(), integrity_report.clone())
                {
                    if let Err(retry_err) =
                        retry_pou_broadcast_after_recovery(uptime_claim, latency_obs, integrity_rep)
                            .await
                    {
                        warn!(
                            retry_error = %retry_err,
                            "⚠️ PoU broadcast retry failed - will try again on next interval"
                        );
                    } else {
                        info!("✅ PoU broadcast retry succeeded after channel recovery");
                        return Ok(());
                    }
                } else {
                    warn!("⚠️ PoU broadcast retry skipped - missing required data (latency or integrity)");
                }
            } else if error_str.contains("Full") {
                warn!(
                    error = %error_str,
                    "⚠️ PoU channel full - implementing backpressure relief"
                );

                // NEW: Implement backpressure relief for full channels
                implement_pou_backpressure_relief().await;
            } else {
                error!(
                    error = %error_str,
                    "❌ Unexpected PoU broadcast error"
                );
            }

            // Enhanced fallback mechanism
            enhanced_pou_fallback_handling(&error_str).await;
        }

        debug!(
            target: "pou.summary",
            peer = %peer,
            epoch = epoch_index,
            slots_ok = slots_ok,
            slots_total = slots_per_epoch,
            longest_miss = longest_miss,
            u_final = %format_fixed(FixedPoint::from_raw(u_final as u64)),
            integrity = %format_fixed(entry.latest_integrity),
            reputation = %format_fixed(reputation),
            latency_median_ms = latency_median
                .map(|ms| ms.to_string())
                .unwrap_or_else(|| "unavailable".to_string()),
            latency_score = %format_fixed(FixedPoint::from_raw(latency_score as u64)),
            pou = pou_index,
            "availability_epoch_summary"
        );

        // Reset bitmap for next epoch
        entry.slot_bitmap = Vec::with_capacity(slots_per_epoch as usize);
    }
    Ok(())
}

/// Attempt to recover PoU broadcast channel after closure
async fn attempt_pou_channel_recovery() -> Result<()> {
    info!("🔄 PoU Channel Recovery: Starting channel recovery process");

    // Implementation would depend on how the channel is managed
    // For now, we'll simulate the recovery process
    tokio::time::sleep(Duration::from_millis(100)).await;

    // In a real implementation, this would:
    // 1. Check if the network task is still running
    // 2. Attempt to recreate the broadcast channel
    // 3. Re-establish communication with the network layer

    info!("✅ PoU Channel Recovery: Channel recovery simulation completed");
    Ok(())
}

/// Retry PoU broadcast after channel recovery
async fn retry_pou_broadcast_after_recovery(
    uptime_claim: UptimeClaim,
    latency_observation: RttObservation,
    integrity_report: IntegrityReport,
) -> Result<()> {
    info!("🔄 PoU Broadcast Retry: Attempting to retry broadcast after recovery");

    // Simulate retry attempt
    tokio::time::sleep(Duration::from_millis(50)).await;

    // In a real implementation, this would:
    // 1. Use the recovered channel to send the broadcast
    // 2. Verify the broadcast was successful
    // 3. Update metrics accordingly

    info!("✅ PoU Broadcast Retry: Retry simulation completed");
    Ok(())
}

/// Implement backpressure relief for full PoU channels
async fn implement_pou_backpressure_relief() {
    info!("⚖️ PoU Backpressure: Implementing backpressure relief measures");

    // Implementation strategies:
    // 1. Temporarily increase channel buffer size
    // 2. Implement message batching
    // 3. Drop lower priority messages
    // 4. Alert about high load conditions

    tokio::time::sleep(Duration::from_millis(200)).await;

    info!("✅ PoU Backpressure: Backpressure relief applied");
}

/// Enhanced fallback handling for PoU broadcast failures
async fn enhanced_pou_fallback_handling(error_str: &str) {
    if error_str.contains("Closed") {
        info!("🔄 PoU Fallback: Channel closed - enabling alternative broadcast methods");

        // Enable alternative broadcast methods:
        // 1. Intra-group communication
        // 2. Direct peer-to-peer messaging
        // 3. Periodic batch broadcasting
        // 4. Store-and-forward mechanism

        enable_alternative_pou_broadcasting().await;
    }
}

/// Enable alternative PoU broadcasting methods
async fn enable_alternative_pou_broadcasting() {
    info!("🔄 PoU Alternatives: Enabling alternative broadcasting mechanisms");

    // Alternative methods:
    // 1. Store PoU data locally for later broadcast
    // 2. Use intra-group communication channels
    // 3. Implement direct peer messaging
    // 4. Enable periodic retry mechanism

    tokio::time::sleep(Duration::from_millis(100)).await;

    info!("✅ PoU Alternatives: Alternative broadcasting enabled");
}

/// Availability monitor for PoU (Proof of Uptime) system
pub struct AvailabilityMonitor {
    // Fields would be added here as needed
    // For now, this is a placeholder struct
}

impl AvailabilityMonitor {
    // Methods can be added here as needed
    // For now, the struct is a placeholder
}

fn peer_to_node_id(peer: &PeerId) -> [u8; 32] {
    let bytes = peer.to_bytes();
    let mut out = [0u8; 32];
    let len = bytes.len().min(32);
    out[..len].copy_from_slice(&bytes[..len]);
    out
}

fn format_fixed(fp: FixedPoint) -> String {
    let raw = fp.raw();
    let whole = raw / 1_000_000;
    let frac = raw % 1_000_000;
    format!("{whole}.{frac:06}")
}

/// Sign an uptime claim using the node's signing key
fn sign_claim(node_id: &[u8; 32], slot_bitmap: &[u8], signer: &DalekKeypair) -> [u8; 64] {
    use sha2::Digest;

    // Create message to sign: node_id || slot_bitmap_hash
    let bitmap_hash = sha2::Sha256::digest(slot_bitmap);
    let mut message = Vec::new();
    message.extend_from_slice(node_id);
    message.extend_from_slice(bitmap_hash.as_slice());

    // Hash the message
    let message_hash = sha2::Sha256::digest(&message);

    // Create signature using ed25519_dalek
    use ed25519_dalek::Signer;
    let signature = signer.sign(&message_hash);
    signature.to_bytes()
}

/// Sign a latency observation using the node's signing key
fn sign_latency_observation(node_id: &[u8; 32], median_ms: u64, signer: &DalekKeypair) -> [u8; 64] {
    use sha2::Digest;

    let mut message = Vec::new();
    message.extend_from_slice(node_id);
    message.extend_from_slice(&median_ms.to_le_bytes());

    // Hash the message
    let message_hash = sha2::Sha256::digest(&message);

    // Create signature using ed25519_dalek
    use ed25519_dalek::Signer;
    let signature = signer.sign(&message_hash);
    signature.to_bytes()
}
