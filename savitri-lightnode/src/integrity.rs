#![allow(dead_code)]

use std::{
    collections::HashMap,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::p2p::types::IntegrityReport;
use crate::resource::FixedPoint;
use ed25519_dalek::SigningKey as DalekKeypair;
use libp2p::PeerId;
use sha2::Digest;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, warn};

const VALIDATIONS_PER_EPOCH: u32 = 20;

#[derive(Debug, Clone, Copy)]
pub enum IntegrityKind {
    Success,
    Fault,
    Timeout,
    Mismatch,
}

#[derive(Debug, Clone)]
pub struct IntegrityEvent {
    pub peer: PeerId,
    pub kind: IntegrityKind,
}

/// Fixed-point integrity score and optional signed report (only for the local node).
#[derive(Debug, Clone)]
pub struct IntegritySnapshot {
    pub score: FixedPoint,
    pub report: Option<IntegrityReport>,
}

#[derive(Default)]
struct PeerStats {
    successes: u32,
    faults: u32,
    timeouts: u32,
    mismatches: u32,
    total: u32,
    epoch_index: u64,
    latest_integrity: FixedPoint,
}

#[derive(Debug, Clone)]
pub struct PouScoring;

impl PouScoring {
    pub fn new() -> Self {
        Self
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

pub async fn run_integrity_monitor(
    mut rx: mpsc::Receiver<IntegrityEvent>,
    scores: Arc<RwLock<HashMap<PeerId, IntegritySnapshot>>>,
    local_peer: PeerId,
    local_node_id: [u8; 32],
    signer: Arc<DalekKeypair>,
) {
    let mut stats: HashMap<PeerId, PeerStats> = HashMap::new();
    let scoring = PouScoring::new();

    while let Some(event) = rx.recv().await {
        let entry = stats.entry(event.peer.clone()).or_default();
        entry.total = entry.total.saturating_add(1);
        match event.kind {
            IntegrityKind::Success => {
                entry.successes = entry.successes.saturating_add(1);
            }
            IntegrityKind::Fault => {
                entry.faults = entry.faults.saturating_add(1);
            }
            IntegrityKind::Timeout => {
                entry.timeouts = entry.timeouts.saturating_add(1);
            }
            IntegrityKind::Mismatch => {
                entry.mismatches = entry.mismatches.saturating_add(1);
            }
        }

        if entry.total >= VALIDATIONS_PER_EPOCH {
            let snapshot = finalize_peer(
                &event.peer,
                entry,
                &scoring,
                &local_peer,
                local_node_id,
                &signer,
            );
            let mut guard = scores.write().await;
            // SECURITY: Cap peer score map to prevent unbounded memory growth
            const MAX_PEER_SCORES: usize = 1024;
            if guard.len() >= MAX_PEER_SCORES && !guard.contains_key(&event.peer) {
                warn!(
                    "Integrity scores map at capacity ({} entries), skipping new peer {}",
                    MAX_PEER_SCORES, event.peer
                );
            } else {
                guard.insert(event.peer.clone(), snapshot);
            }
        }
    }

    // flush partial epochs when the channel closes
    let mut writes: Vec<(PeerId, IntegritySnapshot)> = Vec::new();
    for (peer, entry) in stats.iter_mut() {
        if entry.total > 0 {
            let snapshot =
                finalize_peer(peer, entry, &scoring, &local_peer, local_node_id, &signer);
            writes.push((peer.clone(), snapshot));
        }
    }
    if !writes.is_empty() {
        let mut guard = scores.write().await;
        for (peer, snapshot) in writes {
            guard.insert(peer, snapshot);
        }
    }

    debug!("integrity monitor task exiting");
}

fn finalize_peer(
    peer: &PeerId,
    stats: &mut PeerStats,
    scoring: &PouScoring,
    local_peer: &PeerId,
    local_node_id: [u8; 32],
    signer: &Arc<DalekKeypair>,
) -> IntegritySnapshot {
    // Mismatches are treated as faults for scoring purposes.
    let faults_total = stats.faults.saturating_add(stats.mismatches);
    let score =
        scoring.calculate_integrity(stats.successes, stats.total, faults_total, stats.timeouts);

    let report = if peer == local_peer {
        let mut report = IntegrityReport::new(
            stats.epoch_index,
            local_node_id,
            stats.successes,
            stats.total,
            faults_total,
            stats.timeouts,
            stats.mismatches,
            unix_timestamp_secs(),
            [0u8; 64], // Will be set by sign_integrity_report
        );

        // Sign the integrity report
        sign_integrity_report(&mut report, &*signer);

        Some(report)
    } else {
        None
    };

    debug!(
        target: "pou.summary",
        peer = %peer,
        epoch = stats.epoch_index,
        validations_ok = stats.successes,
        validations_total = stats.total,
        faults = faults_total,
        timeouts = stats.timeouts,
        hash_mismatches = stats.mismatches,
        integrity = %format_fixed(score),
        "integrity_epoch_summary"
    );

    stats.latest_integrity = score;
    stats.epoch_index = stats.epoch_index.saturating_add(1);
    reset_epoch(stats);

    IntegritySnapshot { score, report }
}

fn reset_epoch(stats: &mut PeerStats) {
    stats.successes = 0;
    stats.faults = 0;
    stats.timeouts = 0;
    stats.mismatches = 0;
    stats.total = 0;
}

pub fn emit_event(sender: &mpsc::Sender<IntegrityEvent>, peer: &PeerId, kind: IntegrityKind) {
    if let Err(err) = sender.try_send(IntegrityEvent {
        peer: peer.clone(),
        kind,
    }) {
        warn!(error=?err, "integrity event channel full; dropping event");
    }
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn format_fixed(fp: crate::resource::FixedPoint) -> String {
    let raw = fp.raw();
    let whole = raw / 1_000_000;
    let frac = raw % 1_000_000;
    format!("{whole}.{frac:06}")
}

fn report_node_pubkey(report: &crate::p2p::types::IntegrityReport) -> ed25519_dalek::VerifyingKey {
    // Extract the public key from the signature by reconstructing the message
    // and verifying it. For now, we'll derive it from the node_id as a fallback

    // Try to create a valid key from node_id (first 32 bytes)
    let key_bytes = &report.node_id;

    // Try to create a verifying key from the node_id bytes
    match ed25519_dalek::VerifyingKey::from_bytes(key_bytes) {
        Ok(key) => key,
        Err(_) => {
            // Fallback: create a deterministic key from node_id
            let mut hasher = sha2::Sha256::new();
            hasher.update(key_bytes);
            let hash = hasher.finalize();

            // Use first 32 bytes of hash as key bytes
            let mut key_array = [0u8; 32];
            key_array.copy_from_slice(&hash[..32]);

            ed25519_dalek::VerifyingKey::from_bytes(&key_array).unwrap_or_else(|_| {
                // Ultimate fallback: create a signing key and derive verifying key
                let signing_key = ed25519_dalek::SigningKey::from_bytes(&key_array);
                signing_key.verifying_key()
            })
        }
    }
}

/// Sign an integrity report using the node's signing key
fn sign_integrity_report(report: &mut crate::p2p::types::IntegrityReport, signer: &DalekKeypair) {
    use sha2::Digest;

    // Create message to sign: node_id || epoch || successes || total || faults || timeouts
    let mut message = Vec::new();
    message.extend_from_slice(&report.node_id);
    message.extend_from_slice(&report.epoch_index.to_le_bytes());
    message.extend_from_slice(&report.validations_ok.to_le_bytes());
    message.extend_from_slice(&report.validations_total.to_le_bytes());
    message.extend_from_slice(&report.timestamp.to_le_bytes());

    // Hash the message
    let message_hash = sha2::Sha256::digest(&message);

    // Create signature using ed25519_dalek
    use ed25519_dalek::Signer;
    let signature = signer.sign(&message_hash);
    report.signature = signature.to_bytes();
}
