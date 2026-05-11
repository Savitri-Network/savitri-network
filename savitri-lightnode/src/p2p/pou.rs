#![allow(dead_code)]

use libp2p::PeerId;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// PoU score fixed-point representation.
///
/// Consensus/stateful code MUST NOT use floats. This score is an integer in the range 0..=1000.
pub type PouScore = u16;

/// Max PoU score value (inclusive).
pub const POU_SCORE_MAX: PouScore = 1000;

// A peer's score is only accepted into `peer_scores` when at least MIN_ATTESTATIONS different
// reporters agree (within ATTESTATION_TOLERANCE). A peer's score cannot jump by more than
// MAX_SCORE_DELTA_PER_EPOCH between consecutive epochs. The attestations map is capped at
// MAX_ATTESTATIONS_PER_PEER entries per subject to prevent memory exhaustion.
const MIN_ATTESTATIONS: usize = 2;
const MAX_SCORE_DELTA_PER_EPOCH: u16 = 200;
const ATTESTATION_TOLERANCE: u16 = 50;
const MAX_ATTESTATIONS_PER_PEER: usize = 1024;

/// Format a PoU score (0..=1000) as a normalized fixed-point string with 4 decimals (0.0000..1.0000),
/// without using floats.
pub fn format_pou_score_4dp(score: PouScore) -> String {
    let clamped = score.min(POU_SCORE_MAX) as u32;
    // Convert 0..=1000 (1/1000 steps) to 0..=10000 (1/10000 steps) to print 4 decimals.
    let scaled = clamped.saturating_mul(10);
    let whole = scaled / 10_000;
    let frac = scaled % 10_000;
    format!("{whole}.{frac:04}")
}

#[derive(Clone, Debug)]
pub struct PouState {
    inner: Arc<RwLock<PouSnapshot>>,
    local_peer: PeerId,
}

impl Default for PouState {
    fn default() -> Self {
        Self::new(PeerId::random())
    }
}

impl PouState {
    pub fn new(local_peer: PeerId) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PouSnapshot::new())),
            local_peer,
        }
    }

    /// Original `record_report` — backward compatible.
    ///
    /// SECURITY [AUDIT-001]: This now behaves as a **self-report** (the peer reports its own
    /// score). It counts as only 1 of the MIN_ATTESTATIONS required, so a single self-report
    /// alone is NOT sufficient to set the accepted score. Use `record_attested_report` for
    /// cross-verified reports from other peers.
    pub async fn record_report(&self, peer: &PeerId, epoch: u64, score: PouScore) {
        // Treat as a self-report: the peer itself is the reporter.
        self.record_attested_report(peer, peer, epoch, score).await;
    }

    /// Record a self-report explicitly. Alias kept for clarity.
    ///
    /// SECURITY [AUDIT-001]: A self-report counts as 1 attestation and is NOT enough on its own.
    pub async fn record_self_report(&self, peer: &PeerId, epoch: u64, score: PouScore) {
        self.record_attested_report(peer, peer, epoch, score).await;
    }

    /// Record a cross-verified PoU score attestation.
    ///
    /// SECURITY [AUDIT-001]: `reporter` is the peer who observed the score. `subject` is the
    /// peer whose score is being reported. A score is only promoted to `peer_scores` when at
    /// least `MIN_ATTESTATIONS` independent reporters agree (within ±ATTESTATION_TOLERANCE).
    /// Additionally, the accepted score is rate-limited: it cannot change by more than
    /// `MAX_SCORE_DELTA_PER_EPOCH` from the previous epoch's value.
    pub async fn record_attested_report(
        &self,
        reporter: &PeerId,
        subject: &PeerId,
        epoch: u64,
        score: PouScore,
    ) {
        let mut guard = self.inner.write().await;
        match guard.epoch {
            Some(current) if epoch < current => return,
            Some(current) if epoch > current => guard.reset_for_epoch(epoch),
            None => guard.reset_for_epoch(epoch),
            _ => {}
        }

        let score = score.min(POU_SCORE_MAX);

        // Cap per-subject attestation entries to prevent memory exhaustion.
        let subject_attestations = guard
            .attestations
            .entry(subject.clone())
            .or_insert_with(HashMap::new);
        if subject_attestations.len() >= MAX_ATTESTATIONS_PER_PEER
            && !subject_attestations.contains_key(reporter)
        {
            // Too many attesters for this subject — ignore new reporters to prevent DoS.
            return;
        }
        subject_attestations.insert(reporter.clone(), score);

        let attested_score = Self::compute_attested_score_from_map(subject_attestations);

        if let Some(accepted_score) = attested_score {
            // MAX_SCORE_DELTA_PER_EPOCH from the previous epoch's value.
            let rate_limited_score = if let Some(history) = guard.score_change_history.get(subject)
            {
                if let Some(&prev_score) = history.last() {
                    let delta = if accepted_score > prev_score {
                        accepted_score - prev_score
                    } else {
                        prev_score - accepted_score
                    };
                    if delta > MAX_SCORE_DELTA_PER_EPOCH {
                        // Clamp to the maximum allowed change.
                        if accepted_score > prev_score {
                            prev_score
                                .saturating_add(MAX_SCORE_DELTA_PER_EPOCH)
                                .min(POU_SCORE_MAX)
                        } else {
                            prev_score.saturating_sub(MAX_SCORE_DELTA_PER_EPOCH)
                        }
                    } else {
                        accepted_score
                    }
                } else {
                    accepted_score
                }
            } else {
                accepted_score
            };

            // Update peer_scores with the accepted, rate-limited score.
            guard
                .peer_scores
                .insert(subject.clone(), rate_limited_score);

            // Track score history for rate limiting across epochs.
            guard
                .score_change_history
                .entry(subject.clone())
                .or_insert_with(Vec::new)
                .push(rate_limited_score);

            // Update local score tracking.
            if subject == &self.local_peer {
                guard.local_score = Some(rate_limited_score);
            } else {
                guard.saw_nonlocal_report = true;
                guard.best_remote_score = match guard.best_remote_score {
                    Some(existing) if existing >= rate_limited_score => Some(existing),
                    _ => Some(rate_limited_score),
                };
            }

            // Update leader election.
            if guard.leader.as_ref() == Some(subject) {
                guard.leader_score = Some(rate_limited_score);
                return;
            }

            let replace_leader = match guard.leader_score {
                None => true,
                Some(best) if rate_limited_score > best => true,
                Some(best) if rate_limited_score == best => guard
                    .leader
                    .as_ref()
                    .map(|current| {
                        // Deterministic tie-break: lexicographic compare on PeerId bytes.
                        subject.to_bytes().cmp(&current.to_bytes()) == Ordering::Less
                    })
                    .unwrap_or(true),
                _ => false,
            };

            if replace_leader {
                guard.leader = Some(subject.clone());
                guard.leader_score = Some(rate_limited_score);
            }
        }
        // If not enough attestations yet, score is NOT promoted to peer_scores.
    }

    /// Compute the attested (median) score from a set of attestations for a subject.
    ///
    /// SECURITY [AUDIT-001]: Returns `Some(median)` only when at least `MIN_ATTESTATIONS`
    /// reporters agree within ±ATTESTATION_TOLERANCE. Returns `None` otherwise.
    fn compute_attested_score_from_map(
        attestations: &HashMap<PeerId, PouScore>,
    ) -> Option<PouScore> {
        if attestations.len() < MIN_ATTESTATIONS {
            return None;
        }

        let mut scores: Vec<PouScore> = attestations.values().copied().collect();
        scores.sort_unstable();

        // Find the largest cluster of scores within ATTESTATION_TOLERANCE of each other.
        let mut best_cluster_start = 0;
        let mut best_cluster_len = 0;
        let mut i = 0;
        while i < scores.len() {
            let mut j = i;
            while j < scores.len() && scores[j].saturating_sub(scores[i]) <= ATTESTATION_TOLERANCE {
                j += 1;
            }
            let cluster_len = j - i;
            if cluster_len > best_cluster_len {
                best_cluster_len = cluster_len;
                best_cluster_start = i;
            }
            i += 1;
        }

        if best_cluster_len >= MIN_ATTESTATIONS {
            let cluster = &scores[best_cluster_start..best_cluster_start + best_cluster_len];
            let median = cluster[cluster.len() / 2];
            Some(median)
        } else {
            None
        }
    }

    /// Public helper: compute the attested score for a given peer from current attestations.
    ///
    /// agree within ±ATTESTATION_TOLERANCE, or `None` if fewer than MIN_ATTESTATIONS agree.
    pub async fn compute_attested_score(&self, peer: &PeerId) -> Option<PouScore> {
        let guard = self.inner.read().await;
        guard
            .attestations
            .get(peer)
            .and_then(|atts| Self::compute_attested_score_from_map(atts))
    }

    pub async fn get_all_peer_scores(&self) -> HashMap<PeerId, PouScore> {
        let guard = self.inner.read().await;
        guard.peer_scores.clone()
    }

    /// Get PoU score for a specific account
    pub async fn get_score(&self, account: &[u8; 32]) -> Option<PouScore> {
        let guard = self.inner.read().await;

        // Try to find peer with matching account address
        // For now, we'll search through peer scores to find a match
        // In a full implementation, we'd maintain a mapping from accounts to peers
        for (peer_id, score) in &guard.peer_scores {
            // Convert PeerId to bytes and compare with account
            let peer_bytes = peer_id.to_bytes();

            // Check if the account matches the peer ID (common mapping)
            if &account[..] == &peer_bytes[..] {
                return Some(*score);
            }
        }

        // If no matching peer found, return local score if account matches local peer
        let local_peer_bytes = self.local_peer.to_bytes();
        if &account[..] == &local_peer_bytes[..] {
            guard.local_score
        } else {
            // Account not found in our scoring system
            None
        }
    }

    pub async fn snapshot(&self) -> PouView {
        let guard = self.inner.read().await;
        PouView {
            epoch: guard.epoch,
            leader: guard.leader.clone(),
            leader_score: guard.leader_score,
            local_score: guard.local_score,
            best_remote_score: guard.best_remote_score,
            local_is_leader: guard
                .leader
                .as_ref()
                .map(|peer| peer == &self.local_peer)
                .unwrap_or(false),
            election_ready: guard.saw_nonlocal_report,
        }
    }

    pub async fn local_can_produce(&self) -> bool {
        let guard = self.inner.read().await;
        guard.epoch.is_some()
            && guard.local_score.is_some()
            && guard
                .leader
                .as_ref()
                .map(|peer| peer == &self.local_peer)
                .unwrap_or(false)
    }
}

#[derive(Clone)]
pub struct PouView {
    pub epoch: Option<u64>,
    pub leader: Option<PeerId>,
    pub leader_score: Option<PouScore>,
    pub local_score: Option<PouScore>,
    pub best_remote_score: Option<PouScore>,
    pub local_is_leader: bool,
    pub election_ready: bool,
}

#[derive(Debug)]
struct PouSnapshot {
    epoch: Option<u64>,
    leader: Option<PeerId>,
    leader_score: Option<PouScore>,
    local_score: Option<PouScore>,
    saw_nonlocal_report: bool,
    best_remote_score: Option<PouScore>,
    peer_scores: HashMap<PeerId, PouScore>, // Traccia tutti i peer scores per distribuzione P2P
    // Maps subject_peer -> (reporter_peer -> reported_score).
    attestations: HashMap<PeerId, HashMap<PeerId, PouScore>>,
    // Maps peer -> list of accepted scores (most recent last).
    score_change_history: HashMap<PeerId, Vec<PouScore>>,
}

impl PouSnapshot {
    fn new() -> Self {
        Self {
            epoch: None,
            leader: None,
            leader_score: None,
            local_score: None,
            saw_nonlocal_report: false,
            best_remote_score: None,
            peer_scores: HashMap::new(),
            attestations: HashMap::new(),
            score_change_history: HashMap::new(),
        }
    }

    fn reset_for_epoch(&mut self, epoch: u64) {
        // limiting continuity. Only clear per-epoch transient data.
        self.epoch = Some(epoch);
        self.leader = None;
        self.leader_score = None;
        self.local_score = None;
        self.saw_nonlocal_report = false;
        self.best_remote_score = None;
        self.peer_scores.clear(); // Reset peer scores per nuovo epoch
        self.attestations.clear(); // Reset attestations per nuovo epoch
                                   // Note: score_change_history is intentionally NOT cleared so that rate limiting
                                   // works across epoch boundaries.
    }
}
