//! Latency Canon — canonical, observer-independent peer RTT reporting.
//!
//! Part of Savitri V0.2 Phase 1 (Score Canonicity). See
//! `docs/CONSENSUS_V0.2_DESIGN.md` §3 for the full specification.
//!
//! ## Motivation
//!
//! Pre-V0.2, each lightnode measures RTT to peers locally and computes its own
//! `latency_score`. Even with integer `rtt_ms` storage, the value varies
//! per-observer due to network topology, jitter, and probe scheduling. This
//! variance prevents election certificate attestations from producing matching
//! signatures, causing under-quorum rejections at the masternode boundary
//! (cf. issue #31, blocker (2)).
//!
//! ## Approach
//!
//! Every LN periodically publishes a [`LatencyReport`] containing its RTT
//! observations for every peer in its group. Reports are signed Ed25519,
//! bucketed at 5ms granularity (no floating point), and aggregated by every
//! observer using a deterministic median rule. The resulting per-`(group,
//! peer)` canonical bucket is stored in the [`LatencyTable`] (see
//! `types::latency_table`) and consulted by the PoU score lookup. All
//! observers consuming the same gossip buffer produce the same table.
//!
//! ## Why 5ms buckets
//!
//! - Below typical WAN jitter (10–30ms) so ranking order is stable.
//! - A `u8` covers 0..1275ms, comfortably wider than any healthy RTT.
//! - Integer arithmetic only; no f64 reproducibility issues.
//! - One byte per peer scales to 256 peers per group (Savitri caps groups
//!   at 10 today, so margin is ample).

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

/// Default RTT bucket size in milliseconds. Each `rtt_ms_bucket` value `b`
/// represents an RTT in the half-open interval `[b * 5, (b+1) * 5)`.
pub const RTT_BUCKET_MS: u64 = 5;

/// Saturating bucket cap. `rtt_ms_bucket = u8::MAX = 255` represents any RTT
/// >= 1275ms.
pub const RTT_BUCKET_MAX: u8 = u8::MAX;

/// Bucket a raw RTT in milliseconds. RTT >= 1275ms saturates to `u8::MAX`.
#[inline]
pub fn bucket_from_rtt_ms(rtt_ms: u64) -> u8 {
    let bucketed = rtt_ms / RTT_BUCKET_MS;
    if bucketed >= RTT_BUCKET_MAX as u64 {
        RTT_BUCKET_MAX
    } else {
        bucketed as u8
    }
}

/// One reporter's observation of one peer's RTT for the current window.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PeerLatencyObservation {
    /// PeerId of the observed node (the *target*, not the reporter).
    pub peer_id: String,
    /// Bucketed RTT: `rtt_ms / RTT_BUCKET_MS`, saturating at `u8::MAX`.
    pub rtt_ms_bucket: u8,
    /// Number of probe samples this observation was built from. Used as a
    /// confidence weight by the aggregator. Observations with `samples <
    /// MIN_SAMPLES` are dropped during aggregation.
    pub samples: u8,
}

/// A lightnode's signed observations of peer RTTs in its group. One report per
/// reporter per gossip publication interval. The masternode and every LN in
/// the group consume these reports and reconstruct the same [`LatencyTable`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatencyReport {
    /// Round at which the report is published. In Phase 1 this is the
    /// current finalized block height; in Phase 2 it is the lattice round.
    pub round: u64,
    /// The reporter's group_id. Aggregation only considers reports whose
    /// `group_id` matches the target group.
    pub group_id: String,
    /// The reporter's stable peer identifier (as used in PoU scoring).
    pub reporter: String,
    /// Observations of every peer in the group, excluding `reporter` itself.
    /// Order is not significant for the canonical bucket but is preserved in
    /// the signed payload.
    pub observations: Vec<PeerLatencyObservation>,
    /// Reporter's Ed25519 public key (32 bytes). Used by the verifier to
    /// reject reports whose signature does not match the claimed identity.
    /// Monotonic counter / wall-clock millis ensuring payload
    /// uniqueness across publication ticks. The verifier does not
    /// interpret the value; it exists only so gossipsub's message-id
    /// hash differs between consecutive reports even when `round` and
    /// `observations` are unchanged. Set by publisher at sign time.
    pub nonce: u64,
    pub reporter_pubkey: [u8; 32],
    /// Ed25519 signature over the canonical signable payload (see
    /// [`Self::signable_bytes`]). 64 bytes.
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

impl LatencyReport {
    /// Canonical signable payload. The signature is verified against this
    /// exact byte string. Must NOT depend on any per-observer state — the
    /// payload is reproducible by any node holding the report.
    pub fn signable_bytes(&self) -> Vec<u8> {
        // Domain separation: "savitri-latency-canon-v1|" prefix prevents
        // signature replay across protocol versions or other Savitri
        // signing contexts.
        let mut out = Vec::with_capacity(
            64 + self.group_id.len() + self.reporter.len() + self.observations.len() * 32,
        );
        out.extend_from_slice(b"savitri-latency-canon-v1|");
        out.extend_from_slice(&self.round.to_le_bytes());
        out.push(b'|');
        out.extend_from_slice(&self.nonce.to_le_bytes());
        out.push(b'|');
        out.extend_from_slice(self.group_id.as_bytes());
        out.push(b'|');
        out.extend_from_slice(self.reporter.as_bytes());
        out.push(b'|');
        for obs in &self.observations {
            out.extend_from_slice(obs.peer_id.as_bytes());
            out.push(b':');
            out.push(obs.rtt_ms_bucket);
            out.push(b':');
            out.push(obs.samples);
            out.push(b',');
        }
        out
    }

    /// Verify the report's Ed25519 signature against `reporter_pubkey`.
    /// Returns true on success. Caller still has to enforce other admission
    /// rules (round window, group match, min-samples filter).
    pub fn verify_signature(&self) -> bool {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let key = match VerifyingKey::from_bytes(&self.reporter_pubkey) {
            Ok(k) => k,
            Err(_) => return false,
        };
        let sig = Signature::from_bytes(&self.signature);
        let payload = self.signable_bytes();
        key.verify(&payload, &sig).is_ok()
    }
}
