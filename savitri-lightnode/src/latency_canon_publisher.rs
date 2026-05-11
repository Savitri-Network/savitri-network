//! Latency Canon publisher — periodic LatencyReport gossip task.
//!
//! Part of Savitri V0.2 Phase 1 (Score Canonicity). See
//! `docs/CONSENSUS_V0.2_DESIGN.md` §3.3 for the full specification.
//!
//! Every `LATENCY_CANON_PUBLISH_INTERVAL_SECS` the publisher:
//!   1. Reads the local `ObservationStore` to compute per-peer median RTT
//!      across the rolling window (bucketed at 5ms).
//!   2. Builds a signed `LatencyReport` for the current group.
//!   3. Publishes it on the intra-group gossip topic
//!      `/savitri/group/<gid>/latency_canon/1`.
//!
//! Every subscribed node (LN and MN in the same group) deserializes the
//! report, verifies the signature, and feeds it to its in-memory buffer
//! consumed by `LatencyTable::rebuild_from_reports`.
//!
//! ## Why a separate module
//!
//! Keeping the publisher decoupled from `intra_group::mod.rs` makes the
//! gossip wire-format and timer policy easy to evolve in Phase 2 (where the
//! report migrates into the lattice cell header). The only coupling point
//! is the channel sender for `(topic, payload)` pairs, which is the same
//! interface the existing topics already use.

use std::sync::Arc;
use std::time::Duration;

use ed25519_dalek::Signer;
use libp2p::gossipsub::IdentTopic;
use savitri_consensus::scoring::ObservationStore;
use savitri_consensus::types::LatencyReport;
use savitri_core::crypto::Keypair;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Gossip topic prefix. Full topic per group:
/// `/savitri/group/<group_id>/latency_canon/1`.
pub const LATENCY_CANON_TOPIC_PREFIX: &str = "/savitri/group/";
pub const LATENCY_CANON_TOPIC_SUFFIX: &str = "/latency_canon/1";

/// Publication interval — how often the local node publishes a report.
/// Each report covers the most recent observation window. Defaults are
/// chosen so the gossip overhead is negligible relative to TX traffic
/// (one ≤500 byte message per LN every 10s).
pub const LATENCY_CANON_PUBLISH_INTERVAL_SECS: u64 = 10;

/// V0.2 Phase 2 (latency table convergence): the `round` field on
/// `LatencyReport` carries a wall-clock-aligned bucket index, NOT chain
/// height. All LNs sharing a synchronized clock land in the same bucket
/// for each publication tick, so the aggregator's window filter accepts
/// the same set of reports across all observers — making the canonical
/// table byte-identical cluster-wide. Window size of 3 buckets gives ~30s
/// tolerance for NTP drift / network jitter.
///
/// Bucket size MUST equal `LATENCY_CANON_PUBLISH_INTERVAL_SECS` so each
/// publication tick increments the bucket by exactly 1.
pub const WALL_CLOCK_BUCKET_SECS: u64 = LATENCY_CANON_PUBLISH_INTERVAL_SECS;

/// Compute the current wall-clock-aligned bucket. Used as `round` in
/// `LatencyReport`. Same formula across publisher and aggregator → same
/// canonical table.
#[inline]
pub fn current_wall_clock_bucket() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() / WALL_CLOCK_BUCKET_SECS)
        .unwrap_or(0)
}

/// Build the canonical gossip topic name for a given group_id.
pub fn topic_for_group(group_id: &str) -> IdentTopic {
    IdentTopic::new(format!(
        "{}{}{}",
        LATENCY_CANON_TOPIC_PREFIX, group_id, LATENCY_CANON_TOPIC_SUFFIX
    ))
}

/// State required by the publisher loop. Snapshotted at spawn time; the
/// publisher polls `current_group_id` and `current_round` on each tick so
/// it picks up epoch changes without restart.
#[derive(Clone)]
pub struct LatencyCanonPublisherConfig {
    /// Stable local peer identifier (matches `reporter` field in reports
    /// and the `exclude_self` argument used by the observation builder).
    pub local_peer_id: String,
    /// Reporter signing key. The published report's signature MUST verify
    /// against the corresponding public key.
    pub signing_key: Arc<Keypair>,
    /// Local observation store containing recent RTT samples.
    pub observations: Arc<ObservationStore>,
    /// Channel into the libp2p task. Sending `(topic, bytes)` enqueues a
    /// gossipsub publish on the next event-loop turn.
    pub network_publish_tx: mpsc::Sender<(IdentTopic, Vec<u8>)>,
}

/// Spawn the periodic publisher task. Returns immediately; the task runs
/// until either `group_provider` returns a permanent `None` for too long
/// (defensive: if the LN is not in any group, do not waste cycles) or the
/// process exits.
///
/// `group_and_round_provider` is invoked every tick to fetch
/// `(current_group_id, current_round)`. Returning `None` skips the tick.
/// Common implementation: read from `Arc<RwLock<Option<GroupState>>>`.
pub fn spawn_publisher<F>(config: LatencyCanonPublisherConfig, mut group_and_round_provider: F)
where
    F: FnMut() -> Option<(String, u64)> + Send + 'static,
{
    tokio::spawn(async move {
        let mut tick =
            tokio::time::interval(Duration::from_secs(LATENCY_CANON_PUBLISH_INTERVAL_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Skip the immediate first tick — let the observation window
        // accumulate at least a few samples before publishing the first
        // report.
        tick.tick().await;

        loop {
            tick.tick().await;

            let Some((group_id, round)) = group_and_round_provider() else {
                debug!("LatencyCanon publisher: no current group, skipping tick");
                continue;
            };

            let observations = config
                .observations
                .build_canon_observations(&config.local_peer_id);
            if observations.is_empty() {
                debug!(
                    group_id = %group_id,
                    "LatencyCanon publisher: no peer observations yet, skipping tick"
                );
                continue;
            }

            // Build and sign the report.
            let verifying_key = config.signing_key.verifying_key();
            let nonce_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            let mut report = LatencyReport {
                round,
                group_id: group_id.clone(),
                reporter: config.local_peer_id.clone(),
                observations,
                nonce: nonce_ms,
                reporter_pubkey: verifying_key.to_bytes(),
                signature: [0u8; 64],
            };
            let payload = report.signable_bytes();
            let sig = config.signing_key.sign(&payload);
            report.signature = sig.to_bytes();

            // Serialize and enqueue. We use serde_json to stay symmetric
            // with the existing election/probe topics (which also use
            // JSON). bincode would shrink the wire size by ~30% but adds
            // a per-topic dual-codec path; not worth the complexity at
            // this volume.
            let encoded = match serde_json::to_vec(&report) {
                Ok(v) => v,
                Err(e) => {
                    warn!(error = %e, "LatencyCanon publisher: serialize failed");
                    continue;
                }
            };
            let topic = topic_for_group(&group_id);
            // Best-effort send. If the channel is closed the network task
            // is gone and we should just exit silently.
            if config
                .network_publish_tx
                .send((topic, encoded))
                .await
                .is_err()
            {
                debug!("LatencyCanon publisher: publish channel closed, exiting");
                return;
            }
            debug!(
                group_id = %group_id,
                round,
                peer_count = report.observations.len(),
                "LatencyCanon publisher: report published"
            );
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_for_group_format_matches_spec() {
        let t = topic_for_group("group_42_0");
        // The IdentTopic Display impl gives back the topic string verbatim.
        assert_eq!(
            format!("{}", t),
            "/savitri/group/group_42_0/latency_canon/1"
        );
    }

    #[test]
    fn topic_for_group_handles_complex_ids() {
        let t = topic_for_group("group_410_0_410_e3fde077");
        assert_eq!(
            format!("{}", t),
            "/savitri/group/group_410_0_410_e3fde077/latency_canon/1"
        );
    }
}
