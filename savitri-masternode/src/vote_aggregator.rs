//! Vote Aggregator - Collects and verifies votes from masternodes

use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use metrics::{counter, gauge};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::proposal_validator::{BlockCertificate, MasternodeVote, VoteType};

/// Key for tracking votes per proposal (height, round_id, block_hash, group_id)
/// group_id is included to disambiguate proposals from different groups at the same height/round.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ProposalKey {
    height: u64,
    round_id: u64,
    block_hash: [u8; 64],
    group_id: String,
}

/// Tracks votes for a specific proposal
#[derive(Debug, Clone)]
struct ProposalVotes {
    approve_votes: Vec<MasternodeVote>,
    reject_votes: Vec<MasternodeVote>,
}

impl ProposalVotes {
    fn new() -> Self {
        Self {
            approve_votes: Vec::new(),
            reject_votes: Vec::new(),
        }
    }

    fn add_vote(&mut self, vote: MasternodeVote) {
        match vote.vote_type {
            VoteType::Approve => {
                // Check if this voter already voted
                if !self
                    .approve_votes
                    .iter()
                    .any(|v| v.voter_pubkey == vote.voter_pubkey)
                {
                    self.approve_votes.push(vote);
                }
            }
            VoteType::Reject => {
                if !self
                    .reject_votes
                    .iter()
                    .any(|v| v.voter_pubkey == vote.voter_pubkey)
                {
                    self.reject_votes.push(vote);
                }
            }
        }
    }

    fn get_certificate_votes(&self) -> Vec<MasternodeVote> {
        self.approve_votes.clone()
    }
}

/// BFT supermajority threshold: ceil(2N/3) = (2N + 2) / 3.
///
/// Behavior change: pre-refactor this returned 1 for n=0 (a test fixture
/// special case). The canonical returns 0 for n=0, matching the
/// lightnode `min_voters_for_quorum` and the saner default. The n=0 path
/// is no longer reachable in production callers (`fallback_voters` is
/// always >= 1 by config).
#[inline]
fn quorum_for_voters(n: usize) -> usize {
    savitri_consensus::primitives::quorum::quorum_for_voters(n)
}

/// lifted to `savitri_consensus::primitives::group_id`. Re-exported here
/// so existing callers in this file keep working with the same name; new
/// code in other crates should import directly from the primitives module.
use savitri_consensus::primitives::group_id::group_index_from_id;

/// Special shorthand key used to alias per-group sizes by index, so
/// lookups survive an epoch change on either side.
fn index_alias_key(index: usize) -> String {
    format!("#idx:{}", index)
}

/// Vote aggregator that collects votes and creates certificates.
///
/// (leader + backup + any extra listeners). A fixed global threshold stalls
/// consensus when most groups have fewer MN than the global quorum requires.
/// `group_mn_sizes` holds per-group MN counts fed by group_formation;
/// `fallback_voters` is used when a group isn't in the map (legacy path).
/// `issued_certs` prevents duplicate certificates when late votes arrive
/// after the bucket has already reached quorum and been removed.
pub struct VoteAggregator {
    /// Votes per proposal
    votes: Arc<RwLock<HashMap<ProposalKey, ProposalVotes>>>,
    /// Per-group MN voter count (updated by group_formation sync task)
    group_mn_sizes: Arc<RwLock<HashMap<String, usize>>>,
    /// Keys for which a cert has already been issued; late votes are dropped
    issued_certs: Arc<RwLock<HashSet<ProposalKey>>>,
    /// Fallback voter count used when a group isn't in group_mn_sizes yet
    fallback_voters: usize,
}

impl VoteAggregator {
    pub fn new(total_voters: usize) -> Self {
        Self {
            votes: Arc::new(RwLock::new(HashMap::new())),
            group_mn_sizes: Arc::new(RwLock::new(HashMap::new())),
            issued_certs: Arc::new(RwLock::new(HashSet::new())),
            fallback_voters: total_voters,
        }
    }

    /// Bulk-replace the group→MN-count map. Called from the group_formation sync task.
    ///
    /// For each entry we also insert an index-keyed alias (see [`index_alias_key`])
    /// so the lookup survives epoch drift: if a vote arrives carrying an old
    /// epoch's group_id, we can still find the MN count from the stable index.
    pub async fn set_group_mn_sizes(&self, sizes: HashMap<String, usize>) {
        let mut m = self.group_mn_sizes.write().await;
        m.clear();
        for (gid, n) in sizes {
            if let Some(idx) = group_index_from_id(&gid) {
                m.insert(index_alias_key(idx), n);
            }
            m.insert(gid, n);
        }
    }

    /// Resolve the quorum threshold for a specific group_id. Strategy:
    ///   1. Exact group_id match (fast path — same epoch on both sides)
    ///   2. Index alias match (handles epoch drift between MN and LN)
    ///   3. Global fallback (legacy behaviour)
    async fn quorum_for_group(&self, group_id: &str) -> usize {
        let sizes = self.group_mn_sizes.read().await;
        if let Some(&n) = sizes.get(group_id) {
            return quorum_for_voters(n);
        }
        if let Some(idx) = group_index_from_id(group_id) {
            if let Some(&n) = sizes.get(&index_alias_key(idx)) {
                return quorum_for_voters(n);
            }
        }
        quorum_for_voters(self.fallback_voters)
    }

    /// Verify the signature on a vote
    fn verify_vote_signature(&self, vote: &MasternodeVote) -> bool {
        // Create signable data (must match the signing logic in ProposalValidator::vote_on_proposal)
        // IMPORTANT: group_id is included at the end of the payload to prevent
        // cross-group replay attacks. This must match the signing order exactly.
        let mut vote_data = Vec::new();
        vote_data.extend_from_slice(&vote.round_id.to_le_bytes());
        vote_data.extend_from_slice(&vote.height.to_le_bytes());
        vote_data.extend_from_slice(&vote.block_hash);
        vote_data.extend_from_slice(&vote.voter_pubkey);
        vote_data.push(match vote.vote_type {
            VoteType::Approve => 1,
            VoteType::Reject => 0,
        });
        vote_data.extend_from_slice(vote.group_id.as_bytes());

        // Parse the verifying key from voter_pubkey
        let verifying_key = match VerifyingKey::from_bytes(&vote.voter_pubkey) {
            Ok(key) => key,
            Err(e) => {
                error!("Failed to parse voter public key: {}", e);
                return false;
            }
        };

        // Parse the signature
        let signature = Signature::from_bytes(&vote.signature);

        // Verify the signature
        match verifying_key.verify(&vote_data, &signature) {
            Ok(_) => {
                debug!("Vote signature verification passed");
                true
            }
            Err(e) => {
                warn!("Vote signature verification failed: {}", e);
                false
            }
        }
    }

    /// Add a vote to the aggregator
    /// Returns Some(BlockCertificate) if quorum is reached
    pub async fn add_vote(&self, vote: MasternodeVote) -> Option<BlockCertificate> {
        // Verify the vote signature
        if !self.verify_vote_signature(&vote) {
            warn!(
                height = vote.height,
                round_id = vote.round_id,
                "Invalid vote signature, rejecting"
            );
            return None;
        }

        let key = ProposalKey {
            height: vote.height,
            round_id: vote.round_id,
            block_hash: vote.block_hash,
            group_id: vote.group_id.clone(),
        };

        // Drop late votes for keys we've already certified. With per-group quorum
        // the threshold can be as low as 2, so additional votes arriving after
        // cert issuance must not spawn a fresh bucket and re-issue.
        {
            let issued = self.issued_certs.read().await;
            if issued.contains(&key) {
                debug!(
                    height = key.height,
                    round_id = key.round_id,
                    group_id = %key.group_id,
                    "Ignoring late vote for already-certified proposal"
                );
                return None;
            }
        }

        let quorum = self.quorum_for_group(&key.group_id).await;

        let mut votes = self.votes.write().await;
        let proposal_votes = votes.entry(key.clone()).or_insert_with(ProposalVotes::new);

        proposal_votes.add_vote(vote);
        counter!("consensus_votes_total").increment(1);

        info!(
            height = key.height,
            round_id = key.round_id,
            group_id = %key.group_id,
            approve_count = proposal_votes.approve_votes.len(),
            reject_count = proposal_votes.reject_votes.len(),
            quorum_threshold = quorum,
            "Vote aggregated"
        );

        // Check if quorum is reached
        if proposal_votes.approve_votes.len() >= quorum {
            counter!("consensus_quorum_achieved_total").increment(1);
            info!(
                height = key.height,
                round_id = key.round_id,
                group_id = %key.group_id,
                votes = proposal_votes.approve_votes.len(),
                threshold = quorum,
                "Quorum reached! Creating block certificate"
            );
            info!(
                height = key.height,
                round_id = key.round_id,
                "Block approved by masternode"
            );

            // Roots and parent_hash from first approve vote so LN can build block with MN-agreed header
            let (state_root, tx_root, parent_hash) = proposal_votes
                .approve_votes
                .first()
                .map(|v| (v.state_root, v.tx_root, v.parent_hash))
                .unwrap_or(([0u8; 64], [0u8; 64], [0u8; 64]));

            // Create certificate — group_id is propagated from ProposalKey so the
            // certificate is unambiguously tied to the originating group.
            let certificate = BlockCertificate {
                round_id: key.round_id,
                height: key.height,
                block_hash: key.block_hash,
                votes: proposal_votes.get_certificate_votes(),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs(),
                group_id: key.group_id.clone(),
                state_root,
                tx_root,
                parent_hash,
                reward_recipients: Vec::new(),
            };
            info!(
                height = certificate.height,
                round_id = certificate.round_id,
                votes = certificate.votes.len(),
                "Certification block issued by masternode"
            );
            info!(
                height = certificate.height,
                round_id = certificate.round_id,
                group_id = %certificate.group_id,
                reward_recipients = certificate.reward_recipients.len(),
                "GROUP_CHECK_DEBUG: block certificate created"
            );

            // Remove the bucket and mark this proposal as already-certified so late
            // votes arriving after the fact are dropped instead of creating a
            // fresh bucket that could re-issue a cert.
            votes.remove(&key);
            drop(votes);
            let mut issued = self.issued_certs.write().await;
            issued.insert(key);

            return Some(certificate);
        }

        None
    }

    /// Clean up old votes for proposals that didn't reach quorum and drop
    /// entries in `issued_certs` below the recent-height window.
    pub async fn cleanup_old_votes(&self, max_height: u64) {
        let floor = max_height.saturating_sub(10);
        {
            let mut votes = self.votes.write().await;
            let initial_count = votes.len();
            votes.retain(|key, _| key.height >= floor);
            let removed = initial_count - votes.len();
            if removed > 0 {
                info!("Cleaned up {} old vote entries", removed);
            }
        }
        {
            let mut issued = self.issued_certs.write().await;
            let initial_count = issued.len();
            issued.retain(|key| key.height >= floor);
            let removed = initial_count - issued.len();
            if removed > 0 {
                debug!("Cleaned up {} issued-cert dedup entries", removed);
            }
        }
    }

    /// Get vote statistics for monitoring
    pub async fn get_stats(&self) -> VoteAggregatorStats {
        let votes = self.votes.read().await;
        let active_proposals = votes.len();
        let total_votes: usize = votes
            .values()
            .map(|pv| pv.approve_votes.len() + pv.reject_votes.len())
            .sum();

        VoteAggregatorStats {
            active_proposals,
            total_votes,
        }
    }
}

#[derive(Debug, Clone)]
pub struct VoteAggregatorStats {
    pub active_proposals: usize,
    pub total_votes: usize,
}
