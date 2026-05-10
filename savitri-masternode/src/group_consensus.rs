//! Group Consensus Protocol for Masternodes
//!
//! This module implements BFT-based group approval system where masternodes
//! propose, vote, and approve P2P groups for lightnodes.

use anyhow::{Context, Result};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier};
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use sha2::{Digest, Sha512};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::group_formation::{GroupFormationManager, LightNodeInfo, P2PGroup};

// Custom serialization for Vec<[u8; 64]> arrays
mod signature_array {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    pub fn serialize<S>(data: &Vec<[u8; 64]>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let hex_strings: Vec<String> = data.iter().map(|sig| hex::encode(sig)).collect();
        hex_strings.serialize(serializer)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<[u8; 64]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let hex_strings: Vec<String> = Vec::deserialize(deserializer)?;
        let mut result = Vec::new();

        for hex_str in hex_strings {
            let bytes = hex::decode(&hex_str).map_err(serde::de::Error::custom)?;

            if bytes.len() != 64 {
                return Err(serde::de::Error::custom("Expected 64 bytes for signature"));
            }

            let mut array = [0u8; 64];
            array.copy_from_slice(&bytes);
            result.push(array);
        }

        Ok(result)
    }
}

/// Leader election proposal - a masternode proposes itself as the group creator
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderElectionProposal {
    /// Unique election ID for this round
    pub election_id: String,
    /// Proposing masternode ID
    pub proposer_masternode: String,
    /// Timestamp when proposal was created (used for winner determination)
    pub timestamp: u64,
    /// Epoch for which groups will be created
    pub epoch: u64,
    /// Number of available lightnodes seen by this masternode
    pub available_lightnode_count: usize,
    /// List of available lightnode IDs seen by this masternode
    pub available_lightnode_ids: Vec<String>,
    /// Proposal signature
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

/// Leader election certificate - approval from a masternode for a leader proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderElectionCertificate {
    /// Election ID being certified
    pub election_id: String,
    /// Epoch for which this certificate is valid
    pub epoch: u64,
    /// Approving masternode ID
    pub approver_masternode: String,
    /// Approval timestamp
    pub approval_timestamp: u64,
    /// Whether the approver agrees with the lightnode list
    pub lightnode_list_verified: bool,
    /// Number of lightnodes verified by approver
    pub verified_lightnode_count: usize,
    /// Approval signature
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

/// Leader election state
#[derive(Debug, Clone, PartialEq)]
pub enum LeaderElectionState {
    /// No election in progress
    Idle,
    /// Election proposals are being collected
    Collecting,
    /// A leader has been elected and certified
    Elected {
        leader_masternode: String,
        election_id: String,
    },
    /// Leader is creating groups (only the elected leader)
    CreatingGroups {
        leader_masternode: String,
        election_id: String,
    },
}

/// Group proposal for BFT approval
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupProposal {
    /// Unique proposal ID
    pub proposal_id: String,
    /// Epoch for which groups are proposed
    pub epoch: u64,
    /// Proposed groups
    pub groups: Vec<P2PGroup>,
    /// Proposing masternode ID
    pub proposer_masternode: String,
    /// Lightnodes used in these groups
    pub used_lightnodes: Vec<String>,
    /// Timestamp
    pub timestamp: u64,
    /// Proposal signature
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

/// Vote on group proposal
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupVote {
    /// Proposal ID being voted on
    pub proposal_id: String,
    /// Voting masternode ID
    pub voter_masternode: String,
    /// Vote type
    pub vote_type: GroupVoteType,
    /// Timestamp
    pub timestamp: u64,
    /// Vote signature
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum GroupVoteType {
    Approve,
    Reject,
}

/// Group approval certificate after BFT consensus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupApprovalCertificate {
    /// Original proposal
    pub proposal: GroupProposal,
    /// All votes received
    pub votes: Vec<GroupVote>,
    /// Final approval status
    pub approved: bool,
    /// Approval timestamp
    pub approval_timestamp: u64,
    /// Masternode signatures for the certificate
    #[serde(with = "signature_array")]
    pub masternode_signatures: Vec<[u8; 64]>,
}

/// Group consensus manager
pub struct GroupConsensusManager {
    /// Local masternode ID
    local_masternode_id: String,
    /// Group formation manager
    group_manager: Arc<GroupFormationManager>,
    /// Active proposals
    active_proposals: Arc<RwLock<HashMap<String, GroupProposal>>>,
    /// Received votes
    proposal_votes: Arc<RwLock<HashMap<String, Vec<GroupVote>>>>,
    /// Approved certificates
    approved_certificates: Arc<RwLock<HashMap<u64, GroupApprovalCertificate>>>,
    /// Available lightnodes (not yet assigned to groups)
    available_lightnodes: Arc<RwLock<HashMap<String, LightNodeInfo>>>,
    /// BFT configuration
    bft_config: BftGroupConfig,
    /// Current epoch
    current_epoch: Arc<RwLock<u64>>,
    /// P2P distributor for sending groups to lightnodes
    p2p_distributor: Option<Arc<dyn super::group_formation::MonolithP2PDistributor>>,
    // --- Leader Election State ---
    /// Current leader election state
    leader_election_state: Arc<RwLock<LeaderElectionState>>,
    /// Active leader election proposals (election_id -> proposal)
    leader_proposals: Arc<RwLock<HashMap<String, LeaderElectionProposal>>>,
    /// Received leader election certificates (election_id -> Vec<certificates>)
    leader_certificates: Arc<RwLock<HashMap<String, Vec<LeaderElectionCertificate>>>>,
    /// Timeout for leader election collection phase (seconds)
    leader_election_timeout_secs: u64,
    /// Timestamp when leader election collection started
    leader_election_start_time: Arc<RwLock<Option<u64>>>,
    /// Dynamic count of currently active/connected masternodes (updated by main.rs)
    /// Used for dynamic quorum calculation instead of hardcoded total_masternodes
    active_masternodes_count: Arc<RwLock<usize>>,
    /// List ordinata degli ID masternode (self + connessi) per assegnare un leader per gruppo
    ordered_masternode_ids: Arc<RwLock<Vec<String>>>,
    /// Masternode bannati o non raggiungibili (non eleggibili come leader di gruppo).
    /// Popolato quando un masternode viene bannato; used per il passaggio di proprietà.
    banned_masternode_ids: Arc<RwLock<HashSet<String>>>,
}

/// BFT configuration for group consensus
#[derive(Debug, Clone)]
pub struct BftGroupConfig {
    /// Minimum masternodes required for BFT
    pub min_masternodes: usize,
    /// Maximum faulty masternodes tolerated
    pub max_faulty: usize,
    /// Total masternodes in network
    pub total_masternodes: usize,
    /// Voting timeout in milliseconds
    pub vote_timeout_ms: u64,
    /// Minimum group size
    pub min_group_size: usize,
    /// Maximum group size
    pub max_group_size: usize,
    /// Approval threshold (2/3 by default)
    pub approval_threshold: f64,
    /// Il limit effettivo è dinamico: min(cap, ceil(num_gruppi / num_masternode)).
    pub max_groups_per_masternode_cap: usize,
}

impl Default for BftGroupConfig {
    fn default() -> Self {
        Self {
            min_masternodes: 3,
            max_faulty: 1,
            total_masternodes: 3,
            vote_timeout_ms: 10000, // 10 seconds
            min_group_size: 5,
            max_group_size: 8,
            approval_threshold: 0.67, // 2/3 supermajority
            max_groups_per_masternode_cap: 10,
        }
    }
}

impl GroupConsensusManager {
    /// Refresh internal epoch from group formation scheduler.
    async fn refresh_epoch(&self) -> u64 {
        let epoch = self.group_manager.current_epoch();
        let mut current = self.current_epoch.write().await;
        *current = epoch;
        epoch
    }
    /// Debug helper: address of the leader election state Arc for tracing shared state
    pub fn leader_election_state_ptr(&self) -> usize {
        std::sync::Arc::as_ptr(&self.leader_election_state) as usize
    }

    /// Update the count of currently active/connected masternodes.
    /// Called periodically by main.rs based on actual P2P connections.
    /// The count includes this masternode itself (+1 for self).
    /// Uses a floor of min_masternodes so that nodes still bootstrapping (few connections)
    /// don't drop to 1 and block BFT alignment; they can still receive and apply
    /// GroupApprovalCertificates via GroupSyncResponse.
    pub async fn update_active_masternodes_count(&self, connected_peers: usize) {
        // +1 because we count ourselves as active
        let raw_active = connected_peers + 1;
        // Floor: never go below min_masternodes so bootstrap/late-joining nodes
        // keep a consistent quorum view and can accept certificates from the rest.
        let active = raw_active
            .max(self.bft_config.min_masternodes)
            .min(self.bft_config.total_masternodes);
        let mut count = self.active_masternodes_count.write().await;
        let old = *count;
        *count = active;
        if old != *count {
            info!(
                old_count = old,
                new_count = *count,
                connected_peers = connected_peers,
                raw_active = raw_active,
                total_configured = self.bft_config.total_masternodes,
                "🔄 DYNAMIC QUORUM: Active masternodes count updated (floor=min_masternodes)"
            );
        }
    }

    /// Minimum masternodes required for BFT (from config). Used e.g. to decide when to request group sync.
    pub fn min_masternodes_for_bft(&self) -> usize {
        self.bft_config.min_masternodes
    }

    /// Calculate the dynamic quorum required for BFT decisions.
    /// Uses the actual active masternodes count instead of the hardcoded total.
    /// Returns (required_approvals, active_count).
    /// Returns None if there aren't enough active masternodes for BFT.
    async fn calculate_dynamic_quorum(&self) -> Option<(usize, usize)> {
        let active = *self.active_masternodes_count.read().await;

        // Need at least min_masternodes to operate
        if active < self.bft_config.min_masternodes {
            warn!(
                active = active,
                min_required = self.bft_config.min_masternodes,
                "🔒 DYNAMIC QUORUM: Not enough active masternodes for BFT (have {}, need {})",
                active,
                self.bft_config.min_masternodes
            );
            return None;
        }

        // Calculate required approvals: ceil(active * threshold)
        let required = (active as f64 * self.bft_config.approval_threshold).ceil() as usize;
        // Ensure at least min_masternodes
        let required = required.max(self.bft_config.min_masternodes);

        info!(
            active_masternodes = active,
            required_approvals = required,
            threshold = self.bft_config.approval_threshold,
            "📊 DYNAMIC QUORUM: {} active masternodes, {} approvals required (threshold: {:.0}%)",
            active,
            required,
            self.bft_config.approval_threshold * 100.0
        );

        Some((required, active))
    }

    pub fn new(
        local_masternode_id: String,
        group_manager: Arc<GroupFormationManager>,
        bft_config: BftGroupConfig,
    ) -> Self {
        let initial_active_count = bft_config.total_masternodes;
        Self {
            local_masternode_id,
            group_manager,
            active_proposals: Arc::new(RwLock::new(HashMap::new())),
            proposal_votes: Arc::new(RwLock::new(HashMap::new())),
            approved_certificates: Arc::new(RwLock::new(HashMap::new())),
            available_lightnodes: Arc::new(RwLock::new(HashMap::new())),
            bft_config,
            current_epoch: Arc::new(RwLock::new(0)),
            p2p_distributor: None,
            leader_election_state: Arc::new(RwLock::new(LeaderElectionState::Idle)),
            leader_proposals: Arc::new(RwLock::new(HashMap::new())),
            leader_certificates: Arc::new(RwLock::new(HashMap::new())),
            leader_election_timeout_secs: 10, // 10 seconds to collect proposals
            leader_election_start_time: Arc::new(RwLock::new(None)),
            // Start with configured total; updated dynamically by main.rs based on connected peers
            active_masternodes_count: Arc::new(RwLock::new(initial_active_count)),
            ordered_masternode_ids: Arc::new(RwLock::new(Vec::new())),
            banned_masternode_ids: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    pub async fn set_ordered_masternode_ids(&self, ids: Vec<String>) {
        let mut list = ids;
        list.sort();
        let mut current = self.ordered_masternode_ids.write().await;
        *current = list;
    }

    /// Returns la list degli ID masternode connessi (disponibili per leader di gruppo).
    pub async fn get_ordered_masternode_ids(&self) -> Vec<String> {
        self.ordered_masternode_ids.read().await.clone()
    }

    /// Returns gli ID masternode disponibili (connessi e non bannati) per il passaggio di proprietà.
    pub async fn get_available_masternode_ids(&self) -> Vec<String> {
        let ids = self.ordered_masternode_ids.read().await;
        let banned = self.banned_masternode_ids.read().await;
        ids.iter()
            .filter(|id| !banned.contains(*id))
            .cloned()
            .collect()
    }

    /// Check se un masternode è disponibile (connesso, non bannato, raggiungibile).
    /// Usato per decidere se trasferire la proprietà of the gruppo.
    pub async fn is_masternode_available(&self, masternode_id: &str) -> bool {
        let ids = self.ordered_masternode_ids.read().await;
        let banned = self.banned_masternode_ids.read().await;
        ids.contains(&masternode_id.to_string()) && !banned.contains(masternode_id)
    }

    /// Ban un masternode (non più eleggibile come leader di gruppo fino a revoke).
    pub async fn ban_masternode(&self, masternode_id: &str) {
        let mut banned = self.banned_masternode_ids.write().await;
        banned.insert(masternode_id.to_string());
        info!(masternode_id = %masternode_id, "Masternode banned from group leadership");
    }

    /// Revoke il ban di un masternode.
    pub async fn unban_masternode(&self, masternode_id: &str) {
        let mut banned = self.banned_masternode_ids.write().await;
        banned.remove(masternode_id);
        info!(masternode_id = %masternode_id, "Masternode unbanned from group leadership");
    }

    /// Returns il masternode leader per il gruppo di indice `group_index` (gruppo 0, 1, 2, ...).
    pub async fn get_group_leader_masternode(&self, group_index: usize) -> Option<String> {
        let ids = self.ordered_masternode_ids.read().await;
        if ids.is_empty() {
            return None;
        }
        Some(ids[group_index % ids.len()].clone())
    }

    pub async fn am_i_coordinator_for_epoch(&self, epoch: u64) -> bool {
        let ids = self.ordered_masternode_ids.read().await;
        if ids.is_empty() {
            return false;
        }
        let coordinator = &ids[epoch as usize % ids.len()];
        coordinator == &self.local_masternode_id
    }

    pub async fn has_ordered_masternodes(&self) -> bool {
        !self.ordered_masternode_ids.read().await.is_empty()
    }

    /// True se siamo il coordinatore per l’epoch corrente (usa l’epoch interno aggiornato).
    pub async fn am_i_coordinator_for_current_epoch(&self) -> bool {
        let epoch = self.refresh_epoch().await;
        self.am_i_coordinator_for_epoch(epoch).await
    }

    /// Set P2P distributor for sending groups to lightnodes
    pub fn set_p2p_distributor(
        &mut self,
        distributor: Arc<dyn super::group_formation::MonolithP2PDistributor>,
    ) {
        self.p2p_distributor = Some(distributor);
    }

    /// Distribute groups to lightnodes using the P2P distributor (public method)
    pub async fn distribute_groups(
        &self,
        groups: &[super::group_formation::P2PGroup],
        epoch: u64,
    ) -> Result<()> {
        if groups.is_empty() {
            return Ok(());
        }

        if let Some(ref distributor) = self.p2p_distributor {
            info!(
                groups_count = groups.len(),
                epoch = epoch,
                "🔧 GROUP CONSENSUS: Distributing groups via P2P distributor"
            );

            if let Err(e) = distributor.distribute_groups(groups, epoch).await {
                error!(
                    error = %e,
                    groups_count = groups.len(),
                    "❌ GROUP CONSENSUS: Failed to distribute groups"
                );
                return Err(e);
            }

            info!(
                groups_count = groups.len(),
                epoch = epoch,
                "✅ GROUP CONSENSUS: Successfully distributed groups to lightnodes"
            );
        } else {
            warn!(
                groups_count = groups.len(),
                "⚠️ GROUP CONSENSUS: No P2P distributor available - distribution skipped"
            );
        }

        Ok(())
    }

    /// Initialize available lightnodes from group manager
    pub async fn initialize_available_lightnodes(&self) -> Result<()> {
        let registered_nodes = self.group_manager.get_registered_nodes().await;
        let mut available = self.available_lightnodes.write().await;

        // Only Free nodes are available for new group formation
        let mut free_count = 0usize;
        let mut assigned_count = 0usize;
        for node in registered_nodes {
            if node.assignment == super::group_formation::NodeAssignmentStatus::Free {
                available.insert(node.node_id.clone(), node);
                free_count += 1;
            } else {
                assigned_count += 1;
            }
        }

        info!(
            free = free_count,
            assigned = assigned_count,
            "Initialized available lightnodes (Free only)"
        );
        Ok(())
    }

    /// Initialize available lightnodes in batch for improved performance
    pub async fn initialize_available_lightnodes_batch(
        &self,
        new_nodes: Vec<super::group_formation::LightNodeInfo>,
    ) -> Result<()> {
        if new_nodes.is_empty() {
            return Ok(());
        }

        let mut available = self.available_lightnodes.write().await;
        let mut added_count = 0;

        for node in new_nodes {
            if !available.contains_key(&node.node_id) {
                available.insert(node.node_id.clone(), node);
                added_count += 1;
            }
        }

        info!(
            "Batch initialized {} new lightnodes (total: {})",
            added_count,
            available.len()
        );
        Ok(())
    }

    /// Check if we can create a group with minimum required lightnodes
    pub async fn can_create_group(&self) -> bool {
        let available = self.available_lightnodes.read().await;
        available.len() >= self.bft_config.min_group_size
    }

    /// Create and propose groups when we have enough lightnodes
    pub async fn create_and_propose_groups(&self) -> Result<Option<GroupProposal>> {
        if !self.can_create_group().await {
            debug!("Not enough available lightnodes for group creation");
            return Ok(None);
        }

        // PERF: Skip group creation if existing groups are healthy.
        // This prevents the BFT coordinator from proposing new groups that
        // disrupt active block production. Checked by ALL potential coordinators.
        //
        // BFT STATUS FIX: only `Active` (BFT-approved) groups count as healthy.
        // A `Forming` skeleton in active_groups means BFT did not yet approve
        // and the cluster is NOT in steady state — the coordinator must keep
        // proposing until approval lands. Without this, a stale Forming
        // skeleton inserted by form_groups would silently suppress every
        // proposal from this MN forever.
        {
            let existing = self.group_manager.get_active_groups().await;
            if !existing.is_empty() {
                let min_size = 5; // default min_group_size
                let all_healthy = existing.iter().all(|g| {
                    g.status == super::group_formation::GroupStatus::Active
                        && g.members.len() >= min_size
                });
                if all_healthy {
                    info!(
                        active_groups = existing.len(),
                        "BFT: Skipping group creation — existing groups are healthy (Active+sized)"
                    );
                    return Ok(None);
                }
            }
        }

        let current_epoch = self.refresh_epoch().await;

        // Use GroupFormationManager to create groups (not our own logic)
        let mut groups = self.group_manager.form_groups(true).await?;
        if groups.is_empty() {
            info!("Not enough nodes for group formation yet");
            return Ok(None);
        }

        // The proposer MN becomes leader of all groups in its proposal.
        // A MN can be leader of multiple groups simultaneously.
        // Backup is chosen deterministically from the remaining MNs.
        let ids = self.ordered_masternode_ids.read().await;
        for group in groups.iter_mut() {
            group.group_leader_masternode = Some(self.local_masternode_id.clone());

            // Backup: deterministic hash selection from MNs != leader
            let remaining: Vec<&String> = ids
                .iter()
                .filter(|id| **id != self.local_masternode_id)
                .collect();
            if !remaining.is_empty() {
                use sha2::Digest;
                let mut hasher = sha2::Sha256::new();
                hasher.update(group.group_id.as_bytes());
                hasher.update(b"|backup_seed");
                let hash_result = hasher.finalize();
                let seed = u64::from_le_bytes(hash_result[..8].try_into().unwrap_or([0u8; 8]));
                let backup_idx = (seed % remaining.len() as u64) as usize;
                group.backup_leader_masternode = Some(remaining[backup_idx].clone());
            }
            info!(
                group_id = %group.group_id,
                leader = %self.local_masternode_id,
                backup = ?group.backup_leader_masternode,
                "📋 Proposer MN is leader of group (backup chosen by hash)"
            );
        }
        drop(ids);

        // Extract used lightnodes from created groups
        let used_node_ids: Vec<String> = groups
            .iter()
            .flat_map(|g| g.members.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        if used_node_ids.len() < self.bft_config.min_group_size {
            return Ok(None);
        }

        // Create proposal
        let proposal_id = format!(
            "group_proposal_{}_{}_{}",
            current_epoch,
            self.local_masternode_id,
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs()
        );

        let mut proposal = GroupProposal {
            proposal_id: proposal_id.clone(),
            epoch: current_epoch,
            groups,
            proposer_masternode: self.local_masternode_id.clone(),
            used_lightnodes: used_node_ids,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            signature: [0u8; 64],
        };
        proposal.signature = Self::sign_group_proposal(&proposal, &self.local_masternode_id);

        // Store proposal
        {
            let mut active = self.active_proposals.write().await;
            active.insert(proposal_id.clone(), proposal.clone());
        }

        // Remove used lightnodes from available pool temporarily
        {
            let mut available = self.available_lightnodes.write().await;
            for node_id in &proposal.used_lightnodes {
                available.remove(node_id);
            }
        }

        info!(
            proposal_id = %proposal.proposal_id,
            groups_count = proposal.groups.len(),
            lightnodes_used = proposal.used_lightnodes.len(),
            "Created group proposal for BFT approval using GroupFormationManager"
        );

        Ok(Some(proposal))
    }

    /// Vote on a group proposal
    pub async fn vote_on_proposal(
        &self,
        proposal: &GroupProposal,
        approve: bool,
    ) -> Result<GroupVote> {
        // Validate proposal
        if !self.validate_proposal(proposal).await? {
            warn!("Invalid proposal: {}", proposal.proposal_id);
            return Err(anyhow::anyhow!("Invalid proposal"));
        }

        let mut vote = GroupVote {
            proposal_id: proposal.proposal_id.clone(),
            voter_masternode: self.local_masternode_id.clone(),
            vote_type: if approve {
                GroupVoteType::Approve
            } else {
                GroupVoteType::Reject
            },
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            signature: [0u8; 64],
        };
        vote.signature = Self::sign_group_vote(&vote, proposal.epoch);

        // Store vote
        {
            let mut votes = self.proposal_votes.write().await;
            let proposal_votes = votes
                .entry(proposal.proposal_id.clone())
                .or_insert_with(Vec::new);
            proposal_votes.push(vote.clone());
            info!(
                proposal_id = %proposal.proposal_id,
                total_votes = proposal_votes.len(),
                "Stored local vote for proposal"
            );
        }

        info!(
            proposal_id = %proposal.proposal_id,
            vote_type = ?vote.vote_type,
            "Voted on group proposal"
        );

        // Check if we have enough votes for decision
        // ⚠️ FIX CRITICO: Il certificato veniva creato da check_proposal_consensus()
        let certificate = self.check_proposal_consensus(&proposal.proposal_id).await?;
        if let Some(ref cert) = certificate {
            if cert.approved {
                info!(
                    proposal_id = %cert.proposal.proposal_id,
                    groups_count = cert.proposal.groups.len(),
                    "🔒 BFT FIX: Consensus reached during vote_on_proposal! Processing certificate immediately"
                );
                self.process_approval_certificate(cert.clone()).await?;

                self.on_group_approval_complete().await;
            }
        }

        Ok(vote)
    }

    /// Validate a group proposal.
    /// Checks: epoch, group sizes, lightnode availability, 80% uniqueness vs active groups,
    /// and PoU-weighted conflict resolution (higher avg PoU wins, hash tiebreaker).
    async fn validate_proposal(&self, proposal: &GroupProposal) -> Result<bool> {
        // Check epoch
        let current_epoch = self.refresh_epoch().await;
        if proposal.epoch != current_epoch {
            warn!(
                "Proposal epoch mismatch: expected {}, got {}",
                current_epoch, proposal.epoch
            );
            return Ok(false);
        }

        // Check group sizes
        for group in &proposal.groups {
            if group.members.len() < self.bft_config.min_group_size
                || group.members.len() > self.bft_config.max_group_size
            {
                warn!("Invalid group size: {}", group.members.len());
                return Ok(false);
            }
        }

        // Check if lightnodes are actually available (with tolerance for registration delays)
        let available = self.available_lightnodes.read().await;
        let total_used = proposal.used_lightnodes.len();
        let mut missing_count = 0usize;
        for node_id in &proposal.used_lightnodes {
            if !available.contains_key(node_id) {
                missing_count += 1;
            }
        }
        drop(available);

        if missing_count > 0 {
            let missing_ratio = missing_count as f64 / total_used.max(1) as f64;
            if missing_ratio > 0.10 {
                warn!(
                    missing = missing_count,
                    total = total_used,
                    "Too many lightnodes not available ({:.1}% > 10% threshold)",
                    missing_ratio * 100.0
                );
                return Ok(false);
            }
        }

        // UNIQUENESS CHECK: each proposed group must be ≥80% unique vs existing active groups.
        // Overlap = |intersection(proposed, existing)| / |proposed|. If overlap > 20%, reject.
        let active_groups = self.group_manager.get_active_groups().await;
        for proposed_group in &proposal.groups {
            let proposed_set: std::collections::HashSet<&String> =
                proposed_group.members.iter().collect();
            for existing_group in &active_groups {
                let existing_set: std::collections::HashSet<&String> =
                    existing_group.members.iter().collect();
                let overlap = proposed_set.intersection(&existing_set).count();
                let overlap_ratio = overlap as f64 / proposed_set.len().max(1) as f64;
                if overlap_ratio > 0.20 {
                    // Conflict: >20% overlap. Resolve by PoU score, then hash tiebreaker.
                    let proposed_pou = self.avg_pou_score(&proposed_group.members).await;
                    let existing_pou = self.avg_pou_score(&existing_group.members).await;

                    if proposed_pou > existing_pou {
                        info!(
                            proposed_group = %proposed_group.group_id,
                            existing_group = %existing_group.group_id,
                            overlap_pct = format!("{:.0}%", overlap_ratio * 100.0),
                            proposed_pou,
                            existing_pou,
                            "Group overlap >20% — proposed wins by PoU score, existing will be dissolved"
                        );
                        // Accept: the proposed group is better. The existing group
                        // will be replaced when add_active_groups runs.
                    } else if proposed_pou < existing_pou {
                        warn!(
                            proposed_group = %proposed_group.group_id,
                            existing_group = %existing_group.group_id,
                            overlap_pct = format!("{:.0}%", overlap_ratio * 100.0),
                            proposed_pou,
                            existing_pou,
                            "Group overlap >20% — existing wins by PoU score, rejecting proposal"
                        );
                        return Ok(false);
                    } else {
                        // PoU tie — use deterministic hash tiebreaker
                        use sha2::Digest;
                        let proposed_hash = sha2::Sha256::digest(
                            format!("{}|{}", proposed_group.group_id, proposal.timestamp)
                                .as_bytes(),
                        );
                        let existing_hash = sha2::Sha256::digest(
                            format!("{}|{}", existing_group.group_id, existing_group.created_at)
                                .as_bytes(),
                        );
                        if proposed_hash.as_slice() < existing_hash.as_slice() {
                            info!(
                                proposed_group = %proposed_group.group_id,
                                existing_group = %existing_group.group_id,
                                "Group overlap >20%, PoU tied — proposed wins by hash tiebreaker"
                            );
                        } else {
                            warn!(
                                proposed_group = %proposed_group.group_id,
                                existing_group = %existing_group.group_id,
                                "Group overlap >20%, PoU tied — existing wins by hash tiebreaker, rejecting"
                            );
                            return Ok(false);
                        }
                    }
                }
            }
        }

        Ok(true)
    }

    /// Compute average PoU score for a set of lightnode members.
    async fn avg_pou_score(&self, members: &[String]) -> f64 {
        if members.is_empty() {
            return 0.0;
        }
        let available = self.available_lightnodes.read().await;
        let total: f64 = members
            .iter()
            .filter_map(|m| available.get(m))
            .map(|info| info.pou_score)
            .sum();
        total / members.len() as f64
    }

    /// Check if consensus has been reached on a proposal
    ///
    /// CRITICAL FIX: This function used to hold READ locks on `proposal_votes` and
    /// `active_proposals` while later attempting WRITE locks on the same RwLocks,
    /// causing a permanent deadlock when consensus was reached.
    /// Fixed by cloning data during the READ phase, dropping locks, then doing WRITE ops.
    async fn check_proposal_consensus(
        &self,
        proposal_id: &str,
    ) -> Result<Option<GroupApprovalCertificate>> {
        let (prop, vote_list, approve_votes, total_votes) = {
            let votes = self.proposal_votes.read().await;
            let proposal = self.active_proposals.read().await;

            if let (Some(prop), Some(vote_list)) =
                (proposal.get(proposal_id), votes.get(proposal_id))
            {
                let approve_votes = vote_list
                    .iter()
                    .filter(|v| v.vote_type == GroupVoteType::Approve)
                    .count();
                let total_votes = vote_list.len();
                // Clone both so we can drop the read locks
                (prop.clone(), vote_list.clone(), approve_votes, total_votes)
            } else {
                return Ok(None);
            }
            // READ locks on proposal_votes and active_proposals are DROPPED here
        };

        let (required_votes, active_count) = match self.calculate_dynamic_quorum().await {
            Some(q) => q,
            None => {
                warn!(
                    proposal_id = %proposal_id,
                    "Cannot check consensus - not enough active masternodes"
                );
                return Ok(None);
            }
        };
        info!(
            proposal_id = %proposal_id,
            approve_votes = approve_votes,
            total_votes = total_votes,
            required_votes = required_votes,
            active_masternodes = active_count,
            "Checking group proposal consensus (dynamic quorum)"
        );

        if total_votes >= required_votes {
            let approve_ratio = approve_votes as f64 / total_votes as f64;
            let approved = approve_ratio >= self.bft_config.approval_threshold;

            if approved {
                info!(
                    proposal_id = %proposal_id,
                    approve_votes = approve_votes,
                    total_votes = total_votes,
                    approve_ratio = approve_ratio,
                    "Group proposal approved by BFT consensus"
                );

                // Create approval certificate with real masternode signatures
                let masternode_signatures = self
                    .collect_masternode_signatures(&prop, &vote_list)
                    .await?;

                let certificate = GroupApprovalCertificate {
                    proposal: prop.clone(),
                    votes: vote_list.clone(),
                    approved: true,
                    approval_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
                    masternode_signatures,
                };

                // Validate that we collected enough signatures
                let expected_signatures = approve_votes.min(required_votes);
                if certificate.masternode_signatures.len() < expected_signatures {
                    warn!(
                        proposal_id = %proposal_id,
                        collected_signatures = certificate.masternode_signatures.len(),
                        expected_signatures = expected_signatures,
                        approve_votes = approve_votes,
                        required_votes = required_votes,
                        "⚠️ WARNING: Certificate created with fewer signatures than expected votes!"
                    );
                }

                info!(
                    proposal_id = %proposal_id,
                    signatures = certificate.masternode_signatures.len(),
                    expected_signatures = expected_signatures,
                    approve_votes = approve_votes,
                    groups_count = certificate.proposal.groups.len(),
                    epoch = certificate.proposal.epoch,
                    "Group approval certificate created"
                );

                {
                    let mut certificates = self.approved_certificates.write().await;
                    certificates.insert(prop.epoch, certificate.clone());
                }

                info!(
                    proposal_id = %proposal_id,
                    "🔔 RACCOMANDAZIONE #1: Certificate stored, will be processed by caller"
                );

                // Note: Do NOT remove lightnodes from available pool permanently.
                // They need to remain available for group re-formation in future epochs.
                // The group_formation module tracks assignment status separately.

                // Clean up proposal and votes (no deadlock: read locks already dropped)
                {
                    let mut active = self.active_proposals.write().await;
                    active.remove(proposal_id);
                }
                {
                    let mut votes = self.proposal_votes.write().await;
                    votes.remove(proposal_id);
                }

                return Ok(Some(certificate));
            }
        } else {
            info!(
                proposal_id = %proposal_id,
                total_votes = total_votes,
                required_votes = required_votes,
                "Not enough votes yet to reach BFT decision"
            );

            // Cleanup expired proposals: if a proposal has been pending for more than
            // 2x the vote timeout without reaching quorum, remove it so a fresh
            // proposal can be created instead of blocking forever.
            let now_secs = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let expiry_secs = (self.bft_config.vote_timeout_ms as u64 / 1000) * 2;
            if now_secs.saturating_sub(prop.timestamp) > expiry_secs {
                info!(
                    proposal_id = %proposal_id,
                    age_secs = now_secs.saturating_sub(prop.timestamp),
                    expiry_secs = expiry_secs,
                    "Cleaning up expired proposal (no quorum reached in time)"
                );
                let mut active = self.active_proposals.write().await;
                active.remove(proposal_id);
                let mut votes = self.proposal_votes.write().await;
                votes.remove(proposal_id);
            }
        }

        Ok(None)
    }

    /// Process incoming proposal from another masternode
    pub async fn process_proposal(&self, proposal: GroupProposal) -> Result<GroupVote> {
        info!(
            proposal_id = %proposal.proposal_id,
            proposer = %proposal.proposer_masternode,
            groups_count = proposal.groups.len(),
            "Received group proposal from masternode"
        );

        // Store proposal
        {
            let mut active = self.active_proposals.write().await;
            active.insert(proposal.proposal_id.clone(), proposal.clone());
        }

        let mut should_approve = self.validate_proposal(&proposal).await?;
        if !should_approve {
            if let Err(e) = self.initialize_available_lightnodes().await {
                warn!(
                    "Failed to refresh available lightnodes before re-validation: {}",
                    e
                );
            } else {
                let retry = self.validate_proposal(&proposal).await?;
                if retry {
                    info!(
                        proposal_id = %proposal.proposal_id,
                        "Proposal validated after refreshing available lightnodes"
                    );
                    should_approve = true;
                }
            }
        }
        self.vote_on_proposal(&proposal, should_approve).await
    }

    /// Process incoming vote from another masternode
    pub async fn process_vote(&self, vote: GroupVote) -> Result<Option<GroupApprovalCertificate>> {
        info!(
            proposal_id = %vote.proposal_id,
            voter = %vote.voter_masternode,
            vote_type = ?vote.vote_type,
            "Received vote from masternode"
        );

        // Store vote
        {
            let proposal_id = vote.proposal_id.clone();
            let mut votes = self.proposal_votes.write().await;
            let proposal_votes = votes.entry(proposal_id.clone()).or_insert_with(Vec::new);

            // Check if we already have a vote from this masternode
            if !proposal_votes
                .iter()
                .any(|v| v.voter_masternode == vote.voter_masternode)
            {
                proposal_votes.push(vote.clone());
                info!(
                    proposal_id = %proposal_id,
                    total_votes = proposal_votes.len(),
                    "Stored received vote for proposal"
                );
            } else {
                info!(
                    proposal_id = %proposal_id,
                    voter = %vote.voter_masternode,
                    "Duplicate vote ignored"
                );
            }
        }

        // Check for consensus
        let result = self.check_proposal_consensus(&vote.proposal_id).await?;

        // If consensus reached and approved, remove used lightnodes from all masternodes
        if let Some(ref certificate) = result {
            if certificate.approved {
                info!(
                    proposal_id = %certificate.proposal.proposal_id,
                    lightnodes_count = certificate.proposal.used_lightnodes.len(),
                    "Group approved, removing lightnodes from available pool"
                );

                // This will be handled by the check_proposal_consensus method
                // which already removes the lightnodes from the local pool
                // Other masternodes will receive the certificate and remove them too
            }
        }

        Ok(result)
    }

    /// Process approved group certificate from another masternode
    pub async fn process_approval_certificate(
        &self,
        certificate: GroupApprovalCertificate,
    ) -> Result<()> {
        if !certificate.approved {
            debug!("Received rejected group certificate, ignoring");
            return Ok(());
        }

        // ENHANCED: Validate certificate has sufficient signatures
        let approve_votes_count = certificate
            .votes
            .iter()
            .filter(|v| v.vote_type == GroupVoteType::Approve)
            .count();

        if certificate.masternode_signatures.len() < approve_votes_count {
            warn!(
                proposal_id = %certificate.proposal.proposal_id,
                signatures_count = certificate.masternode_signatures.len(),
                approve_votes_count = approve_votes_count,
                "⚠️ WARNING: Certificate has fewer signatures than approve votes - may be invalid"
            );
        }

        info!(
            epoch = certificate.proposal.epoch,
            groups_count = certificate.proposal.groups.len(),
            proposal_id = %certificate.proposal.proposal_id,
            signatures_count = certificate.masternode_signatures.len(),
            approve_votes_count = approve_votes_count,
            "🔔 RACCOMANDAZIONE #1: Processing approved group certificate"
        );

        // Store certificate
        {
            let mut certificates = self.approved_certificates.write().await;
            certificates.insert(certificate.proposal.epoch, certificate.clone());
        }

        // Remove used lightnodes from available pool
        {
            let mut available = self.available_lightnodes.write().await;
            for node_id in &certificate.proposal.used_lightnodes {
                if available.remove(node_id).is_some() {
                    debug!("Removed lightnode {} from available pool", node_id);
                }
            }
        }

        // Add groups to active groups.
        //
        // BFT STATUS FIX: a BFT-approved certificate transitions the proposed
        // groups from `Forming` to `Active`. The proposal carries `Forming`
        // (set in form_groups when the leader created the group skeleton);
        // approval is the network-wide event that flips state to `Active`.
        // Without this explicit flip, the groups stayed `Forming` forever and
        // the local MN that aggregated the cert kept seeing its own
        // self-formed Forming groups (visible as the 16+ Forming-group
        // accumulation on mn-5).
        let mut approved_groups = certificate.proposal.groups.clone();
        for g in approved_groups.iter_mut() {
            g.status = crate::group_formation::GroupStatus::Active;
        }
        info!(
            "Adding {} groups to active groups (status flipped to Active by BFT approval)",
            approved_groups.len()
        );
        self.group_manager
            .add_active_groups(&approved_groups)
            .await?;
        info!("✅ Groups added to active groups");

        // CRITICAL: Distribute approved groups to lightnodes
        info!(
            groups_count = certificate.proposal.groups.len(),
            "🔔 RACCOMANDAZIONE #1: About to call distribute_groups_to_lightnodes"
        );
        self.distribute_groups_to_lightnodes(&certificate.proposal.groups)
            .await?;

        info!(
            groups_count = certificate.proposal.groups.len(),
            epoch = certificate.proposal.epoch,
            "✅ Processed approved group certificate and distributed to lightnodes"
        );
        Ok(())
    }

    /// Distribute approved groups to their lightnode members
    async fn distribute_groups_to_lightnodes(&self, groups: &[P2PGroup]) -> Result<()> {
        info!(
            groups_count = groups.len(),
            "🔔 RACCOMANDAZIONE #2: distribute_groups_to_lightnodes called - distributing approved groups to lightnodes"
        );

        if let Some(ref distributor) = self.p2p_distributor {
            info!("✅ P2P distributor is available, proceeding with distribution");
            // Use the P2P distributor to send groups to lightnodes
            let current_epoch = *self.current_epoch.read().await;
            info!(
                groups_count = groups.len(),
                epoch = current_epoch,
                "🔔 RACCOMANDAZIONE #2: Calling distributor.distribute_groups()"
            );
            if let Err(e) = distributor.distribute_groups(groups, current_epoch).await {
                error!(
                    error = %e,
                    groups_count = groups.len(),
                    "❌ Failed to distribute groups to lightnodes"
                );
                return Err(e);
            }

            info!(
                groups_count = groups.len(),
                epoch = current_epoch,
                "✅ Successfully distributed {} groups to lightnodes via P2P distributor",
                groups.len()
            );

            for group in groups {
                info!(
                    group_id = %group.group_id,
                    members_count = group.members.len(),
                    epoch = group.epoch,
                    "Distributed group to lightnode members"
                );
            }
        } else {
            warn!(
                groups_count = groups.len(),
                "❌ RACCOMANDAZIONE #3: No P2P distributor available - groups approved but not distributed to lightnodes"
            );

            // Fallback: log the groups that would be distributed
            for group in groups {
                warn!(
                    group_id = %group.group_id,
                    members_count = group.members.len(),
                    members = ?group.members,
                    epoch = group.epoch,
                    "⚠️ Group approved but P2P distribution not available - check p2p_distributor initialization"
                );
            }
        }

        Ok(())
    }

    /// Get approved certificate for an epoch
    pub async fn get_approved_certificate(&self, epoch: u64) -> Option<GroupApprovalCertificate> {
        let certificates = self.approved_certificates.read().await;
        certificates.get(&epoch).cloned()
    }

    /// Get available lightnodes count
    pub async fn get_available_lightnodes_count(&self) -> usize {
        let available = self.available_lightnodes.read().await;
        available.len()
    }

    /// Update current epoch
    pub async fn update_epoch(&self, new_epoch: u64) {
        let mut epoch = self.current_epoch.write().await;
        *epoch = new_epoch;
        debug!("Updated group consensus epoch to {}", new_epoch);
    }

    /// Collect masternode signatures for group approval certificate
    /// ENHANCED: Collects signatures from all approved votes, not just the proposer
    async fn collect_masternode_signatures(
        &self,
        proposal: &GroupProposal,
        vote_list: &[GroupVote],
    ) -> Result<Vec<[u8; 64]>> {
        use crate::signature_verifier::SignatureVerifier;

        let mut verifier = SignatureVerifier::new();
        let mut signatures = Vec::new();
        let mut verified_voters = std::collections::HashSet::new();

        // Create message hash for verification: proposal_id || epoch
        let mut message = Vec::new();
        message.extend_from_slice(proposal.proposal_id.as_bytes());
        message.extend_from_slice(&proposal.epoch.to_le_bytes());
        let mut hasher = sha2::Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();

        // ENHANCED: Collect signatures from all approved votes
        // First, add proposer signature if valid
        if let Some(pubkey) =
            self.get_masternode_pubkey(&proposal.proposer_masternode, proposal.epoch)
        {
            match verifier.verify_ed25519_signature(&message_hash, &pubkey, &proposal.signature) {
                Ok(true) => {
                    signatures.push(proposal.signature);
                    verified_voters.insert(proposal.proposer_masternode.clone());
                    debug!(
                        "✅ CONSENSUS | Verified proposer signature for: {}",
                        hex::encode(pubkey)
                    );
                }
                Ok(false) => {
                    warn!(
                        "⚠️ CONSENSUS | Invalid proposer signature for: {}",
                        hex::encode(pubkey)
                    );
                }
                Err(e) => {
                    warn!(
                        "⚠️ CONSENSUS | Signature verification error for {}: {}",
                        hex::encode(pubkey),
                        e
                    );
                }
            }
        }

        // ENHANCED: Collect signatures from all approved votes
        for vote in vote_list {
            // Only collect signatures from approved votes
            if vote.vote_type != GroupVoteType::Approve {
                continue;
            }

            // Skip if we already have this voter's signature
            if verified_voters.contains(&vote.voter_masternode) {
                continue;
            }

            // CRITICAL FIX: Recreate the exact message hash used when signing the vote
            // The vote is signed with: proposal_id || voter_masternode || vote_byte || timestamp
            // NOT with: proposal_id || epoch (which is used for proposal signature)
            let mut vote_message = Vec::new();
            vote_message.extend_from_slice(vote.proposal_id.as_bytes());
            vote_message.extend_from_slice(vote.voter_masternode.as_bytes());
            let vote_byte: u8 = match vote.vote_type {
                GroupVoteType::Approve => 1,
                GroupVoteType::Reject => 0,
            };
            vote_message.push(vote_byte);
            vote_message.extend_from_slice(&vote.timestamp.to_le_bytes());
            let mut vote_hasher = sha2::Sha256::new();
            vote_hasher.update(&vote_message);
            let vote_message_hash: [u8; 32] = vote_hasher.finalize().into();

            // CRITICAL FIX: Use proposal epoch for vote signature verification (same as signing)
            // The vote is signed with derive_signing_key(&vote.voter_masternode, proposal.epoch)
            if let Some(pubkey) = self.get_masternode_pubkey(&vote.voter_masternode, proposal.epoch)
            {
                // Verify the vote signature using the correct message hash
                match verifier.verify_ed25519_signature(
                    &vote_message_hash,
                    &pubkey,
                    &vote.signature,
                ) {
                    Ok(true) => {
                        signatures.push(vote.signature);
                        verified_voters.insert(vote.voter_masternode.clone());
                        debug!(
                            "✅ CONSENSUS | Verified vote signature for: {}",
                            hex::encode(pubkey)
                        );
                    }
                    Ok(false) => {
                        warn!(
                            "⚠️ CONSENSUS | Invalid vote signature for: {}",
                            hex::encode(pubkey)
                        );
                    }
                    Err(e) => {
                        warn!(
                            "⚠️ CONSENSUS | Vote signature verification error for {}: {}",
                            hex::encode(pubkey),
                            e
                        );
                    }
                }
            } else {
                warn!(
                    "⚠️ CONSENSUS | Could not derive pubkey for voter: {}",
                    vote.voter_masternode
                );
            }
        }

        info!(
            "✅ CONSENSUS | Collected {} valid masternode signatures (from {} approved votes)",
            signatures.len(),
            vote_list
                .iter()
                .filter(|v| v.vote_type == GroupVoteType::Approve)
                .count()
        );
        Ok(signatures)
    }

    /// Get masternode public key by ID
    fn get_masternode_pubkey(&self, masternode_id: &str, epoch: u64) -> Option<[u8; 32]> {
        // Deterministic key derivation for testnet
        let signing_key = Self::derive_signing_key(masternode_id, epoch);
        Some(signing_key.verifying_key().to_bytes())
    }

    /// Get masternode signature by ID
    fn get_masternode_signature(&self, masternode_id: &[u8; 32]) -> Option<[u8; 64]> {
        // In a real implementation, this would query the masternode signature store
        // For now, we'll use a deterministic approach based on the ID
        let mut hasher = sha2::Sha512::new();
        hasher.update(b"MASTERNODE-SIGNATURE");
        hasher.update(masternode_id);
        // Use try_read to avoid async - if lock is held, use a default epoch
        let epoch = self.current_epoch.try_read().map(|e| *e).unwrap_or(0);
        hasher.update(&epoch.to_le_bytes());
        let hash = hasher.finalize();

        let mut signature = [0u8; 64];
        signature.copy_from_slice(&hash);
        Some(signature)
    }

    fn derive_signing_key(masternode_id: &str, epoch: u64) -> SigningKey {
        let mut hasher = sha2::Sha512::new();
        hasher.update(b"MASTERNODE-KEY");
        hasher.update(masternode_id.as_bytes());
        hasher.update(&epoch.to_le_bytes());
        let hash = hasher.finalize();
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&hash[..32]);
        SigningKey::from_bytes(&seed)
    }

    fn sign_group_proposal(proposal: &GroupProposal, masternode_id: &str) -> [u8; 64] {
        let mut message = Vec::new();
        message.extend_from_slice(proposal.proposal_id.as_bytes());
        message.extend_from_slice(&proposal.epoch.to_le_bytes());
        let mut hasher = sha2::Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();
        let signing_key = Self::derive_signing_key(masternode_id, proposal.epoch);
        signing_key.sign(&message_hash).to_bytes()
    }

    fn sign_group_vote(vote: &GroupVote, epoch: u64) -> [u8; 64] {
        let mut message = Vec::new();
        message.extend_from_slice(vote.proposal_id.as_bytes());
        message.extend_from_slice(vote.voter_masternode.as_bytes());
        let vote_byte: u8 = match vote.vote_type {
            GroupVoteType::Approve => 1,
            GroupVoteType::Reject => 0,
        };
        message.push(vote_byte);
        message.extend_from_slice(&vote.timestamp.to_le_bytes());
        let mut hasher = sha2::Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();
        // CRITICAL FIX: Use the proposal epoch, not 0
        let signing_key = Self::derive_signing_key(&vote.voter_masternode, epoch);
        signing_key.sign(&message_hash).to_bytes()
    }

    // ========================================================================
    // Leader Election Protocol
    // ========================================================================
    //
    // Flow:
    // 1. A masternode initiates leader election by creating a LeaderElectionProposal
    // 2. The proposal is broadcast to all masternodes
    // 3. Each masternode verifies the proposal and sends a LeaderElectionCertificate
    // 4. After timeout or sufficient certificates, the winner is determined by
    //    earliest timestamp among proposals that received enough certifications
    // 5. Only the elected leader creates groups; others wait for the proposal
    // ========================================================================

    /// Get current leader election state
    pub async fn get_leader_election_state(&self) -> LeaderElectionState {
        self.leader_election_state.read().await.clone()
    }

    /// Check if this masternode is the elected leader
    pub async fn is_elected_leader(&self) -> bool {
        let state = self.leader_election_state.read().await;
        match &*state {
            LeaderElectionState::Elected {
                leader_masternode, ..
            }
            | LeaderElectionState::CreatingGroups {
                leader_masternode, ..
            } => leader_masternode == &self.local_masternode_id,
            _ => false,
        }
    }

    /// Initiate leader election: create a proposal and broadcast it
    /// Returns the proposal to be broadcast via P2P
    pub async fn initiate_leader_election(&self) -> Result<Option<LeaderElectionProposal>> {
        let state = self.leader_election_state.read().await;

        // Don't start a new election if one is already in progress or completed
        match &*state {
            LeaderElectionState::Collecting => {
                debug!("Leader election already in collecting phase");
                return Ok(None);
            }
            LeaderElectionState::Elected { .. } | LeaderElectionState::CreatingGroups { .. } => {
                debug!("Leader already elected, skipping new election");
                return Ok(None);
            }
            LeaderElectionState::Idle => {
                // Proceed with election
            }
        }
        drop(state);

        // After group formation, available_lightnodes is emptied. Without refresh,
        // the proposer would see 0 available and abort the election.
        if let Err(e) = self.initialize_available_lightnodes().await {
            warn!(
                "Failed to refresh available lightnodes for leader election: {}",
                e
            );
        }

        // Check if we have enough lightnodes
        let available = self.available_lightnodes.read().await;
        if available.len() < self.bft_config.min_group_size {
            info!(
                available = available.len(),
                required = self.bft_config.min_group_size,
                "LEADER ELECTION: not enough available lightnodes"
            );
            return Ok(None);
        }

        let available_ids: Vec<String> = available.keys().cloned().collect();
        let available_count = available.len();
        drop(available);

        let current_epoch = self.refresh_epoch().await;
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();

        let election_id = format!(
            "leader_election_{}_{}_{}",
            current_epoch, self.local_masternode_id, timestamp
        );

        let mut proposal = LeaderElectionProposal {
            election_id: election_id.clone(),
            proposer_masternode: self.local_masternode_id.clone(),
            timestamp,
            epoch: current_epoch,
            available_lightnode_count: available_count,
            available_lightnode_ids: available_ids,
            signature: [0u8; 64],
        };
        proposal.signature =
            Self::sign_leader_election_proposal(&proposal, &self.local_masternode_id);

        // Store our own proposal
        {
            let mut proposals = self.leader_proposals.write().await;
            proposals.insert(election_id.clone(), proposal.clone());
        }

        // Transition to collecting state
        {
            let mut state = self.leader_election_state.write().await;
            *state = LeaderElectionState::Collecting;
        }
        {
            let mut start_time = self.leader_election_start_time.write().await;
            *start_time = Some(timestamp);
        }

        // 🔒 BFT SELF-CERTIFICATE: Il proposer genera un auto-certificato.
        // il proposer partecipa al voto con la propria approvazione.
        // Without self-certificate, con N masternodes si ottengono max N-1 certificati,
        // rendendo il quorum fragile (richiede 100% degli altri).
        {
            let mut self_cert = LeaderElectionCertificate {
                election_id: election_id.clone(),
                epoch: current_epoch,
                approver_masternode: self.local_masternode_id.clone(),
                approval_timestamp: timestamp,
                lightnode_list_verified: true, // Il proposer ha creato la list, la check è implicita
                verified_lightnode_count: available_count,
                signature: [0u8; 64],
            };
            self_cert.signature =
                Self::sign_leader_election_certificate(&self_cert, &self.local_masternode_id);

            let mut certs = self.leader_certificates.write().await;
            let cert_list = certs.entry(election_id.clone()).or_insert_with(Vec::new);
            cert_list.push(self_cert);

            info!(
                election_id = %election_id,
                "🗳️ LEADER ELECTION: Self-certificate created (proposer votes for own proposal - standard BFT)"
            );
        }

        info!(
            election_id = %election_id,
            epoch = current_epoch,
            available_lightnodes = available_count,
            "🗳️ LEADER ELECTION: Initiated leader election proposal (with self-certificate)"
        );

        Ok(Some(proposal))
    }

    /// Process an incoming leader election proposal from another masternode
    /// Returns a certificate if the proposal is valid
    pub async fn process_leader_election_proposal(
        &self,
        proposal: LeaderElectionProposal,
    ) -> Result<Option<LeaderElectionCertificate>> {
        info!(
            election_id = %proposal.election_id,
            proposer = %proposal.proposer_masternode,
            timestamp = proposal.timestamp,
            lightnodes = proposal.available_lightnode_count,
            "🗳️ LEADER ELECTION: Received leader election proposal"
        );

        // Validate the proposal
        if !self.validate_leader_proposal(&proposal).await? {
            warn!(
                election_id = %proposal.election_id,
                "🗳️ LEADER ELECTION: Proposal validation failed"
            );
            return Ok(None);
        }

        // Store the proposal
        {
            let mut proposals = self.leader_proposals.write().await;
            proposals.insert(proposal.election_id.clone(), proposal.clone());
        }

        // Transition to collecting if we're idle
        {
            let mut state = self.leader_election_state.write().await;
            if *state == LeaderElectionState::Idle {
                *state = LeaderElectionState::Collecting;
                let mut start_time = self.leader_election_start_time.write().await;
                *start_time = Some(SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs());
            }
        }

        // After group formation, available_lightnodes is emptied (used nodes removed).
        // The proposer MN refreshes before proposing, but verifiers don't — causing
        // verified=0/N failures. This refresh re-populates from the persistent registry.
        if let Err(e) = self.initialize_available_lightnodes().await {
            warn!(
                "Failed to refresh available lightnodes for leader election verification: {}",
                e
            );
        }

        // Create certificate (approve the proposal)
        let local_available = self.available_lightnodes.read().await;
        let local_count = local_available.len();

        // Verify lightnode list overlap
        let mut verified_count = 0;
        for node_id in &proposal.available_lightnode_ids {
            if local_available.contains_key(node_id) {
                verified_count += 1;
            }
        }
        drop(local_available);

        // re-formation, MN peer discovery can lag significantly. With 50%, proposals with
        // verified=2/8 (25%) or verified=4/10 (40%) were rejected, blocking finalization.
        let list_verified = if proposal.available_lightnode_count > 0 {
            (verified_count as f64 / proposal.available_lightnode_count as f64) >= 0.25
        } else {
            false
        };

        if !list_verified {
            warn!(
                election_id = %proposal.election_id,
                verified = verified_count,
                proposed = proposal.available_lightnode_count,
                local_available = local_count,
                "🗳️ LEADER ELECTION: Lightnode list verification failed (< 25% overlap)"
            );
            return Ok(None);
        }

        let mut certificate = LeaderElectionCertificate {
            election_id: proposal.election_id.clone(),
            epoch: proposal.epoch,
            approver_masternode: self.local_masternode_id.clone(),
            approval_timestamp: SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs(),
            lightnode_list_verified: list_verified,
            verified_lightnode_count: verified_count,
            signature: [0u8; 64],
        };
        certificate.signature =
            Self::sign_leader_election_certificate(&certificate, &self.local_masternode_id);

        // Store our own certificate for this proposal
        {
            let mut certs = self.leader_certificates.write().await;
            let cert_list = certs
                .entry(proposal.election_id.clone())
                .or_insert_with(Vec::new);
            if !cert_list
                .iter()
                .any(|c| c.approver_masternode == self.local_masternode_id)
            {
                cert_list.push(certificate.clone());
            }
        }

        info!(
            election_id = %proposal.election_id,
            verified_lightnodes = verified_count,
            "🗳️ LEADER ELECTION: Approved proposal, sending certificate"
        );

        // Check if election can be resolved
        self.try_resolve_leader_election().await?;

        Ok(Some(certificate))
    }

    /// Process an incoming leader election certificate from another masternode
    pub async fn process_leader_election_certificate(
        &self,
        certificate: LeaderElectionCertificate,
    ) -> Result<()> {
        info!(
            election_id = %certificate.election_id,
            approver = %certificate.approver_masternode,
            verified = certificate.lightnode_list_verified,
            "🗳️ LEADER ELECTION: Received leader election certificate"
        );

        // Verify certificate signature before storing
        if !self
            .verify_leader_election_certificate_signature(&certificate)
            .await?
        {
            warn!(
                election_id = %certificate.election_id,
                approver = %certificate.approver_masternode,
                "🗳️ LEADER ELECTION: Certificate signature invalid"
            );
            return Ok(());
        }

        // Store the certificate
        {
            let mut certs = self.leader_certificates.write().await;
            let cert_list = certs
                .entry(certificate.election_id.clone())
                .or_insert_with(Vec::new);
            // Avoid duplicate certificates from same masternode
            if !cert_list
                .iter()
                .any(|c| c.approver_masternode == certificate.approver_masternode)
            {
                cert_list.push(certificate);
            }
        }

        // Try to resolve the election
        self.try_resolve_leader_election().await?;

        Ok(())
    }

    /// Try to resolve the leader election based on collected proposals and certificates
    async fn try_resolve_leader_election(&self) -> Result<()> {
        let state = self.leader_election_state.read().await;
        if *state != LeaderElectionState::Collecting {
            return Ok(());
        }
        drop(state);

        let proposals = self.leader_proposals.read().await;
        let certificates = self.leader_certificates.read().await;

        let (required_approvals, active_count) = match self.calculate_dynamic_quorum().await {
            Some(q) => q,
            None => {
                warn!("🗳️ LEADER ELECTION: Cannot resolve - not enough active masternodes for BFT");
                return Ok(());
            }
        };
        info!(
            proposals = proposals.len(),
            certificate_sets = certificates.len(),
            required = required_approvals,
            active_masternodes = active_count,
            "🗳️ LEADER ELECTION: resolve attempt (dynamic quorum)"
        );

        // Find proposals that have enough certificates
        let mut eligible_proposals: Vec<&LeaderElectionProposal> = Vec::new();

        for (election_id, proposal) in proposals.iter() {
            if let Some(cert_list) = certificates.get(election_id) {
                let valid_certs = cert_list
                    .iter()
                    .filter(|c| c.lightnode_list_verified)
                    .count();

                info!(
                    election_id = %election_id,
                    proposer = %proposal.proposer_masternode,
                    valid_certs = valid_certs,
                    required = required_approvals,
                    active_masternodes = active_count,
                    "🗳️ LEADER ELECTION: quorum check (dynamic)"
                );

                if valid_certs >= required_approvals {
                    eligible_proposals.push(proposal);
                    debug!(
                        election_id = %election_id,
                        certificates = valid_certs,
                        required = required_approvals,
                        "🗳️ LEADER ELECTION: Proposal has enough certificates"
                    );
                }
            }
        }

        if eligible_proposals.is_empty() {
            // Check if timeout has been reached
            let start_time = self.leader_election_start_time.read().await;
            if let Some(start) = *start_time {
                let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                if now - start > self.leader_election_timeout_secs {
                    warn!("🗳️ LEADER ELECTION: Timeout reached without enough certificates, resetting");
                    drop(start_time);
                    drop(proposals);
                    drop(certificates);
                    self.reset_leader_election().await;
                }
            }
            return Ok(());
        }

        // Select winner: earliest timestamp wins
        eligible_proposals.sort_by_key(|p| p.timestamp);
        let winner = eligible_proposals[0];

        let winner_id = winner.proposer_masternode.clone();
        let winner_election_id = winner.election_id.clone();

        drop(proposals);
        drop(certificates);

        // Transition to Elected state
        {
            let mut state = self.leader_election_state.write().await;
            *state = LeaderElectionState::Elected {
                leader_masternode: winner_id.clone(),
                election_id: winner_election_id.clone(),
            };
            info!(
                leader = %winner_id,
                election_id = %winner_election_id,
                local = %self.local_masternode_id,
                state_ptr = self.leader_election_state_ptr(),
                "LEADER ELECTION: state set to Elected"
            );
        }

        info!(
            leader = %winner_id,
            election_id = %winner_election_id,
            is_local = (winner_id == self.local_masternode_id),
            "🏆 LEADER ELECTION: Leader elected for group creation"
        );

        Ok(())
    }

    /// Validate a leader election proposal
    async fn validate_leader_proposal(&self, proposal: &LeaderElectionProposal) -> Result<bool> {
        // Check epoch matches
        let current_epoch = self.refresh_epoch().await;
        if proposal.epoch != current_epoch {
            warn!(
                "Leader proposal epoch mismatch: expected {}, got {}",
                current_epoch, proposal.epoch
            );
            return Ok(false);
        }

        // Check timestamp is recent (within 60 seconds)
        let now = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
        if now.saturating_sub(proposal.timestamp) > 60 {
            warn!(
                "Leader proposal timestamp too old: {} seconds ago",
                now - proposal.timestamp
            );
            return Ok(false);
        }

        // Check proposer is not empty
        if proposal.proposer_masternode.is_empty() {
            warn!("Leader proposal has empty proposer ID");
            return Ok(false);
        }

        // Check that proposal has at least min_group_size lightnodes
        if proposal.available_lightnode_count < self.bft_config.min_group_size {
            warn!(
                "Leader proposal has insufficient lightnodes: {} < {}",
                proposal.available_lightnode_count, self.bft_config.min_group_size
            );
            return Ok(false);
        }

        // Verify proposal signature
        if !self
            .verify_leader_election_proposal_signature(proposal)
            .await?
        {
            warn!(
                election_id = %proposal.election_id,
                proposer = %proposal.proposer_masternode,
                "Leader proposal signature invalid"
            );
            return Ok(false);
        }

        Ok(true)
    }

    fn leader_election_proposal_message(proposal: &LeaderElectionProposal) -> Vec<u8> {
        let mut message = Vec::new();
        message.extend_from_slice(proposal.election_id.as_bytes());
        message.extend_from_slice(proposal.proposer_masternode.as_bytes());
        message.extend_from_slice(&proposal.timestamp.to_le_bytes());
        message.extend_from_slice(&proposal.epoch.to_le_bytes());
        message.extend_from_slice(&(proposal.available_lightnode_count as u64).to_le_bytes());
        for id in &proposal.available_lightnode_ids {
            message.extend_from_slice(id.as_bytes());
        }
        message
    }

    fn leader_election_certificate_message(certificate: &LeaderElectionCertificate) -> Vec<u8> {
        let mut message = Vec::new();
        message.extend_from_slice(certificate.election_id.as_bytes());
        message.extend_from_slice(&certificate.epoch.to_le_bytes());
        message.extend_from_slice(certificate.approver_masternode.as_bytes());
        message.extend_from_slice(&certificate.approval_timestamp.to_le_bytes());
        message.push(if certificate.lightnode_list_verified {
            1
        } else {
            0
        });
        message.extend_from_slice(&(certificate.verified_lightnode_count as u64).to_le_bytes());
        message
    }

    fn sign_leader_election_proposal(
        proposal: &LeaderElectionProposal,
        masternode_id: &str,
    ) -> [u8; 64] {
        let message = Self::leader_election_proposal_message(proposal);
        let mut hasher = sha2::Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();
        let signing_key = Self::derive_signing_key(masternode_id, proposal.epoch);
        signing_key.sign(&message_hash).to_bytes()
    }

    fn sign_leader_election_certificate(
        certificate: &LeaderElectionCertificate,
        masternode_id: &str,
    ) -> [u8; 64] {
        let message = Self::leader_election_certificate_message(certificate);
        let mut hasher = sha2::Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();
        let signing_key = Self::derive_signing_key(masternode_id, certificate.epoch);
        signing_key.sign(&message_hash).to_bytes()
    }

    async fn verify_leader_election_proposal_signature(
        &self,
        proposal: &LeaderElectionProposal,
    ) -> Result<bool> {
        let message = Self::leader_election_proposal_message(proposal);
        let mut hasher = sha2::Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();
        let signing_key = Self::derive_signing_key(&proposal.proposer_masternode, proposal.epoch);
        let verifying_key = signing_key.verifying_key();
        let signature = Signature::from_bytes(&proposal.signature);
        Ok(verifying_key.verify(&message_hash, &signature).is_ok())
    }

    async fn verify_leader_election_certificate_signature(
        &self,
        certificate: &LeaderElectionCertificate,
    ) -> Result<bool> {
        let message = Self::leader_election_certificate_message(certificate);
        let mut hasher = sha2::Sha256::new();
        hasher.update(&message);
        let message_hash: [u8; 32] = hasher.finalize().into();
        let signing_key =
            Self::derive_signing_key(&certificate.approver_masternode, certificate.epoch);
        let verifying_key = signing_key.verifying_key();
        let signature = Signature::from_bytes(&certificate.signature);
        Ok(verifying_key.verify(&message_hash, &signature).is_ok())
    }

    /// Reset leader election state (e.g., after timeout or epoch change)
    pub async fn reset_leader_election(&self) {
        let state_ptr = self.leader_election_state_ptr();
        warn!(state_ptr, "LEADER ELECTION: Resetting state to Idle");
        {
            let mut state = self.leader_election_state.write().await;
            *state = LeaderElectionState::Idle;
        }
        {
            let mut proposals = self.leader_proposals.write().await;
            proposals.clear();
        }
        {
            let mut certs = self.leader_certificates.write().await;
            certs.clear();
        }
        {
            let mut start_time = self.leader_election_start_time.write().await;
            *start_time = None;
        }
        info!("🗳️ LEADER ELECTION: State reset to Idle");
    }

    /// Called by the elected leader to create groups with authority
    /// This replaces the old create_and_propose_groups for the leader election flow
    pub async fn leader_create_and_propose_groups(&self) -> Result<Option<GroupProposal>> {
        info!("LEADER: leader_create_and_propose_groups invoked");
        // Verify we are the elected leader
        let state = self.leader_election_state.read().await;
        match &*state {
            LeaderElectionState::Elected {
                leader_masternode,
                election_id,
            } => {
                if leader_masternode != &self.local_masternode_id {
                    warn!("🏆 LEADER: Not the elected leader, cannot create groups");
                    return Ok(None);
                }
                let election_id = election_id.clone();
                drop(state);

                // Transition to CreatingGroups
                {
                    let mut state = self.leader_election_state.write().await;
                    *state = LeaderElectionState::CreatingGroups {
                        leader_masternode: self.local_masternode_id.clone(),
                        election_id: election_id.clone(),
                    };
                }

                info!(
                    election_id = %election_id,
                    "🏆 LEADER: Elected leader creating groups"
                );

                // Use the existing group creation logic
                let result = self.create_and_propose_groups().await;

                // After group creation, reset election state for next round
                // (will happen after the proposal is approved via BFT)

                result
            }
            _ => {
                warn!(
                    "🏆 LEADER: Cannot create groups - not in Elected state (current: {:?})",
                    *state
                );
                Ok(None)
            }
        }
    }

    /// Reset leader election after successful group approval
    pub async fn on_group_approval_complete(&self) {
        info!(
            "🗳️ LEADER ELECTION: Group approval complete, resetting election state for next round"
        );
        self.reset_leader_election().await;
    }
}
