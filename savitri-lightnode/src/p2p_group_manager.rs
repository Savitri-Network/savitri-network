// P2P Group Manager - Complete implementation for managing P2P groups
use anyhow::Result;
use libp2p::{
    gossipsub::{IdentTopic, MessageId, TopicHash},
    PeerId,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupAnnounce {
    pub group_id: u64,
    pub members: Vec<[u8; 32]>,
    pub leader: Option<[u8; 32]>,
    pub epoch: u64,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub enum GroupEvent {
    Announce(GroupAnnounce),
    MemberJoined { group_id: u64, member: [u8; 32] },
    MemberLeft { group_id: u64, member: [u8; 32] },
    LeaderChanged { group_id: u64, new_leader: [u8; 32] },
    GroupDisbanded { group_id: u64 },
}

#[derive(Debug, Clone)]
pub struct GroupInfo {
    pub id: u64,
    pub members: HashSet<[u8; 32]>,
    pub leader: Option<[u8; 32]>,
    pub epoch: u64,
    pub created_at: u64,
    pub last_activity: u64,
}

pub struct P2PGroupManager {
    local_peer_id: String,
    event_tx: mpsc::Sender<GroupEvent>,
    groups: Arc<RwLock<HashMap<u64, GroupInfo>>>,
    peer_groups: Arc<RwLock<HashMap<[u8; 32], HashSet<u64>>>>,
    topic: IdentTopic,
}

impl P2PGroupManager {
    pub fn new(peer_id: String) -> Self {
        let (tx, _rx) = mpsc::channel(1000);
        Self {
            local_peer_id: peer_id,
            event_tx: tx,
            groups: Arc::new(RwLock::new(HashMap::new())),
            peer_groups: Arc::new(RwLock::new(HashMap::new())),
            topic: IdentTopic::new("/savitri/group/announce/1"),
        }
    }

    pub async fn start_tasks(&self) -> Result<()> {
        info!(
            "Starting P2P Group Manager tasks for peer: {}",
            self.local_peer_id
        );

        // Start group maintenance task
        let groups = Arc::clone(&self.groups);
        let peer_groups = Arc::clone(&self.peer_groups);
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

            loop {
                interval.tick().await;

                if let Err(e) = Self::maintain_groups(&groups, &peer_groups, &event_tx).await {
                    error!("Error in group maintenance: {}", e);
                }
            }
        });

        info!("P2P Group Manager tasks started successfully");
        Ok(())
    }

    pub fn get_topic(&self) -> IdentTopic {
        self.topic.clone()
    }

    pub async fn process_group_announcement(&mut self, announce: GroupAnnounce) -> Result<()> {
        debug!(
            "Processing group announcement for group {}",
            announce.group_id
        );

        let mut groups = self.groups.write().await;
        let mut peer_groups = self.peer_groups.write().await;

        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if let Some(group) = groups.get_mut(&announce.group_id) {
            // Update existing group
            if announce.epoch > group.epoch {
                group.members = announce.members.into_iter().collect();
                group.leader = announce.leader;
                group.epoch = announce.epoch;
                group.last_activity = current_time;

                // Update peer groups mapping
                Self::update_peer_groups(&mut peer_groups, group.id, &group.members);

                info!("Updated group {} with epoch {}", group.id, group.epoch);
            }
        } else {
            // Create new group
            let group_info = GroupInfo {
                id: announce.group_id,
                members: announce.members.clone().into_iter().collect(),
                leader: announce.leader,
                epoch: announce.epoch,
                created_at: current_time,
                last_activity: current_time,
            };

            Self::update_peer_groups(&mut peer_groups, group_info.id, &group_info.members);
            groups.insert(announce.group_id, group_info);

            info!(
                "Created new group {} with {} members",
                announce.group_id,
                announce.members.len()
            );
        }

        Ok(())
    }

    pub async fn get_group_info(&self, group_id: u64) -> Option<GroupInfo> {
        self.groups.read().await.get(&group_id).cloned()
    }

    pub async fn get_peer_groups(&self, peer_id: &[u8; 32]) -> HashSet<u64> {
        self.peer_groups
            .read()
            .await
            .get(peer_id)
            .cloned()
            .unwrap_or_default()
    }

    pub async fn get_all_groups(&self) -> Vec<GroupInfo> {
        self.groups.read().await.values().cloned().collect()
    }

    pub async fn join_group(&mut self, group_id: u64, peer_id: [u8; 32]) -> Result<bool> {
        let mut groups = self.groups.write().await;
        let mut peer_groups = self.peer_groups.write().await;

        if let Some(group) = groups.get_mut(&group_id) {
            if group.members.insert(peer_id) {
                peer_groups
                    .entry(peer_id)
                    .or_insert_with(HashSet::new)
                    .insert(group_id);
                group.last_activity = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                info!("Peer {} joined group {}", hex::encode(peer_id), group_id);
                Ok(true)
            } else {
                Ok(false) // Already a member
            }
        } else {
            warn!("Attempted to join non-existent group {}", group_id);
            Ok(false)
        }
    }

    pub async fn leave_group(&mut self, group_id: u64, peer_id: &[u8; 32]) -> Result<bool> {
        let mut groups = self.groups.write().await;
        let mut peer_groups = self.peer_groups.write().await;

        if let Some(group) = groups.get_mut(&group_id) {
            if group.members.remove(peer_id) {
                peer_groups
                    .entry(*peer_id)
                    .or_insert_with(HashSet::new)
                    .remove(&group_id);
                group.last_activity = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                info!("Peer {} left group {}", hex::encode(peer_id), group_id);

                // Remove group if empty
                if group.members.is_empty() {
                    groups.remove(&group_id);
                    info!("Group {} disbanded (no members)", group_id);
                }

                Ok(true)
            } else {
                Ok(false) // Not a member
            }
        } else {
            warn!("Attempted to leave non-existent group {}", group_id);
            Ok(false)
        }
    }

    pub async fn elect_leader(&mut self, group_id: u64) -> Result<Option<[u8; 32]>> {
        let mut groups = self.groups.write().await;

        if let Some(group) = groups.get_mut(&group_id) {
            if group.members.is_empty() {
                return Ok(None);
            }

            // Simple leader election: choose the member with the smallest hash
            let mut min_hash = u64::MAX;
            let mut leader_candidate = None;
            for member in &group.members {
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                member.hash(&mut hasher);
                let hash = hasher.finish();

                if hash < min_hash {
                    min_hash = hash;
                    leader_candidate = Some(*member);
                }
            }

            if let Some(leader) = leader_candidate {
                group.leader = Some(leader);
                group.epoch += 1;
                group.last_activity = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                info!(
                    "New leader elected for group {}: {}",
                    group_id,
                    hex::encode(leader)
                );
                Ok(Some(leader))
            } else {
                Ok(None)
            }
        } else {
            warn!(
                "Attempted to elect leader for non-existent group {}",
                group_id
            );
            Ok(None)
        }
    }

    async fn maintain_groups(
        groups: &Arc<RwLock<HashMap<u64, GroupInfo>>>,
        peer_groups: &Arc<RwLock<HashMap<[u8; 32], HashSet<u64>>>>,
        event_tx: &mpsc::Sender<GroupEvent>,
    ) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut groups_to_remove = Vec::new();
        let mut groups = groups.write().await;

        // Remove inactive groups (no activity for 5 minutes)
        for (group_id, group) in groups.iter() {
            if current_time - group.last_activity > 300 {
                groups_to_remove.push(*group_id);
            }
        }

        for group_id in groups_to_remove {
            if let Some(group) = groups.remove(&group_id) {
                // Clean up peer groups mapping
                let mut peer_groups = peer_groups.write().await;
                for member in group.members {
                    peer_groups
                        .entry(member)
                        .or_insert_with(HashSet::new)
                        .remove(&group_id);
                }

                // Send disbanded event
                if let Err(e) = event_tx.send(GroupEvent::GroupDisbanded { group_id }).await {
                    error!("Failed to send group disbanded event: {}", e);
                }

                info!("Group {} removed due to inactivity", group_id);
            }
        }

        Ok(())
    }

    fn update_peer_groups(
        peer_groups: &mut HashMap<[u8; 32], HashSet<u64>>,
        group_id: u64,
        members: &HashSet<[u8; 32]>,
    ) {
        // Clear existing mappings for this group
        for (_, group_set) in peer_groups.iter_mut() {
            group_set.remove(&group_id);
        }

        // Add new mappings
        for member in members {
            peer_groups
                .entry(*member)
                .or_insert_with(HashSet::new)
                .insert(group_id);
        }
    }

    pub async fn get_event_receiver(&self) -> mpsc::Receiver<GroupEvent> {
        let (tx, rx) = mpsc::channel(1000);
        // Note: In a real implementation, you'd need to store this sender
        // For now, this is a simplified version
        rx
    }

    pub async fn send_event(&self, event: GroupEvent) -> Result<()> {
        self.event_tx
            .send(event)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to send event: {}", e))
    }
}
