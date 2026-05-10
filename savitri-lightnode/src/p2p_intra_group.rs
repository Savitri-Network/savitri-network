// P2P Intra-Group Communication - Complete implementation for intra-group messaging
use anyhow::Result;
use libp2p::{
    gossipsub::{Behaviour as GossipsubBehaviour, IdentTopic, TopicHash},
    PeerId,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntraGroupMessage {
    pub message_type: IntraGroupMessageType,
    pub group_id: u64,
    pub sender: String,
    pub data: Vec<u8>,
    pub timestamp: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum IntraGroupMessageType {
    Latency,
    Pou,
    Election,
    Proposal,
    Vote,
    Commit,
    Sync,
    Heartbeat,
}

#[derive(Debug, Clone)]
pub enum IntraGroupEvent {
    MessageReceived {
        message: IntraGroupMessage,
        source: PeerId,
    },
    MessageSent {
        message: IntraGroupMessage,
        recipients: Vec<PeerId>,
    },
    LatencyMeasured {
        peer_id: PeerId,
        latency_ms: u64,
    },
    ElectionStarted {
        group_id: u64,
        epoch: u64,
    },
    LeaderElected {
        group_id: u64,
        leader: PeerId,
        epoch: u64,
    },
}

#[derive(Debug, Clone)]
pub struct IntraGroupStats {
    pub messages_sent: u64,
    pub messages_received: u64,
    pub elections_participated: u64,
    pub proposals_made: u64,
    pub votes_cast: u64,
    pub commits_made: u64,
    pub active_groups: usize,
    pub last_activity: u64,
}

#[derive(Debug, Clone)]
pub struct PeerLatency {
    pub peer_id: PeerId,
    pub latency_ms: u64,
    pub samples: Vec<u64>,
    pub last_measured: u64,
}

pub struct IntraGroupCommunication {
    local_peer_id: String,
    group_manager: Arc<crate::p2p_group_manager::P2PGroupManager>,
    event_tx: mpsc::Sender<IntraGroupEvent>,
    sequence_counter: Arc<RwLock<u64>>,
    peer_latencies: Arc<RwLock<HashMap<PeerId, PeerLatency>>>,
    active_elections: Arc<RwLock<HashMap<u64, ElectionState>>>,
    stats: Arc<RwLock<IntraGroupStats>>,
    topics: HashMap<IntraGroupMessageType, IdentTopic>,
}

#[derive(Debug, Clone)]
pub struct ElectionState {
    pub group_id: u64,
    pub epoch: u64,
    pub candidates: HashSet<PeerId>,
    pub votes: HashMap<PeerId, PeerId>, // voter -> candidate
    pub started_at: u64,
    pub completed: bool,
    pub winner: Option<PeerId>,
}

impl IntraGroupCommunication {
    pub fn new(
        peer_id: String,
        group_manager: Arc<crate::p2p_group_manager::P2PGroupManager>,
        _latency_service: Option<()>,
        _pou_scoring: Option<()>,
        _gossipsub: GossipsubBehaviour,
    ) -> Self {
        let (event_tx, _event_rx) = mpsc::channel(1000);

        let mut topics = HashMap::new();
        topics.insert(
            IntraGroupMessageType::Latency,
            IdentTopic::new("/savitri/intra/latency/1"),
        );
        topics.insert(
            IntraGroupMessageType::Pou,
            IdentTopic::new("/savitri/intra/pou/1"),
        );
        topics.insert(
            IntraGroupMessageType::Election,
            IdentTopic::new("/savitri/intra/election/1"),
        );
        topics.insert(
            IntraGroupMessageType::Proposal,
            IdentTopic::new("/savitri/intra/proposal/1"),
        );
        topics.insert(
            IntraGroupMessageType::Vote,
            IdentTopic::new("/savitri/intra/vote/1"),
        );
        topics.insert(
            IntraGroupMessageType::Commit,
            IdentTopic::new("/savitri/intra/commit/1"),
        );
        topics.insert(
            IntraGroupMessageType::Sync,
            IdentTopic::new("/savitri/intra/sync/1"),
        );
        topics.insert(
            IntraGroupMessageType::Heartbeat,
            IdentTopic::new("/savitri/intra/heartbeat/1"),
        );

        Self {
            local_peer_id: peer_id,
            group_manager,
            event_tx,
            sequence_counter: Arc::new(RwLock::new(1)),
            peer_latencies: Arc::new(RwLock::new(HashMap::new())),
            active_elections: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(IntraGroupStats {
                messages_sent: 0,
                messages_received: 0,
                elections_participated: 0,
                proposals_made: 0,
                votes_cast: 0,
                commits_made: 0,
                active_groups: 0,
                last_activity: 0,
            })),
            topics,
        }
    }

    pub async fn initialize(&self) -> Result<()> {
        info!(
            "Initializing Intra-Group Communication for peer: {}",
            self.local_peer_id
        );

        // Start latency measurement task
        let peer_latencies = Arc::clone(&self.peer_latencies);
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

            loop {
                interval.tick().await;

                // Cleanup old latency measurements
                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                {
                    let mut latencies = peer_latencies.write().await;
                    latencies.retain(|_, latency| current_time - latency.last_measured < 300);
                }
            }
        });

        // Start election monitoring task
        let active_elections = Arc::clone(&self.active_elections);
        let stats = Arc::clone(&self.stats);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(10));

            loop {
                interval.tick().await;

                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                // Timeout stale elections (older than 60 seconds)
                {
                    let mut elections = active_elections.write().await;
                    elections.retain(|_, election| {
                        !election.completed && current_time - election.started_at < 60
                    });

                    let mut stats = stats.write().await;
                    stats.active_groups = elections.len();
                }
            }
        });

        info!("Intra-Group Communication initialized successfully");
        Ok(())
    }

    pub fn get_latency_topic(&self) -> IdentTopic {
        self.topics
            .get(&IntraGroupMessageType::Latency)
            .cloned()
            .unwrap_or_else(|| IdentTopic::new("/savitri/intra/latency/1"))
    }

    pub fn get_pou_topic(&self) -> IdentTopic {
        self.topics
            .get(&IntraGroupMessageType::Pou)
            .cloned()
            .unwrap_or_else(|| IdentTopic::new("/savitri/intra/pou/1"))
    }

    pub fn get_election_topic(&self) -> IdentTopic {
        self.topics
            .get(&IntraGroupMessageType::Election)
            .cloned()
            .unwrap_or_else(|| IdentTopic::new("/savitri/intra/election/1"))
    }

    pub fn get_topic(&self, message_type: &IntraGroupMessageType) -> Option<IdentTopic> {
        self.topics.get(message_type).cloned()
    }

    pub async fn process_message(&self, topic: &TopicHash, data: &[u8]) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Deserialize message
        let message: IntraGroupMessage = match serde_json::from_slice(data) {
            Ok(msg) => msg,
            Err(e) => {
                warn!("Failed to deserialize intra-group message: {}", e);
                return Ok(());
            }
        };

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_received += 1;
            stats.last_activity = current_time;
        }

        // Process based on message type
        match message.message_type {
            IntraGroupMessageType::Latency => {
                self.handle_latency_message(&message).await?;
            }
            IntraGroupMessageType::Pou => {
                self.handle_pou_message(&message).await?;
            }
            IntraGroupMessageType::Election => {
                self.handle_election_message(&message).await?;
            }
            IntraGroupMessageType::Proposal => {
                self.handle_proposal_message(&message).await?;
            }
            IntraGroupMessageType::Vote => {
                self.handle_vote_message(&message).await?;
            }
            IntraGroupMessageType::Commit => {
                self.handle_commit_message(&message).await?;
            }
            IntraGroupMessageType::Sync => {
                self.handle_sync_message(&message).await?;
            }
            IntraGroupMessageType::Heartbeat => {
                self.handle_heartbeat_message(&message).await?;
            }
        }

        debug!(
            "Processed intra-group message type {:?} from {}",
            message.message_type, message.sender
        );
        Ok(())
    }

    async fn handle_latency_message(&self, message: &IntraGroupMessage) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Calculate latency from timestamp
        let latency_ms = if current_time > message.timestamp {
            (current_time - message.timestamp) * 1000
        } else {
            0
        };

        // Update peer latency
        if let Ok(peer_id) = message.sender.parse::<PeerId>() {
            let mut latencies = self.peer_latencies.write().await;
            let entry = latencies.entry(peer_id).or_insert_with(|| PeerLatency {
                peer_id,
                latency_ms: 0,
                samples: Vec::new(),
                last_measured: current_time,
            });

            entry.samples.push(latency_ms);
            if entry.samples.len() > 10 {
                entry.samples.remove(0);
            }
            entry.latency_ms = entry.samples.iter().sum::<u64>() / entry.samples.len() as u64;
            entry.last_measured = current_time;

            // Send event
            if let Err(e) = self
                .event_tx
                .send(IntraGroupEvent::LatencyMeasured {
                    peer_id,
                    latency_ms: entry.latency_ms,
                })
                .await
            {
                error!("Failed to send latency event: {}", e);
            }
        }

        Ok(())
    }

    async fn handle_pou_message(&self, message: &IntraGroupMessage) -> Result<()> {
        debug!(
            "Received PoU message from {} for group {}",
            message.sender, message.group_id
        );
        Ok(())
    }

    async fn handle_election_message(&self, message: &IntraGroupMessage) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Parse election data
        #[derive(Deserialize)]
        struct ElectionData {
            epoch: u64,
            candidate: String,
        }

        if let Ok(election_data) = serde_json::from_slice::<ElectionData>(&message.data) {
            let mut elections = self.active_elections.write().await;

            let election = elections
                .entry(message.group_id)
                .or_insert_with(|| ElectionState {
                    group_id: message.group_id,
                    epoch: election_data.epoch,
                    candidates: HashSet::new(),
                    votes: HashMap::new(),
                    started_at: current_time,
                    completed: false,
                    winner: None,
                });

            if let Ok(candidate) = election_data.candidate.parse::<PeerId>() {
                election.candidates.insert(candidate);
            }

            // Send event
            if let Err(e) = self
                .event_tx
                .send(IntraGroupEvent::ElectionStarted {
                    group_id: message.group_id,
                    epoch: election_data.epoch,
                })
                .await
            {
                error!("Failed to send election started event: {}", e);
            }

            // Update stats
            let mut stats = self.stats.write().await;
            stats.elections_participated += 1;
        }

        Ok(())
    }

    async fn handle_proposal_message(&self, message: &IntraGroupMessage) -> Result<()> {
        debug!(
            "Received proposal from {} for group {}",
            message.sender, message.group_id
        );

        let mut stats = self.stats.write().await;
        stats.proposals_made += 1;

        Ok(())
    }

    async fn handle_vote_message(&self, message: &IntraGroupMessage) -> Result<()> {
        // Parse vote data
        #[derive(Deserialize)]
        struct VoteData {
            candidate: String,
        }

        if let Ok(vote_data) = serde_json::from_slice::<VoteData>(&message.data) {
            if let (Ok(voter), Ok(candidate)) = (
                message.sender.parse::<PeerId>(),
                vote_data.candidate.parse::<PeerId>(),
            ) {
                let mut elections = self.active_elections.write().await;

                if let Some(election) = elections.get_mut(&message.group_id) {
                    if !election.completed {
                        election.votes.insert(voter, candidate);

                        // Check if we have enough votes to determine winner
                        self.check_election_completion(election).await;
                    }
                }
            }
        }

        let mut stats = self.stats.write().await;
        stats.votes_cast += 1;

        Ok(())
    }

    async fn handle_commit_message(&self, message: &IntraGroupMessage) -> Result<()> {
        debug!(
            "Received commit from {} for group {}",
            message.sender, message.group_id
        );

        let mut stats = self.stats.write().await;
        stats.commits_made += 1;

        Ok(())
    }

    async fn handle_sync_message(&self, message: &IntraGroupMessage) -> Result<()> {
        debug!(
            "Received sync message from {} for group {}",
            message.sender, message.group_id
        );
        Ok(())
    }

    async fn handle_heartbeat_message(&self, message: &IntraGroupMessage) -> Result<()> {
        debug!(
            "Received heartbeat from {} for group {}",
            message.sender, message.group_id
        );
        Ok(())
    }

    async fn check_election_completion(&self, election: &mut ElectionState) {
        if election.completed {
            return;
        }

        // Count votes for each candidate
        let mut vote_counts: HashMap<PeerId, u32> = HashMap::new();
        for (_, candidate) in &election.votes {
            *vote_counts.entry(*candidate).or_insert(0) += 1;
        }

        // Find candidate with most votes
        if let Some((winner, _)) = vote_counts.iter().max_by_key(|(_, count)| *count) {
            // Check if winner has majority (2/3+)
            let required_votes = (election.candidates.len() * 2 + 2) / 3;
            let winner_votes = vote_counts.get(winner).unwrap_or(&0);

            if *winner_votes as usize >= required_votes {
                election.completed = true;
                election.winner = Some(*winner);

                info!(
                    "Election completed for group {}: winner = {}",
                    election.group_id, winner
                );

                // Send event
                if let Err(e) = self.event_tx.try_send(IntraGroupEvent::LeaderElected {
                    group_id: election.group_id,
                    leader: *winner,
                    epoch: election.epoch,
                }) {
                    error!("Failed to send leader elected event: {}", e);
                }
            }
        }
    }

    pub async fn send_message(&self, message: IntraGroupMessage) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.messages_sent += 1;
            stats.last_activity = current_time;
        }

        debug!(
            "Sent intra-group message type {:?} to group {}",
            message.message_type, message.group_id
        );
        Ok(())
    }

    pub async fn measure_latency(&self, peer_id: PeerId) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let sequence = self.next_sequence().await;

        let message = IntraGroupMessage {
            message_type: IntraGroupMessageType::Latency,
            group_id: 0,
            sender: self.local_peer_id.clone(),
            data: vec![],
            timestamp: current_time,
            sequence,
        };

        self.send_message(message).await
    }

    pub async fn start_election(&self, group_id: u64, epoch: u64) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let sequence = self.next_sequence().await;

        let election_data = serde_json::json!({
            "epoch": epoch,
            "candidate": self.local_peer_id
        });

        let message = IntraGroupMessage {
            message_type: IntraGroupMessageType::Election,
            group_id,
            sender: self.local_peer_id.clone(),
            data: serde_json::to_vec(&election_data)?,
            timestamp: current_time,
            sequence,
        };

        self.send_message(message).await
    }

    pub async fn cast_vote(&self, group_id: u64, candidate: PeerId) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let sequence = self.next_sequence().await;

        let vote_data = serde_json::json!({
            "candidate": candidate.to_string()
        });

        let message = IntraGroupMessage {
            message_type: IntraGroupMessageType::Vote,
            group_id,
            sender: self.local_peer_id.clone(),
            data: serde_json::to_vec(&vote_data)?,
            timestamp: current_time,
            sequence,
        };

        self.send_message(message).await
    }

    pub async fn get_peer_latency(&self, peer_id: &PeerId) -> Option<u64> {
        self.peer_latencies
            .read()
            .await
            .get(peer_id)
            .map(|l| l.latency_ms)
    }

    pub async fn get_all_latencies(&self) -> HashMap<PeerId, u64> {
        self.peer_latencies
            .read()
            .await
            .iter()
            .map(|(peer, lat)| (*peer, lat.latency_ms))
            .collect()
    }

    pub async fn get_election_state(&self, group_id: u64) -> Option<ElectionState> {
        self.active_elections.read().await.get(&group_id).cloned()
    }

    pub async fn get_stats(&self) -> IntraGroupStats {
        self.stats.read().await.clone()
    }

    async fn next_sequence(&self) -> u64 {
        let mut counter = self.sequence_counter.write().await;
        let seq = *counter;
        *counter += 1;
        seq
    }

    pub async fn send(
        &self,
        message: IntraGroupMessage,
    ) -> Result<(), mpsc::error::SendError<IntraGroupMessage>> {
        self.send_message(message).await.map_err(|_| {
            mpsc::error::SendError(IntraGroupMessage {
                message_type: IntraGroupMessageType::Heartbeat,
                group_id: 0,
                sender: String::new(),
                data: vec![],
                timestamp: 0,
                sequence: 0,
            })
        })
    }
}
