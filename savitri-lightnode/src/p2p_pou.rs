// P2P Proof of Uptime Receiver - Complete implementation for PoU management
use anyhow::Result;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

pub type PouScore = u16; // 0-1000 representing 0.0-1.0

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PouReport {
    pub peer_id: String,
    pub epoch: u64,
    pub uptime_seconds: u64,
    pub availability_score: PouScore,
    pub resource_score: PouScore,
    pub integrity_score: PouScore,
    pub total_score: PouScore,
    pub timestamp: u64,
    pub signature: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct PouEvent {
    pub event_type: PouEventType,
    pub data: Vec<u8>,
    pub source: Option<PeerId>,
    pub timestamp: u64,
}

#[derive(Debug, Clone)]
pub enum PouEventType {
    ReportReceived,
    ScoreUpdated,
    EpochChanged,
    LeaderCandidate,
    ValidationFailed,
}

#[derive(Debug, Clone)]
pub struct PeerPouState {
    pub peer_id: PeerId,
    pub current_score: PouScore,
    pub uptime_seconds: u64,
    pub last_report: u64,
    pub epoch: u64,
    pub reports_count: u32,
    pub average_score: f64,
    pub is_active: bool,
}

#[derive(Debug, Clone)]
pub struct PouStats {
    pub reports_received: u64,
    pub reports_validated: u64,
    pub reports_rejected: u64,
    pub active_peers: usize,
    pub current_epoch: u64,
    pub average_network_score: f64,
    pub last_report: u64,
}

pub struct PouReceiver {
    local_peer_id: PeerId,
    event_tx: mpsc::Sender<PouEvent>,
    peer_states: Arc<RwLock<HashMap<PeerId, PeerPouState>>>,
    current_epoch: Arc<RwLock<u64>>,
    stats: Arc<RwLock<PouStats>>,
    min_score_threshold: PouScore,
}

impl PouReceiver {
    pub fn new() -> (Self, mpsc::Receiver<PouEvent>) {
        let (tx, rx) = mpsc::channel(1000);
        let (event_tx, _event_rx) = mpsc::channel(1000);

        let receiver = Self {
            local_peer_id: PeerId::random(),
            event_tx,
            peer_states: Arc::new(RwLock::new(HashMap::new())),
            current_epoch: Arc::new(RwLock::new(0)),
            stats: Arc::new(RwLock::new(PouStats {
                reports_received: 0,
                reports_validated: 0,
                reports_rejected: 0,
                active_peers: 0,
                current_epoch: 0,
                average_network_score: 0.0,
                last_report: 0,
            })),
            min_score_threshold: 100, // Minimum score to be considered active
        };

        (receiver, rx)
    }

    pub fn with_local_peer_id(mut self, peer_id: PeerId) -> Self {
        self.local_peer_id = peer_id;
        self
    }

    pub fn with_min_score_threshold(mut self, threshold: PouScore) -> Self {
        self.min_score_threshold = threshold;
        self
    }

    pub async fn start_tasks(&self) -> Result<()> {
        info!("Starting PoU Receiver for peer: {}", self.local_peer_id);

        // Start epoch management task
        let current_epoch = Arc::clone(&self.current_epoch);
        let peer_states = Arc::clone(&self.peer_states);
        let stats = Arc::clone(&self.stats);
        let event_tx = self.event_tx.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

            loop {
                interval.tick().await;

                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                // Check for epoch change (every 10 minutes)
                let new_epoch = current_time / 600;
                let old_epoch = {
                    let epoch = current_epoch.read().await;
                    *epoch
                };

                if new_epoch > old_epoch {
                    {
                        let mut epoch = current_epoch.write().await;
                        *epoch = new_epoch;
                    }

                    {
                        let mut stats = stats.write().await;
                        stats.current_epoch = new_epoch;
                    }

                    // Send epoch change event
                    if let Err(e) = event_tx
                        .send(PouEvent {
                            event_type: PouEventType::EpochChanged,
                            data: new_epoch.to_le_bytes().to_vec(),
                            source: None,
                            timestamp: current_time,
                        })
                        .await
                    {
                        error!("Failed to send epoch change event: {}", e);
                    }

                    info!("PoU epoch changed: {} -> {}", old_epoch, new_epoch);
                }

                // Cleanup inactive peers (no report for 5 minutes)
                {
                    let mut states = peer_states.write().await;
                    for (_, state) in states.iter_mut() {
                        if current_time - state.last_report > 300 {
                            state.is_active = false;
                        }
                    }

                    // Update stats
                    let active_count = states.values().filter(|s| s.is_active).count();
                    let avg_score = if active_count > 0 {
                        states
                            .values()
                            .filter(|s| s.is_active)
                            .map(|s| s.current_score as f64)
                            .sum::<f64>()
                            / active_count as f64
                    } else {
                        0.0
                    };

                    let mut stats = stats.write().await;
                    stats.active_peers = active_count;
                    stats.average_network_score = avg_score;
                }
            }
        });

        info!("PoU Receiver tasks started successfully");
        Ok(())
    }

    pub async fn process_report(&self, report: PouReport, source: PeerId) -> Result<bool> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Validate report
        if !self.validate_report(&report).await {
            {
                let mut stats = self.stats.write().await;
                stats.reports_rejected += 1;
            }

            if let Err(e) = self
                .event_tx
                .send(PouEvent {
                    event_type: PouEventType::ValidationFailed,
                    data: serde_json::to_vec(&report).unwrap_or_default(),
                    source: Some(source),
                    timestamp: current_time,
                })
                .await
            {
                error!("Failed to send validation failed event: {}", e);
            }

            return Ok(false);
        }

        // Update peer state
        {
            let mut states = self.peer_states.write().await;
            let state = states.entry(source).or_insert_with(|| PeerPouState {
                peer_id: source,
                current_score: 0,
                uptime_seconds: 0,
                last_report: 0,
                epoch: 0,
                reports_count: 0,
                average_score: 0.0,
                is_active: true,
            });

            state.current_score = report.total_score;
            state.uptime_seconds = report.uptime_seconds;
            state.last_report = current_time;
            state.epoch = report.epoch;
            state.reports_count += 1;
            state.average_score = (state.average_score * (state.reports_count - 1) as f64
                + report.total_score as f64)
                / state.reports_count as f64;
            state.is_active = report.total_score >= self.min_score_threshold;
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.reports_received += 1;
            stats.reports_validated += 1;
            stats.last_report = current_time;
        }

        // Send events
        if let Err(e) = self
            .event_tx
            .send(PouEvent {
                event_type: PouEventType::ReportReceived,
                data: serde_json::to_vec(&report).unwrap_or_default(),
                source: Some(source),
                timestamp: current_time,
            })
            .await
        {
            error!("Failed to send report received event: {}", e);
        }

        if let Err(e) = self
            .event_tx
            .send(PouEvent {
                event_type: PouEventType::ScoreUpdated,
                data: report.total_score.to_le_bytes().to_vec(),
                source: Some(source),
                timestamp: current_time,
            })
            .await
        {
            error!("Failed to send score updated event: {}", e);
        }

        debug!(
            "Processed PoU report from {} with score {}",
            source, report.total_score
        );
        Ok(true)
    }

    async fn validate_report(&self, report: &PouReport) -> bool {
        if report.total_score > 1000 {
            warn!("Invalid PoU score: {} (max 1000)", report.total_score);
            return false;
        }

        if report.availability_score > 1000
            || report.resource_score > 1000
            || report.integrity_score > 1000
        {
            warn!("Invalid component score in PoU report");
            return false;
        }

        // Verify total score is reasonable
        let expected_total = (report.availability_score as u32
            + report.resource_score as u32
            + report.integrity_score as u32)
            / 3;
        let diff = (report.total_score as i32 - expected_total as i32).abs();
        if diff > 50 {
            warn!(
                "PoU total score doesn't match component scores: {} vs {}",
                report.total_score, expected_total
            );
            return false;
        }

        // Verify timestamp is reasonable
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if report.timestamp > current_time + 60
            || report.timestamp < current_time.saturating_sub(300)
        {
            warn!(
                "PoU report timestamp out of range: {} (current: {})",
                report.timestamp, current_time
            );
            return false;
        }

        true
    }

    pub async fn get_peer_score(&self, peer_id: &PeerId) -> Option<PouScore> {
        self.peer_states
            .read()
            .await
            .get(peer_id)
            .map(|s| s.current_score)
    }

    pub async fn get_peer_state(&self, peer_id: &PeerId) -> Option<PeerPouState> {
        self.peer_states.read().await.get(peer_id).cloned()
    }

    pub async fn get_active_peers(&self) -> Vec<PeerPouState> {
        self.peer_states
            .read()
            .await
            .values()
            .filter(|s| s.is_active)
            .cloned()
            .collect()
    }

    pub async fn get_top_peers(&self, limit: usize) -> Vec<PeerPouState> {
        let states = self.peer_states.read().await;
        let mut peers: Vec<_> = states.values().filter(|s| s.is_active).cloned().collect();
        peers.sort_by(|a, b| b.current_score.cmp(&a.current_score));
        peers.truncate(limit);
        peers
    }

    pub async fn get_current_epoch(&self) -> u64 {
        *self.current_epoch.read().await
    }

    pub async fn get_stats(&self) -> PouStats {
        self.stats.read().await.clone()
    }

    pub async fn send(&self, event: PouEvent) -> Result<(), mpsc::error::SendError<PouEvent>> {
        self.event_tx.send(event).await
    }

    pub async fn get_event_receiver(&self) -> mpsc::Receiver<PouEvent> {
        let (tx, rx) = mpsc::channel(1000);
        rx
    }
}
