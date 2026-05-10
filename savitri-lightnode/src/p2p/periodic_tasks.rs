//! Periodic Tasks for Light Node Group Management
//!
//! This module handles periodic tasks like latency measurement,
//! PoU sharing, and proposer election.

#![allow(dead_code)]

use anyhow::Result;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tokio::time::interval;
use tracing::{info, warn};

use super::group_manager::P2PGroupManager;
use super::intra_group::IntraGroupCommunication;

/// Periodic task manager for group operations
pub struct PeriodicTaskManager {
    /// Local node ID
    local_node_id: String,
    /// Group manager
    group_manager: Arc<P2PGroupManager>,
    /// Intra-group communication
    intra_group_comm: Arc<RwLock<IntraGroupCommunication>>,
    /// Task intervals
    latency_interval: Duration,
    pou_interval: Duration,
    election_interval: Duration,
}

impl PeriodicTaskManager {
    pub fn new(
        local_node_id: String,
        group_manager: Arc<P2PGroupManager>,
        intra_group_comm: Arc<RwLock<IntraGroupCommunication>>,
    ) -> Self {
        Self {
            local_node_id,
            group_manager,
            intra_group_comm,
            latency_interval: Duration::from_secs(30), // Every 30 seconds
            pou_interval: Duration::from_secs(30), // ROUND 13: 60s→30s for faster candidate data
            election_interval: Duration::from_secs(120), // ROUND 13: 300s→120s for faster recovery
        }
    }

    /// Start all periodic tasks
    pub async fn start_tasks(&self) -> Result<()> {
        info!("Starting periodic group management tasks");

        // Start ping task - probes mesh readiness until we receive a Pong
        let ping_comm = self.intra_group_comm.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(3));
            loop {
                interval.tick().await;
                let comm = ping_comm.read().await;
                if comm.is_in_group().await && !comm.is_mesh_ready().await {
                    if let Err(e) = comm.send_group_ping().await {
                        warn!("Failed to send GroupPing: {}", e);
                    }
                }
            }
        });

        // Start latency measurement task
        let latency_comm = self.intra_group_comm.clone();
        let latency_interval = self.latency_interval;
        tokio::spawn(async move {
            let mut interval = interval(latency_interval);
            loop {
                interval.tick().await;

                // Only run if we're in a group
                if latency_comm.read().await.is_in_group().await {
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        latency_comm.read().await.start_latency_measurement(),
                    )
                    .await
                    {
                        Ok(Err(e)) => {
                            warn!("Failed to start latency measurement: {}", e);
                        }
                        Err(_) => {
                            warn!("Latency measurement timed out after 10s");
                        }
                        Ok(Ok(())) => {}
                    }
                }
            }
        });

        // Start PoU sharing task - only when mesh is ready
        let pou_comm = self.intra_group_comm.clone();
        let pou_interval = self.pou_interval;
        tokio::spawn(async move {
            let mut interval = interval(pou_interval);
            loop {
                interval.tick().await;

                let comm = pou_comm.read().await;
                // Only run if we're in a group and mesh is ready
                if comm.is_in_group().await && comm.is_mesh_ready().await {
                    if let Err(e) = comm.share_pou_score().await {
                        warn!("Failed to share PoU score with group: {}", e);
                        info!(
                            "PoU score sharing failed - group members may not see our reputation"
                        );
                        info!("This affects proposer election and group consensus");
                    }

                    // Also share PoU score with masternode
                    if let Err(e) = comm.share_pou_score_with_masternode().await {
                        warn!("Failed to share PoU score with masternode: {}", e);
                        info!("Masternode may not see our PoU reputation");
                    }
                }
            }
        });

        // Start proposer election task - only when mesh is ready
        let election_comm = self.intra_group_comm.clone();
        let election_interval = self.election_interval;
        tokio::spawn(async move {
            let mut interval = interval(election_interval);
            loop {
                interval.tick().await;

                let comm = election_comm.read().await;
                // Only run if we're in a group and mesh is ready
                if comm.is_in_group().await && comm.is_mesh_ready().await {
                    if let Err(e) = comm.start_proposer_election().await {
                        warn!("Failed to start proposer election: {}", e);
                        info!("Proposer election failed - group may not have a designated leader");
                        info!("This affects block production and consensus coordination");
                    }
                }
            }
        });

        info!("All periodic tasks started");
        Ok(())
    }

    /// Start periodic tasks with custom intervals
    pub async fn start_tasks_with_intervals(
        &self,
        latency_interval: Duration,
        pou_interval: Duration,
        election_interval: Duration,
    ) -> Result<()> {
        info!("Starting periodic tasks with custom intervals");

        // Start ping task
        let ping_comm = self.intra_group_comm.clone();
        tokio::spawn(async move {
            let mut interval = interval(Duration::from_secs(3));
            loop {
                interval.tick().await;
                let comm = ping_comm.read().await;
                if comm.is_in_group().await && !comm.is_mesh_ready().await {
                    if let Err(e) = comm.send_group_ping().await {
                        warn!("Failed to send GroupPing: {}", e);
                    }
                }
            }
        });

        // Start latency measurement task
        let latency_comm = self.intra_group_comm.clone();
        tokio::spawn(async move {
            let mut interval = interval(latency_interval);
            loop {
                interval.tick().await;

                if latency_comm.read().await.is_in_group().await {
                    match tokio::time::timeout(
                        Duration::from_secs(10),
                        latency_comm.read().await.start_latency_measurement(),
                    )
                    .await
                    {
                        Ok(Err(e)) => {
                            warn!("Failed to start latency measurement: {}", e);
                        }
                        Err(_) => {
                            warn!("Latency measurement timed out after 10s");
                        }
                        Ok(Ok(())) => {}
                    }
                }
            }
        });

        // Start PoU sharing task - only when mesh is ready
        let pou_comm = self.intra_group_comm.clone();
        tokio::spawn(async move {
            let mut interval = interval(pou_interval);
            loop {
                interval.tick().await;
                let comm = pou_comm.read().await;
                if comm.is_in_group().await && comm.is_mesh_ready().await {
                    if let Err(e) = comm.share_pou_score().await {
                        warn!("Failed to share PoU score with group: {}", e);
                        info!(
                            "PoU score sharing failed - group members may not see our reputation"
                        );
                        info!("This affects proposer election and group consensus");
                    }
                    if let Err(e) = comm.share_pou_score_with_masternode().await {
                        warn!("Failed to share PoU score with masternode: {}", e);
                        info!("Masternode may not see our PoU reputation");
                    }
                }
            }
        });

        // Start proposer election task - only when mesh is ready
        let election_comm = self.intra_group_comm.clone();
        tokio::spawn(async move {
            let mut interval = interval(election_interval);
            loop {
                interval.tick().await;
                let comm = election_comm.read().await;
                if comm.is_in_group().await && comm.is_mesh_ready().await {
                    if let Err(e) = comm.start_proposer_election().await {
                        warn!("Failed to start proposer election: {}", e);
                        info!("Proposer election failed - group may not have a designated leader");
                        info!("This affects block production and consensus coordination");
                    }
                }
            }
        });

        info!("Custom periodic tasks started");
        Ok(())
    }
}
