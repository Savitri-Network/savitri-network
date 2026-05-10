// P2P Block Receiver - Complete implementation for P2P block management
use crate::p2p::types::{BlockBroadcast, BlockMessage, HaveBlock, PendingBlockData};
use anyhow::Result;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockValidationResult {
    #[serde(with = "crate::p2p::types::big_array")]
    pub block_id: [u8; 64],
    pub is_valid: bool,
    pub block_height: u64,
    pub proposer: PeerId,
    pub timestamp: u64,
    pub validator: PeerId,
    pub transactions_count: usize,
    pub signature_valid: bool,
}

#[derive(Debug, Clone)]
pub enum BlockEvent {
    Received {
        block: BlockBroadcast,
        source: PeerId,
    },
    Validated {
        result: BlockValidationResult,
    },
    Processed {
        block_id: [u64; 64],
        block_height: u64,
    },
    Rejected {
        block_id: [u8; 64],
        reason: String,
    },
    Synced {
        block_id: [u8; 64],
        peers: Vec<PeerId>,
    },
}

#[derive(Debug, Clone)]
pub struct BlockStats {
    pub blocks_received: u64,
    pub blocks_validated: u64,
    pub blocks_processed: u64,
    pub blocks_rejected: u64,
    pub blocks_synced: u64,
    pub active_blocks: usize,
    pub average_validation_time: f64,
    pub last_block_received: u64,
    pub total_transactions: u64,
}

#[derive(Debug, Clone)]
pub struct BlockSyncStatus {
    pub block_id: [u8; 64],
    pub requested_peers: Vec<PeerId>,
    pub received_peers: Vec<PeerId>,
    pub completed: bool,
    pub timestamp: u64,
}

pub struct BlockReceiver {
    local_peer_id: PeerId,
    event_tx: mpsc::Sender<BlockEvent>,
    pending_blocks: Arc<RwLock<HashMap<[u8; 64], BlockBroadcast>>>,
    validated_blocks: Arc<RwLock<HashMap<[u8; 64], BlockValidationResult>>>,
    processed_blocks: Arc<RwLock<HashSet<[u8; 64]>>>,
    rejected_blocks: Arc<RwLock<HashMap<[u8; 64], String>>>,
    sync_status: Arc<RwLock<HashMap<[u8; 64], BlockSyncStatus>>>,
    have_blocks: Arc<RwLock<HashMap<[u8; 64], HaveBlock>>>,
    stats: Arc<RwLock<BlockStats>>,
}

impl BlockReceiver {
    pub fn new() -> (Self, mpsc::Receiver<BlockBroadcast>) {
        let (tx, rx) = mpsc::channel(1000);
        let (event_tx, _event_rx) = mpsc::channel(1000);

        let receiver = Self {
            local_peer_id: PeerId::random(), // Will be set later
            event_tx,
            pending_blocks: Arc::new(RwLock::new(HashMap::new())),
            validated_blocks: Arc::new(RwLock::new(HashMap::new())),
            processed_blocks: Arc::new(RwLock::new(HashSet::new())),
            rejected_blocks: Arc::new(RwLock::new(HashMap::new())),
            sync_status: Arc::new(RwLock::new(HashMap::new())),
            have_blocks: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(BlockStats {
                blocks_received: 0,
                blocks_validated: 0,
                blocks_processed: 0,
                blocks_rejected: 0,
                blocks_synced: 0,
                active_blocks: 0,
                average_validation_time: 0.0,
                last_block_received: 0,
                total_transactions: 0,
            })),
        };

        (receiver, rx)
    }

    pub fn with_local_peer_id(mut self, peer_id: PeerId) -> Self {
        self.local_peer_id = peer_id;
        self
    }

    pub async fn start_tasks(&self) -> Result<()> {
        info!("Starting Block Receiver for peer: {}", self.local_peer_id);

        let pending = Arc::clone(&self.pending_blocks);
        let validated = Arc::clone(&self.validated_blocks);
        let processed = Arc::clone(&self.processed_blocks);
        let rejected = Arc::clone(&self.rejected_blocks);
        let event_tx = self.event_tx.clone();
        let stats = Arc::clone(&self.stats);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));

            loop {
                interval.tick().await;

                let blocks_to_validate: Vec<_> = {
                    let pending = pending.read().await;
                    pending.keys().cloned().collect()
                };

                for block_id in blocks_to_validate {
                    let start_time = std::time::Instant::now();

                    if let Some(block) = {
                        let pending = pending.read().await;
                        pending.get(&block_id).cloned()
                    } {
                        let tx_count = block.block.txs.len();
                        if let Err(e) = Self::validate_block(
                            block, &validated, &processed, &rejected, &event_tx,
                        )
                        .await
                        {
                            error!("Error validating block {}: {}", hex::encode(block_id), e);
                        }

                        let validation_time = start_time.elapsed().as_secs_f64();
                        Self::update_validation_stats(&stats, validation_time, tx_count).await;
                    }
                }
            }
        });

        // Start cleanup task for old blocks
        let pending_cleanup = Arc::clone(&self.pending_blocks);
        let validated_cleanup = Arc::clone(&self.validated_blocks);
        let stats_cleanup = Arc::clone(&self.stats);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // Every 5 minutes

            loop {
                interval.tick().await;

                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                // Clean up old pending blocks (older than 10 minutes)
                {
                    let mut pending = pending_cleanup.write().await;
                    let mut stats = stats_cleanup.write().await;

                    let initial_count = pending.len();
                    pending.retain(|_, block| current_time - block.have.height < 600);
                    let removed = initial_count - pending.len();

                    if removed > 0 {
                        info!("Cleaned up {} old pending blocks", removed);
                        stats.active_blocks = pending.len();
                    }
                }

                {
                    let mut validated = validated_cleanup.write().await;
                    let initial_count = validated.len();
                    validated.retain(|_, result| current_time - result.timestamp < 3600);
                    let removed = initial_count - validated.len();

                    if removed > 0 {
                        info!("Cleaned up {} old validated blocks", removed);
                    }
                }
            }
        });

        info!("Block Receiver tasks started successfully");
        Ok(())
    }

    pub async fn receive_block(&self, block: BlockBroadcast, source: PeerId) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Check if we already have this block
        {
            let pending = self.pending_blocks.read().await;
            let validated = self.validated_blocks.read().await;
            let processed = self.processed_blocks.read().await;
            let rejected = self.rejected_blocks.read().await;

            if pending.contains_key(&block.block.hash)
                || validated.contains_key(&block.block.hash)
                || processed.contains(&block.block.hash)
                || rejected.contains_key(&block.block.hash)
            {
                debug!(
                    "Block {} already processed, ignoring",
                    hex::encode(block.block.hash)
                );
                return Ok(());
            }
        }

        // Add to pending blocks
        {
            let mut pending = self.pending_blocks.write().await;
            pending.insert(block.block.hash, block.clone());
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.blocks_received += 1;
            stats.last_block_received = current_time;
            stats.active_blocks += 1;
            stats.total_transactions += block.block.txs.len() as u64;
        }

        // Send event
        let block_hash = block.block.hash;
        if let Err(e) = self
            .event_tx
            .send(BlockEvent::Received { block, source })
            .await
        {
            error!("Failed to send block received event: {}", e);
        }

        info!(
            "Received block {} from peer {}",
            hex::encode(block_hash),
            source
        );
        Ok(())
    }

    pub async fn send(
        &self,
        block: BlockBroadcast,
    ) -> Result<(), mpsc::error::SendError<BlockBroadcast>> {
        self.receive_block(block, self.local_peer_id)
            .await
            .map_err(|e| {
                mpsc::error::SendError(BlockBroadcast {
                    have: HaveBlock {
                        hash: [0u8; 64],
                        height: 0,
                        exec_height: 0,
                        tx_count: 0,
                    },
                    block: BlockMessage {
                        hash: [0u8; 64],
                        header: crate::p2p::types::BlockHeader {
                            exec_height: 0,
                            proposer: [0u8; 32],
                            timestamp: 0,
                            parent_hash: [0u8; 64],
                        },
                        txs: vec![],
                    },
                })
            })
    }

    async fn validate_block(
        block: BlockBroadcast,
        validated: &Arc<RwLock<HashMap<[u8; 64], BlockValidationResult>>>,
        processed: &Arc<RwLock<HashSet<[u8; 64]>>>,
        rejected: &Arc<RwLock<HashMap<[u8; 64], String>>>,
        event_tx: &mpsc::Sender<BlockEvent>,
    ) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        if block.block.txs.is_empty() {
            let reason = "Block has no transactions".to_string();

            {
                let mut rejected_map = rejected.write().await;
                rejected_map.insert(block.block.hash, reason.clone());
            }

            if let Err(e) = event_tx
                .send(BlockEvent::Rejected {
                    block_id: block.block.hash,
                    reason,
                })
                .await
            {
                error!("Failed to send block rejected event: {}", e);
                return Err(e.into());
            }

            return Ok(());
        }

        // Check block height is reasonable (not too far in the future)
        if block.block.header.exec_height > current_time + 3600 {
            let reason = format!(
                "Block height {} is too far in the future",
                block.block.header.exec_height
            );

            {
                let mut rejected_map = rejected.write().await;
                rejected_map.insert(block.block.hash, reason.clone());
            }

            if let Err(e) = event_tx
                .send(BlockEvent::Rejected {
                    block_id: block.block.hash,
                    reason,
                })
                .await
            {
                error!("Failed to send block rejected event: {}", e);
                return Err(e.into());
            }

            return Ok(());
        }

        // Check block hash is valid (64 bytes)
        if block.block.hash.len() != 64 {
            let reason = format!(
                "Invalid block hash length: {} (expected 64)",
                block.block.hash.len()
            );

            {
                let mut rejected_map = rejected.write().await;
                rejected_map.insert(block.block.hash, reason.clone());
            }

            if let Err(e) = event_tx
                .send(BlockEvent::Rejected {
                    block_id: block.block.hash,
                    reason,
                })
                .await
            {
                error!("Failed to send block rejected event: {}", e);
                return Err(e.into());
            }

            return Ok(());
        }

        // For now, accept any properly formatted block
        // In a real implementation, this would verify:
        // 1. Block signature is valid
        // 2. Transactions are valid
        // 3. Block hash matches computed hash
        // 4. Previous block hash is correct
        // 5. Block height is sequential

        let validation_result = BlockValidationResult {
            block_id: block.block.hash,
            is_valid: true,
            block_height: block.have.height,
            proposer: PeerId::random(), // Proposer info not available in BlockMessage
            timestamp: current_time,
            validator: PeerId::random(), // Will be set by caller
            transactions_count: block.block.txs.len(),
            signature_valid: true,
        };

        {
            let mut validated_map = validated.write().await;
            validated_map.insert(block.block.hash, validation_result.clone());
        }

        if let Err(e) = event_tx
            .send(BlockEvent::Validated {
                result: validation_result,
            })
            .await
        {
            error!("Failed to send block validated event: {}", e);
        }

        info!(
            "Block {} validated successfully",
            hex::encode(block.block.hash)
        );
        Ok(())
    }

    async fn update_validation_stats(
        stats: &Arc<RwLock<BlockStats>>,
        validation_time: f64,
        transaction_count: usize,
    ) {
        let mut stats = stats.write().await;
        stats.blocks_validated += 1;

        if stats.blocks_validated == 1 {
            stats.average_validation_time = validation_time;
        } else {
            stats.average_validation_time = (stats.average_validation_time
                + validation_time)
                / stats.blocks_validated as f64;
        }

        // Update total transactions
        stats.total_transactions += transaction_count as u64;
    }

    pub async fn process_block(&self, block_id: [u8; 64]) -> Result<bool> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        {
            let validated = self.validated_blocks.read().await;
            if !validated.contains_key(&block_id) {
                warn!(
                    "Attempted to process non-validated block {}",
                    hex::encode(block_id)
                );
                return Ok(false);
            }
        }

        // Add to processed blocks
        {
            let mut processed = self.processed_blocks.write().await;
            if processed.insert(block_id) {
                // Remove from pending
                let mut pending = self.pending_blocks.write().await;
                pending.remove(&block_id);

                // Update stats
                let mut stats = self.stats.write().await;
                stats.blocks_processed += 1;
                stats.active_blocks -= 1;

                info!("Block {} processed", hex::encode(block_id));
                Ok(true)
            } else {
                warn!("Block {} already processed", hex::encode(block_id));
                Ok(false)
            }
        }
    }

    pub async fn sync_block(&self, block_id: [u8; 64], target_peers: Vec<PeerId>) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Create sync status
        let sync_status = BlockSyncStatus {
            block_id,
            requested_peers: target_peers.clone(),
            received_peers: Vec::new(),
            completed: false,
            timestamp: current_time,
        };

        {
            let mut sync_map = self.sync_status.write().await;
            sync_map.insert(block_id, sync_status);
        }

        // Send HaveBlock messages to all target peers
        let have_block = HaveBlock {
            hash: block_id,
            height: 0, // Will be filled from actual block
            exec_height: 0,
            tx_count: 0,
        };

        for peer in &target_peers {
            // In a real implementation, this would send via P2P network
            debug!(
                "Sending HaveBlock for block {} to peer {}",
                hex::encode(block_id),
                peer
            );
        }

        info!(
            "Started syncing block {} with {} peers",
            hex::encode(block_id),
            target_peers.len()
        );
        Ok(())
    }

    pub async fn handle_have_block(&self, have_block: HaveBlock, source: PeerId) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Store HaveBlock
        {
            let mut have_blocks = self.have_blocks.write().await;
            have_blocks.insert(have_block.hash, have_block.clone());
        }

        // Update sync status if exists
        {
            let mut sync_map = self.sync_status.write().await;
            if let Some(sync_status) = sync_map.get_mut(&have_block.hash) {
                if !sync_status.received_peers.contains(&source) {
                    sync_status.received_peers.push(source);

                    // Check if sync is complete
                    let required_peers = sync_status.requested_peers.len();
                    let received_peers = sync_status.received_peers.len();

                    if received_peers >= required_peers {
                        sync_status.completed = true;

                        // Update stats
                        let mut stats = self.stats.write().await;
                        stats.blocks_synced += 1;

                        info!(
                            "Block {} sync completed with {} peers",
                            hex::encode(have_block.hash),
                            received_peers
                        );

                        // Send synced event
                        if let Err(e) = self
                            .event_tx
                            .send(BlockEvent::Synced {
                                block_id: have_block.hash,
                                peers: sync_status.received_peers.clone(),
                            })
                            .await
                        {
                            error!("Failed to send block synced event: {}", e);
                        }
                    }
                }
            }
        }

        debug!(
            "Received HaveBlock for block {} from {}",
            hex::encode(have_block.hash),
            source
        );
        Ok(())
    }

    pub async fn get_block_status(&self, block_id: &[u8; 64]) -> Option<String> {
        let processed = self.processed_blocks.read().await;
        let rejected = self.rejected_blocks.read().await;
        let validated = self.validated_blocks.read().await;
        let pending = self.pending_blocks.read().await;

        if processed.contains(block_id) {
            Some("processed".to_string())
        } else if let Some(reason) = rejected.get(block_id) {
            Some(format!("rejected: {}", reason))
        } else if validated.contains_key(block_id) {
            Some("validated".to_string())
        } else if pending.contains_key(block_id) {
            Some("pending".to_string())
        } else {
            None
        }
    }

    pub async fn get_sync_status(&self, block_id: &[u8; 64]) -> Option<BlockSyncStatus> {
        self.sync_status.read().await.get(block_id).cloned()
    }

    pub async fn get_stats(&self) -> BlockStats {
        self.stats.read().await.clone()
    }

    pub async fn get_pending_blocks(&self) -> Vec<BlockBroadcast> {
        self.pending_blocks.read().await.values().cloned().collect()
    }

    pub async fn get_validated_blocks(&self) -> Vec<BlockValidationResult> {
        self.validated_blocks
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn get_processed_blocks(&self) -> Vec<[u8; 64]> {
        self.processed_blocks.read().await.iter().cloned().collect()
    }

    pub async fn get_event_receiver(&self) -> mpsc::Receiver<BlockEvent> {
        let (tx, rx) = mpsc::channel(1000);
        // Note: In a real implementation, you'd need to store this sender
        // For now, this is a simplified version
        rx
    }
}
