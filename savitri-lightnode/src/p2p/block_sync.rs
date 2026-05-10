//! Block Synchronization Protocol
//!
//! Implements pull-based block synchronization to handle gaps and stalls.
//! When a lightnode detects it's behind peers, it actively requests missing blocks.

use anyhow::{Context, Result};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::p2p::types::{RequestMessage, ResponseMessage};
use crate::tx::Block;

const SYNC_GAP_THRESHOLD: u64 = 5;
const DEFAULT_SYNC_BATCH_SIZE: usize = 500;

/// Block sync request message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSyncRequest {
    /// Starting height to request (inclusive)
    pub start_height: u64,
    /// Ending height to request (inclusive), None means up to current tip
    pub end_height: Option<u64>,
    /// Maximum number of blocks per response batch
    pub batch_size: usize,
    /// Requester's current tip height
    pub requester_height: u64,
}

/// Block sync response message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockSyncResponse {
    /// Requested blocks (may be empty if none available)
    pub blocks: Vec<Block>,
    /// Height of the responder's chain tip
    pub tip_height: u64,
    /// Whether this is the final batch for the requested range
    pub is_final: bool,
    /// If blocks empty, reason why
    pub reason: Option<String>,
}

/// Block sync manager
pub struct BlockSyncManager {
    /// Connected peers with their known heights
    peer_heights: HashMap<PeerId, u64>,
    /// Last sync attempt timestamp
    last_sync_attempt: Option<Instant>,
    /// Minimum sync interval
    min_sync_interval: Duration,
    /// Sync timeout
    sync_timeout: Duration,
    /// Maximum batch size
    max_batch_size: usize,
}

impl BlockSyncManager {
    pub fn new() -> Self {
        Self {
            peer_heights: HashMap::new(),
            last_sync_attempt: None,
            min_sync_interval: Duration::from_secs(30),
            sync_timeout: Duration::from_secs(60),
            max_batch_size: DEFAULT_SYNC_BATCH_SIZE,
        }
    }

    /// Update peer height information
    pub fn update_peer_height(&mut self, peer_id: PeerId, height: u64) {
        if height == u64::MAX {
            warn!(peer = %peer_id, "Ignoring invalid peer height u64::MAX");
            return;
        }
        let old_height = self.peer_heights.insert(peer_id, height);
        if old_height != Some(height) {
            debug!(peer = %peer_id, height, "Updated peer height");
        }
    }

    /// Remove peer from tracking
    pub fn remove_peer(&mut self, peer_id: &PeerId) {
        self.peer_heights.remove(peer_id);
    }

    /// Check if sync is needed and return request details
    pub fn check_sync_needed(&mut self, local_height: u64) -> Option<(PeerId, BlockSyncRequest)> {
        if local_height == u64::MAX {
            warn!("Ignoring sync check with invalid local height u64::MAX");
            return None;
        }

        // Check minimum interval
        if let Some(last) = self.last_sync_attempt {
            if last.elapsed() < self.min_sync_interval {
                return None;
            }
        }

        // Find best peer (highest height)
        let best_peer = self
            .peer_heights
            .iter()
            .max_by_key(|(_, &height)| height)
            .filter(|(_, &peer_height)| peer_height > local_height + SYNC_GAP_THRESHOLD); // Only sync if significantly behind

        if let Some((&peer_id, &peer_height)) = best_peer {
            self.last_sync_attempt = Some(Instant::now());

            let request = BlockSyncRequest {
                start_height: local_height + 1,
                end_height: Some(peer_height),
                batch_size: self.max_batch_size,
                requester_height: local_height,
            };

            info!(
                local_height,
                peer_height,
                peer = %peer_id,
                "Initiating block sync"
            );

            Some((peer_id, request))
        } else {
            None
        }
    }

    /// Build a probe request against a peer even when its height is unknown.
    /// Used when local node is stalled and needs to discover remote tip height.
    pub fn make_probe_request(
        &mut self,
        local_height: u64,
        peer_id: PeerId,
    ) -> Option<(PeerId, BlockSyncRequest)> {
        if local_height == u64::MAX {
            warn!("Ignoring sync probe with invalid local height u64::MAX");
            return None;
        }

        if let Some(last) = self.last_sync_attempt {
            if last.elapsed() < self.min_sync_interval {
                return None;
            }
        }

        self.last_sync_attempt = Some(Instant::now());
        let request = BlockSyncRequest {
            start_height: local_height.saturating_add(1),
            end_height: None,
            batch_size: self.max_batch_size,
            requester_height: local_height,
        };
        Some((peer_id, request))
    }

    /// Handle sync response and return next request if needed
    pub fn handle_sync_response(
        &mut self,
        response: BlockSyncResponse,
        request: BlockSyncRequest,
    ) -> Option<BlockSyncRequest> {
        if response.blocks.is_empty() {
            warn!(reason = ?response.reason, "Received empty sync response");
            return None;
        }

        info!(
            received_blocks = response.blocks.len(),
            tip_height = response.tip_height,
            "Received sync response"
        );

        // If we received blocks and there are more to get, create next request
        if !response.is_final && response.blocks.len() == request.batch_size {
            let next_start = response.blocks.last().unwrap().height + 1;
            Some(BlockSyncRequest {
                start_height: next_start,
                end_height: request.end_height,
                batch_size: request.batch_size,
                requester_height: request.requester_height,
            })
        } else {
            None
        }
    }
}

impl Default for BlockSyncManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Create block sync request message
pub fn create_block_sync_request(request: BlockSyncRequest) -> Result<RequestMessage> {
    let payload = bincode::serialize(&request).context("Failed to serialize block sync request")?;

    Ok(RequestMessage::Block(payload))
}

/// Parse block sync response message
pub fn parse_block_sync_response(response: ResponseMessage) -> Result<BlockSyncResponse> {
    match response {
        ResponseMessage::Block(payload) => {
            bincode::deserialize(&payload).context("Failed to deserialize block sync response")
        }
        other => anyhow::bail!("Unexpected response message for block sync: {:?}", other),
    }
}

/// Create block sync response message
pub fn create_block_sync_response(response: BlockSyncResponse) -> Result<ResponseMessage> {
    let payload =
        bincode::serialize(&response).context("Failed to serialize block sync response")?;

    Ok(ResponseMessage::Block(payload))
}
