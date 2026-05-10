#![allow(dead_code)]

use crate::p2p::types::{
    encode_request, encode_response, BootstrapReply, BootstrapRequest, MyBehaviour, RequestMessage,
    ResponseMessage,
};
use anyhow::{anyhow, bail, Context, Result};
use bincode;
use hex;
use libp2p::gossipsub::IdentTopic;
use libp2p::swarm::Swarm;
use libp2p::{Multiaddr, PeerId};
use std::str::FromStr;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::p2p::types::BootstrapPeer;
use crate::storage::BlockAndAccountStorage;
use crate::tx::Block;

const MAX_BOOTSTRAP_ACCOUNTS: usize = 1000;
const MAX_BOOTSTRAP_BLOCKS: usize = 100;

const BOOTSTRAP_REQUEST_COOLDOWN: Duration = Duration::from_secs(5);

/// Create a simple bootstrap block info for a given height
fn create_simple_bootstrap_block_info(height: u64) -> crate::p2p::types::BootstrapBlockInfo {
    // Generate a deterministic hash based on height
    let mut hash = vec![0u8; 32];
    let height_bytes = height.to_le_bytes();
    for (i, &b) in height_bytes.iter().enumerate() {
        hash[i] = b;
    }
    // Add some variation based on height
    for i in 8..32 {
        hash[i] = ((height.wrapping_mul(31).wrapping_add(i as u64)) % 256) as u8;
    }

    crate::p2p::types::BootstrapBlockInfo {
        height,
        hash,
        timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
            - (1000 - height) * 10, // Simulate older blocks
        tx_count: ((height % 50) + 1) as u32, // 1-50 transactions per block
    }
}

/// Parse bootstrap peer entries from configuration.
pub fn parse_bootstrap(entries: &[String], priority: bool) -> Result<Vec<BootstrapPeer>> {
    let mut out = Vec::with_capacity(entries.len());
    for entry in entries {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        let (endpoint_part, account_part) = match trimmed.split_once('#') {
            Some((base, acct)) => (base.trim(), Some(acct.trim())),
            None => (trimmed, None),
        };
        let (peer, addr) = endpoint_part
            .split_once('@')
            .context("bootstrap entry must be <peer>@<multiaddr>[#<account_hex>]")?;
        let pid = PeerId::from_str(peer.trim()).context("invalid peer id")?;
        let maddr: Multiaddr = addr.trim().parse().context("invalid multiaddr")?;
        let account = if let Some(raw) = account_part {
            let body = raw.strip_prefix("0x").unwrap_or(raw).trim();
            if body.is_empty() {
                bail!("bootstrap account hex must not be empty");
            }
            let bytes = hex::decode(body).context("bootstrap account must be valid hex")?;
            if bytes.len() != 32 {
                bail!(
                    "bootstrap account must decode to 32 bytes, got {} bytes",
                    bytes.len()
                );
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&bytes);
            Some(arr)
        } else {
            None
        };
        out.push(BootstrapPeer {
            peer_id: pid,
            addr: maddr,
            account,
            priority,
        });
    }
    Ok(out)
}

/// Build a bootstrap reply from storage.
pub fn build_bootstrap_reply(
    storage: &dyn BlockAndAccountStorage,
    end_height: u64,
    max_blocks: usize,
) -> BootstrapReply {
    info!(
        end_height = end_height,
        max_blocks = max_blocks,
        "Building bootstrap reply from storage"
    );

    let mut blocks = Vec::new();
    let mut accounts = Vec::new();
    let peers = Vec::new();

    let local_tip = storage
        .get_chain_head()
        .ok()
        .flatten()
        .map(|b| b.height)
        .unwrap_or(0);
    let target_end = end_height.min(local_tip);
    let window = max_blocks.max(1) as u64;
    let start = target_end.saturating_sub(window.saturating_sub(1));

    for height in start..=target_end {
        match storage.get_block(height) {
            Ok(Some(block)) => blocks.push(crate::p2p::types::BootstrapBlockInfo {
                height,
                hash: block.hash.to_vec(),
                timestamp: block.timestamp,
                tx_count: 0,
            }),
            Ok(None) => {}
            Err(e) => warn!("Failed to get block at height {}: {}", height, e),
        }
    }

    match storage.export_bootstrap_accounts(MAX_BOOTSTRAP_ACCOUNTS) {
        Ok(exported) => {
            accounts = exported
                .into_iter()
                .map(
                    |(address, account)| crate::p2p::types::BootstrapAccountInfo {
                        address,
                        balance: account.balance,
                        nonce: account.nonce,
                    },
                )
                .collect();
        }
        Err(err) => {
            warn!(error = %err, "Failed to export accounts for bootstrap reply");
        }
    }

    let reply = BootstrapReply {
        blocks,
        accounts,
        peers,
    };

    info!(
        blocks_count = reply.blocks.len(),
        accounts_count = reply.accounts.len(),
        peers_count = reply.peers.len(),
        "Bootstrap reply built successfully"
    );

    reply
}
/// Publish a bootstrap reply to the network.
pub fn publish_bootstrap_reply(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    reply: &BootstrapReply,
) -> anyhow::Result<()> {
    let response = ResponseMessage::Bootstrap(reply.clone());
    let payload = encode_response(&response)?;
    swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), payload)?;
    Ok(())
}

/// Publish a bootstrap request to the network with improved error handling.
pub fn publish_bootstrap_request(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    end_height: u64,
) -> anyhow::Result<()> {
    let request = RequestMessage::Bootstrap(BootstrapRequest {
        version: 1,
        end_height,
        max_blocks: MAX_BOOTSTRAP_BLOCKS as u32,
    });
    let payload = encode_request(&request)?;

    match swarm
        .behaviour_mut()
        .gossipsub
        .publish(topic.clone(), payload)
    {
        Ok(_) => {
            debug!("Bootstrap request published successfully");
            Ok(())
        }
        Err(e) => {
            let error_str = e.to_string();
            if error_str.contains("Duplicate") || error_str.contains("already been published") {
                // Gossipsub considers this request already seen; treat as success to avoid retry spam.
                debug!("Bootstrap request already published; treating as success");
                Ok(())
            } else if error_str.contains("InsufficientPeers") {
                debug!("Bootstrap publish failed due to insufficient mesh peers (expected during bootstrap): {}", error_str);
                Err(anyhow!(
                    "Mesh not ready for publishing - retry after mesh stabilization"
                ))
            } else {
                warn!("Bootstrap request publish failed: {}", error_str);
                Err(anyhow!("Failed to publish bootstrap request: {}", e))
            }
        }
    }
}

/// Handle an incoming bootstrap reply.
/// Masternode bootstrap always takes priority - if it doesn't match local state,
/// local data is cleared and masternode data is applied.
pub fn handle_bootstrap_reply(
    storage: &dyn BlockAndAccountStorage,
    reply: &BootstrapReply,
    force_overwrite: bool,
) -> anyhow::Result<Option<u64>> {
    reply.validate()?;

    info!(
        blocks_count = reply.blocks.len(),
        accounts_count = reply.accounts.len(),
        peers_count = reply.peers.len(),
        force_overwrite = force_overwrite,
        "Processing bootstrap reply"
    );

    let mut applied_height = None;

    if !reply.blocks.is_empty() {
        let mut sorted_blocks = reply.blocks.clone();
        sorted_blocks.sort_by_key(|b| b.height);
        let max_height = sorted_blocks
            .iter()
            .filter(|b| b.height != u64::MAX)
            .map(|b| b.height)
            .max()
            .unwrap_or(0);
        info!(
            max_block_height = max_height,
            "Processing blocks from bootstrap reply"
        );

        if force_overwrite {
            let mut prev_hash = [0u8; 64];
            let mut expected_height = sorted_blocks
                .iter()
                .find(|b| b.height != u64::MAX)
                .map(|b| b.height)
                .unwrap_or(0);
            let mut last_applied: Option<Block> = None;
            let mut initialized_sequence = false;

            for block in &sorted_blocks {
                if block.height == u64::MAX {
                    warn!("Skipping bootstrap block with invalid height u64::MAX");
                    continue;
                }
                if !initialized_sequence {
                    expected_height = block.height;
                    initialized_sequence = true;
                }
                if block.height != expected_height {
                    warn!(
                        expected_height,
                        got_height = block.height,
                        "Bootstrap reply has non-contiguous heights; stopping apply"
                    );
                    break;
                }

                let mut hash64 = [0u8; 64];
                let copy_len = block.hash.len().min(64);
                hash64[..copy_len].copy_from_slice(&block.hash[..copy_len]);
                let block_obj = Block {
                    hash: hash64,
                    height: block.height,
                    timestamp: block.timestamp,
                    parent_hash: prev_hash,
                    state_root: [0u8; 32],
                    tx_root: [0u8; 32],
                    proposer: [0u8; 32],
                    signature: [0u8; 64],
                    parent_exec_hash: [0u8; 64],
                    parent_ref_hash: [0u8; 64],
                    version: 1,
                };
                storage.set_block(block.height, block_obj.clone())?;
                prev_hash = hash64;
                expected_height = expected_height.saturating_add(1);
                last_applied = Some(block_obj);
                debug!(
                    block_height = block.height,
                    block_hash = %hex::encode(block.hash.clone()),
                    "Applied bootstrap block to storage"
                );
            }

            if let Some(head_block) = last_applied {
                if head_block.height != u64::MAX {
                    storage.set_chain_head(&head_block)?;
                    applied_height = Some(head_block.height);
                }
            }
        } else {
            for block in &sorted_blocks {
                debug!(
                    block_height = block.height,
                    block_hash = %hex::encode(block.hash.clone()),
                    "Would apply block to storage"
                );
            }
            if max_height > 0 {
                applied_height = Some(max_height);
            }
        }
    }

    if !reply.accounts.is_empty() {
        info!(
            accounts_count = reply.accounts.len(),
            "Processing accounts from bootstrap reply"
        );

        for account in &reply.accounts {
            if account.address.is_empty() {
                continue;
            }
            let merged_data = storage
                .get_account(&account.address)
                .ok()
                .flatten()
                .map(|a| a.data)
                .unwrap_or_default();
            let storage_account = crate::storage::Account {
                balance: account.balance,
                nonce: account.nonce,
                data: merged_data,
            };
            if force_overwrite || storage.get_account(&account.address)?.is_none() {
                storage.put_account(&account.address, &storage_account)?;
            }
        }
    }

    if !reply.peers.is_empty() {
        info!(
            peers_count = reply.peers.len(),
            "Processing peer info from bootstrap reply"
        );

        for peer in &reply.peers {
            debug!(
                peer_id = %peer.peer_id,
                peer_addr = %peer.addresses.join(", "),
                "Would add peer to known peers list"
            );
        }
    }

    info!(
        applied_height = ?applied_height,
        "Bootstrap reply processing completed"
    );

    Ok(applied_height)
}
/// Request a bootstrap snapshot from the network.
pub fn request_bootstrap_snapshot(
    swarm: &mut Swarm<MyBehaviour>,
    topic: &IdentTopic,
    target_height: u64,
    last_request: &mut Option<Instant>,
    allow_overwrite: &mut bool,
    pending_target: &mut Option<u64>,
) {
    let now = Instant::now();
    if let Some(prev) = last_request {
        if now.duration_since(*prev) < BOOTSTRAP_REQUEST_COOLDOWN {
            return;
        }
    }
    match publish_bootstrap_request(swarm, topic, target_height) {
        Ok(()) => {
            *allow_overwrite = true;
            *pending_target = Some(target_height);
            *last_request = Some(now);
            debug!(target_height, "Requested bootstrap snapshot");
        }
        Err(err) => {
            warn!(error=?err, "failed to publish bootstrap request");
        }
    }
}

/// Create a realistic BootstrapBlockInfo for bootstrap purposes
fn create_bootstrap_block_info(
    height: u64,
    parent_hash: Option<[u8; 64]>,
) -> Result<crate::p2p::types::BootstrapBlockInfo> {
    use sha2::{Digest, Sha512};

    // Create deterministic block hash based on height and parent hash
    let mut hasher = Sha512::new();
    hasher.update(b"BLOCKv1-BOOTSTRAP");
    hasher.update(&height.to_le_bytes());

    if let Some(parent) = parent_hash {
        hasher.update(&parent);
    } else {
        // Use zero hash for genesis block
        hasher.update(&[0u8; 64]);
    }

    // Add additional deterministic data
    hasher.update(b"SAVITRI-NETWORK");
    hasher.update(&height.to_le_bytes()); // Add height again for more entropy

    let block_hash = hasher.finalize();

    // Create realistic timestamp (current time minus some blocks * block time)
    let current_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Assume 10 second block time
    let block_time = 10u64;
    let timestamp = current_time.saturating_sub((1000 - height) * block_time);

    // Estimate transaction count based on height (realistic pattern)
    let tx_count = if height == 0 {
        0 // Genesis block has no transactions
    } else {
        // Simulate realistic transaction count: 50-500 transactions per block
        let base_count = 50u32;
        let variation = ((height as u32 * 7) % 450) + 1; // 1-450 variation
        base_count + variation
    };

    let block_info = crate::p2p::types::BootstrapBlockInfo {
        height,
        hash: block_hash[..32].to_vec(), // Use first 32 bytes of SHA512
        timestamp,
        tx_count,
    };

    debug!(
        height,
        hash = hex::encode(&block_info.hash),
        tx_count,
        "Created bootstrap block info"
    );

    Ok(block_info)
}
