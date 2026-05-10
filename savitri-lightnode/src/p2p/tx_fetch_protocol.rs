//! Announce-hash TX fetch protocol using libp2p request-response.
//!
//! Flow:
//! 1. RPC node accepts TX → stores hash→bytes in TxStore → gossipsub: HaveTx(hashes) [32 bytes each]
//! 2. All nodes receive HaveTx → record (hash, source_peer)
//! 3. Proposer at block time → sends TxFetchRequest(hashes) to source peers → receives full TX bytes
//!
//! This keeps gossipsub lightweight (hashes only) and uses direct P2P for full TX transfer.

use futures::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use libp2p::PeerId;
use libp2p::{request_response, StreamProtocol};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::RwLock;

/// Protocol identifier for TX fetch.
pub const TX_FETCH_PROTOCOL: &str = "/savitri/tx-fetch/1.0.0";

/// Maximum request size (32 bytes * 2000 hashes + overhead = ~70KB).
const MAX_REQUEST_SIZE: usize = 128 * 1024;

/// Maximum response size (300 bytes * 2000 TX + overhead = ~700KB).
const MAX_RESPONSE_SIZE: usize = 1024 * 1024;

/// Request: list of TX hashes to fetch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxFetchRequest {
    pub hashes: Vec<[u8; 32]>,
}

/// Response: TX bytes keyed by hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TxFetchResponse {
    /// Hash → raw TX bytes. Missing hashes are omitted.
    pub txs: Vec<([u8; 32], Vec<u8>)>,
}

/// Length-prefixed bincode codec for TX fetch request-response.
#[derive(Debug, Clone, Default)]
pub struct TxFetchCodec;

#[async_trait::async_trait]
impl request_response::Codec for TxFetchCodec {
    type Protocol = StreamProtocol;
    type Request = TxFetchRequest;
    type Response = TxFetchResponse;

    async fn read_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Request>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_REQUEST_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tx fetch request too large",
            ));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        bincode::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn read_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
    ) -> io::Result<Self::Response>
    where
        T: AsyncRead + Unpin + Send,
    {
        let mut len_buf = [0u8; 4];
        io.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len > MAX_RESPONSE_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "tx fetch response too large",
            ));
        }
        let mut buf = vec![0u8; len];
        io.read_exact(&mut buf).await?;
        bincode::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    async fn write_request<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        req: Self::Request,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data =
            bincode::serialize(&req).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        io.write_all(&(data.len() as u32).to_le_bytes()).await?;
        io.write_all(&data).await?;
        io.close().await
    }

    async fn write_response<T>(
        &mut self,
        _protocol: &Self::Protocol,
        io: &mut T,
        resp: Self::Response,
    ) -> io::Result<()>
    where
        T: AsyncWrite + Unpin + Send,
    {
        let data =
            bincode::serialize(&resp).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        io.write_all(&(data.len() as u32).to_le_bytes()).await?;
        io.write_all(&data).await?;
        io.close().await
    }
}

/// Shared TX byte store. The RPC consumer stores hash→bytes when a TX is accepted.
/// The network layer serves fetch requests from this store.
#[derive(Clone)]
pub struct TxStore {
    /// hash → raw TX bytes
    store: Arc<RwLock<HashMap<[u8; 32], Vec<u8>>>>,
    /// Announced TX hashes we know about: hash → list of peers that have the bytes.
    /// Starts as the original publisher, and grows as other peers fetch and
    /// re-announce. Enables load-balanced fetching across the mesh rather than
    /// funneling every request back to the single RPC ingress node.
    announced: Arc<RwLock<HashMap<[u8; 32], Vec<PeerId>>>>,
}

impl TxStore {
    pub fn new() -> Self {
        Self {
            store: Arc::new(RwLock::new(HashMap::with_capacity(10_000))),
            announced: Arc::new(RwLock::new(HashMap::with_capacity(10_000))),
        }
    }

    /// Store a TX (called by RPC consumer after mempool accepts it).
    /// Uses std::sync::RwLock — never blocks the tokio event loop.
    pub fn insert(&self, hash: [u8; 32], bytes: Vec<u8>) {
        if let Ok(mut store) = self.store.write() {
            store.insert(hash, bytes);
        }
    }

    /// Get TX bytes by hash (for serving fetch requests)
    pub fn get(&self, hash: &[u8; 32]) -> Option<Vec<u8>> {
        self.store.read().ok()?.get(hash).cloned()
    }

    /// Check whether we already have the TX bytes locally (for fetch dedup).
    pub fn contains(&self, hash: &[u8; 32]) -> bool {
        self.store
            .read()
            .map(|s| s.contains_key(hash))
            .unwrap_or(false)
    }

    /// Get multiple TX bytes (batch) — sync, safe in select! loop
    pub fn get_batch(&self, hashes: &[[u8; 32]]) -> Vec<([u8; 32], Vec<u8>)> {
        let store = match self.store.read() {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        hashes
            .iter()
            .filter_map(|h| store.get(h).map(|b| (*h, b.clone())))
            .collect()
    }

    /// Record an announced TX hash from a remote peer. Appends to the peer list
    /// for that hash (deduped) so multiple sources can serve the bytes.
    pub fn record_announcement(&self, hash: [u8; 32], source: PeerId) {
        if let Ok(mut announced) = self.announced.write() {
            let peers = announced
                .entry(hash)
                .or_insert_with(|| Vec::with_capacity(2));
            if !peers.contains(&source) {
                peers.push(source);
            }
        }
    }

    /// Record batch of announced TX hashes (deduped per hash).
    pub fn record_announcements(&self, hashes: &[[u8; 32]], source: PeerId) {
        if let Ok(mut announced) = self.announced.write() {
            for h in hashes {
                let peers = announced.entry(*h).or_insert_with(|| Vec::with_capacity(2));
                if !peers.contains(&source) {
                    peers.push(source);
                }
            }
        }
    }

    /// Pick a pseudo-random peer that has claimed to have the bytes for this
    /// hash, or None if the hash isn't announced. Used by the HaveTx handler
    /// to load-balance fetch requests across all known sources (original RPC
    /// publisher + any LN that fetched and re-announced the hash).
    pub fn pick_fetch_source(&self, hash: &[u8; 32]) -> Option<PeerId> {
        let announced = self.announced.read().ok()?;
        let peers = announced.get(hash)?;
        if peers.is_empty() {
            return None;
        }
        // Nanosecond-based pick — avoids pulling in rand dependency here and is
        // good enough for load balancing (we don't need crypto randomness).
        let idx = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.subsec_nanos() as usize)
            .unwrap_or(0)
            % peers.len();
        Some(peers[idx])
    }

    /// Get unfetched TX hashes with a chosen source peer each (for proposer to request)
    pub fn get_unfetched(&self, limit: usize) -> Vec<([u8; 32], PeerId)> {
        let store = match self.store.read() {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let announced = match self.announced.read() {
            Ok(a) => a,
            Err(_) => return Vec::new(),
        };
        announced
            .iter()
            .filter(|(h, _)| !store.contains_key(*h))
            .take(limit)
            .filter_map(|(h, peers)| peers.first().map(|p| (*h, *p)))
            .collect()
    }

    /// Mark hashes as fetched (remove from announced after successful fetch)
    pub fn mark_fetched(&self, hashes: &[[u8; 32]]) {
        if let Ok(mut announced) = self.announced.write() {
            for h in hashes {
                announced.remove(h);
            }
        }
    }

    /// Cleanup old entries (call periodically)
    pub fn cleanup(&self, max_entries: usize) {
        if let Ok(mut store) = self.store.write() {
            if store.len() > max_entries {
                let to_remove: Vec<[u8; 32]> =
                    store.keys().take(store.len() / 2).cloned().collect();
                for k in to_remove {
                    store.remove(&k);
                }
            }
        }
        if let Ok(mut announced) = self.announced.write() {
            if announced.len() > max_entries {
                let to_remove: Vec<[u8; 32]> = announced
                    .keys()
                    .take(announced.len() / 2)
                    .cloned()
                    .collect();
                for k in to_remove {
                    announced.remove(&k);
                }
            }
        }
    }

    /// Number of locally stored TX
    pub fn local_count(&self) -> usize {
        self.store.read().map(|s| s.len()).unwrap_or(0)
    }

    /// Number of announced but unfetched TX
    pub fn announced_count(&self) -> usize {
        self.announced.read().map(|a| a.len()).unwrap_or(0)
    }
}
