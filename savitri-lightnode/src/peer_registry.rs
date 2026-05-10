//! Peer registry HTTP fetcher.
//!
//! Fetches the canonical list of masternodes (peer_id + multiaddrs) from a remote
//! JSON endpoint (e.g. https://peers.savitrinetwork.com/peers.json) so that nodes
//! can bootstrap without hard-coded bootstrap_peers in their TOML.
//!
//! The expected JSON schema matches `deploy/cloudflare/worker.js`:
//! ```json
//! {
//!   "version": 1,
//!   "updated": "2026-04-12T12:30:00Z",
//!   "network": "savitri-testnet-v0.1.0",
//!   "masternodes": [
//!     {"peer_id":"12D3KooW...","multiaddrs":["/ip4/.../tcp/5021","/ip4/.../udp/5021/quic-v1"]}
//!   ],
//!   "bootstrap_lightnodes": [...]
//! }
//! ```

use serde::Deserialize;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct PeerRegistry {
    #[serde(default)]
    pub version: u32,
    #[serde(default)]
    pub updated: String,
    #[serde(default)]
    pub network: String,
    #[serde(default)]
    pub masternodes: Vec<PeerEntry>,
    #[serde(default)]
    pub bootstrap_lightnodes: Vec<PeerEntry>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PeerEntry {
    pub peer_id: String,
    #[serde(default)]
    pub multiaddrs: Vec<String>,
}

impl PeerEntry {
    /// Format as the `<peer_id>@<multiaddr>` strings that bootstrap_peers expects.
    /// Returns one string per multiaddr.
    pub fn as_bootstrap_strings(&self) -> Vec<String> {
        self.multiaddrs
            .iter()
            .map(|m| format!("{}@{}", self.peer_id, m))
            .collect()
    }
}

/// Fetch peer registry from the given URL with a short timeout and one retry.
/// Returns parsed `PeerRegistry` or the network/parse error.
pub async fn fetch(url: &str) -> Result<PeerRegistry, anyhow::Error> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .user_agent("savitri-lightnode/0.1.0")
        .build()?;

    // Try twice with 1s gap; CDN edge cache should normally respond instantly.
    let mut last_err: Option<anyhow::Error> = None;
    for attempt in 1..=2 {
        match client.get(url).send().await {
            Ok(resp) => {
                if !resp.status().is_success() {
                    last_err = Some(anyhow::anyhow!("HTTP {}", resp.status()));
                } else {
                    match resp.json::<PeerRegistry>().await {
                        Ok(r) => return Ok(r),
                        Err(e) => last_err = Some(anyhow::anyhow!("parse error: {}", e)),
                    }
                }
            }
            Err(e) => last_err = Some(anyhow::anyhow!("request error: {}", e)),
        }
        if attempt == 1 {
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow::anyhow!("unknown fetch error")))
}

/// Seed `bootstrap_peers` and `masternode_peers` from a registry URL if they are
/// currently empty. No-op if they already have entries (explicit config wins).
///
/// Returns the number of masternode entries added (0 if skipped or failed).
pub async fn seed_from_url(
    url: &str,
    bootstrap_peers: &mut Vec<String>,
    masternode_peers: &mut Vec<String>,
) -> usize {
    match fetch(url).await {
        Ok(reg) => {
            let mn_strs: Vec<String> = reg
                .masternodes
                .iter()
                .flat_map(|m| m.as_bootstrap_strings())
                .collect();
            let ln_strs: Vec<String> = reg
                .bootstrap_lightnodes
                .iter()
                .flat_map(|m| m.as_bootstrap_strings())
                .collect();

            if bootstrap_peers.is_empty() {
                bootstrap_peers.extend(mn_strs.iter().cloned());
                bootstrap_peers.extend(ln_strs.iter().cloned());
            }
            if masternode_peers.is_empty() {
                masternode_peers.extend(mn_strs.iter().cloned());
            }

            tracing::info!(
                url = %url,
                network = %reg.network,
                updated = %reg.updated,
                mn_count = reg.masternodes.len(),
                ln_count = reg.bootstrap_lightnodes.len(),
                "Peer registry fetched and applied"
            );
            reg.masternodes.len()
        }
        Err(e) => {
            tracing::warn!(url = %url, error = %e, "Peer registry fetch failed — continuing with local bootstrap_peers");
            0
        }
    }
}
