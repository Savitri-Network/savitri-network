use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;

use super::wire::{GetPeersResponse, NodePresence, PeerRecord, UnregisterRequest};

#[derive(Debug, Clone)]
pub struct PeerServerClient {
    base_url: String,
    client: reqwest::Client,
}

#[derive(Debug, Clone, Default)]
pub struct GetPeersQuery {
    pub network_id: String,
    pub exclude_peer_id: String,
    pub limit: usize,
    pub include_rpc_endpoints: bool,
    pub roles: Vec<String>,
}

#[async_trait]
pub trait PeerServerApi: Send + Sync {
    async fn register(&self, payload: &NodePresence) -> anyhow::Result<()>;
    async fn heartbeat(&self, payload: &NodePresence) -> anyhow::Result<()>;
    async fn unregister(&self, payload: &UnregisterRequest) -> anyhow::Result<()>;
    async fn get_peers(&self, query: &GetPeersQuery) -> anyhow::Result<Vec<PeerRecord>>;
}

impl PeerServerClient {
    pub fn new(base_url: impl Into<String>, request_timeout: Duration) -> anyhow::Result<Self> {
        let base_url = base_url.into().trim_end_matches('/').to_string();
        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .user_agent(format!("savitri-lightnode/{}", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build peer server HTTP client")?;
        Ok(Self { base_url, client })
    }

    fn endpoint(&self, path: &str) -> String {
        format!("{}/{}", self.base_url, path.trim_start_matches('/'))
    }
}

#[async_trait]
impl PeerServerApi for PeerServerClient {
    async fn register(&self, payload: &NodePresence) -> anyhow::Result<()> {
        self.client
            .post(self.endpoint("/v1/register"))
            .json(payload)
            .send()
            .await
            .context("register request failed")?
            .error_for_status()
            .context("register request returned non-success status")?;
        Ok(())
    }

    async fn heartbeat(&self, payload: &NodePresence) -> anyhow::Result<()> {
        self.client
            .post(self.endpoint("/v1/heartbeat"))
            .json(payload)
            .send()
            .await
            .context("heartbeat request failed")?
            .error_for_status()
            .context("heartbeat request returned non-success status")?;
        Ok(())
    }

    async fn unregister(&self, payload: &UnregisterRequest) -> anyhow::Result<()> {
        self.client
            .post(self.endpoint("/v1/unregister"))
            .json(payload)
            .send()
            .await
            .context("unregister request failed")?
            .error_for_status()
            .context("unregister request returned non-success status")?;
        Ok(())
    }

    async fn get_peers(&self, query: &GetPeersQuery) -> anyhow::Result<Vec<PeerRecord>> {
        let mut params: Vec<(&str, String)> = vec![
            ("network_id", query.network_id.clone()),
            ("exclude_peer_id", query.exclude_peer_id.clone()),
            ("limit", query.limit.to_string()),
            (
                "include_rpc_endpoints",
                query.include_rpc_endpoints.to_string(),
            ),
        ];
        for role in &query.roles {
            params.push(("role", role.clone()));
        }

        let response = self
            .client
            .get(self.endpoint("/v1/peers"))
            .query(&params)
            .send()
            .await
            .context("get_peers request failed")?
            .error_for_status()
            .context("get_peers request returned non-success status")?;
        let peers = response
            .json::<GetPeersResponse>()
            .await
            .context("failed to parse peer server peers response")?;
        Ok(peers.into_peers())
    }
}
