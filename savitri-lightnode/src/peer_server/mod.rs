use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Context};
use libp2p::{Multiaddr, PeerId};
#[cfg(feature = "metrics")]
use metrics::counter;
use tokio::sync::{broadcast, mpsc, watch, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::config::{Config, PeerServerConfig};
use crate::p2p::swarm_commands::{NetworkEvent, SwarmCommand};

pub mod address_publisher;
pub mod client;
pub mod selection;
pub mod wire;

use address_publisher::{
    build_public_rpc_endpoint, compute_publishable_addresses, AddressPublishOptions,
};
use client::{GetPeersQuery, PeerServerApi, PeerServerClient};
use selection::{select_candidates, DialSelectionState};
use wire::{NodePresence, UnregisterRequest};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedPeerServerUrl {
    pub value: String,
    pub source: &'static str,
}

#[derive(Debug, Clone)]
pub struct ResolvedPeerServerConfig {
    pub peer_server: PeerServerConfig,
    pub resolved_url: Option<ResolvedPeerServerUrl>,
}

#[derive(Debug, Clone)]
pub struct PeerServerRuntimeConfig {
    pub peer_server: PeerServerConfig,
    pub local_peer_id: PeerId,
    pub command_tx: mpsc::Sender<SwarmCommand>,
    pub network_events: broadcast::Sender<NetworkEvent>,
    pub listen_addrs: Arc<RwLock<Vec<Multiaddr>>>,
    pub observed_addr: Arc<RwLock<String>>,
    pub rpc_port: Option<u16>,
    pub rpc_bind_addr: Option<String>,
}

pub struct PeerServerTaskHandle {
    shutdown_tx: watch::Sender<bool>,
    join: JoinHandle<()>,
}

impl PeerServerTaskHandle {
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        let _ = self.join.await;
    }
}

#[derive(Debug)]
struct LoopBackoff {
    name: &'static str,
    consecutive_failures: u32,
    next_allowed_at: Instant,
    was_healthy: Option<bool>,
}

impl LoopBackoff {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            consecutive_failures: 0,
            next_allowed_at: Instant::now(),
            was_healthy: None,
        }
    }

    fn ready(&self, now: Instant) -> bool {
        now >= self.next_allowed_at
    }

    fn on_success(&mut self) {
        let recovered = self.was_healthy == Some(false);
        self.consecutive_failures = 0;
        self.next_allowed_at = Instant::now();
        self.was_healthy = Some(true);
        if recovered {
            info!(loop_name = self.name, "Peer server loop recovered");
        }
    }

    fn on_failure(&mut self, err: &anyhow::Error) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let backoff_secs = 2u64
            .saturating_pow(self.consecutive_failures.min(5))
            .min(60);
        self.next_allowed_at = Instant::now() + Duration::from_secs(backoff_secs);
        let state_changed = self.was_healthy != Some(false);
        self.was_healthy = Some(false);
        if state_changed {
            warn!(
                loop_name = self.name,
                backoff_secs,
                error = %err,
                "Peer server loop entered backoff"
            );
        } else {
            debug!(
                loop_name = self.name,
                backoff_secs,
                error = %err,
                "Peer server loop still failing"
            );
        }
    }
}

pub fn resolve_peer_server_url(
    cli_url: Option<&str>,
    env_url: Option<&str>,
    config: Option<&PeerServerConfig>,
) -> Option<ResolvedPeerServerUrl> {
    if let Some(url) = cli_url.and_then(normalize_url) {
        return Some(ResolvedPeerServerUrl {
            value: url,
            source: "cli",
        });
    }
    if let Some(url) = env_url.and_then(normalize_url) {
        return Some(ResolvedPeerServerUrl {
            value: url,
            source: "env",
        });
    }
    if let Some(url) = config
        .and_then(|cfg| cfg.base_url.as_deref())
        .and_then(normalize_url)
    {
        return Some(ResolvedPeerServerUrl {
            value: url,
            source: "config",
        });
    }
    None
}

fn normalize_url(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.trim_end_matches('/').to_string())
    }
}

pub fn resolve_runtime_config(
    file_cfg: Option<&Config>,
    cli_url: Option<String>,
) -> anyhow::Result<ResolvedPeerServerConfig> {
    let mut peer_server = file_cfg
        .map(|cfg| cfg.peer_server.clone())
        .unwrap_or_default();
    let env_url = std::env::var("PEER_SERVER_URL").ok();
    let resolved_url =
        resolve_peer_server_url(cli_url.as_deref(), env_url.as_deref(), Some(&peer_server));
    peer_server.base_url = resolved_url.as_ref().map(|url| url.value.clone());

    if peer_server.enabled && resolved_url.is_none() && !peer_server.allow_start_without_server {
        bail!("peer_server.enabled=true but no peer server URL was resolved");
    }

    Ok(ResolvedPeerServerConfig {
        peer_server,
        resolved_url,
    })
}

pub fn spawn(runtime: PeerServerRuntimeConfig) -> anyhow::Result<Option<PeerServerTaskHandle>> {
    if !runtime.peer_server.enabled {
        return Ok(None);
    }

    let Some(base_url) = runtime.peer_server.base_url.clone() else {
        if runtime.peer_server.allow_start_without_server {
            warn!(
                "Peer server enabled but no URL resolved; continuing without centralized discovery"
            );
            return Ok(None);
        }
        bail!("peer_server.enabled=true but no peer server base URL is available");
    };

    let api = Arc::new(PeerServerClient::new(
        base_url,
        Duration::from_secs(runtime.peer_server.request_timeout_secs.max(1)),
    )?);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let join = tokio::spawn(async move {
        let mut coordinator = PeerServerCoordinator::new(runtime, api);
        coordinator.run(shutdown_rx).await;
    });

    Ok(Some(PeerServerTaskHandle { shutdown_tx, join }))
}

struct PeerServerCoordinator<A: PeerServerApi> {
    runtime: PeerServerRuntimeConfig,
    api: Arc<A>,
    selection_state: DialSelectionState,
    heartbeat_backoff: LoopBackoff,
    fetch_backoff: LoopBackoff,
    registered: bool,
}

impl<A: PeerServerApi + 'static> PeerServerCoordinator<A> {
    fn new(runtime: PeerServerRuntimeConfig, api: Arc<A>) -> Self {
        Self {
            runtime,
            api,
            selection_state: DialSelectionState::new(
                Duration::from_secs(45),
                Duration::from_secs(60),
            ),
            heartbeat_backoff: LoopBackoff::new("heartbeat"),
            fetch_backoff: LoopBackoff::new("fetch"),
            registered: false,
        }
    }

    async fn run(&mut self, mut shutdown_rx: watch::Receiver<bool>) {
        let mut events = self.runtime.network_events.subscribe();
        let mut heartbeat_interval = tokio::time::interval(Duration::from_secs(
            self.runtime.peer_server.heartbeat_interval_secs.max(1),
        ));
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut fetch_interval = tokio::time::interval(Duration::from_secs(
            self.runtime.peer_server.get_peers_interval_secs.max(1),
        ));
        fetch_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        if self.runtime.peer_server.register_on_startup {
            match self.try_register().await {
                Ok(true) => {}
                Ok(false) => {}
                Err(err) => {
                    if self.runtime.peer_server.allow_start_without_server {
                        warn!(error = %err, "Peer server startup registration failed; continuing");
                    } else {
                        warn!(error = %err, "Peer server startup registration failed");
                    }
                }
            }
        }

        loop {
            tokio::select! {
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        if self.runtime.peer_server.auto_unregister_on_shutdown && self.registered {
                            if let Err(err) = self.unregister().await {
                                warn!(error = %err, "Peer server unregister failed during shutdown");
                            }
                        }
                        break;
                    }
                }
                event = events.recv() => {
                    self.handle_network_event(event);
                }
                _ = heartbeat_interval.tick() => {
                    if let Err(err) = self.on_heartbeat_tick().await {
                        warn!(error = %err, "Peer server heartbeat loop failed");
                    }
                }
                _ = fetch_interval.tick() => {
                    if let Err(err) = self.on_fetch_tick().await {
                        warn!(error = %err, "Peer server fetch loop failed");
                    }
                }
            }
        }
    }

    fn handle_network_event(&mut self, event: Result<NetworkEvent, broadcast::error::RecvError>) {
        match event {
            Ok(NetworkEvent::PeerConnected { peer_id, .. }) => {
                self.selection_state.mark_connected(peer_id);
            }
            Ok(NetworkEvent::PeerDisconnected { peer_id }) => {
                self.selection_state.mark_disconnected(&peer_id);
            }
            Ok(NetworkEvent::OutgoingConnectionError { peer_id, .. }) => {
                if let Some(peer_id) = peer_id {
                    self.selection_state.mark_failed(peer_id, Instant::now());
                }
            }
            Ok(NetworkEvent::NewListenAddr { .. }) => {}
            Ok(NetworkEvent::GossipMessage { .. }) => {}
            Ok(NetworkEvent::GossipSubscribed { .. }) => {}
            Ok(NetworkEvent::GroupMembersUpdated { .. }) => {}
            Ok(NetworkEvent::BlockSyncResponse { .. }) => {}
            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                debug!(
                    skipped,
                    "Peer server coordinator lagged on network event stream"
                );
            }
            Err(broadcast::error::RecvError::Closed) => {}
        }
    }

    async fn on_heartbeat_tick(&mut self) -> anyhow::Result<()> {
        let now = Instant::now();
        if !self.heartbeat_backoff.ready(now) {
            return Ok(());
        }

        if !self.registered {
            match self.try_register().await {
                Ok(true) => return Ok(()),
                Ok(false) => return Ok(()),
                Err(err) => {
                    self.heartbeat_backoff.on_failure(&err);
                    return Ok(());
                }
            }
        }

        let payload = self.build_presence_payload().await?;
        match self.api.heartbeat(&payload).await {
            Ok(()) => {
                #[cfg(feature = "metrics")]
                counter!("peer_server_heartbeat_success_total").increment(1);
                self.heartbeat_backoff.on_success();
            }
            Err(err) => {
                #[cfg(feature = "metrics")]
                counter!("peer_server_heartbeat_failure_total").increment(1);
                self.heartbeat_backoff.on_failure(&err);
            }
        }
        Ok(())
    }

    async fn on_fetch_tick(&mut self) -> anyhow::Result<()> {
        let now = Instant::now();
        if !self.fetch_backoff.ready(now) {
            return Ok(());
        }

        let peers = match self
            .api
            .get_peers(&GetPeersQuery {
                network_id: self.runtime.peer_server.network_id.clone(),
                exclude_peer_id: self.runtime.local_peer_id.to_string(),
                limit: self.runtime.peer_server.peer_request_limit,
                include_rpc_endpoints: self.runtime.peer_server.include_rpc_endpoints,
                roles: Vec::new(),
            })
            .await
        {
            Ok(peers) => {
                #[cfg(feature = "metrics")]
                {
                    counter!("peer_server_fetch_success_total").increment(1);
                    counter!("peer_server_peers_received_total").increment(peers.len() as u64);
                }
                self.fetch_backoff.on_success();
                peers
            }
            Err(err) => {
                #[cfg(feature = "metrics")]
                counter!("peer_server_fetch_failure_total").increment(1);
                self.fetch_backoff.on_failure(&err);
                return Ok(());
            }
        };

        let candidates = select_candidates(
            &peers,
            &mut self.selection_state,
            &self.runtime.local_peer_id,
            &self.runtime.peer_server.network_id,
            self.runtime.peer_server.peer_request_limit,
            now,
        );

        for candidate in candidates {
            let command = SwarmCommand::Dial {
                peer_id: candidate.peer_id,
                addresses: candidate.addresses,
            };
            if self.runtime.command_tx.send(command).await.is_ok() {
                self.selection_state
                    .mark_pending(candidate.peer_id, Instant::now());
                #[cfg(feature = "metrics")]
                counter!("peer_server_dials_attempted_total").increment(1);
            }
        }

        Ok(())
    }

    async fn try_register(&mut self) -> anyhow::Result<bool> {
        let payload = self.build_presence_payload().await?;
        match self.api.register(&payload).await {
            Ok(()) => {
                self.registered = true;
                self.heartbeat_backoff.on_success();
                #[cfg(feature = "metrics")]
                counter!("peer_server_register_success_total").increment(1);
                info!(
                    peer_id = %self.runtime.local_peer_id,
                    network_id = %self.runtime.peer_server.network_id,
                    "Peer server registration succeeded"
                );
                Ok(true)
            }
            Err(err) => {
                #[cfg(feature = "metrics")]
                counter!("peer_server_register_failure_total").increment(1);
                self.registered = false;
                Err(err)
            }
        }
    }

    async fn unregister(&self) -> anyhow::Result<()> {
        self.api
            .unregister(&UnregisterRequest {
                peer_id: self.runtime.local_peer_id.to_string(),
                network_id: self.runtime.peer_server.network_id.clone(),
            })
            .await
            .context("peer server unregister request failed")
    }

    async fn build_presence_payload(&self) -> anyhow::Result<NodePresence> {
        let listen_addrs = self.runtime.listen_addrs.read().await.clone();
        let observed_addr = {
            let current = self.runtime.observed_addr.read().await.clone();
            if current.trim().is_empty() {
                None
            } else {
                Some(
                    current
                        .parse::<Multiaddr>()
                        .with_context(|| format!("invalid observed multiaddr: {}", current))?,
                )
            }
        };
        let publish_options = AddressPublishOptions {
            publish_private_addresses: self.runtime.peer_server.publish_private_addresses,
            rpc_port: self.runtime.rpc_port,
            rpc_bind_addr: self.runtime.rpc_bind_addr.clone(),
        };
        let publish_addrs =
            compute_publishable_addresses(&listen_addrs, observed_addr.as_ref(), &publish_options);

        let mut roles = if self.runtime.peer_server.roles.is_empty() {
            vec!["lightnode".to_string()]
        } else {
            self.runtime.peer_server.roles.clone()
        };

        let rpc_endpoint = if self.runtime.peer_server.include_rpc_endpoints {
            let rpc = build_public_rpc_endpoint(&publish_addrs, &publish_options);
            if rpc.is_some() && !roles.iter().any(|role| role == "rpc") {
                roles.push("rpc".to_string());
            }
            rpc
        } else {
            None
        };

        Ok(NodePresence {
            peer_id: self.runtime.local_peer_id.to_string(),
            network_id: self.runtime.peer_server.network_id.clone(),
            listen_addresses: publish_addrs.iter().map(|addr| addr.to_string()).collect(),
            supported_protocols: supported_protocols(),
            roles,
            agent_version: Some(format!("savitri-lightnode/{}", env!("CARGO_PKG_VERSION"))),
            build_version: Some(env!("CARGO_PKG_VERSION").to_string()),
            rpc_endpoint,
        })
    }
}

fn supported_protocols() -> Vec<String> {
    vec![
        "/savitri/1.0.0".to_string(),
        crate::p2p::aux_protocol::AUX_PROTOCOL.to_string(),
        crate::p2p::consensus_protocol::CONSENSUS_PROTOCOL.to_string(),
        crate::p2p::tx_fetch_protocol::TX_FETCH_PROTOCOL.to_string(),
    ]
}
