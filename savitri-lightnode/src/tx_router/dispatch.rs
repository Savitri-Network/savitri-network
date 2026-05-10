//! Cross-group TX dispatcher (Tier 4 Fase 2 step 5).
//!
//! Two dispatch paths:
//!
//! 1. `forward_via_gossip(target_group, payload)` — publishes raw TX bytes
//!    to the target group's intra-group gossipsub topic
//!    `/savitri/group/{target}/tx`. Best-effort, mesh fan-out, no ACK.
//!    This is the legacy default path (current Tier 4 Fase 1 behaviour).
//!
//! 2. `forward_direct(peer, payload)` — sends `AuxMessage::TxForward`
//!    via libp2p request-response to the cached elected proposer of the
//!    target group, with a 100 ms ACK timeout. Lower latency, deterministic
//!    delivery for the hot path. Gated by `SAVITRI_TX_ROUTER_DIRECT_SEND=1`
//!
//! Wiring expectation: the dispatcher does NOT directly own the swarm
//! channel — it receives one as constructor argument. This keeps the
//! struct testable (mock channel) and decouples it from the libp2p layer.

use std::time::Duration;

use libp2p::PeerId;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::p2p::aux_protocol::AuxMessage;
use crate::p2p::swarm_commands::SwarmCommand;

use super::metrics::TxRoutingMetrics;

/// Default timeout for direct-send ACK. After this we record a fallback
/// counter and tell the caller to gossip-publish instead.
pub const DEFAULT_DIRECT_SEND_TIMEOUT: Duration = Duration::from_millis(100);

/// Outcome of `forward_direct`. Mirrors the decision tree in `route()`
/// for the cross-group hot path.
#[derive(Debug, Clone)]
pub enum DispatchOutcome {
    /// Direct send queued successfully — caller treats as `Forwarded`.
    DirectQueued,
    /// Channel full / closed — caller falls back to gossipsub.
    DirectChannelUnavailable { reason: &'static str },
}

/// Sends raw TX bytes either via gossipsub fan-out (legacy) or via direct
/// request-response to the cached proposer (new in Tier 4 Fase 2).
#[derive(Clone)]
pub struct Dispatcher {
    swarm_tx: mpsc::Sender<SwarmCommand>,
    /// Reads `SAVITRI_TX_ROUTER_DIRECT_SEND` at construction time.
    /// Default is `false` so this commit is purely additive — existing
    /// gossip path remains the only active code path until the env is
    /// flipped on after the 24 h soak.
    direct_send_enabled: bool,
}

impl Dispatcher {
    pub fn new(swarm_tx: mpsc::Sender<SwarmCommand>) -> Self {
        let direct_send_enabled = std::env::var("SAVITRI_TX_ROUTER_DIRECT_SEND")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        if direct_send_enabled {
            tracing::info!(
                "Tier 4 Fase 2: SAVITRI_TX_ROUTER_DIRECT_SEND=1 — direct-send path enabled"
            );
        }
        Self {
            swarm_tx,
            direct_send_enabled,
        }
    }

    /// Whether direct-send is currently enabled (env-gated).
    pub fn is_direct_enabled(&self) -> bool {
        self.direct_send_enabled
    }

    /// Best-effort gossip publish to the target group's TX topic.
    /// Returns `Ok(())` if the SwarmCommand was accepted by the channel,
    /// `Err` if the channel is full / closed (caller may fall back to
    /// `FallbackLocal`).
    pub fn forward_via_gossip(
        &self,
        target_group: &str,
        payload: Vec<u8>,
    ) -> Result<(), &'static str> {
        let topic =
            libp2p::gossipsub::IdentTopic::new(format!("/savitri/group/{}/tx", target_group));
        match self
            .swarm_tx
            .try_send(SwarmCommand::Publish { topic, payload })
        {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                warn!(target_group, "Dispatcher: gossip channel full");
                Err("channel_full")
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                warn!(target_group, "Dispatcher: gossip channel closed");
                Err("channel_closed")
            }
        }
    }

    /// Direct request-response forward to the elected proposer of the
    /// target group. The receiver returns `AuxAck { ok: true }` on
    /// successful local admit. Fire-and-forget at the dispatcher level
    /// — the swarm task does its own ACK timeout.
    ///
    /// Returns `DirectQueued` on success, `DirectChannelUnavailable`
    /// when the swarm channel cannot accept the command (caller MUST
    /// fall back to `forward_via_gossip`).
    pub fn forward_direct(&self, peer: PeerId, payload: Vec<u8>) -> DispatchOutcome {
        let cmd = SwarmCommand::SendAuxRequest {
            peer_id: peer,
            message: AuxMessage::TxForward(payload),
        };
        match self.swarm_tx.try_send(cmd) {
            Ok(()) => {
                debug!(?peer, "Dispatcher: direct-send queued");
                DispatchOutcome::DirectQueued
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                TxRoutingMetrics::inc_fallback_local("direct_channel_full");
                DispatchOutcome::DirectChannelUnavailable {
                    reason: "channel_full",
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                TxRoutingMetrics::inc_fallback_local("direct_channel_closed");
                DispatchOutcome::DirectChannelUnavailable {
                    reason: "channel_closed",
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fresh_channel() -> (Dispatcher, mpsc::Receiver<SwarmCommand>) {
        let (tx, rx) = mpsc::channel(8);
        // Don't depend on env in tests; explicitly construct without env read.
        let dispatcher = Dispatcher {
            swarm_tx: tx,
            direct_send_enabled: true,
        };
        (dispatcher, rx)
    }

    #[tokio::test]
    async fn forward_gossip_emits_publish_cmd() {
        let (d, mut rx) = fresh_channel();
        d.forward_via_gossip("group_a", b"raw_bytes".to_vec())
            .unwrap();
        match rx.recv().await {
            Some(SwarmCommand::Publish { topic, payload }) => {
                assert_eq!(topic.to_string(), "/savitri/group/group_a/tx");
                assert_eq!(payload, b"raw_bytes");
            }
            other => panic!("expected Publish, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn forward_direct_emits_aux_request() {
        let (d, mut rx) = fresh_channel();
        let peer = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        match d.forward_direct(peer, b"raw_tx".to_vec()) {
            DispatchOutcome::DirectQueued => (),
            other => panic!("expected DirectQueued, got {:?}", other),
        }
        match rx.recv().await {
            Some(SwarmCommand::SendAuxRequest { peer_id, message }) => {
                assert_eq!(peer_id, peer);
                match message {
                    AuxMessage::TxForward(bytes) => assert_eq!(bytes, b"raw_tx"),
                    other => panic!("expected TxForward, got {:?}", other),
                }
            }
            other => panic!("expected SendAuxRequest, got {:?}", other),
        }
    }

    #[tokio::test]
    async fn forward_gossip_returns_err_when_channel_closed() {
        let (d, rx) = fresh_channel();
        drop(rx);
        let res = d.forward_via_gossip("group_b", b"x".to_vec());
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn forward_direct_returns_unavailable_when_channel_closed() {
        let (d, rx) = fresh_channel();
        drop(rx);
        let peer = libp2p::identity::Keypair::generate_ed25519()
            .public()
            .to_peer_id();
        match d.forward_direct(peer, b"x".to_vec()) {
            DispatchOutcome::DirectChannelUnavailable { reason } => {
                assert_eq!(reason, "channel_closed");
            }
            other => panic!("expected DirectChannelUnavailable, got {:?}", other),
        }
    }
}
