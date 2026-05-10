#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use libp2p::{
    core::{
        muxing::StreamMuxerBox,
        transport::{timeout::TransportTimeout, upgrade, Boxed, OrTransport},
    },
    dns::tokio::Transport as DnsTransport,
    gossipsub::{
        Behaviour as Gossipsub, ConfigBuilder as GossipsubConfigBuilder, MessageAuthenticity,
        MessageId, ValidationMode,
    },
    identify::Behaviour as Identify,
    identity::Keypair as IdentityKeypair,
    kad::{store::MemoryStore, Behaviour as Kademlia},
    noise,
    tcp::{tokio::Transport as TokioTcpTransport, Config as TcpConfig},
    yamux::Config as YamuxConfig,
    Multiaddr, PeerId, Transport,
};
use std::net::IpAddr;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Timeout per il dial (connessioni in uscita). Se il dial non completa in tempo
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(15);

/// Build the libp2p transport (TCP + DNS + Noise + Yamux) con timeout sul dial.
pub fn build_transport(
    identity: IdentityKeypair,
) -> Result<Boxed<(libp2p::PeerId, StreamMuxerBox)>> {
    // ROUND 8: Enable nodelay for lower latency. Port reuse is automatic in libp2p 0.55+.
    let tcp = TokioTcpTransport::new(TcpConfig::default().nodelay(true));
    let dns_tcp = DnsTransport::system(tcp)?;
    let with_timeout = TransportTimeout::with_outgoing_timeout(dns_tcp, DIAL_TIMEOUT);
    Ok(with_timeout
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&identity)?)
        .multiplex(YamuxConfig::default())
        .boxed())
}

/// Build the gossipsub behavior con parametri ottimizzati per mesh formation.
pub fn build_gossipsub(identity: IdentityKeypair) -> Result<libp2p::gossipsub::Behaviour> {
    let cfg = GossipsubConfigBuilder::default()
        // MESH PARAMETERS - ROUND 8: Scaled for 25-30 node networks (20 LN + 5 MN + TX generators).
        // mesh_n=8 was insufficient for 25+ nodes: caused 22K+ IDONTWANT errors and unstable
        // mesh topology leading to missed block gossip (Block payload still missing on MN).
        .mesh_n_low(6) // Minimo 6 peer (was 4) — ensures quorum connectivity
        .mesh_n(12) // Target 12 peer (was 8) — covers ~50% of 25-node network
        .mesh_n_high(18) // Max 18 peer (was 12) — room for all LN + MN + TX gen
        .mesh_outbound_min(4) // Minimo 4 connessioni outbound (was 3)
        // HEARTBEAT - Ottimizzato per reti grandi (120 nodi)
        .heartbeat_interval(Duration::from_millis(7000)) // 7s: riduce carico heartbeat per reti grandi
        // TIMEOUTS - Ottimizzati per reti più grandi
        .fanout_ttl(Duration::from_secs(30))
        .history_length(10) // Aumentato per buffer più grande (da 3)
        .history_gossip(5) // Aumentato per più gossip history (da 2)
        // GRAFT/PRUNE - Ottimizzati per stabilità rapida
        .graft_flood_threshold(Duration::from_secs(5))
        .prune_peers(8) // Pruning più aggressiva
        // QUEUE CONFIGURATION - Aumentato 4x per testnet 120 nodi
        .max_transmit_size(4_194_304) // 4MB per message (supports blocks with 2000+ TXs in JSON serialization)
        // VALIDATION
        .validation_mode(ValidationMode::Permissive)
        // DUPLICATE DETECTION - Cache più lunga per reti grandi
        .duplicate_cache_time(Duration::from_secs(60)) // 60s (da 30s)
        // CONNECTION HANDLER QUEUE - ROUND 7: Increased from 25K to 50K to prevent "Send Queue full"
        // on lightnodes during high TX throughput. The previous 25K was still causing 196K+ warnings
        // in a 6-minute test, saturating 3 out of 10 LNs.
        .connection_handler_queue_len(50000)
        // PUBLISH - flood_publish disabled: Fix 2 (30s mesh delay) ensures mesh is formed before
        // messages are published. flood_publish(true) was sending every message to ALL peers (not
        // just mesh), massively amplifying traffic and causing Send Queue full on lightnodes.
        .flood_publish(false)
        // e propagazione allineate tra LN e MN. Diverso formato (es. to_string()) darebbe ID diversi
        // per lo stesso messaggio e duplicati / propagazione incoerente.
        .message_id_fn(|message| {
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};
            let mut hasher = DefaultHasher::new();
            std::hash::Hash::hash(&message.data, &mut hasher);
            MessageId::from(hasher.finish().to_be_bytes().to_vec())
        })
        .build()
        .context("gossipsub config")?;

    libp2p::gossipsub::Behaviour::new(MessageAuthenticity::Signed(identity), cfg)
        .map_err(|err| anyhow!("failed to create gossipsub: {err}"))
}

/// Load or generate libp2p identity keypair.
pub fn load_or_generate_identity(path: &std::path::Path) -> Result<IdentityKeypair> {
    use std::fs;

    if path.exists() {
        // SECURITY: Check file permissions on Unix (should be 0600)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = fs::metadata(path)?;
            let mode = metadata.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                tracing::warn!(
                    path = %path.display(),
                    mode = format!("{:o}", mode),
                    "Identity key file has excessive permissions (expected 0600). Fixing..."
                );
                fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
        }

        let bytes = fs::read(path)?;
        return Ok(IdentityKeypair::from_protobuf_encoding(&bytes)?);
    }
    let kp = IdentityKeypair::generate_ed25519();
    let encoded = kp.to_protobuf_encoding()?;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(path, encoded)?;

    // SECURITY: Set restrictive permissions on newly created key file (Unix only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }

    Ok(kp)
}

/// Build Kademlia DHT for decentralized peer discovery with new API.
pub fn build_kademlia(peer_id: libp2p::PeerId) -> Result<Kademlia<MemoryStore>> {
    use libp2p::kad;
    use std::time::Duration;

    // Configure Kademlia with optimal settings for Savitri Network
    let store = MemoryStore::new(peer_id);

    let mut kad_config = kad::Config::default();
    kad_config
        .set_query_timeout(Duration::from_secs(60))
        .set_replication_factor(std::num::NonZeroUsize::new(20).unwrap());

    let kad = Kademlia::with_config(peer_id, store, kad_config);

    info!("Kademlia DHT initialized for peer: {}", peer_id);
    Ok(kad)
}

/// Build Identify protocol for peer information exchange.
pub fn build_identify(keypair: &libp2p::identity::Keypair) -> Result<libp2p::identify::Behaviour> {
    use libp2p::identify;

    let cfg = identify::Config::new("/savitri/1.0.0".to_string(), keypair.public())
        .with_agent_version("savitri-lightnode/0.1.0".to_string());

    Ok(identify::Behaviour::new(cfg))
}

/// Detect the IP address the OS would use for outbound connections without
/// sending any packet.  Opens a UDP socket and connects to a public address
/// so the OS fills in the local address, then reads it back.
/// Returns None if detection fails or the result is loopback/unspecified.
pub fn detect_outbound_ip() -> Option<IpAddr> {
    use std::net::UdpSocket;
    let socket = UdpSocket::bind("0.0.0.0:0").ok()?;
    // No packet is sent; connect() just causes the OS to select a route.
    socket.connect("8.8.8.8:53").ok()?;
    let ip = socket.local_addr().ok()?.ip();
    if ip.is_loopback() || ip.is_unspecified() {
        return None;
    }
    // a 8.8.8.8:53 può passare via docker0 (172.17.0.1) PRIMA of the NAT outbound,
    // ritorniamo None per forzare configurazione esplicita di `external_ip` in
    let is_private = match ip {
        IpAddr::V4(v4) => v4.is_private() || v4.is_link_local(),
        IpAddr::V6(v6) => {
            // RFC4193 unique local (fc00::/7), RFC4291 link-local (fe80::/10)
            (v6.segments()[0] & 0xfe00) == 0xfc00 || (v6.segments()[0] & 0xffc0) == 0xfe80
        }
    };
    if is_private {
        tracing::warn!(
            detected_ip = %ip,
            "detect_outbound_ip: route to 8.8.8.8 returned RFC1918/link-local IP \
             (Docker bridge?). Returning None — set `external_ip` explicitly in \
             config TOML to advertise the correct public IP."
        );
        return None;
    }
    Some(ip)
}

fn is_public_ip(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V4(ipv4) => {
            let octets = ipv4.octets();
            let is_documentation = matches!(
                octets,
                [192, 0, 2, _] | [198, 51, 100, _] | [203, 0, 113, _]
            );
            !ipv4.is_private()
                && !ipv4.is_loopback()
                && !ipv4.is_unspecified()
                && !ipv4.is_link_local()
                && !ipv4.is_broadcast()
                && !is_documentation
        }
        IpAddr::V6(ipv6) => {
            !ipv6.is_loopback()
                && !ipv6.is_unspecified()
                && !ipv6.is_unicast_link_local()
                && !ipv6.is_unique_local()
        }
    }
}

pub async fn detect_public_ip() -> Option<IpAddr> {
    const PUBLIC_IP_ENDPOINTS: [&str; 2] = ["https://api.ipify.org", "https://api64.ipify.org"];

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .ok()?;

    for endpoint in PUBLIC_IP_ENDPOINTS {
        let response = match client.get(endpoint).send().await {
            Ok(response) => response,
            Err(err) => {
                debug!(endpoint, error = %err, "Public IP lookup failed");
                continue;
            }
        };

        let body = match response.text().await {
            Ok(body) => body,
            Err(err) => {
                debug!(endpoint, error = %err, "Public IP lookup response body failed");
                continue;
            }
        };

        let candidate = body.trim();
        let ip = match candidate.parse::<IpAddr>() {
            Ok(ip) => ip,
            Err(err) => {
                debug!(endpoint, candidate, error = %err, "Public IP lookup returned invalid address");
                continue;
            }
        };

        if is_public_ip(&ip) {
            info!(endpoint, ip = %ip, "Detected public IP via external lookup");
            return Some(ip);
        }

        warn!(endpoint, ip = %ip, "Ignoring non-public IP returned by external lookup");
    }

    None
}

/// Initialize the libp2p swarm with Kademlia DHT for decentralized peer discovery.
pub async fn initialize_swarm(
    config: &crate::config::Config,
    keypair: libp2p::identity::Keypair,
) -> Result<libp2p::swarm::Swarm<crate::p2p::types::MyBehaviour>> {
    let peer_id = keypair.public().to_peer_id();
    let gossipsub = build_gossipsub(keypair.clone())?;
    let kad = build_kademlia(peer_id)?;
    let identify = build_identify(&keypair)?;
    let consensus = crate::p2p::consensus_protocol::build_consensus_behaviour();
    let aux = crate::p2p::aux_protocol::build_aux_behaviour();

    // Create relay client behaviour + transport for NAT traversal
    let (relay_transport, relay_behaviour) = libp2p::relay::client::new(peer_id);

    // Build base TCP transport
    // ROUND 8: Enable nodelay for lower latency. Port reuse is automatic in libp2p 0.55+.
    let tcp = TokioTcpTransport::new(TcpConfig::default().nodelay(true));
    let dns_tcp = DnsTransport::system(tcp)?;
    let with_timeout = TransportTimeout::with_outgoing_timeout(dns_tcp, DIAL_TIMEOUT);

    // TCP + Relay combined, then Noise + Yamux
    let tcp_relay = OrTransport::new(relay_transport, with_timeout)
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&keypair)?)
        .multiplex(YamuxConfig::default())
        .map(|(peer_id, muxer), _| (peer_id, StreamMuxerBox::new(muxer)));

    // QUIC transport (built-in TLS 1.3, no Noise needed)
    let quic = libp2p::quic::tokio::Transport::new(libp2p::quic::Config::new(&keypair));

    // Compose: prefer QUIC, fallback to TCP+Relay
    let transport = quic
        .or_transport(tcp_relay)
        .map(|either, _| match either {
            futures::future::Either::Left((peer_id, muxer)) => {
                (peer_id, StreamMuxerBox::new(muxer))
            }
            futures::future::Either::Right((peer_id, muxer)) => (peer_id, muxer),
        })
        .boxed();

    let dcutr = libp2p::dcutr::Behaviour::new(peer_id);
    let autonat = libp2p::autonat::Behaviour::new(peer_id, libp2p::autonat::Config::default());
    let upnp = libp2p::upnp::tokio::Behaviour::default();

    // TX fetch: announce-hash protocol (proposer requests full TX from peers)
    let tx_fetch = libp2p::request_response::Behaviour::new(
        [(
            libp2p::StreamProtocol::new(crate::p2p::tx_fetch_protocol::TX_FETCH_PROTOCOL),
            libp2p::request_response::ProtocolSupport::Full,
        )],
        libp2p::request_response::Config::default(),
    );

    let behaviour = crate::p2p::types::MyBehaviour {
        gossipsub,
        kademlia: kad,
        identify,
        consensus,
        aux,
        tx_fetch,
        relay_client: relay_behaviour,
        dcutr,
        autonat,
        upnp,
    };

    let mut swarm = libp2p::Swarm::new(
        transport,
        behaviour,
        peer_id,
        libp2p::swarm::Config::with_tokio_executor(),
    );

    // Listen on all interfaces (TCP + QUIC)
    let listen_addr = format!("/ip4/0.0.0.0/tcp/{}", config.listen_port);
    swarm.listen_on(listen_addr.parse()?)?;

    let quic_listen_addr = format!("/ip4/0.0.0.0/udp/{}/quic-v1", config.listen_port);
    swarm.listen_on(quic_listen_addr.parse()?)?;
    info!("Lightnode QUIC listen on UDP port {}", config.listen_port);

    // Register the external/announce address so the Identify protocol advertises
    // the right IP to other peers. Only use an explicitly configured or already
    // resolved public IP; never treat the local outbound interface IP as public.
    let external_ip: Option<IpAddr> = config
        .external_ip
        .as_deref()
        .and_then(|s| s.parse().ok())
        .filter(is_public_ip);

    match external_ip {
        Some(ip) => {
            let ext_addr: Multiaddr = match ip {
                IpAddr::V4(v4) => format!("/ip4/{}/tcp/{}", v4, config.listen_port).parse()?,
                IpAddr::V6(v6) => format!("/ip6/{}/tcp/{}", v6, config.listen_port).parse()?,
            };
            swarm.add_external_address(ext_addr.clone());
            info!("External TCP address registered: {}", ext_addr);

            let quic_ext: Multiaddr = match ip {
                IpAddr::V4(v4) => {
                    format!("/ip4/{}/udp/{}/quic-v1", v4, config.listen_port).parse()?
                }
                IpAddr::V6(v6) => {
                    format!("/ip6/{}/udp/{}/quic-v1", v6, config.listen_port).parse()?
                }
            };
            swarm.add_external_address(quic_ext.clone());
            info!("External QUIC address registered: {}", quic_ext);
        }
        None => {
            warn!("Could not detect external IP; other nodes may not be able to dial this node");
        }
    }

    info!("Swarm initialized with Kademlia DHT for peer: {}", peer_id);
    Ok(swarm)
}

/// Bootstrap Kademlia with multiple nodes for redundancy
pub async fn bootstrap_kademlia_with_redundancy(
    swarm: &mut libp2p::swarm::Swarm<crate::p2p::types::MyBehaviour>,
    bootstrap_config: &crate::p2p::types::BootstrapConfig,
) -> Result<()> {
    let mut bootstrap_attempts = 0;
    let max_attempts = bootstrap_config.max_bootstrap_attempts;

    // Try primary nodes first
    for node in &bootstrap_config.primary_nodes {
        if bootstrap_attempts >= max_attempts {
            break;
        }

        debug!(
            "Attempting to dial primary bootstrap node: {}",
            node.peer_id
        );
        match swarm.dial(node.addr.clone()) {
            Ok(_) => {
                bootstrap_attempts += 1;
                // Add to Kademlia routing table
                let _ = swarm
                    .behaviour_mut()
                    .kademlia
                    .add_address(&node.peer_id, node.addr.clone());
            }
            Err(e) => {
                debug!(
                    "Failed to dial primary bootstrap node {}: {}",
                    node.peer_id, e
                );
            }
        }
    }

    // If primary nodes failed, try secondary nodes
    if bootstrap_attempts == 0 {
        for node in &bootstrap_config.secondary_nodes {
            if bootstrap_attempts >= max_attempts {
                break;
            }

            debug!(
                "Attempting to dial secondary bootstrap node: {}",
                node.peer_id
            );
            match swarm.dial(node.addr.clone()) {
                Ok(_) => {
                    bootstrap_attempts += 1;
                    // Add to Kademlia routing table
                    let _ = swarm
                        .behaviour_mut()
                        .kademlia
                        .add_address(&node.peer_id, node.addr.clone());
                }
                Err(e) => {
                    debug!(
                        "Failed to dial secondary bootstrap node {}: {}",
                        node.peer_id, e
                    );
                }
            }
        }
    }

    // Initiate Kademlia bootstrap if we have any connected nodes
    if bootstrap_attempts > 0 {
        match swarm.behaviour_mut().kademlia.bootstrap() {
            Ok(query_id) => {
                info!("Kademlia bootstrap initiated with query ID: {:?}", query_id);
            }
            Err(e) => {
                debug!("Failed to initiate Kademlia bootstrap: {}", e);
            }
        }
    } else {
        return Err(anyhow!("No bootstrap nodes could be contacted"));
    }

    info!(
        "Bootstrap completed with {} nodes contacted",
        bootstrap_attempts
    );
    Ok(())
}

/// Create a default bootstrap configuration for Savitri Network
pub fn create_default_bootstrap_config() -> crate::p2p::types::BootstrapConfig {
    use libp2p::{Multiaddr, PeerId};
    use std::str::FromStr;

    let mut config = crate::p2p::types::BootstrapConfig::new();

    // Add known Savitri bootstrap nodes (these would be configured from genesis or config file)
    let bootstrap_nodes = vec![
        // Primary bootstrap nodes (high priority)
        ("12D3KooWExample1", "/ip4/85.208.236.21/tcp/8333", true),
        ("12D3KooWExample2", "/ip4/85.208.236.22/tcp/8333", true),
        ("12D3KooWExample3", "/ip4/85.208.236.23/tcp/8333", true),
        // Secondary bootstrap nodes (lower priority)
        ("12D3KooWExample4", "/ip4/85.208.236.24/tcp/8333", false),
        ("12D3KooWExample5", "/ip4/85.208.236.25/tcp/8333", false),
    ];

    for (peer_id_str, addr_str, priority) in bootstrap_nodes {
        if let (Ok(peer_id), Ok(addr)) =
            (PeerId::from_str(peer_id_str), addr_str.parse::<Multiaddr>())
        {
            let bootstrap_peer = crate::p2p::types::BootstrapPeer {
                peer_id,
                addr,
                account: None, // Would be populated from on-chain data
                priority,
            };

            if priority {
                config.add_primary_node(bootstrap_peer);
            } else {
                config.add_secondary_node(bootstrap_peer);
            }
        }
    }

    config
}
