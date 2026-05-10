//! SECURITY (F-02): Noise-encrypted libp2p transport builder
//!
//! Provides a function to create a fully authenticated and encrypted
//! libp2p transport stack using:
//! - TCP as the base transport
//! - Noise protocol (XX handshake) for authentication + encryption
//! - Yamux for stream multiplexing
//!
//! This module is the single source of truth for transport construction.
//! All P2P connections should use `build_secure_transport()` instead of
//! raw TCP to ensure traffic is encrypted end-to-end.

use libp2p::identity::Keypair;
use libp2p::PeerId;

/// Build a Noise-encrypted, Yamux-multiplexed libp2p transport.
///
/// The Noise XX handshake authenticates both peers and establishes an
/// encrypted channel. Yamux provides multiplexed streams over a single
/// TCP connection.
///
/// # Arguments
/// * `keypair` — The node's Ed25519 identity keypair. Used for Noise
///   authentication and PeerId derivation.
///
/// # Returns
/// `(PeerId, Boxed<(PeerId, StreamMuxerBox)>)` — The local PeerId and
/// a boxed transport ready for `SwarmBuilder`.
///
/// # Example
/// ```ignore
/// let keypair = libp2p::identity::Keypair::generate_ed25519();
/// let (peer_id, transport) = build_secure_transport(&keypair)?;
/// ```
pub fn build_secure_transport(
    keypair: &Keypair,
) -> Result<
    (
        PeerId,
        libp2p::core::transport::Boxed<(PeerId, libp2p::core::muxing::StreamMuxerBox)>,
    ),
    TransportError,
> {
    use libp2p::core::upgrade;
    use libp2p::core::Transport;

    let peer_id = PeerId::from(keypair.public());

    // 1. TCP base transport (async-io via tokio)
    let tcp_config = libp2p::tcp::Config::default().nodelay(true);
    let tcp_transport = libp2p::tcp::tokio::Transport::new(tcp_config);

    // 2. DNS wrapper for resolving /dns4/... multiaddrs
    let dns_transport = libp2p::dns::tokio::Transport::system(tcp_transport)
        .map_err(|e| TransportError::DnsInit(e.to_string()))?;

    // 3. Noise XX handshake (mutual authentication + encryption)
    let noise_config = libp2p::noise::Config::new(keypair)
        .map_err(|e| TransportError::NoiseInit(e.to_string()))?;

    // 4. Yamux stream multiplexing
    let yamux_config = libp2p::yamux::Config::default();

    // 5. Assemble the transport stack
    let transport = dns_transport
        .upgrade(upgrade::Version::V1)
        .authenticate(noise_config)
        .multiplex(yamux_config)
        .timeout(std::time::Duration::from_secs(30))
        .boxed();

    tracing::info!(
        "Secure transport built: Noise XX + Yamux over TCP/DNS (peer_id={})",
        peer_id
    );

    Ok((peer_id, transport))
}

/// Generate or load a persistent Ed25519 keypair from disk.
///
/// If the file exists, loads and returns it. Otherwise generates a new
/// keypair and saves it for future use.
pub fn load_or_generate_keypair(path: &std::path::Path) -> Result<Keypair, TransportError> {
    if path.exists() {
        let bytes = std::fs::read(path)
            .map_err(|e| TransportError::KeyLoad(format!("{}: {}", path.display(), e)))?;
        let keypair = Keypair::from_protobuf_encoding(&bytes)
            .map_err(|e| TransportError::KeyLoad(format!("Invalid keypair: {}", e)))?;
        tracing::info!("Loaded identity keypair from {}", path.display());
        Ok(keypair)
    } else {
        let keypair = Keypair::generate_ed25519();
        let bytes = keypair
            .to_protobuf_encoding()
            .map_err(|e| TransportError::KeyGeneration(e.to_string()))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| TransportError::KeyGeneration(e.to_string()))?;
        }
        // SECURITY (PT-H02): Write keypair with restrictive permissions (owner-only)
        #[cfg(unix)]
        {
            use std::io::Write;
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .mode(0o600)
                .open(path)
                .map_err(|e| {
                    TransportError::KeyGeneration(format!("Cannot write {}: {}", path.display(), e))
                })?;
            file.write_all(&bytes).map_err(|e| {
                TransportError::KeyGeneration(format!("Cannot write {}: {}", path.display(), e))
            })?;
        }
        #[cfg(not(unix))]
        {
            std::fs::write(path, &bytes).map_err(|e| {
                TransportError::KeyGeneration(format!("Cannot write {}: {}", path.display(), e))
            })?;
        }
        tracing::info!(
            "Generated new identity keypair and saved to {} (mode 0600)",
            path.display()
        );
        Ok(keypair)
    }
}

/// Transport construction errors
#[derive(Debug, Clone, thiserror::Error)]
pub enum TransportError {
    #[error("Failed to initialize DNS transport: {0}")]
    DnsInit(String),
    #[error("Failed to initialize Noise protocol: {0}")]
    NoiseInit(String),
    #[error("Failed to load keypair: {0}")]
    KeyLoad(String),
    #[error("Failed to generate keypair: {0}")]
    KeyGeneration(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_secure_transport() {
        let keypair = Keypair::generate_ed25519();
        let result = build_secure_transport(&keypair);
        assert!(result.is_ok(), "Transport build should succeed");

        let (peer_id, _transport) = result.unwrap();
        assert_eq!(peer_id, PeerId::from(keypair.public()));
    }

    #[test]
    fn test_keypair_persistence() {
        let dir = std::env::temp_dir().join("savitri_test_keypair");
        let path = dir.join("test_identity.key");

        // Clean up from previous runs
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);

        // Generate new
        let kp1 = load_or_generate_keypair(&path).unwrap();
        assert!(path.exists());

        // Load existing
        let kp2 = load_or_generate_keypair(&path).unwrap();
        assert_eq!(
            PeerId::from(kp1.public()),
            PeerId::from(kp2.public()),
            "Reloaded keypair must produce same PeerId"
        );

        // Clean up
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_dir(&dir);
    }
}
