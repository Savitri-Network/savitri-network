//! Fixed P2P Protocols Module
//!
//! Conservative implementation compatible with Savitri architecture

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

/// Protocol configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtocolConfig {
    pub name: String,
    pub version: String,
    pub max_message_size: usize,
    pub connection_timeout: Duration,
    pub keep_alive: Duration,
    pub max_concurrent_streams: u32,
    pub enable_compression: bool,
    pub enable_encryption: bool,
}

impl Default for ProtocolConfig {
    fn default() -> Self {
        Self {
            name: "savitri-p2p".to_string(),
            version: "1.0".to_string(),
            max_message_size: 1024 * 1024, // 1MB
            connection_timeout: Duration::from_secs(30),
            keep_alive: Duration::from_secs(60),
            max_concurrent_streams: 100,
            enable_compression: true,
            enable_encryption: true,
        }
    }
}

/// Protocol statistics
#[derive(Debug, Clone, Default)]
pub struct ProtocolStats {
    pub connections_established: u64,
    pub connections_failed: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub active_connections: usize,
    pub average_connection_duration: f64,
}

/// Protocol events
#[derive(Debug, Clone)]
pub enum ProtocolEvent {
    ConnectionEstablished { peer: String, protocol: String },
    ConnectionClosed { peer: String, protocol: String },
    MessageSent { peer: String, size: usize },
    MessageReceived { peer: String, size: usize },
    ProtocolError { protocol: String, error: String },
}

/// Simple protocol handler
pub struct ProtocolHandler {
    config: ProtocolConfig,
    stats: ProtocolStats,
    event_sender: mpsc::UnboundedSender<ProtocolEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<ProtocolEvent>>,
    active_connections: HashMap<String, Instant>,
}

impl ProtocolHandler {
    pub fn new(config: ProtocolConfig) -> Self {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Self {
            config,
            stats: ProtocolStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
            active_connections: HashMap::new(),
        }
    }

    pub async fn establish_connection(
        &mut self,
        peer: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Simulate connection establishment
        tokio::time::sleep(Duration::from_millis(100)).await;

        self.active_connections.insert(peer.clone(), Instant::now());
        self.stats.connections_established += 1;
        self.stats.active_connections = self.active_connections.len();

        // Send event
        let _ = self
            .event_sender
            .send(ProtocolEvent::ConnectionEstablished {
                peer: peer.clone(),
                protocol: self.config.name.clone(),
            });

        tracing::info!(
            "Established connection with {} using protocol {}",
            peer,
            self.config.name
        );
        Ok(())
    }

    pub async fn close_connection(
        &mut self,
        peer: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(start_time) = self.active_connections.remove(&peer) {
            let duration = start_time.elapsed();

            // Update average connection duration
            let total_connections =
                self.stats.connections_established + self.stats.connections_failed;
            if total_connections > 0 {
                self.stats.average_connection_duration = (self.stats.average_connection_duration
                    * (total_connections - 1) as f64
                    + duration.as_secs_f64())
                    / total_connections as f64;
            }

            self.stats.active_connections = self.active_connections.len();

            // Send event
            let _ = self.event_sender.send(ProtocolEvent::ConnectionClosed {
                peer: peer.clone(),
                protocol: self.config.name.clone(),
            });

            tracing::info!("Closed connection with {} (duration: {:?})", peer, duration);
        }

        Ok(())
    }

    pub async fn send_message(
        &mut self,
        peer: String,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check connection exists
        if !self.active_connections.contains_key(&peer) {
            return Err(format!("No active connection with {}", peer).into());
        }

        // Check message size
        if data.len() > self.config.max_message_size {
            return Err(format!(
                "Message size {} exceeds maximum {}",
                data.len(),
                self.config.max_message_size
            )
            .into());
        }

        // Simulate message sending
        let size = data.len();
        tokio::time::sleep(Duration::from_micros(size as u64)).await;

        self.stats.messages_sent += 1;
        self.stats.bytes_sent += size as u64;

        // Send event
        let _ = self.event_sender.send(ProtocolEvent::MessageSent {
            peer: peer.clone(),
            size,
        });

        tracing::debug!("Sent {} bytes to {}", size, peer);
        Ok(())
    }

    pub async fn receive_message(
        &mut self,
        peer: String,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check connection exists
        if !self.active_connections.contains_key(&peer) {
            return Err(format!("No active connection with {}", peer).into());
        }

        // Check message size
        if data.len() > self.config.max_message_size {
            return Err(format!(
                "Message size {} exceeds maximum {}",
                data.len(),
                self.config.max_message_size
            )
            .into());
        }

        // Simulate message processing
        let size = data.len();
        tokio::time::sleep(Duration::from_micros(size as u64 / 2)).await;

        self.stats.messages_received += 1;
        self.stats.bytes_received += size as u64;

        // Send event
        let _ = self.event_sender.send(ProtocolEvent::MessageReceived {
            peer: peer.clone(),
            size,
        });

        tracing::debug!("Received {} bytes from {}", size, peer);
        Ok(())
    }

    pub fn get_stats(&self) -> ProtocolStats {
        self.stats.clone()
    }

    pub fn get_active_peers(&self) -> Vec<String> {
        self.active_connections.keys().cloned().collect()
    }

    pub fn is_connected(&self, peer: &str) -> bool {
        self.active_connections.contains_key(peer)
    }

    pub fn cleanup_stale_connections(&mut self) {
        let now = Instant::now();
        let timeout = self.config.connection_timeout;

        let stale_peers: Vec<String> = self
            .active_connections
            .iter()
            .filter(|(_, start_time)| now.duration_since(**start_time) > timeout)
            .map(|(peer, _)| peer.clone())
            .collect();

        for peer in stale_peers {
            tracing::warn!("Cleaning up stale connection: {}", peer);
            let _ = self.close_connection(peer);
        }
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<ProtocolEvent>> {
        self.event_receiver.take()
    }
}

impl Default for ProtocolHandler {
    fn default() -> Self {
        Self::new(ProtocolConfig::default())
    }
}

/// Protocol manager for handling multiple protocols
pub struct ProtocolManager {
    handlers: HashMap<String, ProtocolHandler>,
    default_protocol: String,
    #[allow(dead_code)]
    stats: ProtocolStats,
    #[allow(dead_code)]
    event_sender: mpsc::UnboundedSender<ProtocolEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<ProtocolEvent>>,
}

impl ProtocolManager {
    pub fn new(default_config: ProtocolConfig) -> Self {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();
        let default_protocol = default_config.name.clone();

        let mut handlers = HashMap::new();
        handlers.insert(
            default_protocol.clone(),
            ProtocolHandler::new(default_config),
        );

        Self {
            handlers,
            default_protocol,
            stats: ProtocolStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
        }
    }

    pub fn add_protocol(
        &mut self,
        config: ProtocolConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let name = config.name.clone();

        if self.handlers.contains_key(&name) {
            return Err(format!("Protocol {} already exists", name).into());
        }

        self.handlers
            .insert(name.clone(), ProtocolHandler::new(config));
        tracing::info!("Added protocol: {}", name);
        Ok(())
    }

    pub async fn establish_connection(
        &mut self,
        peer: String,
        protocol: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let protocol_name = protocol.unwrap_or_else(|| self.default_protocol.clone());

        if let Some(handler) = self.handlers.get_mut(&protocol_name) {
            handler.establish_connection(peer).await
        } else {
            Err(format!("Protocol {} not found", protocol_name).into())
        }
    }

    pub async fn close_connection(
        &mut self,
        peer: String,
        protocol: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let protocol_name = protocol.unwrap_or_else(|| self.default_protocol.clone());

        if let Some(handler) = self.handlers.get_mut(&protocol_name) {
            handler.close_connection(peer).await
        } else {
            Err(format!("Protocol {} not found", protocol_name).into())
        }
    }

    pub async fn send_message(
        &mut self,
        peer: String,
        data: Vec<u8>,
        protocol: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let protocol_name = protocol.unwrap_or_else(|| self.default_protocol.clone());

        if let Some(handler) = self.handlers.get_mut(&protocol_name) {
            handler.send_message(peer, data).await
        } else {
            Err(format!("Protocol {} not found", protocol_name).into())
        }
    }

    pub async fn receive_message(
        &mut self,
        peer: String,
        data: Vec<u8>,
        protocol: Option<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let protocol_name = protocol.unwrap_or_else(|| self.default_protocol.clone());

        if let Some(handler) = self.handlers.get_mut(&protocol_name) {
            handler.receive_message(peer, data).await
        } else {
            Err(format!("Protocol {} not found", protocol_name).into())
        }
    }

    pub fn get_protocol_stats(&self, protocol: &str) -> Option<ProtocolStats> {
        self.handlers.get(protocol).map(|h| h.get_stats())
    }

    pub fn get_all_stats(&self) -> HashMap<String, ProtocolStats> {
        self.handlers
            .iter()
            .map(|(name, handler)| (name.clone(), handler.get_stats()))
            .collect()
    }

    pub fn cleanup_stale_connections(&mut self) {
        for handler in self.handlers.values_mut() {
            handler.cleanup_stale_connections();
        }
    }

    pub fn get_active_peers(&self) -> HashMap<String, Vec<String>> {
        self.handlers
            .iter()
            .map(|(name, handler)| (name.clone(), handler.get_active_peers()))
            .collect()
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<ProtocolEvent>> {
        self.event_receiver.take()
    }
}
