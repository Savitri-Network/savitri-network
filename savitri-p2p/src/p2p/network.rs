//! Fixed Network Module
//!
//! Conservative implementation compatible with Savitri architecture

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};

/// Network configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub listen_address: String,
    pub listen_port: u16,
    pub max_connections: usize,
    pub connection_timeout: Duration,
    pub keep_alive_interval: Duration,
    pub enable_encryption: bool,
    pub enable_compression: bool,
    pub max_message_size: usize,
    pub reconnect_interval: Duration,
    pub max_reconnect_attempts: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            listen_address: "0.0.0.0".to_string(),
            listen_port: 8333,
            max_connections: 50,
            connection_timeout: Duration::from_secs(30),
            keep_alive_interval: Duration::from_secs(60),
            enable_encryption: true,
            enable_compression: true,
            max_message_size: 1024 * 1024, // 1MB
            reconnect_interval: Duration::from_secs(5),
            max_reconnect_attempts: 3,
        }
    }
}

/// Connection information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionInfo {
    pub peer_id: String,
    pub address: String,
    pub established_at: u64,
    pub last_activity: u64,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub is_outgoing: bool,
}

impl ConnectionInfo {
    pub fn new(peer_id: String, address: String, is_outgoing: bool) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            peer_id,
            address,
            established_at: now,
            last_activity: now,
            bytes_sent: 0,
            bytes_received: 0,
            messages_sent: 0,
            messages_received: 0,
            is_outgoing,
        }
    }

    pub fn update_activity(&mut self) {
        self.last_activity = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
    }

    pub fn is_stale(&self, timeout: Duration) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        now > self.last_activity + timeout.as_secs()
    }

    pub fn age(&self) -> Duration {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Duration::from_secs(now.saturating_sub(self.established_at))
    }
}

/// Network behaviour trait
pub trait NetworkBehaviour {
    fn handle_event(&mut self, event: BehaviourEvent);
}

/// Network behaviour events
#[derive(Debug, Clone)]
pub enum BehaviourEvent {
    PeerConnected(String),
    PeerDisconnected(String),
    MessageReceived(String, Vec<u8>),
}

/// Network statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetworkStats {
    pub connections_established: u64,
    pub connections_closed: u64,
    pub connection_failures: u64,
    pub active_connections: usize,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    pub messages_sent: u64,
    pub messages_received: u64,
    pub average_connection_duration: f64,
    pub peak_connections: usize,
}

/// Network events
#[derive(Debug, Clone)]
pub enum NetworkEvent {
    ConnectionEstablished {
        peer_id: String,
        address: String,
    },
    ConnectionClosed {
        peer_id: String,
        reason: String,
    },
    ConnectionFailed {
        peer_id: String,
        address: String,
        error: String,
    },
    MessageSent {
        peer_id: String,
        size: usize,
    },
    MessageReceived {
        peer_id: String,
        size: usize,
    },
    ListenerStarted {
        address: String,
    },
    ListenerStopped {
        address: String,
    },
    NetworkError {
        error: String,
    },
}

/// Simple network manager implementation
pub struct NetworkManager {
    config: NetworkConfig,
    connections: Arc<Mutex<HashMap<String, ConnectionInfo>>>,
    stats: NetworkStats,
    event_sender: mpsc::UnboundedSender<NetworkEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<NetworkEvent>>,
    is_listening: bool,
    local_peer_id: String,
}

impl NetworkManager {
    pub fn new(config: NetworkConfig) -> anyhow::Result<Self> {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Ok(Self {
            config,
            connections: Arc::new(Mutex::new(HashMap::new())),
            stats: NetworkStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
            is_listening: false,
            local_peer_id: "local_peer".to_string(),
        })
    }

    pub fn with_local_peer_id(mut self, peer_id: String) -> Self {
        self.local_peer_id = peer_id;
        self
    }

    pub fn with_keypair(config: NetworkConfig, _keypair: String) -> anyhow::Result<Self> {
        let mut manager = Self::new(config)?;
        manager.local_peer_id = "keypair_peer".to_string();
        Ok(manager)
    }

    pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if self.is_listening {
            return Ok(());
        }

        tracing::info!(
            "Starting network manager on {}:{}",
            self.config.listen_address,
            self.config.listen_port
        );

        // Actually bind the port
        let listen_addr = format!("{}:{}", self.config.listen_address, self.config.listen_port);
        let listener = tokio::net::TcpListener::bind(&listen_addr)
            .await
            .map_err(|e| format!("Failed to bind to {}: {}", listen_addr, e))?;

        tracing::info!("Successfully bound to {}", listen_addr);

        // Start listening in background
        let event_sender = self.event_sender.clone();
        let listen_addr_clone = listen_addr.clone();
        tokio::spawn(async move {
            tracing::info!("TCP listener active on {}", listen_addr_clone);

            // Accept connections to keep listener alive
            let _connection_counter = 0;
            loop {
                match listener.accept().await {
                    Ok((stream, addr)) => {
                        // Generate unique peer ID based on address and port
                        let peer_id = format!("real_peer_{}_{}", addr.ip(), addr.port());
                        tracing::info!("🔗 New REAL connection from {} (ID: {})", addr, peer_id);

                        // Send connection event
                        let _ = event_sender.send(NetworkEvent::ConnectionEstablished {
                            peer_id: peer_id.clone(),
                            address: addr.to_string(),
                        });

                        // SECURITY (PT-L03): Connection handler — no plaintext welcome message.
                        // Real P2P should use Noise-encrypted transport (secure_transport.rs).
                        tokio::spawn(async move {
                            use tokio::io::AsyncReadExt;
                            let mut stream = stream;

                            // Keep connection alive and handle messages
                            let mut buffer = [0u8; 1024];
                            loop {
                                match stream.read(&mut buffer).await {
                                    Ok(0) => {
                                        tracing::info!("Connection closed by peer: {}", peer_id);
                                        break;
                                    }
                                    Ok(n) => {
                                        tracing::debug!("Received {} bytes from {}", n, peer_id);
                                    }
                                    Err(e) => {
                                        tracing::warn!("Error reading from {}: {}", peer_id, e);
                                        break;
                                    }
                                }
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("Error accepting connection: {}", e);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });

        self.is_listening = true;

        // Send event
        let _ = self.event_sender.send(NetworkEvent::ListenerStarted {
            address: format!("{}:{}", self.config.listen_address, self.config.listen_port),
        });

        tracing::info!("Network manager started successfully");

        // Start bootstrap connections
        self.start_bootstrap_connections().await;

        // Start event processing loop
        self.start_event_processing().await;

        Ok(())
    }

    async fn start_event_processing(&mut self) {
        if let Some(mut event_receiver) = self.event_receiver.take() {
            let connections = self.connections.clone();

            tokio::spawn(async move {
                while let Some(event) = event_receiver.recv().await {
                    match event {
                        NetworkEvent::ConnectionEstablished { peer_id, address } => {
                            tracing::info!("🎯 Processing connection: {} -> {}", peer_id, address);
                            // SECURITY (PT-L05): Use unwrap_or_default instead of unwrap for clock safety
                            let now_secs = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs();
                            let connection_info = ConnectionInfo {
                                peer_id: peer_id.clone(),
                                address,
                                established_at: now_secs,
                                last_activity: now_secs,
                                bytes_sent: 0,
                                bytes_received: 0,
                                messages_sent: 0,
                                messages_received: 0,
                                is_outgoing: false,
                            };
                            let mut conn = connections.lock().await;
                            conn.insert(peer_id.clone(), connection_info);
                            tracing::info!("Now connected to {} peers", conn.len());
                        }
                        NetworkEvent::ConnectionClosed { peer_id, reason } => {
                            tracing::info!("🔌 Removing connection: {} ({})", peer_id, reason);
                            let mut conn = connections.lock().await;
                            conn.remove(&peer_id);
                        }
                        _ => {}
                    }
                }
            });
        }
    }

    /// SECURITY (PT-M03/PT-I02): Bootstrap connections placeholder.
    /// In production, this should dial real bootstrap peers from config/bootstrap_nodes.json
    /// and authenticate them via their PeerId (derived from Noise public key).
    /// Simulated fake peers have been removed to prevent phantom connections.
    async fn start_bootstrap_connections(&self) {
        tracing::info!("Bootstrap connection phase — no hardcoded peers (use dial() with authenticated PeerIds)");
    }

    pub async fn stop(&mut self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.is_listening {
            return Ok(());
        }

        tracing::info!("Stopping network manager");

        // Close all connections
        let peer_ids: Vec<String> = {
            let conn = self.connections.lock().await;
            conn.keys().cloned().collect()
        };
        for peer_id in peer_ids {
            self.close_connection(&peer_id).await?;
        }

        // Simulate stopping the listener
        tokio::time::sleep(Duration::from_millis(50)).await;

        self.is_listening = false;

        // Send event
        let _ = self.event_sender.send(NetworkEvent::ListenerStopped {
            address: format!("{}:{}", self.config.listen_address, self.config.listen_port),
        });

        tracing::info!("Network manager stopped");
        Ok(())
    }

    /// SECURITY (PT-H04): Fixed double-lock deadlock — all checks under a single lock.
    pub async fn connect_to_peer(
        &mut self,
        peer_id: String,
        address: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Single lock acquisition for both checks and insertion
        {
            let mut conn = self.connections.lock().await;
            if conn.len() >= self.config.max_connections {
                return Err("Maximum connections reached".into());
            }
            if conn.contains_key(&peer_id) {
                return Err("Already connected to this peer".into());
            }

            tracing::info!("Connecting to peer {} at {}", peer_id, address);

            // Add connection
            let connection = ConnectionInfo::new(peer_id.clone(), address.clone(), true);
            conn.insert(peer_id.clone(), connection);

            // Update stats
            self.stats.connections_established += 1;
            self.stats.active_connections = conn.len();
            self.stats.peak_connections = self
                .stats
                .peak_connections
                .max(self.stats.active_connections);
        }
        // Lock released here

        // Send event (outside lock to avoid holding it during channel send)
        let _ = self.event_sender.send(NetworkEvent::ConnectionEstablished {
            peer_id: peer_id.clone(),
            address,
        });

        tracing::info!("Successfully connected to peer {}", peer_id);
        Ok(())
    }

    /// SECURITY (PT-H05): Fixed double-lock deadlock — remove + stats under single lock.
    pub async fn close_connection(
        &mut self,
        peer_id: &str,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let removed = {
            let mut conn = self.connections.lock().await;
            let connection = conn.remove(peer_id);

            if let Some(ref connection) = connection {
                // Update stats under the same lock
                self.stats.connections_closed += 1;
                self.stats.active_connections = conn.len();

                // Update average connection duration
                let total_connections =
                    self.stats.connections_established + self.stats.connections_closed;
                if total_connections > 0 {
                    let duration = connection.age().as_secs_f64();
                    self.stats.average_connection_duration =
                        (self.stats.average_connection_duration * (total_connections - 1) as f64
                            + duration)
                            / total_connections as f64;
                }
            }
            connection
        };
        // Lock released here

        if removed.is_some() {
            // Send event outside lock
            let _ = self.event_sender.send(NetworkEvent::ConnectionClosed {
                peer_id: peer_id.to_string(),
                reason: "Manual close".to_string(),
            });
            tracing::info!("Closed connection to peer {}", peer_id);
        }

        Ok(())
    }

    pub async fn send_message(
        &mut self,
        peer_id: String,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check connection exists
        let conn = self.connections.lock().await;
        if !conn.contains_key(&peer_id) {
            return Err(format!("Not connected to peer {}", peer_id).into());
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

        // Update connection stats
        let mut conn = self.connections.lock().await;
        if let Some(connection) = conn.get_mut(&peer_id) {
            connection.messages_sent += 1;
            connection.bytes_sent += size as u64;
            connection.update_activity();
        }

        // Update network stats
        self.stats.messages_sent += 1;
        self.stats.bytes_sent += size as u64;

        // Send event
        let _ = self.event_sender.send(NetworkEvent::MessageSent {
            peer_id: peer_id.clone(),
            size,
        });

        tracing::debug!("Sent {} bytes to peer {}", size, peer_id);
        Ok(())
    }

    pub async fn dial(
        &mut self,
        address: String,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let peer_id = {
            let conn = self.connections.lock().await;
            format!("peer_{}", conn.len())
        };
        self.connect_to_peer(peer_id, address).await
    }

    pub async fn connected_peers(&self) -> Vec<String> {
        let connections = self.connections.lock().await;
        connections.keys().cloned().collect()
    }

    pub async fn get_connected_peers(&self) -> Vec<String> {
        self.connected_peers().await
    }

    pub fn local_peer_id(&self) -> String {
        self.local_peer_id.clone()
    }

    pub fn get_stats(&self) -> NetworkStats {
        self.stats.clone()
    }

    pub async fn handle_network_event(&mut self, event: NetworkEvent) {
        match event {
            NetworkEvent::ConnectionEstablished { peer_id, address } => {
                tracing::info!("Adding connection: {} -> {}", peer_id, address);
                let now_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let connection_info = ConnectionInfo {
                    peer_id: peer_id.clone(),
                    address,
                    established_at: now_secs,
                    last_activity: now_secs,
                    bytes_sent: 0,
                    bytes_received: 0,
                    messages_sent: 0,
                    messages_received: 0,
                    is_outgoing: false,
                };
                let mut conn = self.connections.lock().await;
                conn.insert(peer_id.clone(), connection_info);
                self.stats.active_connections = conn.len();
                self.stats.connections_established += 1;
                tracing::info!("Now connected to {} peers", conn.len());
            }
            NetworkEvent::ConnectionClosed { peer_id, reason } => {
                tracing::info!("Removing connection: {} ({})", peer_id, reason);
                let mut conn = self.connections.lock().await;
                conn.remove(&peer_id);
                self.stats.active_connections = conn.len();
                self.stats.connections_closed += 1;
            }
            NetworkEvent::ConnectionFailed {
                peer_id,
                address,
                error,
            } => {
                tracing::warn!("Connection failed: {} -> {} ({})", peer_id, address, error);
                self.stats.connection_failures += 1;
            }
            _ => {}
        }
    }

    pub async fn receive_message(
        &mut self,
        peer_id: String,
        data: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Check connection exists
        let conn = self.connections.lock().await;
        if !conn.contains_key(&peer_id) {
            return Err(format!("Not connected to peer {}", peer_id).into());
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

        // Update connection stats
        let mut conn = self.connections.lock().await;
        if let Some(connection) = conn.get_mut(&peer_id) {
            connection.messages_received += 1;
            connection.bytes_received += size as u64;
            connection.update_activity();
        }

        // Update network stats
        self.stats.messages_received += 1;
        self.stats.bytes_received += size as u64;

        // Send event
        let _ = self.event_sender.send(NetworkEvent::MessageReceived {
            peer_id: peer_id.clone(),
            size,
        });

        tracing::debug!("Received {} bytes from peer {}", size, peer_id);
        Ok(())
    }

    pub async fn get_connection(&self, peer_id: &str) -> Option<ConnectionInfo> {
        let conn = self.connections.lock().await;
        conn.get(peer_id).cloned()
    }

    pub async fn get_all_connections(&self) -> Vec<ConnectionInfo> {
        let conn = self.connections.lock().await;
        conn.values().cloned().collect()
    }

    pub async fn cleanup_stale_connections(&mut self) {
        let timeout = self.config.keep_alive_interval;
        let stale_peers: Vec<String> = {
            let conn = self.connections.lock().await;
            conn.iter()
                .filter(|(_, connection)| connection.is_stale(timeout))
                .map(|(peer_id, _)| peer_id.clone())
                .collect()
        };

        for peer_id in stale_peers {
            tracing::warn!("Cleaning up stale connection: {}", peer_id);
            let _ = self.close_connection(&peer_id).await;
        }
    }

    pub fn is_listening(&self) -> bool {
        self.is_listening
    }

    pub fn get_listen_address(&self) -> String {
        format!("{}:{}", self.config.listen_address, self.config.listen_port)
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<NetworkEvent>> {
        self.event_receiver.take()
    }
}

impl Default for NetworkManager {
    fn default() -> Self {
        Self::new(NetworkConfig::default()).unwrap()
    }
}
