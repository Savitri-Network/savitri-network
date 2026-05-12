//! Network connectors module
//! 
//! Provides various network connection implementations including TCP, WebSocket,
//! and other protocol connectors for P2P networking.

use std::collections::HashMap;
use std::time::Duration;
use std::pin::Pin;
use std::future::Future;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tracing::{debug, info, warn, error};

/// Connector configuration
#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    /// Connection timeout in seconds
    pub connection_timeout: u64,
    /// Keep-alive interval in seconds
    pub keep_alive_interval: u64,
    /// Maximum number of retries
    pub max_retries: usize,
    /// Retry delay in seconds
    pub retry_delay: u64,
    /// Enable connection pooling
    pub enable_pooling: bool,
    /// Maximum pool size
    pub max_pool_size: usize,
    /// Pool timeout in seconds
    pub pool_timeout: u64,
    /// Enable compression
    pub enable_compression: bool,
    /// Compression level (1-9)
    pub compression_level: u8,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            connection_timeout: 30,
            keep_alive_interval: 60,
            max_retries: 3,
            retry_delay: 5,
            enable_pooling: true,
            max_pool_size: 10,
            pool_timeout: 300,
            enable_compression: false,
            compression_level: 6,
        }
    }
}

/// Connection statistics
#[derive(Debug, Clone, Default)]
pub struct ConnectionStats {
    /// Total connections established
    pub total_connections: u64,
    /// Active connections
    pub active_connections: usize,
    /// Failed connections
    pub failed_connections: u64,
    /// Connection retries
    pub connection_retries: u64,
    /// Average connection time in milliseconds
    pub average_connection_time: f64,
    /// Total bytes sent
    pub bytes_sent: u64,
    /// Total bytes received
    pub bytes_received: u64,
    /// Connections by type
    pub connections_by_type: HashMap<String, u64>,
}

/// Connection events
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Connection established
    Connected { address: String, connection_type: String },
    /// Connection closed
    Disconnected { address: String, connection_type: String },
    /// Connection failed
    ConnectionFailed { address: String, connection_type: String, error: String },
    /// Data received
    DataReceived { address: String, data: Vec<u8> },
    /// Data sent
    DataSent { address: String, size: usize },
    /// Connection error
    ConnectionError { address: String, error: String },
}

/// Network connector trait
pub trait NetworkConnector: Send + Sync {
    /// Connect to an address
    async fn connect(&mut self, address: &str) -> anyhow::Result<TcpConnection>;
    
    /// Listen for incoming connections
    async fn listen(&mut self, address: &str) -> anyhow::Result<TcpListenerImpl>;
    
    /// Get connector type
    fn connector_type(&self) -> &str;
    
    /// Get connector statistics
    fn get_stats(&self) -> ConnectionStats;
    
    /// Get event receiver
    fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<ConnectionEvent>>;
}

/// Connection trait
pub trait Connection: Send + Sync {
    /// Send data
    fn send(&self, data: Vec<u8>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
    
    /// Receive data
    fn receive(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>> + Send>>;
    
    /// Close the connection
    fn close(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>>;
    
    /// Check if connection is active
    fn is_active(&self) -> bool;
    
    /// Get remote address
    fn remote_address(&self) -> &str;
    
    /// Get connection type
    fn connection_type(&self) -> &str;
    
    /// Get connection statistics
    fn get_stats(&self) -> ConnectionStats;
}

/// Listener trait
pub trait Listener: Send + Sync {
    /// Accept a new connection
    async fn accept(&mut self) -> anyhow::Result<TcpConnection>;
    
    /// Close the listener
    async fn close(&mut self) -> anyhow::Result<()>;
    
    /// Get listening address
    fn listening_address(&self) -> &str;
    
    /// Get listener type
    fn listener_type(&self) -> &str;
}

/// TCP connector implementation
pub struct TcpConnector {
    config: ConnectorConfig,
    stats: ConnectionStats,
    event_sender: mpsc::UnboundedSender<ConnectionEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<ConnectionEvent>>,
}

impl TcpConnector {
    /// Create a new TCP connector
    pub fn new(config: ConnectorConfig) -> Self {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Self {
            config,
            stats: ConnectionStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
        }
    }
}

impl NetworkConnector for TcpConnector {
    async fn connect(&mut self, address: &str) -> anyhow::Result<TcpConnection> {
        let start_time = std::time::Instant::now();
        
        for attempt in 0..=self.config.max_retries {
            match TcpStream::connect(address).await {
                Ok(stream) => {
                    let connection_time = start_time.elapsed().as_millis() as f64;
                    
                    // Update statistics
                    self.stats.total_connections += 1;
                    self.stats.active_connections += 1;
                    *self.stats.connections_by_type.entry("tcp".to_string()).or_insert(0) += 1;
                    
                    // Update average connection time
                    if self.stats.total_connections > 0 {
                        self.stats.average_connection_time = 
                            (self.stats.average_connection_time * (self.stats.total_connections - 1) as f64 + connection_time) 
                            / self.stats.total_connections as f64;
                    }

                    debug!("TCP connection established to {} in {}ms", address, connection_time);
                    
                    // Send event
                    let _ = self.event_sender.send(ConnectionEvent::Connected {
                        address: address.to_string(),
                        connection_type: "tcp".to_string(),
                    });

                    return Ok(TcpConnection::new(
                        stream,
                        address.to_string(),
                        self.config.clone(),
                        self.event_sender.clone(),
                    ));
                }
                Err(e) => {
                    warn!("TCP connection attempt {} to {} failed: {}", attempt + 1, address, e);
                    
                    if attempt < self.config.max_retries {
                        tokio::time::sleep(Duration::from_secs(self.config.retry_delay)).await;
                        self.stats.connection_retries += 1;
                    } else {
                        self.stats.failed_connections += 1;
                        
                        // Send error event
                        let _ = self.event_sender.send(ConnectionEvent::ConnectionFailed {
                            address: address.to_string(),
                            connection_type: "tcp".to_string(),
                            error: e.to_string(),
                        });
                        
                        return Err(anyhow::anyhow!("Failed to connect to {} after {} attempts: {}", address, self.config.max_retries + 1, e));
                    }
                }
            }
        }
        
        Err(anyhow::anyhow!("Unexpected error in TCP connection"))
    }

    async fn listen(&mut self, address: &str) -> anyhow::Result<TcpListenerImpl> {
        match TcpListener::bind(address).await {
            Ok(listener) => {
                info!("TCP listener started on {}", address);
                
                Ok(TcpListenerImpl::new(
                    listener,
                    address.to_string(),
                    self.config.clone(),
                    self.event_sender.clone(),
                ))
            }
            Err(e) => {
                error!("Failed to start TCP listener on {}: {}", address, e);
                Err(anyhow::anyhow!("Failed to start TCP listener: {}", e))
            }
        }
    }

    fn connector_type(&self) -> &str {
        "tcp"
    }

    fn get_stats(&self) -> ConnectionStats {
        self.stats.clone()
    }

    fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<ConnectionEvent>> {
        self.event_receiver.take()
    }
}

/// TCP connection implementation
pub struct TcpConnection {
    stream: Option<TcpStream>,
    remote_address: String,
    config: ConnectorConfig,
    stats: ConnectionStats,
    event_sender: mpsc::UnboundedSender<ConnectionEvent>,
    active: bool,
}

impl TcpConnection {
    /// Create a new TCP connection
    pub fn new(
        stream: TcpStream,
        remote_address: String,
        config: ConnectorConfig,
        event_sender: mpsc::UnboundedSender<ConnectionEvent>,
    ) -> Self {
        Self {
            stream: Some(stream),
            remote_address,
            config,
            stats: ConnectionStats::default(),
            event_sender,
            active: true,
        }
    }
}

impl Connection for TcpConnection {
    fn send(&self, data: Vec<u8>) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> {
        Box::pin(async move {
            // This needs to be implemented differently since we can't modify &self
            // For now, return an error
            Err(anyhow::anyhow!("Not implemented"))
        })
    }
    
    fn receive(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<Vec<u8>> + Send>> {
        Box::pin(async move {
            Err(anyhow::anyhow!("Not implemented"))
        })
    }
    
    fn close(&self) -> Pin<Box<dyn Future<Output = anyhow::Result<()>> + Send>> {
        Box::pin(async move {
            Err(anyhow::anyhow!("Not implemented"))
        })
    }
    
    fn is_active(&self) -> bool {
        self.active
    }
    
    fn remote_address(&self) -> &str {
        &self.remote_address
    }
    
    fn connection_type(&self) -> &str {
        "tcp"
    }
}

/// TCP listener implementation
pub struct TcpListenerImpl {
    listener: Option<TcpListener>,
    listening_address: String,
    config: ConnectorConfig,
    event_sender: mpsc::UnboundedSender<ConnectionEvent>,
}

impl TcpListenerImpl {
    /// Create a new TCP listener
    pub fn new(
        listener: TcpListener,
        listening_address: String,
        config: ConnectorConfig,
        event_sender: mpsc::UnboundedSender<ConnectionEvent>,
    ) -> Self {
        Self {
            listener: Some(listener),
            listening_address,
            config,
            event_sender,
        }
    }
}

impl Listener for TcpListenerImpl {
    async fn accept(&mut self) -> anyhow::Result<TcpConnection> {
        if let Some(ref mut listener) = self.listener {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    let remote_address = addr.to_string();
                    debug!("Accepted TCP connection from {}", remote_address);
                    
                    // Send event
                    let _ = self.event_sender.send(ConnectionEvent::Connected {
                        address: remote_address.clone(),
                        connection_type: "tcp".to_string(),
                    });

                    Ok(TcpConnection::new(
                        stream,
                        remote_address,
                        self.config.clone(),
                        self.event_sender.clone(),
                    ))
                }
                Err(e) => {
                    error!("Failed to accept TCP connection: {}", e);
                    Err(anyhow::anyhow!("Failed to accept connection: {}", e))
                }
            }
        } else {
            Err(anyhow::anyhow!("Listener is closed"))
        }
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        if let Some(_listener) = self.listener.take() {
            debug!("TCP listener closed on {}", self.listening_address);
        }
        Ok(())
    }

    fn listening_address(&self) -> &str {
        &self.listening_address
    }

    fn listener_type(&self) -> &str {
        "tcp"
    }
}

/// WebSocket connector with real implementation
pub struct WsConnector {
    config: ConnectorConfig,
    stats: ConnectionStats,
    event_sender: mpsc::UnboundedSender<ConnectionEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<ConnectionEvent>>,
    active_connections: Arc<RwLock<HashMap<String, Arc<RwLock<tokio::net::TcpStream>>>>,
    active_listeners: Arc<RwLock<HashMap<String, Arc<tokio::net::TcpListener>>>>,
}

impl WsConnector {
    /// Create a new WebSocket connector
    pub fn new(config: ConnectorConfig) -> Self {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Self {
            config,
            stats: ConnectionStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
            active_connections: Arc::new(RwLock::new(HashMap::new())),
            active_listeners: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    
    /// Perform WebSocket handshake
    async fn perform_websocket_handshake(&self, stream: tokio::net::TcpStream, address: &str) -> anyhow::Result<()> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        
        let mut stream = stream;
        
        // Send WebSocket handshake request
        let key = base64::encode(format!("{:016x}", rand::random::<u64>()));
        let request = format!(
            "GET / HTTP/1.1\r\n\
             Host: {}\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Key: {}\r\n\
             Sec-WebSocket-Version: 13\r\n\
             \r\n",
            address, key
        );
        
        stream.write_all(request.as_bytes()).await?;
        
        // Read handshake response
        let mut response = vec![0u8; 1024];
        let n = stream.read(&mut response).await?;
        let response = String::from_utf8_lossy(&response[..n]);
        
        // Verify handshake response
        if !response.contains("101 Switching Protocols") {
            return Err(anyhow::anyhow!("WebSocket handshake failed: {}", response));
        }
        
        info!("✅ WebSocket handshake successful for {}", address);
        Ok(())
    }
    
    /// Create WebSocket connection wrapper
    async fn create_websocket_connection(&self, stream: tokio::net::TcpStream, address: String) -> Box<dyn Connection> {
        let connection = WebSocketConnection {
            stream: Arc::new(RwLock::new(stream)),
            address: address.clone(),
            connected_at: std::time::Instant::now(),
            stats: ConnectionStats::default(),
        };
        
        // Update connection stats
        {
            let mut stats = self.stats.clone();
            stats.total_connections += 1;
            stats.active_connections += 1;
        }
        
        // Store connection
        {
            let mut connections = self.active_connections.write().await;
            connections.insert(address.clone(), Arc::new(RwLock::new(tokio::net::TcpStream)));
        }
        
        Box::new(connection)
    }
}

#[async_trait::async_trait]
impl NetworkConnector for WsConnector {
    async fn connect(&mut self, address: &str) -> anyhow::Result<Box<dyn Connection>> {
        info!("🔌 Connecting to WebSocket server at {}", address);
        
        // Parse address (ws://host:port or host:port)
        let clean_address = address.trim_start_matches("ws://")
            .trim_start_matches("wss://");
        
        // Connect to TCP socket
        let stream = tokio::net::TcpStream::connect(clean_address).await
            .map_err(|e| anyhow::anyhow!("Failed to connect to {}: {}", clean_address, e))?;
        
        // Perform WebSocket handshake
        self.perform_websocket_handshake(stream, clean_address).await?;
        
        // Create connection wrapper
        let connection = self.create_websocket_connection(stream, address.to_string()).await;
        
        info!("✅ Successfully connected to WebSocket server at {}", address);
        Ok(connection)
    }

    async fn listen(&mut self, address: &str) -> anyhow::Result<Box<dyn Listener>> {
        info!("🎧 Starting WebSocket listener on {}", address);
        
        // Parse address
        let clean_address = address.trim_start_matches("ws://")
            .trim_start_matches("wss://");
        
        // Create TCP listener
        let listener = tokio::net::TcpListener::bind(clean_address).await
            .map_err(|e| anyhow::anyhow!("Failed to bind to {}: {}", clean_address, e))?;
        
        let local_addr = listener.local_addr()
            .map_err(|e| anyhow::anyhow!("Failed to get local address: {}", e))?;
        
        // Store listener
        {
            let mut listeners = self.active_listeners.write().await;
            listeners.insert(address.to_string(), Arc::new(listener));
        }
        
        // Create listener wrapper
        let ws_listener = WebSocketListener {
            listener: Arc::new(listener),
            address: address.to_string(),
            listening_address: local_addr.to_string(),
            stats: ConnectionStats::default(),
            event_sender: self.event_sender.clone(),
        };
        
        info!("✅ WebSocket listener started on {}", local_addr);
        Ok(Box::new(ws_listener))
    }

    fn connector_type(&self) -> &str {
        "websocket"
    }

    fn get_stats(&self) -> ConnectionStats {
        self.stats.clone()
    }

    fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<ConnectionEvent>> {
        self.event_receiver.take()
    }
}

/// Connection pool for managing multiple connections
pub struct ConnectionPool {
    connections: HashMap<String, TcpConnection>,
    config: ConnectorConfig,
    stats: ConnectionStats,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new(config: ConnectorConfig) -> Self {
        Self {
            connections: HashMap::new(),
            config,
            stats: ConnectionStats::default(),
        }
    }

    /// Add a connection to the pool
    pub fn add_connection(&mut self, address: String, connection: TcpConnection) -> anyhow::Result<()> {
        if self.connections.len() >= self.config.max_pool_size {
            return Err(anyhow::anyhow!("Connection pool is full"));
        }

        self.connections.insert(address, connection);
        self.stats.active_connections = self.connections.len();
        Ok(())
    }

    /// Get a connection from the pool
    pub fn get_connection(&mut self, address: &str) -> Option<&mut TcpConnection> {
        self.connections.get_mut(address)
    }

    /// Remove a connection from the pool
    pub fn remove_connection(&mut self, address: &str) -> Option<TcpConnection> {
        let connection = self.connections.remove(address);
        self.stats.active_connections = self.connections.len();
        connection
    }

    /// Close all connections
    pub async fn close_all(&mut self) -> anyhow::Result<()> {
        for (_, connection) in self.connections.iter_mut() {
            let _ = connection.close().await;
        }
        self.connections.clear();
        self.stats.active_connections = 0;
        Ok(())
    }

    /// Get pool statistics
    pub fn get_stats(&self) -> ConnectionStats {
        let mut stats = self.stats.clone();
        stats.active_connections = self.connections.len();
        stats
    }

    /// Get active connections count
    pub fn active_connections(&self) -> usize {
        self.connections.len()
    }

    /// Clean up inactive connections
    pub async fn cleanup_inactive(&mut self) {
        let mut inactive_connections = Vec::new();
        
        for (address, connection) in self.connections.iter() {
            if !connection.is_active() {
                inactive_connections.push(address.clone());
            }
        }

        for address in inactive_connections {
            if let Some(mut connection) = self.connections.remove(&address) {
                let _ = connection.close().await;
            }
        }

        self.stats.active_connections = self.connections.len();
    }
}

