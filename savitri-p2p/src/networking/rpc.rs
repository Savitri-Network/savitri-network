//! RPC (Remote Procedure Call) module
//! 
//! Provides RPC client and server implementations for P2P network communication.
//! Supports JSON-RPC 2.0 protocol with custom method registration and handling.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::{debug, info, warn, error};

/// RPC configuration
#[derive(Debug, Clone)]
pub struct RpcConfig {
    /// Server address
    pub server_address: String,
    /// Server port
    pub server_port: u16,
    /// Enable RPC server
    pub enabled: bool,
    /// Maximum request size in bytes
    pub max_request_size: usize,
    /// Request timeout in seconds
    pub request_timeout: u64,
    /// Enable authentication
    pub enable_auth: bool,
    /// Authentication token
    pub auth_token: Option<String>,
    /// Enable CORS
    pub enable_cors: bool,
    /// CORS allowed origins
    pub cors_origins: Vec<String>,
    /// Maximum concurrent requests
    pub max_concurrent_requests: usize,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            server_address: "127.0.0.1".to_string(),
            server_port: 8545,
            enabled: false,
            max_request_size: 1024 * 1024, // 1MB
            request_timeout: 30,
            enable_auth: false,
            auth_token: None,
            enable_cors: true,
            cors_origins: vec!["*".to_string()],
            max_concurrent_requests: 100,
        }
    }
}

/// RPC method registry using concrete types
pub enum RpcMethodEnum {
    GetNodeInfo(GetNodeInfoMethod),
    GetPeerCount(GetPeerCountMethod),
}

impl RpcMethodEnum {
    pub fn name(&self) -> &str {
        match self {
            RpcMethodEnum::GetNodeInfo(m) => m.name(),
            RpcMethodEnum::GetPeerCount(m) => m.name(),
        }
    }
    
    pub fn description(&self) -> &str {
        match self {
            RpcMethodEnum::GetNodeInfo(m) => m.description(),
            RpcMethodEnum::GetPeerCount(m) => m.description(),
        }
    }
    
    pub fn parameters_schema(&self) -> Value {
        match self {
            RpcMethodEnum::GetNodeInfo(m) => m.parameters_schema(),
            RpcMethodEnum::GetPeerCount(m) => m.parameters_schema(),
        }
    }
    
    pub async fn execute(&self, params: Value) -> anyhow::Result<Value> {
        match self {
            RpcMethodEnum::GetNodeInfo(m) => m.execute(params).await,
            RpcMethodEnum::GetPeerCount(m) => m.execute(params).await,
        }
    }
    
    pub fn version(&self) -> &str {
        match self {
            RpcMethodEnum::GetNodeInfo(m) => m.version(),
            RpcMethodEnum::GetPeerCount(m) => m.version(),
        }
    }
}

/// RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    /// JSON-RPC version
    pub jsonrpc: String,
    /// Request ID
    pub id: Value,
    /// Method name
    pub method: String,
    /// Method parameters
    pub params: Value,
}

/// RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    /// JSON-RPC version
    pub jsonrpc: String,
    /// Request ID
    pub id: Value,
    /// Result (if successful)
    pub result: Option<Value>,
    /// Error (if failed)
    pub error: Option<RpcError>,
}

/// RPC error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    /// Error code
    pub code: i32,
    /// Error message
    pub message: String,
    /// Error data (optional)
    pub data: Option<Value>,
}

impl RpcError {
    /// Create a new RPC error
    pub fn new(code: i32, message: &str) -> Self {
        Self {
            code,
            message: message.to_string(),
            data: None,
        }
    }

    /// Create an error with data
    pub fn with_data(code: i32, message: &str, data: Value) -> Self {
        Self {
            code,
            message: message.to_string(),
            data: Some(data),
        }
    }

    /// Parse error (-32700)
    pub fn parse_error(message: &str) -> Self {
        Self::new(-32700, message)
    }

    /// Invalid request (-32600)
    pub fn invalid_request(message: &str) -> Self {
        Self::new(-32600, message)
    }

    /// Method not found (-32601)
    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, &format!("Method '{}' not found", method))
    }

    /// Invalid params (-32602)
    pub fn invalid_params(message: &str) -> Self {
        Self::new(-32602, message)
    }

    /// Internal error (-32603)
    pub fn internal_error(message: &str) -> Self {
        Self::new(-32603, message)
    }
}

/// RPC statistics
#[derive(Debug, Clone, Default)]
pub struct RpcStats {
    /// Total requests received
    pub requests_received: u64,
    /// Total requests processed successfully
    pub requests_successful: u64,
    /// Total requests failed
    pub requests_failed: u64,
    /// Average request processing time in milliseconds
    pub average_processing_time: f64,
    /// Requests by method
    pub requests_by_method: HashMap<String, u64>,
    /// Active connections
    pub active_connections: usize,
    /// Total bytes received
    pub bytes_received: u64,
    /// Total bytes sent
    pub bytes_sent: u64,
}

/// RPC server
pub struct RpcServer {
    config: RpcConfig,
    methods: HashMap<String, RpcMethodEnum>,
    stats: RpcStats,
    event_sender: mpsc::UnboundedSender<RpcServerEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<RpcServerEvent>>,
}

/// RPC server events
#[derive(Debug, Clone)]
pub enum RpcServerEvent {
    /// Request received
    RequestReceived { id: Value, method: String, params: Value },
    /// Method executed successfully
    MethodExecuted { id: Value, result: Value },
    /// Method execution failed
    MethodFailed { id: Value, error: RpcError },
    /// Client connected
    ClientConnected { address: String },
    /// Client disconnected
    ClientDisconnected { address: String },
    /// Server error
    ServerError(String),
}

impl RpcServer {
    /// Create a new RPC server
    pub fn new(config: RpcConfig) -> anyhow::Result<Self> {
        let (event_sender, event_receiver) = mpsc::unbounded_channel();

        Ok(Self {
            config,
            methods: HashMap::new(),
            stats: RpcStats::default(),
            event_sender,
            event_receiver: Some(event_receiver),
        })
    }

    /// Register an RPC method
    pub fn register_method(&mut self, method: RpcMethodEnum) -> anyhow::Result<()> {
        let name = method.name().to_string();
        
        if self.methods.contains_key(&name) {
            return Err(anyhow::anyhow!("Method '{}' already registered", name));
        }

        self.methods.insert(name.clone(), method);
        
        info!("Registered RPC method: {}", name);
        Ok(())
    }

    /// Unregister an RPC method
    pub fn unregister_method(&mut self, name: &str) -> Option<Box<dyn RpcMethod>> {
        let method = self.methods.remove(name);
        
        if method.is_some() {
            info!("Unregistered RPC method: {}", name);
        }
        
        method
    }

    /// Start the RPC server
    pub async fn start(&mut self) -> anyhow::Result<()> {
        if !self.config.enabled {
            info!("RPC server is disabled");
            return Ok(());
        }

        let address = format!("{}:{}", self.config.server_address, self.config.server_port);
        
        // In a real implementation, you would start an HTTP server here
        // For now, we'll just log the start
        info!("RPC server started on {}", address);
        
        Ok(())
    }

    /// Stop the RPC server
    pub async fn stop(&mut self) -> anyhow::Result<()> {
        info!("RPC server stopped");
        Ok(())
    }

    /// Process an RPC request
    pub async fn process_request(&mut self, request: RpcRequest) -> RpcResponse {
        let start_time = std::time::Instant::now();
        
        // Update statistics
        self.stats.requests_received += 1;
        *self.stats.requests_by_method.entry(request.method.clone()).or_insert(0) += 1;

        debug!("Processing RPC request: {} (ID: {:?})", request.method, request.id);

        // Send event
        let _ = self.event_sender.send(RpcServerEvent::RequestReceived {
            id: request.id.clone(),
            method: request.method.clone(),
            params: request.params.clone(),
        });

        // Find and execute the method
        let response = if let Some(method) = self.methods.get_mut(&request.method) {
            match method.execute(request.params).await {
                Ok(result) => {
                    self.stats.requests_successful += 1;
                    
                    // Send success event
                    let _ = self.event_sender.send(RpcServerEvent::MethodExecuted {
                        id: request.id.clone(),
                        result: result.clone(),
                    });

                    RpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id: request.id,
                        result: Some(result),
                        error: None,
                    }
                }
                Err(e) => {
                    self.stats.requests_failed += 1;
                    let error = RpcError::internal_error(&e.to_string());
                    
                    // Send error event
                    let _ = self.event_sender.send(RpcServerEvent::MethodFailed {
                        id: request.id.clone(),
                        error: error.clone(),
                    });

                    RpcResponse {
                        jsonrpc: "2.0".to_string(),
                        id: request.id,
                        result: None,
                        error: Some(error),
                    }
                }
            }
        } else {
            self.stats.requests_failed += 1;
            let error = RpcError::method_not_found(&request.method);

            RpcResponse {
                jsonrpc: "2.0".to_string(),
                id: request.id,
                result: None,
                error: Some(error),
            }
        };

        // Update processing time
        let processing_time = start_time.elapsed().as_millis() as f64;
        if self.stats.requests_received > 0 {
            self.stats.average_processing_time = 
                (self.stats.average_processing_time * (self.stats.requests_received - 1) as f64 + processing_time) 
                / self.stats.requests_received as f64;
        }

        debug!("RPC request processed in {}ms", processing_time);
        response
    }

    /// Get server statistics
    pub fn get_stats(&self) -> RpcStats {
        self.stats.clone()
    }

    /// Get event receiver
    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<RpcServerEvent>> {
        self.event_receiver.take()
    }

    /// Get registered methods
    pub fn get_registered_methods(&self) -> Vec<String> {
        self.methods.keys().cloned().collect()
    }

    /// Check if a method is registered
    pub fn is_method_registered(&self, name: &str) -> bool {
        self.methods.contains_key(name)
    }

    /// Get method information
    pub fn get_method_info(&self, name: &str) -> Option<MethodInfo> {
        self.methods.get(name).map(|method| MethodInfo {
            name: method.name().to_string(),
            description: method.description().to_string(),
            parameters_schema: method.parameters_schema(),
            version: method.version().to_string(),
        })
    }

    /// Get all methods information
    pub fn get_all_methods_info(&self) -> Vec<MethodInfo> {
        self.methods.values().map(|method| MethodInfo {
            name: method.name().to_string(),
            description: method.description().to_string(),
            parameters_schema: method.parameters_schema(),
            version: method.version().to_string(),
        }).collect()
    }
}

/// Method information
#[derive(Debug, Clone, Serialize)]
pub struct MethodInfo {
    /// Method name
    pub name: String,
    /// Method description
    pub description: String,
    /// Parameters schema
    pub parameters_schema: Value,
    /// Method version
    pub version: String,
}

/// RPC client
pub struct RpcClient {
    server_url: String,
    auth_token: Option<String>,
    request_timeout: std::time::Duration,
}

impl RpcClient {
    /// Create a new RPC client
    pub fn new(server_url: String) -> Self {
        Self {
            server_url,
            auth_token: None,
            request_timeout: std::time::Duration::from_secs(30),
        }
    }

    /// Create a new RPC client with authentication
    pub fn with_auth(server_url: String, auth_token: String) -> Self {
        Self {
            server_url,
            auth_token: Some(auth_token),
            request_timeout: std::time::Duration::from_secs(30),
        }
    }

    /// Set request timeout
    pub fn set_timeout(&mut self, timeout: std::time::Duration) {
        self.request_timeout = timeout;
    }

    /// Call an RPC method
    pub async fn call(&self, method: &str, params: Value) -> anyhow::Result<Value> {
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(1),
            method: method.to_string(),
            params,
        };

        // In a real implementation, you would make an HTTP request here
        // For now, we'll return an error
        Err(anyhow::anyhow!("RPC client not fully implemented"))
    }

    /// Call an RPC method with custom ID
    pub async fn call_with_id(&self, method: &str, params: Value, id: Value) -> anyhow::Result<Value> {
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id,
            method: method.to_string(),
            params,
        };

        // In a real implementation, you would make an HTTP request here
        Err(anyhow::anyhow!("RPC client not fully implemented"))
    }

    /// Batch call multiple RPC methods
    pub async fn batch_call(&self, requests: Vec<RpcRequest>) -> anyhow::Result<Vec<RpcResponse>> {
        // In a real implementation, you would make a batch HTTP request here
        Err(anyhow::anyhow!("Batch RPC calls not fully implemented"))
    }
}

/// Example RPC method: Get node info
pub struct GetNodeInfoMethod {
    node_info: Value,
}

impl GetNodeInfoMethod {
    /// Create a new get node info method
    pub fn new(node_info: Value) -> Self {
        Self { node_info }
    }
}

impl GetNodeInfoMethod {
    pub fn name(&self) -> &str {
        "savitri_getNodeInfo"
    }

    pub fn description(&self) -> &str {
        "Get information about the Savitri node"
    }

    pub fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {},
            "required": []
        })
    }

    pub fn version(&self) -> &str {
        "1.0.0"
    }

    pub async fn execute(&self, _params: Value) -> anyhow::Result<Value> {
        Ok(self.node_info.clone())
    }
}

/// Example RPC method: Get peer count
pub struct GetPeerCountMethod {
    peer_count: u64,
}

impl GetPeerCountMethod {
    /// Create a new get peer count method
    pub fn new(peer_count: u64) -> Self {
        Self { peer_count }
    }
}

impl GetPeerCountMethod {
    pub fn name(&self) -> &str {
        "savitri_getPeerCount"
    }

    pub fn description(&self) -> &str {
        "Get the number of connected peers"
    }

    pub fn parameters_schema(&self) -> Value {
        json!({
            "type": "object",
            "properties": {},
            "required": []
        })
    }

    pub fn version(&self) -> &str {
        "1.0.0"
    }

    pub async fn execute(&self, _params: Value) -> anyhow::Result<Value> {
        Ok(json!({
            "connected": self.peer_count,
            "max": 50
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_config_default() {
        let config = RpcConfig::default();
        assert_eq!(config.server_address, "127.0.0.1");
        assert_eq!(config.server_port, 8545);
        assert!(!config.enabled);
        assert_eq!(config.max_request_size, 1024 * 1024);
    }

    #[test]
    fn test_rpc_error_creation() {
        let error = RpcError::new(-32601, "Method not found");
        assert_eq!(error.code, -32601);
        assert_eq!(error.message, "Method not found");
        assert!(error.data.is_none());

        let error_with_data = RpcError::with_data(-32602, "Invalid params", json!({"field": "value"}));
        assert_eq!(error_with_data.code, -32602);
        assert!(error_with_data.data.is_some());
    }

    #[test]
    fn test_rpc_error_standard_errors() {
        let parse_error = RpcError::parse_error("Invalid JSON");
        assert_eq!(parse_error.code, -32700);

        let invalid_request = RpcError::invalid_request("Invalid request");
        assert_eq!(invalid_request.code, -32600);

        let method_not_found = RpcError::method_not_found("test_method");
        assert_eq!(method_not_found.code, -32601);

        let invalid_params = RpcError::invalid_params("Invalid parameters");
        assert_eq!(invalid_params.code, -32602);

        let internal_error = RpcError::internal_error("Internal error");
        assert_eq!(internal_error.code, -32603);
    }

    #[test]
    fn test_rpc_request_creation() {
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(1),
            method: "test_method".to_string(),
            params: json!({"param1": "value1"}),
        };

        assert_eq!(request.jsonrpc, "2.0");
        assert_eq!(request.method, "test_method");
        assert_eq!(request.id, json!(1));
    }

    #[test]
    fn test_rpc_response_creation() {
        let success_response = RpcResponse {
            jsonrpc: "2.0".to_string(),
            id: json!(1),
            result: Some(json!({"result": "success"})),
            error: None,
        };

        assert!(success_response.result.is_some());
        assert!(success_response.error.is_none());

        let error_response = RpcResponse {
            jsonrpc: "2.0".to_string(),
            id: json!(1),
            result: None,
            error: Some(RpcError::internal_error("Test error")),
        };

        assert!(error_response.result.is_none());
        assert!(error_response.error.is_some());
    }

    #[tokio::test]
    async fn test_rpc_server_creation() {
        let config = RpcConfig::default();
        let server = RpcServer::new(config);
        assert!(server.is_ok());
    }

    #[tokio::test]
    async fn test_rpc_server_register_method() {
        let mut config = RpcConfig::default();
        config.enabled = true;
        
        let mut server = RpcServer::new(config).unwrap();
        
        // Register a test method
        let method = RpcMethodEnum::GetNodeInfo(GetNodeInfoMethod::new(json!({
            "version": "1.0.0",
            "protocol": "savitri"
        })));

        assert!(server.register_method(method).is_ok());
    }

    #[tokio::test]
    async fn test_process_request() {
        let mut config = RpcConfig::default();
        config.enabled = true;
        
        let mut server = RpcServer::new(config).unwrap();
        
        // Register a test method
        let method = RpcMethodEnum::GetPeerCount(GetPeerCountMethod::new(42));
        server.register_method(method).unwrap();

        // Process a request
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(1),
            method: "savitri_getPeerCount".to_string(),
            params: json!({}),
        };

        let response = server.process_request(request).await;
        
        assert!(response.result.is_some());
        assert!(response.error.is_none());
        
        let result = response.result.unwrap();
        assert_eq!(result.get("connected").unwrap(), &json!(42));
    }

    #[tokio::test]
    async fn test_process_request_method_not_found() {
        let mut config = RpcConfig::default();
        config.enabled = true;
        
        let mut server = RpcServer::new(config).unwrap();

        // Process a request for non-existent method
        let request = RpcRequest {
            jsonrpc: "2.0".to_string(),
            id: json!(1),
            method: "non_existent_method".to_string(),
            params: json!({}),
        };

        let response = server.process_request(request).await;
        
        assert!(response.result.is_none());
        assert!(response.error.is_some());
        
        let error = response.error.unwrap();
        assert_eq!(error.code, -32601);
    }

    #[test]
    fn test_get_node_info_method() {
        let node_info = json!({
            "version": "1.0.0",
            "protocol": "savitri",
            "network": "testnet"
        });
        
        let method = GetNodeInfoMethod::new(node_info.clone());
        
        assert_eq!(method.name(), "savitri_getNodeInfo");
        assert_eq!(method.description(), "Get information about the Savitri node");
        assert_eq!(method.version(), "1.0.0");
    }

    #[test]
    fn test_get_peer_count_method() {
        let method = GetPeerCountMethod::new(25);
        
        assert_eq!(method.name(), "savitri_getPeerCount");
        assert_eq!(method.description(), "Get the number of connected peers");
        assert_eq!(method.version(), "1.0.0");
    }

    #[tokio::test]
    async fn test_get_node_info_method_execution() {
        let node_info = json!({
            "version": "1.0.0",
            "protocol": "savitri"
        });
        
        let method = GetNodeInfoMethod::new(node_info.clone());
        
        let result = method.execute(json!({})).await.unwrap();
        assert_eq!(result, node_info);
    }

    #[tokio::test]
    async fn test_get_peer_count_method_execution() {
        let method = GetPeerCountMethod::new(42);
        
        let result = method.execute(json!({})).await.unwrap();
        
        assert_eq!(result.get("connected").unwrap(), &json!(42));
        assert_eq!(result.get("max").unwrap(), &json!(50));
    }
}
