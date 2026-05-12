//! Simplified RPC implementation compatible with Savitri architecture
//!
//! This is a conservative implementation that avoids trait objects and complex async patterns

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use tokio::sync::mpsc;

/// RPC configuration
#[derive(Debug, Clone)]
pub struct RpcConfig {
    pub server_address: String,
    pub server_port: u16,
    pub enabled: bool,
    pub max_request_size: usize,
    pub request_timeout: u64,
    pub enable_auth: bool,
    pub auth_token: Option<String>,
    pub enable_cors: bool,
    pub cors_origins: Vec<String>,
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

/// RPC error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcError {
    pub code: i32,
    pub message: String,
    pub data: Option<Value>,
}

impl RpcError {
    pub fn new(code: i32, message: &str) -> Self {
        Self {
            code,
            message: message.to_string(),
            data: None,
        }
    }

    pub fn with_data(code: i32, message: &str, data: Value) -> Self {
        Self {
            code,
            message: message.to_string(),
            data: Some(data),
        }
    }

    pub fn parse_error(message: &str) -> Self {
        Self::new(-32700, message)
    }

    pub fn invalid_request(message: &str) -> Self {
        Self::new(-32600, message)
    }

    pub fn method_not_found(method: &str) -> Self {
        Self::new(-32601, &format!("Method '{}' not found", method))
    }

    pub fn invalid_params(message: &str) -> Self {
        Self::new(-32602, message)
    }

    pub fn internal_error(message: &str) -> Self {
        Self::new(-32603, message)
    }
}

/// RPC request
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcRequest {
    pub jsonrpc: String,
    pub id: Value,
    pub method: String,
    pub params: Value,
}

/// RPC response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcResponse {
    pub jsonrpc: String,
    pub id: Value,
    pub result: Option<Value>,
    pub error: Option<RpcError>,
}

/// RPC statistics
#[derive(Debug, Clone, Default)]
pub struct RpcStats {
    pub requests_received: u64,
    pub requests_successful: u64,
    pub requests_failed: u64,
    pub average_processing_time: f64,
    pub active_connections: usize,
    pub bytes_received: u64,
    pub bytes_sent: u64,
}

/// RPC server events
#[derive(Debug, Clone)]
pub enum RpcServerEvent {
    RequestReceived {
        id: Value,
        method: String,
        params: Value,
    },
    MethodExecuted {
        id: Value,
        result: Value,
    },
    MethodFailed {
        id: Value,
        error: RpcError,
    },
    ClientConnected {
        address: String,
    },
    ClientDisconnected {
        address: String,
    },
}

/// Simple RPC server with concrete types
pub struct SimpleRpcServer {
    config: RpcConfig,
    methods: HashMap<String, SimpleRpcMethod>,
    stats: RpcStats,
    event_sender: mpsc::UnboundedSender<RpcServerEvent>,
    event_receiver: Option<mpsc::UnboundedReceiver<RpcServerEvent>>,
}

/// Simple RPC method enum
pub enum SimpleRpcMethod {
    GetNodeInfo { node_info: Value },
    GetPeerCount { peer_count: u64 },
}

impl SimpleRpcMethod {
    pub fn name(&self) -> &str {
        match self {
            SimpleRpcMethod::GetNodeInfo { .. } => "savitri_getNodeInfo",
            SimpleRpcMethod::GetPeerCount { .. } => "savitri_getPeerCount",
        }
    }

    pub fn description(&self) -> &str {
        match self {
            SimpleRpcMethod::GetNodeInfo { .. } => "Get information about the Savitri node",
            SimpleRpcMethod::GetPeerCount { .. } => "Get the number of connected peers",
        }
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
        match self {
            SimpleRpcMethod::GetNodeInfo { node_info } => Ok(node_info.clone()),
            SimpleRpcMethod::GetPeerCount { peer_count } => Ok(json!({
                "connected": peer_count,
                "max": 50
            })),
        }
    }
}

impl SimpleRpcServer {
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

    pub fn register_method(&mut self, method: SimpleRpcMethod) -> anyhow::Result<()> {
        let name = method.name().to_string();

        if self.methods.contains_key(&name) {
            return Err(anyhow::anyhow!("Method '{}' already registered", name));
        }

        self.methods.insert(name.clone(), method);

        tracing::info!("Registered RPC method: {}", name);
        Ok(())
    }

    pub fn is_method_registered(&self, name: &str) -> bool {
        self.methods.contains_key(name)
    }

    pub async fn start(&mut self) -> anyhow::Result<()> {
        if !self.config.enabled {
            tracing::info!("RPC server is disabled");
            return Ok(());
        }

        let address = format!("{}:{}", self.config.server_address, self.config.server_port);

        // In a real implementation, you would start an HTTP server here
        // For now, we'll just log the start
        tracing::info!("RPC server started on {}", address);

        Ok(())
    }

    pub async fn stop(&mut self) -> anyhow::Result<()> {
        tracing::info!("RPC server stopped");
        Ok(())
    }

    pub async fn process_request(&mut self, request: RpcRequest) -> RpcResponse {
        let start_time = std::time::Instant::now();

        // Update statistics
        self.stats.requests_received += 1;

        // Find and execute the method
        let response = if let Some(method) = self.methods.get(&request.method) {
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
            self.stats.average_processing_time = (self.stats.average_processing_time
                * (self.stats.requests_received - 1) as f64
                + processing_time)
                / self.stats.requests_received as f64;
        }

        tracing::debug!("RPC request processed in {}ms", processing_time);
        response
    }

    pub fn get_stats(&self) -> RpcStats {
        self.stats.clone()
    }

    pub fn take_event_receiver(&mut self) -> Option<mpsc::UnboundedReceiver<RpcServerEvent>> {
        self.event_receiver.take()
    }
}

/// Method information
#[derive(Debug, Clone, Serialize)]
pub struct MethodInfo {
    pub name: String,
    pub description: String,
    pub parameters_schema: Value,
    pub version: String,
}

impl SimpleRpcServer {
    pub fn get_method_info(&self, name: &str) -> Option<MethodInfo> {
        self.methods.get(name).map(|method| MethodInfo {
            name: method.name().to_string(),
            description: method.description().to_string(),
            parameters_schema: method.parameters_schema(),
            version: method.version().to_string(),
        })
    }

    pub fn get_all_methods_info(&self) -> Vec<MethodInfo> {
        self.methods
            .values()
            .map(|method| MethodInfo {
                name: method.name().to_string(),
                description: method.description().to_string(),
                parameters_schema: method.parameters_schema(),
                version: method.version().to_string(),
            })
            .collect()
    }
}
