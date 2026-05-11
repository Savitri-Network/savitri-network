//! Networking utilities module
//!
//! Provides networking functionality including RPC, compression, and connectors

// Use the fixed implementations that are compatible with Savitri architecture
pub mod mod_fixed;

// Re-export the fixed networking components
pub use mod_fixed::{
    CompressionAlgorithm, CompressionConfig, CompressionResult, ConnectionEvent, ConnectionStats,
    ConnectorConfig, MethodInfo, NetworkingConfig, RpcConfig, RpcError, RpcRequest, RpcResponse,
    RpcServerEvent, RpcStats, SimpleCompressionEngine, SimpleConnectionPool, SimpleNetworkManager,
    SimpleRpcMethod, SimpleRpcServer, SimpleTcpConnection,
};

/// Main networking manager using the fixed implementation
pub type NetworkingManager = SimpleNetworkManager;
