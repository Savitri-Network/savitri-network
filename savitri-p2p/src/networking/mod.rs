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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_networking_config_default() {
        let config = NetworkingConfig::default();
        assert!(!config.rpc.enabled);
        assert_eq!(
            config.compression.default_algorithm,
            CompressionAlgorithm::None
        );
        assert_eq!(config.connectors.max_retries, 3);
    }

    #[tokio::test]
    async fn test_networking_manager_creation() {
        let config = NetworkingConfig::default();
        let manager = NetworkingManager::new(config);
        assert!(manager.is_ok());
    }
}
