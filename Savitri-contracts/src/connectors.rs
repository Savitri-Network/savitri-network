//! Connectors module for oracle integration
//!
//! Provides connector types for external data sources

use serde::{Deserialize, Serialize};

/// Connector information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorInfo {
    pub id: Vec<u8>,
    pub name: String,
    pub endpoint: String,
    pub pubkey: Vec<u8>,
    pub registered_at: u64,
    pub active: bool,
    pub connector_id: String,
    pub config: Vec<u8>,
}

impl ConnectorInfo {
    pub fn new(id: Vec<u8>, name: String, endpoint: String, pubkey: Vec<u8>) -> Self {
        let connector_id = hex::encode(&id);
        Self {
            id,
            name,
            endpoint,
            pubkey,
            registered_at: 0,
            active: true,
            connector_id,
            config: Vec::new(),
        }
    }
}

/// Connector configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorConfig {
    pub max_requests_per_second: u32,
    pub timeout_ms: u32,
    pub retry_attempts: u32,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            max_requests_per_second: 100,
            timeout_ms: 5000,
            retry_attempts: 3,
        }
    }
}

/// Types submodule for connector-related types
pub mod types {
    use super::*;

    /// Connector status
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum ConnectorStatus {
        Active,
        Inactive,
        Suspended,
        Banned,
    }

    /// Connector metrics
    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct ConnectorMetrics {
        pub requests_served: u64,
        pub errors_count: u64,
        pub average_response_time: f64,
        pub last_request_at: u64,
    }

    impl Default for ConnectorMetrics {
        fn default() -> Self {
            Self {
                requests_served: 0,
                errors_count: 0,
                average_response_time: 0.0,
                last_request_at: 0,
            }
        }
    }
}
