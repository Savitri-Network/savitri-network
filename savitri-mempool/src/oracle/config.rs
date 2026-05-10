//! Oracle configuration

use std::time::Duration;

#[derive(Debug, Clone)]
pub struct OracleConfig {
    pub enabled: bool,
    pub timeout_ms: u64,
    pub max_retries: u32,
    pub retry_delay: Duration,
    pub oracle_addresses: Vec<String>,
    pub min_confirmations: u32,
    pub price_tolerance: f64,
}

impl Default for OracleConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            timeout_ms: 5000,
            max_retries: 3,
            retry_delay: Duration::from_millis(1000),
            oracle_addresses: vec![
                "oracle.savitri.network:8080".to_string(),
                "backup.oracle.savitri.network:8080".to_string(),
            ],
            min_confirmations: 2,
            price_tolerance: 0.01, // 1%
        }
    }
}

impl OracleConfig {
    pub fn new() -> Self {
        Self::default()
    }
    
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }
    
    pub fn with_oracle_addresses(mut self, addresses: Vec<String>) -> Self {
        self.oracle_addresses = addresses;
        self
    }
    
    pub fn validate(&self) -> Result<(), String> {
        if self.timeout_ms == 0 {
            return Err("Timeout must be greater than 0".to_string());
        }
        
        if self.oracle_addresses.is_empty() {
            return Err("At least one oracle address required".to_string());
        }
        
        if self.min_confirmations == 0 {
            return Err("Minimum confirmations must be at least 1".to_string());
        }
        
        if self.min_confirmations > self.oracle_addresses.len() as u32 {
            return Err("Minimum confirmations cannot exceed number of oracles".to_string());
        }
        
        Ok(())
    }
}
