//! Oracle integration utilities

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::time::timeout;
use serde::{Deserialize, Serialize};
use crate::oracle::config::OracleConfig;

pub struct OracleIntegration {
    config: OracleConfig,
    cache: Arc<Mutex<HashMap<String, CachedOracleData>>>,
    request_count: AtomicU64,
    success_count: AtomicU64,
}

#[derive(Debug, Clone)]
struct CachedOracleData {
    value: OracleData,
    timestamp: Instant,
    ttl: Duration,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OracleData {
    pub price: f64,
    pub timestamp: u64,
    pub source: String,
    pub confidence: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriceFeed {
    pub symbol: String,
    pub price: f64,
    pub volume: f64,
    pub change_24h: f64,
}

impl OracleIntegration {
    pub fn new() -> Self {
        Self::with_config(OracleConfig::default())
    }
    
    pub fn with_config(config: OracleConfig) -> Self {
        Self {
            config,
            cache: Arc::new(Mutex::new(HashMap::new())),
            request_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
        }
    }
    
    /// Get price data from oracle with caching
    pub async fn get_price(&self, symbol: &str) -> Result<OracleData, OracleError> {
        self.request_count.fetch_add(1, Ordering::Relaxed);
        
        // Check cache first
        if let Some(cached) = self.get_cached_data(symbol) {
            return Ok(cached.value);
        }
        
        // Fetch from oracle
        let data = self.fetch_price_from_oracle(symbol).await?;
        
        // Cache the result
        self.cache_data(symbol.to_string(), data.clone());
        
        self.success_count.fetch_add(1, Ordering::Relaxed);
        Ok(data)
    }
    
    /// Get multiple prices in batch
    pub async fn get_prices_batch(&self, symbols: &[String]) -> Result<HashMap<String, OracleData>, OracleError> {
        let mut results = HashMap::new();
        
        // Process symbols in parallel
        let futures: Vec<_> = symbols
            .iter()
            .map(|symbol| async {
                let result = self.get_price(symbol).await;
                (symbol.clone(), result)
            })
            .collect();
        
        // Wait for all requests to complete
        let futures = futures::future::join_all(futures).await;
        
        for (symbol, result) in futures {
            match result {
                Ok(data) => { results.insert(symbol, data); }
                Err(e) => { tracing::warn!("Failed to get price for {}: {:?}", symbol, e); }
            }
        }
        
        Ok(results)
    }
    
    /// Fetch price from oracle with retry logic
    async fn fetch_price_from_oracle(&self, symbol: &str) -> Result<OracleData, OracleError> {
        let mut last_error = None;
        
        for attempt in 0..=self.config.max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_delay).await;
            }
            
            match self.try_fetch_price(symbol).await {
                Ok(data) => return Ok(data),
                Err(e) => {
                    last_error = Some(e.clone());
                    tracing::warn!("Oracle fetch attempt {} failed for {}: {:?}", attempt + 1, symbol, e);
                }
            }
        }
        
        Err(last_error.unwrap_or(OracleError::UnknownError))
    }
    
    /// Try to fetch price from a single oracle
    async fn try_fetch_price(&self, symbol: &str) -> Result<OracleData, OracleError> {
        // Simulate oracle API call
        let duration = Duration::from_millis(self.config.timeout_ms);
        
        let result = timeout(duration, async {
            // Simulate network latency and processing
            tokio::time::sleep(Duration::from_millis(50)).await;
            
            // Mock oracle response
            Ok(OracleData {
                price: self.mock_price_for_symbol(symbol),
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
                source: "mock_oracle".to_string(),
                confidence: 0.95,
            })
        }).await;
        
        match result {
            Ok(Ok(data)) => Ok(data),
            Ok(Err(e)) => Err(e),
            Err(_) => Err(OracleError::Timeout),
        }
    }
    
    /// Mock price generation for demonstration
    fn mock_price_for_symbol(&self, symbol: &str) -> f64 {
        match symbol.to_uppercase().as_str() {
            "BTC" => 45000.0 + (rand::random::<f64>() - 0.5) * 1000.0,
            "ETH" => 3000.0 + (rand::random::<f64>() - 0.5) * 100.0,
            "SAV" => 1.0 + (rand::random::<f64>() - 0.5) * 0.1,
            _ => 100.0 + (rand::random::<f64>() - 0.5) * 10.0,
        }
    }
    
    /// Get cached data if still valid
    fn get_cached_data(&self, symbol: &str) -> Option<CachedOracleData> {
        let cache = self.cache.lock().unwrap();
        cache.get(symbol).filter(|cached| {
            Instant::now().duration_since(cached.timestamp) < cached.ttl
        }).cloned()
    }
    
    /// Cache oracle data
    fn cache_data(&self, symbol: String, data: OracleData) {
        let mut cache = self.cache.lock().unwrap();
        cache.insert(symbol, CachedOracleData {
            value: data,
            timestamp: Instant::now(),
            ttl: Duration::from_secs(60), // 1 minute cache
        });
    }
    
    /// Get integration statistics
    pub fn get_stats(&self) -> OracleStats {
        let total = self.request_count.load(Ordering::Relaxed);
        let successes = self.success_count.load(Ordering::Relaxed);
        let success_rate = if total > 0 { successes as f64 / total as f64 } else { 0.0 };
        
        OracleStats {
            total_requests: total,
            successful_requests: successes,
            success_rate,
            cache_size: self.cache.lock().unwrap().len(),
        }
    }
    
    /// Clear cache
    pub fn clear_cache(&self) {
        self.cache.lock().unwrap().clear();
    }
}

/// Check if transaction is an oracle feed transaction
pub fn is_oracle_feed_tx(tx: &[u8]) -> bool {
    if tx.len() < 10 {
        return false;
    }
    
    // Check for oracle transaction marker
    // Assuming oracle transactions start with specific prefix
    let oracle_prefix = b"ORACLE_FEED";
    tx.starts_with(oracle_prefix)
}

/// Extract oracle data from transaction
pub fn extract_oracle_data(tx: &[u8]) -> Result<PriceFeed, OracleError> {
    if !is_oracle_feed_tx(tx) {
        return Err(OracleError::InvalidTransaction);
    }
    
    // Parse oracle data from transaction
    // Simplified parsing for demonstration
    let data_start = 11; // After "ORACLE_FEED" + null terminator
    
    if tx.len() < data_start + 50 {
        return Err(OracleError::InvalidData);
    }
    
    // Mock extraction - real implementation would parse actual format
    Ok(PriceFeed {
        symbol: "BTC".to_string(),
        price: 45000.0,
        volume: 1000000.0,
        change_24h: 0.05,
    })
}

#[derive(Debug, Clone)]
pub enum OracleError {
    Timeout,
    InvalidTransaction,
    InvalidData,
    NetworkError(String),
    SerializationError(String),
    UnknownError,
}

impl std::fmt::Display for OracleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OracleError::Timeout => write!(f, "Oracle request timeout"),
            OracleError::InvalidTransaction => write!(f, "Invalid oracle transaction"),
            OracleError::InvalidData => write!(f, "Invalid oracle data"),
            OracleError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            OracleError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            OracleError::UnknownError => write!(f, "Unknown oracle error"),
        }
    }
}

impl std::error::Error for OracleError {}

#[derive(Debug, Clone)]
pub struct OracleStats {
    pub total_requests: u64,
    pub successful_requests: u64,
    pub success_rate: f64,
    pub cache_size: usize,
}

impl Default for OracleIntegration {
    fn default() -> Self {
        Self::new()
    }
}
