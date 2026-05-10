
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use crate::oracle::integration::{OracleData, PriceFeed, OracleError};
use crate::oracle::config::OracleConfig;

pub struct OracleValidator {
    config: OracleConfig,
    validation_count: AtomicU64,
    success_count: AtomicU64,
    price_cache: HashMap<String, CachedPrice>,
}

#[derive(Debug, Clone)]
struct CachedPrice {
    price: f64,
    timestamp: Instant,
    source: String,
}

impl Default for OracleValidator {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleValidator {
    pub fn new() -> Self {
        Self::with_config(OracleConfig::default())
    }
    
    pub fn with_config(config: OracleConfig) -> Self {
        Self {
            config,
            validation_count: AtomicU64::new(0),
            success_count: AtomicU64::new(0),
            price_cache: HashMap::new(),
        }
    }
    
    pub fn prevalidate_oracle_tx(&self, tx: &[u8]) -> Result<bool, String> {
        self.validation_count.fetch_add(1, Ordering::Relaxed);
        
        // Check minimum transaction size
        if tx.len() < 50 {
            return Err("Transaction too short for oracle data".to_string());
        }
        
        // Check oracle transaction prefix
        if !tx.starts_with(b"ORACLE_FEED") {
            return Err("Invalid oracle transaction prefix".to_string());
        }
        
        // Validate transaction structure
        let validation_result = self.validate_tx_structure(tx);
        
        if validation_result.is_ok() {
            self.success_count.fetch_add(1, Ordering::Relaxed);
        }
        
        validation_result
    }
    
    /// Validate oracle transaction structure
    fn validate_tx_structure(&self, tx: &[u8]) -> Result<bool, String> {
        // Parse transaction structure
        let parsed_data = self.parse_oracle_tx(tx)?;
        
        // Validate price feed data
        self.validate_price_feed(&parsed_data)?;
        
        // Check price sanity
        self.validate_price_sanity(&parsed_data)?;
        
        // Validate timestamp
        self.validate_timestamp(parsed_data.timestamp)?;
        
        Ok(true)
    }
    
    /// Parse oracle transaction
    fn parse_oracle_tx(&self, tx: &[u8]) -> Result<PriceFeed, String> {
        // Skip oracle prefix
        let data_start = 11; // After "ORACLE_FEED" + null terminator
        
        if tx.len() < data_start + 100 {
            return Err("Insufficient data for price feed".to_string());
        }
        
        // Parse symbol (first 32 bytes after prefix)
        let symbol_end = data_start + 32;
        let symbol_bytes = &tx[data_start..symbol_end];
        let symbol = String::from_utf8_lossy(symbol_bytes)
            .trim_end_matches('\0')
            .to_string();
        
        // Parse price (next 8 bytes)
        let price_start = symbol_end;
        let price_end = price_start + 8;
        if tx.len() < price_end {
            return Err("Insufficient data for price".to_string());
        }
        let price_bytes = &tx[price_start..price_end];
        let price = f64::from_le_bytes(price_bytes.try_into()
            .map_err(|_| "Invalid price bytes".to_string())?);
        
        // Parse volume (next 8 bytes)
        let volume_start = price_end;
        let volume_end = volume_start + 8;
        if tx.len() < volume_end {
            return Err("Insufficient data for volume".to_string());
        }
        let volume_bytes = &tx[volume_start..volume_end];
        let volume = f64::from_le_bytes(volume_bytes.try_into()
            .map_err(|_| "Invalid volume bytes".to_string())?);
        
        // Parse 24h change (next 8 bytes)
        let change_start = volume_end;
        let change_end = change_start + 8;
        if tx.len() < change_end {
            return Err("Insufficient data for 24h change".to_string());
        }
        let change_bytes = &tx[change_start..change_end];
        let change_24h = f64::from_le_bytes(change_bytes.try_into()
            .map_err(|_| "Invalid change bytes".to_string())?);
        
        Ok(PriceFeed {
            symbol,
            price,
            volume,
            change_24h,
        })
    }
    
    /// Validate price feed data
    fn validate_price_feed(&self, feed: &PriceFeed) -> Result<(), String> {
        // Validate symbol
        if feed.symbol.is_empty() || feed.symbol.len() > 10 {
            return Err("Invalid symbol length".to_string());
        }
        
        // Check if symbol contains only valid characters
        if !feed.symbol.chars().all(|c| c.is_ascii_alphabetic() || c == '_') {
            return Err("Invalid symbol characters".to_string());
        }
        
        // Validate price range
        if feed.price <= 0.0 || feed.price > 1_000_000.0 {
            return Err("Price out of reasonable range".to_string());
        }
        
        // Validate volume
        if feed.volume < 0.0 {
            return Err("Negative volume not allowed".to_string());
        }
        
        // Validate 24h change (should be between -100% and +1000%)
        if feed.change_24h < -1.0 || feed.change_24h > 10.0 {
            return Err("24h change out of reasonable range".to_string());
        }
        
        Ok(())
    }
    
    /// Validate price sanity against historical data
    fn validate_price_sanity(&self, feed: &PriceFeed) -> Result<(), String> {
        // Check against cached price if available
        if let Some(cached) = self.price_cache.get(&feed.symbol) {
            let price_change = (feed.price - cached.price).abs() / cached.price;
            
            // If price changed more than 50%, flag as suspicious
            if price_change > 0.5 {
                return Err(format!("Suspicious price change for {}: {:.2}%", 
                    feed.symbol, price_change * 100.0));
            }
        }
        
        // Additional sanity checks based on symbol
        match feed.symbol.to_uppercase().as_str() {
            "BTC" => {
                if feed.price < 1000.0 || feed.price > 1_000_000.0 {
                    return Err("BTC price out of expected range".to_string());
                }
            }
            "ETH" => {
                if feed.price < 50.0 || feed.price > 100_000.0 {
                    return Err("ETH price out of expected range".to_string());
                }
            }
            "SAV" => {
                if feed.price < 0.01 || feed.price > 100.0 {
                    return Err("SAV price out of expected range".to_string());
                }
            }
            _ => {
                // Generic check for other symbols
                if feed.price < 0.0001 || feed.price > 10_000_000.0 {
                    return Err("Price out of generic acceptable range".to_string());
                }
            }
        }
        
        Ok(())
    }
    
    /// Validate timestamp
    fn validate_timestamp(&self, timestamp: u64) -> Result<(), String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        
        // Check if timestamp is not too old (more than 1 hour)
        if now > timestamp && now - timestamp > 3600 {
            return Err("Timestamp too old".to_string());
        }
        
        // Check if timestamp is not too far in future (more than 5 minutes)
        if timestamp > now && timestamp - now > 300 {
            return Err("Timestamp too far in future".to_string());
        }
        
        Ok(())
    }
    
    /// Validate oracle data consistency
    pub fn validate_oracle_data(&mut self, data: &OracleData) -> Result<(), OracleError> {
        // Check confidence level
        if data.confidence < 0.5 || data.confidence > 1.0 {
            return Err(OracleError::InvalidData);
        }
        
        // Validate price
        if data.price <= 0.0 || data.price > 1_000_000.0 {
            return Err(OracleError::InvalidData);
        }
        
        // Update cache
        self.price_cache.insert(data.source.clone(), CachedPrice {
            price: data.price,
            timestamp: Instant::now(),
            source: data.source.clone(),
        });
        
        Ok(())
    }
    
    /// Validate multiple oracle sources for consensus
    pub fn validate_consensus(&mut self, oracle_data: &[OracleData]) -> Result<f64, OracleError> {
        if oracle_data.len() < self.config.min_confirmations as usize {
            return Err(OracleError::InvalidData);
        }
        
        let prices: Vec<f64> = oracle_data.iter().map(|d| d.price).collect();
        let avg_price = prices.iter().sum::<f64>() / prices.len() as f64;
        
        // Check if prices are within tolerance
        for price in &prices {
            let deviation = (price - avg_price).abs() / avg_price;
            if deviation > self.config.price_tolerance {
                return Err(OracleError::InvalidData);
            }
        }
        
        Ok(avg_price)
    }
    
    pub fn get_validation_stats(&self) -> ValidationStats {
        let total = self.validation_count.load(Ordering::Relaxed);
        let successes = self.success_count.load(Ordering::Relaxed);
        let success_rate = if total > 0 { successes as f64 / total as f64 } else { 0.0 };
        
        ValidationStats {
            total_validations: total,
            successful_validations: successes,
            success_rate,
            cached_symbols: self.price_cache.len(),
        }
    }
    
    /// Clear price cache
    pub fn clear_cache(&mut self) {
        self.price_cache.clear();
    }
}

#[derive(Debug, Clone)]
pub struct ValidationStats {
    pub total_validations: u64,
    pub successful_validations: u64,
    pub success_rate: f64,
    pub cached_symbols: usize,
}
