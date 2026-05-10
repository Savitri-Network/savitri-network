//! Core metrics provider for Savitri Network
//! 
//! This module provides a lightweight metrics system for the core library,
//! focusing on essential metrics without external dependencies.

use std::collections::HashMap;
use std::time::{Duration, Instant};
use log::{info, debug};
use serde::{Serialize, Deserialize};

/// Metric type enumeration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
    Summary,
}

/// Basic metric structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub value: f64,
    pub metric_type: MetricType,
    pub timestamp: u64,
    pub labels: HashMap<String, String>,
}

impl Metric {
    /// Create a new metric
    pub fn new(name: String, value: f64, metric_type: MetricType) -> Self {
        Self {
            name,
            value,
            metric_type,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            labels: HashMap::new(),
        }
    }

    /// Create a metric with labels
    pub fn with_labels(
        name: String,
        value: f64,
        metric_type: MetricType,
        labels: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            value,
            metric_type,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            labels,
        }
    }

    /// Add a label to the metric
    pub fn with_label(mut self, key: String, value: String) -> Self {
        self.labels.insert(key, value);
        self
    }
}

/// Metrics configuration
#[derive(Debug, Clone)]
pub struct MetricsConfig {
    pub enabled: bool,
    pub max_metrics: usize,
    pub cleanup_interval_secs: u64,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: false, // Disabled by default for security
            max_metrics: 1000,
            cleanup_interval_secs: 300, // 5 minutes
        }
    }
}

impl MetricsConfig {
    /// Create configuration from environment variables
    pub fn from_env() -> Self {
        let enabled = std::env::var("SAVITRI_METRICS_ENABLED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(false);
        
        let max_metrics = std::env::var("SAVITRI_METRICS_MAX")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1000);
        
        let cleanup_interval_secs = std::env::var("SAVITRI_METRICS_CLEANUP_INTERVAL")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300);

        Self {
            enabled,
            max_metrics,
            cleanup_interval_secs,
        }
    }
}

/// Basic metrics provider
#[derive(Debug)]
pub struct MetricsProvider {
    pub config: MetricsConfig,
    metrics: HashMap<String, Metric>,
    created_at: Instant,
    last_cleanup: Instant,
}

impl MetricsProvider {
    /// Create a new metrics provider
    pub fn new(config: MetricsConfig) -> Self {
        Self {
            config,
            metrics: HashMap::new(),
            created_at: Instant::now(),
            last_cleanup: Instant::now(),
        }
    }

    /// Check if metrics are enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Register a metric
    pub fn register_metric(&mut self, name: String, value: f64, metric_type: MetricType) {
        if !self.is_enabled() {
            return;
        }

        self.cleanup_if_needed();

        let metric = Metric::new(name, value, metric_type);
        self.metrics.insert(metric.name.clone(), metric);
    }

    /// Register a metric with labels
    pub fn register_metric_with_labels(
        &mut self,
        name: String,
        value: f64,
        metric_type: MetricType,
        labels: HashMap<String, String>,
    ) {
        if !self.is_enabled() {
            return;
        }

        self.cleanup_if_needed();

        let metric = Metric::with_labels(name, value, metric_type, labels);
        self.metrics.insert(metric.name.clone(), metric);
    }

    /// Increment a counter metric
    pub fn increment_counter(&mut self, name: String, delta: f64) {
        if let Some(metric) = self.metrics.get_mut(&name) {
            if matches!(metric.metric_type, MetricType::Counter) {
                metric.value += delta;
                metric.timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
            }
        } else {
            self.register_metric(name, delta, MetricType::Counter);
        }
    }

    /// Set a gauge metric
    pub fn set_gauge(&mut self, name: String, value: f64) {
        if let Some(metric) = self.metrics.get_mut(&name) {
            if matches!(metric.metric_type, MetricType::Gauge) {
                metric.value = value;
                metric.timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
            }
        } else {
            self.register_metric(name, value, MetricType::Gauge);
        }
    }

    /// Record a histogram observation
    pub fn record_histogram(&mut self, name: String, value: f64) {
        // For simplicity, we'll just update the value
        // In a real implementation, this would maintain buckets
        if let Some(metric) = self.metrics.get_mut(&name) {
            if matches!(metric.metric_type, MetricType::Histogram) {
                metric.value = value; // Simplified: just store latest value
                metric.timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
            }
        } else {
            self.register_metric(name, value, MetricType::Histogram);
        }
    }

    /// Get a metric value
    pub fn get_metric(&self, name: &str) -> Option<&Metric> {
        self.metrics.get(name)
    }

    /// Get all metrics
    pub fn get_all_metrics(&self) -> &HashMap<String, Metric> {
        &self.metrics
    }

    /// Get metrics by type
    pub fn get_metrics_by_type(&self, metric_type: MetricType) -> Vec<&Metric> {
        self.metrics
            .values()
            .filter(|m| m.metric_type == metric_type)
            .collect()
    }

    /// Clear all metrics
    pub fn clear(&mut self) {
        self.metrics.clear();
    }

    /// Clear metrics older than the specified duration
    pub fn cleanup_old_metrics(&mut self, max_age: Duration) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        self.metrics.retain(|_, metric| {
            now - metric.timestamp < max_age.as_secs()
        });
    }

    /// Get statistics about the provider
    pub fn get_stats(&self) -> ProviderStats {
        ProviderStats {
            total_metrics: self.metrics.len(),
            counters: self.get_metrics_by_type(MetricType::Counter).len(),
            gauges: self.get_metrics_by_type(MetricType::Gauge).len(),
            histograms: self.get_metrics_by_type(MetricType::Histogram).len(),
            summaries: self.get_metrics_by_type(MetricType::Summary).len(),
            uptime_seconds: self.created_at.elapsed().as_secs(),
        }
    }

    /// Cleanup if needed
    fn cleanup_if_needed(&mut self) {
        let now = Instant::now();
        if now.duration_since(self.last_cleanup).as_secs() >= self.config.cleanup_interval_secs {
            self.cleanup_old_metrics(Duration::from_secs(self.config.cleanup_interval_secs));
            self.last_cleanup = now;
        }

        // Enforce max metrics limit
        if self.metrics.len() > self.config.max_metrics {
            // Remove oldest metrics (simplified approach)
            let mut metrics: Vec<_> = self.metrics.drain().collect();
            metrics.sort_by_key(|(_, m)| m.timestamp);
            let to_keep = metrics.split_off(metrics.len() - self.config.max_metrics);
            for (name, metric) in to_keep {
                self.metrics.insert(name, metric);
            }
        }
    }
}

/// Provider statistics
#[derive(Debug, Clone)]
pub struct ProviderStats {
    pub total_metrics: usize,
    pub counters: usize,
    pub gauges: usize,
    pub histograms: usize,
    pub summaries: usize,
    pub uptime_seconds: u64,
}

pub mod savitri_metrics {
    /// Block height metric
    pub const BLOCK_HEIGHT: &str = "savitri_block_height";
    
    /// Transactions per block
    pub const TX_PER_BLOCK: &str = "savitri_tx_per_block";
    
    /// Mempool size
    pub const MEMPOOL_SIZE: &str = "savitri_mempool_size";
    
    /// Peer count
    pub const PEER_COUNT: &str = "savitri_peer_count";
    
    /// CPU usage percentage
    pub const CPU_USAGE: &str = "savitri_cpu_usage";
    
    /// Memory usage in bytes
    pub const MEMORY_USAGE: &str = "savitri_memory_usage";
    
    /// Network latency in milliseconds
    pub const NETWORK_LATENCY: &str = "savitri_network_latency";
    
    /// Transactions per second
    pub const TRANSACTIONS_PER_SECOND: &str = "savitri_tps";
    
    /// Consensus round time in milliseconds
    pub const CONSENSUS_ROUND_TIME: &str = "savitri_consensus_round_time";
    
    /// Active slots
    pub const ACTIVE_SLOTS: &str = "savitri_active_slots";
    
    /// Validator count
    pub const VALIDATOR_COUNT: &str = "savitri_validator_count";
}

/// Utility functions for common operations
pub mod utils {
    use super::*;

    /// Create a counter metric
    pub fn counter(name: &str, value: f64) -> Metric {
        Metric::new(name.to_string(), value, MetricType::Counter)
    }

    /// Create a gauge metric
    pub fn gauge(name: &str, value: f64) -> Metric {
        Metric::new(name.to_string(), value, MetricType::Gauge)
    }

    /// Create a histogram metric
    pub fn histogram(name: &str, value: f64) -> Metric {
        Metric::new(name.to_string(), value, MetricType::Histogram)
    }

    /// Create a summary metric
    pub fn summary(name: &str, value: f64) -> Metric {
        Metric::new(name.to_string(), value, MetricType::Summary)
    }

    /// Create a metric with a single label
    pub fn labeled_metric(
        name: &str,
        value: f64,
        metric_type: MetricType,
        label_key: &str,
        label_value: &str,
    ) -> Metric {
        let mut labels = HashMap::new();
        labels.insert(label_key.to_string(), label_value.to_string());
        Metric::with_labels(name.to_string(), value, metric_type, labels)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let config = MetricsConfig {
            enabled: true,
            max_metrics: 1000,
            cleanup_interval_secs: 300,
        };
        let mut provider = MetricsProvider::new(config);

        provider.register_metric("test_counter".to_string(), 42.0, MetricType::Counter);
        provider.register_metric("test_gauge".to_string(), 3.14, MetricType::Gauge);

        assert_eq!(provider.metrics.len(), 2);
        assert!(provider.get_metric("test_counter").is_some());
        assert!(provider.get_metric("test_gauge").is_some());
    }

    #[test]
    fn test_counter_increment() {
        let config = MetricsConfig {
            enabled: true,
            max_metrics: 1000,
            cleanup_interval_secs: 300,
        };
        let mut provider = MetricsProvider::new(config);

        provider.register_metric("test_counter".to_string(), 0.0, MetricType::Counter);
        provider.increment_counter("test_counter".to_string(), 1.0);
        assert_eq!(provider.get_metric("test_counter").unwrap().value, 1.0);

        provider.increment_counter("test_counter".to_string(), 5.0);
        assert_eq!(provider.get_metric("test_counter").unwrap().value, 6.0);
    }

    #[test]
    fn test_gauge_set() {
        let config = MetricsConfig {
            enabled: true,
            max_metrics: 1000,
            cleanup_interval_secs: 300,
        };
        let mut provider = MetricsProvider::new(config);

        provider.register_metric("test_gauge".to_string(), 0.0, MetricType::Gauge);
        provider.set_gauge("test_gauge".to_string(), 42.5);
        assert_eq!(provider.get_metric("test_gauge").unwrap().value, 42.5);

        provider.set_gauge("test_gauge".to_string(), 100.0);
        assert_eq!(provider.get_metric("test_gauge").unwrap().value, 100.0);
    }

    #[test]
    fn test_metrics_with_labels() {
        let config = MetricsConfig {
            enabled: true,
            max_metrics: 1000,
            cleanup_interval_secs: 300,
        };
        let mut provider = MetricsProvider::new(config);

        let mut labels = HashMap::new();
        labels.insert("type".to_string(), "test".to_string());
        labels.insert("source".to_string(), "unit".to_string());

        provider.register_metric_with_labels(
            "labeled_metric".to_string(),
            100.0,
            MetricType::Counter,
            labels,
        );

        let metric = provider.get_metric("labeled_metric").unwrap();
        assert_eq!(metric.labels.get("type"), Some(&"test".to_string()));
        assert_eq!(metric.labels.get("source"), Some(&"unit".to_string()));
    }

    #[test]
    fn test_metrics_by_type() {
        let config = MetricsConfig {
            enabled: true,
            max_metrics: 1000,
            cleanup_interval_secs: 300,
        };
        let mut provider = MetricsProvider::new(config);

        provider.register_metric("counter1".to_string(), 1.0, MetricType::Counter);
        provider.register_metric("counter2".to_string(), 2.0, MetricType::Counter);
        provider.register_metric("gauge1".to_string(), 3.0, MetricType::Gauge);

        let counters = provider.get_metrics_by_type(MetricType::Counter);
        let gauges = provider.get_metrics_by_type(MetricType::Gauge);

        assert_eq!(counters.len(), 2);
        assert_eq!(gauges.len(), 1);
    }

    #[test]
    fn test_cleanup() {
        let config = MetricsConfig::default();
        let mut provider = MetricsProvider::new(config);

        provider.register_metric("old_metric".to_string(), 1.0, MetricType::Counter);
        
        // Simulate old metric by setting old timestamp
        if let Some(metric) = provider.metrics.get_mut("old_metric") {
            metric.timestamp = 0; // Very old timestamp
        }

        provider.cleanup_old_metrics(Duration::from_secs(1));
        // Should remove the old metric
        assert!(provider.get_metric("old_metric").is_none());
    }

    #[test]
    fn test_provider_stats() {
        let config = MetricsConfig {
            enabled: true,
            max_metrics: 1000,
            cleanup_interval_secs: 300,
        };
        let mut provider = MetricsProvider::new(config);

        provider.register_metric("counter1".to_string(), 1.0, MetricType::Counter);
        provider.register_metric("gauge1".to_string(), 2.0, MetricType::Gauge);

        let stats = provider.get_stats();
        assert_eq!(stats.total_metrics, 2);
        assert_eq!(stats.counters, 1);
        assert_eq!(stats.gauges, 1);
        assert_eq!(stats.histograms, 0);
        assert_eq!(stats.summaries, 0);
    }

    #[test]
    fn test_config_from_env() {
        // Test default config
        std::env::remove_var("SAVITRI_METRICS_ENABLED");
        std::env::remove_var("SAVITRI_METRICS_MAX");
        std::env::remove_var("SAVITRI_METRICS_CLEANUP_INTERVAL");

        let config = MetricsConfig::from_env();
        assert!(!config.enabled);
        assert_eq!(config.max_metrics, 1000);
        assert_eq!(config.cleanup_interval_secs, 300);
    }

    #[test]
    fn test_utility_functions() {
        use utils::*;

        let counter = counter("test", 42.0);
        assert_eq!(counter.name, "test");
        assert_eq!(counter.value, 42.0);
        assert!(matches!(counter.metric_type, MetricType::Counter));

        let labeled = labeled_metric("test", 100.0, MetricType::Gauge, "type", "test");
        assert_eq!(labeled.labels.get("type"), Some(&"test".to_string()));
    }
}
