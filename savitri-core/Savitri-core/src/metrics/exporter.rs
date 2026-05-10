//! Core metrics exporter for Savitri Network
//! 
//! This module provides a basic metrics exporter for the core library,
//! focusing on Prometheus format without HTTP server dependencies.

use std::collections::HashMap;
use crate::metrics::{Metric, MetricType};

/// Prometheus exporter configuration
#[derive(Debug, Clone)]
pub struct PrometheusExporterConfig {
    pub enabled: bool,
    pub prefix: String,
    pub include_timestamp: bool,
}

impl Default for PrometheusExporterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            prefix: "savitri".to_string(),
            include_timestamp: true,
        }
    }
}

/// Basic Prometheus exporter
#[derive(Debug)]
pub struct PrometheusExporter {
    config: PrometheusExporterConfig,
}

impl PrometheusExporter {
    /// Create a new exporter
    pub fn new(config: PrometheusExporterConfig) -> Self {
        Self { config }
    }

    /// Export metrics in Prometheus format
    pub fn export_metrics(&self, metrics: &HashMap<String, Metric>) -> String {
        if !self.config.enabled {
            return String::new();
        }

        let mut output = String::new();

        for metric in metrics.values() {
            let prometheus_name = if self.config.prefix.is_empty() {
                metric.name.clone()
            } else {
                format!("{}_{}", self.config.prefix, metric.name)
            };

            let labels_str = if metric.labels.is_empty() {
                String::new()
            } else {
                let labels: Vec<String> = metric.labels
                    .iter()
                    .map(|(k, v)| format!("{}=\"{}\"", k, v))
                    .collect();
                format!("{{{}}}", labels.join(","))
            };

            let help_comment = format!("# HELP {} {}", prometheus_name, prometheus_name);
            let type_comment = format!("# TYPE {} {}", prometheus_name, self.metric_type_to_prometheus(&metric.metric_type));

            output.push_str(&help_comment);
            output.push('\n');
            output.push_str(&type_comment);
            output.push('\n');

            match metric.metric_type {
                MetricType::Counter | MetricType::Gauge => {
                    let metric_line = if self.config.include_timestamp {
                        format!("{}{} {} {}", prometheus_name, labels_str, metric.value, metric.timestamp)
                    } else {
                        format!("{}{} {}", prometheus_name, labels_str, metric.value)
                    };
                    output.push_str(&metric_line);
                    output.push('\n');
                }
                MetricType::Histogram | MetricType::Summary => {
                    // Simplified export for histogram/summary
                    let metric_line = if self.config.include_timestamp {
                        format!("{}{} {} {}", prometheus_name, labels_str, metric.value, metric.timestamp)
                    } else {
                        format!("{}{} {}", prometheus_name, labels_str, metric.value)
                    };
                    output.push_str(&metric_line);
                    output.push('\n');
                }
            }
        }

        output
    }

    /// Export metrics as JSON
    pub fn export_json(&self, metrics: &HashMap<String, Metric>) -> String {
        if !self.config.enabled {
            return String::new();
        }

        serde_json::to_string(metrics).unwrap_or_default()
    }

    /// Convert metric type to Prometheus type
    fn metric_type_to_prometheus(&self, metric_type: &MetricType) -> &'static str {
        match metric_type {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
            MetricType::Summary => "summary",
        }
    }
}

/// Exporter statistics
#[derive(Debug, Clone)]
pub struct ExporterStats {
    pub metrics_exported: usize,
    pub export_duration_ms: u64,
    pub last_export_timestamp: u64,
}

/// Health checker for metrics
#[derive(Debug)]
pub struct HealthChecker {
    provider: std::sync::Arc<crate::metrics::MetricsProvider>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(provider: std::sync::Arc<crate::metrics::MetricsProvider>) -> Self {
        Self { provider }
    }

    /// Check health of the metrics system
    pub fn check_health(&self) -> HealthStatus {
        let stats = self.provider.get_stats();
        
        // Basic health checks
        let healthy = stats.total_metrics < self.provider.config.max_metrics;
        let message = if healthy {
            "Metrics provider is healthy".to_string()
        } else {
            format!("Metrics provider has {} metrics (max: {})", stats.total_metrics, self.provider.config.max_metrics)
        };

        HealthStatus {
            healthy,
            message,
            uptime_seconds: stats.uptime_seconds,
            metrics_count: stats.total_metrics,
        }
    }
}

/// Health status
#[derive(Debug, Clone)]
pub struct HealthStatus {
    pub healthy: bool,
    pub message: String,
    pub uptime_seconds: u64,
    pub metrics_count: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::{MetricsProvider, MetricsConfig};

    #[test]
    fn test_prometheus_export() {
        let config = PrometheusExporterConfig {
            enabled: true,
            prefix: "test".to_string(),
            include_timestamp: false,
        };
        let exporter = PrometheusExporter::new(config);

        let mut metrics = HashMap::new();
        metrics.insert(
            "counter".to_string(),
            Metric::new("counter".to_string(), 42.0, MetricType::Counter),
        );
        metrics.insert(
            "gauge".to_string(),
            Metric::new("gauge".to_string(), 3.14, MetricType::Gauge),
        );

        let exported = exporter.export_metrics(&metrics);
        
        assert!(exported.contains("# HELP test_counter test_counter"));
        assert!(exported.contains("# TYPE test_counter counter"));
        assert!(exported.contains("test_counter 42"));
        assert!(exported.contains("# HELP test_gauge test_gauge"));
        assert!(exported.contains("# TYPE test_gauge gauge"));
        assert!(exported.contains("test_gauge 3.14"));
    }

    #[test]
    fn test_prometheus_export_with_labels() {
        let config = PrometheusExporterConfig {
            enabled: true,
            prefix: "".to_string(),
            include_timestamp: false,
        };
        let exporter = PrometheusExporter::new(config);

        let mut metrics = HashMap::new();
        let mut labels = HashMap::new();
        labels.insert("type".to_string(), "test".to_string());
        labels.insert("source".to_string(), "unit".to_string());

        metrics.insert(
            "labeled_metric".to_string(),
            Metric::with_labels("labeled_metric".to_string(), 100.0, MetricType::Counter, labels),
        );

        let exported = exporter.export_metrics(&metrics);
        
        assert!(exported.contains("labeled_metric{type=\"test\",source=\"unit\"} 100") || 
               exported.contains("labeled_metric{source=\"unit\",type=\"test\"} 100"));
    }

    #[test]
    fn test_prometheus_export_disabled() {
        let config = PrometheusExporterConfig {
            enabled: false,
            prefix: "test".to_string(),
            include_timestamp: false,
        };
        let exporter = PrometheusExporter::new(config);

        let metrics = HashMap::new();
        let exported = exporter.export_metrics(&metrics);
        
        assert!(exported.is_empty());
    }

    #[test]
    fn test_json_export() {
        let config = PrometheusExporterConfig {
            enabled: true,
            prefix: "".to_string(),
            include_timestamp: false,
        };
        let exporter = PrometheusExporter::new(config);

        let mut metrics = HashMap::new();
        metrics.insert(
            "test".to_string(),
            Metric::new("test".to_string(), 42.0, MetricType::Counter),
        );

        let exported = exporter.export_json(&metrics);
        
        assert!(!exported.is_empty());
        // Should be valid JSON
        assert!(exported.starts_with('{'));
        assert!(exported.ends_with('}'));
    }

    #[test]
    fn test_health_checker() {
        let config = MetricsConfig {
            enabled: true,
            max_metrics: 100,
            cleanup_interval_secs: 300,
        };
        let provider = std::sync::Arc::new(MetricsProvider::new(config));
        let checker = HealthChecker::new(provider);

        let health = checker.check_health();
        assert!(health.healthy);
        assert_eq!(health.metrics_count, 0);
        assert!(health.message.contains("healthy"));
    }
}
