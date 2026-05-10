//! Metrics manifest generator for Savitri Network
//! 
//! This module provides utilities for generating metrics manifests
//! that describe available metrics and their properties.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metric definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricDefinition {
    pub name: String,
    pub description: String,
    pub metric_type: String,
    pub unit: String,
    pub labels: Vec<LabelDefinition>,
}

/// Label definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LabelDefinition {
    pub name: String,
    pub description: String,
    pub allowed_values: Vec<String>,
}

/// Metric category
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MetricCategory {
    Blockchain,
    Network,
    System,
    Consensus,
    Mempool,
    Storage,
    Custom(String),
}

/// Platform information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlatformInfo {
    pub platform: String,
    pub architecture: String,
    pub version: String,
    pub build_timestamp: u64,
}

/// Security information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityInfo {
    pub metrics_enabled: bool,
    pub export_endpoint: String,
    pub authentication_required: bool,
    pub rate_limiting: bool,
}

/// Threshold definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdDefinition {
    pub metric_name: String,
    pub warning_threshold: f64,
    pub critical_threshold: f64,
    pub operator: String, // "gt", "lt", "eq", "ne"
}

/// Threshold type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ThresholdType {
    Warning,
    Critical,
}

/// Metrics manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsManifest {
    pub version: String,
    pub generated_at: u64,
    pub platform: PlatformInfo,
    pub security: SecurityInfo,
    pub categories: HashMap<String, Vec<MetricDefinition>>,
    pub thresholds: Vec<ThresholdDefinition>,
}

impl MetricsManifest {
    /// Generate a complete metrics manifest
    pub fn generate_manifest() -> Self {
        let platform = PlatformInfo {
            platform: std::env::consts::OS.to_string(),
            architecture: std::env::consts::ARCH.to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            build_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        };

        let security = SecurityInfo {
            metrics_enabled: std::env::var("SAVITRI_METRICS_ENABLED")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(false),
            export_endpoint: "localhost:9090/metrics".to_string(),
            authentication_required: false,
            rate_limiting: false,
        };

        let mut categories = HashMap::new();

        // Blockchain metrics
        categories.insert(
            "blockchain".to_string(),
            vec![
                MetricDefinition {
                    name: "block_height".to_string(),
                    description: "Current blockchain height".to_string(),
                    metric_type: "counter".to_string(),
                    unit: "blocks".to_string(),
                    labels: vec![],
                },
                MetricDefinition {
                    name: "tx_per_block".to_string(),
                    description: "Number of transactions per block".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "transactions".to_string(),
                    labels: vec![],
                },
                MetricDefinition {
                    name: "block_time".to_string(),
                    description: "Time to produce a block".to_string(),
                    metric_type: "histogram".to_string(),
                    unit: "milliseconds".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "proposer".to_string(),
                            description: "Block proposer ID".to_string(),
                            allowed_values: vec![],
                        },
                    ],
                },
            ],
        );

        // Network metrics
        categories.insert(
            "network".to_string(),
            vec![
                MetricDefinition {
                    name: "peer_count".to_string(),
                    description: "Number of connected peers".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "peers".to_string(),
                    labels: vec![],
                },
                MetricDefinition {
                    name: "network_latency".to_string(),
                    description: "Network latency in milliseconds".to_string(),
                    metric_type: "histogram".to_string(),
                    unit: "milliseconds".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "peer_id".to_string(),
                            description: "Peer identifier".to_string(),
                            allowed_values: vec![],
                        },
                    ],
                },
                MetricDefinition {
                    name: "bandwidth_usage".to_string(),
                    description: "Network bandwidth usage".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "bytes_per_second".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "direction".to_string(),
                            description: "Upload or download".to_string(),
                            allowed_values: vec!["upload".to_string(), "download".to_string()],
                        },
                    ],
                },
            ],
        );

        // System metrics
        categories.insert(
            "system".to_string(),
            vec![
                MetricDefinition {
                    name: "cpu_usage".to_string(),
                    description: "CPU usage percentage".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "percent".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "core".to_string(),
                            description: "CPU core number".to_string(),
                            allowed_values: vec!["0".to_string(), "1".to_string(), "2".to_string(), "3".to_string()],
                        },
                    ],
                },
                MetricDefinition {
                    name: "memory_usage".to_string(),
                    description: "Memory usage in bytes".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "bytes".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "type".to_string(),
                            description: "Memory type".to_string(),
                            allowed_values: vec!["heap".to_string(), "stack".to_string()],
                        },
                    ],
                },
                MetricDefinition {
                    name: "disk_usage".to_string(),
                    description: "Disk usage in bytes".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "bytes".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "mount_point".to_string(),
                            description: "Disk mount point".to_string(),
                            allowed_values: vec![],
                        },
                    ],
                },
            ],
        );

        // Consensus metrics
        categories.insert(
            "consensus".to_string(),
            vec![
                MetricDefinition {
                    name: "consensus_round_time".to_string(),
                    description: "Time to complete consensus round".to_string(),
                    metric_type: "histogram".to_string(),
                    unit: "milliseconds".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "round_type".to_string(),
                            description: "Type of consensus round".to_string(),
                            allowed_values: vec!["normal".to_string(), "timeout".to_string(), "failed".to_string()],
                        },
                    ],
                },
                MetricDefinition {
                    name: "validator_count".to_string(),
                    description: "Number of active validators".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "validators".to_string(),
                    labels: vec![],
                },
                MetricDefinition {
                    name: "consensus_messages".to_string(),
                    description: "Number of consensus messages".to_string(),
                    metric_type: "counter".to_string(),
                    unit: "messages".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "message_type".to_string(),
                            description: "Type of consensus message".to_string(),
                            allowed_values: vec!["proposal".to_string(), "vote".to_string(), "certificate".to_string()],
                        },
                    ],
                },
            ],
        );

        // Mempool metrics
        categories.insert(
            "mempool".to_string(),
            vec![
                MetricDefinition {
                    name: "mempool_size".to_string(),
                    description: "Number of transactions in mempool".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "transactions".to_string(),
                    labels: vec![],
                },
                MetricDefinition {
                    name: "mempool_tx_rate".to_string(),
                    description: "Transaction rate in mempool".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "transactions_per_second".to_string(),
                    labels: vec![],
                },
                MetricDefinition {
                    name: "mempool_fee_rate".to_string(),
                    description: "Average fee rate in mempool".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "wei".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "priority".to_string(),
                            description: "Transaction priority level".to_string(),
                            allowed_values: vec!["high".to_string(), "medium".to_string(), "low".to_string()],
                        },
                    ],
                },
            ],
        );

        // Storage metrics
        categories.insert(
            "storage".to_string(),
            vec![
                MetricDefinition {
                    name: "storage_size".to_string(),
                    description: "Storage database size".to_string(),
                    metric_type: "gauge".to_string(),
                    unit: "bytes".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "type".to_string(),
                            description: "Storage type".to_string(),
                            allowed_values: vec!["rocksdb".to_string(), "memory".to_string()],
                        },
                    ],
                },
                MetricDefinition {
                    name: "storage_operations".to_string(),
                    description: "Number of storage operations".to_string(),
                    metric_type: "counter".to_string(),
                    unit: "operations".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "operation".to_string(),
                            description: "Storage operation type".to_string(),
                            allowed_values: vec!["read".to_string(), "write".to_string(), "delete".to_string()],
                        },
                    ],
                },
                MetricDefinition {
                    name: "storage_latency".to_string(),
                    description: "Storage operation latency".to_string(),
                    metric_type: "histogram".to_string(),
                    unit: "microseconds".to_string(),
                    labels: vec![
                        LabelDefinition {
                            name: "operation".to_string(),
                            description: "Storage operation type".to_string(),
                            allowed_values: vec!["get".to_string(), "put".to_string(), "delete".to_string()],
                        },
                    ],
                },
            ],
        );

        let thresholds = vec![
            ThresholdDefinition {
                metric_name: "cpu_usage".to_string(),
                warning_threshold: 80.0,
                critical_threshold: 95.0,
                operator: "gt".to_string(),
            },
            ThresholdDefinition {
                metric_name: "memory_usage".to_string(),
                warning_threshold: 0.8, // 80% of available memory
                critical_threshold: 0.95,
                operator: "gt".to_string(),
            },
            ThresholdDefinition {
                metric_name: "network_latency".to_string(),
                warning_threshold: 100.0, // 100ms
                critical_threshold: 500.0, // 500ms
                operator: "gt".to_string(),
            },
            ThresholdDefinition {
                metric_name: "consensus_round_time".to_string(),
                warning_threshold: 1000.0, // 1 second
                critical_threshold: 5000.0, // 5 seconds
                operator: "gt".to_string(),
            },
        ];

        Self {
            version: "1.0".to_string(),
            generated_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            platform,
            security,
            categories,
            thresholds,
        }
    }

    /// Validate the manifest
    pub fn validate_manifest(manifest: &MetricsManifest) -> anyhow::Result<()> {
        // Check required fields
        if manifest.version.is_empty() {
            return Err(anyhow::anyhow!("Manifest version is required"));
        }

        if manifest.categories.is_empty() {
            return Err(anyhow::anyhow!("Manifest must have at least one category"));
        }

        // Validate each category
        for (category_name, metrics) in &manifest.categories {
            if category_name.is_empty() {
                return Err(anyhow::anyhow!("Category name cannot be empty"));
            }

            for metric in metrics {
                if metric.name.is_empty() {
                    return Err(anyhow::anyhow!("Metric name cannot be empty"));
                }

                if metric.description.is_empty() {
                    return Err(anyhow::anyhow!("Metric description cannot be empty"));
                }

                if !["counter", "gauge", "histogram", "summary"].contains(&metric.metric_type.as_str()) {
                    return Err(anyhow::anyhow!("Invalid metric type: {}", metric.metric_type));
                }
            }
        }

        // Validate thresholds
        for threshold in &manifest.thresholds {
            if threshold.metric_name.is_empty() {
                return Err(anyhow::anyhow!("Threshold metric name cannot be empty"));
            }

            if !["gt", "lt", "eq", "ne"].contains(&threshold.operator.as_str()) {
                return Err(anyhow::anyhow!("Invalid threshold operator: {}", threshold.operator));
            }

            if threshold.warning_threshold >= threshold.critical_threshold {
                return Err(anyhow::anyhow!("Warning threshold must be less than critical threshold"));
            }
        }

        Ok(())
    }

    /// Save manifest to file
    pub fn save_manifest(manifest: &MetricsManifest, path: &str) -> anyhow::Result<()> {
        let json = serde_json::to_string_pretty(manifest)?;
        std::fs::write(path, json)?;
        Ok(())
    }

    /// Load manifest from file
    pub fn load_manifest(path: &str) -> anyhow::Result<MetricsManifest> {
        let json = std::fs::read_to_string(path)?;
        let manifest: MetricsManifest = serde_json::from_str(&json)?;
        Ok(manifest)
    }
}

/// Manifest generator utilities
pub struct ManifestGenerator;

impl ManifestGenerator {
    pub fn generate_and_validate() -> anyhow::Result<MetricsManifest> {
        let manifest = MetricsManifest::generate_manifest();
        MetricsManifest::validate_manifest(&manifest)?;
        Ok(manifest)
    }

    /// Generate manifest and save to file
    pub fn generate_and_save(path: &str) -> anyhow::Result<()> {
        let manifest = Self::generate_and_validate()?;
        MetricsManifest::save_manifest(&manifest, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_manifest_generation() {
        let manifest = MetricsManifest::generate_manifest();
        
        assert!(!manifest.version.is_empty());
        assert!(!manifest.categories.is_empty());
        assert!(!manifest.thresholds.is_empty());
        
        // Check that blockchain category exists
        assert!(manifest.categories.contains_key("blockchain"));
        let blockchain_metrics = &manifest.categories["blockchain"];
        assert!(!blockchain_metrics.is_empty());
        
        // Check that block_height metric exists
        let block_height = blockchain_metrics.iter().find(|m| m.name == "block_height");
        assert!(block_height.is_some());
        let block_height = block_height.unwrap();
        assert_eq!(block_height.metric_type, "counter");
        assert_eq!(block_height.unit, "blocks");
    }

    #[test]
    fn test_manifest_validation() {
        let manifest = MetricsManifest::generate_manifest();
        assert!(MetricsManifest::validate_manifest(&manifest).is_ok());
    }

    #[test]
    fn test_manifest_validation_invalid() {
        let mut manifest = MetricsManifest::generate_manifest();
        manifest.version = "".to_string(); // Invalid empty version
        
        assert!(MetricsManifest::validate_manifest(&manifest).is_err());
    }

    #[test]
    fn test_threshold_validation() {
        let manifest = MetricsManifest::generate_manifest();
        
        // All thresholds should be valid
        for threshold in &manifest.thresholds {
            assert!(threshold.warning_threshold < threshold.critical_threshold);
            assert!(["gt", "lt", "eq", "ne"].contains(&threshold.operator.as_str()));
        }
    }

    #[test]
    fn test_manifest_save_load() {
        let manifest = MetricsManifest::generate_manifest();
        let temp_path = "test_manifest.json";
        
        // Save and load
        assert!(MetricsManifest::save_manifest(&manifest, temp_path).is_ok());
        let loaded = MetricsManifest::load_manifest(temp_path).unwrap();
        
        // Compare
        assert_eq!(manifest.version, loaded.version);
        assert_eq!(manifest.categories.len(), loaded.categories.len());
        
        // Clean up
        std::fs::remove_file(temp_path).unwrap();
    }

    #[test]
    fn test_manifest_generator() {
        let temp_path = "test_manifest_generator.json";
        
        // Generate and save
        assert!(ManifestGenerator::generate_and_save(temp_path).is_ok());
        
        // Verify file exists
        assert!(std::path::Path::new(temp_path).exists());
        
        // Clean up
        std::fs::remove_file(temp_path).unwrap();
    }
}
