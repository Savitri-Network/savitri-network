//! Metrics system for Savitri Network Core
//! 
//! This module provides a lightweight metrics system for the core library,
//! including a metrics provider, exporter, and manifest generation.

pub mod provider;
pub mod exporter;
pub mod manifest;

// Re-export main types and functions
pub use provider::{
    MetricsProvider, MetricsConfig, Metric, MetricType, ProviderStats,
    savitri_metrics, utils
};
pub use exporter::{
    PrometheusExporter, PrometheusExporterConfig, ExporterStats, HealthChecker, HealthStatus
};
pub use manifest::{
    MetricsManifest, MetricDefinition, LabelDefinition, MetricCategory,
    PlatformInfo, SecurityInfo, ThresholdDefinition, ThresholdType,
    ManifestGenerator
};
