//! Modulo Metrics Decentralizzato per Savitri Network
//!
//!
//! Caratteristiche:
//! - Overhead minimo (<1% CPU)
//! - 200+ metriche predefinite
//! - Configurazione via .toml
//! - Nessun riferimento IP/URL esterno

pub mod exporter;
pub mod manifest;
pub mod provider;

pub use exporter::{
    ExporterStats, HealthChecker, HealthStatus, PrometheusExporter, PrometheusExporterConfig,
};
pub use manifest::{
    LabelDefinition, ManifestGenerator, MetricCategory, MetricDefinition, MetricsManifest,
    PlatformInfo, SecurityInfo, ThresholdDefinition, ThresholdType,
};
pub use provider::{
    Metric, MetricType, MetricsConfig, MetricsProvider, ProviderStats, SavitriMetrics,
};

/// Metrics utilities and helper functions
///
/// This module provides utility functions for working with metrics,
/// including factory functions for creating different metric types
/// and helper functions for common metrics operations.
#[doc(alias = "helpers")]
pub mod utils {
    use super::*;

    /// Create a new metrics instance
    pub fn savitri_metrics() -> SavitriMetrics {
        SavitriMetrics::new()
    }

    /// Get default metrics configuration
    pub fn default_metrics_config() -> MetricsConfig {
        MetricsConfig::default()
    }

    /// Initialize metrics with default configuration
    pub fn init_metrics() -> MetricsManager {
        MetricsManager::new(default_metrics_config())
    }

    /// Create a custom metric
    pub fn create_metric(name: &str, help: &str, metric_type: MetricType) -> Metric {
        Metric::new(name.to_string(), help.to_string(), metric_type)
    }

    /// Create a counter metric
    pub fn create_counter(name: &str, help: &str) -> Metric {
        create_metric(name, help, MetricType::Counter)
    }

    /// Create a gauge metric
    pub fn create_gauge(name: &str, help: &str) -> Metric {
        create_metric(name, help, MetricType::Gauge)
    }

    /// Create a histogram metric
    pub fn create_histogram(name: &str, help: &str) -> Metric {
        create_metric(name, help, MetricType::Histogram)
    }

    /// Create a summary metric
    pub fn create_summary(name: &str, help: &str) -> Metric {
        create_metric(name, help, MetricType::Summary)
    }

    /// Register a metric with the provider
    pub async fn register_metric(
        provider: &MetricsProvider,
        name: String,
        value: f64,
        metric_type: MetricType,
    ) -> Result<(), String> {
        provider.register_metric(name, value, metric_type).await;
        Ok(())
    }

    /// Record a counter observation
    pub async fn record_counter(
        provider: &MetricsProvider,
        name: &str,
        value: f64,
    ) -> Result<(), String> {
        provider.record_counter(name, value).await;
        Ok(())
    }

    /// Set a gauge value
    pub async fn set_gauge(
        provider: &MetricsProvider,
        name: &str,
        value: f64,
    ) -> Result<(), String> {
        provider.set_gauge(name, value).await;
        Ok(())
    }

    /// Record a histogram observation
    pub async fn record_histogram(
        provider: &MetricsProvider,
        name: &str,
        value: f64,
    ) -> Result<(), String> {
        provider.record_histogram(name, value).await;
        Ok(())
    }

    /// Record a summary observation
    pub async fn record_summary(
        provider: &MetricsProvider,
        name: &str,
        value: f64,
    ) -> Result<(), String> {
        provider.record_summary(name, value).await;
        Ok(())
    }

    /// Get metric value
    pub async fn get_metric_value(provider: &MetricsProvider, name: &str) -> Option<f64> {
        provider.get_metric_value(name).await
    }

    /// Get all metric values
    pub async fn get_all_metrics(
        provider: &MetricsProvider,
    ) -> std::collections::HashMap<String, f64> {
        provider.get_all_metrics().await
    }

    /// Reset all metrics
    pub async fn reset_metrics(provider: &MetricsProvider) -> Result<(), String> {
        provider.reset_all_metrics().await;
        Ok(())
    }

    /// Export metrics to Prometheus format
    pub async fn export_prometheus(provider: &MetricsProvider) -> Result<String, String> {
        Ok(provider.export_prometheus().await)
    }

    /// Check if metric exists
    pub async fn metric_exists(provider: &MetricsProvider, name: &str) -> bool {
        provider.metric_exists(name).await
    }

    /// Remove a metric
    pub async fn remove_metric(provider: &MetricsProvider, name: &str) -> Result<bool, String> {
        Ok(provider.remove_metric(name).await)
    }

    /// Get provider statistics
    pub async fn get_provider_stats(provider: &MetricsProvider) -> ProviderStats {
        provider.get_stats().await
    }

    /// Create a metrics manifest
    pub fn create_metrics_manifest() -> MetricsManifest {
        // Return a default manifest since MetricsManifest::new() doesn't exist
        MetricsManifest {
            version: "1.0.0".to_string(),
            generated_at: chrono::Utc::now().to_rfc3339(),
            platform: PlatformInfo {
                name: "Savitri Network".to_string(),
                version: "1.0.0".to_string(),
                deployment_type: "decentralized".to_string(),
                security_principles: vec![
                    "local_only".to_string(),
                    "no_external_ips".to_string(),
                    "minimal_overhead".to_string(),
                ],
            },
            categories: vec![],
            metrics: vec![],
            security: SecurityInfo {
                principles: vec![
                    "local_only".to_string(),
                    "no_external_ips".to_string(),
                    "minimal_overhead".to_string(),
                ],
                access_controls: vec![
                    "localhost_only".to_string(),
                    "max_connections_10".to_string(),
                ],
                exposure_limits: vec![
                    "no_platform_data".to_string(),
                    "no_sensitive_info".to_string(),
                ],
                recommendations: vec![
                    "Monitor only localhost".to_string(),
                    "Keep overhead < 1%".to_string(),
                ],
            },
        }
    }

    /// Generate metrics manifest
    pub fn generate_manifest(_provider: &MetricsProvider) -> MetricsManifest {
        ManifestGenerator::generate_manifest()
    }

    /// Validate metrics configuration
    pub fn validate_config(config: &MetricsConfig) -> Result<(), String> {
        if config.port < 1024 {
            return Err("Port must be between 1024 and 65535".to_string());
        }
        // Note: port > 65535 check is unnecessary as u16 cannot exceed 65535 by definition
        Ok(())
    }

    /// Start metrics exporter
    pub fn start_exporter(config: PrometheusExporterConfig) -> Result<PrometheusExporter, String> {
        let provider_config = default_metrics_config();
        let provider = Arc::new(MetricsProvider::new(provider_config));
        Ok(PrometheusExporter::new(config, provider))
    }

    /// Stop metrics exporter
    pub async fn stop_exporter(exporter: &PrometheusExporter) -> Result<(), String> {
        exporter.stop().await;
        Ok(())
    }

    /// Check exporter health
    pub async fn check_exporter_health(exporter: &PrometheusExporter) -> HealthStatus {
        exporter.health_check().await
    }

    /// Get exporter statistics
    pub async fn get_exporter_stats(exporter: &PrometheusExporter) -> ExporterStats {
        exporter.get_stats().await
    }

    /// Create default exporter configuration
    pub fn default_exporter_config() -> PrometheusExporterConfig {
        PrometheusExporterConfig::default()
    }

    /// Initialize metrics with exporter
    pub fn init_metrics_with_exporter() -> (MetricsManager, PrometheusExporter) {
        let metrics_config = default_metrics_config();
        let exporter_config = default_exporter_config();

        let manager = MetricsManager::new(metrics_config);
        let provider = Arc::new(MetricsProvider::new(default_metrics_config()));
        let exporter = PrometheusExporter::new(exporter_config, provider);

        (manager, exporter)
    }
}

use log::{error, info, warn};
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct MetricsManager {
    provider: Arc<MetricsProvider>,
    exporter: Option<Arc<PrometheusExporter>>,
    health_checker: Arc<HealthChecker>,
    running: Arc<RwLock<bool>>,
}

impl MetricsManager {
    pub fn new(config: MetricsConfig) -> Self {
        info!("Creating MetricsManager with decentralized design");

        let provider = Arc::new(MetricsProvider::new(config.clone()));
        let health_checker = Arc::new(HealthChecker::new(provider.clone()));

        Self {
            provider,
            exporter: None,
            health_checker,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Configure and start the Prometheus exporter
    pub async fn start_exporter(
        &mut self,
        exporter_config: PrometheusExporterConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.provider.is_enabled() {
            warn!("Metrics provider is disabled, not starting exporter");
            return Ok(());
        }

        if self.exporter.is_some() {
            warn!("Exporter already started");
            return Ok(());
        }

        let exporter = Arc::new(PrometheusExporter::new(
            exporter_config,
            self.provider.clone(),
        ));

        let exporter_clone = exporter.clone();
        let running = self.running.clone();
        tokio::spawn(async move {
            if let Err(e) = exporter_clone.start().await {
                error!("Exporter error: {:?}", e);
            }

            // Segnala che l'exporter si è fermato
            {
                let mut is_running = running.write().await;
                *is_running = false;
            }
        });

        self.exporter = Some(exporter);

        {
            let mut running = self.running.write().await;
            *running = true;
        }

        info!("Metrics exporter started successfully");
        Ok(())
    }

    /// Ferma il Metrics Manager
    pub async fn stop(&self) {
        info!("Stopping MetricsManager");

        // Ferma l'exporter se attivo
        if let Some(exporter) = &self.exporter {
            exporter.stop().await;
        }

        // Segnala che non è più in esecuzione
        {
            let mut running = self.running.write().await;
            *running = false;
        }

        info!("MetricsManager stopped");
    }

    /// Registra una metrica semplice
    pub async fn register_metric(&self, name: String, value: f64, metric_type: MetricType) {
        self.provider
            .register_metric(name, value, metric_type)
            .await;
    }

    /// Registra una metrica con labels
    pub async fn register_metric_with_labels(
        &self,
        name: String,
        value: f64,
        metric_type: MetricType,
        labels: std::collections::HashMap<String, String>,
    ) {
        self.provider
            .register_metric_with_labels(name, value, metric_type, labels)
            .await;
    }

    /// Registra metriche di base per Savitri
    pub async fn register_savitri_metrics(&self) {
        if !self.provider.is_enabled() {
            return;
        }

        // Metriche blockchain
        self.register_metric(
            SavitriMetrics::BLOCK_HEIGHT.to_string(),
            0.0,
            MetricType::Gauge,
        )
        .await;

        self.register_metric(
            SavitriMetrics::TX_PER_BLOCK.to_string(),
            0.0,
            MetricType::Gauge,
        )
        .await;

        // Metriche mempool
        self.register_metric(
            SavitriMetrics::MEMPOOL_SIZE.to_string(),
            0.0,
            MetricType::Gauge,
        )
        .await;

        // Metriche network
        self.register_metric(
            SavitriMetrics::PEER_COUNT.to_string(),
            0.0,
            MetricType::Gauge,
        )
        .await;

        // Metriche system
        self.register_metric(
            SavitriMetrics::CPU_USAGE.to_string(),
            0.0,
            MetricType::Gauge,
        )
        .await;

        self.register_metric(
            SavitriMetrics::MEMORY_USAGE.to_string(),
            0.0,
            MetricType::Gauge,
        )
        .await;

        info!("Registered basic Savitri metrics");
    }

    /// Ottiene statistiche complete
    pub async fn get_stats(&self) -> ManagerStats {
        let provider_stats = self.provider.get_stats().await;
        let exporter_stats = if let Some(exporter) = &self.exporter {
            Some(exporter.get_stats().await)
        } else {
            None
        };
        let health_status = self.health_checker.check_health().await;
        let running = *self.running.read().await;

        ManagerStats {
            running,
            provider_stats,
            exporter_stats,
            health_status,
        }
    }

    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    pub fn get_provider(&self) -> Arc<MetricsProvider> {
        self.provider.clone()
    }

    pub async fn generate_and_save_manifest(
        &self,
        path: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let manifest = ManifestGenerator::generate_manifest();

        if let Err(e) = ManifestGenerator::validate_manifest(&manifest) {
            return Err(format!("Invalid manifest: {}", e).into());
        }

        // Salva il manifesto
        ManifestGenerator::save_manifest(&manifest, path)?;

        info!("Metrics manifest generated and saved to: {}", path);
        Ok(())
    }

    /// Pulisce metriche vecchie
    pub async fn cleanup_old_metrics(&self, max_age: std::time::Duration) {
        self.provider.cleanup_old_metrics(max_age).await;
    }
}

/// Statistiche complete of the Metrics Manager
#[derive(Debug, Clone)]
pub struct ManagerStats {
    /// Manager in esecuzione
    pub running: bool,
    /// Statistiche of the provider
    pub provider_stats: ProviderStats,
    /// Statistiche dell'exporter (se attivo)
    pub exporter_stats: Option<ExporterStats>,
    /// Stato di salute
    pub health_status: HealthStatus,
}

pub fn create_metrics_manager_from_env() -> MetricsManager {
    let config = MetricsConfig::from_env();
    MetricsManager::new(config)
}

pub fn create_default_metrics_manager() -> MetricsManager {
    let config = MetricsConfig::default();
    MetricsManager::new(config)
}
