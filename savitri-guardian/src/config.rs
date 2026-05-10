use anyhow::{Context, Result};
use serde::Deserialize;
use std::{fs, path::Path};

#[derive(Debug, Clone, Deserialize, Default)]
pub struct RateLimitConfig {
    pub max_history_blocks: Option<u32>,
    pub max_history_span: Option<u64>,
    pub max_history_reply_bytes: Option<usize>,
    pub max_proof_bytes: Option<usize>,
    pub requests_per_minute: Option<u32>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct MonitoringConfig {
    /// Disk usage alert threshold in GB
    pub disk_alert_threshold_gb: Option<f64>,
    /// Metrics collection interval in seconds
    pub metrics_interval_secs: Option<u64>,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct GuardianConfig {
    pub db_path: Option<String>,
    pub listen_port: Option<u16>,
    pub network_key_path: Option<String>,
    #[serde(default)]
    pub bootstrap_peers: Vec<String>,
    #[serde(default)]
    pub rate_limits: RateLimitConfig,
    #[serde(default)]
    pub monitoring: MonitoringConfig,
}

impl GuardianConfig {
    pub fn load(path: &Path) -> Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let bytes =
            fs::read(path).with_context(|| format!("failed to read config {}", path.display()))?;
        let raw = String::from_utf8_lossy(&bytes);
        let cfg: Self =
            toml::from_str(&raw).with_context(|| format!("failed to parse {}", path.display()))?;
        Ok(cfg)
    }
}
