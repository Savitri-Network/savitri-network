use crate::config::RateLimitConfig;
use std::str::FromStr;

/// Archive configuration for rate limiting
#[derive(Debug, Clone, Default)]
pub struct ArchiveConfig {
    pub max_history_blocks: Option<usize>,
    pub max_history_span: Option<u64>,
    pub max_history_reply_bytes: Option<usize>,
    pub max_proof_bytes: Option<usize>,
    pub requests_per_minute: Option<u32>,
}

pub fn archive_limits(cfg: &RateLimitConfig) -> ArchiveConfig {
    let mut limits = ArchiveConfig::default();
    if let Some(v) = cfg.max_history_blocks {
        limits.max_history_blocks = Some(v as usize);
    }
    if let Some(v) = cfg.max_history_span {
        limits.max_history_span = Some(v);
    }
    if let Some(v) = cfg.max_history_reply_bytes {
        limits.max_history_reply_bytes = Some(v);
    }
    limits.max_proof_bytes = cfg.max_proof_bytes;
    limits.requests_per_minute = cfg.requests_per_minute;
    limits
}

pub fn parse_bootstrap(peers: &[String]) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for entry in peers {
        if let Some((id, addr)) = entry.split_once('@') {
            if libp2p::PeerId::from_str(id).is_ok() && addr.parse::<libp2p::Multiaddr>().is_ok() {
                out.push((id.to_string(), addr.to_string()));
            }
        }
    }
    out
}
