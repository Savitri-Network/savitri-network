#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use sha2::Digest;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::RwLock;

use ed25519_dalek::SigningKey as DalekKeypair;
use libp2p::PeerId;

// FixedPoint maximum value for resource calculations
pub const MAX: FixedPoint = FixedPoint(u64::MAX);

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct FixedPoint(u64);

impl FixedPoint {
    pub fn raw(&self) -> u64 {
        self.0
    }

    pub fn from_basis_points(bp: u32) -> Option<Self> {
        Some(Self(bp as u64))
    }

    pub fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    pub fn from_raw_u32(raw: u32) -> Self {
        Self(raw as u64)
    }

    pub fn from_pou_index(index: u16) -> Self {
        Self(index as u64)
    }

    pub fn checked_mul(&self, other: FixedPoint) -> Option<FixedPoint> {
        Some(FixedPoint(self.0.saturating_mul(other.0)))
    }

    pub fn checked_add(&self, other: FixedPoint) -> Option<FixedPoint> {
        Some(FixedPoint(self.0.saturating_add(other.0)))
    }

    pub fn checked_sub(&self, other: FixedPoint) -> Option<FixedPoint> {
        Some(FixedPoint(self.0.saturating_sub(other.0)))
    }

    pub fn checked_div(&self, other: FixedPoint) -> Option<FixedPoint> {
        if other.0 == 0 {
            None
        } else {
            Some(FixedPoint(self.0.saturating_div(other.0)))
        }
    }

    pub fn min(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0.min(other.0))
    }

    pub fn max(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0.max(other.0))
    }

    pub fn saturating_mul(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0.saturating_mul(other.0))
    }

    pub fn saturating_add(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0.saturating_add(other.0))
    }

    pub fn saturating_sub(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0.saturating_sub(other.0))
    }
}

impl std::ops::Add for FixedPoint {
    type Output = FixedPoint;

    fn add(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0 + other.0)
    }
}

impl std::ops::Sub for FixedPoint {
    type Output = FixedPoint;

    fn sub(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0 - other.0)
    }
}

impl std::ops::Mul for FixedPoint {
    type Output = FixedPoint;

    fn mul(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0 * other.0)
    }
}

impl std::ops::Div for FixedPoint {
    type Output = FixedPoint;

    fn div(self, other: FixedPoint) -> FixedPoint {
        FixedPoint(self.0 / other.0.max(1))
    }
}

impl std::ops::Add<u64> for FixedPoint {
    type Output = FixedPoint;

    fn add(self, other: u64) -> FixedPoint {
        FixedPoint(self.0 + other)
    }
}

impl std::ops::Mul<u64> for FixedPoint {
    type Output = FixedPoint;

    fn mul(self, other: u64) -> FixedPoint {
        FixedPoint(self.0 * other)
    }
}

impl std::ops::Div<u64> for FixedPoint {
    type Output = FixedPoint;

    fn div(self, other: u64) -> FixedPoint {
        FixedPoint(self.0 / other.max(1))
    }
}

// Re-export ResourceClaim from p2p::types
pub use crate::p2p::types::ResourceClaim;

use tokio::{
    sync::mpsc,
    task,
    time::{self, MissedTickBehavior},
};
use tracing::{debug, warn};

use crate::{
    config::{ResourceConfig, ResourceWeights},
    logging::{flagged_message, FLAG_RESOURCE},
};

#[derive(Debug, Clone, Copy, Default)]
pub struct Overrides {
    pub bandwidth_mbps: Option<f64>,
    pub cpu_cores: Option<f64>,
    pub storage_gb: Option<f64>,
    pub epoch_secs: Option<u64>,
    pub tolerance: Option<f64>,
    pub weight_bandwidth: Option<f64>,
    pub weight_cpu: Option<f64>,
    pub weight_storage: Option<f64>,
}

#[derive(Debug, Clone)]
pub struct MonitorConfig {
    pub bandwidth_mbps: Option<f64>,
    pub cpu_cores: Option<f64>,
    pub storage_gb: Option<f64>,
    pub weights: ResourceWeights,
    pub epoch_secs: u64,
    pub tolerance: f64,
}

impl MonitorConfig {
    pub fn from_sources(file: Option<ResourceConfig>, overrides: Overrides) -> Self {
        let file_cfg = file.unwrap_or_default();
        let weights = ResourceWeights {
            bandwidth: overrides
                .weight_bandwidth
                .unwrap_or(file_cfg.weights.bandwidth),
            cpu: overrides.weight_cpu.unwrap_or(file_cfg.weights.cpu),
            storage: overrides.weight_storage.unwrap_or(file_cfg.weights.storage),
        }
        .normalized();

        let bandwidth_mbps = overrides
            .bandwidth_mbps
            .or(file_cfg.bandwidth_mbps)
            .filter(|v| *v > 0.0);
        let cpu_cores = overrides
            .cpu_cores
            .or(file_cfg.cpu_cores)
            .filter(|v| *v > 0.0);
        let storage_gb = overrides
            .storage_gb
            .or(file_cfg.storage_gb)
            .filter(|v| *v > 0.0);
        let epoch_secs = overrides.epoch_secs.unwrap_or(file_cfg.epoch_secs.max(1));
        let tolerance = overrides.tolerance.unwrap_or(file_cfg.tolerance.max(0.0));

        Self {
            bandwidth_mbps,
            cpu_cores,
            storage_gb,
            weights,
            epoch_secs,
            tolerance,
        }
    }
}

#[derive(Debug)]
pub enum ResourceEvent {
    Gossip {
        direction: TrafficDirection,
        bytes: usize,
    },
    BlockExecution {
        duration: Duration,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum TrafficDirection {
    Inbound,
    Outbound,
}

#[derive(Debug, Default)]
struct Aggregates {
    inbound_bytes: u64,
    outbound_bytes: u64,
    cpu_time_ns: u128,
}

impl Aggregates {
    fn record(&mut self, event: ResourceEvent) {
        match event {
            ResourceEvent::Gossip { direction, bytes } => match direction {
                TrafficDirection::Inbound => {
                    self.inbound_bytes = self.inbound_bytes.saturating_add(bytes as u64);
                }
                TrafficDirection::Outbound => {
                    self.outbound_bytes = self.outbound_bytes.saturating_add(bytes as u64);
                }
            },
            ResourceEvent::BlockExecution { duration } => {
                self.cpu_time_ns = self.cpu_time_ns.saturating_add(duration.as_nanos() as u128);
            }
        }
    }

    fn reset(&mut self) {
        self.inbound_bytes = 0;
        self.outbound_bytes = 0;
        self.cpu_time_ns = 0;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceSnapshot {
    pub score: FixedPoint,
    pub claim: Option<ResourceClaim>,
}

#[derive(Debug, Clone, Copy)]
struct FixedWeights {
    bandwidth: FixedPoint,
    cpu: FixedPoint,
    storage: FixedPoint,
}

impl FixedWeights {
    fn from(weights: ResourceWeights) -> Self {
        // Convert floating weights to integer basis points and renormalize to 10000.
        let mut bw = (weights.bandwidth * 10_000.0).round().clamp(0.0, 10_000.0) as u32;
        let mut cpu = (weights.cpu * 10_000.0).round().clamp(0.0, 10_000.0) as u32;
        let mut storage = (weights.storage * 10_000.0).round().clamp(0.0, 10_000.0) as u32;
        let total = bw.saturating_add(cpu).saturating_add(storage);
        if total == 0 {
            bw = 4000;
            cpu = 3000;
            storage = 3000;
        } else if total != 10_000 {
            bw = bw.saturating_mul(10_000) / total;
            cpu = cpu.saturating_mul(10_000) / total;
            storage = storage.saturating_mul(10_000) / total;
        }

        Self {
            bandwidth: FixedPoint::from_basis_points(bw).unwrap_or(MAX),
            cpu: FixedPoint::from_basis_points(cpu).unwrap_or(MAX),
            storage: FixedPoint::from_basis_points(storage).unwrap_or(MAX),
        }
    }
}

pub async fn run_resource_monitor(
    mut rx: mpsc::Receiver<ResourceEvent>,
    config: MonitorConfig,
    storage_path: PathBuf,
    local_peer: PeerId,
    local_node_id: [u8; 32],
    signer: Arc<DalekKeypair>,
    scores: Arc<RwLock<HashMap<PeerId, ResourceSnapshot>>>,
) {
    let mut ticker = time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut aggregates = Aggregates::default();
    let mut elapsed_in_epoch: u64 = 0;
    let mut epoch_index: u64 = 0;
    let weights = FixedWeights::from(config.weights);
    let tolerance_fp =
        FixedPoint::from_basis_points((config.tolerance * 10_000.0).round().max(0.0) as u32)
            .unwrap_or(MAX);

    // Convert declared resources to integer capacities to avoid floats in the scoring loop.
    let declared_bandwidth_bytes_per_sec = config.bandwidth_mbps.and_then(|mbps| {
        let value = (mbps * 1_000_000.0).round();
        (value > 0.0).then_some(value as u64)
    });
    let declared_cpu_capacity_ns = config.cpu_cores.and_then(|cores| {
        let capacity = (cores * config.epoch_secs as f64 * 1_000_000_000.0).round();
        (capacity > 0.0).then_some(capacity as u128)
    });
    let declared_storage_bytes = config
        .storage_gb
        .map(|gb| (gb * 1_000_000_000.0).round() as u64);

    loop {
        tokio::select! {
            maybe_evt = rx.recv() => {
                match maybe_evt {
                    Some(evt) => aggregates.record(evt),
                    None => break,
                }
            }
            _ = ticker.tick() => {
                elapsed_in_epoch = elapsed_in_epoch.saturating_add(1);
                if elapsed_in_epoch < config.epoch_secs {
                    continue;
                }

                // Bandwidth ratio: observed bytes per second vs declared.
                let total_bytes = aggregates.inbound_bytes.saturating_add(aggregates.outbound_bytes);
                let observed_bytes_per_sec = if config.epoch_secs > 0 {
                    total_bytes.saturating_div(config.epoch_secs)
                } else {
                    total_bytes
                };
                let bandwidth_ratio = match declared_bandwidth_bytes_per_sec {
                    Some(declared) if declared > 0 => ratio_to_fixed(observed_bytes_per_sec as u128, declared as u128),
                    _ => MAX,
                };

                // CPU ratio: time spent vs declared capacity.
                let cpu_ratio = match declared_cpu_capacity_ns {
                    Some(capacity) if capacity > 0 => ratio_to_fixed(aggregates.cpu_time_ns, capacity),
                    _ => MAX,
                };

                // Storage ratio: free space vs declared target.
                let storage_ratio = match declared_storage_bytes {
                    Some(target_bytes) => match measure_storage_size(&storage_path).await {
                        Ok(used_bytes) => {
                            let free_bytes = target_bytes.saturating_sub(used_bytes);
                            ratio_to_fixed(free_bytes as u128, target_bytes as u128)
                        }
                        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                            // If the storage path doesn't exist yet, treat usage as 0 to avoid noisy warnings.
                            ratio_to_fixed(target_bytes as u128, target_bytes as u128)
                        }
                        Err(err) => {
                            warn!(error=?err, "resource monitor failed to measure storage footprint");
                            MAX
                        }
                    },
                    None => MAX,
                };

                let composite = weighted_sum(&weights, bandwidth_ratio, cpu_ratio, storage_ratio);

                // Compute used storage bytes for the claim
                let used_storage_bytes = match declared_storage_bytes {
                    Some(target) => measure_storage_size(&storage_path).await.ok().map(|used| used.min(target)).unwrap_or(0),
                    None => 0,
                };

                let claim = {
                    // Build signed resource claim for the local node.
                    let mut resource_claim = ResourceClaim::new(
                        local_node_id,
                        observed_bytes_per_sec as u32,
                        aggregates.cpu_time_ns.min(u32::MAX as u128) as u32,
                        used_storage_bytes as u32,
                        unix_timestamp_secs(),
                        [0u8; 64], // Initial empty signature, will be replaced by real signature
                    );

                    // Sign the resource claim with real cryptographic signature
                    sign_resource_claim(&mut resource_claim, &signer);

                    Some(resource_claim)
                };

                {
                    let mut guard = scores.write().await;
                    guard.insert(
                        local_peer.clone(),
                        ResourceSnapshot {
                            score: composite,
                            claim,
                        },
                    );
                }

                debug!(
                    target: "pou.summary",
                    peer = %local_peer,
                    epoch = epoch_index,
                    bandwidth_ratio = %format_fixed(bandwidth_ratio),
                    cpu_ratio = %format_fixed(cpu_ratio),
                    storage_ratio = %format_fixed(storage_ratio),
                    composite = %format_fixed(composite),
                    tolerance = %format_fixed(tolerance_fp),
                    "{}",
                    flagged_message(
                        FLAG_RESOURCE,
                        format!(
                            "epoch={epoch} bw_obs={obs}B/s cpu_ns={cpu} storage_target_bytes={storage:?}",
                            epoch = epoch_index,
                            obs = observed_bytes_per_sec,
                            cpu = aggregates.cpu_time_ns,
                            storage = declared_storage_bytes,
                        )
                    )
                );

                aggregates.reset();
                elapsed_in_epoch = 0;
                epoch_index = epoch_index.saturating_add(1);
            }
        }
    }
}

async fn measure_storage_size(path: &Path) -> std::io::Result<u64> {
    let path = path.to_owned();
    task::spawn_blocking(move || dir_size(&path))
        .await
        .unwrap_or_else(|err| {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("storage size task failed: {err}"),
            ))
        })
}

fn dir_size(path: &Path) -> std::io::Result<u64> {
    let metadata = match fs::metadata(path) {
        Ok(meta) => meta,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };
    if metadata.is_file() {
        return Ok(metadata.len());
    }

    let entries = match fs::read_dir(path) {
        Ok(entries) => entries,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(err) => return Err(err),
    };

    let mut total = 0u64;
    for entry in entries {
        let entry = entry?;
        let entry_path = entry.path();
        total = total.saturating_add(dir_size(&entry_path)?);
    }
    Ok(total)
}

pub fn emit_event(sender: &mut Option<mpsc::Sender<ResourceEvent>>, event: ResourceEvent) {
    if let Some(channel) = sender.as_mut() {
        match channel.try_send(event) {
            Ok(_) => {}
            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                warn!("resource event queue full; dropping sample");
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                *sender = None;
            }
        }
    }
}

fn ratio_to_fixed(numer: u128, denom: u128) -> FixedPoint {
    if denom == 0 {
        return MAX;
    }
    if numer >= denom {
        return MAX;
    }

    let scaled = numer
        .saturating_mul(1_000_000)
        .saturating_div(denom)
        .min(1_000_000) as u32;
    FixedPoint::from_raw_u32(scaled)
}

fn weighted_sum(
    weights: &FixedWeights,
    bw: FixedPoint,
    cpu: FixedPoint,
    storage: FixedPoint,
) -> FixedPoint {
    let mut sum = weights.bandwidth.checked_mul(bw).unwrap_or_default();
    sum = sum
        .checked_add(weights.cpu.checked_mul(cpu).unwrap_or_default())
        .unwrap_or(MAX);
    sum = sum
        .checked_add(weights.storage.checked_mul(storage).unwrap_or_default())
        .unwrap_or(MAX);
    sum
}

fn unix_timestamp_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[allow(dead_code)]
fn format_fixed(fp: FixedPoint) -> String {
    let raw = fp.raw();
    let whole = raw / 1_000_000;
    let frac = raw % 1_000_000;
    format!("{whole}.{frac:06}")
}

#[allow(dead_code)]
fn resource_claim_pubkey(
    claim: &ResourceClaim,
) -> Result<ed25519_dalek::VerifyingKey, Box<dyn std::error::Error>> {
    use ed25519_dalek::Verifier;

    // Create the same message that was signed
    let mut message = Vec::new();
    message.extend_from_slice(&claim.node_id);
    message.extend_from_slice(&claim.observed_bandwidth.to_le_bytes());
    message.extend_from_slice(&claim.declared_bandwidth.to_le_bytes());
    message.extend_from_slice(&claim.observed_cpu.to_le_bytes());
    message.extend_from_slice(&claim.declared_cpu.to_le_bytes());
    message.extend_from_slice(&claim.observed_storage.to_le_bytes());
    message.extend_from_slice(&claim.declared_storage.to_le_bytes());
    message.extend_from_slice(&claim.timestamp.to_le_bytes());

    // Hash the message using SHA256
    let message_hash = sha2::Sha256::digest(&message);

    // Try to extract public key from signature verification
    // Since ed25519 doesn't support public key recovery directly,
    // we need to find the public key that verifies this signature
    // For now, we'll return an error as this requires additional context
    Err("Public key extraction from signature requires additional context or key storage".into())
}

/// Sign a resource claim using the node's signing key
fn sign_resource_claim(claim: &mut ResourceClaim, signer: &ed25519_dalek::SigningKey) {
    use sha2::Digest;

    // Create message to sign: node_id || bandwidth || cpu || storage || timestamp
    let mut message = Vec::new();
    message.extend_from_slice(&claim.node_id);
    message.extend_from_slice(&claim.observed_cpu.to_le_bytes());
    message.extend_from_slice(&claim.declared_cpu.to_le_bytes());
    message.extend_from_slice(&claim.observed_storage.to_le_bytes());
    message.extend_from_slice(&claim.timestamp.to_le_bytes());

    // Hash the message
    let message_hash = sha2::Sha256::digest(&message);

    // Create signature using ed25519_dalek
    use ed25519_dalek::Signer;
    let signature = signer.sign(&message_hash);
    claim.signature = signature.to_bytes();
}
