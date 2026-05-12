//! Score and PoU (Proof-of-Unity) related types
//!
//! This module defines the score-related data structures used across
/// all consensus implementations, particularly for PoU-based consensus.
///
/// AUDIT-003 FIX: All consensus-critical arithmetic uses integer fixed-point
/// (u64 with 1000x scale factor) instead of f64 to guarantee determinism
/// across architectures (x86, ARM, WASM, etc.).
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// PoU score fixed-point representation
///
/// Consensus/stateful code MUST NOT use floats. This score is an integer in the range 0..=1000.
pub type PouScore = u16;

/// Max PoU score value (inclusive)
pub const POU_SCORE_MAX: PouScore = 1000;

/// Min PoU score value (inclusive)
pub const POU_SCORE_MIN: PouScore = 0;

/// Default PoU score
pub const POU_SCORE_DEFAULT: PouScore = 500;

/// Fixed-point scale factor: weights and intermediate scores use permille (parts per 1000)
const PERMILLE: u64 = 1000;

/// Score calculation components
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ScoreComponents {
    /// Latency score (0-1000)
    pub latency_score: PouScore,
    /// Availability score (0-1000)
    pub availability_score: PouScore,
    /// Integrity score (0-1000)
    pub integrity_score: PouScore,
    /// Geographic score (0-1000)
    pub geographic_score: PouScore,
    /// Performance score (0-1000)
    pub performance_score: PouScore,
    /// Reputation score (0-1000)
    pub reputation_score: PouScore,
    /// Federated Learning contribution integrity (0-1000).
    /// Measures how close the peer's gradient updates are to the
    /// A peer that never participates in FL rounds defaults to 1000
    /// ("no data, no penalty") matching `integrity_score` semantics.
    pub fl_integrity_score: PouScore,
}

/// PoU score calculation result
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PouScoreResult {
    /// Final calculated score
    pub score: PouScore,
    /// Score components
    pub components: ScoreComponents,
    /// Calculation timestamp
    pub timestamp: u64,
    /// Calculation epoch
    pub epoch: u64,
    /// Node ID
    pub node_id: String,
    /// Peer ID
    pub peer_id: String,
}

/// Score calculation configuration
///
/// AUDIT-003 FIX: All weights are u32 in permille (parts per 1000).
/// For example, latency_weight = 300 means 30.0%.
/// Weights MUST sum to 1000 for correct scoring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreConfig {
    /// Weight for latency score (permille, 0-1000)
    pub latency_weight: u32,
    /// Weight for availability score (permille, 0-1000)
    pub availability_weight: u32,
    /// Weight for integrity score (permille, 0-1000)
    pub integrity_weight: u32,
    /// Weight for geographic score (permille, 0-1000)
    pub geographic_weight: u32,
    /// Weight for performance score (permille, 0-1000)
    pub performance_weight: u32,
    /// Weight for reputation score (permille, 0-1000)
    pub reputation_weight: u32,
    /// Weight for FL contribution integrity (permille, 0-1000)
    pub fl_integrity_weight: u32,
    /// Score update interval in seconds
    pub update_interval_secs: u64,
    /// Score decay rate (permille, 0-1000; e.g. 10 = 1.0% decay)
    pub decay_rate: u32,
    /// Minimum score threshold
    pub min_threshold: PouScore,
    /// Maximum score threshold
    pub max_threshold: PouScore,
}

/// Latency measurement data
///
/// AUDIT-003 FIX: rtt_ms changed from f64 to u64 (integer milliseconds).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct LatencyMeasurement {
    /// Peer ID
    pub peer_id: String,
    /// Round-trip time in milliseconds (integer)
    pub rtt_ms: u64,
    /// Measurement timestamp
    pub timestamp: u64,
    /// Measurement type
    pub measurement_type: LatencyType,
}

/// Latency measurement type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum LatencyType {
    /// Ping measurement
    Ping,
    /// Block propagation measurement
    BlockPropagation,
    /// Transaction propagation measurement
    TransactionPropagation,
    /// Consensus message measurement
    ConsensusMessage,
}

/// Availability measurement data
///
/// AUDIT-003 FIX: uptime_percentage removed (was f64). Use
/// successful_pings / total_pings for integer ratio instead.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AvailabilityMeasurement {
    /// Node ID
    pub node_id: String,
    /// Successful pings
    pub successful_pings: u32,
    /// Total pings
    pub total_pings: u32,
    /// Last seen timestamp
    pub last_seen: u64,
    /// Measurement window in seconds
    pub window_secs: u64,
}

/// Integrity measurement data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IntegrityMeasurement {
    /// Node ID
    pub node_id: String,
    /// Valid blocks produced
    pub valid_blocks: u32,
    /// Total blocks attempted
    pub total_blocks: u32,
    /// Valid transactions
    pub valid_transactions: u32,
    /// Total transactions
    pub total_transactions: u32,
    /// Slash events
    pub slash_events: u32,
    /// Measurement epoch
    pub epoch: u64,
}

/// Geographic information
///
/// NOTE: latitude/longitude are kept as f64 because they are display/logging
/// only and never used in consensus-critical arithmetic.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GeographicInfo {
    /// Node ID
    pub node_id: String,
    /// Geographic region
    pub region: String,
    /// Country code
    pub country_code: String,
    /// Latitude
    pub latitude: f64,
    /// Longitude
    pub longitude: f64,
    /// Timezone
    pub timezone: String,
}

/// Performance metrics
///
/// AUDIT-003 FIX: All fields converted to integer permille (0-1000) or
/// integer milliseconds to eliminate f64 non-determinism.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PerformanceMetrics {
    /// Node ID
    pub node_id: String,
    /// CPU usage in permille (0-1000, e.g. 500 = 50.0%)
    pub cpu_usage_permille: u32,
    /// Memory usage in permille (0-1000, e.g. 750 = 75.0%)
    pub memory_usage_permille: u32,
    /// Network bandwidth in kbps (integer)
    pub network_bandwidth_kbps: u64,
    /// Block processing time in milliseconds (integer)
    pub block_processing_time_ms: u64,
    /// Transaction processing time in milliseconds (integer)
    pub tx_processing_time_ms: u64,
    /// Measurement timestamp
    pub timestamp: u64,
}

/// Reputation information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ReputationInfo {
    /// Node ID
    pub node_id: String,
    /// Reputation score (0-1000)
    pub reputation_score: PouScore,
    /// Positive interactions
    pub positive_interactions: u32,
    /// Negative interactions
    pub negative_interactions: u32,
    /// Total interactions
    pub total_interactions: u32,
    /// Last updated timestamp
    pub last_updated: u64,
}

/// Score snapshot for a given epoch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreSnapshot {
    /// Epoch number
    pub epoch: u64,
    /// Node scores
    pub node_scores: std::collections::HashMap<String, PouScoreResult>,
    /// Global statistics
    pub global_stats: ScoreGlobalStats,
    /// Snapshot timestamp
    pub timestamp: u64,
}

/// Global score statistics
///
/// AUDIT-003 FIX: average_score changed from f64 to u32 (0-1000 scale).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreGlobalStats {
    /// Total nodes
    pub total_nodes: u32,
    /// Average score (0-1000 integer scale)
    pub average_score: u32,
    /// Median score
    pub median_score: PouScore,
    /// Score distribution
    pub score_distribution: ScoreDistribution,
    /// Top performers
    pub top_performers: Vec<String>,
}

/// Score distribution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreDistribution {
    /// Excellent scores (900-1000)
    pub excellent: u32,
    /// Good scores (700-899)
    pub good: u32,
    /// Average scores (500-699)
    pub average: u32,
    /// Poor scores (300-499)
    pub poor: u32,
    /// Very poor scores (0-299)
    pub very_poor: u32,
}

/// Score calculation trait
pub trait ScoreCalculator: Send + Sync {
    /// Calculate PoU score from components
    fn calculate_score(&self, components: &ScoreComponents, config: &ScoreConfig) -> PouScore;

    /// Calculate latency score
    fn calculate_latency_score(&self, measurements: &[LatencyMeasurement]) -> PouScore;

    /// Calculate availability score
    fn calculate_availability_score(&self, measurement: &AvailabilityMeasurement) -> PouScore;

    /// Calculate integrity score
    fn calculate_integrity_score(&self, measurement: &IntegrityMeasurement) -> PouScore;

    /// Calculate geographic score
    fn calculate_geographic_score(&self, info: &GeographicInfo, target_region: &str) -> PouScore;

    /// Calculate performance score
    fn calculate_performance_score(&self, metrics: &PerformanceMetrics) -> PouScore;

    /// Calculate reputation score
    fn calculate_reputation_score(&self, info: &ReputationInfo) -> PouScore;
}

/// Default score calculator implementation
///
/// AUDIT-003 FIX: All arithmetic uses u64 integer math with PERMILLE (1000x)
/// scale factor. No f64 is used anywhere in consensus-critical paths.
pub struct DefaultScoreCalculator;

impl ScoreCalculator for DefaultScoreCalculator {
    fn calculate_score(&self, components: &ScoreComponents, config: &ScoreConfig) -> PouScore {
        // Weighted sum using integer arithmetic:
        // weighted_score = sum(component * weight) / 1000
        // Each component is 0-1000, each weight is 0-1000 (permille).
        // Max intermediate: 1000 * 1000 * 7 = 7_000_000, fits u64 easily.
        let weighted_sum: u64 = (components.latency_score as u64) * (config.latency_weight as u64)
            + (components.availability_score as u64) * (config.availability_weight as u64)
            + (components.integrity_score as u64) * (config.integrity_weight as u64)
            + (components.geographic_score as u64) * (config.geographic_weight as u64)
            + (components.performance_score as u64) * (config.performance_weight as u64)
            + (components.reputation_score as u64) * (config.reputation_weight as u64)
            + (components.fl_integrity_score as u64) * (config.fl_integrity_weight as u64);

        // Divide by PERMILLE to get final score in 0-1000 range.
        // Add PERMILLE/2 for rounding (equivalent to round-half-up).
        let rounded = (weighted_sum + PERMILLE / 2) / PERMILLE;

        // SECURITY: Clamp to valid range
        let clamped = rounded.min(POU_SCORE_MAX as u64);
        clamped as PouScore
    }

    fn calculate_latency_score(&self, measurements: &[LatencyMeasurement]) -> PouScore {
        if measurements.is_empty() {
            return POU_SCORE_DEFAULT;
        }

        // Integer average RTT in milliseconds
        let total_rtt: u64 = measurements.iter().map(|m| m.rtt_ms).sum();
        let count = measurements.len() as u64;
        let avg_rtt = total_rtt / count;

        // Lower RTT = higher score
        // RTT <= 50ms = 1000, RTT >= 500ms = 0
        // Linear interpolation: score = (500 - avg_rtt) * 1000 / 450
        // Using integer math with clamping.
        let score: PouScore = if avg_rtt <= 50 {
            POU_SCORE_MAX
        } else if avg_rtt >= 500 {
            POU_SCORE_MIN
        } else {
            // (500 - avg_rtt) is in range 1..450
            // Multiply by 1000, divide by 450 to get 0-1000 range
            let numerator = (500 - avg_rtt) * (POU_SCORE_MAX as u64);
            let score_val = numerator / 450;
            (score_val as PouScore).min(POU_SCORE_MAX)
        };

        score.clamp(POU_SCORE_MIN, POU_SCORE_MAX)
    }

    fn calculate_availability_score(&self, measurement: &AvailabilityMeasurement) -> PouScore {
        if measurement.total_pings == 0 {
            return POU_SCORE_MIN;
        }

        // Integer ratio: (successful * 1000) / total
        let score = (measurement.successful_pings as u64) * (POU_SCORE_MAX as u64)
            / (measurement.total_pings as u64);

        (score as PouScore).clamp(POU_SCORE_MIN, POU_SCORE_MAX)
    }

    fn calculate_integrity_score(&self, measurement: &IntegrityMeasurement) -> PouScore {
        // Block integrity: valid_blocks * 1000 / total_blocks (or 1000 if no blocks)
        let block_integrity: u64 = if measurement.total_blocks > 0 {
            (measurement.valid_blocks as u64) * PERMILLE / (measurement.total_blocks as u64)
        } else {
            PERMILLE // Perfect integrity when no blocks
        };

        // Tx integrity: valid_transactions * 1000 / total_transactions (or 1000 if no txs)
        let tx_integrity: u64 = if measurement.total_transactions > 0 {
            (measurement.valid_transactions as u64) * PERMILLE
                / (measurement.total_transactions as u64)
        } else {
            PERMILLE // Perfect integrity when no transactions
        };

        // Slash penalty: 100 permille (10%) per slash event
        let slash_penalty: u64 = (measurement.slash_events as u64) * 100;

        // Combined: average of block and tx integrity
        let combined = (block_integrity + tx_integrity) / 2;

        // Apply slash penalty, clamping to zero floor
        let adjusted = combined.saturating_sub(slash_penalty);

        // Clamp to 0-1000 range
        let score = adjusted.min(POU_SCORE_MAX as u64);
        score as PouScore
    }

    fn calculate_geographic_score(&self, info: &GeographicInfo, target_region: &str) -> PouScore {
        if info.region == target_region {
            POU_SCORE_MAX
        } else {
            // Simple geographic scoring - same region = 1000, different = 500
            POU_SCORE_DEFAULT
        }
    }

    fn calculate_performance_score(&self, metrics: &PerformanceMetrics) -> PouScore {
        // CPU score: (1000 - cpu_usage_permille), clamped to 0-1000
        let cpu_score: u64 = (PERMILLE).saturating_sub(metrics.cpu_usage_permille as u64);

        // Memory score: (1000 - memory_usage_permille), clamped to 0-1000
        let memory_score: u64 = (PERMILLE).saturating_sub(metrics.memory_usage_permille as u64);

        // Processing score based on block_processing_time_ms:
        // < 100ms = 1000, > 1000ms = 0, linear interpolation in between
        let processing_score: u64 = if metrics.block_processing_time_ms < 100 {
            PERMILLE
        } else if metrics.block_processing_time_ms > 1000 {
            0
        } else {
            // (1000 - bpt) * 1000 / 900
            let diff = 1000u64.saturating_sub(metrics.block_processing_time_ms);
            diff * PERMILLE / 900
        };

        // Average of three scores: (cpu + memory + processing) / 3
        let combined = (cpu_score + memory_score + processing_score) / 3;

        // SECURITY: Clamp to valid range
        let score = combined.min(POU_SCORE_MAX as u64);
        score as PouScore
    }

    fn calculate_reputation_score(&self, info: &ReputationInfo) -> PouScore {
        if info.total_interactions == 0 {
            return POU_SCORE_DEFAULT;
        }

        // Integer ratio: positive * 1000 / total
        let score = (info.positive_interactions as u64) * (POU_SCORE_MAX as u64)
            / (info.total_interactions as u64);

        (score as PouScore).clamp(POU_SCORE_MIN, POU_SCORE_MAX)
    }
}

/// Format a PoU score (0..=1000) as a normalized fixed-point string with 4 decimals (0.0000..1.0000),
/// without using floats.
pub fn format_pou_score_4dp(score: PouScore) -> String {
    let clamped = score.clamp(POU_SCORE_MIN, POU_SCORE_MAX);
    // Convert 0..=1000 (1/1000 steps) to 0..=10000 (1/10000 steps) to print 4 decimals.
    let scaled = clamped as u32 * 10;
    let whole = scaled / 10_000;
    let frac = scaled % 10_000;
    format!("{}.{}{:03}", whole, frac / 1000, frac % 1000)
}

/// Compare two PoU scores with deterministic tie-breaking
pub fn compare_pou_scores(
    score1: PouScore,
    peer_id1: &str,
    score2: PouScore,
    peer_id2: &str,
) -> Ordering {
    match score1.cmp(&score2) {
        Ordering::Equal => {
            // Deterministic tie-break: lexicographic compare on peer IDs
            peer_id1.cmp(peer_id2)
        }
        other => other,
    }
}

impl Default for ScoreConfig {
    fn default() -> Self {
        Self {
            // Weights sum to 1000 (permille):
            // 250 + 200 + 150 + 50 + 100 + 50 + 200 = 1000
            // Rebalanced to make room for FL contribution integrity. The
            // FL component sits at 200 permille (20%) — significant enough
            // that a consistently malicious FL client loses consensus
            // eligibility, but not dominant over block-production metrics.
            latency_weight: 250,
            availability_weight: 200,
            integrity_weight: 150,
            geographic_weight: 50,
            performance_weight: 100,
            reputation_weight: 50,
            fl_integrity_weight: 200,
            update_interval_secs: 60,
            decay_rate: 10, // 10 permille = 1.0%
            min_threshold: POU_SCORE_MIN,
            max_threshold: POU_SCORE_MAX,
        }
    }
}

impl Default for ScoreComponents {
    fn default() -> Self {
        Self {
            latency_score: POU_SCORE_DEFAULT,
            availability_score: POU_SCORE_DEFAULT,
            integrity_score: POU_SCORE_DEFAULT,
            geographic_score: POU_SCORE_DEFAULT,
            performance_score: POU_SCORE_DEFAULT,
            reputation_score: POU_SCORE_DEFAULT,
            // FL non-participation must not penalise — fresh peer defaults
            // to MAX, same as `integrity_score` semantics for empty data.
            fl_integrity_score: POU_SCORE_MAX,
        }
    }
}

impl Default for ScoreGlobalStats {
    fn default() -> Self {
        Self {
            total_nodes: 0,
            average_score: 0,
            median_score: POU_SCORE_DEFAULT,
            score_distribution: ScoreDistribution::default(),
            top_performers: Vec::new(),
        }
    }
}

impl Default for ScoreDistribution {
    fn default() -> Self {
        Self {
            excellent: 0,
            good: 0,
            average: 0,
            poor: 0,
            very_poor: 0,
        }
    }
}
