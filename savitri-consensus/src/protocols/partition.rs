//! SECURITY (F-04): Network partition detection
//!
//! network partitions. When a partition is detected, consensus should pause
//! block production to prevent forks.

use std::time::{Duration, Instant};

/// Partition detection threshold in permille (parts per thousand).
///
/// AUDIT-003: Replaced f64 with integer permille for cross-platform determinism.
const PARTITION_THRESHOLD_PERMILLE: u32 = 300; // 30%

/// Minimum number of consecutive checks that must detect a partition before
/// the state transitions (avoids flapping on transient disconnects).
const MIN_CONSECUTIVE_DETECTIONS: u32 = 3;

/// Network partition status
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionStatus {
    /// Network is healthy — quorum is reachable
    Healthy,
    /// Degraded — some peers lost but quorum still reachable
    Degraded,
    /// Partitioned — cannot reach quorum, block production should pause
    Partitioned,
    /// Recovering — was partitioned, peers reconnecting
    Recovering,
}

impl Default for PartitionStatus {
    fn default() -> Self {
        PartitionStatus::Healthy
    }
}

/// Partition detection event for logging / metrics
#[derive(Debug, Clone)]
pub struct PartitionEvent {
    pub status: PartitionStatus,
    pub connected_peers: usize,
    pub expected_validators: usize,
    /// Connectivity expressed in permille (0–1000).
    /// AUDIT-003: Replaced f64 with integer permille.
    pub connectivity_permille: u32,
    pub quorum_reachable: bool,
    pub timestamp: Instant,
}

/// Network partition detector
///
/// Call `update()` periodically with the current connected peer count and
/// and determines whether the node can still reach BFT quorum (2/3 + 1).
#[derive(Debug)]
pub struct PartitionDetector {
    status: PartitionStatus,
    consecutive_partition_checks: u32,
    consecutive_healthy_checks: u32,
    last_check: Option<Instant>,
    last_event: Option<PartitionEvent>,
    /// How often to run the check (minimum interval between updates)
    pub check_interval: Duration,
    /// Custom partition threshold override in permille (default: PARTITION_THRESHOLD_PERMILLE)
    threshold_permille: u32,
    /// Total partition events detected since startup
    pub total_partition_events: u64,
    /// Total recovery events since startup
    pub total_recovery_events: u64,
}

impl PartitionDetector {
    /// Create a new partition detector with default settings.
    pub fn new() -> Self {
        Self {
            status: PartitionStatus::Healthy,
            consecutive_partition_checks: 0,
            consecutive_healthy_checks: 0,
            last_check: None,
            last_event: None,
            check_interval: Duration::from_secs(5),
            threshold_permille: PARTITION_THRESHOLD_PERMILLE,
            total_partition_events: 0,
            total_recovery_events: 0,
        }
    }

    /// Create with a custom threshold in permille (0–1000).
    ///
    /// AUDIT-003: Replaced f64 with integer permille.
    pub fn with_threshold(mut self, threshold_permille: u32) -> Self {
        self.threshold_permille = threshold_permille.min(1000);
        self
    }

    /// Get current partition status.
    pub fn status(&self) -> PartitionStatus {
        self.status
    }

    /// Whether the node should pause block production.
    pub fn should_pause_production(&self) -> bool {
        self.status == PartitionStatus::Partitioned
    }

    /// Get the last partition event (if any).
    pub fn last_event(&self) -> Option<&PartitionEvent> {
        self.last_event.as_ref()
    }

    /// Update the detector with current network state.
    ///
    /// Returns `Some(event)` if the status changed, `None` otherwise.
    pub fn update(
        &mut self,
        connected_peers: usize,
        expected_validators: usize,
    ) -> Option<PartitionEvent> {
        let now = Instant::now();

        // Rate-limit checks
        if let Some(last) = self.last_check {
            if now.duration_since(last) < self.check_interval {
                return None;
            }
        }
        self.last_check = Some(now);

        if expected_validators == 0 {
            return None;
        }

        let connectivity_permille: u32 =
            ((connected_peers as u64 * 1000) / expected_validators as u64) as u32;

        // With this node as one of them, we need (2*n/3) peers
        let quorum_size = (expected_validators * 2 + 2) / 3; // ceiling division for 2/3
        let quorum_reachable = (connected_peers + 1) >= quorum_size; // +1 for self

        let previous_status = self.status;

        // Determine new status based on connectivity
        if connectivity_permille < self.threshold_permille {
            // Below partition threshold
            self.consecutive_healthy_checks = 0;
            self.consecutive_partition_checks += 1;

            if self.consecutive_partition_checks >= MIN_CONSECUTIVE_DETECTIONS {
                self.status = PartitionStatus::Partitioned;
            }
        } else if !quorum_reachable {
            // Above threshold but can't reach quorum
            self.consecutive_healthy_checks = 0;
            self.consecutive_partition_checks += 1;

            if self.consecutive_partition_checks >= MIN_CONSECUTIVE_DETECTIONS {
                self.status = PartitionStatus::Degraded;
            }
        } else {
            // Healthy
            self.consecutive_partition_checks = 0;
            self.consecutive_healthy_checks += 1;

            if self.status == PartitionStatus::Partitioned
                || self.status == PartitionStatus::Degraded
            {
                if self.consecutive_healthy_checks >= MIN_CONSECUTIVE_DETECTIONS {
                    self.status = PartitionStatus::Healthy;
                } else {
                    self.status = PartitionStatus::Recovering;
                }
            } else if self.status == PartitionStatus::Recovering {
                if self.consecutive_healthy_checks >= MIN_CONSECUTIVE_DETECTIONS {
                    self.status = PartitionStatus::Healthy;
                }
            }
        }

        // Emit event if status changed
        if self.status != previous_status {
            if self.status == PartitionStatus::Partitioned {
                self.total_partition_events += 1;
                tracing::error!(
                    "NETWORK PARTITION DETECTED — connected {}/{} peers ({:.1}%), quorum unreachable. \
                     Pausing block production.",
                    connected_peers,
                    expected_validators,
                    connectivity_permille as f32 / 10.0
                );
            } else if self.status == PartitionStatus::Degraded {
                tracing::warn!(
                    "Network degraded — connected {}/{} peers ({:.1}%), quorum may be at risk.",
                    connected_peers,
                    expected_validators,
                    connectivity_permille as f32 / 10.0
                );
            } else if self.status == PartitionStatus::Recovering {
                tracing::info!(
                    "Network recovering — connected {}/{} peers ({:.1}%)",
                    connected_peers,
                    expected_validators,
                    connectivity_permille as f32 / 10.0
                );
            } else if self.status == PartitionStatus::Healthy
                && (previous_status == PartitionStatus::Recovering
                    || previous_status == PartitionStatus::Partitioned)
            {
                self.total_recovery_events += 1;
                tracing::info!(
                    "Network partition RECOVERED — connected {}/{} peers ({:.1}%), \
                     resuming block production.",
                    connected_peers,
                    expected_validators,
                    connectivity_permille as f32 / 10.0
                );
            }

            let event = PartitionEvent {
                status: self.status,
                connected_peers,
                expected_validators,
                connectivity_permille,
                quorum_reachable,
                timestamp: now,
            };
            self.last_event = Some(event.clone());
            return Some(event);
        }

        None
    }
}

impl Default for PartitionDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: force multiple updates ignoring rate-limit by resetting last_check
    fn force_update(
        det: &mut PartitionDetector,
        connected: usize,
        expected: usize,
    ) -> Option<PartitionEvent> {
        det.last_check = None;
        det.update(connected, expected)
    }

    #[test]
    fn test_healthy_network() {
        let mut det = PartitionDetector::new();
        // 10 of 10 peers connected
        for _ in 0..5 {
            force_update(&mut det, 10, 10);
        }
        assert_eq!(det.status(), PartitionStatus::Healthy);
        assert!(!det.should_pause_production());
    }

    #[test]
    fn test_partition_detection_requires_consecutive_checks() {
        let mut det = PartitionDetector::new();
        // 1 of 10 peers — below threshold, but first check shouldn't trigger
        let ev1 = force_update(&mut det, 1, 10);
        assert!(
            ev1.is_none(),
            "First check should not immediately partition"
        );

        let ev2 = force_update(&mut det, 1, 10);
        assert!(ev2.is_none(), "Second check still below min consecutive");

        let ev3 = force_update(&mut det, 1, 10);
        assert!(
            ev3.is_some(),
            "Third consecutive check should trigger partition"
        );
        assert_eq!(det.status(), PartitionStatus::Partitioned);
        assert!(det.should_pause_production());
    }

    #[test]
    fn test_recovery_after_partition() {
        let mut det = PartitionDetector::new();

        // Trigger partition
        for _ in 0..3 {
            force_update(&mut det, 0, 10);
        }
        assert_eq!(det.status(), PartitionStatus::Partitioned);

        // Start recovering — should go to Recovering first
        let ev = force_update(&mut det, 10, 10);
        assert!(ev.is_some());
        assert_eq!(det.status(), PartitionStatus::Recovering);

        // Need more healthy checks to fully recover
        force_update(&mut det, 10, 10);
        force_update(&mut det, 10, 10);
        assert_eq!(det.status(), PartitionStatus::Healthy);
        assert_eq!(det.total_partition_events, 1);
        assert_eq!(det.total_recovery_events, 1);
    }

    #[test]
    fn test_degraded_state() {
        let mut det = PartitionDetector::new();
        // 4 of 10 peers: above 30% threshold but can't reach 2/3 quorum
        // quorum_size = ceil(10*2/3) = 7, connected+self = 5 < 7
        for _ in 0..3 {
            force_update(&mut det, 4, 10);
        }
        assert_eq!(det.status(), PartitionStatus::Degraded);
        assert!(
            !det.should_pause_production(),
            "Degraded doesn't pause — only full partition does"
        );
    }

    #[test]
    fn test_quorum_reachable_4_validators() {
        let mut det = PartitionDetector::new();
        for _ in 0..5 {
            force_update(&mut det, 2, 4);
        }
        // 2 peers + self = 3 >= 3 quorum, and 2/4 = 0.5 > 0.3 threshold
        assert_eq!(det.status(), PartitionStatus::Healthy);
    }

    #[test]
    fn test_transient_disconnect_no_flap() {
        let mut det = PartitionDetector::new();

        // Healthy
        for _ in 0..3 {
            force_update(&mut det, 10, 10);
        }
        assert_eq!(det.status(), PartitionStatus::Healthy);

        // Brief disconnect (1 check)
        force_update(&mut det, 0, 10);
        assert_eq!(
            det.status(),
            PartitionStatus::Healthy,
            "Single drop shouldn't trigger"
        );

        // Back to healthy
        force_update(&mut det, 10, 10);
        assert_eq!(det.status(), PartitionStatus::Healthy);
    }

    #[test]
    fn test_zero_expected_validators() {
        let mut det = PartitionDetector::new();
        let ev = force_update(&mut det, 0, 0);
        assert!(ev.is_none());
        assert_eq!(det.status(), PartitionStatus::Healthy);
    }

    #[test]
    fn test_custom_threshold() {
        let mut det = PartitionDetector::new().with_threshold(500);
        // 4 of 10 = 40%, below 50% custom threshold
        for _ in 0..3 {
            force_update(&mut det, 4, 10);
        }
        assert_eq!(det.status(), PartitionStatus::Partitioned);
    }
}
