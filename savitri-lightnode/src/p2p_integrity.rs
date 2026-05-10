// P2P Integrity Receiver - Complete implementation for data integrity monitoring
use anyhow::Result;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityCheckResult {
    pub check_id: u64,
    pub data_type: DataType,
    #[serde(with = "crate::p2p::types::big_array")]
    pub hash: [u8; 64],
    pub is_valid: bool,
    pub peer_id: PeerId,
    pub timestamp: u64,
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DataType {
    Block,
    Transaction,
    Certificate,
    State,
    Monolith,
    Account,
}

#[derive(Debug, Clone)]
pub enum IntegrityEvent {
    Mismatch {
        data_type: DataType,
        expected_hash: [u8; 64],
        actual_hash: [u8; 64],
        source: PeerId,
    },
    Fault {
        data_type: DataType,
        reason: String,
        source: PeerId,
    },
    CheckPassed {
        result: IntegrityCheckResult,
    },
    CheckFailed {
        result: IntegrityCheckResult,
        reason: String,
    },
    PeerFlagged {
        peer_id: PeerId,
        reason: String,
        violations: u32,
    },
}

#[derive(Debug, Clone)]
pub struct IntegrityStats {
    pub checks_performed: u64,
    pub checks_passed: u64,
    pub checks_failed: u64,
    pub mismatches_detected: u64,
    pub faults_detected: u64,
    pub peers_flagged: u64,
    pub last_check: u64,
}

#[derive(Debug, Clone)]
pub struct PeerIntegrityRecord {
    pub peer_id: PeerId,
    pub violations: u32,
    pub checks_passed: u32,
    pub checks_failed: u32,
    pub last_violation: Option<u64>,
    pub is_flagged: bool,
    pub trust_score: f64, // 0.0 to 1.0
}

pub struct IntegrityReceiver {
    local_peer_id: PeerId,
    event_tx: mpsc::Sender<IntegrityEvent>,
    check_id_counter: Arc<RwLock<u64>>,
    check_results: Arc<RwLock<HashMap<u64, IntegrityCheckResult>>>,
    peer_records: Arc<RwLock<HashMap<PeerId, PeerIntegrityRecord>>>,
    flagged_peers: Arc<RwLock<HashSet<PeerId>>>,
    expected_hashes: Arc<RwLock<HashMap<(DataType, u64), [u8; 64]>>>, // (data_type, id) -> hash
    stats: Arc<RwLock<IntegrityStats>>,
    max_violations: u32,
}

impl IntegrityReceiver {
    pub fn new() -> (Self, mpsc::Receiver<IntegrityEvent>) {
        let (tx, rx) = mpsc::channel(1000);
        let (event_tx, _event_rx) = mpsc::channel(1000);

        let receiver = Self {
            local_peer_id: PeerId::random(),
            event_tx,
            check_id_counter: Arc::new(RwLock::new(1)),
            check_results: Arc::new(RwLock::new(HashMap::new())),
            peer_records: Arc::new(RwLock::new(HashMap::new())),
            flagged_peers: Arc::new(RwLock::new(HashSet::new())),
            expected_hashes: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(IntegrityStats {
                checks_performed: 0,
                checks_passed: 0,
                checks_failed: 0,
                mismatches_detected: 0,
                faults_detected: 0,
                peers_flagged: 0,
                last_check: 0,
            })),
            max_violations: 5, // Max violations before flagging
        };

        (receiver, rx)
    }

    pub fn with_local_peer_id(mut self, peer_id: PeerId) -> Self {
        self.local_peer_id = peer_id;
        self
    }

    pub fn with_max_violations(mut self, max: u32) -> Self {
        self.max_violations = max;
        self
    }

    pub async fn start_tasks(&self) -> Result<()> {
        info!(
            "Starting Integrity Receiver for peer: {}",
            self.local_peer_id
        );

        // Start periodic integrity check task
        let check_results = Arc::clone(&self.check_results);
        let peer_records = Arc::clone(&self.peer_records);
        let flagged_peers = Arc::clone(&self.flagged_peers);
        let stats = Arc::clone(&self.stats);
        let event_tx = self.event_tx.clone();
        let max_violations = self.max_violations;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(60));

            loop {
                interval.tick().await;

                // Check for peers that should be flagged
                let peers_to_flag: Vec<PeerId> = {
                    let records = peer_records.read().await;
                    records
                        .iter()
                        .filter(|(_, record)| {
                            record.violations >= max_violations && !record.is_flagged
                        })
                        .map(|(peer_id, _)| *peer_id)
                        .collect()
                };

                for peer_id in peers_to_flag {
                    let mut records = peer_records.write().await;
                    let mut flagged = flagged_peers.write().await;

                    if let Some(record) = records.get_mut(&peer_id) {
                        record.is_flagged = true;
                        record.trust_score = 0.0;
                        flagged.insert(peer_id);

                        // Update stats
                        let mut stats = stats.write().await;
                        stats.peers_flagged += 1;

                        // Send event
                        if let Err(e) = event_tx
                            .send(IntegrityEvent::PeerFlagged {
                                peer_id,
                                reason: format!("Exceeded max violations: {}", record.violations),
                                violations: record.violations,
                            })
                            .await
                        {
                            error!("Failed to send peer flagged event: {}", e);
                        }

                        warn!(
                            "Peer {} flagged for integrity violations: {}",
                            peer_id, record.violations
                        );
                    }
                }

                // Cleanup old check results (older than 1 hour)
                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                {
                    let mut results = check_results.write().await;
                    let initial_count = results.len();
                    results.retain(|_, result| current_time - result.timestamp < 3600);
                    let removed = initial_count - results.len();

                    if removed > 0 {
                        debug!("Cleaned up {} old integrity check results", removed);
                    }
                }
            }
        });

        info!("Integrity Receiver tasks started successfully");
        Ok(())
    }

    pub async fn verify_data_integrity(
        &self,
        data_type: DataType,
        data_id: u64,
        actual_hash: [u8; 64],
        source: PeerId,
    ) -> Result<IntegrityCheckResult> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let check_id = self.next_check_id().await;

        // Get expected hash if available
        let expected_hash = {
            let hashes = self.expected_hashes.read().await;
            hashes.get(&(data_type.clone(), data_id)).cloned()
        };

        let is_valid = match expected_hash {
            Some(expected) => actual_hash == expected,
            None => true, // No expected hash, assume valid
        };

        let result = IntegrityCheckResult {
            check_id,
            data_type: data_type.clone(),
            hash: actual_hash,
            is_valid,
            peer_id: source,
            timestamp: current_time,
            details: if is_valid {
                None
            } else {
                Some("Hash mismatch".to_string())
            },
        };

        // Store result
        {
            let mut results = self.check_results.write().await;
            results.insert(check_id, result.clone());
        }

        // Update peer record
        self.update_peer_record(source, is_valid).await;

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.checks_performed += 1;
            stats.last_check = current_time;

            if is_valid {
                stats.checks_passed += 1;
            } else {
                stats.checks_failed += 1;
                stats.mismatches_detected += 1;
            }
        }

        // Send event
        if is_valid {
            if let Err(e) = self
                .event_tx
                .send(IntegrityEvent::CheckPassed {
                    result: result.clone(),
                })
                .await
            {
                error!("Failed to send check passed event: {}", e);
            }
        } else {
            if let Some(expected) = expected_hash {
                if let Err(e) = self
                    .event_tx
                    .send(IntegrityEvent::Mismatch {
                        data_type,
                        expected_hash: expected,
                        actual_hash,
                        source,
                    })
                    .await
                {
                    error!("Failed to send mismatch event: {}", e);
                }
            }

            if let Err(e) = self
                .event_tx
                .send(IntegrityEvent::CheckFailed {
                    result: result.clone(),
                    reason: "Hash mismatch".to_string(),
                })
                .await
            {
                error!("Failed to send check failed event: {}", e);
            }
        }

        Ok(result)
    }

    pub async fn report_fault(
        &self,
        data_type: DataType,
        reason: String,
        source: PeerId,
    ) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Update peer record with fault
        self.update_peer_record(source, false).await;

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.faults_detected += 1;
        }

        // Send event
        if let Err(e) = self
            .event_tx
            .send(IntegrityEvent::Fault {
                data_type,
                reason: reason.clone(),
                source,
            })
            .await
        {
            error!("Failed to send fault event: {}", e);
        }

        warn!("Integrity fault reported from peer {}: {}", source, reason);
        Ok(())
    }

    pub async fn register_expected_hash(&self, data_type: DataType, data_id: u64, hash: [u8; 64]) {
        let mut hashes = self.expected_hashes.write().await;
        hashes.insert((data_type, data_id), hash);
    }

    pub async fn unregister_expected_hash(&self, data_type: DataType, data_id: u64) {
        let mut hashes = self.expected_hashes.write().await;
        hashes.remove(&(data_type, data_id));
    }

    async fn update_peer_record(&self, peer_id: PeerId, check_passed: bool) {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let mut records = self.peer_records.write().await;

        let record = records
            .entry(peer_id)
            .or_insert_with(|| PeerIntegrityRecord {
                peer_id,
                violations: 0,
                checks_passed: 0,
                checks_failed: 0,
                last_violation: None,
                is_flagged: false,
                trust_score: 1.0,
            });

        if check_passed {
            record.checks_passed += 1;
            // Slowly recover trust
            record.trust_score = (record.trust_score + 0.01).min(1.0);
        } else {
            record.checks_failed += 1;
            record.violations += 1;
            record.last_violation = Some(current_time);
            // Decrease trust
            record.trust_score = (record.trust_score - 0.1).max(0.0);
        }
    }

    pub async fn is_peer_trusted(&self, peer_id: &PeerId) -> bool {
        let flagged = self.flagged_peers.read().await;
        if flagged.contains(peer_id) {
            return false;
        }

        let records = self.peer_records.read().await;
        if let Some(record) = records.get(peer_id) {
            record.trust_score >= 0.5
        } else {
            true // Unknown peer, assume trusted until proven otherwise
        }
    }

    pub async fn get_peer_trust_score(&self, peer_id: &PeerId) -> f64 {
        let records = self.peer_records.read().await;
        records.get(peer_id).map(|r| r.trust_score).unwrap_or(1.0)
    }

    pub async fn get_peer_record(&self, peer_id: &PeerId) -> Option<PeerIntegrityRecord> {
        self.peer_records.read().await.get(peer_id).cloned()
    }

    pub async fn get_flagged_peers(&self) -> Vec<PeerId> {
        self.flagged_peers.read().await.iter().cloned().collect()
    }

    pub async fn unflag_peer(&self, peer_id: PeerId) -> bool {
        let mut flagged = self.flagged_peers.write().await;
        let mut records = self.peer_records.write().await;

        if flagged.remove(&peer_id) {
            if let Some(record) = records.get_mut(&peer_id) {
                record.is_flagged = false;
                record.violations = 0;
                record.trust_score = 0.5; // Reset to neutral trust
            }
            info!("Peer {} unflagged", peer_id);
            true
        } else {
            false
        }
    }

    pub async fn get_stats(&self) -> IntegrityStats {
        self.stats.read().await.clone()
    }

    pub async fn get_check_result(&self, check_id: u64) -> Option<IntegrityCheckResult> {
        self.check_results.read().await.get(&check_id).cloned()
    }

    pub async fn get_recent_checks(&self, limit: usize) -> Vec<IntegrityCheckResult> {
        let results = self.check_results.read().await;
        let mut checks: Vec<_> = results.values().cloned().collect();
        checks.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));
        checks.truncate(limit);
        checks
    }

    async fn next_check_id(&self) -> u64 {
        let mut counter = self.check_id_counter.write().await;
        let id = *counter;
        *counter += 1;
        id
    }

    pub async fn send(
        &self,
        event: IntegrityEvent,
    ) -> Result<(), mpsc::error::SendError<IntegrityEvent>> {
        self.event_tx.send(event).await
    }

    pub async fn get_event_receiver(&self) -> mpsc::Receiver<IntegrityEvent> {
        let (tx, rx) = mpsc::channel(1000);
        rx
    }
}
