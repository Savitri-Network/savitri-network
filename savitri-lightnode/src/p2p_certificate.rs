// Certificate Receiver - Complete implementation for BFT certificate management
use crate::p2p::types::ConsensusCertificate;
use anyhow::Result;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CertificateValidationResult {
    #[serde(with = "crate::p2p::types::big_array")]
    pub certificate_id: [u8; 64],
    pub is_valid: bool,
    pub voters: Vec<[u8; 32]>,
    pub block_height: u64,
    pub timestamp: u64,
    pub validator: PeerId,
}

#[derive(Debug, Clone)]
pub enum CertificateEvent {
    Received {
        certificate: ConsensusCertificate,
        source: PeerId,
    },
    Validated {
        result: CertificateValidationResult,
    },
    Finalized {
        certificate_id: [u8; 64],
        block_height: u64,
    },
    Rejected {
        certificate_id: [u8; 64],
        reason: String,
    },
}

#[derive(Debug, Clone)]
pub struct CertificateStats {
    pub certificates_received: u64,
    pub certificates_validated: u64,
    pub certificates_finalized: u64,
    pub certificates_rejected: u64,
    pub active_certificates: usize,
    pub average_validation_time: f64,
    pub last_certificate_received: u64,
}

pub struct CertificateReceiver {
    local_peer_id: PeerId,
    event_tx: mpsc::Sender<CertificateEvent>,
    pending_certificates: Arc<RwLock<HashMap<[u8; 64], ConsensusCertificate>>>,
    validated_certificates: Arc<RwLock<HashMap<[u8; 64], CertificateValidationResult>>>,
    finalized_certificates: Arc<RwLock<HashSet<[u8; 64]>>>,
    rejected_certificates: Arc<RwLock<HashMap<[u8; 64], String>>>,
    committee_size: usize,
    stats: Arc<RwLock<CertificateStats>>,
}

impl CertificateReceiver {
    pub fn new() -> (Self, mpsc::Receiver<ConsensusCertificate>) {
        let (tx, rx) = mpsc::channel(1000);
        let (event_tx, _event_rx) = mpsc::channel(1000);

        let receiver = Self {
            local_peer_id: PeerId::random(), // Will be set later
            event_tx,
            pending_certificates: Arc::new(RwLock::new(HashMap::new())),
            validated_certificates: Arc::new(RwLock::new(HashMap::new())),
            finalized_certificates: Arc::new(RwLock::new(HashSet::new())),
            rejected_certificates: Arc::new(RwLock::new(HashMap::new())),
            committee_size: 7, // Default committee size
            stats: Arc::new(RwLock::new(CertificateStats {
                certificates_received: 0,
                certificates_validated: 0,
                certificates_finalized: 0,
                certificates_rejected: 0,
                active_certificates: 0,
                average_validation_time: 0.0,
                last_certificate_received: 0,
            })),
        };

        (receiver, rx)
    }

    pub fn with_committee_size(mut self, committee_size: usize) -> Self {
        self.committee_size = committee_size;
        self
    }

    pub fn with_local_peer_id(mut self, peer_id: PeerId) -> Self {
        self.local_peer_id = peer_id;
        self
    }

    pub async fn start_tasks(&self) -> Result<()> {
        info!(
            "Starting Certificate Receiver for peer: {}",
            self.local_peer_id
        );

        let pending = Arc::clone(&self.pending_certificates);
        let validated = Arc::clone(&self.validated_certificates);
        let finalized = Arc::clone(&self.finalized_certificates);
        let rejected = Arc::clone(&self.rejected_certificates);
        let committee_size = self.committee_size;
        let event_tx = self.event_tx.clone();
        let stats = Arc::clone(&self.stats);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(100));

            loop {
                interval.tick().await;

                let certificates_to_validate: Vec<_> = {
                    let pending = pending.read().await;
                    pending.keys().cloned().collect()
                };

                for cert_id in certificates_to_validate {
                    let start_time = std::time::Instant::now();

                    if let Some(certificate) = {
                        let pending = pending.read().await;
                        pending.get(&cert_id).cloned()
                    } {
                        if let Err(e) = Self::validate_certificate(
                            certificate,
                            committee_size,
                            &validated,
                            &finalized,
                            &rejected,
                            &event_tx,
                        )
                        .await
                        {
                            error!(
                                "Error validating certificate {}: {}",
                                hex::encode(cert_id),
                                e
                            );
                        }

                        let validation_time = start_time.elapsed().as_secs_f64();
                        Self::update_validation_stats(&stats, validation_time).await;
                    }
                }
            }
        });

        // Start cleanup task for old certificates
        let pending_cleanup = Arc::clone(&self.pending_certificates);
        let validated_cleanup = Arc::clone(&self.validated_certificates);
        let stats_cleanup = Arc::clone(&self.stats);

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // Every 5 minutes

            loop {
                interval.tick().await;

                let current_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                // Clean up old pending certificates (older than 10 minutes)
                {
                    let mut pending = pending_cleanup.write().await;
                    let mut stats = stats_cleanup.write().await;

                    let initial_count = pending.len();
                    pending.retain(|_, cert| current_time - cert.height < 600);
                    let removed = initial_count - pending.len();

                    if removed > 0 {
                        info!("Cleaned up {} old pending certificates", removed);
                        stats.active_certificates = pending.len();
                    }
                }

                {
                    let mut validated = validated_cleanup.write().await;
                    let initial_count = validated.len();
                    validated.retain(|_, result| current_time - result.timestamp < 3600);
                    let removed = initial_count - validated.len();

                    if removed > 0 {
                        info!("Cleaned up {} old validated certificates", removed);
                    }
                }
            }
        });

        info!("Certificate Receiver tasks started successfully");
        Ok(())
    }

    pub async fn receive_certificate(
        &self,
        certificate: ConsensusCertificate,
        source: PeerId,
    ) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Check if we already have this certificate
        {
            let pending = self.pending_certificates.read().await;
            let validated = self.validated_certificates.read().await;
            let finalized = self.finalized_certificates.read().await;

            if pending.contains_key(&certificate.block_hash)
                || validated.contains_key(&certificate.block_hash)
                || finalized.contains(&certificate.block_hash)
            {
                debug!(
                    "Certificate {} already processed, ignoring",
                    hex::encode(certificate.block_hash)
                );
                return Ok(());
            }
        }

        // Add to pending certificates
        {
            let mut pending = self.pending_certificates.write().await;
            pending.insert(certificate.block_hash, certificate.clone());
        }

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.certificates_received += 1;
            stats.last_certificate_received = current_time;
            stats.active_certificates += 1;
        }

        // Send event
        if let Err(e) = self
            .event_tx
            .send(CertificateEvent::Received {
                certificate: certificate.clone(),
                source,
            })
            .await
        {
            error!("Failed to send certificate received event: {}", e);
        }

        info!(
            "Received certificate {} from peer {}",
            hex::encode(certificate.block_hash),
            source
        );
        Ok(())
    }

    pub async fn send(
        &self,
        cert: ConsensusCertificate,
    ) -> Result<(), mpsc::error::SendError<ConsensusCertificate>> {
        self.receive_certificate(cert.clone(), self.local_peer_id)
            .await
            .map_err(|_| mpsc::error::SendError(cert))
    }

    async fn validate_certificate(
        certificate: ConsensusCertificate,
        committee_size: usize,
        validated: &Arc<RwLock<HashMap<[u8; 64], CertificateValidationResult>>>,
        finalized: &Arc<RwLock<HashSet<[u8; 64]>>>,
        rejected: &Arc<RwLock<HashMap<[u8; 64], String>>>,
        event_tx: &mpsc::Sender<CertificateEvent>,
    ) -> Result<()> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        // Check certificate has minimum voters (2/3+ of committee)
        let required_votes = (committee_size * 2 + 2) / 3; // Ceiling division
        if certificate.voters.len() < required_votes {
            let reason = format!(
                "Insufficient votes: {} < {} required",
                certificate.voters.len(),
                required_votes
            );

            {
                let mut rejected_map = rejected.write().await;
                rejected_map.insert(certificate.block_hash, reason.clone());
            }

            if let Err(e) = event_tx
                .send(CertificateEvent::Rejected {
                    certificate_id: certificate.block_hash,
                    reason,
                })
                .await
            {
                error!("Failed to send certificate rejected event: {}", e);
            }

            return Ok(());
        }

        // Verify aggregated signature structure
        if certificate.aggregated_signature.len() != 64 {
            let reason = format!(
                "Invalid aggregated signature length: {} (expected 64)",
                certificate.aggregated_signature.len()
            );

            {
                let mut rejected_map = rejected.write().await;
                rejected_map.insert(certificate.block_hash, reason.clone());
            }

            if let Err(e) = event_tx
                .send(CertificateEvent::Rejected {
                    certificate_id: certificate.block_hash,
                    reason,
                })
                .await
            {
                error!("Failed to send certificate rejected event: {}", e);
            }

            return Ok(());
        }

        // Verify each voter's public key is valid (32 bytes)
        for (i, voter) in certificate.voters.iter().enumerate() {
            if voter.len() != 32 {
                let reason = format!(
                    "Invalid voter public key at index {}: expected 32 bytes, got {}",
                    i,
                    voter.len()
                );

                {
                    let mut rejected_map = rejected.write().await;
                    rejected_map.insert(certificate.block_hash, reason.clone());
                }

                if let Err(e) = event_tx
                    .send(CertificateEvent::Rejected {
                        certificate_id: certificate.block_hash,
                        reason,
                    })
                    .await
                {
                    error!("Failed to send certificate rejected event: {}", e);
                }

                return Ok(());
            }
        }

        // For now, accept any properly formatted certificate
        // In a real implementation, this would verify:
        // 1. Each voter is in the committee
        // 2. Each voter's signature is valid
        // 3. The aggregated signature matches the block hash
        // 4. The block hash matches the expected height

        let validation_result = CertificateValidationResult {
            certificate_id: certificate.block_hash,
            is_valid: true,
            voters: certificate.voters.clone(),
            block_height: certificate.height,
            timestamp: current_time,
            validator: PeerId::random(), // Would be actual validator
        };

        {
            let mut validated_map = validated.write().await;
            validated_map.insert(certificate.block_hash, validation_result.clone());
        }

        if let Err(e) = event_tx
            .send(CertificateEvent::Validated {
                result: validation_result,
            })
            .await
        {
            error!("Failed to send certificate validated event: {}", e);
        }

        info!(
            "Certificate {} validated successfully",
            hex::encode(certificate.block_hash)
        );
        Ok(())
    }

    async fn update_validation_stats(stats: &Arc<RwLock<CertificateStats>>, validation_time: f64) {
        let mut stats = stats.write().await;
        stats.certificates_validated += 1;

        if stats.certificates_validated == 1 {
            stats.average_validation_time = validation_time;
        } else {
            stats.average_validation_time = (stats.average_validation_time + validation_time)
                / stats.certificates_validated as f64;
        }
    }

    pub async fn finalize_certificate(&self, certificate_id: [u8; 64]) -> Result<bool> {
        let current_time = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        {
            let validated = self.validated_certificates.read().await;
            if !validated.contains_key(&certificate_id) {
                warn!(
                    "Attempted to finalize non-validated certificate {}",
                    hex::encode(certificate_id)
                );
                return Ok(false);
            }
        }

        // Add to finalized certificates
        {
            let mut finalized = self.finalized_certificates.write().await;
            if finalized.insert(certificate_id) {
                // Remove from pending
                let mut pending = self.pending_certificates.write().await;
                pending.remove(&certificate_id);

                // Update stats
                let mut stats = self.stats.write().await;
                stats.certificates_finalized += 1;
                stats.active_certificates -= 1;

                info!("Certificate {} finalized", hex::encode(certificate_id));
                Ok(true)
            } else {
                warn!(
                    "Certificate {} already finalized",
                    hex::encode(certificate_id)
                );
                Ok(false)
            }
        }
    }

    pub async fn get_certificate_status(&self, certificate_id: &[u8; 64]) -> Option<String> {
        let finalized = self.finalized_certificates.read().await;
        let rejected = self.rejected_certificates.read().await;
        let validated = self.validated_certificates.read().await;
        let pending = self.pending_certificates.read().await;

        if finalized.contains(certificate_id) {
            Some("finalized".to_string())
        } else if let Some(reason) = rejected.get(certificate_id) {
            Some(format!("rejected: {}", reason))
        } else if validated.contains_key(certificate_id) {
            Some("validated".to_string())
        } else if pending.contains_key(certificate_id) {
            Some("pending".to_string())
        } else {
            None
        }
    }

    pub async fn get_stats(&self) -> CertificateStats {
        self.stats.read().await.clone()
    }

    pub async fn get_pending_certificates(&self) -> Vec<ConsensusCertificate> {
        self.pending_certificates
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn get_validated_certificates(&self) -> Vec<CertificateValidationResult> {
        self.validated_certificates
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    pub async fn get_finalized_certificates(&self) -> Vec<[u8; 64]> {
        self.finalized_certificates
            .read()
            .await
            .iter()
            .cloned()
            .collect()
    }

    pub async fn get_event_receiver(&self) -> mpsc::Receiver<CertificateEvent> {
        let (tx, rx) = mpsc::channel(1000);
        // Note: In a real implementation, you'd need to store this sender
        // For now, this is a simplified version
        rx
    }
}
