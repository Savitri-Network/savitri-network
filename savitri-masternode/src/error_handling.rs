//! Error Handling Module
//!
//! This module provides comprehensive error handling for edge cases,

use std::fmt;
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Main error type for masternode operations
#[derive(Debug, Clone)]
pub enum MasternodeError {
    // Network errors
    NetworkTimeout {
        operation: String,
        timeout: Duration,
    },
    ConnectionFailed {
        peer_id: String,
        reason: String,
    },
    MessageDecodeFailed {
        topic: String,
        error: String,
    },
    InsufficientPeers {
        required: usize,
        available: usize,
    },

    // Validation errors
    InvalidTransaction {
        tx_hash: String,
        reason: String,
    },
    InvalidBlock {
        block_hash: String,
        reason: String,
    },
    InvalidSignature {
        signer: String,
        reason: String,
    },
    InvalidCertificate {
        block_hash: String,
        reason: String,
    },

    // Consensus errors
    DuplicateTransaction {
        tx_hash: String,
    },
    BlockRejected {
        block_hash: String,
        uniqueness_ratio: f64,
    },
    ConsensusTimeout {
        height: u64,
        round: u32,
    },
    InsufficientVotes {
        required: usize,
        received: usize,
    },

    // State errors
    MempoolFull {
        max_size: usize,
    },
    CacheFull {
        cache_name: String,
    },
    InvalidState {
        expected: String,
        actual: String,
    },

    // Configuration errors
    InvalidConfig {
        field: String,
        value: String,
    },
    MissingConfig {
        field: String,
    },
}

impl fmt::Display for MasternodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MasternodeError::NetworkTimeout { operation, timeout } => {
                write!(f, "Network timeout: {} (timeout: {:?})", operation, timeout)
            }
            MasternodeError::ConnectionFailed { peer_id, reason } => {
                write!(f, "Connection failed to {}: {}", peer_id, reason)
            }
            MasternodeError::MessageDecodeFailed { topic, error } => {
                write!(f, "Failed to decode message on topic {}: {}", topic, error)
            }
            MasternodeError::InsufficientPeers {
                required,
                available,
            } => {
                write!(
                    f,
                    "Insufficient peers: {} required, {} available",
                    required, available
                )
            }
            MasternodeError::InvalidTransaction { tx_hash, reason } => {
                write!(f, "Invalid transaction {}: {}", tx_hash, reason)
            }
            MasternodeError::InvalidBlock { block_hash, reason } => {
                write!(f, "Invalid block {}: {}", block_hash, reason)
            }
            MasternodeError::InvalidSignature { signer, reason } => {
                write!(f, "Invalid signature from {}: {}", signer, reason)
            }
            MasternodeError::InvalidCertificate { block_hash, reason } => {
                write!(
                    f,
                    "Invalid certificate for block {}: {}",
                    block_hash, reason
                )
            }
            MasternodeError::DuplicateTransaction { tx_hash } => {
                write!(f, "Duplicate transaction: {}", tx_hash)
            }
            MasternodeError::BlockRejected {
                block_hash,
                uniqueness_ratio,
            } => {
                write!(
                    f,
                    "Block {} rejected (uniqueness: {:.2}%)",
                    block_hash,
                    uniqueness_ratio * 100.0
                )
            }
            MasternodeError::ConsensusTimeout { height, round } => {
                write!(f, "Consensus timeout at height {} round {}", height, round)
            }
            MasternodeError::InsufficientVotes { required, received } => {
                write!(
                    f,
                    "Insufficient votes: {} required, {} received",
                    required, received
                )
            }
            MasternodeError::MempoolFull { max_size } => {
                write!(f, "Mempool full (max: {})", max_size)
            }
            MasternodeError::CacheFull { cache_name } => {
                write!(f, "Cache full: {}", cache_name)
            }
            MasternodeError::InvalidState { expected, actual } => {
                write!(f, "Invalid state: expected {}, got {}", expected, actual)
            }
            MasternodeError::InvalidConfig { field, value } => {
                write!(f, "Invalid configuration: {} = {}", field, value)
            }
            MasternodeError::MissingConfig { field } => {
                write!(f, "Missing configuration: {}", field)
            }
        }
    }
}

impl std::error::Error for MasternodeError {}

/// Error recovery strategies
#[derive(Debug, Clone)]
pub enum RecoveryStrategy {
    Retry { max_attempts: u32, delay: Duration },
    Skip,
    Fallback { alternative: String },
    Abort,
}

/// Error handler with retry logic
#[derive(Debug)]
pub struct ErrorHandler {
    max_retries: u32,
    retry_delay: Duration,
    error_counts: std::collections::HashMap<String, u32>,
}

impl ErrorHandler {
    pub fn new(max_retries: u32, retry_delay_ms: u64) -> Self {
        Self {
            max_retries,
            retry_delay: Duration::from_millis(retry_delay_ms),
            error_counts: std::collections::HashMap::new(),
        }
    }

    /// Handle an error with automatic retry logic
    pub async fn handle_with_retry<F, T, E>(
        &mut self,
        operation_name: &str,
        mut operation: F,
    ) -> Result<T, MasternodeError>
    where
        F: FnMut() -> Result<T, E>,
        E: std::fmt::Display,
    {
        let mut attempts = 0;

        loop {
            attempts += 1;

            match operation() {
                Ok(result) => {
                    // Reset error count on success
                    self.error_counts.remove(operation_name);
                    return Ok(result);
                }
                Err(e) => {
                    let error_key = operation_name.to_string();
                    let count = self.error_counts.entry(error_key).or_insert(0);
                    *count += 1;

                    if attempts >= self.max_retries {
                        error!(
                            operation = operation_name,
                            attempts = attempts,
                            error = %e,
                            "Operation failed after max retries"
                        );
                        return Err(MasternodeError::NetworkTimeout {
                            operation: operation_name.to_string(),
                            timeout: self.retry_delay * attempts,
                        });
                    }

                    warn!(
                        operation = operation_name,
                        attempt = attempts,
                        max_retries = self.max_retries,
                        error = %e,
                        "Operation failed, retrying..."
                    );

                    tokio::time::sleep(self.retry_delay).await;
                }
            }
        }
    }

    /// Get recovery strategy for an error
    pub fn get_recovery_strategy(&self, error: &MasternodeError) -> RecoveryStrategy {
        match error {
            MasternodeError::NetworkTimeout { .. } => RecoveryStrategy::Retry {
                max_attempts: 3,
                delay: Duration::from_millis(100),
            },
            MasternodeError::ConnectionFailed { .. } => RecoveryStrategy::Retry {
                max_attempts: 5,
                delay: Duration::from_millis(500),
            },
            MasternodeError::MessageDecodeFailed { .. } => RecoveryStrategy::Skip,
            MasternodeError::InsufficientPeers { .. } => RecoveryStrategy::Retry {
                max_attempts: 10,
                delay: Duration::from_secs(1),
            },
            MasternodeError::InvalidTransaction { .. } => RecoveryStrategy::Skip,
            MasternodeError::InvalidBlock { .. } => RecoveryStrategy::Skip,
            MasternodeError::InvalidSignature { .. } => RecoveryStrategy::Skip,
            MasternodeError::InvalidCertificate { .. } => RecoveryStrategy::Skip,
            MasternodeError::DuplicateTransaction { .. } => RecoveryStrategy::Skip,
            MasternodeError::BlockRejected { .. } => RecoveryStrategy::Skip,
            MasternodeError::ConsensusTimeout { .. } => RecoveryStrategy::Retry {
                max_attempts: 3,
                delay: Duration::from_secs(2),
            },
            MasternodeError::InsufficientVotes { .. } => RecoveryStrategy::Retry {
                max_attempts: 3,
                delay: Duration::from_secs(1),
            },
            MasternodeError::MempoolFull { .. } => RecoveryStrategy::Fallback {
                alternative: "evict_oldest".to_string(),
            },
            MasternodeError::CacheFull { .. } => RecoveryStrategy::Fallback {
                alternative: "clear_lru".to_string(),
            },
            MasternodeError::InvalidState { .. } => RecoveryStrategy::Abort,
            MasternodeError::InvalidConfig { .. } => RecoveryStrategy::Abort,
            MasternodeError::MissingConfig { .. } => RecoveryStrategy::Abort,
        }
    }

    /// Log error with appropriate level
    pub fn log_error(&self, error: &MasternodeError) {
        match error {
            MasternodeError::NetworkTimeout { .. }
            | MasternodeError::ConnectionFailed { .. }
            | MasternodeError::InsufficientPeers { .. } => {
                warn!(error = %error, "Network error");
            }
            MasternodeError::MessageDecodeFailed { .. }
            | MasternodeError::InvalidTransaction { .. }
            | MasternodeError::DuplicateTransaction { .. } => {
                debug!(error = %error, "Validation error");
            }
            MasternodeError::InvalidBlock { .. }
            | MasternodeError::InvalidSignature { .. }
            | MasternodeError::InvalidCertificate { .. }
            | MasternodeError::BlockRejected { .. } => {
                warn!(error = %error, "Block validation error");
            }
            MasternodeError::ConsensusTimeout { .. }
            | MasternodeError::InsufficientVotes { .. } => {
                warn!(error = %error, "Consensus error");
            }
            MasternodeError::MempoolFull { .. } | MasternodeError::CacheFull { .. } => {
                info!(error = %error, "Resource limit reached");
            }
            MasternodeError::InvalidState { .. }
            | MasternodeError::InvalidConfig { .. }
            | MasternodeError::MissingConfig { .. } => {
                error!(error = %error, "Critical error");
            }
        }
    }

    /// Get error statistics
    pub fn get_error_stats(&self) -> ErrorStats {
        let total_errors: u32 = self.error_counts.values().sum();
        let unique_errors = self.error_counts.len();
        let most_frequent = self
            .error_counts
            .iter()
            .max_by_key(|(_, &count)| count)
            .map(|(name, &count)| (name.clone(), count));

        ErrorStats {
            total_errors,
            unique_errors,
            most_frequent,
        }
    }

    /// Reset error counts
    pub fn reset_stats(&mut self) {
        self.error_counts.clear();
    }
}

impl Default for ErrorHandler {
    fn default() -> Self {
        Self::new(3, 100)
    }
}

#[derive(Debug, Clone)]
pub struct ErrorStats {
    pub total_errors: u32,
    pub unique_errors: usize,
    pub most_frequent: Option<(String, u32)>,
}

/// Circuit breaker for failing operations
#[derive(Debug)]
pub struct CircuitBreaker {
    failure_threshold: u32,
    success_threshold: u32,
    timeout: Duration,
    state: CircuitState,
    failure_count: u32,
    success_count: u32,
    last_failure: Option<std::time::Instant>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitBreaker {
    pub fn new(failure_threshold: u32, success_threshold: u32, timeout_secs: u64) -> Self {
        Self {
            failure_threshold,
            success_threshold,
            timeout: Duration::from_secs(timeout_secs),
            state: CircuitState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure: None,
        }
    }

    /// Check if operation should be allowed
    pub fn allow_request(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                // Check if timeout has passed
                if let Some(last_failure) = self.last_failure {
                    if last_failure.elapsed() >= self.timeout {
                        self.state = CircuitState::HalfOpen;
                        self.success_count = 0;
                        info!("Circuit breaker moved to half-open state");
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    /// Record a successful operation
    pub fn record_success(&mut self) {
        match self.state {
            CircuitState::Closed => {
                self.failure_count = 0;
            }
            CircuitState::HalfOpen => {
                self.success_count += 1;
                if self.success_count >= self.success_threshold {
                    self.state = CircuitState::Closed;
                    self.failure_count = 0;
                    info!("Circuit breaker closed after successful recovery");
                }
            }
            CircuitState::Open => {}
        }
    }

    /// Record a failed operation
    pub fn record_failure(&mut self) {
        self.failure_count += 1;
        self.last_failure = Some(std::time::Instant::now());

        match self.state {
            CircuitState::Closed => {
                if self.failure_count >= self.failure_threshold {
                    self.state = CircuitState::Open;
                    warn!(
                        failures = self.failure_count,
                        "Circuit breaker opened due to failures"
                    );
                }
            }
            CircuitState::HalfOpen => {
                self.state = CircuitState::Open;
                warn!("Circuit breaker re-opened after failure in half-open state");
            }
            CircuitState::Open => {}
        }
    }

    /// Get current state
    pub fn state(&self) -> &CircuitState {
        &self.state
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let error = MasternodeError::InvalidTransaction {
            tx_hash: "abc123".to_string(),
            reason: "invalid signature".to_string(),
        };
        assert!(error.to_string().contains("Invalid transaction"));
    }

    #[test]
    fn test_circuit_breaker_closed() {
        let mut breaker = CircuitBreaker::new(3, 2, 10);
        assert!(breaker.allow_request());
        assert_eq!(breaker.state(), &CircuitState::Closed);
    }

    #[test]
    fn test_circuit_breaker_opens_on_failures() {
        let mut breaker = CircuitBreaker::new(3, 2, 10);

        // Record 3 failures
        breaker.record_failure();
        breaker.record_failure();
        assert_eq!(breaker.state(), &CircuitState::Closed);

        breaker.record_failure();
        assert_eq!(breaker.state(), &CircuitState::Open);
        assert!(!breaker.allow_request());
    }

    #[test]
    fn test_error_handler_stats() {
        let mut handler = ErrorHandler::new(3, 100);

        // Simulate errors
        handler.error_counts.insert("network_error".to_string(), 5);
        handler
            .error_counts
            .insert("validation_error".to_string(), 3);

        let stats = handler.get_error_stats();
        assert_eq!(stats.total_errors, 8);
        assert_eq!(stats.unique_errors, 2);
    }

    #[test]
    fn test_recovery_strategy() {
        let handler = ErrorHandler::new(3, 100);

        let timeout_error = MasternodeError::NetworkTimeout {
            operation: "test".to_string(),
            timeout: Duration::from_secs(1),
        };

        let strategy = handler.get_recovery_strategy(&timeout_error);
        assert!(matches!(strategy, RecoveryStrategy::Retry { .. }));

        let invalid_tx_error = MasternodeError::InvalidTransaction {
            tx_hash: "test".to_string(),
            reason: "test".to_string(),
        };

        let strategy = handler.get_recovery_strategy(&invalid_tx_error);
        assert!(matches!(strategy, RecoveryStrategy::Skip));
    }
}
