// SPDX-License-Identifier: MIT
// © 2026 Savitri Network

//! Savitri Network Core Library
//! 
//! This is the foundational library for the Savitri Network blockchain ecosystem.
//! It provides core types, cryptographic primitives, utilities, and metrics
//! without external dependencies on networking, storage, or consensus layers.
//! 
//! ## Features
//! 
//! - **Core Types**: Basic data structures like Account, Transaction, FeeLimits
//! - **Cryptography**: Ed25519 signatures, SHA-256/512 hashing, key management
//! - **Slot Scheduler**: Deterministic slot scheduling and leader rotation
//! - **Monolith**: Basic monolith data structures and utilities
//! - **Utilities**: Type conversions, time helpers, mathematical functions
//! - **Metrics**: Lightweight metrics system for monitoring
//! 
//! ## Usage
//! 
//! ```rust
//! use savitri_core::{Account, generate_keypair, sign, verify};
//! 
//! // Create an account
//! let mut account = Account::default();
//! account.credit(1000).unwrap();
//! 
//! // Generate a keypair
//! let keypair = generate_keypair();
//! 
//! // Sign and verify a message
//! let message = b"Hello, Savitri!";
//! let signature = sign(message, &keypair);
//! let public_key = keypair.verifying_key();
//! assert!(verify(message, &signature, &public_key));
//! 
//! // Use slot scheduler
//! use savitri_core::{SlotScheduler, SlotSchedulerConfig};
//! 
//! let config = SlotSchedulerConfig {
//!     slot_duration: std::time::Duration::from_millis(1000),
//!     slot_base_ms: Some(1000000),
//! };
//! 
//! let scheduler = SlotScheduler::new(config).unwrap();
//! let slot_info = scheduler.current_slot_info().unwrap();
//! println!("Current slot: {}", slot_info.slot);
//! ```
//! 
//! ## Architecture
//! 
//! This library is designed to be:
//! - **Minimal**: No external dependencies beyond basic crypto libraries
//! - **Fast**: Sub-30 second compilation time
//! - **Portable**: Works across different platforms and architectures
//! - **Well-documented**: Comprehensive documentation and examples
//! 
//! ## Dependencies
//! 
//! - `serde`: Serialization support
//! - `serde_json`: JSON serialization
//! - `sha2`, `sha3`: Cryptographic hash functions
//! - `rand`: Random number generation
//! - `ed25519-dalek`: Digital signatures
//! - `blake3`: Modern hash function
//! - `chrono`: Time handling
//! - `hex`: Hex encoding/decoding
//! - `anyhow`: Error handling

#![cfg_attr(not(test), warn(missing_docs))]
#![cfg_attr(not(test), warn(clippy::all))]

// Core modules
pub mod core;
pub mod crypto;
pub mod utils;
pub mod metrics;

// Re-export commonly used types for convenience
pub use core::{
    Account, Transaction, FeeLimits,
    SlotScheduler, SlotSchedulerConfig, SlotInfo, SlotRole,
    MonolithHeader, MonolithPolicy, generate_monolith, compute_monolith_id
};

pub use crypto::{
    sha256, sha512, blake3, hash, merkle_root, hash_with_domain,
    sign, verify, generate_keypair, verify_with_security_level,
    KeyPair, KeyManager, MemoryKeyStorage, KeyStorage,
    AesGcmCipher, encrypt_with_password, decrypt_with_password, SecureRng,
    compute_tx_root, sign_data, load_or_generate_identity, sign_message,
    Keypair, keypair_to_bytes, keypair_from_bytes, public_key_to_bytes,
    public_key_from_bytes, signature_to_bytes, signature_from_bytes
};

pub use utils::{
    bytes_to_hex, hex_to_bytes, bytes_to_hex_prefixed, hex_to_bytes_prefixed,
    str_to_u64, str_to_u128, u64_to_str, u128_to_str,
    bytes_to_u64_le, bytes_to_u64_be, u64_to_bytes_le, u64_to_bytes_be,
    bytes_to_u128_le, bytes_to_u128_be, u128_to_bytes_le, u128_to_bytes_be,
    timestamp_to_datetime, duration_to_ms, ms_to_duration, duration_to_human,
    slice_to_array, array_to_vec, safe_int_convert,
    float_to_fixed, fixed_to_float, percent_to_bps, bps_to_percent,
    wei_to_ether, ether_to_wei,
    now_timestamp, now_timestamp_ms, now_timestamp_us, now_timestamp_ns,
    timestamp_to_ms, ms_to_timestamp, ms_to_datetime,
    duration_between, duration_between_ms, is_within_last_seconds, is_within_last_ms,
    add_seconds, add_ms, subtract_seconds, subtract_ms,
    format_duration, format_duration_ms, format_duration_secs,
    now_iso8601, parse_iso8601,
    slot, epoch, perf,
    fixed_point, stats,
    consensus_bincode, serialize_consensus, deserialize_consensus,
    default_bincode, serialize_default, deserialize_default,
    serialized_size, can_deserialize, serialize_to_hex, deserialize_from_hex,
    batch, compression, versioning
};

pub use metrics::{
    MetricsProvider, MetricsConfig, Metric, MetricType, ProviderStats,
    savitri_metrics, utils as metrics_utils,
    PrometheusExporter, PrometheusExporterConfig, ExporterStats, HealthChecker, HealthStatus,
    MetricsManifest, MetricDefinition, LabelDefinition, MetricCategory,
    PlatformInfo, SecurityInfo, ThresholdDefinition, ThresholdType,
    ManifestGenerator
};

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Library name
pub const NAME: &str = env!("CARGO_PKG_NAME");

/// Library description
pub const DESCRIPTION: &str = env!("CARGO_PKG_DESCRIPTION");

/// Default slot duration in milliseconds
pub const DEFAULT_SLOT_DURATION_MS: u64 = 1000;

/// Default block time in milliseconds
pub const DEFAULT_BLOCK_TIME_MS: u64 = 500;

/// Default consensus timeout in milliseconds
pub const DEFAULT_CONSENSUS_TIMEOUT_MS: u64 = 5000;

/// Default network timeout in milliseconds
pub const DEFAULT_NETWORK_TIMEOUT_MS: u64 = 30000;

/// Default metrics port
pub const DEFAULT_METRICS_PORT: u16 = 9090;

/// Default P2P port
pub const DEFAULT_P2P_PORT: u16 = 8333;

/// Default RPC port
pub const DEFAULT_RPC_PORT: u16 = 8545;

/// Genesis block height
pub const GENESIS_HEIGHT: u64 = 0;

/// Genesis block timestamp
pub const GENESIS_TIMESTAMP: u64 = 1000000;

/// Default account balance (in wei)
pub const DEFAULT_BALANCE: u128 = 1_000_000_000_000_000_000; // 1000 tokens

/// Default minimum fee (in wei)
pub const DEFAULT_MIN_FEE: u128 = 100_000_000_000_000; // 0.0001 tokens

/// Default maximum fee (in wei)
pub const DEFAULT_MAX_FEE: u128 = 1_000_000_000_000_000_000; // 1 token

/// Token decimals (18 decimal places like Ethereum)
pub const TOKEN_DECIMALS: u8 = 18;

/// Wei per token
pub const WEI_PER_TOKEN: u128 = 10u128.pow(TOKEN_DECIMALS as u32);

/// Error types for the core library
#[derive(Debug, Clone, thiserror::Error)]
pub enum CoreError {
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    
    #[error("Serialization error: {0}")]
    SerializationError(String),
    
    #[error("Deserialization error: {0}")]
    DeserializationError(String),
    
    #[error("Cryptographic error: {0}")]
    CryptographicError(String),
    
    #[error("Invalid configuration: {0}")]
    InvalidConfiguration(String),
    
    #[error("Slot calculation error: {0}")]
    SlotCalculationError(String),
    
    #[error("Monolith error: {0}")]
    MonolithError(String),
    
    #[error("Key error: {0}")]
    KeyError(String),
    
    #[error("Metric error: {0}")]
    MetricError(String),
}

impl CoreError {
    /// Create a new invalid input error
    pub fn invalid_input(msg: impl Into<String>) -> Self {
        Self::InvalidInput(msg.into())
    }

    /// Create a new serialization error
    pub fn serialization_error(msg: impl Into<String>) -> Self {
        Self::SerializationError(msg.into())
    }

    /// Create a new deserialization error
    pub fn deserialization_error(msg: impl Into<String>) -> Self {
        Self::DeserializationError(msg.into())
    }

    /// Create a new cryptographic error
    pub fn cryptographic_error(msg: impl Into<String>) -> Self {
        Self::CryptographicError(msg.into())
    }

    /// Create a new configuration error
    pub fn invalid_configuration(msg: impl Into<String>) -> Self {
        Self::InvalidConfiguration(msg.into())
    }

    /// Create a new slot calculation error
    pub fn slot_calculation_error(msg: impl Into<String>) -> Self {
        Self::SlotCalculationError(msg.into())
    }

    /// Create a new monolith error
    pub fn monolith_error(msg: impl Into<String>) -> Self {
        Self::MonolithError(msg.into())
    }

    /// Create a new key error
    pub fn key_error(msg: impl Into<String>) -> Self {
        Self::KeyError(msg.into())
    }

    /// Create a new metric error
    pub fn metric_error(msg: impl Into<String>) -> Self {
        Self::MetricError(msg.into())
    }
}

/// Result type alias for the core library
pub type CoreResult<T> = Result<T, CoreError>;

pub trait Validate {
    /// Validate the type and return an error if invalid
    fn validate(&self) -> CoreResult<()>;
}

impl Validate for Account {
    fn validate(&self) -> CoreResult<()> {
        if self.balance > u128::MAX / 2 {
            return Err(CoreError::invalid_input("Account balance too large"));
        }
        
        if self.nonce > u64::MAX / 2 {
            return Err(CoreError::invalid_input("Account nonce too large"));
        }
        
        Ok(())
    }
}

impl Validate for MonolithHeader {
    fn validate(&self) -> CoreResult<()> {
        if self.exec_height < self.window_start {
            return Err(CoreError::monolith_error("Exec height cannot be less than window start"));
        }
        
        if self.window_start > self.exec_height {
            return Err(CoreError::monolith_error("Window start cannot be greater than exec height"));
        }
        
        Ok(())
    }
}

/// Utility functions for common operations
pub mod helpers {
    use super::*;

    /// Check if a value is within a range
    pub fn in_range<T: PartialOrd>(value: T, min: T, max: T) -> bool {
        value >= min && value <= max
    }

    /// Clamp a value to a range
    pub fn clamp_range<T: PartialOrd + Copy + std::cmp::Ord>(value: T, min: T, max: T) -> T {
        std::cmp::max(min, std::cmp::min(value, max))
    }

    /// Check if a string is empty or whitespace
    pub fn is_empty_or_whitespace(s: &str) -> bool {
        s.trim().is_empty()
    }

    /// Validate that a string is not empty or whitespace
    pub fn validate_nonempty(s: &str) -> CoreResult<()> {
        if is_empty_or_whitespace(s) {
            Err(CoreError::invalid_input("String cannot be empty or whitespace"))
        } else {
            Ok(())
        }
    }

    /// Validate that a vector is not empty
    pub fn validate_nonempty_vec<T>(vec: &[T]) -> CoreResult<()> {
        if vec.is_empty() {
            Err(CoreError::invalid_input("Vector cannot be empty"))
        } else {
            Ok(())
        }
    }

    /// Validate that a value is within bounds
    pub fn validate_bounds<T: PartialOrd + Copy + std::fmt::Display>(value: T, min: T, max: T) -> CoreResult<()> {
        if !in_range(value, min, max) {
            Err(CoreError::invalid_input(format!(
                "Value {} is not within range [{}, {}]",
                value, min, max
            )))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_library_constants() {
        assert!(!VERSION.is_empty());
        assert!(!NAME.is_empty());
        assert!(!DESCRIPTION.is_empty());
        assert_eq!(DEFAULT_SLOT_DURATION_MS, 1000);
        assert_eq!(DEFAULT_BLOCK_TIME_MS, 500);
        assert_eq!(TOKEN_DECIMALS, 18);
        assert_eq!(WEI_PER_TOKEN, 10u128.pow(18));
    }

    #[test]
    fn test_account_validation() {
        let mut account = Account::default();
        assert!(account.validate().is_ok());

        // Test invalid balance
        account.balance = u128::MAX;
        assert!(account.validate().is_err());

        // Test invalid nonce
        account.balance = 1000;
        account.nonce = u64::MAX;
        assert!(account.validate().is_err());
    }

    #[test]
    fn test_monolith_validation() {
        let header = MonolithHeader::new(
            100,  // height
            50,   // timestamp
            [0u8; 64],  // hash
            [0u8; 64],  // parent_hash
            [0u8; 64],  // headers_commit
            [0u8; 64],  // state_commit
            1,    // block_count
            1024, // size_bytes
            100,  // exec_height
            50,   // window_start
            1,    // epoch_id
            [0u8; 32],  // producer
        );
        assert!(header.validate().is_ok());

        // Test invalid range
        let invalid_header = MonolithHeader::new(
            50,   // height
            100,  // timestamp
            [0u8; 64],  // hash
            [0u8; 64],  // parent_hash
            [0u8; 64],  // headers_commit
            [0u8; 64],  // state_commit
            1,    // block_count
            1024, // size_bytes
            50,   // exec_height < window_start
            100,  // window_start
            1,    // epoch_id
            [0u8; 32],  // producer
        );
        assert!(invalid_header.validate().is_err());
    }

    #[test]
    fn test_helpers() {
        use helpers::*;

        assert!(in_range(5, 1, 10));
        assert!(!in_range(0, 1, 10));
        assert!(!in_range(11, 1, 10));

        assert_eq!(clamp_range(5, 1, 10), 5);
        assert_eq!(clamp_range(0, 1, 10), 1);
        assert_eq!(clamp_range(15, 1, 10), 10);

        assert!(is_empty_or_whitespace(""));
        assert!(is_empty_or_whitespace("   "));
        assert!(!is_empty_or_whitespace("test"));

        assert!(validate_nonempty("test").is_ok());
        assert!(validate_nonempty("").is_err());
        assert!(validate_nonempty("   ").is_err());

        assert!(validate_nonempty_vec(&[1, 2, 3]).is_ok());
        assert!(validate_nonempty_vec::<i32>(&[]).is_err());

        assert!(validate_bounds(5, 1, 10).is_ok());
        assert!(validate_bounds(0, 1, 10).is_err());
        assert!(validate_bounds(15, 1, 10).is_err());
    }

    #[test]
    fn test_core_error() {
        let error = CoreError::invalid_input("test error");
        assert!(matches!(error, CoreError::InvalidInput(_)));

        let error = CoreError::cryptographic_error("crypto error");
        assert!(matches!(error, CoreError::CryptographicError(_)));
    }

    #[test]
    fn test_basic_usage() {
        // Test basic functionality
        let keypair = generate_keypair();
        let message = b"test message";
        let signature = sign(message, &keypair);
        let public_key = keypair.verifying_key();
        
        assert!(verify(message, &signature, &public_key));

        // Test hashing
        let hash = sha256(message);
        assert_eq!(hash.len(), 32);

        // Test slot scheduler
        let config = SlotSchedulerConfig {
            slot_duration: std::time::Duration::from_millis(1000),
            validators: vec!["validator1".to_string()],
            local_id: "validator1".to_string(),
            slot_base_ms: Some(1000000),
        };
        let scheduler = SlotScheduler::new(config).unwrap();
        assert!(scheduler.is_validator());
    }
}
