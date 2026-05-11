#![allow(missing_docs)]

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
//! ```ignore
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
//! - `chrono`: Time handling
//! - `hex`: Hex encoding/decoding
//! - `thiserror`: Error handling

// Documentation warnings enabled - comprehensive docs now implemented
#![cfg_attr(not(test), warn(missing_docs))]
#![cfg_attr(not(test), warn(clippy::all))]

// Core modules
pub mod compression;
pub mod core;
pub mod crypto;
pub mod fl_robust;
pub mod metrics;
pub mod sharding;
pub mod utils;

// Re-export commonly used types for convenience
pub use core::types::{Account, FeeLimits, Transaction};

// Re-export identity types
pub use core::crypto::{
    generate_identity, load_identity_from_path, load_or_generate_identity,
    peer_id_from_public_key_bytes, save_identity_to_path, validate_identity_keypair,
    IdentityKeypair,
};

// Re-export monolith types
pub use core::monolith::MonolithHeader;

pub use core::tx::{
    calculate_contract_address,
    create_deployment_transaction,
    create_transaction,
    deployment_to_signed_tx,
    estimate_tx_gas,
    execute_contract_call,
    execute_contract_constructor,
    generate_tx_hash,
    get_nonce,
    get_recipient,
    get_sender,
    // tx_equals_ignoring_sig, // Commented out - doesn't exist
    get_tx_fee,
    get_value,
    is_contract_deployment,
    is_pre_verified,
    mark_pre_verified,
    set_fee,
    set_nonce,
    signed_tx_to_deployment,
    validate_transaction_signature,
    CallResult,
    CallTransaction,
    ConstructorResult,
    DeployTransaction,
    Receipt,
    ReceiptEvent,
    SignedTx,
};

pub use core::block::Block;
// Re-enable genesis module exports
pub use core::genesis::{
    compute_genesis_hash, compute_tx_root, create_dev_genesis_block,
    create_genesis_from_transactions, ensure_genesis_block, export_genesis_to_json,
    genesis_block_exists, get_genesis_addresses, get_genesis_block_from_storage, get_genesis_hash,
    get_genesis_metadata, get_genesis_proposer, get_genesis_state_root, get_genesis_timestamp,
    get_genesis_transactions, get_genesis_tx_root, get_genesis_version, get_initial_supply,
    import_genesis_from_json, initialize_accounts_from_genesis, initialize_genesis_state,
    is_genesis_address, is_genesis_block, is_genesis_initialized, load_genesis_block,
    reset_genesis_block, sign_data, validate_genesis_block, validate_genesis_in_storage,
    verify_genesis_signature, verify_signature, GenesisMetadata,
};

// Re-enable utils module exports
pub use utils::{
    add_ms, add_seconds, array_to_vec, bps_to_percent, bytes_to_hex, bytes_to_hex_prefixed,
    bytes_to_u128_be, bytes_to_u128_le, bytes_to_u64_be, bytes_to_u64_le, duration_between,
    duration_between_ms, epoch, ether_to_wei, fixed_point, fixed_to_float, float_to_fixed,
    format_duration, format_duration_ms, format_duration_secs, hex_to_bytes, hex_to_bytes_prefixed,
    is_within_last_ms, is_within_last_seconds, ms_to_datetime, ms_to_timestamp, now_iso8601,
    now_timestamp, now_timestamp_ms, now_timestamp_ns, now_timestamp_us, parse_iso8601,
    percent_to_bps, perf, safe_int_convert, slice_to_array, slot, stats, str_to_u128, str_to_u64,
    subtract_ms, subtract_seconds, timestamp_to_ms, u128_to_bytes_be, u128_to_bytes_le,
    u128_to_str, u64_to_bytes_be, u64_to_bytes_le, u64_to_str, wei_to_ether,
};

// Re-export bincode utilities
pub use utils::{consensus_bincode, deserialize_consensus, serialize_consensus};

pub use metrics::{
    ExporterStats,
    HealthChecker,
    HealthStatus,
    LabelDefinition,
    ManifestGenerator,
    Metric,
    MetricCategory,
    MetricDefinition,
    MetricType,
    MetricsConfig,
    MetricsManifest,
    MetricsProvider,
    PlatformInfo,
    // Limited metrics exports - only working functions
    PrometheusExporter,
    PrometheusExporterConfig,
    ProviderStats,
    SecurityInfo,
    ThresholdDefinition,
    ThresholdType,
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

// Re-enable monolith module and implement Validate trait
impl Validate for MonolithHeader {
    fn validate(&self) -> CoreResult<()> {
        if self.window_start > self.exec_height {
            return Err(CoreError::monolith_error(
                "Window start cannot be greater than exec height",
            ));
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
            Err(CoreError::invalid_input(
                "String cannot be empty or whitespace",
            ))
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
    pub fn validate_bounds<T: PartialOrd + Copy + std::fmt::Display>(
        value: T,
        min: T,
        max: T,
    ) -> CoreResult<()> {
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
