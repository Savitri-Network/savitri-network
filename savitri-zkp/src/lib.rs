//! Savitri-ZKP - Zero Knowledge Proof Implementation
//!
//! This crate provides zero-knowledge proof functionality for the Savitri Network blockchain.
//! It supports multiple ZKP backends including mock implementations for testing
//! and production-ready implementations using cryptographic libraries.

#![cfg_attr(docsrs, feature(doc_cfg))]
#![allow(missing_docs)]

pub mod monolith;
pub mod verifier;
pub mod zkp;

// Re-export main types
pub use verifier::{Statement, ZkProof, ZkVerifier};

#[cfg(test)]
mod tests;

/// ZKP backend selection
#[derive(Debug, Clone, Copy)]
pub enum ZkpBackend {
    /// Mock implementation for testing
    Mock,
    /// PLONK backend for production
    Plonk,
    /// Arkworks backend for advanced ZKP
    Arkworks,
}

/// ZKP configuration
#[derive(Debug, Clone)]
pub struct ZkpConfig {
    pub backend: ZkpBackend,
    pub max_proof_size: usize,
    pub verification_timeout_ms: u64,
}

impl Default for ZkpConfig {
    fn default() -> Self {
        Self {
            backend: ZkpBackend::Mock,     // Keep Mock as default for testing
            max_proof_size: 1024 * 1024,   // 1MB
            verification_timeout_ms: 5000, // 5 seconds
        }
    }
}

/// ZKP configuration for production environments
impl ZkpConfig {
    /// Create production configuration with Arkworks backend
    pub fn production() -> Self {
        Self {
            backend: ZkpBackend::Arkworks,
            max_proof_size: 4 * 1024 * 1024, // 4MB for complex proofs
            verification_timeout_ms: 15000,  // 15 seconds
        }
    }

    /// Create development configuration (Mock backend)
    pub fn development() -> Self {
        Self::default() // Mock backend
    }

    pub fn testing() -> Self {
        Self {
            backend: ZkpBackend::Mock,
            max_proof_size: 512 * 1024,    // 512KB for testing
            verification_timeout_ms: 1000, // 1 second for fast testing
        }
    }
}

/// Create a ZKP verifier based on configuration.
///
/// SECURITY: When a production backend (Plonk/Arkworks) is requested but
/// its feature flag is not enabled, this function panics instead of silently
/// falling back to MockVerifier. This prevents accidentally running
/// production nodes with mock proof verification.
pub fn create_verifier(config: ZkpConfig) -> Box<dyn ZkVerifier> {
    match config.backend {
        ZkpBackend::Mock => {
            tracing::warn!(
                "ZKP MockVerifier active — accepts ALL proofs. \
                 NOT safe for production. Use Plonk or Arkworks backend."
            );
            Box::new(crate::verifier::MockVerifier::default())
        }
        ZkpBackend::Plonk => {
            #[cfg(feature = "plonk")]
            {
                Box::new(crate::verifier::PlonkVerifier::new(config))
            }
            #[cfg(not(feature = "plonk"))]
            {
                panic!(
                    "ZKP backend 'Plonk' requested but the 'plonk' feature is not enabled. \
                     Compile with `--features plonk` or switch to ZkpBackend::Mock for testing. \
                     Refusing to silently fall back to MockVerifier."
                );
            }
        }
        ZkpBackend::Arkworks => {
            #[cfg(feature = "arkworks")]
            {
                Box::new(crate::verifier::ArkworksVerifier::new(config))
            }
            #[cfg(not(feature = "arkworks"))]
            {
                panic!(
                    "ZKP backend 'Arkworks' requested but the 'arkworks' feature is not enabled. \
                     Compile with `--features arkworks` or switch to ZkpBackend::Mock for testing. \
                     Refusing to silently fall back to MockVerifier."
                );
            }
        }
    }
}
