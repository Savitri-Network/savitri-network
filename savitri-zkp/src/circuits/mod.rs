//! ZKP circuit definitions for Savitri Network.
//!
//! Provides algebraic circuits for monolith integrity proofs.
//! The initial circuit proves w1 + w2 + w3 = public_sum where
//! w1, w2, w3 are field elements derived from monolith header fields.

#[cfg(feature = "arkworks")]
pub mod monolith_circuit;

#[cfg(feature = "plonk")]
pub mod plonk_circuit;
