//! Oracle Framework: Sistema per feed di dati esterni con proof firmate
//!
//! - Feed tipizzati e versionati (feed_id, schema_id/version)
//! - Proof MVP: ed25519 signature con domain separation + anti-replay (sequence/nonce)
//! - TTL e timestamping: rifiuta dati scaduti o nel futuro oltre tolleranza configurabile
//! - Registry/ACL: writer/reader/auditor governabili
//! - Schema registry: definire schema per feed types comuni

pub mod feed;
pub mod integration;
pub mod proof;
pub mod schema;
pub mod types;

pub use feed::{Feed, FeedData, FeedId};
pub use integration::{is_oracle_feed_tx, OracleFeedTx, OracleValidationResult, OracleValidator};
pub use proof::{OracleProof, ProofVerifier};
pub use schema::{Schema, SchemaId, SchemaRegistry, SchemaType};
pub use types::{OracleConfig, OracleError, OracleRole};
