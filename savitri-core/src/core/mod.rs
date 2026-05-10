//! Core functionality for Savitri Network

pub mod block;
pub mod config;
pub mod crypto;
pub mod executor;
pub mod executor_callbacks;
pub mod executor_dag_metrics;
pub mod genesis;
pub mod identity;
pub mod monolith;
#[cfg(feature = "shared-types")]
pub mod p2p_messages;
pub mod receipt;
#[cfg(feature = "shared-types")]
pub mod shared_types;
pub mod slot_scheduler;
pub mod tx;
pub mod types;
pub mod unified_slot;
pub mod validate;

// Re-export crypto functions
// Re-enable genesis functions
pub use crypto::generate_keypair;
pub use genesis::{compute_tx_root, sign_data, verify_signature};

// Re-export monolith functions for backward compatibility
pub use monolith::{
    add_cosignatures,
    compute_monolith_id,
    create_genesis_monolith,
    create_test_monolith,
    default_monolith_policy,
    export_monolith_to_json,
    filter_monoliths_by_epoch,
    filter_monoliths_by_producer,
    find_monolith_by_height,
    format_monolith_age,
    generate_monolith,
    generate_proof_bytes,
    generate_proof_commit,
    get_coverage_percentage,
    get_formatted_age,
    get_monolith_chain_coverage,
    get_monolith_chain_gaps,
    get_monolith_chain_height,
    get_monolith_stats,
    get_monoliths_by_age,
    get_monoliths_in_range,
    get_monoliths_needing_cosignatures,
    has_chain_gaps,
    headers_commit_from_hashes,
    import_monolith_from_json,
    is_stale,
    monolith_from_summary,
    needs_cosignatures,
    sort_monoliths_by_age,
    sort_monoliths_by_height,
    validate_monolith_chain,
    validate_monolith_policy,
    // headers_commit, // Commentato temporaneamente
    verify_headers_commit,
    verify_monolith_proof,
    MonolithHeader,
    MonolithPolicy,
    MonolithStats,
};
