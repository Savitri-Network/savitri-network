//! Governance & DAO: Sistema di governance decentralizzato
//!
//! - Creazione e gestione proposte
//! - Sistema di votazione con vote token
//! - Esecuzione di proposte approvate
//! - Gestione deposit e burn

pub mod deposit;
pub mod execution;
pub mod fl_proposals;
pub mod proposals;
pub mod vote_token;
pub mod voting;

pub use deposit::DepositManager;
pub use execution::ProposalExecutor;
pub use fl_proposals::{
    get_fl_policy, is_aggregator_whitelisted, is_fl_model_approved, is_fl_round_aborted,
    validate_abort_fl_round, validate_approve_fl_model, validate_fl_proposal_action,
    validate_set_fl_policy, validate_trainer_limits,
};
pub use proposals::Proposal;
pub use proposals::ProposalAction;
pub use proposals::ProposalStatus;
pub use vote_token::VoteToken;
pub use voting::VotingSystem;
