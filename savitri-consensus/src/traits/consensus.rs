//! Main consensus engine trait
//!
//! This trait defines the core interface that all consensus implementations
//! must provide, ensuring a unified API across different node types.

use crate::error::Result;
use crate::types::*;
use crate::ProposerInfo;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Main consensus engine trait
///
/// This trait provides the fundamental interface for consensus operations,
///
/// # Type Parameters
///
/// * `Config` - Configuration type for the consensus engine
/// * `Proposal` - Type of block proposals used by this consensus
/// * `Storage` - Storage backend type
///
/// # Examples
///
/// ```ignore
/// use savitri_consensus::traits::ConsensusEngine;
/// use savitri_consensus::protocols::GroupAwareConsensus;
///
/// let mut consensus = GroupAwareConsensus::new(config, storage)?;
/// let proposer = consensus.get_proposer(current_slot).await?;
///
/// if let Some(proposer_info) = proposer {
///     println!("Current proposer: {:?}", proposer_info.node_id);
/// }
/// ```
pub trait ConsensusEngine: Send + Sync {
    /// Configuration type for this consensus engine
    type Config: Clone + Send + Sync + 'static;

    /// Proposal type used by this consensus engine
    type Proposal: Proposal + Clone + Send + Sync + 'static;

    /// Validation result type
    type Validation: Validation + Clone + Send + Sync + 'static;

    /// Storage backend type
    type Storage: crate::traits::Storage + Send + Sync + 'static + ?Sized;

    /// Initialize the consensus engine
    ///
    /// # Arguments
    ///
    /// * `config` - Configuration for the consensus engine
    /// * `storage` - Shared storage backend
    ///
    /// # Returns
    ///
    /// Returns the initialized consensus engine
    ///
    /// # Errors
    ///
    /// Returns an error if initialization fails
    fn new(config: Self::Config, storage: Arc<Self::Storage>) -> Result<Self>
    where
        Self: Sized;

    /// Get consensus state asynchronously
    async fn get_state(&self) -> crate::error::Result<ConsensusState>;

    /// Get mutable consensus state asynchronously
    async fn get_state_mut(&mut self) -> crate::error::Result<ConsensusState>;

    /// Get consensus state (legacy sync method for compatibility)
    fn state(&self) -> &ConsensusState {
        // This method is deprecated - use get_state() instead
        static DEFAULT_STATE: std::sync::OnceLock<ConsensusState> = std::sync::OnceLock::new();
        DEFAULT_STATE.get_or_init(ConsensusState::default)
    }

    /// Get mutable consensus state (legacy sync method for compatibility).
    ///
    /// # Deprecated
    ///
    /// Use `get_state_mut()` for async contexts. This method exists only for
    /// backward compatibility and returns a thread-local default state.
    ///
    /// **WARNING**: The returned reference is NOT connected to the engine's
    /// actual consensus state. Implementations should override this method
    /// with their own storage, or migrate to `get_state_mut()`.
    #[deprecated(note = "Use get_state_mut() for async contexts")]
    fn state_mut(&mut self) -> &mut ConsensusState {
        // SAFETY: thread_local! guarantees single-threaded access per thread.
        // &mut self prevents re-entrant calls on the same instance.
        // Do NOT hold this reference across .await points.
        thread_local! {
            static MUTABLE_STATE: std::cell::RefCell<ConsensusState> =
                std::cell::RefCell::new(ConsensusState::default());
        }

        // SECURITY (HIGH-05): Added re-entrancy guard before taking the raw pointer.
        // The original code used `as_ptr()` directly, which bypassed RefCell's borrow
        // tracking entirely — a double-borrow would silently produce two aliasing &mut
        // references (undefined behaviour). The fix adds `borrow_mut()` first: it
        // panics if a borrow is already active, ensuring exclusivity before we proceed.
        //
        // SAFETY invariants (unchanged from original):
        // 1. thread_local! guarantees single-thread access per thread.
        // 2. &mut self is an exclusive borrow — no re-entrant call to state_mut() is
        //    possible while the returned &mut reference is held by the caller.
        // 3. The RefCell borrow-check below adds a runtime panic for any violation.
        MUTABLE_STATE.with(|cell| {
            // Panics on re-entrancy (double-borrow), replacing silent UB with an
            // explicit error. Immediately dropped so as_ptr() gets an unborrowed cell.
            let _guard = cell.borrow_mut();
            drop(_guard);
            let ptr = cell.as_ptr();
            // SAFETY: exclusivity verified above; thread_local + &mut self prevent
            // any concurrent or re-entrant access.
            unsafe { &mut *ptr }
        })
    }

    /// Process a consensus message asynchronously
    ///
    /// # Arguments
    ///
    /// * `message` - The consensus message to process
    ///
    /// # Returns
    ///
    /// Returns the response to the message
    ///
    /// # Errors
    ///
    /// Returns an error if message processing fails
    async fn process_message_async(
        &mut self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse>;

    /// Process a consensus message (legacy sync method for compatibility)
    fn process_message(
        &mut self,
        message: ConsensusMessage,
    ) -> crate::error::Result<ConsensusResponse> {
        use tokio::runtime::Handle;
        let rt = Handle::current();
        rt.block_on(self.process_message_async(message))
    }

    /// Validate a proposal asynchronously
    ///
    /// # Arguments
    ///
    ///
    /// # Returns
    ///
    ///
    /// # Errors
    ///
    async fn validate_proposal_async(
        &self,
        proposal: &Self::Proposal,
    ) -> crate::error::Result<Self::Validation>;

    /// Validate a proposal (legacy sync method for compatibility)
    fn validate_proposal(
        &self,
        proposal: &Self::Proposal,
    ) -> crate::error::Result<Self::Validation> {
        use tokio::runtime::Handle;
        let rt = Handle::current();
        rt.block_on(self.validate_proposal_async(proposal))
    }

    /// Get current proposer for given slot asynchronously
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot number to get the proposer for
    ///
    /// # Returns
    ///
    /// Returns the proposer information if available
    ///
    /// # Errors
    ///
    /// Returns an error if proposer selection fails
    async fn get_proposer_async(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>>;

    /// Get current proposer for given slot (legacy sync method for compatibility)
    fn get_proposer(&self, slot: u64) -> crate::error::Result<Option<ProposerInfo>> {
        use tokio::runtime::Handle;
        let rt = Handle::current();
        rt.block_on(self.get_proposer_async(slot))
    }

    /// Create a new proposal asynchronously (if this node is proposer)
    ///
    /// # Arguments
    ///
    /// * `slot` - The slot for which to create a proposal
    ///
    /// # Returns
    ///
    /// Returns a new proposal if this node is the proposer, None otherwise
    ///
    /// # Errors
    ///
    /// Returns an error if proposal creation fails
    async fn create_proposal_async(
        &self,
        slot: u64,
    ) -> crate::error::Result<Option<Self::Proposal>>;

    /// Create a new proposal (legacy sync method for compatibility)
    fn create_proposal(&self, slot: u64) -> crate::error::Result<Option<Self::Proposal>> {
        use tokio::runtime::Handle;
        let rt = Handle::current();
        rt.block_on(self.create_proposal_async(slot))
    }

    /// Get consensus statistics
    ///
    /// # Returns
    ///
    /// Returns current consensus statistics
    fn stats(&self) -> ConsensusStats;

    /// Get configuration
    ///
    /// # Returns
    ///
    /// Returns the current configuration
    fn config(&self) -> &Self::Config;

    /// Get storage backend
    ///
    /// # Returns
    ///
    /// Returns a reference to the storage backend
    fn storage(&self) -> &Arc<Self::Storage>;

    /// Check if the consensus engine is healthy
    ///
    /// # Returns
    ///
    /// Returns true if the engine is healthy, false otherwise
    fn is_healthy(&self) -> bool {
        let stats = self.stats();
        stats.failed_validations < stats.total_validations / 10 && // Less than 10% failure rate
        stats.average_validation_time_ms < 1000.0 // Under 1 second average validation
    }

    /// Reset consensus statistics
    fn reset_stats(&mut self);

    /// Get supported consensus version
    ///
    /// # Returns
    ///
    /// Returns the supported consensus version
    fn supported_version(&self) -> ConsensusVersion;

    /// Check compatibility with another version
    ///
    /// # Arguments
    ///
    /// * `other_version` - The version to check compatibility with
    ///
    /// # Returns
    ///
    /// Returns true if compatible, false otherwise
    fn is_compatible_with(&self, other_version: &ConsensusVersion) -> bool {
        self.supported_version().is_compatible(other_version)
    }
}
