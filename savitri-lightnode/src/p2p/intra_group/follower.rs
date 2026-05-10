//! Follower-side logic for intra-group consensus.
//! participating in BFT consensus voting, and querying group/epoch state.

use anyhow::Result;
use tracing::{info, warn, debug, error};

use super::*;

impl IntraGroupCommunication {
    /// Start following the elected proposer
    async fn start_following_proposer(&mut self, proposer_id: &str) -> Result<()> {
        let new_proposer_is_us = proposer_id == self.local_node_id;

        // Stop existing proposer block production loop if we were previously the proposer.
        // Guard: don't interrupt an active proposer that has not yet used its full tenure.
        // This prevents election ACKs for the next epoch from killing a running block loop
        // that has only produced a few blocks.
        if let Some(ref proposer_state) = self.proposer_state {
            let (is_active, blocks_done) = {
                let s = proposer_state.read().await;
                (s.is_active, s.block_proposal_count)
            };
            if new_proposer_is_us && is_active && blocks_done < PROPOSER_TENURE_BLOCKS {
                info!(
                    blocks_proposed = blocks_done,
                    tenure = PROPOSER_TENURE_BLOCKS,
                    next_proposer = %proposer_id,
                    "Mid-tenure guard: ignoring election handoff until tenure is complete"
                );
                return Ok(());
            }
            let mut state = proposer_state.write().await;
            state.is_active = false;
            info!("Stopped proposer block production loop (now following {})", proposer_id);
        }
        self.proposer_state = None;
        // Reset block_loop_running so a future re-election can start a new block loop.
        // The spawned loop will also reset it on exit, but that is async — reset now to
        // avoid a window where compare_exchange fails in start_proposer_duties.
        self.block_loop_running.store(false, AtomicOrdering::SeqCst);

        if let Some(ref flag) = self.is_intragroup_proposer {
            *flag.write().await = false;
        }
        info!("Starting to follow proposer: {}", proposer_id);

        // Initialize follower state
        let follower_state = Arc::new(RwLock::new(FollowerState {
            current_proposer: proposer_id.to_string(),
            last_seen_block: 0,
            is_active: true,
            blocks_received: 0,
            proposals_validated: 0,
        }));
        self.follower_state = Some(follower_state.clone());

        // Start listening for blocks from proposer
        let follower_state_clone = follower_state.clone();
        let intra_group_comm_clone = self.clone();
        tokio::spawn(async move {
            let mut block_listener_interval = tokio::time::interval(Duration::from_secs(1)); // Check every second
            loop {
                block_listener_interval.tick().await;

                let state = follower_state_clone.write().await;
                if state.is_active {
                    if let Err(e) = intra_group_comm_clone.listen_for_blocks_from_proposer(&state.current_proposer).await {
                        error!("Failed to listen for blocks from proposer: {}", e);
                    }
                }
            }
        });

        let follower_state_clone = follower_state.clone();
        let intra_group_comm_clone = self.clone();
        tokio::spawn(async move {
            let mut validation_interval = tokio::time::interval(Duration::from_secs(3)); // Validate every 3 seconds
            loop {
                validation_interval.tick().await;

                let state = follower_state_clone.write().await;
                if state.is_active {
                    if let Err(e) = intra_group_comm_clone.validate_proposer_proposals(&state.current_proposer).await {
                        error!("Failed to validate proposer proposals: {}", e);
                    }
                }
            }
        });

        // Start consensus participation
        let intra_group_comm_clone = self.clone();
        tokio::spawn(async move {
            let mut consensus_interval = tokio::time::interval(Duration::from_secs(5)); // Participate every 5 seconds
            loop {
                consensus_interval.tick().await;

                if let Err(e) = intra_group_comm_clone.participate_in_consensus().await {
                    error!("Failed to participate in consensus: {}", e);
                }
            }
        });

        debug!("Follower duties initiated - block listening, validation, and consensus participation started");
        Ok(())
    }

    /// Listen for blocks from proposer - processes real proposals from received_proposals queue
    async fn listen_for_blocks_from_proposer(&self, proposer_id: &str) -> Result<()> {
        // Process received proposals (from intra-group gossip)
        while let Some((round_id, pid, proposal)) = self.take_next_proposal().await {
            if pid != proposer_id {
                continue; // Skip proposals from other proposers
            }
            match self.validate_block_proposal(&proposal).await {
                Ok(()) => {
                    if let Some(ref follower_state) = self.follower_state {
                        let mut state = follower_state.write().await;
                        state.blocks_received += 1;
                        state.last_seen_block = proposal.height;
                    }
                    info!(
                        round = round_id,
                        height = proposal.height,
                        proposer = %proposer_id,
                        "Valid block proposal received and validated"
                    );
                }
                Err(e) => {
                    warn!(
                        round = round_id,
                        height = proposal.height,
                        error = %e,
                        "Block proposal validation failed"
                    );
                }
            }
        }
        Ok(())
    }

    async fn validate_block_proposal(&self, proposal: &crate::proposer::BlockProposal) -> Result<()> {
        // 1. Verify proposer signature
        let signable = crate::proposer::proposal_signable_bytes(proposal);
        let pk = ed25519_dalek::VerifyingKey::from_bytes(&proposal.proposer_pubkey)
            .map_err(|e| anyhow::anyhow!("Invalid proposer pubkey: {}", e))?;
        let sig = ed25519_dalek::Signature::from_bytes(&proposal.signature);
        pk.verify_strict(&signable, &sig)
            .map_err(|e| anyhow::anyhow!("Proposal signature invalid: {}", e))?;

        // 2. Verify parent_hash matches local chain
        let local_parent = self.get_parent_hash().await;
        if proposal.parent_hash != local_parent {
            return Err(anyhow::anyhow!(
                "Parent hash mismatch: expected {}",
                hex::encode(local_parent)
            ));
        }

        // 2b. Merge policy: stale frontier tips must be referenced so branches are not left behind.
        if let Some(ref dag) = self.dag {
            let frontier = dag.get_frontier_tips().await;
            let max_frontier_height = frontier.iter().map(|tip| tip.height).max().unwrap_or(0);
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            const MAX_TIP_LAG_BLOCKS: u64 = 5;
            const MAX_TIP_AGE_SECS: u64 = 120;

            // Build canonical parent set from proposal (primary + additional)
            let mut canonical_parents: Vec<[u8; 64]> = vec![proposal.parent_hash];
            for hash in &proposal.parent_hashes {
                let mut normalized = [0u8; 64];
                let len = hash.len().min(64);
                normalized[..len].copy_from_slice(&hash[..len]);
                if !canonical_parents.contains(&normalized) {
                    canonical_parents.push(normalized);
                }
            }

            for tip in frontier {
                if tip.hash == proposal.parent_hash {
                    continue;
                }
                let lagged = max_frontier_height.saturating_sub(tip.height) > MAX_TIP_LAG_BLOCKS;
                let old = tip.timestamp > 0 && now.saturating_sub(tip.timestamp) > MAX_TIP_AGE_SECS;
                if (lagged || old) && !canonical_parents.contains(&tip.hash) {
                    return Err(anyhow::anyhow!(
                        "Missing stale frontier parent {} (height={}, lag={}, age_secs={})",
                        hex::encode(tip.hash),
                        tip.height,
                        max_frontier_height.saturating_sub(tip.height),
                        now.saturating_sub(tip.timestamp)
                    ));
                }
            }
        }

        // 3. Verify height is valid (next expected)
        let current = self.get_current_block_height().await;
        if proposal.height != current + 1 {
            return Err(anyhow::anyhow!(
                "Invalid height: got {} expected {}",
                proposal.height,
                current + 1
            ));
        }

        // 4. Verify timestamp is within acceptable range (5 min skew)
        let now = get_safe_timestamp();
        if proposal.timestamp > now + 300 {
            return Err(anyhow::anyhow!("Proposal timestamp too far in future"));
        }

        // 5. Verify state_root and tx_root via overlay execution
        if !proposal.transactions.is_empty() {
            if let Some(ref storage) = self.storage {
                // Convert ProposalTransaction → SignedTx for overlay execution
                // SECURITY: Attempt real signature verification instead of hardcoding pre_verified: true.
                // In this system, ptx.from IS the raw 32-byte Ed25519 public key (addresses are
                // hex-encoded public keys), so we can use it as the pubkey for verification.
                // Note: apply_certified_block_direct does NOT check pre_verified — it only
                let signed_txs: Vec<crate::tx::SignedTx> = proposal.transactions.iter().map(|ptx| {
                    let mut stx = crate::tx::SignedTx {
                        from: hex::encode(ptx.from),
                        to: hex::encode(ptx.to),
                        amount: ptx.amount,
                        nonce: ptx.nonce,
                        fee: Some(ptx.fee as u128),
                        data: if ptx.data.is_empty() { None } else { Some(ptx.data.clone()) },
                        sig: ptx.signature,
                        pubkey: ptx.from.to_vec(), // ptx.from is the raw 32-byte Ed25519 public key
                        pre_verified: false,        // will be set by verification below
                    };
                    stx.pre_verified = crate::tx::verify_transaction_signature_ext(&stx);
                    if !stx.pre_verified {
                        warn!(
                            from = %stx.from.chars().take(16).collect::<String>(),
                            nonce = stx.nonce,
                            "Signature verification failed for TX in MN-certified proposal \
                             (will still apply via certified block path)"
                        );
                    }
                    stx
                }).collect();

                let temp_block = crate::tx::Block {
                    height: proposal.height,
                    parent_hash: proposal.parent_hash,
                    ..Default::default()
                };
                match crate::p2p::block::apply_certified_block_direct(
                    storage.as_ref(), &temp_block, &signed_txs,
                ) {
                    Ok((overlay, receipts)) => {
                        let sr32 = crate::p2p::block::compute_state_root_from_overlay(&overlay);
                        let tr32 = crate::p2p::block::compute_tx_root_from_receipts(&receipts);
                        let mut expected_sr = [0u8; 64]; expected_sr[..32].copy_from_slice(&sr32);
                        let mut expected_tr = [0u8; 64]; expected_tr[..32].copy_from_slice(&tr32);
                        if proposal.state_root != expected_sr {
                            return Err(anyhow::anyhow!("State root mismatch in proposal verification"));
                        }
                        if proposal.tx_root != expected_tr {
                            return Err(anyhow::anyhow!("Transaction root mismatch in proposal verification"));
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Could not verify roots via overlay, skipping root check");
                    }
                }
            } else {
                warn!("No storage for proposal root verification, skipping root check");
            }
        }

        // 6. Basic PoU check (proposer should have sufficient score)
        if proposal.proposer_pou_score < 3000 {
            return Err(anyhow::anyhow!(
                "Insufficient PoU score: {}",
                proposal.proposer_pou_score
            ));
        }

        Ok(())
    }

    /// Validate proposer proposals - process any pending proposals from received queue
    async fn validate_proposer_proposals(&self, proposer_id: &str) -> Result<()> {
        let mut validated = 0u64;
        while let Some((_round_id, pid, proposal)) = self.take_next_proposal().await {
            if pid != proposer_id {
                continue;
            }
            if self.validate_block_proposal(&proposal).await.is_ok() {
                validated += 1;
                if let Some(ref follower_state) = self.follower_state {
                    let mut state = follower_state.write().await;
                    state.proposals_validated += 1;
                }
            }
        }
        if validated > 0 {
            info!("Validated {} proposals from {}", validated, proposer_id);
        }
        Ok(())
    }

    /// Get proposer's PoU score
    async fn get_proposer_pou_score(&self, proposer_id: &str) -> u32 {
        // Check if we have the proposer's PoU score
        let pou_scores = self.member_pou_scores.read().await;

        if let Some(&(score, _)) = pou_scores.get(proposer_id) {
            score
        } else {
            // If we don't have the score, estimate based on network position
            // In real implementation, this would query the PoU scoring service
            let estimated_score = 5000 + (rand::random::<u32>() % 3000); // 50-80% range
            estimated_score
        }
    }

    /// Participate in consensus
    async fn participate_in_consensus(&self) -> Result<()> {
        debug!("Participating in consensus");

        // In real implementation, this would:
        // 1. Check if we should vote on current proposals
        // 2. Validate proposals from current proposer
        //  3. Cast votes on valid proposals
        // 4. Participate in BFT consensus
        // 5. Handle view changes if needed

        // Get current proposer
        let current_proposer = if let Some(ref follower_state) = self.follower_state {
            let state = follower_state.read().await;
            Some(state.current_proposer.clone())
        } else {
            None
        };

        if let Some(proposer_id) = current_proposer {
            // Simulate consensus participation
            let should_vote = rand::random::<f64>() < 0.8; // 80% participation rate

            if should_vote {
                // Create a vote for the current round
                match self.create_consensus_vote(&proposer_id).await {
                    Ok(vote) => {
                        // Submit vote to consensus
                        if let Err(e) = self.submit_consensus_vote(&vote).await {
                            error!("Failed to submit consensus vote: {}", e);
                        } else {
                            debug!("Submitted consensus vote for round {}", vote.round);
                        }
                    }
                    Err(e) => {
                        error!("Failed to create consensus vote: {}", e);
                    }
                }
            }
        }

        debug!("Consensus participation completed");
        Ok(())
    }

    /// Create consensus vote
    async fn create_consensus_vote(&self, proposer_id: &str) -> Result<ConsensusVote> {
        let current_round = if let Some(ref proposer_state) = self.proposer_state {
            let state = proposer_state.read().await;
            state.current_round
        } else {
            0
        };
        let mut vote = ConsensusVote {
            voter: self.local_node_id.clone(),
            proposer: proposer_id.to_string(),
            round: current_round,
            vote_type: VoteType::Approve, // Default to approve
            timestamp: get_safe_timestamp(),
            group_id: self.get_current_group_id().await,
            signature: [0u8; 64],
        };
        let signable = vote.signable_bytes()?;
        vote.signature = self.sign_intragroup_message(&vote.group_id, "consensus_vote", &signable);
        Ok(vote)
    }

    /// Submit consensus vote to group members via direct P2P (or gossipsub fallback)
    async fn submit_consensus_vote(&self, vote: &ConsensusVote) -> Result<()> {
        let payload = serde_json::to_vec(vote)?;
        self.broadcast_consensus(
            ConsensusMessage::Vote(payload.clone()),
            self.vote_topic.clone(),
            payload,
        ).await?;
        debug!(round = vote.round, group_id = %vote.group_id, "Consensus vote sent via direct P2P");
        Ok(())
    }

    /// Get current group ID
    async fn get_current_group_id(&self) -> String {
        self.group_manager.get_current_group()
            .await
            .map(|g| g.group_id)
            .unwrap_or_else(|| "unknown".to_string())
    }

    /// Get current epoch
    async fn get_current_epoch(&self) -> u64 {
        self.group_manager.get_current_group()
            .await
            .map(|g| g.epoch)
            .unwrap_or(0)
    }
}
