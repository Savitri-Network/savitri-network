//! Cell aggregator — collects raw cells and attestations from gossip,
//! verifies signatures, emits `CellCertificate`s once BFT quorum is met.
//!
//! Part of Savitri V0.2 Phase 2 (Lattice ordering). This module is the
//! receive-side runtime that turns the wire-format primitives in
//! `crate::types::lattice` into a usable DAG state machine.
//!
//! ## Ingress paths
//!
//! Two gossip topics feed the aggregator:
//!
//! - `/savitri/group/<gid>/lattice/cell/1` — raw cells from group
//!   members. `observe_cell` validates the author signature, stores the
//!   cell as pending, and returns to the caller (the caller is expected
//!   to publish their own [`CellAttestation`] in response).
//! - `/savitri/group/<gid>/lattice/attestation/1` — attestations.
//!   `observe_attestation` validates the signer signature, deduplicates
//!   against per-cell signer set, and returns an [`AttestationOutcome`]
//!   indicating whether the cell now meets the BFT quorum.
//!
//! ## State
//!
//! Cells live in two stages:
//!
//! 1. **Pending** — verified by author but not yet certified.
//!    `pending[cell_id] -> (cell, attestations_so_far)`.
//! 2. **Certified** — `>= quorum` distinct signers attested.
//!    `certified[(round, author)] -> CellCertificate`.
//!
//! Pending cells age out per `AggregatorConfig::retention_rounds` to
//! cap memory under pathological gossip.
//!
//! ## Determinism
//!
//! The aggregator does NOT decide ordering — that is the lineage commit
//! walker's job (see [`crate::lattice::commit`]). The aggregator's only
//! contract is: given the same sequence of observed (cell, attestation)
//! pairs, two observers produce the same set of `CellCertificate`s for
//! the same `(round, author)` pairs.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::types::lattice::{
    lattice_quorum, CellAttestation, CellCertificate, CellId, LatticeCell, LatticeRound,
};

/// Configuration knobs for the aggregator. Hot-path defaults are tuned
/// for typical Savitri group sizes (5–10 LNs) and gossipsub latency
/// profiles observed on the testnet.
#[derive(Clone, Debug)]
pub struct AggregatorConfig {
    /// Group size used to derive the BFT quorum threshold via
    /// `lattice_quorum`. Caller is responsible for keeping this in
    /// sync with the actual group membership.
    pub group_size: usize,
    /// Pending cells older than `current_round - retention_rounds` are
    /// garbage-collected. 64 rounds is roughly 64 seconds at a 1s
    /// round duration — comfortably wider than gossip propagation
    /// while bounding the working set.
    pub retention_rounds: u64,
    /// Per-cell cap on attestations stored. Bounds memory if a
    /// malicious peer floods attestations for the same cell. Default
    /// 256 = enough for any realistic group size + a generous
    /// duplicate margin (deduplicated by signer below, so the cap is
    /// mostly a defense against pathological churn).
    pub max_attestations_per_cell: usize,
}

impl Default for AggregatorConfig {
    fn default() -> Self {
        Self {
            group_size: 5,
            retention_rounds: 64,
            max_attestations_per_cell: 256,
        }
    }
}

/// Errors surfaced by the aggregator. All are non-fatal — the receive
/// loop logs and continues.
#[derive(Debug, thiserror::Error)]
pub enum AggregatorError {
    #[error("cell author signature did not verify")]
    BadCellSignature,
    #[error("attestation signature did not verify against signer pubkey")]
    BadAttestationSignature,
    #[error("attestation references unknown cell {0}")]
    UnknownCell(String),
    #[error("attestation cap reached for cell — dropping")]
    AttestationCapReached,
}

/// What [`LatticeAggregator::observe_attestation`] returns to the caller.
///
/// Note: this enum is intentionally not `Eq + PartialEq` — the
/// `Rejected` variant contains [`AggregatorError`] which derives
/// `thiserror::Error` and is not naturally comparable. Tests should
/// use `matches!` to inspect the variant.
#[derive(Debug)]
pub enum AttestationOutcome {
    /// Attestation accepted but the cell has not yet reached quorum.
    /// The aggregator stored it in the pending bucket.
    Pending {
        /// Distinct signer count after this insertion.
        signer_count: usize,
        /// Quorum threshold (informational for the caller).
        quorum: usize,
    },
    /// Attestation accepted and pushed the cell over the quorum
    /// threshold. The aggregator promoted the cell to the certified
    /// table. The returned `CellCertificate` is a clone — the caller
    /// is free to gossip it forward to peers / persist it.
    Certified(CellCertificate),
    /// Attestation accepted but the cell was already certified.
    /// Late attestations are kept up to `max_attestations_per_cell`
    /// (useful for BLS aggregation later); no new cert is emitted.
    AlreadyCertified,
    /// Attestation rejected for one of the reasons in
    /// [`AggregatorError`]. The caller decides whether to log /
    /// counter-meter / slash.
    Rejected(AggregatorError),
}

/// Aggregator state. Cheap to clone (everything behind `Arc` if the
/// caller needs sharing — current implementation is single-owner
/// behind a `tokio::sync::RwLock` upstream).
pub struct LatticeAggregator {
    config: AggregatorConfig,
    /// Cells observed by author but not yet meeting quorum.
    pending: HashMap<CellId, PendingCell>,
    /// Cells that crossed the quorum threshold.
    /// Keyed by `(round, author)` for fast lookup by the lineage
    /// commit walker — `BTreeMap` keeps a stable iteration order
    /// (round-major, author-lex) which matches the deterministic
    /// commit ordering convention.
    certified: BTreeMap<(LatticeRound, String), CellCertificate>,
    /// The maximum round we have observed. Used for retention-window
    /// garbage collection.
    high_water_round: LatticeRound,
}

struct PendingCell {
    cell: LatticeCell,
    /// Signer peer_id -> attestation. HashMap dedups by signer.
    attestations: HashMap<String, CellAttestation>,
}

impl LatticeAggregator {
    /// Construct an empty aggregator with the given configuration.
    pub fn new(config: AggregatorConfig) -> Self {
        Self {
            config,
            pending: HashMap::new(),
            certified: BTreeMap::new(),
            high_water_round: 0,
        }
    }

    /// Update the group size (and therefore the quorum threshold).
    /// Caller invokes this after a group rotation. Has no effect on
    /// already-certified cells — they remain valid under the old
    /// threshold by which they were issued.
    pub fn set_group_size(&mut self, group_size: usize) {
        self.config.group_size = group_size;
    }

    /// Current BFT quorum threshold.
    #[inline]
    pub fn quorum(&self) -> usize {
        lattice_quorum(self.config.group_size)
    }

    /// Observe a raw cell from gossip. The caller is responsible for
    /// matching `cell.group_id` against the local group BEFORE calling
    /// this — the aggregator does not enforce group membership.
    ///
    /// Returns `Ok(cell_id)` on accepted cell (stored as pending);
    /// `Err(BadCellSignature)` if the author signature fails. Cells
    /// already pending or certified are silently no-op (the caller
    /// may safely re-observe duplicates).
    pub fn observe_cell(&mut self, cell: LatticeCell) -> Result<CellId, AggregatorError> {
        if !cell.verify_author_signature() {
            return Err(AggregatorError::BadCellSignature);
        }
        let cell_id = cell.cell_id();
        if self.high_water_round < cell.round {
            self.high_water_round = cell.round;
        }
        // Already certified — silently ignore.
        if self
            .certified
            .contains_key(&(cell.round, cell.author.clone()))
        {
            return Ok(cell_id);
        }
        // Already pending — silently keep the existing entry (a re-
        // observation should not lose accumulated attestations).
        self.pending.entry(cell_id).or_insert_with(|| PendingCell {
            cell,
            attestations: HashMap::new(),
        });
        Ok(cell_id)
    }

    /// Observe an attestation. Returns an outcome the caller uses to
    /// drive downstream behaviour (broadcast the new cert / log /
    /// counter).
    ///
    /// The attestation MUST carry the `cell_id` it references; the
    /// caller is expected to wire that into the attestation gossip
    /// envelope. Here we pass it as an explicit argument to keep this
    /// module independent of the wire envelope shape.
    pub fn observe_attestation(
        &mut self,
        cell_id: CellId,
        att: CellAttestation,
    ) -> AttestationOutcome {
        // Verify the signature against the cell's signable bytes.
        // We need the cell to know what was signed.
        let pending = match self.pending.get_mut(&cell_id) {
            Some(p) => p,
            None => {
                // Maybe already certified.
                let key = self
                    .certified
                    .iter()
                    .find(|(_, c)| c.cell_id() == cell_id)
                    .map(|(k, _)| k.clone());
                if let Some(_k) = key {
                    return AttestationOutcome::AlreadyCertified;
                }
                return AttestationOutcome::Rejected(AggregatorError::UnknownCell(hex::encode(
                    cell_id,
                )));
            }
        };

        // Verify attestation signature against the cell's signable_bytes.
        if !verify_attestation_against_cell(&att, &pending.cell) {
            return AttestationOutcome::Rejected(AggregatorError::BadAttestationSignature);
        }

        // Cap check (defense against malicious flood).
        if pending.attestations.len() >= self.config.max_attestations_per_cell
            && !pending.attestations.contains_key(&att.signer)
        {
            return AttestationOutcome::Rejected(AggregatorError::AttestationCapReached);
        }

        // Dedup by signer; the HashMap insert is idempotent on
        // duplicate signer (latest wins, but signature is
        // verified already).
        pending.attestations.insert(att.signer.clone(), att);

        let signer_count = pending.attestations.len();
        let quorum = lattice_quorum(self.config.group_size);

        if signer_count >= quorum {
            // Promote to certified table.
            let cell_round = pending.cell.round;
            let cell_author = pending.cell.author.clone();
            let attestations_vec: Vec<CellAttestation> = {
                let mut v: Vec<_> = pending.attestations.values().cloned().collect();
                v.sort_by(|a, b| a.signer.cmp(&b.signer));
                v
            };
            let cell = pending.cell.clone();
            // Remove from pending, insert into certified.
            self.pending.remove(&cell_id);
            let cert = CellCertificate {
                cell,
                attestations: attestations_vec,
            };
            self.certified
                .insert((cell_round, cell_author), cert.clone());
            AttestationOutcome::Certified(cert)
        } else {
            AttestationOutcome::Pending {
                signer_count,
                quorum,
            }
        }
    }

    /// Garbage collect cells whose round is older than the retention
    /// window. Called periodically by the runtime (e.g. every commit
    /// pass). Returns the number of evicted entries (informational).
    pub fn gc_old_cells(&mut self) -> usize {
        let cutoff = self
            .high_water_round
            .saturating_sub(self.config.retention_rounds);
        let mut evicted = 0;

        // Pending: evict by cell.round.
        self.pending.retain(|_, p| {
            if p.cell.round < cutoff {
                evicted += 1;
                false
            } else {
                true
            }
        });

        // Certified: evict by key.0 (round).
        let to_drop: Vec<_> = self
            .certified
            .keys()
            .filter(|(r, _)| *r < cutoff)
            .cloned()
            .collect();
        for k in to_drop {
            self.certified.remove(&k);
            evicted += 1;
        }

        evicted
    }

    /// Query all certified cells at a given round, in canonical
    /// iteration order (author lexicographic). The lineage commit
    /// walker consumes this.
    pub fn certified_at_round(
        &self,
        round: LatticeRound,
    ) -> impl Iterator<Item = &CellCertificate> {
        self.certified
            .range((round, String::new())..(round + 1, String::new()))
            .map(|(_, c)| c)
    }

    /// Look up a specific certified cell by `(round, author)`.
    pub fn certified_get(&self, round: LatticeRound, author: &str) -> Option<&CellCertificate> {
        self.certified.get(&(round, author.to_string()))
    }

    /// Total certified count. For DIAG / observability.
    #[inline]
    pub fn certified_count(&self) -> usize {
        self.certified.len()
    }

    /// Total pending count. For DIAG / observability.
    #[inline]
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    /// High-water round seen so far.
    #[inline]
    pub fn high_water_round(&self) -> LatticeRound {
        self.high_water_round
    }
}

/// Verify a `CellAttestation` against the given cell. The attester
/// signs the cell's `signable_bytes()` with their identity key. We
/// reconstruct the payload and verify Ed25519.
///
/// Returns true on success. Side-effect-free.
fn verify_attestation_against_cell(att: &CellAttestation, cell: &LatticeCell) -> bool {
    use ed25519_dalek::{Signature, Verifier, VerifyingKey};
    let key = match VerifyingKey::from_bytes(&att.signer_pubkey) {
        Ok(k) => k,
        Err(_) => return false,
    };
    let sig = Signature::from_bytes(&att.signature);
    key.verify(&cell.signable_bytes(), &sig).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ed25519_dalek::{Signer, SigningKey};
    use rand::rngs::OsRng;

    /// Helper: produce a (signing_key, cell) pair for the given author.
    fn make_cell(round: LatticeRound, author: &str) -> (SigningKey, LatticeCell) {
        let mut csprng = OsRng;
        let sk = SigningKey::generate(&mut csprng);
        let pk = sk.verifying_key();

        let mut cell = LatticeCell::with_sorted_parents(
            round,
            "group_test".to_string(),
            author.to_string(),
            pk.to_bytes(),
            vec![[1u8; 32], [2u8; 32]],
            [3u8; 32],
            [0u8; 64],
        );
        cell.author_signature = sk.sign(&cell.signable_bytes()).to_bytes();
        (sk, cell)
    }

    /// Helper: produce a `CellAttestation` from `attester` on `cell`.
    fn make_attestation(cell: &LatticeCell, attester_id: &str) -> CellAttestation {
        let mut csprng = OsRng;
        let sk = SigningKey::generate(&mut csprng);
        let pk = sk.verifying_key();
        let sig = sk.sign(&cell.signable_bytes()).to_bytes();
        CellAttestation {
            signer: attester_id.to_string(),
            signer_pubkey: pk.to_bytes(),
            signature: sig,
        }
    }

    #[test]
    fn observe_cell_accepts_well_formed() {
        let mut agg = LatticeAggregator::new(AggregatorConfig::default());
        let (_, cell) = make_cell(1, "ln-1");
        let id = agg.observe_cell(cell).expect("good cell accepted");
        assert_eq!(agg.pending_count(), 1);
        assert_eq!(agg.certified_count(), 0);
        assert_ne!(id, [0u8; 32]);
    }

    #[test]
    fn observe_cell_rejects_bad_signature() {
        let mut agg = LatticeAggregator::new(AggregatorConfig::default());
        let (_, mut cell) = make_cell(1, "ln-1");
        cell.author_signature[0] ^= 0x01; // tamper
        let err = agg.observe_cell(cell).unwrap_err();
        assert!(matches!(err, AggregatorError::BadCellSignature));
    }

    #[test]
    fn attestations_below_quorum_stay_pending() {
        // group_size=5 → quorum=3. Two attestations should keep
        // the cell in pending.
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 5,
            ..Default::default()
        });
        let (_, cell) = make_cell(1, "ln-1");
        let id = agg.observe_cell(cell.clone()).unwrap();
        let a1 = make_attestation(&cell, "ln-2");
        let a2 = make_attestation(&cell, "ln-3");
        let o1 = agg.observe_attestation(id, a1);
        let o2 = agg.observe_attestation(id, a2);
        assert!(matches!(
            o1,
            AttestationOutcome::Pending {
                signer_count: 1,
                quorum: 3
            }
        ));
        assert!(matches!(
            o2,
            AttestationOutcome::Pending {
                signer_count: 2,
                quorum: 3
            }
        ));
        assert_eq!(agg.pending_count(), 1);
        assert_eq!(agg.certified_count(), 0);
    }

    #[test]
    fn quorum_promotes_to_certified() {
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 5,
            ..Default::default()
        });
        let (_, cell) = make_cell(7, "ln-author");
        let id = agg.observe_cell(cell.clone()).unwrap();
        let _ = agg.observe_attestation(id, make_attestation(&cell, "ln-A"));
        let _ = agg.observe_attestation(id, make_attestation(&cell, "ln-B"));
        let third = agg.observe_attestation(id, make_attestation(&cell, "ln-C"));
        assert!(matches!(third, AttestationOutcome::Certified(_)));
        assert_eq!(agg.pending_count(), 0);
        assert_eq!(agg.certified_count(), 1);
        assert!(agg.certified_get(7, "ln-author").is_some());
    }

    #[test]
    fn duplicate_signer_does_not_double_count() {
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 5,
            ..Default::default()
        });
        let (_, cell) = make_cell(1, "ln-1");
        let id = agg.observe_cell(cell.clone()).unwrap();
        let same_signer_a = make_attestation(&cell, "ln-X");
        let same_signer_b = make_attestation(&cell, "ln-X");
        let _ = agg.observe_attestation(id, same_signer_a);
        let second = agg.observe_attestation(id, same_signer_b);
        // Second insert overrides but does NOT increase count beyond 1.
        assert!(matches!(
            second,
            AttestationOutcome::Pending {
                signer_count: 1,
                ..
            }
        ));
    }

    #[test]
    fn unknown_cell_rejects_attestation() {
        let mut agg = LatticeAggregator::new(AggregatorConfig::default());
        let (_, cell) = make_cell(1, "ln-x");
        let att = make_attestation(&cell, "ln-y");
        let o = agg.observe_attestation([99u8; 32], att);
        assert!(matches!(
            o,
            AttestationOutcome::Rejected(AggregatorError::UnknownCell(_))
        ));
    }

    #[test]
    fn bad_attestation_signature_rejected() {
        let mut agg = LatticeAggregator::new(AggregatorConfig::default());
        let (_, cell) = make_cell(1, "ln-author");
        let id = agg.observe_cell(cell.clone()).unwrap();
        let mut att = make_attestation(&cell, "ln-X");
        att.signature[0] ^= 0x01; // tamper
        let o = agg.observe_attestation(id, att);
        assert!(matches!(
            o,
            AttestationOutcome::Rejected(AggregatorError::BadAttestationSignature)
        ));
    }

    #[test]
    fn gc_evicts_old_cells() {
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 5,
            retention_rounds: 5,
            ..Default::default()
        });
        // Cells at rounds 1..=10. high_water = 10. Cutoff = 10 - 5 = 5.
        // Rounds 1..=4 evict (round < 5).
        for r in 1..=10 {
            let (_, cell) = make_cell(r, &format!("ln-{}", r));
            let _ = agg.observe_cell(cell);
        }
        assert_eq!(agg.pending_count(), 10);
        let evicted = agg.gc_old_cells();
        assert_eq!(evicted, 4); // rounds 1, 2, 3, 4
        assert_eq!(agg.pending_count(), 6); // rounds 5..=10
    }

    #[test]
    fn certified_at_round_iterates_in_lex_order() {
        let mut agg = LatticeAggregator::new(AggregatorConfig {
            group_size: 4,
            ..Default::default()
        });
        // quorum = 3 for group_size=4. Build 3 certified cells at the
        // same round with authors in non-sorted insertion order.
        for author in ["ln-c", "ln-a", "ln-b"] {
            let (_, cell) = make_cell(5, author);
            let id = agg.observe_cell(cell.clone()).unwrap();
            let _ = agg.observe_attestation(id, make_attestation(&cell, "x1"));
            let _ = agg.observe_attestation(id, make_attestation(&cell, "x2"));
            let _ = agg.observe_attestation(id, make_attestation(&cell, "x3"));
        }
        let authors: Vec<_> = agg
            .certified_at_round(5)
            .map(|c| c.cell.author.clone())
            .collect();
        assert_eq!(authors, vec!["ln-a", "ln-b", "ln-c"]);
    }
}
