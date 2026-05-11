//! Lattice ordering — DAG-based parallel block ordering primitive.
//!
//! Part of Savitri V0.2 Phase 2 (Lattice ordering, follow-up to Phase 1
//! Score Canonicity / issue #31). See `docs/CONSENSUS_V0.2_DESIGN.md` §4
//! for the full specification and rationale.
//!
//! ## Status
//!
//! TYPE-LEVEL SPIKE. The structs, identifiers, and basic helpers are
//! defined here. Aggregation rules (cell certificate quorum, lineage
//! commit, cycle pivot election) live alongside the existing consensus
//! protocols and will be wired in a follow-up issue. This module is
//! standalone — it compiles without altering any existing code path.
//!
//! ## Overview
//!
//! In Phase 1, a single elected proposer per slot builds a block. The
//! proposer is a bottleneck: while it works, the other lightnodes idle.
//!
//! Phase 2 generalizes to a directed acyclic graph of certified cells.
//! Every lightnode in a group publishes one [`LatticeCell`] per lattice
//! round, carrying a batch of transactions. A cell becomes part of the
//! lattice once `2f+1` group members sign its header (a
//! [`CellCertificate`]). Throughput equals the sum of cell sizes across
//! all members, not the throughput of a single proposer.
//!
//! Ordering is achieved by [`Cycle`]s: every two consecutive lattice
//! rounds form a cycle. The PoU-weighted round-robin schedule elects
//! one cell author as the [`CyclePivot`] for each cycle. When the pivot
//! cell has `2f+1` followers in the round after it, all cells in the
//! pivot's causal history (transitively reachable via `parents`) commit
//! deterministically. This is the "lineage commit" rule.
//!
//! ## Compatibility with Phase 1
//!
//! - The PoU-weighted RR helper (`build_weighted_proposer_schedule`)
//!   introduced in earlier work is reused; the slot lookup unit changes
//!   from "block index" to "cycle index", but the schedule generation
//!   is identical.
//! - The `LatencyTable` from Phase 1 powers per-author scoring inside
//!   the pivot election. The wall-clock-aligned convergence introduced
//!   alongside this spike (see `latency_canon_publisher::current_wall_clock_bucket`)
//!   guarantees the table is byte-identical cluster-wide, a prerequisite
//!   for deterministic cycle pivot rotation.
//!
//! ## Wire format stability
//!
//! All on-the-wire types in this module derive `Serialize` /
//! `Deserialize` against a canonical layout. Field order MUST NOT change
//! after the first deployment without a coordinated wire-format bump.
//! Integer encoding only (no f64) for byte-canonicity across observers.

use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;

/// Lattice round index. Time step at which cells are published. One round
/// per `LATTICE_ROUND_DURATION_SECS` wall-clock seconds (default 1s).
pub type LatticeRound = u64;

/// Cycle index. Every two consecutive lattice rounds form a cycle.
/// `cycle = round / 2`. The pivot for each cycle is elected from the
/// PoU-weighted RR schedule using `cycle as slot`.
pub type CycleIndex = u64;

/// Default lattice round duration. Conservative starting point — tune
/// once the consensus pipeline is exercised on the testnet cluster.
pub const LATTICE_ROUND_DURATION_SECS: u64 = 1;

/// Stable identifier for a single cell. blake3 hash over the cell's
/// canonical signable bytes.
pub type CellId = [u8; 32];

/// Stable identifier for a transaction batch root inside a cell. The
/// batch itself is gossipped separately (Narwhal-like data availability);
/// the cell header carries only the root hash to keep the lattice DAG
/// compact.
pub type BatchRoot = [u8; 32];

/// One vertex in the lattice. Published by exactly one author per round.
///
/// The author signs `signable_bytes()` with their network identity key.
/// Other group members verify the signature, then attest by signing the
/// same payload with their own keys — the collected attestations form a
/// [`CellCertificate`].
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LatticeCell {
    /// Lattice round at which this cell was authored.
    pub round: LatticeRound,
    /// Author's group_id (`group_<epoch>_<idx>_<epoch>` per V0.1
    /// convention; will evolve once group rotation is reworked).
    pub group_id: String,
    /// Author's stable peer_id (as used in PoU scoring).
    pub author: String,
    /// Author's Ed25519 public key, used by the verifier.
    pub author_pubkey: [u8; 32],
    /// Parent cell ids from round `round - 1`. The cell must reference
    /// `2f+1` parents to be admissible into the lattice (anti-stall
    /// rule from Bullshark-family DAGs). Order is sorted ascending for
    /// canonicity of the signable.
    pub parents: Vec<CellId>,
    /// Root of the transaction batch carried by this cell. The batch
    /// is propagated separately on a per-cell topic (mirror of the
    /// existing `/savitri/group/<gid>/tx/1` design); the lattice
    /// itself only references the root.
    pub batch_root: BatchRoot,
    /// Author's signature over `signable_bytes()`. Verifies against
    /// `author_pubkey`.
    #[serde(with = "BigArray")]
    pub author_signature: [u8; 64],
}

impl LatticeCell {
    /// Canonical signable payload. Used both by the author at sign time
    /// and by every attester / verifier.
    ///
    /// Layout: `b"savitri-lattice-cell-v1|" || round || group_id ||
    /// author || author_pubkey || parents_sorted_concat || batch_root`.
    /// Integer fields are little-endian. Parent ids are pre-sorted by
    /// the constructor so observers cannot disagree on the canonical
    /// concatenation.
    pub fn signable_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(
            64 + self.group_id.len() + self.author.len() + 32 * self.parents.len() + 64,
        );
        out.extend_from_slice(b"savitri-lattice-cell-v1|");
        out.extend_from_slice(&self.round.to_le_bytes());
        out.push(b'|');
        out.extend_from_slice(self.group_id.as_bytes());
        out.push(b'|');
        out.extend_from_slice(self.author.as_bytes());
        out.push(b'|');
        out.extend_from_slice(&self.author_pubkey);
        out.push(b'|');
        // Parents are sorted by `with_sorted_parents`; concatenate without
        // any separator (each id is fixed 32 bytes).
        for p in &self.parents {
            out.extend_from_slice(p);
        }
        out.push(b'|');
        out.extend_from_slice(&self.batch_root);
        out
    }

    /// Construct a cell with the parents in sorted order. Callers should
    /// always use this helper — passing unsorted parents directly to the
    /// struct literal would produce a non-canonical signable.
    pub fn with_sorted_parents(
        round: LatticeRound,
        group_id: String,
        author: String,
        author_pubkey: [u8; 32],
        mut parents: Vec<CellId>,
        batch_root: BatchRoot,
        author_signature: [u8; 64],
    ) -> Self {
        parents.sort_unstable();
        Self {
            round,
            group_id,
            author,
            author_pubkey,
            parents,
            batch_root,
            author_signature,
        }
    }

    /// Compute the cell's stable identifier (blake3 of signable_bytes).
    pub fn cell_id(&self) -> CellId {
        *blake3::hash(&self.signable_bytes()).as_bytes()
    }

    /// Verify the author's signature on this cell. Does NOT enforce the
    /// parent quorum (`2f+1`) nor that parents actually exist in the
    /// lattice — those checks live in the aggregator.
    pub fn verify_author_signature(&self) -> bool {
        use ed25519_dalek::{Signature, Verifier, VerifyingKey};
        let key = match VerifyingKey::from_bytes(&self.author_pubkey) {
            Ok(k) => k,
            Err(_) => return false,
        };
        let sig = Signature::from_bytes(&self.author_signature);
        key.verify(&self.signable_bytes(), &sig).is_ok()
    }
}

/// One attestation on a cell. A group member signs the cell's
/// `signable_bytes()` with their own key, asserting the cell is well-
/// formed and the parent set is acceptable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CellAttestation {
    /// Signer's stable peer_id.
    pub signer: String,
    /// Signer's Ed25519 public key.
    pub signer_pubkey: [u8; 32],
    /// Signature over the cell's `signable_bytes()`.
    #[serde(with = "BigArray")]
    pub signature: [u8; 64],
}

/// A certified lattice cell. Carries the original cell plus the set of
/// attestations that meet BFT 2f+1 quorum. Once a cell is certified, it
/// is admissible as a parent of subsequent-round cells.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CellCertificate {
    /// The cell being certified.
    pub cell: LatticeCell,
    /// Attestations from distinct group members. Sorted by signer for
    /// canonicity (signature aggregation can later replace this with a
    /// BLS aggregate sig).
    pub attestations: Vec<CellAttestation>,
}

impl CellCertificate {
    /// Number of distinct attesters. Caller compares against the
    /// per-group `2f+1` threshold.
    #[inline]
    pub fn attestation_count(&self) -> usize {
        self.attestations.len()
    }

    /// Cell id (stable hash). Convenience forward to the inner cell.
    #[inline]
    pub fn cell_id(&self) -> CellId {
        self.cell.cell_id()
    }
}

/// A cycle commit decision. The pivot for cycle `k` is elected via the
/// existing PoU-weighted RR schedule using `cycle_index = k` as the slot
/// lookup. When the pivot's cell at round `2k` has `2f+1` followers (cells
/// at round `2k+1` that reference it in their `parents`), the cycle
/// commits.
///
/// Concretely "committing" means: every certified cell transitively
/// reachable from the pivot cell via the `parents` relation is appended
/// to the canonical ordered stream, in deterministic topological order
/// (round-major, then author lexicographic tiebreak).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cycle {
    /// Cycle index `k`. Anchor round is `2 * k`, follow round is `2 * k + 1`.
    pub index: CycleIndex,
    /// Group this cycle belongs to.
    pub group_id: String,
    /// PoU-elected pivot author for this cycle (peer_id).
    pub pivot: String,
    /// Pivot's cell id at the anchor round (round `2k`).
    pub pivot_cell: CellId,
    /// Cells committed by this cycle's lineage rule, in deterministic
    /// topological order. Empty if the cycle skipped (pivot did not
    /// achieve 2f+1 followers).
    pub committed_cells: Vec<CellId>,
}

impl Cycle {
    /// Convenience: anchor round for this cycle.
    #[inline]
    pub fn anchor_round(&self) -> LatticeRound {
        self.index.saturating_mul(2)
    }

    /// Convenience: follow round for this cycle.
    #[inline]
    pub fn follow_round(&self) -> LatticeRound {
        self.index.saturating_mul(2).saturating_add(1)
    }

    /// Convenience: did this cycle commit (or did it skip)?
    #[inline]
    pub fn did_commit(&self) -> bool {
        !self.committed_cells.is_empty()
    }
}

/// BFT quorum threshold for the lattice. Mirrors the canonical
/// `savitri_consensus::primitives::quorum::quorum_for_voters`: with `n`
/// distinct group members, `f = (n - 1) / 3` and `quorum = 2f + 1`.
#[inline]
pub fn lattice_quorum(group_size: usize) -> usize {
    if group_size == 0 {
        return 0;
    }
    let f = (group_size - 1) / 3;
    2 * f + 1
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quorum_classic_pbft_values() {
        // PBFT: f = (n - 1) / 3, quorum = 2f + 1.
        // For n in {3,4,5,6}, f = (n-1)/3 ∈ {0,1,1,1} → quorum ∈ {1,3,3,3}.
        // For n in {7,8,9}, f = 2 → quorum = 5.
        // For n=10, f = 3 → quorum = 7.
        assert_eq!(lattice_quorum(0), 0);
        assert_eq!(lattice_quorum(1), 1); // f=0
        assert_eq!(lattice_quorum(3), 1); // f=0
        assert_eq!(lattice_quorum(4), 3); // f=1 (3f+1=4)
        assert_eq!(lattice_quorum(5), 3); // f=1
        assert_eq!(lattice_quorum(6), 3); // f=1
        assert_eq!(lattice_quorum(7), 5); // f=2 (3f+1=7)
        assert_eq!(lattice_quorum(9), 5); // f=2 (still — (9-1)/3=2)
        assert_eq!(lattice_quorum(10), 7); // f=3 (3f+1=10)
    }

    fn sample_cell() -> LatticeCell {
        LatticeCell::with_sorted_parents(
            42,
            "group_42_0".to_string(),
            "ln-1".to_string(),
            [1u8; 32],
            vec![[3u8; 32], [1u8; 32], [2u8; 32]],
            [9u8; 32],
            [0u8; 64],
        )
    }

    #[test]
    fn parents_sorted_after_with_sorted_parents() {
        let c = sample_cell();
        assert_eq!(c.parents, vec![[1u8; 32], [2u8; 32], [3u8; 32]]);
    }

    #[test]
    fn signable_bytes_observer_independent() {
        let c1 = sample_cell();
        let c2 = sample_cell();
        assert_eq!(c1.signable_bytes(), c2.signable_bytes());
        assert!(c1.signable_bytes().starts_with(b"savitri-lattice-cell-v1|"));
    }

    #[test]
    fn cell_id_deterministic() {
        let c1 = sample_cell();
        let c2 = sample_cell();
        assert_eq!(c1.cell_id(), c2.cell_id());
    }

    #[test]
    fn signable_bytes_change_with_any_field() {
        let base = sample_cell();
        let base_bytes = base.signable_bytes();

        let mut other = base.clone();
        other.round = 43;
        assert_ne!(base_bytes, other.signable_bytes());

        let mut other = base.clone();
        other.group_id = "group_42_1".to_string();
        assert_ne!(base_bytes, other.signable_bytes());

        let mut other = base.clone();
        other.batch_root = [7u8; 32];
        assert_ne!(base_bytes, other.signable_bytes());
    }

    #[test]
    fn signature_round_trip() {
        use ed25519_dalek::{Signer, SigningKey};
        use rand::rngs::OsRng;
        let mut csprng = OsRng;
        let signing_key = SigningKey::generate(&mut csprng);
        let verifying_key = signing_key.verifying_key();

        let mut c = LatticeCell::with_sorted_parents(
            100,
            "group_100_0".to_string(),
            "ln-7".to_string(),
            verifying_key.to_bytes(),
            vec![[5u8; 32], [4u8; 32]],
            [11u8; 32],
            [0u8; 64],
        );
        let payload = c.signable_bytes();
        c.author_signature = signing_key.sign(&payload).to_bytes();
        assert!(c.verify_author_signature());

        // Tamper the batch_root — signature must no longer verify.
        c.batch_root = [99u8; 32];
        assert!(!c.verify_author_signature());
    }

    #[test]
    fn cycle_helpers() {
        let cy = Cycle {
            index: 7,
            group_id: "g".to_string(),
            pivot: "ln-3".to_string(),
            pivot_cell: [0u8; 32],
            committed_cells: vec![[1u8; 32], [2u8; 32]],
        };
        assert_eq!(cy.anchor_round(), 14);
        assert_eq!(cy.follow_round(), 15);
        assert!(cy.did_commit());

        let skipped = Cycle {
            committed_cells: vec![],
            ..cy.clone()
        };
        assert!(!skipped.did_commit());
    }
}
