//! Foundations regression tests — golden tests for consensus primitives.
//!
//! before consolidating duplicated implementations of `current_epoch`,
//! `compute_block_hash`, and `quorum_threshold` scattered across the
//! workspace.
//!
//! Tests are gated with `#[ignore]` until each Tier 1 sub-task lands
//! its canonical primitive. Once landed, remove `#[ignore]` and the
//! test becomes a permanent regression guard.
//!
//! See `memory/refactor_plan_2026-04-28.md` for the full plan and
//! `memory/architectural_debt.md` for the duplication map.

use savitri_consensus::primitives::epoch::{current_epoch, current_slot};
use savitri_consensus::primitives::hashing::compute_block_hash;
use savitri_consensus::primitives::quorum::quorum_for_voters;

//
// Canonical formula (target):
//     epoch = floor((now_ms - genesis_ms) / heartbeat_ms / slots_per_epoch)
//
// Pre-conditions:
//     - heartbeat_ms > 0, slots_per_epoch > 0
//     - now_ms < genesis_ms → 0 (clamped)
//
// lands `savitri_consensus::primitives::epoch::current_epoch`.

const TEST_GENESIS_MS: u64 = 1_777_234_282_043;
const TEST_HEARTBEAT_MS: u64 = 5_000;
const TEST_SLOTS_PER_EPOCH: u64 = 20;
const SLOT_LEN_MS: u64 = TEST_HEARTBEAT_MS * TEST_SLOTS_PER_EPOCH; // 100_000 ms = 100 s

#[test]
fn current_epoch_at_genesis_is_zero() {
    assert_eq!(
        current_epoch(
            TEST_GENESIS_MS,
            TEST_GENESIS_MS,
            TEST_HEARTBEAT_MS,
            TEST_SLOTS_PER_EPOCH
        ),
        0
    );
}

#[test]
fn current_epoch_within_first_epoch_is_zero() {
    for delta in [1u64, 1_000, SLOT_LEN_MS - 1] {
        let e = current_epoch(
            TEST_GENESIS_MS + delta,
            TEST_GENESIS_MS,
            TEST_HEARTBEAT_MS,
            TEST_SLOTS_PER_EPOCH,
        );
        assert_eq!(e, 0, "delta={}", delta);
    }
}

#[test]
fn current_epoch_advances_on_slot_boundary() {
    assert_eq!(
        current_epoch(
            TEST_GENESIS_MS + SLOT_LEN_MS,
            TEST_GENESIS_MS,
            TEST_HEARTBEAT_MS,
            TEST_SLOTS_PER_EPOCH
        ),
        1
    );
    assert_eq!(
        current_epoch(
            TEST_GENESIS_MS + 7 * SLOT_LEN_MS,
            TEST_GENESIS_MS,
            TEST_HEARTBEAT_MS,
            TEST_SLOTS_PER_EPOCH
        ),
        7
    );
}

#[test]
fn current_epoch_before_genesis_clamps_to_zero() {
    assert_eq!(
        current_epoch(
            TEST_GENESIS_MS - 1,
            TEST_GENESIS_MS,
            TEST_HEARTBEAT_MS,
            TEST_SLOTS_PER_EPOCH
        ),
        0
    );
}

#[test]
fn current_epoch_far_future_no_overflow() {
    // year 2200 ≈ 7_257_600_000_000 ms after epoch
    let e = current_epoch(
        7_257_600_000_000,
        0,
        TEST_HEARTBEAT_MS,
        TEST_SLOTS_PER_EPOCH,
    );
    assert!(e > 0);
}

#[test]
fn current_slot_advances_each_heartbeat() {
    for n in 0..5u64 {
        assert_eq!(
            current_slot(
                TEST_GENESIS_MS + n * TEST_HEARTBEAT_MS,
                TEST_GENESIS_MS,
                TEST_HEARTBEAT_MS
            ),
            n
        );
    }
}

/// Parity check: the canonical primitive in savitri-consensus must agree
/// byte-for-byte with the formula in savitri-core::core::unified_slot.
/// Both crates need their own implementation (savitri-core cannot depend
/// on savitri-consensus due to the dependency direction), but they must
/// stay in lockstep. This test fails if either side drifts.
#[test]
fn current_epoch_parity_with_savitri_core_unified_slot() {
    // Reproduce savitri-core/src/core/unified_slot.rs:68-76 inline:
    //     elapsed = saturating_sub(now_ms, genesis_ms)
    //     slot = elapsed / heartbeat_ms.max(1)
    //     epoch = slot / slots_per_epoch.max(1)
    let cases = [
        (TEST_GENESIS_MS, 0u64),
        (TEST_GENESIS_MS + SLOT_LEN_MS, 1u64),
        (TEST_GENESIS_MS + 100 * SLOT_LEN_MS, 100u64),
        (TEST_GENESIS_MS + 12_345_678, 123u64), // 12_345_678 / 5_000 / 20 = 123
    ];
    for (now, expected) in cases {
        let core_formula_epoch = {
            let elapsed = now.saturating_sub(TEST_GENESIS_MS);
            let slot = elapsed / TEST_HEARTBEAT_MS.max(1);
            slot / TEST_SLOTS_PER_EPOCH.max(1)
        };
        let consensus_canonical = current_epoch(
            now,
            TEST_GENESIS_MS,
            TEST_HEARTBEAT_MS,
            TEST_SLOTS_PER_EPOCH,
        );
        assert_eq!(
            consensus_canonical, core_formula_epoch,
            "parity broken at now={now}"
        );
        assert_eq!(
            consensus_canonical, expected,
            "expected mismatch at now={now}"
        );
    }
}

//
// into the canonical primitive in savitri_consensus::primitives::hashing.
// Two distinct hash functions now exist:
//
//   compute_block_hash         — block IDENTITY (SHA-256, parent ‖ state_root_pad64
//                                ‖ tx_root_pad64 ‖ height_le, output 64 bytes)
//   compute_signed_proposal_hash — BFT signed proposal (SHA-512, 9 fields)
//
// Migrated callers:
//   savitri-lightnode/src/p2p/block.rs:1783         — wrapper to canonical
//   savitri-masternode/src/libp2p_network.rs:5056   — uses compute_signed_proposal_hash
//
// Removed (dead code):
//   savitri-consensus/src/crypto/hashes.rs:67 (5-param SHA-512, no callers)
//

#[test]
fn block_hash_genesis_is_pinned() {
    // Pinned golden: canonical primitive in savitri_consensus::primitives::hashing.
    // If this test breaks, the canonical formula has changed and ALL
    // callers must be updated in the same commit (and a coordinated
    // testnet wipe is required since storage keys depend on this hash).
    let h = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
    let hex_prefix = hex::encode(&h[..8]);
    assert_eq!(
        hex_prefix, "6d9c54dee5660c46",
        "compute_block_hash genesis pin broken"
    );
    assert_eq!(&h[32..], &[0u8; 32], "second half must be zero padding");
}

#[test]
fn block_hash_changes_on_height() {
    let h0 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
    let h1 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 1);
    assert_ne!(h0, h1, "height delta must change hash");
}

#[test]
fn block_hash_changes_on_prev_hash() {
    let mut prev = [0u8; 64];
    prev[0] = 1;
    let h0 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
    let h1 = compute_block_hash(&prev, &[0; 32], &[0; 32], 0);
    assert_ne!(h0, h1, "prev_hash delta must change hash");
}

#[test]
fn block_hash_changes_on_tx_root() {
    let mut tx_root = [0u8; 32];
    tx_root[0] = 1;
    let h0 = compute_block_hash(&[0; 64], &[0; 32], &[0; 32], 0);
    let h1 = compute_block_hash(&[0; 64], &[0; 32], &tx_root, 0);
    assert_ne!(h0, h1, "tx_root delta must change hash");
}

#[test]
fn block_hash_deterministic() {
    let h_a = compute_block_hash(&[7; 64], &[3; 32], &[5; 32], 42);
    let h_b = compute_block_hash(&[7; 64], &[3; 32], &[5; 32], 42);
    assert_eq!(h_a, h_b, "compute_block_hash must be deterministic");
}

/// Lightnode wrapper (`savitri-lightnode/src/p2p/block.rs::compute_block_hash`)
/// must produce the same hash as the canonical for the same Block input.
/// This is enforced indirectly: the wrapper just forwards the four
/// primitive fields to the canonical, so byte equality follows from
/// the unit tests in primitives::hashing::tests. We re-prove it here
/// inline as a redundant tripwire: if a future change adds extra fields
/// to either side, this test fails.
#[test]
fn block_hash_matches_legacy_lightnode_formula() {
    use sha2::{Digest, Sha256};
    let parent = [42u8; 64];
    let state_root = [7u8; 32];
    let tx_root = [13u8; 32];
    let height = 99u64;

    let canonical = compute_block_hash(&parent, &state_root, &tx_root, height);

    let legacy = {
        let mut state_root_64 = [0u8; 64];
        state_root_64[..32].copy_from_slice(&state_root);
        let mut tx_root_64 = [0u8; 64];
        tx_root_64[..32].copy_from_slice(&tx_root);
        let mut hasher = Sha256::new();
        hasher.update(&parent);
        hasher.update(&state_root_64);
        hasher.update(&tx_root_64);
        hasher.update(&height.to_le_bytes());
        let digest = hasher.finalize();
        let mut hash = [0u8; 64];
        hash[..32].copy_from_slice(digest.as_slice());
        hash
    };

    assert_eq!(
        canonical, legacy,
        "canonical must match legacy lightnode formula byte-for-byte"
    );
}

//
//
//   savitri-masternode/src/vote_aggregator.rs:145
//     formula: per-group, ceil(2 * group_size / 3) + 1   (an earlier fix)
//
//   savitri-lightnode/src/p2p/certificate.rs:446
//     formula: committee-global, (2 * N + 2) / 3   (incompatible with per-group)
//

#[test]
fn quorum_for_group_size_3() {
    assert_eq!(quorum_for_voters(3), 2); // ceil(2*3/3) = 2
}

#[test]
fn quorum_for_group_size_4() {
    // ceil(2*4/3) = ceil(2.67) = 3
    assert_eq!(quorum_for_voters(4), 3);
}

#[test]
fn quorum_for_group_size_zero() {
    assert_eq!(quorum_for_voters(0), 0);
}

//
// The current 3-layer Arc<Mutex<>> wrapping (MempoolPipeline → Real →
// Mempool) survives only because `inner_for_rpc()` returns the same
// inner Arc that the proposer holds. A refactor that breaks this
// invariant silently splits RPC ingress from proposer drain.
//
// where Clone gives a new handle, Arc::ptr_eq always true.

// roundtrip tests live in `savitri-mempool/tests/mempool_handle_golden.rs`
// (3 tests, all green). They cannot be replicated here because savitri-consensus
// does not depend on savitri-mempool (cycle would be created — mempool already
// depends on consensus).
