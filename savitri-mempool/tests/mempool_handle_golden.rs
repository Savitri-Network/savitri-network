//! Golden tests for `MempoolHandle` (Tier 3 refactor, Phase 1).
//!
//! These tests guard the single most important invariant of the new
//! flat handle: cloning shares state. If `MempoolHandle::clone()` ever
//! starts producing a fresh backing pipeline, RPC ingress and proposer
//! drain silently disconnect — exactly the failure mode of the an earlier fix
//! family (see `memory/mempool_handle_audit_2026-04-28.md`).
//!
//! Run only this target to bypass the pre-existing `--lib` compile
//! errors in unrelated modules:
//!   `cargo test --release -p savitri-mempool --test mempool_handle_golden`

use std::sync::Arc;

use savitri_mempool::mempool::MempoolHandle;
use savitri_storage::{Storage, StorageTrait};
use tempfile::TempDir;

/// Helper: build a handle backed by a fresh storage in a tempdir. The
/// `TempDir` is returned alongside so it lives for the test duration
/// (dropping it removes the on-disk files).
fn make_handle() -> (MempoolHandle, TempDir) {
    let tmp = TempDir::new().expect("tempdir");
    let storage: Arc<dyn StorageTrait> = Arc::new(
        Storage::new(tmp.path().to_str().expect("utf-8 path")).expect("Storage::new on tempdir"),
    );
    (MempoolHandle::new(storage), tmp)
}

/// Cloning a handle MUST share state. Two clones must observe each
/// other's mempool through `ptr_eq`. This is the invariant whose
/// violation causes an earlier fix (RPC writes vanish from proposer view).
#[tokio::test]
async fn handle_clone_shares_state() {
    let (h1, _tmp) = make_handle();
    let h2 = h1.clone();
    let h3 = h2.clone();

    assert!(h1.ptr_eq(&h2), "h1 and h2 must share state after clone");
    assert!(h2.ptr_eq(&h3), "h2 and h3 must share state after clone");
    assert!(h1.ptr_eq(&h3), "ptr_eq must be transitive");

    // Stats read through any clone must observe the same counters. We
    // signing harness, but the snapshot must at least be reachable from
    // every clone and remain consistent.
    let s1 = h1.stats().await;
    let s3 = h3.stats().await;
    assert_eq!(
        s1.total, s3.total,
        "stats observed via two clones must agree"
    );
}

/// Chained clones (A -> B -> C -> ...) must remain `ptr_eq` to the
/// original. Guards against an accidental `Arc::new(...)` inside the
/// `Clone` derive during Phase 2/3 refactors.
#[tokio::test]
async fn handle_ptr_eq_after_clone() {
    let (original, _tmp) = make_handle();
    let mut current = original.clone();
    for i in 0..5 {
        let next = current.clone();
        assert!(
            original.ptr_eq(&next),
            "clone link #{} must ptr_eq the original",
            i
        );
        current = next;
    }
}

/// `Arc::strong_count` MUST grow as we clone. If the handle ever
/// degrades to a deep-copy `Clone`, this will catch it: the count would
/// stay at 1 while new instances are produced.
#[tokio::test]
async fn handle_arc_count_grows_with_clones() {
    let (h1, _tmp) = make_handle();
    let baseline = h1.arc_strong_count();
    assert_eq!(baseline, 1, "fresh handle must have strong_count == 1");

    let h2 = h1.clone();
    assert_eq!(h1.arc_strong_count(), baseline + 1);

    let h3 = h1.clone();
    let h4 = h1.clone();
    assert_eq!(h1.arc_strong_count(), baseline + 3);

    // Drop the intermediate clones and confirm the count shrinks back.
    drop(h2);
    drop(h3);
    drop(h4);
    assert_eq!(
        h1.arc_strong_count(),
        baseline,
        "strong_count must shrink when clones are dropped"
    );
}
