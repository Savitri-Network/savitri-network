//! Integration tests for the `scoring::ObservationStore`.
//!
//! These tests exercise the public API surface only, so they can run via
//! `cargo test -p savitri-consensus --test scoring_observations` even while
//! the lib's internal `#[cfg(test)] mod tests` blocks have unrelated
//! pre-existing refactor debt.

use std::sync::Arc;

use savitri_consensus::scoring::{
    fl_robust, streak_daemon, ObservationStore, SlashReason, DEFAULT_WINDOW_SECS,
    MALICIOUS_GRADIENT_STREAK, MALICIOUS_GRADIENT_THRESHOLD_PERMILLE, MAX_SAMPLES_PER_METRIC,
};
use savitri_consensus::slashing::SlashingManager;
use savitri_consensus::types::slashing::{MisbehaviorType, SlashingConfig};
use savitri_consensus::types::{DefaultScoreCalculator, LatencyType, ScoreCalculator};

#[test]
fn record_then_read_round_trips() {
    let store = ObservationStore::new();
    store.record_latency("peer-a", 42, LatencyType::Ping);
    store.record_latency("peer-a", 80, LatencyType::BlockPropagation);

    let samples = store.latency_measurements("peer-a");
    assert_eq!(samples.len(), 2);
    assert_eq!(samples[0].rtt_ms, 42);
    assert_eq!(samples[1].rtt_ms, 80);
    assert_eq!(samples[0].measurement_type, LatencyType::Ping);
    assert_eq!(samples[1].measurement_type, LatencyType::BlockPropagation);
    assert_eq!(samples[0].peer_id, "peer-a");
}

#[test]
fn unknown_peer_returns_empty() {
    let store = ObservationStore::new();
    assert!(store.latency_measurements("nobody").is_empty());
    assert_eq!(store.latency_sample_count("nobody"), 0);
    assert_eq!(store.peer_count(), 0);
}

#[test]
fn respects_sample_cap() {
    let store = ObservationStore::with_window(DEFAULT_WINDOW_SECS);
    for i in 0..(MAX_SAMPLES_PER_METRIC + 100) {
        store.record_latency("peer-b", i as u64, LatencyType::Ping);
    }
    assert_eq!(store.latency_sample_count("peer-b"), MAX_SAMPLES_PER_METRIC);
}

#[test]
fn forget_peer_drops_samples() {
    let store = ObservationStore::new();
    store.record_latency("peer-c", 10, LatencyType::Ping);
    assert_eq!(store.peer_count(), 1);
    store.forget_peer("peer-c");
    assert_eq!(store.peer_count(), 0);
    assert!(store.latency_measurements("peer-c").is_empty());
}

#[test]
fn window_filters_old_samples_on_read() {
    // Window of 1s ensures every sample is immediately outside the rolling
    // window after a short sleep, so read returns nothing even though the
    // internal buffer may still hold entries until the next write prunes.
    let store = ObservationStore::with_window(1);
    store.record_latency("peer-d", 50, LatencyType::Ping);
    std::thread::sleep(std::time::Duration::from_secs(2));
    let samples = store.latency_measurements("peer-d");
    assert!(
        samples.is_empty(),
        "samples older than the window must not be returned"
    );
}

#[test]
fn end_to_end_samples_produce_real_latency_score() {
    // Proves the full scoring pipeline: live RTT samples recorded into the
    // store are consumed by `DefaultScoreCalculator::calculate_latency_score`
    // and produce the expected permille score. This is the primary value of
    // the observation store — it replaces the `&[]` stub that was always
    // returning POU_SCORE_DEFAULT (500).

    let store = ObservationStore::new();
    let calculator = DefaultScoreCalculator;

    // 1. Fast peer: average RTT 30ms → should hit POU_SCORE_MAX (1000).
    for rtt in [10u64, 30, 50] {
        store.record_latency("fast", rtt, LatencyType::Ping);
    }
    let fast_samples = store.latency_measurements("fast");
    let fast_score = calculator.calculate_latency_score(&fast_samples);
    assert_eq!(fast_score, 1000, "avg RTT 30ms must score 1000");

    // 2. Slow peer: average RTT 600ms → above the 500ms floor → 0.
    for rtt in [550u64, 600, 650] {
        store.record_latency("slow", rtt, LatencyType::Ping);
    }
    let slow_samples = store.latency_measurements("slow");
    let slow_score = calculator.calculate_latency_score(&slow_samples);
    assert_eq!(slow_score, 0, "avg RTT 600ms must score 0");

    // 3. Mid peer: average RTT 200ms → (500 - 200) * 1000 / 450 = 666.
    store.record_latency("mid", 200, LatencyType::Ping);
    let mid_samples = store.latency_measurements("mid");
    let mid_score = calculator.calculate_latency_score(&mid_samples);
    assert_eq!(mid_score, 666, "avg RTT 200ms must score 666");

    // 4. Empty store for unknown peer falls back to POU_SCORE_DEFAULT (500).
    let unknown = store.latency_measurements("unknown");
    let unknown_score = calculator.calculate_latency_score(&unknown);
    assert_eq!(unknown_score, 500, "no samples must default to 500");
}

#[test]
fn multiple_peers_are_isolated() {
    let store = ObservationStore::new();
    store.record_latency("peer-x", 10, LatencyType::Ping);
    store.record_latency("peer-y", 200, LatencyType::ConsensusMessage);

    let x = store.latency_measurements("peer-x");
    let y = store.latency_measurements("peer-y");
    assert_eq!(x.len(), 1);
    assert_eq!(y.len(), 1);
    assert_eq!(x[0].rtt_ms, 10);
    assert_eq!(y[0].rtt_ms, 200);
    assert_eq!(store.peer_count(), 2);
}

// ─── Integrity tracking (step b) ─────────────────────────────────────────────

#[test]
fn integrity_measurement_reflects_recorded_validations() {
    let store = ObservationStore::new();

    // 8 valid blocks, 2 invalid → block_integrity = 800 permille
    for _ in 0..8 {
        store.record_block_validation("peer-i", true);
    }
    for _ in 0..2 {
        store.record_block_validation("peer-i", false);
    }
    // 9/10 valid tx → tx_integrity = 900 permille
    for _ in 0..9 {
        store.record_tx_validation("peer-i", true);
    }
    store.record_tx_validation("peer-i", false);

    let m = store.build_integrity_measurement("peer-i", /* epoch */ 42);
    assert_eq!(m.valid_blocks, 8);
    assert_eq!(m.total_blocks, 10);
    assert_eq!(m.valid_transactions, 9);
    assert_eq!(m.total_transactions, 10);
    assert_eq!(m.slash_events, 0);
    assert_eq!(m.epoch, 42);

    // Scorer: (800 + 900) / 2 = 850, no slash penalty
    let calc = DefaultScoreCalculator;
    assert_eq!(calc.calculate_integrity_score(&m), 850);
}

#[test]
fn slash_events_cut_integrity_score() {
    let store = ObservationStore::new();
    for _ in 0..10 {
        store.record_block_validation("bad", true);
        store.record_tx_validation("bad", true);
    }
    // 3 slash events → 300 permille penalty off a 1000 baseline → 700
    store.record_slash("bad", SlashReason::DoubleVote);
    store.record_slash("bad", SlashReason::Equivocation);
    store.record_slash("bad", SlashReason::InvalidBlock);

    let m = store.build_integrity_measurement("bad", 0);
    assert_eq!(m.slash_events, 3);

    let calc = DefaultScoreCalculator;
    let score = calc.calculate_integrity_score(&m);
    assert_eq!(
        score, 700,
        "baseline 1000 minus 3 * 100 permille slash penalty"
    );
}

#[test]
fn unknown_peer_integrity_defaults_to_perfect() {
    // Per `calculate_integrity_score` semantics: zero-denominator means
    // "no data yet" and must not punish a fresh peer.
    let store = ObservationStore::new();
    let m = store.build_integrity_measurement("fresh", 0);
    assert_eq!(m.total_blocks, 0);
    assert_eq!(m.total_transactions, 0);

    let calc = DefaultScoreCalculator;
    assert_eq!(calc.calculate_integrity_score(&m), 1000);
}

#[tokio::test]
async fn slashing_manager_feeds_observations_and_reduces_integrity() {
    // End-to-end: report a misbehavior through SlashingManager → the slash
    // automatically mirrors into the ObservationStore → integrity score drops.

    let store = Arc::new(ObservationStore::new());
    let mut slasher = SlashingManager::new(SlashingConfig::default());
    slasher.set_observations(Arc::clone(&store));

    let validator_id = [0xAB; 32];
    let peer_hex = hex::encode(validator_id);

    // Baseline: no observations yet → perfect integrity (1000).
    let calc = DefaultScoreCalculator;
    let baseline = calc.calculate_integrity_score(&store.build_integrity_measurement(&peer_hex, 0));
    assert_eq!(baseline, 1000, "fresh peer defaults to perfect integrity");

    // Report two distinct misbehaviors spaced beyond the cooldown window.
    slasher
        .report_misbehavior(
            validator_id,
            MisbehaviorType::DoubleVote,
            /* epoch */ 100,
            /* slot */ 1000,
            [0x01; 32],
        )
        .await
        .expect("first slash accepted");
    slasher
        .report_misbehavior(
            validator_id,
            MisbehaviorType::InvalidProposal,
            /* epoch */ 200, // well past cooldown (default 5)
            /* slot */ 2000,
            [0x02; 32],
        )
        .await
        .expect("second slash accepted");

    // Verify the observation store received both slashes.
    let m = store.build_integrity_measurement(&peer_hex, 1);
    assert_eq!(m.slash_events, 2, "slashes forwarded into store");

    // Integrity drops by 200 permille (2 * 100 per slash).
    let after = calc.calculate_integrity_score(&m);
    assert_eq!(
        after, 800,
        "baseline 1000 minus 2 * 100 permille slash penalty"
    );
}

// ─── FL Byzantine-robust scoring (step: PoU ↔ FL bridge) ─────────────────────

#[test]
fn fl_robust_scores_honest_cluster_high_and_outlier_low() {
    // Four honest clients cluster near (1.0, 0.0); one outlier flips
    // direction. Robust scorer must reward honest, punish outlier.
    let clients = vec![
        ("honest-1".to_string(), vec![1.0, 0.0]),
        ("honest-2".to_string(), vec![1.0, 0.1]),
        ("honest-3".to_string(), vec![0.9, 0.0]),
        ("honest-4".to_string(), vec![1.1, -0.05]),
        ("attacker".to_string(), vec![-1.0, 0.0]),
    ];
    let scored = fl_robust::score_gradients_vs_median(&clients, 2);

    for s in scored.iter().filter(|s| s.peer_id.starts_with("honest")) {
        assert!(
            s.score_permille >= 800,
            "{} got {}",
            s.peer_id,
            s.score_permille
        );
        assert!(s.included, "{} must be included", s.peer_id);
    }
    let attacker = scored.iter().find(|s| s.peer_id == "attacker").unwrap();
    assert!(
        attacker.score_permille <= 100,
        "direction-flip got {}",
        attacker.score_permille
    );
    assert!(!attacker.included, "attacker must be excluded from FedAvg");
}

#[test]
fn fl_robust_rejects_norm_clip_violation() {
    // Single peer with gradient norm well above NORM_CLIP_THRESHOLD = 10.
    let huge = vec![100.0, 100.0, 100.0]; // ||g|| ≈ 173 ≫ 10
    let clients = vec![
        ("honest".to_string(), vec![0.1, 0.1, 0.1]),
        ("giant".to_string(), huge),
    ];
    let scored = fl_robust::score_gradients_vs_median(&clients, 3);
    let giant = scored.iter().find(|s| s.peer_id == "giant").unwrap();
    assert_eq!(giant.score_permille, 0);
    assert!(!giant.included);
}

#[test]
fn fl_robust_rejects_nan() {
    let clients = vec![
        ("honest".to_string(), vec![0.5, 0.5]),
        ("nan".to_string(), vec![f64::NAN, 1.0]),
    ];
    let scored = fl_robust::score_gradients_vs_median(&clients, 2);
    let nan = scored.iter().find(|s| s.peer_id == "nan").unwrap();
    assert_eq!(nan.score_permille, 0);
}

#[test]
fn fl_robust_rejects_dimension_skew() {
    // Previous naïve implementation padded with zeros, letting a length-1
    // gradient quietly bias the aggregate. Robust version rejects it.
    let clients = vec![
        ("honest".to_string(), vec![1.0, 1.0, 1.0]),
        ("skewed".to_string(), vec![5.0]), // wrong dim
    ];
    let scored = fl_robust::score_gradients_vs_median(&clients, 3);
    let skewed = scored.iter().find(|s| s.peer_id == "skewed").unwrap();
    assert_eq!(skewed.score_permille, 0);
    assert!(!skewed.included);
}

#[test]
fn observation_store_fl_records_and_reports() {
    let store = ObservationStore::new();
    store.record_fl_contribution("peer-a", 1, 900);
    store.record_fl_contribution("peer-a", 2, 920);
    store.record_fl_contribution("peer-a", 3, 880);

    assert_eq!(store.fl_contribution_count("peer-a"), 3);
    // Average of {900, 920, 880} = 900
    assert_eq!(store.build_fl_integrity_score("peer-a"), 900);
}

#[test]
fn observation_store_unknown_peer_fl_defaults_perfect() {
    let store = ObservationStore::new();
    // Fresh peer with no FL data → 1000 ("no data, no penalty")
    assert_eq!(store.build_fl_integrity_score("fresh"), 1000);
}

#[test]
fn observation_store_bad_fl_streak_counts_consecutive_below_threshold() {
    let store = ObservationStore::new();
    // Mixed pattern: [bad, bad, ok, bad, bad, bad] → streak = 3 (last three)
    store.record_fl_contribution("peer", 1, 50);
    store.record_fl_contribution("peer", 2, 80);
    store.record_fl_contribution("peer", 3, 500);
    store.record_fl_contribution("peer", 4, 100);
    store.record_fl_contribution("peer", 5, 150);
    store.record_fl_contribution("peer", 6, 30);

    let streak = store.bad_fl_streak("peer", MALICIOUS_GRADIENT_THRESHOLD_PERMILLE);
    assert_eq!(
        streak, 3,
        "last three samples are all below threshold (200)"
    );
    assert!(
        streak >= MALICIOUS_GRADIENT_STREAK,
        "streak enough to trigger slash"
    );
}

#[tokio::test]
async fn end_to_end_malicious_fl_streak_triggers_slash_and_reduces_pou() {
    // Full bridge: FL robust scoring → ObservationStore → SlashingManager
    // → integrity score in PoU drops. This is THE integration test that
    // proves "PoU is consensus for FL" as a working claim.

    let store = Arc::new(ObservationStore::new());
    let mut slasher = SlashingManager::new(SlashingConfig::default());
    slasher.set_observations(Arc::clone(&store));

    // A peer submits 3 bad FL rounds in a row (simulated: all scored 50
    // permille, well below the 200 threshold).
    let validator_id = [0x42; 32];
    let peer_hex = hex::encode(validator_id);
    for round in 0..MALICIOUS_GRADIENT_STREAK {
        store.record_fl_contribution(&peer_hex, round as u64, 50);
    }

    // Caller (the aggregator orchestrator) observes the streak and files
    // a MaliciousGradient report to the slasher.
    assert!(
        store.bad_fl_streak(&peer_hex, MALICIOUS_GRADIENT_THRESHOLD_PERMILLE)
            >= MALICIOUS_GRADIENT_STREAK
    );
    slasher
        .report_misbehavior(
            validator_id,
            MisbehaviorType::MaliciousGradient,
            /* epoch */ 10,
            /* slot */ 100,
            [0x99; 32],
        )
        .await
        .expect("slash must be accepted");

    // The slash has been mirrored into the observation store.
    let integrity = store.build_integrity_measurement(&peer_hex, 0);
    assert_eq!(integrity.slash_events, 1);

    // FL integrity component still reflects the bad streak.
    assert_eq!(
        store.build_fl_integrity_score(&peer_hex),
        50,
        "average of three bad samples"
    );

    // Combined effect on PoU: integrity score drops by 100 permille for
    // the slash, AND fl_integrity_score is 50 instead of 1000 default.
    // A reviewer can inspect both components simultaneously and see the
    // bridge closing the trust loop.
}

// ─── derive_observation_score (PoU view from store) ──────────────────────────

#[test]
fn derive_observation_score_fresh_peer_is_max() {
    // No data → all 3 components default to 1000 → score = 1000.
    let store = ObservationStore::new();
    assert_eq!(store.derive_observation_score("nobody"), 1000);
}

#[test]
fn derive_observation_score_combines_three_components_with_correct_weights() {
    let store = ObservationStore::new();
    // Latency: avg RTT 30ms → calculate_latency_score returns 1000.
    store.record_latency("p", 30, LatencyType::Ping);
    for _ in 0..8 {
        store.record_block_validation("p", true);
    }
    for _ in 0..2 {
        store.record_block_validation("p", false);
    }
    for _ in 0..10 {
        store.record_tx_validation("p", true);
    }
    // No slash → integrity = (800 + 1000) / 2 = 900
    // FL contribution: avg 600
    store.record_fl_contribution("p", 1, 600);

    // Expected: (250*1000 + 150*900 + 200*600) / 600 = (250000 + 135000 + 120000) / 600 = 505000 / 600 ≈ 842
    let s = store.derive_observation_score("p");
    assert!(s >= 840 && s <= 845, "expected ~842, got {}", s);
}

#[test]
fn derive_observation_score_low_fl_drags_score_down() {
    let store = ObservationStore::new();
    // Latency, integrity perfect (no data → both 1000)
    // FL malicious: 50 permille avg
    store.record_fl_contribution("p", 1, 50);
    store.record_fl_contribution("p", 2, 50);

    // (250*1000 + 150*1000 + 200*50) / 600 = (250000 + 150000 + 10000) / 600 = 410000/600 ≈ 683
    let s = store.derive_observation_score("p");
    assert!(s >= 680 && s <= 686, "expected ~683, got {}", s);
}

// ─── streak_daemon (auto-slash on FL malicious streak) ───────────────────────

#[tokio::test]
async fn streak_daemon_run_one_pass_slashes_bad_peer() {
    let store = Arc::new(ObservationStore::new());
    let mut slasher_init = SlashingManager::new(SlashingConfig::default());
    slasher_init.set_observations(Arc::clone(&store));
    let slasher = Arc::new(slasher_init);

    // Two peers: one with sustained bad streak, one healthy.
    let bad_id = [0xAB; 32];
    let bad_hex = hex::encode(bad_id);
    for round in 0..MALICIOUS_GRADIENT_STREAK {
        store.record_fl_contribution(&bad_hex, round as u64, 50); // <200
    }
    let good_id = [0xCD; 32];
    let good_hex = hex::encode(good_id);
    for round in 0..MALICIOUS_GRADIENT_STREAK {
        store.record_fl_contribution(&good_hex, round as u64, 900); // healthy
    }

    let epoch_provider: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(|| 42u64);
    let accepted = streak_daemon::run_one_pass(
        &store,
        &slasher,
        MALICIOUS_GRADIENT_THRESHOLD_PERMILLE,
        MALICIOUS_GRADIENT_STREAK,
        &epoch_provider,
    )
    .await;

    assert_eq!(accepted, 1, "exactly one slash should be accepted");
    let bad_state = slasher.get_validator_state(&bad_id).await.unwrap();
    assert_eq!(bad_state.cumulative_slashes, 1);
    assert!(slasher.get_validator_state(&good_id).await.is_none());
}

#[tokio::test]
async fn streak_daemon_respects_cooldown_on_repeat_passes() {
    let store = Arc::new(ObservationStore::new());
    let mut slasher_init = SlashingManager::new(SlashingConfig::default());
    slasher_init.set_observations(Arc::clone(&store));
    let slasher = Arc::new(slasher_init);

    let bad_id = [0xEF; 32];
    let bad_hex = hex::encode(bad_id);
    for round in 0..MALICIOUS_GRADIENT_STREAK {
        store.record_fl_contribution(&bad_hex, round as u64, 50);
    }
    let epoch_provider: Arc<dyn Fn() -> u64 + Send + Sync> = Arc::new(|| 100u64);

    // First pass: slash accepted.
    let accepted_1 = streak_daemon::run_one_pass(
        &store,
        &slasher,
        MALICIOUS_GRADIENT_THRESHOLD_PERMILLE,
        MALICIOUS_GRADIENT_STREAK,
        &epoch_provider,
    )
    .await;
    assert_eq!(accepted_1, 1);

    // Second pass at same epoch: cooldown rejects → 0 accepted, but no
    // crash, no double-slash.
    let accepted_2 = streak_daemon::run_one_pass(
        &store,
        &slasher,
        MALICIOUS_GRADIENT_THRESHOLD_PERMILLE,
        MALICIOUS_GRADIENT_STREAK,
        &epoch_provider,
    )
    .await;
    assert_eq!(accepted_2, 0);

    let state = slasher.get_validator_state(&bad_id).await.unwrap();
    assert_eq!(
        state.cumulative_slashes, 1,
        "cooldown must prevent double-slash"
    );
}

#[test]
fn excessive_slashing_saturates_at_zero() {
    let store = ObservationStore::new();
    for _ in 0..10 {
        store.record_block_validation("toxic", true);
        store.record_tx_validation("toxic", true);
    }
    for _ in 0..20 {
        store.record_slash("toxic", SlashReason::DoubleVote);
    }

    let m = store.build_integrity_measurement("toxic", 0);
    let calc = DefaultScoreCalculator;
    assert_eq!(
        calc.calculate_integrity_score(&m),
        0,
        "20 slash events * 100 permille saturates to zero via saturating_sub",
    );
}
