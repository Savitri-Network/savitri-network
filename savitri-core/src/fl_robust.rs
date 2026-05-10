//! Byzantine-robust scoring for Federated Learning gradient updates.
//!
//! Pure math — lives in `savitri-core` so it can be shared between
//! `savitri-mempool` (gradient aggregation pipeline) and
//! `savitri-consensus` (PoU `ObservationStore` feeds). Downstream
//! consumers re-export from their own modules.
//!
//! ## Algorithm overview
//!
//! 1. **Dimension check.** Gradients whose length differs from
//!    `expected_dim` score 0. Padding with zeros was the legacy
//!    behaviour and biased the aggregate toward the origin.
//!
//! 2. **NaN / Inf check.** Any non-finite coordinate fails the
//!    gradient.
//!
//! 3. **Norm clipping.** `l2_norm(g) > NORM_CLIP_THRESHOLD` fails.
//!    Blocks "giant gradient" model-poisoning.
//!
//!    moving.
//!
//!    permille `[0, 1000]`.
//!
//! 6. **Zero-median edge case.** Falls back to norm-based scoring so
//!    the zero vector.

/// Maximum L2 norm allowed for a gradient update.
pub const NORM_CLIP_THRESHOLD: f64 = 10.0;

/// Default streak length before a sustained low-score peer is
/// forwarded to `SlashingManager` as `MaliciousGradient`.
pub const MALICIOUS_GRADIENT_STREAK: usize = 3;

/// Default "bad score" cutoff (permille).
pub const MALICIOUS_GRADIENT_THRESHOLD_PERMILLE: u16 = 200;

/// Scored outcome for one client's gradient in one round.
#[derive(Debug, Clone)]
pub struct ScoredGradient {
    pub peer_id: String,
    pub score_permille: u16,
    pub included: bool,
}

/// baseline. See module docs for the gate chain.
pub fn score_gradients_vs_median(
    clients: &[(String, Vec<f64>)],
    expected_dim: usize,
) -> Vec<ScoredGradient> {
    if clients.is_empty() {
        return Vec::new();
    }

    let per_client_norm: Vec<Option<f64>> = clients
        .iter()
        .map(|(_, g)| {
            if g.len() != expected_dim {
                return None;
            }
            if !g.iter().all(|v| v.is_finite()) {
                return None;
            }
            let n = l2_norm(g);
            if n > NORM_CLIP_THRESHOLD {
                return None;
            }
            Some(n)
        })
        .collect();

    let survivors: Vec<&Vec<f64>> = clients
        .iter()
        .zip(per_client_norm.iter())
        .filter_map(|((_, g), n)| n.as_ref().map(|_| g))
        .collect();

    if survivors.is_empty() {
        return clients
            .iter()
            .map(|(peer, _)| ScoredGradient {
                peer_id: peer.clone(),
                score_permille: 0,
                included: false,
            })
            .collect();
    }

    let median = coordinate_wise_median(&survivors, expected_dim);
    let median_norm = l2_norm(&median);

    if survivors.len() == 1 {
        return clients
            .iter()
            .zip(per_client_norm.iter())
            .map(|((peer, _), gate)| ScoredGradient {
                peer_id: peer.clone(),
                score_permille: if gate.is_some() { 1000 } else { 0 },
                included: gate.is_some(),
            })
            .collect();
    }

    clients
        .iter()
        .zip(per_client_norm.iter())
        .map(|((peer, g), gate)| {
            let Some(client_norm) = gate else {
                return ScoredGradient {
                    peer_id: peer.clone(),
                    score_permille: 0,
                    included: false,
                };
            };
            let score = if median_norm == 0.0 {
                let scaled = (1.0 - client_norm / NORM_CLIP_THRESHOLD).max(0.0);
                (scaled * 1000.0).round() as u16
            } else if *client_norm == 0.0 {
                500
            } else {
                let sim = dot(g, &median) / (client_norm * median_norm);
                let permille = ((sim + 1.0) * 500.0).round();
                permille.clamp(0.0, 1000.0) as u16
            };
            ScoredGradient {
                peer_id: peer.clone(),
                score_permille: score,
                included: score >= MALICIOUS_GRADIENT_THRESHOLD_PERMILLE,
            }
        })
        .collect()
}

fn l2_norm(v: &[f64]) -> f64 {
    v.iter().map(|x| x * x).sum::<f64>().sqrt()
}

fn dot(a: &[f64], b: &[f64]) -> f64 {
    a.iter().zip(b.iter()).map(|(x, y)| x * y).sum()
}

/// middle value is chosen — the result itself is one of the input
pub fn coordinate_wise_median(survivors: &[&Vec<f64>], dim: usize) -> Vec<f64> {
    let mut out = vec![0.0_f64; dim];
    if survivors.is_empty() {
        return out;
    }
    let mut column = Vec::with_capacity(survivors.len());
    for i in 0..dim {
        column.clear();
        for g in survivors {
            column.push(g[i]);
        }
        column.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = (column.len() - 1) / 2;
        out[i] = column[mid];
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str, g: Vec<f64>) -> (String, Vec<f64>) {
        (id.to_string(), g)
    }

    #[test]
    fn dimension_mismatch_fails() {
        let out = score_gradients_vs_median(&[peer("a", vec![1.0, 2.0, 3.0])], 4);
        assert_eq!(out[0].score_permille, 0);
        assert!(!out[0].included);
    }

    #[test]
    fn nan_fails() {
        let out = score_gradients_vs_median(&[peer("a", vec![1.0, f64::NAN])], 2);
        assert_eq!(out[0].score_permille, 0);
    }

    #[test]
    fn giant_norm_fails() {
        let out = score_gradients_vs_median(&[peer("a", vec![1000.0, 0.0])], 2);
        assert_eq!(out[0].score_permille, 0);
    }

    #[test]
    fn honest_majority_dominates_one_outlier() {
        let clients = vec![
            peer("h1", vec![1.0, 0.1]),
            peer("h2", vec![1.0, 0.0]),
            peer("h3", vec![0.9, 0.0]),
            peer("h4", vec![1.1, 0.0]),
            peer("attack", vec![0.0, 5.0]),
        ];
        let scored = score_gradients_vs_median(&clients, 2);
        for s in scored.iter().filter(|s| s.peer_id != "attack") {
            assert!(
                s.score_permille >= 800,
                "{} got {}",
                s.peer_id,
                s.score_permille
            );
            assert!(s.included);
        }
        let attacker = scored.iter().find(|s| s.peer_id == "attack").unwrap();
        assert!(attacker.score_permille < 600);
    }

    #[test]
    fn opposite_direction_scores_near_zero() {
        let clients = vec![
            peer("h1", vec![1.0, 0.0]),
            peer("h2", vec![1.0, 0.0]),
            peer("h3", vec![1.0, 0.0]),
            peer("flip", vec![-1.0, 0.0]),
        ];
        let scored = score_gradients_vs_median(&clients, 2);
        let flip = scored.iter().find(|s| s.peer_id == "flip").unwrap();
        assert!(flip.score_permille <= 50);
        assert!(!flip.included);
    }

    #[test]
    fn single_client_round_scores_max() {
        let out = score_gradients_vs_median(&[peer("solo", vec![0.5, 0.5])], 2);
        assert_eq!(out[0].score_permille, 1000);
        assert!(out[0].included);
    }

    #[test]
    fn empty_round_returns_empty() {
        assert!(score_gradients_vs_median(&[], 4).is_empty());
    }

    #[test]
    fn coordinate_median_resists_single_outlier() {
        let g1 = vec![1.0, 1.0];
        let g2 = vec![1.0, 1.0];
        let g3 = vec![1.0, 1.0];
        let g4 = vec![9.0, 9.0];
        let survivors: Vec<&Vec<f64>> = vec![&g1, &g2, &g3, &g4];
        assert_eq!(coordinate_wise_median(&survivors, 2), vec![1.0, 1.0]);
    }
}
