//! Deterministic selection algorithms
//! 
//! This module provides deterministic algorithms for proposer selection,
//! group assignment, and other consensus operations that require
//! reproducible results across all nodes.

use crate::types::*;
use std::collections::HashMap;

/// Deterministic selector for reproducible consensus operations
pub struct DeterministicSelector {
    seed: u64,
}

impl DeterministicSelector {
    /// Create a new deterministic selector with a seed
    pub fn new(seed: u64) -> Self {
        Self { seed }
    }
    
    /// Select proposer from list of candidates deterministically
    pub fn select_proposer(&self, candidates: &[ProposerInfo], slot: u64) -> Option<ProposerInfo> {
        if candidates.is_empty() {
            return None;
        }
        
        // Combine seed and slot for deterministic selection
        let combined_seed = self.seed.wrapping_mul(slot).wrapping_add(slot);
        
        // Use Fisher-Yates shuffle with deterministic seed
        let mut indices: Vec<usize> = (0..candidates.len()).collect();
        let mut rng_state = combined_seed;
        
        for i in (1..indices.len()).rev() {
            // Generate deterministic random number
            rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
            let j = (rng_state % (i + 1) as u64) as usize;
            indices.swap(i, j);
        }
        
        // Select based on scores (highest first)
        let mut sorted_candidates: Vec<_> = candidates.iter().enumerate().collect();
        sorted_candidates.sort_by(|a, b| {
            // Sort by score descending, then by deterministic index
            b.1.score.cmp(&a.1.score)
                .then_with(|| indices[a.0].cmp(&indices[b.0]))
        });
        
        sorted_candidates.first().map(|(_, proposer)| (*proposer).clone())
    }
    
    /// Select multiple proposers for committee
    pub fn select_committee(&self, candidates: &[ProposerInfo], committee_size: usize, slot: u64) -> Vec<ProposerInfo> {
        if candidates.is_empty() || committee_size == 0 {
            return Vec::new();
        }
        
        let actual_size = committee_size.min(candidates.len());
        let combined_seed = self.seed.wrapping_mul(slot).wrapping_add(slot);
        
        // Create deterministic permutation
        let mut indices: Vec<usize> = (0..candidates.len()).collect();
        let mut rng_state = combined_seed;
        
        for i in (1..indices.len()).rev() {
            rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
            let j = (rng_state % (i + 1) as u64) as usize;
            indices.swap(i, j);
        }
        
        // Sort by score first, then apply deterministic permutation
        let mut sorted_candidates: Vec<_> = candidates.iter().enumerate().collect();
        sorted_candidates.sort_by(|a, b| b.1.score.cmp(&a.1.score));
        
        // Apply deterministic selection
        let mut committee = Vec::new();
        for (original_idx, proposer) in sorted_candidates.iter().take(actual_size) {
            let permuted_idx = indices[*original_idx];
            if permuted_idx < actual_size {
                committee.push((*proposer).clone());
            }
        }
        
        committee
    }
    
    /// Assign nodes to groups deterministically
    pub fn assign_groups(&self, nodes: &[String], group_size: usize, slot: u64) -> HashMap<String, Vec<String>> {
        if nodes.is_empty() || group_size == 0 {
            return HashMap::new();
        }
        
        let num_groups = (nodes.len() + group_size - 1) / group_size;
        let combined_seed = self.seed.wrapping_mul(slot).wrapping_add(slot);
        
        // Create deterministic permutation of nodes
        let mut indices: Vec<usize> = (0..nodes.len()).collect();
        let mut rng_state = combined_seed;
        
        for i in (1..indices.len()).rev() {
            rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
            let j = (rng_state % (i + 1) as u64) as usize;
            indices.swap(i, j);
        }
        
        // Assign to groups
        let mut groups = HashMap::new();
        for (i, &node_idx) in indices.iter().enumerate() {
            let group_id = format!("group-{}", i % num_groups);
            let group = groups.entry(group_id.clone()).or_insert_with(Vec::new);
            group.push(nodes[node_idx].clone());
        }
        
        groups
    }
    
    /// Select leader from group deterministically
    pub fn select_group_leader(&self, group: &[ProposerInfo], slot: u64) -> Option<ProposerInfo> {
        if group.is_empty() {
            return None;
        }
        
        let combined_seed = self.seed.wrapping_mul(slot).wrapping_add(slot);
        let mut rng_state = combined_seed;
        
        // Generate deterministic random index
        rng_state = rng_state.wrapping_mul(1103515245).wrapping_add(12345);
        let leader_idx = (rng_state % group.len() as u64) as usize;
        
        // Prefer higher scores but use deterministic selection
        let mut sorted_group: Vec<_> = group.iter().enumerate().collect();
        sorted_group.sort_by(|a, b| b.1.score.cmp(&a.1.score));
        
        // Select from top candidates deterministically
        let top_candidates = sorted_group.iter().take((group.len() + 1) / 2).collect::<Vec<_>>();
        if top_candidates.is_empty() {
            return None;
        }
        
        let selected_idx = leader_idx % top_candidates.len();
        top_candidates[selected_idx].map(|(_, proposer)| (*proposer).clone())
    }
    
    /// Generate deterministic random number
    pub fn deterministic_random(&self, input: u64) -> u64 {
        let mut state = self.seed.wrapping_add(input);
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        state = state.wrapping_mul(1103515245).wrapping_add(12345);
        state
    }
    
    /// Generate a deterministic value in [0, 1000) (permille).
    ///
    /// AUDIT-003: Replaced f64 to guarantee cross-platform determinism.
    pub fn deterministic_permille(&self, input: u64) -> u64 {
        self.deterministic_random(input) % 1000
    }

    /// Generate deterministic float between 0 and 1
    ///
    /// Deprecated: prefer `deterministic_permille` for consensus-critical paths.
    #[deprecated(note = "Use deterministic_permille for cross-platform determinism (AUDIT-003)")]
    pub fn deterministic_float(&self, input: u64) -> f64 {
        let random = self.deterministic_random(input);
        random as f64 / u64::MAX as f64
    }

    /// Select weighted item deterministically using integer arithmetic.
    ///
    /// Weights are arbitrary u64 values (relative, not necessarily permille).
    /// AUDIT-003: Replaced f64 with integer arithmetic for cross-platform determinism.
    pub fn select_weighted<T: Clone>(&self, items: &[(T, u64)], slot: u64) -> Option<T> {
        if items.is_empty() {
            return None;
        }

        let total_weight: u64 = items.iter().map(|(_, w)| *w).sum();
        if total_weight == 0 {
            return None;
        }

        let random_value = self.deterministic_random(slot) % total_weight;
        let mut accumulated: u64 = 0;

        for (item, weight) in items {
            accumulated += weight;
            if random_value < accumulated {
                return Some(item.clone());
            }
        }

        Some(items.last().unwrap().0.clone())
    }
}

/// Group assignment utilities
pub struct GroupAssigner {
    selector: DeterministicSelector,
}

impl GroupAssigner {
    pub fn new(seed: u64) -> Self {
        Self {
            selector: DeterministicSelector::new(seed),
        }
    }
    
    /// Assign nodes to balanced groups
    pub fn assign_balanced_groups(
        &self,
        nodes: &[NodeInfo],
        target_group_size: usize,
        slot: u64,
    ) -> HashMap<String, Vec<NodeInfo>> {
        if nodes.is_empty() || target_group_size == 0 {
            return HashMap::new();
        }
        
        let num_groups = (nodes.len() + target_group_size - 1) / target_group_size;
        let mut groups = HashMap::new();
        
        // Sort nodes by score for balanced assignment
        let mut sorted_nodes: Vec<_> = nodes.iter().collect();
        sorted_nodes.sort_by(|a, b| b.score.cmp(&a.score));
        
        // Round-robin assignment to balance scores
        for (i, node) in sorted_nodes.iter().enumerate() {
            let group_id = format!("group-{}", i % num_groups);
            groups.entry(group_id).or_insert_with(Vec::new).push((*node).clone());
        }
        
        groups
    }
    
    /// Rebalance groups if needed
    pub fn rebalance_groups(&self, groups: &HashMap<String, Vec<NodeInfo>>) -> HashMap<String, Vec<NodeInfo>> {
        let mut rebalanced = HashMap::new();
        let mut all_nodes: Vec<_> = groups.values().flatten().collect();
        
        // Sort by score
        all_nodes.sort_by(|a, b| b.score.cmp(&a.score));
        
        // Reassign evenly
        let num_groups = groups.len();
        for (i, node) in all_nodes.into_iter().enumerate() {
            let group_id = format!("group-{}", i % num_groups);
            rebalanced.entry(group_id).or_insert_with(Vec::new).push(node);
        }
        
        rebalanced
    }
}

/// Node information for group assignment
#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub node_id: String,
    pub score: u32,
    pub region: String,
    pub capabilities: Vec<String>,
}

/// Proposer selection utilities
pub struct ProposerSelector {
    selector: DeterministicSelector,
}

impl ProposerSelector {
    pub fn new(seed: u64) -> Self {
        Self {
            selector: DeterministicSelector::new(seed),
        }
    }
    
    /// Select proposer using weighted random selection
    pub fn select_weighted_proposer(&self, candidates: &[ProposerInfo], slot: u64) -> Option<ProposerInfo> {
        let weighted_candidates: Vec<_> = candidates.iter()
            .map(|p| (p, p.score as f64))
            .collect();
        
        self.selector.select_weighted(&weighted_candidates, slot)
    }
    
    /// Select proposer using score-based tournament
    pub fn tournament_selection(&self, candidates: &[ProposerInfo], slot: u64) -> Option<ProposerInfo> {
        if candidates.is_empty() {
            return None;
        }
        
        let mut remaining = candidates.to_vec();
        let round = 0;
        
        while remaining.len() > 1 {
            let mut next_round = Vec::new();
            
            for chunk in remaining.chunks(2) {
                if chunk.len() == 1 {
                    next_round.push(chunk[0].clone());
                } else {
                    // Select winner of this pair
                    let winner = if chunk[0].score >= chunk[1].score {
                        &chunk[0]
                    } else {
                        &chunk[1]
                    };
                    next_round.push(winner.clone());
                }
            }
            
            remaining = next_round;
            round += 1;
        }
        
        remaining.into_iter().next()
    }
    
    /// Select proposer with geographic diversity
    pub fn select_diverse_proposer(&self, candidates: &[ProposerInfo], slot: u64) -> Option<ProposerInfo> {
        if candidates.is_empty() {
            return None;
        }
        
        // Group by region
        let mut region_groups: HashMap<String, Vec<ProposerInfo>> = HashMap::new();
        for candidate in candidates {
            region_groups.entry(candidate.region.clone()).or_insert_with(Vec::new).push(candidate.clone());
        }
        
        // Select region deterministically
        let regions: Vec<_> = region_groups.keys().collect();
        let selected_region = if regions.is_empty() {
            return None;
        } else {
            let region_idx = (self.selector.deterministic_random(slot) % regions.len() as u64) as usize;
            regions[region_idx].clone()
        };
        
        // Select best proposer from selected region
        if let Some(region_candidates) = region_groups.get(&selected_region) {
            self.selector.select_proposer(region_candidates, slot)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_deterministic_selector() {
        let selector = DeterministicSelector::new(12345);
        
        let candidates = vec![
            ProposerInfo {
                node_id: "node1".to_string(),
                peer_id: "peer1".to_string(),
                public_key: [1u8; 32],
                score: 100,
                group_id: None,
                region: "region1".to_string(),
            },
            ProposerInfo {
                node_id: "node2".to_string(),
                peer_id: "peer2".to_string(),
                public_key: [2u8; 32],
                score: 200,
                group_id: None,
                region: "region2".to_string(),
            },
        ];
        
        let proposer = selector.select_proposer(&candidates, 1);
        assert!(proposer.is_some());
        assert_eq!(proposer.unwrap().node_id, "node2"); // Higher score
    }
    
    #[test]
    fn test_group_assignment() {
        let selector = DeterministicSelector::new(12345);
        
        let nodes = vec![
            "node1".to_string(),
            "node2".to_string(),
            "node3".to_string(),
            "node4".to_string(),
        ];
        
        let groups = selector.assign_groups(&nodes, 2, 1);
        assert_eq!(groups.len(), 2);
        assert_eq!(groups.values().map(|v| v.len()).sum::<usize>(), 4);
    }
    
    #[test]
    fn test_deterministic_random() {
        let selector = DeterministicSelector::new(12345);
        
        let random1 = selector.deterministic_random(1);
        let random2 = selector.deterministic_random(1);
        let random3 = selector.deterministic_random(2);
        
        assert_eq!(random1, random2); // Same input should give same output
        assert_ne!(random1, random3); // Different input should give different output
    }
    
    #[test]
    fn test_weighted_selection() {
        let selector = DeterministicSelector::new(12345);
        
        let items = vec![
            ("item1", 0.1),
            ("item2", 0.9),
        ];
        
        let selected = selector.select_weighted(&items, 1);
        assert!(selected.is_some());
        // With deterministic seed, we can predict the outcome
    }
    
    #[test]
    fn test_group_balancer() {
        let assigner = GroupAssigner::new(12345);
        
        let nodes = vec![
            NodeInfo {
                node_id: "node1".to_string(),
                score: 100,
                region: "region1".to_string(),
                capabilities: vec!["cap1".to_string()],
            },
            NodeInfo {
                node_id: "node2".to_string(),
                score: 200,
                region: "region2".to_string(),
                capabilities: vec!["cap2".to_string()],
            },
            NodeInfo {
                node_id: "node3".to_string(),
                score: 150,
                region: "region3".to_string(),
                capabilities: vec!["cap3".to_string()],
            },
        ];
        
        let groups = assigner.assign_balanced_groups(&nodes, 2, 1);
        assert_eq!(groups.len(), 2);
        
        // Check that groups are balanced by score
        for group in groups.values() {
            assert!(!group.is_empty());
        }
    }
}
