//! Federated Learning storage module
//!
//! This is a minimal implementation for testing purposes.

use anyhow::Result;
use std::collections::HashMap;

/// FL Storage interface
pub struct FlStorage {
    models: HashMap<u64, ModelData>,
    rounds: HashMap<u64, RoundState>,
}

/// Model data
#[derive(Debug, Clone)]
pub struct ModelData {
    pub model_id: u64,
    pub version: u32,
    pub data: Vec<u8>,
}

/// Round state
#[derive(Debug, Clone)]
pub struct RoundState {
    pub round_id: u64,
    pub status: String,
    pub participants: Vec<[u8; 32]>,
}

/// FL retention configuration
#[derive(Debug, Clone)]
pub struct FlRetentionConfig {
    pub max_models: usize,
    pub max_rounds: usize,
}

/// FL retention outcome
#[derive(Debug)]
pub struct FlRetentionOutcome {
    pub models_removed: usize,
    pub rounds_removed: usize,
}

impl FlStorage {
    /// Create new FL storage
    pub fn new() -> Result<Self> {
        Ok(Self {
            models: HashMap::new(),
            rounds: HashMap::new(),
        })
    }

    /// Store model
    pub fn put_model(&mut self, model: ModelData) -> Result<()> {
        self.models.insert(model.model_id, model);
        Ok(())
    }

    /// Get model
    pub fn get_model(&self, model_id: u64) -> Result<Option<ModelData>> {
        Ok(self.models.get(&model_id).cloned())
    }

    /// Store round
    pub fn put_round(&mut self, round: RoundState) -> Result<()> {
        self.rounds.insert(round.round_id, round);
        Ok(())
    }

    /// Get round
    pub fn get_round(&self, round_id: u64) -> Result<Option<RoundState>> {
        Ok(self.rounds.get(&round_id).cloned())
    }

    /// Apply retention policy
    pub fn apply_retention(&mut self, config: FlRetentionConfig) -> Result<FlRetentionOutcome> {
        let mut outcome = FlRetentionOutcome {
            models_removed: 0,
            rounds_removed: 0,
        };

        // Remove excess models
        if self.models.len() > config.max_models {
            let to_remove = self.models.len() - config.max_models;
            let keys_to_remove: Vec<_> = self.models.keys().take(to_remove).cloned().collect();
            for key in keys_to_remove {
                self.models.remove(&key);
                outcome.models_removed += 1;
            }
        }

        // Remove excess rounds
        if self.rounds.len() > config.max_rounds {
            let to_remove = self.rounds.len() - config.max_rounds;
            let keys_to_remove: Vec<_> = self.rounds.keys().take(to_remove).cloned().collect();
            for key in keys_to_remove {
                self.rounds.remove(&key);
                outcome.rounds_removed += 1;
            }
        }

        Ok(outcome)
    }
}

impl Default for FlRetentionConfig {
    fn default() -> Self {
        Self {
            max_models: 1000,
            max_rounds: 100,
        }
    }
}
