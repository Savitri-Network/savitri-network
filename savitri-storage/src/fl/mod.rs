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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fl_storage() -> Result<()> {
        let mut fl_storage = FlStorage::new()?;

        // Test model storage
        let model = ModelData {
            model_id: 1,
            version: 1,
            data: vec![1, 2, 3, 4, 5],
        };
        fl_storage.put_model(model.clone())?;

        let retrieved = fl_storage.get_model(1)?;
        if let Some(model) = retrieved {
            assert_eq!(model.model_id, 1);
        } else {
            anyhow::bail!("Model not found");
        }

        // Test round storage
        let round = RoundState {
            round_id: 1,
            status: "active".to_string(),
            participants: vec![[1; 32], [2; 32]],
        };
        fl_storage.put_round(round.clone())?;

        let retrieved_round = fl_storage.get_round(1)?;
        if let Some(round) = retrieved_round {
            assert_eq!(round.status, "active");
        } else {
            anyhow::bail!("Round not found");
        }

        // Test retention
        let config = FlRetentionConfig {
            max_models: 0,
            max_rounds: 0,
        };
        let outcome = fl_storage.apply_retention(config)?;
        assert_eq!(outcome.models_removed, 1);
        assert_eq!(outcome.rounds_removed, 1);

        Ok(())
    }
}
