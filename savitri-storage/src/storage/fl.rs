//! FL Storage Module for Federated Learning
//!
//! Complete storage implementation for Federated Learning system including:
//! - FL model management and versioning
//! - FL round lifecycle management
//! - FL update and contribution tracking
//! - FL reward distribution system
//! - FL policy management and enforcement
//! - Comprehensive FL statistics and monitoring

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Column family for FL models
pub const CF_FL_MODELS: &str = "fl_models";
/// Column family for FL rounds
pub const CF_FL_ROUNDS: &str = "fl_rounds";
/// Column family for FL updates
pub const CF_FL_UPDATES: &str = "fl_updates";
/// Column family for FL contributions
pub const CF_FL_CONTRIBUTIONS: &str = "fl_contributions";
/// Column family for FL rewards
pub const CF_FL_REWARDS: &str = "fl_rewards";

/// FL Policy configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlPolicy {
    pub fee_treasury_bps: u16,
    pub max_models: u32,
    pub whitelist_aggregators: Vec<Vec<u8>>,
    pub min_contributions_per_round: u32,
    pub max_contributions_per_round: u32,
    pub reward_per_contribution: u128,
    pub round_duration_blocks: u64,
    pub model_approval_required: bool,
}

impl Default for FlPolicy {
    fn default() -> Self {
        Self {
            fee_treasury_bps: 100,
            max_models: 100,
            whitelist_aggregators: Vec::new(),
            min_contributions_per_round: 5,
            max_contributions_per_round: 100,
            reward_per_contribution: 1000000, // 1 SAV token
            round_duration_blocks: 10080,     // ~1 week
            model_approval_required: true,
        }
    }
}

/// FL Model information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlModel {
    pub model_id: [u8; 32],
    pub owner: [u8; 32],
    pub name: String,
    pub description: String,
    pub version: String,
    pub model_hash: [u8; 32],
    pub model_size: u64,
    pub created_at: u64,
    pub updated_at: u64,
    pub status: ModelStatus,
    pub metadata: Vec<u8>,
    pub training_data_hash: Option<[u8; 32]>,
    pub accuracy_metrics: Option<ModelMetrics>,
}

/// Model status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ModelStatus {
    Proposed,
    Approved,
    Active,
    Deprecated,
    Rejected,
}

/// Model performance metrics
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ModelMetrics {
    pub accuracy: f64,
    pub precision: f64,
    pub recall: f64,
    pub f1_score: f64,
    pub loss: f64,
    pub updated_at: u64,
}

/// FL Round information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlRound {
    pub round_id: u64,
    pub model_id: [u8; 32],
    pub round_number: u32,
    pub status: RoundStatus,
    pub created_at: u64,
    pub started_at: Option<u64>,
    pub ended_at: Option<u64>,
    pub target_contributions: u32,
    pub received_contributions: u32,
    pub aggregation_result: Option<AggregationResult>,
    pub reward_pool: u128,
    pub distributed_rewards: u128,
}

/// Round status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RoundStatus {
    Proposed,
    Open,
    Active,
    Closed,
    Finalized,
    Aborted,
}

/// Aggregation result for a round
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AggregationResult {
    pub aggregated_model_hash: [u8; 32],
    pub contributor_count: u32,
    pub aggregation_timestamp: u64,
    pub aggregator: [u8; 32],
    pub quality_score: f64,
    pub improvement_metrics: Option<ModelMetrics>,
}

/// FL Update/Contribution information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct FlUpdate {
    pub update_id: [u8; 32],
    pub round_id: u64,
    pub model_id: [u8; 32],
    pub contributor: [u8; 32],
    pub update_hash: [u8; 32],
    pub update_data_hash: [u8; 32],
    pub update_size: u64,
    pub submitted_at: u64,
    pub status: UpdateStatus,
    pub quality_score: Option<f64>,
    pub validation_result: Option<ValidationResult>,
}

/// Update status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum UpdateStatus {
    Submitted,
    Validating,
    Accepted,
    Rejected,
    Included,
}

/// Validation result for an update
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ValidationResult {
    pub validator: [u8; 32],
    pub validated_at: u64,
    pub is_valid: bool,
    pub validation_score: f64,
    pub feedback: String,
}

/// FL Reward information
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlReward {
    pub reward_id: [u8; 32],
    pub round_id: u64,
    pub recipient: [u8; 32],
    pub reward_type: RewardType,
    pub amount: u128,
    pub distributed_at: u64,
    pub transaction_hash: Option<[u8; 32]>,
    pub reason: String,
}

/// Reward type
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RewardType {
    Contribution,
    Validation,
    Aggregation,
    Bonus,
}

/// FL storage interface
pub struct FlStorage;

impl FlStorage {
    /// Create a new FL model
    pub fn create_model(
        storage: &crate::Storage,
        model_id: [u8; 32],
        owner: [u8; 32],
        name: String,
        description: String,
        version: String,
        model_hash: [u8; 32],
        model_size: u64,
        metadata: Vec<u8>,
        current_time: u64,
    ) -> Result<()> {
        // Check if model already exists
        if Self::model_exists(storage, &model_id)? {
            return Err(anyhow!("Model already exists"));
        }

        // Check policy constraints
        let policy = Self::get_fl_policy(storage)?;
        if Self::get_active_model_count(storage)? >= policy.max_models as usize {
            return Err(anyhow!("Maximum number of models reached"));
        }

        let model = FlModel {
            model_id,
            owner,
            name,
            description,
            version,
            model_hash,
            model_size,
            created_at: current_time,
            updated_at: current_time,
            status: if policy.model_approval_required {
                ModelStatus::Proposed
            } else {
                ModelStatus::Active
            },
            metadata,
            training_data_hash: None,
            accuracy_metrics: None,
        };

        // Store model
        let model_data = bincode::serialize(&model)?;
        let key = format!("model:{}", hex::encode(&model.model_id));
        storage.put(key.as_bytes(), &model_data)?;

        // Update model count
        Self::increment_model_count(storage)?;

        Ok(())
    }

    /// Check if model exists
    pub fn model_exists(storage: &crate::Storage, model_id: &[u8; 32]) -> Result<bool> {
        let key = format!("model:{}", hex::encode(model_id));
        storage
            .get(key.as_bytes())
            .map(|opt: Option<Vec<u8>>| opt.is_some())
    }

    /// Get FL model by ID
    pub fn get_model(storage: &crate::Storage, model_id: &[u8; 32]) -> Result<Option<FlModel>> {
        let key = format!("model:{}", hex::encode(model_id));
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let model: FlModel = crate::safe_deserialize(&data)?;
                Ok(Some(model))
            }
            None => Ok(None),
        }
    }

    /// Update model status
    pub fn update_model_status(
        storage: &crate::Storage,
        model_id: &[u8; 32],
        new_status: ModelStatus,
    ) -> Result<()> {
        let mut model = match Self::get_model(storage, model_id)? {
            Some(m) => m,
            None => return Err(anyhow!("Model not found")),
        };

        model.status = new_status;
        model.updated_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        let model_data = bincode::serialize(&model)?;
        let key = format!("model:{}", hex::encode(model_id));
        storage.put(key.as_bytes(), &model_data)?;

        Ok(())
    }

    /// Update model metrics
    pub fn update_model_metrics(
        storage: &crate::Storage,
        model_id: &[u8; 32],
        metrics: ModelMetrics,
    ) -> Result<()> {
        let mut model = match Self::get_model(storage, model_id)? {
            Some(m) => m,
            None => return Err(anyhow!("Model not found")),
        };

        model.accuracy_metrics = Some(metrics.clone());
        model.updated_at = metrics.updated_at;

        let model_data = bincode::serialize(&model)?;
        let key = format!("model:{}", hex::encode(model_id));
        storage.put(key.as_bytes(), &model_data)?;

        Ok(())
    }

    /// Get all models
    pub fn get_all_models(storage: &crate::Storage) -> Result<Vec<FlModel>> {
        let mut models = Vec::new();
        let prefix = "model:";

        let iter = storage.iterator_cf(CF_FL_MODELS)?;
        for item in iter {
            let (key, value): (Vec<u8>, Vec<u8>) = item?;
            let key_str = String::from_utf8_lossy(&key);
            if key_str.starts_with(prefix) {
                if let Ok(model) = crate::safe_deserialize::<FlModel>(&value) {
                    models.push(model);
                }
            }
        }

        Ok(models)
    }

    /// Get active models only
    pub fn get_active_models(storage: &crate::Storage) -> Result<Vec<FlModel>> {
        let all_models = Self::get_all_models(storage)?;
        Ok(all_models
            .into_iter()
            .filter(|m| m.status == ModelStatus::Active)
            .collect())
    }

    /// Get active model count
    pub fn get_active_model_count(storage: &crate::Storage) -> Result<usize> {
        Self::get_active_models(storage).map(|models| models.len())
    }

    /// Increment model count
    fn increment_model_count(storage: &crate::Storage) -> Result<()> {
        let count_key = "model_count";
        let current_count = match storage.get(count_key.as_bytes())? {
            Some(data) if data.len() >= 8 => {
                u64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8]))
            }
            _ => 0,
        };

        let new_count = current_count + 1;
        storage.put(count_key.as_bytes(), &new_count.to_le_bytes())?;
        Ok(())
    }

    /// Create a new FL round
    pub fn create_round(
        storage: &crate::Storage,
        model_id: [u8; 32],
        round_number: u32,
        target_contributions: u32,
        reward_pool: u128,
        current_time: u64,
    ) -> Result<u64> {
        // Verify model exists and is active
        let model = match Self::get_model(storage, &model_id)? {
            Some(m) => m,
            None => return Err(anyhow!("Model not found")),
        };

        if model.status != ModelStatus::Active {
            return Err(anyhow!("Model is not active"));
        }

        // Get next round ID
        let round_id = Self::get_next_round_id(storage)?;

        let round = FlRound {
            round_id,
            model_id,
            round_number,
            status: RoundStatus::Proposed,
            created_at: current_time,
            started_at: None,
            ended_at: None,
            target_contributions,
            received_contributions: 0,
            aggregation_result: None,
            reward_pool,
            distributed_rewards: 0,
        };

        // Store round
        let round_data = bincode::serialize(&round)?;
        let key = format!("round:{}", round_id);
        storage.put(key.as_bytes(), &round_data)?;

        // Update next round ID
        storage.put(b"next_round_id", &(round_id + 1).to_le_bytes())?;

        Ok(round_id)
    }

    /// Get next round ID
    fn get_next_round_id(storage: &crate::Storage) -> Result<u64> {
        match storage.get(b"next_round_id")? {
            Some(data) if data.len() >= 8 => {
                Ok(u64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8])))
            }
            _ => Ok(1),
        }
    }

    /// Get FL round by ID
    pub fn get_round(storage: &crate::Storage, round_id: u64) -> Result<Option<FlRound>> {
        let key = format!("round:{}", round_id);
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let round: FlRound = crate::safe_deserialize(&data)?;
                Ok(Some(round))
            }
            None => Ok(None),
        }
    }

    /// Update round status
    pub fn update_round_status(
        storage: &crate::Storage,
        round_id: u64,
        new_status: RoundStatus,
        current_time: u64,
    ) -> Result<()> {
        let mut round = match Self::get_round(storage, round_id)? {
            Some(r) => r,
            None => return Err(anyhow!("Round not found")),
        };

        round.status = new_status;

        match new_status {
            RoundStatus::Active => {
                round.started_at = Some(current_time);
            }
            RoundStatus::Closed | RoundStatus::Finalized | RoundStatus::Aborted => {
                round.ended_at = Some(current_time);
            }
            _ => {}
        }

        let round_data = bincode::serialize(&round)?;
        let key = format!("round:{}", round_id);
        storage.put(key.as_bytes(), &round_data)?;

        Ok(())
    }

    /// Submit an update/contribution
    pub fn submit_update(
        storage: &crate::Storage,
        update_id: [u8; 32],
        round_id: u64,
        model_id: [u8; 32],
        contributor: [u8; 32],
        update_hash: [u8; 32],
        update_data_hash: [u8; 32],
        update_size: u64,
        current_time: u64,
    ) -> Result<()> {
        // Verify round exists and is open/active
        let round = match Self::get_round(storage, round_id)? {
            Some(r) => r,
            None => return Err(anyhow!("Round not found")),
        };

        if !matches!(round.status, RoundStatus::Open | RoundStatus::Active) {
            return Err(anyhow!("Round is not accepting contributions"));
        }

        // Check contribution limits
        let policy = Self::get_fl_policy(storage)?;
        if round.received_contributions >= policy.max_contributions_per_round {
            return Err(anyhow!("Maximum contributions reached for this round"));
        }

        // Check if contributor already submitted
        if Self::has_contributor_submitted(storage, round_id, &contributor)? {
            return Err(anyhow!("Contributor already submitted for this round"));
        }

        let update = FlUpdate {
            update_id,
            round_id,
            model_id,
            contributor,
            update_hash,
            update_data_hash,
            update_size,
            submitted_at: current_time,
            status: UpdateStatus::Submitted,
            quality_score: None,
            validation_result: None,
        };

        // Store update
        let update_data = bincode::serialize(&update)?;
        let key = format!("update:{}", hex::encode(&update_id));
        storage.put(key.as_bytes(), &update_data)?;

        // Update round contribution count
        Self::increment_round_contributions(storage, round_id)?;

        Ok(())
    }

    /// Check if contributor has already submitted for a round
    fn has_contributor_submitted(
        storage: &crate::Storage,
        round_id: u64,
        contributor: &[u8; 32],
    ) -> Result<bool> {
        let key = format!("contributor:{}:{}", round_id, hex::encode(contributor));
        storage
            .get(key.as_bytes())
            .map(|opt: Option<Vec<u8>>| opt.is_some())
    }

    /// Increment round contribution count
    fn increment_round_contributions(storage: &crate::Storage, round_id: u64) -> Result<()> {
        let mut round = match Self::get_round(storage, round_id)? {
            Some(r) => r,
            None => return Err(anyhow!("Round not found")),
        };

        round.received_contributions += 1;

        // Check if round should close
        let policy = Self::get_fl_policy(storage)?;
        if round.received_contributions >= policy.min_contributions_per_round {
            round.status = RoundStatus::Closed;
        }

        let round_data = bincode::serialize(&round)?;
        let key = format!("round:{}", round_id);
        storage.put(key.as_bytes(), &round_data)?;

        Ok(())
    }

    /// Get update by ID
    pub fn get_update(storage: &crate::Storage, update_id: &[u8; 32]) -> Result<Option<FlUpdate>> {
        let key = format!("update:{}", hex::encode(update_id));
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let update: FlUpdate = crate::safe_deserialize(&data)?;
                Ok(Some(update))
            }
            None => Ok(None),
        }
    }

    /// Get all updates for a round
    pub fn get_round_updates(storage: &crate::Storage, round_id: u64) -> Result<Vec<FlUpdate>> {
        let mut updates = Vec::new();
        let prefix = "update:";

        let iter = storage.iterator_cf(CF_FL_UPDATES)?;
        for item in iter {
            let (key, value): (Vec<u8>, Vec<u8>) = item?;
            let key_str = String::from_utf8_lossy(&key);
            if key_str.starts_with(prefix) {
                if let Ok(update) = crate::safe_deserialize::<FlUpdate>(&value) {
                    if update.round_id == round_id {
                        updates.push(update);
                    }
                }
            }
        }

        Ok(updates)
    }

    /// Validate an update
    pub fn validate_update(
        storage: &crate::Storage,
        update_id: &[u8; 32],
        validator: [u8; 32],
        is_valid: bool,
        validation_score: f64,
        feedback: String,
        current_time: u64,
    ) -> Result<()> {
        let mut update = match Self::get_update(storage, update_id)? {
            Some(u) => u,
            None => return Err(anyhow!("Update not found")),
        };

        let validation_result = ValidationResult {
            validator,
            validated_at: current_time,
            is_valid,
            validation_score,
            feedback,
        };

        update.validation_result = Some(validation_result.clone());
        update.status = if is_valid {
            UpdateStatus::Accepted
        } else {
            UpdateStatus::Rejected
        };
        update.quality_score = Some(validation_score);

        // Store updated update
        let update_data = bincode::serialize(&update)?;
        let key = format!("update:{}", hex::encode(update_id));
        storage.put(key.as_bytes(), &update_data)?;

        Ok(())
    }

    /// Distribute rewards
    pub fn distribute_rewards(
        storage: &crate::Storage,
        round_id: u64,
        rewards: Vec<([u8; 32], RewardType, u128, String)>,
        current_time: u64,
    ) -> Result<()> {
        let round = match Self::get_round(storage, round_id)? {
            Some(r) => r,
            None => return Err(anyhow!("Round not found")),
        };

        let mut total_distributed = 0u128;

        for (recipient, reward_type, amount, reason) in rewards {
            let reward_id = Self::generate_reward_id(&recipient, round_id, current_time);

            let reward = FlReward {
                reward_id,
                round_id,
                recipient,
                reward_type,
                amount,
                distributed_at: current_time,
                transaction_hash: None,
                reason,
            };

            // Store reward
            let reward_data = bincode::serialize(&reward)?;
            let key = format!("reward:{}", hex::encode(&reward_id));
            storage.put(key.as_bytes(), &reward_data)?;

            total_distributed += amount;
        }

        // Update round distributed rewards
        let mut updated_round = round;
        updated_round.distributed_rewards = total_distributed;
        let round_data = bincode::serialize(&updated_round)?;
        let key = format!("round:{}", round_id);
        storage.put(key.as_bytes(), &round_data)?;

        Ok(())
    }

    /// Generate reward ID
    fn generate_reward_id(recipient: &[u8; 32], round_id: u64, timestamp: u64) -> [u8; 32] {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        recipient.hash(&mut hasher);
        round_id.hash(&mut hasher);
        timestamp.hash(&mut hasher);

        let hash = hasher.finish();
        let mut result = [0u8; 32];
        let hash_bytes = hash.to_le_bytes();
        result.copy_from_slice(&hash_bytes);
        result
    }

    /// Get FL policy
    pub fn get_fl_policy(storage: &crate::Storage) -> Result<FlPolicy> {
        match storage.get(b"fl_policy")? {
            Some(data) => {
                let policy: FlPolicy = crate::safe_deserialize(&data)?;
                Ok(policy)
            }
            None => Ok(FlPolicy::default()),
        }
    }

    /// Set FL policy
    pub fn set_fl_policy(storage: &crate::Storage, policy: &FlPolicy) -> Result<()> {
        let policy_data = bincode::serialize(policy)?;
        storage.put(b"fl_policy", &policy_data)?;
        Ok(())
    }

    /// Get FL statistics
    pub fn get_fl_stats(storage: &crate::Storage) -> Result<FlStats> {
        let models = Self::get_all_models(storage)?;
        let active_models = models
            .iter()
            .filter(|m| m.status == ModelStatus::Active)
            .count();

        let mut total_rounds = 0;
        let mut active_rounds = 0;
        let mut total_updates = 0;
        let mut total_rewards = 0u128;

        // Count rounds
        let iter = storage.iterator_cf(CF_FL_ROUNDS)?;
        for item in iter {
            let (_, value): (_, Vec<u8>) = item?;
            if let Ok(round) = crate::safe_deserialize::<FlRound>(&value) {
                total_rounds += 1;
                if matches!(round.status, RoundStatus::Open | RoundStatus::Active) {
                    active_rounds += 1;
                }
            }
        }

        // Count updates
        let iter = storage.iterator_cf(CF_FL_UPDATES)?;
        for item in iter {
            let (_, value): (_, Vec<u8>) = item?;
            if let Ok(_) = crate::safe_deserialize::<FlUpdate>(&value) {
                total_updates += 1;
            }
        }

        // Sum rewards
        let iter = storage.iterator_cf(CF_FL_REWARDS)?;
        for item in iter {
            let (_, value): (_, Vec<u8>) = item?;
            if let Ok(reward) = crate::safe_deserialize::<FlReward>(&value) {
                total_rewards += reward.amount;
            }
        }

        Ok(FlStats {
            total_models: models.len(),
            active_models,
            total_rounds,
            active_rounds,
            total_updates,
            total_rewards,
        })
    }
}

/// FL statistics
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FlStats {
    pub total_models: usize,
    pub active_models: usize,
    pub total_rounds: usize,
    pub active_rounds: usize,
    pub total_updates: usize,
    pub total_rewards: u128,
}
