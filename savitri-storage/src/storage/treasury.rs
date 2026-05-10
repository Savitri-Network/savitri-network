//! Treasury Storage: Implementation for Savitri Network
//!
//! This module implements treasury management for governance funds and rewards.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Column family for treasury
pub const CF_TREASURY: &str = "treasury";

/// Treasury transaction types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TreasuryTransactionType {
    Deposit {
        from: Vec<u8>,
        amount: u128,
        reason: String,
    },
    Withdrawal {
        to: Vec<u8>,
        amount: u128,
        proposal_id: Option<u64>,
        reason: String,
    },
    Reward {
        to: Vec<u8>,
        amount: u128,
        reward_type: RewardType,
    },
    Slash {
        from: Vec<u8>,
        amount: u128,
        reason: String,
    },
    Burn {
        amount: u128,
        reason: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum RewardType {
    BlockReward,
    StakeReward,
    GovernanceReward,
    NetworkReward,
    IncentiveReward,
}

/// Treasury transaction record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreasuryTransaction {
    pub id: u64,
    pub tx_type: TreasuryTransactionType,
    pub amount: u128,
    pub timestamp: u64,
    pub block_height: u64,
    pub executor: Vec<u8>,
    pub signature: Option<Vec<u8>>,
    pub status: TransactionStatus,
    pub memo: String,
}

/// Transaction status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransactionStatus {
    Pending,
    Approved,
    Executed,
    Rejected,
    Failed,
}

/// Treasury balance snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreasurySnapshot {
    pub total_balance: u128,
    pub available_balance: u128,
    pub locked_balance: u128,
    pub pending_transactions: u128,
    pub total_deposits: u128,
    pub total_withdrawals: u128,
    pub total_rewards: u128,
    pub total_slashes: u128,
    pub timestamp: u64,
    pub block_height: u64,
}

/// Treasury statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreasuryStats {
    pub daily_volume: u128,
    pub weekly_volume: u128,
    pub monthly_volume: u128,
    pub total_transactions: u64,
    pub successful_transactions: u64,
    pub failed_transactions: u64,
    pub average_transaction_size: u128,
    pub top_withdrawals: Vec<(Vec<u8>, u128)>,
    pub reward_distribution: HashMap<RewardType, u128>,
}

/// Treasury manager with full storage integration
pub struct Treasury {
    storage: Option<std::sync::Arc<dyn crate::traits::StorageTrait>>,
    next_transaction_id: std::sync::atomic::AtomicU64,
    cache: std::sync::RwLock<HashMap<String, TreasuryTransaction>>,
    balance_cache: std::sync::RwLock<TreasurySnapshot>,
}

impl std::fmt::Debug for Treasury {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Treasury")
            .field("storage", &self.storage.is_some())
            .field(
                "next_transaction_id",
                &self
                    .next_transaction_id
                    .load(std::sync::atomic::Ordering::Relaxed),
            )
            .field(
                "cache_entries",
                &self.cache.try_read().map(|guard| guard.len()).unwrap_or(0),
            )
            .field("balance_cached", &self.balance_cache.try_read().is_ok())
            .finish()
    }
}

impl Treasury {
    pub fn new() -> Self {
        Self {
            storage: None,
            next_transaction_id: std::sync::atomic::AtomicU64::new(1),
            cache: std::sync::RwLock::new(HashMap::new()),
            balance_cache: std::sync::RwLock::new(TreasurySnapshot::default()),
        }
    }

    pub fn with_storage(storage: std::sync::Arc<dyn crate::traits::StorageTrait>) -> Self {
        Self {
            storage: Some(storage),
            next_transaction_id: std::sync::atomic::AtomicU64::new(1),
            cache: std::sync::RwLock::new(HashMap::new()),
            balance_cache: std::sync::RwLock::new(TreasurySnapshot::default()),
        }
    }

    /// Get current treasury balance
    pub fn get_balance(&self) -> u128 {
        if let Ok(cache) = self.balance_cache.read() {
            cache.available_balance
        } else {
            0
        }
    }

    /// Set treasury balance (admin function)
    pub fn set_balance(&self, balance: u128) -> Result<()> {
        if let Ok(mut cache) = self.balance_cache.write() {
            cache.total_balance = balance;
            cache.available_balance = balance;
            cache.timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        if let Some(storage) = &self.storage {
            let key = "treasury:balance";
            let snapshot = if let Ok(cache) = self.balance_cache.read() {
                cache.clone()
            } else {
                TreasurySnapshot::default()
            };
            let data = bincode::serialize(&snapshot)?;
            storage.put(key.as_bytes(), &data)?;
        }

        Ok(())
    }

    /// Deposit funds to treasury
    pub fn deposit(&self, from: Vec<u8>, amount: u128, reason: String) -> Result<u64> {
        let tx_id = self
            .next_transaction_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let transaction = TreasuryTransaction {
            id: tx_id,
            tx_type: TreasuryTransactionType::Deposit {
                from,
                amount: amount.clone(),
                reason,
            },
            amount,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            block_height: 0,      // Will be set when included in block
            executor: Vec::new(), // Will be set by validator
            signature: None,
            status: TransactionStatus::Pending,
            memo: String::new(),
        };

        self.save_transaction(&transaction)?;
        self.update_balance_on_deposit(amount)?;

        Ok(tx_id)
    }

    /// Withdraw funds from treasury
    pub fn withdraw(
        &self,
        to: Vec<u8>,
        amount: u128,
        proposal_id: Option<u64>,
        reason: String,
    ) -> Result<u64> {
        // Check available balance
        if let Ok(cache) = self.balance_cache.read() {
            if cache.available_balance < amount {
                return Err(anyhow::anyhow!("Insufficient treasury balance"));
            }
        }

        let tx_id = self
            .next_transaction_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let transaction = TreasuryTransaction {
            id: tx_id,
            tx_type: TreasuryTransactionType::Withdrawal {
                to: to.clone(),
                amount: amount.clone(),
                proposal_id,
                reason,
            },
            amount,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            block_height: 0,
            executor: Vec::new(),
            signature: None,
            status: TransactionStatus::Pending,
            memo: String::new(),
        };

        self.save_transaction(&transaction)?;
        self.update_balance_on_withdrawal(amount)?;

        Ok(tx_id)
    }

    /// Distribute rewards
    pub fn distribute_reward(
        &self,
        to: Vec<u8>,
        amount: u128,
        reward_type: RewardType,
    ) -> Result<u64> {
        let tx_id = self
            .next_transaction_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let transaction = TreasuryTransaction {
            id: tx_id,
            tx_type: TreasuryTransactionType::Reward {
                to: to.clone(),
                amount: amount.clone(),
                reward_type,
            },
            amount,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            block_height: 0,
            executor: Vec::new(),
            signature: None,
            status: TransactionStatus::Approved,
            memo: String::new(),
        };

        self.save_transaction(&transaction)?;
        self.update_balance_on_withdrawal(amount)?;

        Ok(tx_id)
    }

    pub fn slash(&self, from: Vec<u8>, amount: u128, reason: String) -> Result<u64> {
        let tx_id = self
            .next_transaction_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let transaction = TreasuryTransaction {
            id: tx_id,
            tx_type: TreasuryTransactionType::Slash {
                from,
                amount: amount.clone(),
                reason,
            },
            amount,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            block_height: 0,
            executor: Vec::new(),
            signature: None,
            status: TransactionStatus::Approved,
            memo: String::new(),
        };

        self.save_transaction(&transaction)?;
        self.update_balance_on_deposit(amount)?;

        Ok(tx_id)
    }

    /// Burn tokens (reduce supply)
    pub fn burn(&self, amount: u128, reason: String) -> Result<u64> {
        // Check available balance
        if let Ok(cache) = self.balance_cache.read() {
            if cache.available_balance < amount {
                return Err(anyhow::anyhow!("Insufficient treasury balance for burning"));
            }
        }

        let tx_id = self
            .next_transaction_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);

        let transaction = TreasuryTransaction {
            id: tx_id,
            tx_type: TreasuryTransactionType::Burn {
                amount: amount.clone(),
                reason,
            },
            amount,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            block_height: 0,
            executor: Vec::new(),
            signature: None,
            status: TransactionStatus::Approved,
            memo: String::new(),
        };

        self.save_transaction(&transaction)?;
        self.update_balance_on_withdrawal(amount)?;

        Ok(tx_id)
    }

    /// Get transaction by ID
    pub fn get_transaction(&self, tx_id: u64) -> Option<TreasuryTransaction> {
        // Try cache first
        if let Ok(cache) = self.cache.read() {
            if let Some(tx) = cache.get(&tx_id.to_string()) {
                return Some(tx.clone());
            }
        }

        // Try storage
        if let Some(storage) = &self.storage {
            let key = format!("treasury:tx:{}", tx_id);
            if let Ok(Some(data)) = storage.get(key.as_bytes()) {
                if let Ok(tx) = crate::safe_deserialize::<TreasuryTransaction>(&data) {
                    if let Ok(mut cache) = self.cache.write() {
                        cache.insert(tx_id.to_string(), tx.clone());
                    }
                    return Some(tx);
                }
            }
        }

        None
    }

    /// Get treasury snapshot
    pub fn get_snapshot(&self) -> TreasurySnapshot {
        if let Ok(cache) = self.balance_cache.read() {
            cache.clone()
        } else {
            TreasurySnapshot::default()
        }
    }

    /// Get treasury statistics
    pub fn get_statistics(&self) -> TreasuryStats {
        let mut stats = TreasuryStats::default();

        if let Ok(cache) = self.cache.read() {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();

            let one_day = 86400;
            let one_week = 7 * one_day;
            let one_month = 30 * one_day;

            for tx in cache.values() {
                stats.total_transactions += 1;
                stats.average_transaction_size = (stats.average_transaction_size + tx.amount) / 2;

                match tx.status {
                    TransactionStatus::Executed | TransactionStatus::Approved => {
                        stats.successful_transactions += 1;
                    }
                    TransactionStatus::Failed | TransactionStatus::Rejected => {
                        stats.failed_transactions += 1;
                    }
                    _ => {}
                }

                // Volume calculations
                if now - tx.timestamp <= one_day {
                    stats.daily_volume += tx.amount;
                }
                if now - tx.timestamp <= one_week {
                    stats.weekly_volume += tx.amount;
                }
                if now - tx.timestamp <= one_month {
                    stats.monthly_volume += tx.amount;
                }

                // Reward distribution
                if let TreasuryTransactionType::Reward {
                    reward_type,
                    amount,
                    ..
                } = &tx.tx_type
                {
                    *stats
                        .reward_distribution
                        .entry(reward_type.clone())
                        .or_insert(0) += amount;
                }

                // Top withdrawals
                if let TreasuryTransactionType::Withdrawal { to, amount, .. } = &tx.tx_type {
                    stats.top_withdrawals.push((to.clone(), *amount));
                }
            }

            // Sort top withdrawals
            stats.top_withdrawals.sort_by(|a, b| b.1.cmp(&a.1));
            stats.top_withdrawals.truncate(10);
        }

        stats
    }

    /// Save transaction to storage and cache
    fn save_transaction(&self, transaction: &TreasuryTransaction) -> Result<()> {
        // Update cache
        if let Ok(mut cache) = self.cache.write() {
            cache.insert(transaction.id.to_string(), transaction.clone());
        }

        // Save to storage
        if let Some(storage) = &self.storage {
            let key = format!("treasury:tx:{}", transaction.id);
            let data = bincode::serialize(transaction)?;
            storage.put(key.as_bytes(), &data)?;
        }

        Ok(())
    }

    /// Update balance on deposit
    fn update_balance_on_deposit(&self, amount: u128) -> Result<()> {
        if let Ok(mut cache) = self.balance_cache.write() {
            cache.total_balance += amount;
            cache.available_balance += amount;
            cache.total_deposits += amount;
            cache.timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        if let Some(storage) = &self.storage {
            let snapshot = if let Ok(cache) = self.balance_cache.read() {
                cache.clone()
            } else {
                TreasurySnapshot::default()
            };
            let data = bincode::serialize(&snapshot)?;
            storage.put("treasury:balance".as_bytes(), &data)?;
        }

        Ok(())
    }

    /// Update balance on withdrawal
    fn update_balance_on_withdrawal(&self, amount: u128) -> Result<()> {
        if let Ok(mut cache) = self.balance_cache.write() {
            cache.total_balance = cache.total_balance.saturating_sub(amount);
            cache.available_balance = cache.available_balance.saturating_sub(amount);
            cache.total_withdrawals += amount;
            cache.timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
        }

        if let Some(storage) = &self.storage {
            let snapshot = if let Ok(cache) = self.balance_cache.read() {
                cache.clone()
            } else {
                TreasurySnapshot::default()
            };
            let data = bincode::serialize(&snapshot)?;
            storage.put("treasury:balance".as_bytes(), &data)?;
        }

        Ok(())
    }
}

impl Default for Treasury {
    fn default() -> Self {
        Self::new()
    }
}

impl Default for TreasurySnapshot {
    fn default() -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Self {
            total_balance: 0,
            available_balance: 0,
            locked_balance: 0,
            pending_transactions: 0,
            total_deposits: 0,
            total_withdrawals: 0,
            total_rewards: 0,
            total_slashes: 0,
            timestamp: now,
            block_height: 0,
        }
    }
}

impl Default for TreasuryStats {
    fn default() -> Self {
        Self {
            daily_volume: 0,
            weekly_volume: 0,
            monthly_volume: 0,
            total_transactions: 0,
            successful_transactions: 0,
            failed_transactions: 0,
            average_transaction_size: 0,
            top_withdrawals: Vec::new(),
            reward_distribution: HashMap::new(),
        }
    }
}
