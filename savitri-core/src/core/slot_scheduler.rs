use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use ed25519_dalek::VerifyingKey as PublicKey;
use tokio::sync::{mpsc, Mutex};
use tokio::task::yield_now;
use tokio::time::sleep;

use savitri_storage::Storage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotRole {
    Leader,
    Follower,
    Observer,
}

impl SlotRole {
    pub fn is_leader(self) -> bool {
        matches!(self, SlotRole::Leader)
    }

    pub fn is_validator(self) -> bool {
        matches!(self, SlotRole::Leader | SlotRole::Follower)
    }
}

#[derive(Debug, Clone)]
pub struct SlotInfo {
    pub slot: u64,
    pub round: u32,
    pub leader: Option<String>,
    pub role: SlotRole,
    pub start_ms: u64,
    pub end_ms: u64,
}

impl SlotInfo {
    pub fn is_leader(&self) -> bool {
        self.role.is_leader()
    }

    pub fn leader_id(&self) -> Option<&str> {
        self.leader.as_deref()
    }
}

#[derive(Clone)]
pub struct SlotSchedulerConfig {
    pub slot_duration: Duration,
    pub validators: Vec<String>,
    pub local_id: String,
    pub slot_base_ms: Option<u64>,
}

impl SlotSchedulerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.slot_duration.is_zero() {
            bail!("slot_duration must be greater than zero");
        }
        if self.validators.is_empty() {
            bail!("validator set must not be empty");
        }
        if self.local_id.trim().is_empty() {
            bail!("local validator id must not be empty");
        }
        if self.slot_base_ms.is_none() {
            bail!("slot_base_ms must be configured to ensure deterministic leader rotation");
        }
        Ok(())
    }
}

struct SchedulerState {
    last_slot: u64,
}

struct SlotSchedulerInner {
    storage: Arc<Storage>,
    slot_duration_ms: u64,
    base_ms: u64,
    validators: Vec<String>,
    local_id: String,
    is_validator: bool,
    state: Mutex<SchedulerState>,
}

#[derive(Clone)]
pub struct SlotScheduler {
    inner: Arc<SlotSchedulerInner>,
}

pub struct SlotTicker {
    rx: mpsc::Receiver<SlotInfo>,
    handle: tokio::task::JoinHandle<()>,
}

impl SlotTicker {
    pub async fn recv(&mut self) -> Option<SlotInfo> {
        self.rx.recv().await
    }
}

impl Drop for SlotTicker {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

impl SlotScheduler {
    /// ⭐ PHASE 2: Calculate leader based on PoU scores
    /// REQUIRED: PoU-based leader selection
    pub fn calculate_leader(
        &self,
        slot: u64,
        pou_scores: &HashMap<PublicKey, u32>,
    ) -> Option<PublicKey> {
        if self.inner.validators.is_empty() {
            return None;
        }

        let validator_keys: Vec<PublicKey> = self
            .inner
            .validators
            .iter()
            .filter_map(|v_str| {
                // Convert string to bytes and then to PublicKey
                let bytes: Vec<u8> = v_str
                    .chars()
                    .filter_map(|c| c.to_digit(16))
                    .collect::<Vec<_>>()
                    .chunks(2)
                    .map(|chunk| (chunk[0] * 16 + chunk[1]) as u8)
                    .collect();

                if bytes.len() == 32 {
                    let mut array = [0u8; 32];
                    array.copy_from_slice(&bytes);
                    PublicKey::from_bytes(&array).ok()
                } else {
                    None
                }
            })
            .collect();

        if validator_keys.is_empty() {
            // Fallback to simple round-robin if no valid keys
            return None; // Cannot convert to PublicKey
        }

        // Calculate deterministic seed for this slot
        let seed = self.calculate_deterministic_seed(slot);

        // Simple deterministic selection without complex RNG
        let total_score: u32 = validator_keys
            .iter()
            .map(|pk| pou_scores.get(pk).unwrap_or(&1))
            .sum();

        if total_score == 0 {
            return validator_keys.first().copied();
        }

        // Use seed for deterministic selection
        let seed_value = u32::from_le_bytes([seed[0], seed[1], seed[2], seed[3]]);
        let mut selection_score = seed_value % total_score;

        for (pk, score) in validator_keys.iter().map(|pk| {
            let score = pou_scores.get(pk).unwrap_or(&1);
            (*pk, *score)
        }) {
            if selection_score < score {
                return Some(pk);
            }
            selection_score -= score;
        }

        validator_keys.first().copied()
    }

    /// ⭐ PHASE 2: Calculate deterministic seed for slot selection
    fn calculate_deterministic_seed(&self, slot: u64) -> [u8; 32] {
        use sha2::{Digest, Sha256};

        let mut hasher = Sha256::new();

        // Include all deterministic factors
        hasher.update(b"savitri-slot-leader-v1");
        hasher.update(self.inner.base_ms.to_le_bytes());
        hasher.update(slot.to_le_bytes());
        hasher.update(self.inner.slot_duration_ms.to_le_bytes());

        for validator in &self.inner.validators {
            hasher.update(validator.as_bytes());
        }

        let hash = hasher.finalize();
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&hash);
        seed
    }

    pub fn new(storage: Arc<Storage>, cfg: SlotSchedulerConfig) -> Result<Self> {
        cfg.validate()?;
        let slot_duration_ms: u64 = cfg
            .slot_duration
            .as_millis()
            .try_into()
            .context("slot_duration exceeds u64 range")?;

        let base_ms = if let Some(ms) = storage.get_consensus_slot_base_ms()? {
            ms
        } else {
            let cfg_base = cfg
                .slot_base_ms
                .context("slot_base_ms must be provided when initializing slot scheduler")?;
            storage.set_consensus_slot_base_ms(cfg_base)?;
            cfg_base
        };

        let last_slot = storage.get_consensus_last_slot()?;
        let now_slot = slot_from_time(base_ms, slot_duration_ms, current_millis()?);
        let initial_slot = match last_slot {
            Some(stored) => stored.max(now_slot),
            None => now_slot,
        };

        let inner = SlotSchedulerInner {
            storage,
            slot_duration_ms,
            base_ms,
            validators: cfg.validators,
            local_id: cfg.local_id,
            is_validator: false, // set below
            state: Mutex::new(SchedulerState {
                last_slot: initial_slot,
            }),
        };
        let mut scheduler = Self {
            inner: Arc::new(inner),
        };
        let is_validator = scheduler
            .inner
            .validators
            .iter()
            .any(|v| v == &scheduler.inner.local_id);
        Arc::get_mut(&mut scheduler.inner)
            .expect("no other refs")
            .is_validator = is_validator;
        if last_slot.map(|s| s < initial_slot).unwrap_or(true) {
            scheduler
                .inner
                .storage
                .set_consensus_last_slot(initial_slot)?;
        }
        Ok(scheduler)
    }

    pub async fn start(self) -> Result<SlotTicker> {
        let (tx, rx) = mpsc::channel(8);
        let initial = self.current_slot_info().await?;
        if tx.send(initial).await.is_err() {
            bail!("slot ticker receiver dropped before initialization");
        }
        let runner = self.clone();
        let handle = tokio::spawn(async move {
            if let Err(err) = runner.run_loop(tx).await {
                println!("⚠️ slot scheduler terminated: {}", err);
            }
        });
        Ok(SlotTicker { rx, handle })
    }

    pub async fn current_slot_info(&self) -> Result<SlotInfo> {
        let now_ms = current_millis()?;
        self.slot_info_at(now_ms).await
    }

    async fn run_loop(&self, tx: mpsc::Sender<SlotInfo>) -> Result<()> {
        loop {
            let next_start = self.next_slot_start_ms().await?;
            let now_ms = current_millis()?;
            if next_start > now_ms {
                sleep(Duration::from_millis(next_start - now_ms)).await;
            } else {
                yield_now().await;
            }
            let info = self.current_slot_info().await?;
            if tx.send(info).await.is_err() {
                break;
            }
        }
        Ok(())
    }

    async fn next_slot_start_ms(&self) -> Result<u64> {
        let now_ms = current_millis()?;
        let guard = self.inner.state.lock().await;
        let mut slot = slot_from_time(self.inner.base_ms, self.inner.slot_duration_ms, now_ms);
        if slot < guard.last_slot {
            slot = guard.last_slot;
        }
        let next_slot = slot.saturating_add(1);
        Ok(self.inner.base_ms + next_slot * self.inner.slot_duration_ms)
    }

    async fn slot_info_at(&self, now_ms: u64) -> Result<SlotInfo> {
        let mut guard = self.inner.state.lock().await;
        let mut slot = slot_from_time(self.inner.base_ms, self.inner.slot_duration_ms, now_ms);
        if slot < guard.last_slot {
            slot = guard.last_slot;
        }
        if slot > guard.last_slot {
            self.inner.storage.set_consensus_last_slot(slot)?;
            guard.last_slot = slot;
        }
        drop(guard);

        let start_ms = self.inner.base_ms + slot * self.inner.slot_duration_ms;
        let end_ms = start_ms + self.inner.slot_duration_ms;

        let leader = if self.inner.validators.is_empty() {
            None
        } else {
            self.calculate_leader(slot, &HashMap::new())
                .map(|pk| pk.to_bytes().iter().map(|&b| b.to_string()).collect())
        };

        let role = match (&leader, self.inner.is_validator) {
            (Some(l), true) if *l == self.inner.local_id => SlotRole::Leader,
            (Some(_), true) => SlotRole::Follower,
            _ => SlotRole::Observer,
        };

        let round = if self.inner.validators.is_empty() {
            0
        } else {
            (slot % self.inner.validators.len() as u64) as u32
        };

        Ok(SlotInfo {
            slot,
            round,
            leader,
            role,
            start_ms,
            end_ms,
        })
    }
}

fn slot_from_time(base_ms: u64, slot_duration_ms: u64, now_ms: u64) -> u64 {
    if now_ms <= base_ms {
        0
    } else {
        (now_ms - base_ms) / slot_duration_ms
    }
}

fn current_millis() -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock before UNIX_EPOCH")?;
    Ok(now
        .as_millis()
        .try_into()
        .context("system time beyond u64 range")?)
}
