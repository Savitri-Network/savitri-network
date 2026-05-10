//! Callback Queue Management: persistenza eventi CallbackTriggered per worker
//!
//! - Queue persistente per eventi CallbackTriggered (RocksDB)
//! - Dequeue eventi per worker asincrono
//! - Gestione retry e error handling

use savitri_storage::Storage;

// Column family name for callbacks
const CF_CALLBACKS: &str = "callbacks";
use anyhow::{Context, Result};
use bincode;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};
use std::sync::Arc;

/// Evento CallbackTriggered da processare
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackTriggeredEvent {
    /// ID of the callback (32 bytes, hex encoded)
    pub callback_id: String,
    /// Hash of the payload (32 bytes, hex encoded)
    pub payload_hash: String,
    /// Payload completo (serializzato JSON)
    pub payload: Vec<u8>,
    /// Altezza of the blocco dove è stato emesso
    pub block_height: u64,
    /// Timestamp of the blocco
    pub block_timestamp: u64,
    pub retry_count: u8,
    /// Timestamp dell'ultimo tentativo
    pub last_attempt_timestamp: Option<u64>,
}

/// Queue persistente per eventi CallbackTriggered
pub struct CallbackQueue {
    storage: Arc<Storage>,
}

impl CallbackQueue {
    ///
    /// # Arguments
    /// * `storage` - Storage layer (RocksDB)
    pub fn new(storage: Arc<Storage>) -> Self {
        Self { storage }
    }

    /// Adds un evento CallbackTriggered alla queue
    ///
    /// L'evento viene persistito in RocksDB per processing asincrono da worker.
    ///
    /// # Arguments
    /// * `callback_id` - ID of the callback (32 bytes, hex encoded)
    /// * `payload_hash` - Hash of the payload (32 bytes, hex encoded)
    /// * `payload` - Payload completo (serializzato JSON)
    /// * `block_height` - Altezza of the blocco
    /// * `block_timestamp` - Timestamp of the blocco
    ///
    /// # Returns
    /// Chiave dell'evento in the queue (per tracking)
    pub fn enqueue(
        &self,
        callback_id: &str,
        payload_hash: &str,
        payload: Vec<u8>,
        block_height: u64,
        block_timestamp: u64,
    ) -> Result<Vec<u8>> {
        // Creates evento
        let event = CallbackTriggeredEvent {
            callback_id: callback_id.to_string(),
            payload_hash: payload_hash.to_string(),
            payload,
            block_height,
            block_timestamp,
            retry_count: 0,
            last_attempt_timestamp: None,
        };

        // Serializza evento
        let event_bytes =
            bincode::serialize(&event).context("Failed to serialize CallbackTriggeredEvent")?;

        // Genera chiave: keccak256(callback_id || block_height || payload_hash)
        let mut hasher = Keccak256::new();
        hasher.update(callback_id.as_bytes());
        hasher.update(&block_height.to_be_bytes());
        hasher.update(payload_hash.as_bytes());
        let key_hash = hasher.finalize();
        let key = key_hash.as_slice().to_vec();

        // Salva in RocksDB (CF_CALLBACKS)
        self.storage
            .put_cf(CF_CALLBACKS, &key, &event_bytes)
            .context("Failed to store callback event in queue")?;

        Ok(key)
    }

    /// Rimuove un evento dalla queue (dopo processing riuscito)
    ///
    /// # Arguments
    pub fn dequeue(&self, key: &[u8]) -> Result<()> {
        self.storage
            .delete_cf(CF_CALLBACKS, key)
            .context("Failed to remove callback event from queue")?;
        Ok(())
    }

    ///
    /// # Arguments
    /// * `key` - Chiave dell'evento
    ///
    /// # Returns
    /// Evento se trovato, None altrimenti
    /// Maximum allowed size for callback event deserialization (1 MB).
    const MAX_CALLBACK_SIZE: usize = 1 * 1024 * 1024;

    pub fn get(&self, key: &[u8]) -> Result<Option<CallbackTriggeredEvent>> {
        match self.storage.get_cf(CF_CALLBACKS, key)? {
            Some(bytes) => {
                if bytes.len() > Self::MAX_CALLBACK_SIZE {
                    anyhow::bail!(
                        "Callback event data too large: {} bytes (max {})",
                        bytes.len(),
                        Self::MAX_CALLBACK_SIZE
                    );
                }
                let event: CallbackTriggeredEvent =
                    bincode::deserialize::<CallbackTriggeredEvent>(&bytes)
                        .context("Failed to deserialize CallbackTriggeredEvent")?;
                Ok(Some(event))
            }
            None => Ok(None),
        }
    }

    ///
    /// # Arguments
    /// * `key` - Chiave dell'evento
    /// * `retry_count` - Nuovo retry count
    /// * `last_attempt_timestamp` - Timestamp dell'ultimo tentativo
    pub fn update_retry(
        &self,
        key: &[u8],
        retry_count: u8,
        last_attempt_timestamp: u64,
    ) -> Result<()> {
        if let Some(mut event) = self.get(key)? {
            event.retry_count = retry_count;
            event.last_attempt_timestamp = Some(last_attempt_timestamp);

            let event_bytes = bincode::serialize(&event)
                .context("Failed to serialize updated CallbackTriggeredEvent")?;

            self.storage
                .put_cf(CF_CALLBACKS, key, &event_bytes)
                .context("Failed to update callback event retry count")?;
        }
        Ok(())
    }

    ///
    /// # Returns
    ///
    /// # Note
    /// Per performance, considera di limitare il numero di eventi processati per batch.
    pub fn list_all(&self) -> Result<Vec<(Vec<u8>, CallbackTriggeredEvent)>> {
        let mut events = Vec::new();

        let iter = self.storage.iterator_cf(CF_CALLBACKS)?;

        for item in iter {
            let (key, value) = item.context("Failed to iterate callback queue")?;
            if value.len() > Self::MAX_CALLBACK_SIZE {
                eprintln!(
                    "Callback event data too large: {} bytes (max {}), skipping",
                    value.len(),
                    Self::MAX_CALLBACK_SIZE
                );
                continue;
            }
            match bincode::deserialize::<CallbackTriggeredEvent>(&value) {
                Ok(event) => events.push((key.to_vec(), event)),
                Err(e) => {
                    eprintln!("Failed to deserialize callback event: {}", e);
                }
            }
        }

        Ok(events)
    }

    /// Conta il numero di eventi in the queue
    ///
    /// # Returns
    pub fn count(&self) -> Result<usize> {
        let mut count = 0;

        let iter = self.storage.iterator_cf(CF_CALLBACKS)?;

        for item in iter {
            let _ = item.context("Failed to iterate callback queue")?;
            count += 1;
        }

        Ok(count)
    }
}
