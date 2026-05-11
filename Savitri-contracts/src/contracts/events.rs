//! Contract Events: Sistema di eventi per contratti
//!
//! - Event standard (OwnershipTransferred, Paused, Unpaused, Upgraded)
//! - Event custom
//! - Emissione e query
//!
//! ## Eventi Standard BaseContract
//!
//! Gli eventi standard are emessi automaticamente dalle funzioni BaseContract:
//! - `OwnershipTransferred`: emesso da `transfer_ownership()`
//! - `Paused`: emesso da `pause()`
//! - `Unpaused`: emesso da `unpause()`
//! - `Upgraded`: emesso da `upgrade()`
//! - `GovernanceHookTriggered`: emesso da `on_governance_proposal()`
//! - `FeeHookTriggered`: emesso da `on_fee_paid()`
//! - `CallbackTriggered`: emesso quando un callback HTTP viene triggerato

use crate::contracts::gas::GasMeter;
use savitri_storage::storage::Storage;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// Evento standard emesso dai contratti
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StandardEvent {
    /// Ownership trasferita
    OwnershipTransferred {
        previous_owner: String,
        new_owner: String,
    },
    /// Contratto pausato
    Paused { account: String },
    /// Contratto unpausato
    Unpaused { account: String },
    /// Contratto upgradato
    Upgraded {
        contract_address: String,
        new_version: u64,
    },
    /// Governance hook triggerato
    GovernanceHookTriggered {
        contract_address: String,
        proposal_id: String,
        action_type: String,
    },
    /// Fee hook triggerato
    FeeHookTriggered {
        contract_address: String,
        caller: String,
        amount: u128,
    },
    /// Callback HTTP triggerato
    CallbackTriggered {
        callback_id: String,
        payload_hash: String,
        /// Payload raw of the callback (massimo 1MB)
        payload: Vec<u8>,
    },
}

/// Evento custom emesso da a contract
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomEvent {
    pub contract_address: String,
    pub event_name: String,
    pub topics: Vec<[u8; 32]>,
    pub data: Vec<u8>,
}

/// Evento persistito in the storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    /// Tipo di evento (standard o custom)
    pub event_type: EventType,
    /// Dati dell'evento (serializzati)
    pub event_data: Vec<u8>,
    /// Blocco in cui l'evento è stato emesso
    pub block_number: u64,
    /// Timestamp dell'evento
    pub timestamp: u64,
    pub transaction_hash: [u8; 32],
    pub contract_address: [u8; 32],
}

/// Tipo di evento
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum EventType {
    Standard,
    Custom,
}

/// Chiave per evento in the storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventKey {
    /// Hash dell'evento per identificazione univoca
    pub event_hash: [u8; 32],
    pub block_number: u64,
}

/// Risultato query eventi
#[derive(Debug, Clone)]
pub struct EventQueryResult {
    /// Eventi trovati
    pub events: Vec<StoredEvent>,
    /// Numero totale di eventi (se più di quelli richiesti)
    pub total_count: u64,
    /// Blocco iniziale disponibile
    pub first_block: u64,
    /// Blocco finale disponibile
    pub last_block: u64,
}

/// Sistema di gestione eventi
///
/// Il sistema di eventi registra gli eventi emessi dai contratti durante l'esecuzione.
/// inclusi nei blocchi e persistiti in the storage.
pub struct EventSystem {
    /// Buffer degli eventi standard emessi durante l'esecuzione corrente
    events_buffer: Arc<Mutex<VecDeque<StandardEvent>>>,
    /// Buffer degli eventi custom emessi durante l'esecuzione corrente
    /// Gli eventi custom are emessi dai contratti per eventi personalizzati
    custom_events_buffer: Arc<Mutex<VecDeque<CustomEvent>>>,
    /// Storage layer per persistenza eventi
    storage: Option<Arc<Storage>>,
    /// Blocco corrente per timestamp eventi
    current_block: Arc<Mutex<u64>>,
    current_tx_hash: Arc<Mutex<[u8; 32]>>,
}

impl EventSystem {
    ///
    /// An event system is created per contract execution
    /// e raccoglie gli eventi emessi durante l'esecuzione.
    pub fn new() -> Self {
        Self {
            events_buffer: Arc::new(Mutex::new(VecDeque::new())),
            custom_events_buffer: Arc::new(Mutex::new(VecDeque::new())),
            storage: None,
            current_block: Arc::new(Mutex::new(0)),
            current_tx_hash: Arc::new(Mutex::new([0u8; 32])),
        }
    }

    ///
    /// Versione che include il layer di storage per la persistenza degli eventi.
    ///
    /// # Arguments
    /// * `storage` - Storage layer per persistenza eventi
    pub fn with_storage(storage: Arc<Storage>, block_number: u64, tx_hash: [u8; 32]) -> Self {
        Self {
            events_buffer: Arc::new(Mutex::new(VecDeque::new())),
            custom_events_buffer: Arc::new(Mutex::new(VecDeque::new())),
            storage: Some(storage),
            current_block: Arc::new(Mutex::new(block_number)),
            current_tx_hash: Arc::new(Mutex::new(tx_hash)),
        }
    }

    ///
    /// # Arguments
    /// * `block_number` - Nuovo numero di blocco
    pub fn update_context(&self, block_number: u64, tx_hash: [u8; 32]) {
        if let Ok(mut block) = self.current_block.lock() {
            *block = block_number;
        }
        if let Ok(mut hash) = self.current_tx_hash.lock() {
            *hash = tx_hash;
        }
    }

    /// Compute l'hash di un evento per identificazione univoca
    ///
    /// # Arguments
    /// * `event_data` - Dati serializzati dell'evento
    ///
    /// # Returns
    /// Hash SHA-256 dell'evento
    fn calculate_event_hash(&self, event_data: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(event_data);
        let hash = hasher.finalize();
        let mut result = [0u8; 32];
        result.copy_from_slice(&hash);
        result
    }

    /// Computes the key di storage per un evento
    ///
    /// # Arguments
    /// * `event_hash` - Hash dell'evento
    ///
    /// # Returns
    /// Chiave di storage per l'evento
    fn calculate_event_key(&self, event_hash: [u8; 32], block_number: u64) -> Vec<u8> {
        // Chiave = "event" || event_hash || block_number (little-endian)
        let mut key = Vec::with_capacity(5 + 32 + 8);
        key.extend_from_slice(b"event");
        key.extend_from_slice(&event_hash);
        key.extend_from_slice(&block_number.to_le_bytes());
        key
    }

    /// Persiste gli eventi nel buffer in the storage
    ///
    /// Gli eventi are associati al blocco e transazione correnti.
    ///
    /// # Returns
    pub fn persist_events(&self) -> Result<u64, String> {
        let storage = match &self.storage {
            Some(s) => s,
            None => return Err("No storage available for event persistence".to_string()),
        };

        let block_number = match self.current_block.lock() {
            Ok(block) => *block,
            Err(_) => return Err("Failed to get current block number".to_string()),
        };

        let tx_hash = match self.current_tx_hash.lock() {
            Ok(hash) => *hash,
            Err(_) => return Err("Failed to get current transaction hash".to_string()),
        };

        let mut persisted_count = 0u64;
        let mut event_hashes: Vec<[u8; 32]> = Vec::new();

        // Load existing event hashes for this block (if any)
        let block_key = format!("block_events_{}", block_number);
        if let Ok(Some(existing_bytes)) = storage.get(block_key.as_bytes()) {
            if let Ok(existing_hashes) = bincode::deserialize::<Vec<[u8; 32]>>(&existing_bytes) {
                event_hashes = existing_hashes;
            }
        }

        // Persisti eventi standard
        if let Ok(mut buffer) = self.events_buffer.lock() {
            for event in buffer.drain(..) {
                let event_data = match bincode::serialize(&event) {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("Failed to serialize standard event: {}", e);
                        continue;
                    }
                };

                let event_hash = self.calculate_event_hash(&event_data);
                let stored_event = StoredEvent {
                    event_type: EventType::Standard,
                    event_data,
                    block_number,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    transaction_hash: tx_hash,
                    contract_address: self.extract_contract_address_from_standard_event(&event),
                };

                let key = self.calculate_event_key(event_hash, block_number);

                if let Err(e) = storage.put(&key, &bincode::serialize(&stored_event).unwrap()) {
                    eprintln!("Failed to persist standard event: {}", e);
                } else {
                    event_hashes.push(event_hash);
                    persisted_count += 1;
                }
            }
        }

        // Persisti eventi custom
        if let Ok(mut buffer) = self.custom_events_buffer.lock() {
            for event in buffer.drain(..) {
                let event_data = match bincode::serialize(&event) {
                    Ok(data) => data,
                    Err(e) => {
                        eprintln!("Failed to serialize custom event: {}", e);
                        continue;
                    }
                };

                let event_hash = self.calculate_event_hash(&event_data);
                let stored_event = StoredEvent {
                    event_type: EventType::Custom,
                    event_data,
                    block_number,
                    timestamp: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs(),
                    transaction_hash: tx_hash,
                    contract_address: self.extract_contract_address_from_custom_event(&event),
                };

                let key = self.calculate_event_key(event_hash, block_number);

                if let Err(e) = storage.put(&key, &bincode::serialize(&stored_event).unwrap()) {
                    eprintln!("Failed to persist custom event: {}", e);
                } else {
                    event_hashes.push(event_hash);
                    persisted_count += 1;
                }
            }
        }

        // Write the block event index so query_events can find these events
        if !event_hashes.is_empty() {
            if let Ok(index_data) = bincode::serialize(&event_hashes) {
                if let Err(e) = storage.put(block_key.as_bytes(), &index_data) {
                    eprintln!("Failed to persist block event index: {}", e);
                }
            }
        }

        Ok(persisted_count)
    }

    fn extract_contract_address_from_standard_event(&self, event: &StandardEvent) -> [u8; 32] {
        match event {
            StandardEvent::OwnershipTransferred { new_owner, .. } => self.parse_address(new_owner),
            StandardEvent::Paused { account } => self.parse_address(account),
            StandardEvent::Unpaused { account } => self.parse_address(account),
            StandardEvent::Upgraded {
                contract_address, ..
            } => self.parse_address(contract_address),
            StandardEvent::GovernanceHookTriggered {
                contract_address, ..
            } => self.parse_address(contract_address),
            StandardEvent::FeeHookTriggered {
                contract_address, ..
            } => self.parse_address(contract_address),
            StandardEvent::CallbackTriggered {
                callback_id,
                payload_hash,
                ..
            } => {
                // CallbackTriggered non ha contract_address diretto, ma possiamo derivarlo
                // dal callback_id o dal payload_hash se seguono un formato specifico
                if let Some((address_part, _)) = callback_id.split_once(':') {
                    self.parse_address(address_part)
                } else if let Ok(payload_bytes) =
                    hex::decode(payload_hash.strip_prefix("0x").unwrap_or(payload_hash))
                {
                    if payload_bytes.len() >= 32 {
                        // Se il payload hash decodificato è abbastanza lungo, usa i primi 32 bytes.
                        let mut address = [0u8; 32];
                        address.copy_from_slice(&payload_bytes[..32]);
                        address
                    } else {
                        use sha2::Digest;
                        let mut hasher = sha2::Sha256::new();
                        hasher.update(callback_id.as_bytes());
                        let hash = hasher.finalize();
                        let mut address = [0u8; 32];
                        address.copy_from_slice(&hash);
                        address
                    }
                } else {
                    // Fallback: usa un hash of the callback_id per generare un address deterministico
                    use sha2::{Digest, Sha256};
                    let mut hasher = Sha256::new();
                    hasher.update(callback_id.as_bytes());
                    let hash = hasher.finalize();
                    let mut address = [0u8; 32];
                    address.copy_from_slice(&hash);
                    address
                }
            }
        }
    }

    fn extract_contract_address_from_custom_event(&self, event: &CustomEvent) -> [u8; 32] {
        self.parse_address(&event.contract_address)
    }

    /// Converte un address stringa in [u8; 32]
    fn parse_address(&self, address_str: &str) -> [u8; 32] {
        let address_hex = address_str.strip_prefix("0x").unwrap_or(address_str);
        if let Ok(bytes) = hex::decode(address_hex) {
            if bytes.len() == 32 {
                let mut result = [0u8; 32];
                result.copy_from_slice(&bytes);
                result
            } else {
                [0u8; 32] // Invalid address
            }
        } else {
            [0u8; 32] // Invalid address
        }
    }

    /// Emette un evento standard
    ///
    /// Registra l'evento nel buffer degli eventi. Gli eventi are emessi
    /// automaticamente dalle funzioni BaseContract:
    /// - `OwnershipTransferred`: quando l'ownership viene trasferita
    /// - `GovernanceHookTriggered`: quando un governance hook viene triggerato
    /// - `FeeHookTriggered`: quando un fee hook viene triggerato
    /// - `CallbackTriggered`: quando un callback HTTP viene triggerato
    ///
    /// # Arguments
    /// * `event` - L'evento standard da emettere
    /// * `gas_meter` - Gas meter opzionale per consumare gas per LOG
    ///
    /// # Note
    /// Gli eventi are registrati in memoria durante l'esecuzione.
    /// L'integrazione completa con il sistema di storage/blocchi per la persistenza
    /// degli eventi sarà implementata in futuro.
    ///
    /// # Gas Cost
    /// Consuma gas per LOG se gas_meter è fornito:
    /// - Base cost: 375 gas
    /// - Topics aggiuntivi: 375 gas per topic (il primo è incluso nel base cost)
    /// - Data: 8 gas per byte
    pub fn emit_standard_event(&self, event: StandardEvent, gas_meter: Option<&mut GasMeter>) {
        // Compute numero di topics e data length per calcolo gas
        let (topics_count, data_len) = match &event {
            StandardEvent::OwnershipTransferred { .. } => (2, 0), // previous_owner, new_owner
            StandardEvent::Paused { .. } => (1, 0),
            StandardEvent::Unpaused { .. } => (1, 0),
            StandardEvent::Upgraded { .. } => (2, 0), // contract_address, new_version
            StandardEvent::GovernanceHookTriggered { .. } => (3, 0), // contract_address, proposal_id, action_type
            StandardEvent::FeeHookTriggered { .. } => (2, 32), // contract_address, caller, amount (u128 = 16 bytes, padded to 32)
            StandardEvent::CallbackTriggered { payload, .. } => (2, payload.len()),
        };

        // Consuma gas per LOG se gas_meter è fornito
        if let Some(gas_meter) = gas_meter {
            if let Err(e) = gas_meter.consume_log(topics_count, data_len) {
                // Se il gas è insufficiente, non emettere l'evento
                eprintln!("Failed to consume LOG gas: {}", e);
                return;
            }
        }

        // Registra l'evento nel buffer
        if let Ok(mut buffer) = self.events_buffer.lock() {
            buffer.push_back(event);
        }
    }

    /// Emette un evento custom
    ///
    /// Gli eventi custom permettono ai contratti di emettere eventi personalizzati
    /// oltre agli eventi standard BaseContract.
    ///
    /// # Arguments
    /// * `event` - L'evento custom da emettere
    /// * `gas_meter` - Gas meter opzionale per consumare gas per LOG
    ///
    /// # Note
    /// Gli eventi custom are registrati in memoria durante l'esecuzione.
    /// L'integrazione completa con il sistema di storage/blocchi per la persistenza
    /// degli eventi sarà implementata in futuro.
    ///
    /// # Gas Cost
    /// Consuma gas per LOG se gas_meter è fornito:
    /// - Base cost: 375 gas
    /// - Topics aggiuntivi: 375 gas per topic (il primo è incluso nel base cost)
    /// - Data: 8 gas per byte
    pub fn emit_custom_event(&self, event: CustomEvent, gas_meter: Option<&mut GasMeter>) {
        // Compute numero di topics e data length per calcolo gas
        let topics_count = event.topics.len();
        let data_len = event.data.len();

        // Consuma gas per LOG se gas_meter è fornito
        if let Some(gas_meter) = gas_meter {
            if let Err(e) = gas_meter.consume_log(topics_count, data_len) {
                // Se il gas è insufficiente, non emettere l'evento
                eprintln!("Failed to consume LOG gas: {}", e);
                return;
            }
        }

        // Registra l'evento custom nel buffer
        if let Ok(mut buffer) = self.custom_events_buffer.lock() {
            buffer.push_back(event);
        }
    }

    /// Query events by contract
    ///
    ///
    /// # Arguments
    /// * `from_block` - Blocco iniziale (inclusivo)
    /// * `to_block` - Blocco finale (inclusivo)
    ///
    /// # Returns
    ///
    /// # Note
    /// contract_address e range di blocchi. Returns sia eventi standard
    /// che custom.
    pub fn query_events(
        &self,
        contract_address: &str,
        from_block: u64,
        to_block: u64,
    ) -> EventQueryResult {
        let storage = match &self.storage {
            Some(s) => s,
            None => {
                return EventQueryResult {
                    events: vec![],
                    total_count: 0,
                    first_block: 0,
                    last_block: 0,
                };
            }
        };

        let contract_address_bytes = self.parse_address(contract_address);

        let mut events = Vec::new();
        let mut total_count = 0u64;
        let mut first_block_found = None;
        let mut last_block_found = None;

        for block_number in from_block..=to_block {
            let block_key = format!("block_events_{}", block_number);

            const MAX_EVENT_DATA_SIZE: usize = 4 * 1024 * 1024;

            if let Ok(Some(event_hashes_bytes)) = storage.get(block_key.as_bytes()) {
                if event_hashes_bytes.len() > MAX_EVENT_DATA_SIZE {
                    continue;
                }
                let event_hashes: Vec<[u8; 32]> = match bincode::deserialize(&event_hashes_bytes) {
                    Ok(hashes) => hashes,
                    Err(_) => continue,
                };

                for event_hash in event_hashes {
                    // Computes the key dell'evento
                    let event_key = self.calculate_event_key(event_hash, block_number);

                    if let Ok(Some(event_data)) = storage.get(&event_key) {
                        if event_data.len() > MAX_EVENT_DATA_SIZE {
                            continue;
                        }
                        if let Ok(stored_event) = bincode::deserialize::<StoredEvent>(&event_data) {
                            // Filtra per contract_address
                            if stored_event.contract_address == contract_address_bytes {
                                // Validate the event data can be deserialized
                                let valid = if stored_event.event_type == EventType::Custom {
                                    if stored_event.event_data.len() > MAX_EVENT_DATA_SIZE {
                                        false
                                    } else {
                                        bincode::deserialize::<CustomEvent>(
                                            &stored_event.event_data,
                                        )
                                        .is_ok()
                                    }
                                } else {
                                    if stored_event.event_data.len() > MAX_EVENT_DATA_SIZE {
                                        false
                                    } else {
                                        bincode::deserialize::<StandardEvent>(
                                            &stored_event.event_data,
                                        )
                                        .is_ok()
                                    }
                                };
                                if !valid {
                                    continue;
                                }
                                events.push(stored_event);

                                total_count += 1;

                                // Traccia i limiti of the blocco
                                if first_block_found.is_none() {
                                    first_block_found = Some(block_number);
                                }
                                last_block_found = Some(block_number);
                            }
                        }
                    }
                }
            }
        }

        EventQueryResult {
            events,
            total_count,
            first_block: first_block_found.unwrap_or(0),
            last_block: last_block_found.unwrap_or(0),
        }
    }

    /// Query custom events by contract
    ///
    ///
    /// # Arguments
    /// * `from_block` - Blocco iniziale (inclusivo)
    /// * `to_block` - Blocco finale (inclusivo)
    ///
    /// # Returns
    pub fn query_custom_events(
        &self,
        contract_address: &str,
        from_block: u64,
        to_block: u64,
    ) -> Vec<CustomEvent> {
        let result = self.query_events(contract_address, from_block, to_block);

        const MAX_EVENT_DATA_SIZE: usize = 4 * 1024 * 1024;

        // Filtra solo eventi custom e deserializzali
        result
            .events
            .into_iter()
            .filter_map(|event| {
                if event.event_type == EventType::Custom {
                    if event.event_data.len() > MAX_EVENT_DATA_SIZE {
                        return None;
                    }
                    bincode::deserialize::<CustomEvent>(&event.event_data).ok()
                } else {
                    None
                }
            })
            .collect()
    }

    /// Query standard events by contract
    ///
    ///
    /// # Arguments
    /// * `from_block` - Blocco iniziale (inclusivo)
    /// * `to_block` - Blocco finale (inclusivo)
    ///
    /// # Returns
    pub fn query_standard_events(
        &self,
        contract_address: &str,
        from_block: u64,
        to_block: u64,
    ) -> Vec<StandardEvent> {
        let result = self.query_events(contract_address, from_block, to_block);

        const MAX_EVENT_DATA_SIZE: usize = 4 * 1024 * 1024;

        // Filtra solo eventi standard e deserializzati
        result
            .events
            .into_iter()
            .filter_map(|event| {
                if event.event_type == EventType::Standard {
                    if event.event_data.len() > MAX_EVENT_DATA_SIZE {
                        return None;
                    }
                    bincode::deserialize::<StandardEvent>(&event.event_data).ok()
                } else {
                    None
                }
            })
            .collect()
    }

    /// Query eventi per nome evento
    ///
    ///
    /// # Arguments
    /// * `event_name` - Nome dell'evento da cercare
    /// * `from_block` - Blocco iniziale (inclusivo)
    /// * `to_block` - Blocco finale (inclusivo)
    ///
    /// # Returns
    /// Vettore di eventi custom con il nome specificato
    pub fn query_events_by_name(
        &self,
        contract_address: &str,
        event_name: &str,
        from_block: u64,
        to_block: u64,
    ) -> Vec<CustomEvent> {
        let custom_events = self.query_custom_events(contract_address, from_block, to_block);

        // Filtra per nome evento
        custom_events
            .into_iter()
            .filter(|event| event.event_name == event_name)
            .collect()
    }

    /// Ottiene gli eventi standard emessi durante l'esecuzione corrente
    ///
    /// Utile per recuperare gli eventi emessi durante l'esecuzione di una transazione.
    ///
    /// # Returns
    /// Vettore di eventi standard emessi durante l'esecuzione corrente
    pub fn get_standard_events(&self) -> Vec<StandardEvent> {
        if let Ok(buffer) = self.events_buffer.lock() {
            buffer.iter().cloned().collect()
        } else {
            vec![]
        }
    }

    /// Ottiene gli eventi custom emessi durante l'esecuzione corrente
    ///
    /// Utile per recuperare gli eventi emessi durante l'esecuzione di una transazione.
    ///
    /// # Returns
    /// Vettore di eventi custom emessi durante l'esecuzione corrente
    pub fn get_custom_events(&self) -> Vec<CustomEvent> {
        if let Ok(buffer) = self.custom_events_buffer.lock() {
            buffer.iter().cloned().collect()
        } else {
            vec![]
        }
    }

    /// Pulisce il buffer degli eventi
    ///
    pub fn clear_events(&self) {
        if let Ok(mut buffer) = self.events_buffer.lock() {
            buffer.clear();
        }
        if let Ok(mut buffer) = self.custom_events_buffer.lock() {
            buffer.clear();
        }
    }
}

impl Default for EventSystem {
    fn default() -> Self {
        Self::new()
    }
}
