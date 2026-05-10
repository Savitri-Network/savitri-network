//! Oracle Registry: Smart contract per gestione Oracle feed e ACL
//!
//! - Registrazione feed Oracle
//! - ACL (Access Control List) con ruoli writer/reader/auditor
//! - Integrazione con governance per modifiche ACL
//! - Query feed
//!
//! # Storage Layout
//! - Slot 0-99: BaseContract (riservato)
//! - Slot 100: next_feed_id (u64)
//! - Slot 101+: feeds[feed_id] -> feed_data (mapping)
//! - Slot 200+: acl[feed_id][address] -> role (nested mapping)

use crate::contracts::base::BaseContract;
use crate::contracts::gas::GasMeter;
use crate::contracts::storage::ContractStorage;
use crate::oracle::feed::Feed;
use crate::oracle::schema::{Schema, SchemaId, SchemaRegistry};
use crate::oracle::types::{OracleConfig, OracleError, OracleRole};
use crate::storage::Storage;
use anyhow::{Context, Result};
use bincode;
use hex;
use sha3::{Digest, Keccak256};

/// Slot per next_feed_id
const SLOT_NEXT_FEED_ID: u64 = 100;

/// Slot base per feeds mapping
const SLOT_FEEDS_BASE: u64 = 101;

/// Slot base per ACL mapping (nested: feed_id -> address -> role)
const SLOT_ACL_BASE: u64 = 200;

/// Slot base per connector mapping
const SLOT_CONNECTOR_BASE: u64 = 300;

/// Oracle Registry Contract
///
/// Gestisce la registrazione e l'accesso ai feed Oracle con ACL.
pub struct OracleRegistry;

impl OracleRegistry {
    /// Risolve uno schema partendo dallo storage; fallback ai predefiniti
    /// Maximum allowed size for oracle deserialization (1 MB).
    const MAX_ORACLE_DESERIALIZE_SIZE: usize = 1 * 1024 * 1024;

    fn load_schema(storage: &Storage, schema_id: &SchemaId) -> Result<Schema> {
        if let Some(schema) = storage.get_oracle_schema(schema_id)? {
            if schema.len() > Self::MAX_ORACLE_DESERIALIZE_SIZE {
                anyhow::bail!(
                    "Oracle schema data too large: {} bytes (max {})",
                    schema.len(),
                    Self::MAX_ORACLE_DESERIALIZE_SIZE
                );
            }
            let schema: Schema =
                bincode::deserialize(&schema).context("Failed to deserialize oracle schema")?;
            return Ok(schema);
        }

        let registry = SchemaRegistry::new();
        registry
            .get(schema_id)
            .cloned()
            .ok_or_else(|| OracleError::SchemaNotFound(hex::encode(schema_id)).into())
    }

    /// Converte u64 a storage value (32 bytes, little-endian)
    fn u64_to_storage_value(value: u64) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[..8].copy_from_slice(&value.to_le_bytes());
        bytes
    }

    /// Converte storage value (32 bytes) a u64 (little-endian)
    fn storage_value_to_u64(value: &[u8]) -> Result<u64> {
        if value.len() < 8 {
            anyhow::bail!("Storage value too short for u64");
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&value[..8]);
        Ok(u64::from_le_bytes(bytes))
    }
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Gas meter opzionale
    pub fn initialize(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        owner_address: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Inizializza BaseContract
        BaseContract::initialize(
            contract_storage,
            storage,
            owner_address,
            gas_meter.as_deref_mut(),
        )?;

        // Inizializza next_feed_id a 1
        let next_id_value = Self::u64_to_storage_value(1);
        contract_storage
            .sstore(
                storage,
                SLOT_NEXT_FEED_ID,
                next_id_value,
                gas_meter.as_deref_mut(),
            )
            .context("Failed to initialize next_feed_id")?;

        Ok(())
    }

    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Gas meter opzionale
    ///
    /// # Returns
    /// Nuovo feed_id (32 bytes)
    pub fn next_feed_id(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<[u8; 32]> {
        // Leggi next_feed_id
        let value = contract_storage
            .sload(storage, SLOT_NEXT_FEED_ID, gas_meter.as_deref_mut())
            .context("Failed to read next_feed_id")?;

        let next_id = Self::storage_value_to_u64(&value)?;

        // Incrementa e salva
        let new_id = next_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("feed_id overflow"))?;
        let new_id_value = Self::u64_to_storage_value(new_id);
        contract_storage
            .sstore(
                storage,
                SLOT_NEXT_FEED_ID,
                new_id_value,
                gas_meter.as_deref_mut(),
            )
            .context("Failed to update next_feed_id")?;

        // Genera feed_id deterministico: hash(contract_address || next_id)
        let contract_address = contract_storage.contract_address();
        let mut hasher = Keccak256::new();
        hasher.update(contract_address);
        hasher.update(&next_id.to_le_bytes());
        let hash = hasher.finalize();
        let mut feed_id = [0u8; 32];
        feed_id.copy_from_slice(&hash);

        Ok(feed_id)
    }

    fn feed_slot(feed_id: &[u8; 32]) -> u64 {
        // Slot = keccak256(feed_id || SLOT_FEEDS_BASE)
        let mut hasher = Keccak256::new();
        hasher.update(feed_id);
        hasher.update(&SLOT_FEEDS_BASE.to_le_bytes());
        let hash = hasher.finalize();
        // Prendi i primi 8 bytes e converti in u64
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&hash[..8]);
        u64::from_le_bytes(bytes)
    }

    fn acl_slot(feed_id: &[u8; 32], address: &[u8]) -> u64 {
        // Slot intermedio per feed_id
        let mut hasher1 = Keccak256::new();
        hasher1.update(feed_id);
        hasher1.update(&SLOT_ACL_BASE.to_le_bytes());
        let intermediate = hasher1.finalize();

        // Slot finale per address
        let mut hasher2 = Keccak256::new();
        hasher2.update(address);
        hasher2.update(&intermediate);
        let hash = hasher2.finalize();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&hash[..8]);
        u64::from_le_bytes(bytes)
    }

    fn acl_storage_key(feed_id: &[u8; 32], address: &[u8]) -> Vec<u8> {
        let mut key = Vec::with_capacity(feed_id.len() + address.len());
        key.extend_from_slice(feed_id);
        key.extend_from_slice(address);
        key
    }

    /// Registra un feed Oracle
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `feed` - Feed da registrare
    /// * `current_time` - Timestamp corrente (per TTL e future tolerance)
    /// * `config` - Configurazione Oracle (default se None)
    /// * `gas_meter` - Gas meter opzionale
    pub fn register_feed(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        feed: &Feed,
        caller: &[u8],
        current_time: u64,
        config: Option<&OracleConfig>,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        let is_owner = caller == owner.as_slice();

        if !is_owner {
            let role = Self::get_acl_role(
                contract_storage,
                storage,
                &feed.feed_id,
                caller,
                gas_meter.as_deref_mut(),
            )?;
            if role != Some(OracleRole::Writer) {
                return Err(OracleError::PermissionDenied {
                    address: caller.to_vec(),
                    role: role.unwrap_or(OracleRole::Reader), // Default a Reader se non trovato
                    action: "register_feed".to_string(),
                }
                .into());
            }
        }

        // Carica/risolve schema (persistendo quello predefinito se assente)
        let schema = Self::load_schema(storage, &feed.schema_id)?;
        let schema_bytes =
            bincode::serialize(&schema).context("Failed to serialize oracle schema")?;
        storage
            .put_oracle_schema(&schema.id, &schema_bytes)
            .context("Failed to persist oracle schema")?;

        let config = config.cloned().unwrap_or_default();
        config
            .validate()
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        feed.validate(&schema, &config, current_time)?;

        if let Some(max_seq) = storage.get_oracle_max_sequence(&feed.feed_id)? {
            if feed.proof.sequence <= max_seq {
                return Err(OracleError::ReplayAttack {
                    feed_id: hex::encode(feed.feed_id),
                    sequence: feed.proof.sequence,
                }
                .into());
            }
        }

        // Salva feed in the contract storage
        let slot = Self::feed_slot(&feed.feed_id);
        let feed_bytes = bincode::serialize(feed).context("Failed to serialize feed")?;

        // Padding a 32 bytes per storage
        let mut storage_value = vec![0u8; 32];
        if feed_bytes.len() <= 32 {
            storage_value[..feed_bytes.len()].copy_from_slice(&feed_bytes);
        } else {
            let hash = Keccak256::digest(&feed_bytes);
            storage_value[..32].copy_from_slice(&hash);
        }

        contract_storage
            .sstore(storage, slot, storage_value, gas_meter.as_deref_mut())
            .context("Failed to store feed")?;

        storage
            .put_oracle_feed(&feed.feed_id, feed.proof.sequence, &feed_bytes)
            .context("Failed to store feed in storage layer")?;

        storage
            .put_oracle_max_sequence(&feed.feed_id, feed.proof.sequence)
            .context("Failed to update oracle max sequence")?;

        Ok(())
    }

    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `feed_id` - ID of the feed
    /// * `gas_meter` - Gas meter opzionale
    pub fn get_feed(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        feed_id: &[u8; 32],
        caller: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<Option<Feed>> {
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        let is_owner = caller == owner.as_slice();

        if !is_owner {
            let role = Self::get_acl_role(
                contract_storage,
                storage,
                feed_id,
                caller,
                gas_meter.as_deref_mut(),
            )?;
            if role.is_none() {
                return Err(OracleError::PermissionDenied {
                    address: caller.to_vec(),
                    role: OracleRole::Reader, // Default
                    action: "get_feed".to_string(),
                }
                .into());
            }
        }

        let Some(sequence) = storage
            .get_oracle_max_sequence(feed_id)
            .context("Failed to get oracle max sequence")?
        else {
            return Ok(None);
        };
        let Some(feed_bytes) = storage
            .get_oracle_feed(feed_id, sequence)
            .context("Failed to get feed from storage")?
        else {
            return Ok(None);
        };
        if feed_bytes.len() > Self::MAX_ORACLE_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Oracle feed data too large: {} bytes (max {})",
                feed_bytes.len(),
                Self::MAX_ORACLE_DESERIALIZE_SIZE
            );
        }
        let feed: Feed = bincode::deserialize(&feed_bytes).context("Failed to deserialize feed")?;
        Ok(Some(feed))
    }

    /// Set un ruolo ACL per un feed
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `feed_id` - ID of the feed
    /// * `address` - Address a cui assegnare il ruolo
    /// * `role` - Ruolo da assegnare
    /// * `gas_meter` - Gas meter opzionale
    pub fn set_acl_role(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        feed_id: &[u8; 32],
        address: &[u8],
        role: OracleRole,
        caller: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Check permessi: solo owner o auditor possono modificare ACL
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        let is_owner = caller == owner.as_slice();

        if !is_owner {
            let caller_role = Self::get_acl_role(
                contract_storage,
                storage,
                feed_id,
                caller,
                gas_meter.as_deref_mut(),
            )?;
            if caller_role != Some(OracleRole::Auditor) {
                return Err(OracleError::PermissionDenied {
                    address: caller.to_vec(),
                    role: caller_role.unwrap_or(OracleRole::Reader),
                    action: "set_acl_role".to_string(),
                }
                .into());
            }
        }

        // Salva ruolo in the contract storage
        let slot = Self::acl_slot(feed_id, address);
        let role_value = match role {
            OracleRole::Writer => Self::u64_to_storage_value(1),
            OracleRole::Reader => Self::u64_to_storage_value(2),
            OracleRole::Auditor => Self::u64_to_storage_value(3),
        };

        contract_storage
            .sstore(storage, slot, role_value, gas_meter.as_deref_mut())
            .context("Failed to store ACL role")?;

        // Salva anche in the storage layer
        let acl_key = Self::acl_storage_key(feed_id, address);
        let acl_bytes = bincode::serialize(&role).context("Failed to serialize ACL role")?;
        storage
            .put_oracle_acl(&acl_key, &acl_bytes)
            .context("Failed to store ACL in storage layer")?;

        Ok(())
    }

    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `feed_id` - ID of the feed
    /// * `address` - Address da verificare
    /// * `gas_meter` - Gas meter opzionale
    pub fn get_acl_role(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        feed_id: &[u8; 32],
        address: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<Option<OracleRole>> {
        let acl_key = Self::acl_storage_key(feed_id, address);
        if let Ok(Some(role_bytes)) = storage.get_oracle_acl(&acl_key) {
            if role_bytes.len() > Self::MAX_ORACLE_DESERIALIZE_SIZE {
                anyhow::bail!(
                    "Oracle ACL role data too large: {} bytes (max {})",
                    role_bytes.len(),
                    Self::MAX_ORACLE_DESERIALIZE_SIZE
                );
            }
            let role: OracleRole =
                bincode::deserialize(&role_bytes).context("Failed to deserialize ACL role")?;
            return Ok(Some(role));
        }

        // Fallback: leggi dallo contract storage
        let slot = Self::acl_slot(feed_id, address);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .context("Failed to read ACL role")?;

        if value.iter().all(|&b| b == 0) {
            return Ok(None);
        }

        let role_num = Self::storage_value_to_u64(&value)?;
        let role = match role_num {
            1 => OracleRole::Writer,
            2 => OracleRole::Reader,
            3 => OracleRole::Auditor,
            _ => return Err(anyhow::anyhow!("Invalid role number: {}", role_num)),
        };

        Ok(Some(role))
    }

    fn connector_slot(connector_id: &str) -> u64 {
        // Slot = keccak256(connector_id || SLOT_CONNECTOR_BASE)
        let mut hasher = Keccak256::new();
        hasher.update(connector_id.as_bytes());
        hasher.update(&SLOT_CONNECTOR_BASE.to_le_bytes());
        let hash = hasher.finalize();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&hash[..8]);
        u64::from_le_bytes(bytes)
    }

    /// Registra un connector in the whitelist
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `connector_id` - ID of the connector (string)
    /// * `pubkey` - Public key of the connector (32 bytes)
    /// * `config` - Configurazione of the connector
    /// * `current_time` - Timestamp corrente
    /// * `gas_meter` - Gas meter opzionale
    pub fn register_connector(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        connector_id: &str,
        pubkey: &[u8; 32],
        config: &crate::oracle::types::ConnectorConfig,
        caller: &[u8],
        current_time: u64,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Check permessi: solo owner o governance
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        let is_owner = caller == owner.as_slice();

        if !is_owner {
            return Err(OracleError::PermissionDenied {
                address: caller.to_vec(),
                role: OracleRole::Reader,
                action: "register_connector".to_string(),
            }
            .into());
        }

        config
            .validate()
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;

        // Check che connector non sia già registrato
        if Self::is_connector_whitelisted(
            contract_storage,
            storage,
            connector_id,
            gas_meter.as_deref_mut(),
        )? {
            anyhow::bail!("Connector {} already registered", connector_id);
        }

        // Salva connector info in the storage layer
        let connector_payload = bincode::serialize(&(pubkey.to_vec(), config, current_time))
            .context("Failed to serialize connector payload")?;
        storage
            .put_connector_info(connector_id.as_bytes(), &connector_payload)
            .context("Failed to store connector info")?;

        let slot = Self::connector_slot(connector_id);
        let connector_data = crate::connectors::ConnectorInfo {
            id: connector_id.as_bytes().to_vec(),
            name: connector_id.to_string(),
            endpoint: String::new(),
            pubkey: pubkey.to_vec(),
            registered_at: current_time,
            active: true,
            connector_id: connector_id.to_string(),
            config: bincode::serialize(config).context("Failed to serialize connector config")?,
        };
        let connector_bytes =
            bincode::serialize(&connector_data).context("Failed to serialize connector info")?;

        let mut storage_value = vec![0u8; 32];
        if connector_bytes.len() <= 32 {
            storage_value[..connector_bytes.len()].copy_from_slice(&connector_bytes);
        } else {
            let hash = Keccak256::digest(&connector_bytes);
            storage_value[..32].copy_from_slice(&hash);
        }

        contract_storage
            .sstore(storage, slot, storage_value, gas_meter.as_deref_mut())
            .context("Failed to store connector in contract storage")?;

        Ok(())
    }

    /// Check se un connector è whitelisted
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `connector_id` - ID of the connector
    /// * `gas_meter` - Gas meter opzionale
    ///
    /// # Returns
    /// `true` se whitelisted, `false` altrimenti
    pub fn is_connector_whitelisted(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        connector_id: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        if storage.connector_exists(connector_id.as_bytes())? {
            return Ok(true);
        }

        // Fallback: check in the contract storage
        let slot = Self::connector_slot(connector_id);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .context("Failed to read connector from contract storage")?;

        Ok(!value.iter().all(|&b| b == 0))
    }

    /// Rimuove un connector dalla whitelist
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Gas meter opzionale
    pub fn remove_connector(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        connector_id: &str,
        caller: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Check permessi: solo owner o governance
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        let is_owner = caller == owner.as_slice();

        if !is_owner {
            return Err(OracleError::PermissionDenied {
                address: caller.to_vec(),
                role: OracleRole::Reader,
                action: "remove_connector".to_string(),
            }
            .into());
        }

        // Check che connector esista
        if !Self::is_connector_whitelisted(
            contract_storage,
            storage,
            connector_id,
            gas_meter.as_deref_mut(),
        )? {
            anyhow::bail!("Connector {} not found", connector_id);
        }

        // Rimuovi dallo storage layer
        storage
            .delete_connector_info(connector_id.as_bytes())
            .context("Failed to delete connector info")?;

        let slot = Self::connector_slot(connector_id);
        let zero_value = vec![0u8; 32];
        contract_storage
            .sstore(storage, slot, zero_value, gas_meter.as_deref_mut())
            .context("Failed to remove connector from contract storage")?;

        Ok(())
    }
}
