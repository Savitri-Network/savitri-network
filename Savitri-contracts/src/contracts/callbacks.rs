//! Callback Registry: Smart contract per gestione callbacks HTTP sicuri
//!
//! - Registrazione callbacks HTTP con event_type, url, auth_header, retry_policy
//! - Whitelist di domini URL consentiti (configurabile)
//! - Validazione URL (reject localhost/private IP)
//! - Emissione eventi CallbackTriggered quando un callback viene triggerato
//! - Determinismo: callback NON influenza stato on-chain (fire-and-forget async)
//!
//! # Storage Layout
//! - Slot 0-99: BaseContract (riservato)
//! - Slot 100: next_callback_id (u64)
//! - Slot 101: whitelist_enabled (bool)
//! - Slot 102+: callbacks[callback_id] -> callback_data (mapping)
//! - Slot 200+: whitelist_domains[domain_hash] -> enabled (mapping)
//!
//! # Policy
//! - Timeout: 10s
//! - Max payload: 1MB
//! - Max retry: 3

use crate::contracts::base::BaseContract;
use crate::contracts::events::{EventSystem, StandardEvent};
use crate::contracts::gas::GasMeter;
use crate::contracts::storage::ContractStorage;
use crate::storage::Storage;
use anyhow::{Context, Result};
use bincode;
use hex;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Keccak256};

/// Slot per next_callback_id
const SLOT_NEXT_CALLBACK_ID: u64 = 100;

/// Slot per whitelist_enabled
const SLOT_WHITELIST_ENABLED: u64 = 101;

/// Slot base per callbacks mapping
const SLOT_CALLBACKS_BASE: u64 = 102;

/// Slot base per whitelist domains mapping
const SLOT_WHITELIST_BASE: u64 = 200;

/// Max payload size: 1MB
const MAX_PAYLOAD_SIZE: usize = 1_048_576;

/// Max retry count
const MAX_RETRY_COUNT: u8 = 3;

/// Timeout in seconds
#[allow(dead_code)]
const TIMEOUT_SECONDS: u64 = 10;

/// Dati di un callback registrato
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackData {
    /// Tipo di evento che triggera il callback
    pub event_type: String,
    /// URL of the callback HTTP
    pub url: String,
    /// Header di autenticazione (opzionale)
    pub auth_header: Option<String>,
    /// Policy di retry (numero massimo di tentativi)
    pub retry_policy: u8,
    /// Indirizzo of the registrante
    pub registrant: Vec<u8>,
}

/// Callback Registry Contract
///
/// Gestisce la registrazione e il triggering di callbacks HTTP sicuri.
pub struct CallbackRegistry;

impl CallbackRegistry {
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

    /// Converte bool a storage value (32 bytes)
    fn bool_to_storage_value(value: bool) -> Vec<u8> {
        let mut bytes = vec![0u8; 32];
        bytes[0] = if value { 1 } else { 0 };
        bytes
    }

    /// Converte storage value (32 bytes) a bool
    fn storage_value_to_bool(value: &[u8]) -> Result<bool> {
        Ok(value[0] != 0)
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

        // Inizializza next_callback_id a 1
        let next_id_value = Self::u64_to_storage_value(1);
        contract_storage
            .sstore(
                storage,
                SLOT_NEXT_CALLBACK_ID,
                next_id_value,
                gas_meter.as_deref_mut(),
            )
            .context("Failed to initialize next_callback_id")?;

        // Inizializza whitelist_enabled a true (default: whitelist abilitata)
        let whitelist_enabled_value = Self::bool_to_storage_value(true);
        contract_storage
            .sstore(
                storage,
                SLOT_WHITELIST_ENABLED,
                whitelist_enabled_value,
                gas_meter.as_deref_mut(),
            )
            .context("Failed to initialize whitelist_enabled")?;

        Ok(())
    }

    ///
    /// Check che:
    /// - L'URL sia valido
    /// - Non sia localhost o IP privato
    /// - Il dominio sia in the whitelist (se whitelist è abilitata)
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Gas meter opzionale
    fn validate_url(
        url: &str,
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Parse URL base (without dipendenze esterne)
        if !url.starts_with("http://") && !url.starts_with("https://") {
            anyhow::bail!("URL must start with http:// or https://");
        }

        // Estrai hostname dall'URL
        let hostname = Self::extract_hostname(url)?;

        // Check che non sia localhost
        if hostname == "localhost" || hostname == "127.0.0.1" || hostname == "::1" {
            anyhow::bail!("Localhost URLs are not allowed");
        }

        // Check che non sia un IP privato
        if Self::is_private_ip(&hostname) {
            anyhow::bail!("Private IP addresses are not allowed");
        }

        // Check whitelist se abilitata
        let whitelist_enabled_value = contract_storage
            .sload(storage, SLOT_WHITELIST_ENABLED, gas_meter.as_deref_mut())
            .context("Failed to read whitelist_enabled")?;
        let whitelist_enabled = Self::storage_value_to_bool(&whitelist_enabled_value)?;

        if whitelist_enabled {
            // Estrai dominio (without porta)
            let domain = Self::extract_domain(&hostname)?;
            if !Self::is_domain_whitelisted(
                contract_storage,
                storage,
                &domain,
                gas_meter.as_deref_mut(),
            )? {
                anyhow::bail!("Domain {} is not in whitelist", domain);
            }
        }

        Ok(())
    }

    /// Estrae l'hostname da un URL
    fn extract_hostname(url: &str) -> Result<String> {
        // Rimuovi protocollo
        let without_protocol = if url.starts_with("https://") {
            &url[8..]
        } else if url.starts_with("http://") {
            &url[7..]
        } else {
            anyhow::bail!("Invalid URL protocol");
        };

        // Trova la fine dell'hostname (prima di / o ? o #)
        let end = without_protocol
            .find('/')
            .or_else(|| without_protocol.find('?'))
            .or_else(|| without_protocol.find('#'))
            .unwrap_or(without_protocol.len());

        let hostname = &without_protocol[..end];
        Ok(hostname.to_string())
    }

    fn extract_domain(hostname: &str) -> Result<String> {
        // Rimuovi porta se presente
        let domain = if let Some(colon_pos) = hostname.find(':') {
            &hostname[..colon_pos]
        } else {
            hostname
        };
        Ok(domain.to_string())
    }

    /// Check se un hostname è un IP privato
    fn is_private_ip(hostname: &str) -> bool {
        // Check IP privati comuni
        // 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
        if hostname.starts_with("10.") {
            return true;
        }
        if hostname.starts_with("172.") {
            // Check range 172.16.0.0 - 172.31.255.255
            let parts: Vec<&str> = hostname.split('.').collect();
            if parts.len() >= 2 {
                if let Ok(second) = parts[1].parse::<u8>() {
                    if second >= 16 && second <= 31 {
                        return true;
                    }
                }
            }
        }
        if hostname.starts_with("192.168.") {
            return true;
        }
        // Link-local IPv6
        if hostname.starts_with("fe80:") || hostname.starts_with("169.254.") {
            return true;
        }
        false
    }

    fn callback_slot(callback_id: &[u8; 32]) -> u64 {
        // Slot = keccak256(callback_id || SLOT_CALLBACKS_BASE)
        let mut hasher = Keccak256::new();
        hasher.update(callback_id);
        hasher.update(&SLOT_CALLBACKS_BASE.to_le_bytes());
        let hash = hasher.finalize();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&hash[..8]);
        u64::from_le_bytes(bytes)
    }

    fn whitelist_domain_slot(domain: &str) -> u64 {
        // Slot = keccak256(domain || SLOT_WHITELIST_BASE)
        let mut hasher = Keccak256::new();
        hasher.update(domain.as_bytes());
        hasher.update(&SLOT_WHITELIST_BASE.to_le_bytes());
        let hash = hasher.finalize();
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&hash[..8]);
        u64::from_le_bytes(bytes)
    }

    /// Generates a raw storage key for callback data.
    ///
    /// Uses a prefix + callback_id to create a unique key for raw storage.
    fn callback_raw_key(callback_id: &[u8; 32]) -> Vec<u8> {
        let mut key = Vec::with_capacity(16 + 32);
        key.extend_from_slice(b"callback_data://");
        key.extend_from_slice(callback_id);
        key
    }

    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `event_type` - Tipo di evento che triggera il callback
    /// * `url` - URL of the callback HTTP
    /// * `auth_header` - Header di autenticazione (opzionale)
    /// * `retry_policy` - Numero massimo di tentativi (max 3)
    /// * `event_system` - Sistema eventi per emettere eventi
    /// * `gas_meter` - Gas meter opzionale
    ///
    /// # Returns
    /// callback_id (32 bytes)
    pub fn register(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        event_type: String,
        url: String,
        auth_header: Option<String>,
        retry_policy: u8,
        caller: &[u8],
        _event_system: &EventSystem,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<[u8; 32]> {
        // Validazione retry_policy
        if retry_policy > MAX_RETRY_COUNT {
            anyhow::bail!("retry_policy cannot exceed {}", MAX_RETRY_COUNT);
        }

        // Validazione URL
        Self::validate_url(&url, contract_storage, storage, gas_meter.as_deref_mut())?;

        // Genera nuovo callback_id
        let value = contract_storage
            .sload(storage, SLOT_NEXT_CALLBACK_ID, gas_meter.as_deref_mut())
            .context("Failed to read next_callback_id")?;

        let next_id = Self::storage_value_to_u64(&value)?;

        // Incrementa e salva
        let new_id = next_id
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("callback_id overflow"))?;
        let new_id_value = Self::u64_to_storage_value(new_id);
        contract_storage
            .sstore(
                storage,
                SLOT_NEXT_CALLBACK_ID,
                new_id_value,
                gas_meter.as_deref_mut(),
            )
            .context("Failed to update next_callback_id")?;

        // Genera callback_id deterministico: hash(contract_address || next_id || caller || event_type)
        let contract_address = contract_storage.contract_address();
        let mut hasher = Keccak256::new();
        hasher.update(contract_address);
        hasher.update(&next_id.to_le_bytes());
        hasher.update(caller);
        hasher.update(event_type.as_bytes());
        let hash = hasher.finalize();
        let mut callback_id = [0u8; 32];
        callback_id.copy_from_slice(&hash);

        // Creates CallbackData
        let callback_data = CallbackData {
            event_type: event_type.clone(),
            url: url.clone(),
            auth_header,
            retry_policy,
            registrant: caller.to_vec(),
        };

        let callback_bytes =
            bincode::serialize(&callback_data).context("Failed to serialize callback data")?;

        let raw_key = Self::callback_raw_key(&callback_id);
        storage
            .put(&raw_key, &callback_bytes)
            .context("Failed to store callback data")?;

        // Salva un marker nel slot (32 bytes) per segnalare che il callback esiste
        let slot = Self::callback_slot(&callback_id);
        let marker = Self::u64_to_storage_value(1); // marker: callback exists
        contract_storage
            .sstore(storage, slot, marker, gas_meter.as_deref_mut())
            .context("Failed to store callback marker")?;

        Ok(callback_id)
    }

    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `callback_id` - ID of the callback
    /// * `gas_meter` - Gas meter opzionale
    pub fn get_callback(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        callback_id: &[u8; 32],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<Option<CallbackData>> {
        // Check if callback exists via the marker slot
        let slot = Self::callback_slot(callback_id);
        let marker = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .context("Failed to read callback marker")?;

        if marker.iter().all(|&b| b == 0) {
            return Ok(None);
        }

        // Retrieve full callback data from raw storage
        let raw_key = Self::callback_raw_key(callback_id);
        let callback_bytes = storage
            .get(&raw_key)
            .context("Failed to read callback data")?;

        match callback_bytes {
            Some(data) => {
                const MAX_CALLBACK_DATA_SIZE: usize = 4 * 1024 * 1024;
                if data.len() > MAX_CALLBACK_DATA_SIZE {
                    anyhow::bail!(
                        "Callback data too large for deserialization: {} bytes (max {})",
                        data.len(),
                        MAX_CALLBACK_DATA_SIZE
                    );
                }
                let callback_data = bincode::deserialize::<CallbackData>(&data)
                    .context("Failed to deserialize callback data")?;
                Ok(Some(callback_data))
            }
            None => Ok(None),
        }
    }

    /// Triggera un callback (emette evento CallbackTriggered)
    ///
    /// asincronamente da un worker separato. Il callback NON influenza lo stato on-chain.
    ///
    /// # Arguments
    /// * `callback_id` - ID of the callback da triggerare
    /// * `payload` - Payload of the callback (max 1MB)
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `event_system` - Sistema eventi per emettere eventi
    /// * `gas_meter` - Gas meter opzionale
    pub fn trigger_callback(
        callback_id: &[u8; 32],
        payload: &[u8],
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        event_system: &EventSystem,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Check che il callback esista
        let _callback_data = Self::get_callback(
            contract_storage,
            storage,
            callback_id,
            gas_meter.as_deref_mut(),
        )?
        .ok_or_else(|| anyhow::anyhow!("Callback not found"))?;

        // Validazione payload size
        if payload.len() > MAX_PAYLOAD_SIZE {
            anyhow::bail!("Payload size exceeds maximum of {} bytes", MAX_PAYLOAD_SIZE);
        }

        // Compute hash of the payload
        let payload_hash = Keccak256::digest(payload);
        let payload_hash_hex = hex::encode(payload_hash);

        // Emetti evento CallbackTriggered (fire-and-forget, non influenza stato on-chain)
        let callback_id_hex = hex::encode(callback_id);
        event_system.emit_standard_event(
            StandardEvent::CallbackTriggered {
                callback_id: callback_id_hex,
                payload_hash: payload_hash_hex,
                payload: payload.to_vec(),
            },
            gas_meter.as_deref_mut(),
        );

        Ok(())
    }

    /// Adds un dominio alla whitelist
    ///
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `domain` - Dominio da aggiungere
    /// * `gas_meter` - Gas meter opzionale
    pub fn add_whitelist_domain(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        domain: &str,
        caller: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Check che il caller sia l'owner
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        if caller != owner.as_slice() {
            anyhow::bail!("Only owner can modify whitelist");
        }

        // Aggiungi dominio alla whitelist
        let slot = Self::whitelist_domain_slot(domain);
        let enabled_value = Self::bool_to_storage_value(true);
        contract_storage
            .sstore(storage, slot, enabled_value, gas_meter.as_deref_mut())
            .context("Failed to add domain to whitelist")?;

        Ok(())
    }

    /// Rimuove un dominio dalla whitelist
    ///
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `gas_meter` - Gas meter opzionale
    pub fn remove_whitelist_domain(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        domain: &str,
        caller: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Check che il caller sia l'owner
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        if caller != owner.as_slice() {
            anyhow::bail!("Only owner can modify whitelist");
        }

        // Rimuovi dominio dalla whitelist (set a false)
        let slot = Self::whitelist_domain_slot(domain);
        let disabled_value = Self::bool_to_storage_value(false);
        contract_storage
            .sstore(storage, slot, disabled_value, gas_meter.as_deref_mut())
            .context("Failed to remove domain from whitelist")?;

        Ok(())
    }

    /// Check se un dominio è in the whitelist
    fn is_domain_whitelisted(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        domain: &str,
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<bool> {
        let slot = Self::whitelist_domain_slot(domain);
        let value = contract_storage
            .sload(storage, slot, gas_meter.as_deref_mut())
            .context("Failed to read whitelist domain")?;

        if value.iter().all(|&b| b == 0) {
            return Ok(false);
        }

        Self::storage_value_to_bool(&value)
    }

    /// Abilita o disabilita la whitelist
    ///
    ///
    /// # Arguments
    /// * `contract_storage` - Contract storage
    /// * `storage` - Storage layer
    /// * `enabled` - Se true, abilita la whitelist
    /// * `gas_meter` - Gas meter opzionale
    pub fn set_whitelist_enabled(
        contract_storage: &mut ContractStorage,
        storage: &Storage,
        enabled: bool,
        caller: &[u8],
        mut gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Check che il caller sia l'owner
        let owner = BaseContract::get_owner(contract_storage, storage, gas_meter.as_deref_mut())?;
        if caller != owner.as_slice() {
            anyhow::bail!("Only owner can modify whitelist settings");
        }

        // Set whitelist_enabled
        let enabled_value = Self::bool_to_storage_value(enabled);
        contract_storage
            .sstore(
                storage,
                SLOT_WHITELIST_ENABLED,
                enabled_value,
                gas_meter.as_deref_mut(),
            )
            .context("Failed to set whitelist_enabled")?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::contracts::events::EventSystem;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn unique_tmp_dir() -> anyhow::Result<PathBuf> {
        let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        let mut p = std::env::temp_dir();
        p.push(format!("savitri-callbacks-test-{}", nanos));
        fs::create_dir_all(&p)?;
        Ok(p)
    }

    fn setup_test_storage() -> anyhow::Result<(Storage, ContractStorage)> {
        let tmp = unique_tmp_dir()?;
        let storage = Storage::new(&tmp)?;
        let contract_address = [1u8; 32];
        let mut contract_storage = ContractStorage::new(contract_address.to_vec())?;
        Ok((storage, contract_storage))
    }

    #[test]
    fn test_small_callback_storage() -> anyhow::Result<()> {
        let (storage, mut contract_storage) = setup_test_storage()?;
        let owner_address = [2u8; 32];
        let caller_address = [3u8; 32];
        let event_system = EventSystem::new();

        // Inizializza the contract
        CallbackRegistry::initialize(&mut contract_storage, &storage, &owner_address, None)?;

        // Add domain to whitelist
        CallbackRegistry::add_whitelist_domain(
            &mut contract_storage,
            &storage,
            "example.com",
            &owner_address,
            None,
        )?;

        let callback_id = CallbackRegistry::register(
            &mut contract_storage,
            &storage,
            "test_event".to_string(),
            "https://example.com/callback".to_string(),
            Some("Bearer token123".to_string()),
            2,
            &caller_address,
            &event_system,
            None,
        )?;

        let retrieved =
            CallbackRegistry::get_callback(&mut contract_storage, &storage, &callback_id, None)?;

        assert!(retrieved.is_some(), "Callback should be retrievable");
        let callback_data = retrieved.unwrap();
        assert_eq!(callback_data.event_type, "test_event");
        assert_eq!(callback_data.url, "https://example.com/callback");
        assert_eq!(
            callback_data.auth_header,
            Some("Bearer token123".to_string())
        );
        assert_eq!(callback_data.retry_policy, 2);
        assert_eq!(callback_data.registrant, caller_address.to_vec());

        Ok(())
    }

    #[test]
    fn test_large_callback_storage() -> anyhow::Result<()> {
        let (storage, mut contract_storage) = setup_test_storage()?;
        let owner_address = [2u8; 32];
        let caller_address = [3u8; 32];
        let event_system = EventSystem::new();

        // Inizializza the contract
        CallbackRegistry::initialize(&mut contract_storage, &storage, &owner_address, None)?;

        // Add domain to whitelist
        CallbackRegistry::add_whitelist_domain(
            &mut contract_storage,
            &storage,
            "very-long-domain-name-with-many-subdomains.example.com",
            &owner_address,
            None,
        )?;

        // Registra un callback grande (con URL lungo e auth header lungo per superare 32 bytes)
        let long_url = "https://very-long-domain-name-with-many-subdomains.example.com/very/long/path/with/many/segments/callback".to_string();
        let long_auth = Some(
            "Bearer very-long-authentication-token-that-makes-the-data-exceed-32-bytes-limit"
                .to_string(),
        );

        let callback_id = CallbackRegistry::register(
            &mut contract_storage,
            &storage,
            "test_large_event".to_string(),
            long_url.clone(),
            long_auth.clone(),
            3,
            &caller_address,
            &event_system,
            None,
        )?;

        let retrieved =
            CallbackRegistry::get_callback(&mut contract_storage, &storage, &callback_id, None)?;

        assert!(retrieved.is_some(), "Large callback should be retrievable");
        let callback_data = retrieved.unwrap();
        assert_eq!(callback_data.event_type, "test_large_event");
        assert_eq!(callback_data.url, long_url);
        assert_eq!(callback_data.auth_header, long_auth);
        assert_eq!(callback_data.retry_policy, 3);
        assert_eq!(callback_data.registrant, caller_address.to_vec());

        Ok(())
    }

    #[test]
    fn test_url_validation() -> anyhow::Result<()> {
        let (storage, mut contract_storage) = setup_test_storage()?;
        let owner_address = [2u8; 32];
        let caller_address = [3u8; 32];
        let event_system = EventSystem::new();

        // Inizializza the contract
        CallbackRegistry::initialize(&mut contract_storage, &storage, &owner_address, None)?;

        // Add domains to whitelist for valid URL tests
        CallbackRegistry::add_whitelist_domain(
            &mut contract_storage,
            &storage,
            "example.com",
            &owner_address,
            None,
        )?;
        CallbackRegistry::add_whitelist_domain(
            &mut contract_storage,
            &storage,
            "api.service.com",
            &owner_address,
            None,
        )?;
        CallbackRegistry::add_whitelist_domain(
            &mut contract_storage,
            &storage,
            "subdomain.example.org",
            &owner_address,
            None,
        )?;

        // Test URL validi
        let valid_urls = vec![
            "https://example.com/callback",
            "http://api.service.com/endpoint",
            "https://subdomain.example.org:8080/path",
        ];

        for url in valid_urls {
            let result = CallbackRegistry::register(
                &mut contract_storage,
                &storage,
                "test_event".to_string(),
                url.to_string(),
                None,
                1,
                &caller_address,
                &event_system,
                None,
            );
            assert!(result.is_ok(), "URL {} should be valid", url);
        }

        // Test URL non validi
        let invalid_urls = vec![
            "ftp://example.com/callback", // Protocollo non supportato
            "https://localhost/callback", // Localhost non consentito
            "http://127.0.0.1/endpoint",  // IP privato non consentito
            "https://192.168.1.1/api",    // IP privato non consentito
        ];

        for url in invalid_urls {
            let result = CallbackRegistry::register(
                &mut contract_storage,
                &storage,
                "test_event".to_string(),
                url.to_string(),
                None,
                1,
                &caller_address,
                &event_system,
                None,
            );
            assert!(result.is_err(), "URL {} should be invalid", url);
        }

        Ok(())
    }

    #[test]
    fn test_whitelist_management() -> anyhow::Result<()> {
        let (storage, mut contract_storage) = setup_test_storage()?;
        let owner_address = [2u8; 32];
        let non_owner_address = [3u8; 32];
        let event_system = EventSystem::new();

        // Inizializza the contract
        CallbackRegistry::initialize(&mut contract_storage, &storage, &owner_address, None)?;

        // Test aggiunta dominio alla whitelist (solo owner)
        let result = CallbackRegistry::add_whitelist_domain(
            &mut contract_storage,
            &storage,
            "example.com",
            &owner_address,
            None,
        );
        assert!(
            result.is_ok(),
            "Owner should be able to add domain to whitelist"
        );

        let result = CallbackRegistry::add_whitelist_domain(
            &mut contract_storage,
            &storage,
            "test.com",
            &non_owner_address,
            None,
        );
        assert!(
            result.is_err(),
            "Non-owner should not be able to add domain to whitelist"
        );

        // Test rimozione dominio dalla whitelist (solo owner)
        let result = CallbackRegistry::remove_whitelist_domain(
            &mut contract_storage,
            &storage,
            "example.com",
            &owner_address,
            None,
        );
        assert!(
            result.is_ok(),
            "Owner should be able to remove domain from whitelist"
        );

        Ok(())
    }

    #[test]
    fn test_retry_policy_validation() -> anyhow::Result<()> {
        let (storage, mut contract_storage) = setup_test_storage()?;
        let owner_address = [2u8; 32];
        let caller_address = [3u8; 32];
        let event_system = EventSystem::new();

        // Inizializza the contract
        CallbackRegistry::initialize(&mut contract_storage, &storage, &owner_address, None)?;

        // Add domain to whitelist
        CallbackRegistry::add_whitelist_domain(
            &mut contract_storage,
            &storage,
            "example.com",
            &owner_address,
            None,
        )?;

        // Test retry policy validi (0-3)
        for retry_policy in 0..=3 {
            let result = CallbackRegistry::register(
                &mut contract_storage,
                &storage,
                "test_event".to_string(),
                "https://example.com/callback".to_string(),
                None,
                retry_policy,
                &caller_address,
                &event_system,
                None,
            );
            assert!(
                result.is_ok(),
                "Retry policy {} should be valid",
                retry_policy
            );
        }

        // Test retry policy non valido (>3)
        let result = CallbackRegistry::register(
            &mut contract_storage,
            &storage,
            "test_event".to_string(),
            "https://example.com/callback".to_string(),
            None,
            4, // Superiore al massimo consentito
            &caller_address,
            &event_system,
            None,
        );
        assert!(result.is_err(), "Retry policy >3 should be invalid");

        Ok(())
    }
}
