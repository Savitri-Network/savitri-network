//! Contract Storage: Storage model per contratti (Merkle tree)
//!
//! - Ogni contract ha un storage_root
//! - Operazioni SLOAD/SSTORE con gas metering
//! - Isolation strict tra contratti
//! - Validazione robusta e gestione errori

use crate::contracts::base::BaseContract;
use crate::contracts::gas::GasMeter;
use anyhow::{Context, Result};
use hex;
use savitri_storage::Storage;
use sha2::{Digest, Sha512};
use std::collections::BTreeMap;

// Column family names per RocksDB
pub const CF_CONTRACTS: &str = "contracts";

///
///
/// # Isolation
pub struct ContractStorage {
    contract_address: Vec<u8>,
    /// Overlay per modifiche temporanee durante l'esecuzione
    overlay: BTreeMap<u64, Vec<u8>>,
    /// Cache per valori letti dal database (per ottimizzazione)
    read_cache: BTreeMap<u64, Vec<u8>>,
}

impl ContractStorage {
    ///
    /// # Arguments
    ///
    /// # Returns
    /// Errore se l'address non è di 32 bytes
    pub fn new(contract_address: Vec<u8>) -> Result<Self> {
        if contract_address.len() != 32 {
            anyhow::bail!(
                "contract address must be exactly 32 bytes, got {}",
                contract_address.len()
            );
        }

        Ok(Self {
            contract_address,
            overlay: BTreeMap::new(),
            read_cache: BTreeMap::new(),
        })
    }

    /// SLOAD: Legge un valore dallo storage con gas metering
    ///
    /// Legge il valore di uno slot dallo storage. Se lo slot non esiste,
    ///
    /// # Isolation
    /// Per garantire isolation durante l'esecuzione, usa `sload_with_isolation()`
    /// o `sload_with_runtime()` che applicano automaticamente l'isolation check.
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `slot` - Slot da leggere (u64)
    /// * `gas_meter` - Gas meter opzionale per consumare gas (None = no gas check)
    ///
    /// # Returns
    ///
    /// # Gas Cost
    /// Consuma gas per SLOAD se gas_meter è fornito (default: 100 gas)
    pub fn sload(
        &mut self,
        storage: &Storage,
        slot: u64,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<Vec<u8>> {
        // Consuma gas per SLOAD se gas_meter è fornito
        if let Some(gas_meter) = gas_meter {
            gas_meter
                .consume_sload()
                .map_err(|e| anyhow::anyhow!("SLOAD gas consumption failed: {}", e))?;
        }

        if let Some(value) = self.overlay.get(&slot) {
            return Ok(value.clone());
        }

        // Poi controlla la cache (per evitare letture ripetute dal DB)
        if let Some(value) = self.read_cache.get(&slot) {
            return Ok(value.clone());
        }

        // Infine controlla il database persistente
        let key = Self::make_storage_key(&self.contract_address, slot);
        let value: Vec<u8> = match storage
            .get_cf(CF_CONTRACTS, &key)
            .with_context(|| "Failed to read storage slot from database")?
        {
            Some(db_value) => {
                if (&db_value as &[u8]).len() != 32 {
                    anyhow::bail!(
                        "invalid storage value length in database: expected 32, got {}",
                        (&db_value as &[u8]).len()
                    );
                }
                db_value
            }
            None => {
                vec![0u8; 32]
            }
        };

        // Cache il valore letto per ottimizzazione
        self.read_cache.insert(slot, value.clone());
        Ok(value)
    }

    /// SSTORE: Scrive un valore in the storage con gas metering
    ///
    /// accumulata nell'overlay e committata al blocco.
    ///
    /// # Isolation
    /// Per garantire isolation durante l'esecuzione, usa `sstore_with_isolation()`
    /// o `sstore_with_runtime()` che applicano automaticamente l'isolation check.
    ///
    /// # Reserved Slots
    /// Per scrivere negli slot riservati, usa `sstore_reserved()` (solo per BaseContract).
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB (per determinare se slot è nuovo)
    /// * `slot` - Slot da scrivere (u64)
    /// * `gas_meter` - Gas meter opzionale per consumare gas (None = no gas check)
    ///
    /// # Returns
    ///
    /// # Gas Cost
    /// Consuma gas per SSTORE se gas_meter è fornito:
    /// - 20,000 gas se lo slot è nuovo (vuoto)
    pub fn sstore(
        &mut self,
        storage: &Storage,
        slot: u64,
        value: Vec<u8>,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if value.len() != 32 {
            anyhow::bail!(
                "storage value must be exactly 32 bytes, got {}",
                value.len()
            );
        }

        // Validazione slot riservati: impedisce che i contratti usino slot 0-99
        BaseContract::validate_slot_not_reserved(slot).with_context(|| {
            format!(
                "Cannot write to reserved slot {} (slots 0-99 are reserved for BaseContract)",
                slot
            )
        })?;

        // 2. Se lo slot è nell'overlay con valore zero o non è nell'overlay: controlla il database originale
        let is_new = if let Some(current_value) = self.overlay.get(&slot) {
            if current_value.iter().all(|&b| b == 0) {
                // Valore zero nell'overlay: controlla se esisteva nel database originale
                let key = Self::make_storage_key(&self.contract_address, slot);
                storage
                    .get_cf(CF_CONTRACTS, &key)
                    .with_context(|| "Failed to check if storage slot exists")?
                    .is_none()
            } else {
                false
            }
        } else {
            // Slot non è nell'overlay: controlla se esiste nel database
            let key = Self::make_storage_key(&self.contract_address, slot);
            storage
                .get_cf(CF_CONTRACTS, &key)
                .with_context(|| "Failed to check if storage slot exists")?
                .is_none()
        };

        // Consuma gas per SSTORE se gas_meter è fornito
        if let Some(gas_meter) = gas_meter {
            gas_meter
                .consume_sstore(is_new)
                .map_err(|e| anyhow::anyhow!("SSTORE gas consumption failed: {}", e))?;
        }

        // Se il valore è zero, rimuovi dall'overlay (per cleanup)
        if value.iter().all(|&b| b == 0) {
            self.overlay.remove(&slot);
            // Rimuovi anche dalla cache se presente
            self.read_cache.remove(&slot);
        } else {
            self.overlay.insert(slot, value.clone());
            self.read_cache.insert(slot, value);
        }

        Ok(())
    }

    /// SSTORE riservato: Scrive un valore negli slot riservati per BaseContract
    ///
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    pub fn sstore_reserved(
        &mut self,
        storage: &Storage,
        slot: u64,
        value: Vec<u8>,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        if value.len() != 32 {
            anyhow::bail!(
                "storage value must be exactly 32 bytes, got {}",
                value.len()
            );
        }

        if !BaseContract::is_reserved_slot(slot) {
            anyhow::bail!(
                "sstore_reserved() can only be used for reserved slots (0-99), got slot {}",
                slot
            );
        }

        // (chiamiamo direttamente la logica interna)
        let is_new = if let Some(current_value) = self.overlay.get(&slot) {
            if current_value.iter().all(|&b| b == 0) {
                let key = Self::make_storage_key(&self.contract_address, slot);
                storage
                    .get_cf(CF_CONTRACTS, &key)
                    .with_context(|| "Failed to check if storage slot exists")?
                    .is_none()
            } else {
                false
            }
        } else {
            let key = Self::make_storage_key(&self.contract_address, slot);
            storage
                .get_cf(CF_CONTRACTS, &key)
                .with_context(|| "Failed to check if storage slot exists")?
                .is_none()
        };

        // Consuma gas per SSTORE se gas_meter è fornito
        if let Some(gas_meter) = gas_meter {
            gas_meter
                .consume_sstore(is_new)
                .map_err(|e| anyhow::anyhow!("SSTORE gas consumption failed: {}", e))?;
        }

        // Se il valore è zero, rimuovi dall'overlay (per cleanup)
        if value.iter().all(|&b| b == 0) {
            self.overlay.remove(&slot);
            self.read_cache.remove(&slot);
        } else {
            self.overlay.insert(slot, value.clone());
            self.read_cache.insert(slot, value);
        }

        Ok(())
    }

    /// SLOAD without gas metering (per backward compatibility e test)
    ///
    /// # Deprecated
    #[deprecated(note = "Use sload() with gas_meter: None instead")]
    pub fn sload_no_gas(&mut self, storage: &Storage, slot: u64) -> Result<Vec<u8>> {
        self.sload(storage, slot, None)
    }

    /// SSTORE without gas metering (per backward compatibility e test)
    ///
    /// # Deprecated
    #[deprecated(note = "Use sstore() with gas_meter: None instead")]
    pub fn sstore_no_gas(&mut self, storage: &Storage, slot: u64, value: Vec<u8>) -> Result<()> {
        self.sstore(storage, slot, value, None)
    }

    ///
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    ///
    /// # Returns
    /// Storage root (32 bytes) come hash SHA-512 troncato a 32 bytes
    pub fn compute_storage_root(&self, storage: &Storage) -> Result<[u8; 32]> {
        // Seed per il Merkle tree: H("CONTRACT-STORAGEv1")
        let seed = Sha512::digest(b"CONTRACT-STORAGEv1");
        let mut root_hasher = Sha512::new();
        root_hasher.update(seed);

        let storage_prefix = Self::make_storage_prefix(&self.contract_address);

        // Prima gli slot dal database, poi quelli dall'overlay
        let _cf = storage.cf(CF_CONTRACTS)?;
        let db_results = storage.iterator_cf("contracts")?;

        use std::cmp::Ordering;
        let mut overlay_iter = self.overlay.iter().peekable();
        let mut db_iter = db_results.into_iter().peekable();
        let mut db_next = db_iter.next();

        loop {
            // Early propagate DB error
            if let Some(Err(e)) = &db_next {
                return Err(anyhow::anyhow!("Database error: {}", e));
            }

            let db_key_opt: Option<Vec<u8>> = match &db_next {
                Some(Ok((k, _))) => {
                    let key: Vec<u8> = k.to_vec();
                    if key.starts_with(storage_prefix.as_slice()) {
                        Some(key)
                    } else {
                        None
                    }
                }
                Some(Err(_)) => None,
                None => None,
            };

            let ov_peek = overlay_iter.peek().map(|(slot, _)| *slot);

            enum Step {
                UseDb,
                UseOv,
                UseBoth,
                Done,
            }

            let step = match (db_key_opt.as_ref(), ov_peek) {
                (None, None) => Step::Done,
                (None, Some(_)) => Step::UseOv,
                (Some(_), None) => Step::UseDb,
                (Some(dbk), Some(ov_slot)) => {
                    // Estrai lo slot dalla chiave DB
                    if (dbk as &[u8]).len() < storage_prefix.len() + 8 {
                        Step::UseOv
                    } else {
                        let db_slot_bytes = &dbk[storage_prefix.len()..storage_prefix.len() + 8];
                        let db_slot =
                            u64::from_le_bytes(db_slot_bytes.try_into().unwrap_or([0u8; 8]));
                        match db_slot.cmp(ov_slot) {
                            Ordering::Less => Step::UseDb,
                            Ordering::Greater => Step::UseOv,
                            Ordering::Equal => Step::UseBoth,
                        }
                    }
                }
            };

            match step {
                Step::Done => break,
                Step::UseOv => {
                    if let Some((slot, value)) = overlay_iter.next() {
                        Self::add_slot_to_hasher(&mut root_hasher, *slot, value);
                    }
                }
                Step::UseDb => {
                    if let Some(Ok((key, value))) = db_next.take() {
                        let slot_bytes = &key[storage_prefix.len()..storage_prefix.len() + 8];
                        let slot = u64::from_le_bytes(slot_bytes.try_into().unwrap_or([0u8; 8]));
                        Self::add_slot_to_hasher(&mut root_hasher, slot, &value);
                    }
                    db_next = db_iter.next();
                }
                Step::UseBoth => {
                    // Overlay sovrascrive DB
                    if let Some((slot, value)) = overlay_iter.next() {
                        Self::add_slot_to_hasher(&mut root_hasher, *slot, value);
                    }
                    // Consuma anche db_next
                    let _ = db_next.take().unwrap()?;
                    db_next = db_iter.next();
                }
            }
        }

        // Compute il root hash (SHA-512, troncato a 32 bytes)
        let out = root_hasher.finalize();
        let mut root = [0u8; 32];
        root.copy_from_slice(&out[..32]);
        Ok(root)
    }

    /// Adds uno slot all'hasher per il calcolo of the Merkle root
    fn add_slot_to_hasher(hasher: &mut Sha512, slot: u64, value: &[u8]) {
        let mut leaf_hasher = Sha512::new();
        // Leaf domain tag: "CONTRACT-STORAGE-LEAF"
        leaf_hasher.update(b"CONTRACT-STORAGE-LEAF");
        leaf_hasher.update(&slot.to_le_bytes());
        leaf_hasher.update(value);
        let leaf = leaf_hasher.finalize();
        hasher.update(&leaf);
    }

    /// Creates the key per uno slot in the storage
    ///
    /// Format: "storage:" (8 bytes) + contract_address (32 bytes) + slot (8 bytes)
    fn make_storage_key(contract_address: &[u8], slot: u64) -> Vec<u8> {
        let mut key = Vec::with_capacity(48);
        key.extend_from_slice(b"storage:");
        key.extend_from_slice(contract_address);
        key.extend_from_slice(&slot.to_le_bytes());
        key
    }

    fn make_storage_prefix(contract_address: &[u8]) -> Vec<u8> {
        let mut prefix = Vec::with_capacity(40);
        prefix.extend_from_slice(b"storage:");
        prefix.extend_from_slice(contract_address);
        prefix
    }

    pub fn contract_address(&self) -> &[u8] {
        &self.contract_address
    }

    pub fn overlay(&self) -> &BTreeMap<u64, Vec<u8>> {
        &self.overlay
    }

    pub fn overlay_mut(&mut self) -> &mut BTreeMap<u64, Vec<u8>> {
        &mut self.overlay
    }

    ///
    ///
    /// # Arguments
    ///
    /// # Returns
    pub fn is_isolated(&self, other_address: &[u8]) -> bool {
        self.contract_address != other_address
    }

    ///
    ///
    /// # Arguments
    ///
    /// # Returns
    /// * `Ok(())` se l'accesso è permesso
    /// * `Err` se l'accesso non è permesso (violazione isolation)
    pub fn validate_access(&self, accessing_address: &[u8]) -> Result<()> {
        if accessing_address.len() != 32 {
            anyhow::bail!(
                "accessing address must be exactly 32 bytes, got {}",
                accessing_address.len()
            );
        }

        if accessing_address != self.contract_address.as_slice() {
            anyhow::bail!(
                "storage access violation: contract {} cannot access storage of contract {}",
                hex::encode(accessing_address),
                hex::encode(&self.contract_address)
            );
        }

        Ok(())
    }

    ///
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `slot` - Slot da leggere (u64)
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    pub fn sload_with_isolation(
        &mut self,
        storage: &Storage,
        accessing_address: &[u8],
        slot: u64,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<Vec<u8>> {
        // Validazione isolation strict
        self.validate_access(accessing_address)
            .with_context(|| "SLOAD isolation violation")?;

        // Esegui SLOAD normale
        self.sload(storage, slot, gas_meter)
    }

    ///
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `slot` - Slot da scrivere (u64)
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Errore se isolation violation o altri errori
    pub fn sstore_with_isolation(
        &mut self,
        storage: &Storage,
        accessing_address: &[u8],
        slot: u64,
        value: Vec<u8>,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        // Validazione isolation strict
        self.validate_access(accessing_address)
            .with_context(|| "SSTORE isolation violation")?;

        // Esegui SSTORE normale
        self.sstore(storage, slot, value, gas_meter)
    }

    ///
    /// Versione di SLOAD che applica automaticamente l'isolation check
    /// raccomandata durante l'esecuzione of the bytecode.
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `slot` - Slot da leggere (u64)
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    pub fn sload_with_runtime(
        &mut self,
        storage: &Storage,
        runtime: &crate::contracts::runtime::Runtime,
        slot: u64,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<Vec<u8>> {
        let current_contract = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Applica isolation check
        self.validate_access(&current_contract)
            .with_context(|| "SLOAD isolation violation")?;

        // Esegui SLOAD normale
        self.sload(storage, slot, gas_meter)
    }

    ///
    /// Versione di SSTORE che applica automaticamente l'isolation check
    /// raccomandata durante l'esecuzione of the bytecode.
    ///
    /// # Reserved Slots
    /// I contratti non possono scrivere negli slot riservati.
    ///
    /// # Arguments
    /// * `storage` - Storage layer per accedere a RocksDB
    /// * `slot` - Slot da scrivere (u64)
    /// * `gas_meter` - Gas meter opzionale per consumare gas
    ///
    /// # Returns
    /// Errore se isolation violation, slot riservato, o altri errori
    pub fn sstore_with_runtime(
        &mut self,
        storage: &Storage,
        runtime: &crate::contracts::runtime::Runtime,
        slot: u64,
        value: Vec<u8>,
        gas_meter: Option<&mut GasMeter>,
    ) -> Result<()> {
        let current_contract = runtime
            .current_contract_address()
            .ok_or_else(|| anyhow::anyhow!("No contract in execution context"))?;

        // Applica isolation check
        self.validate_access(&current_contract)
            .with_context(|| "SSTORE isolation violation")?;

        self.sstore(storage, slot, value, gas_meter)
    }

    /// Pulisce la cache di lettura (utile dopo commit o rollback)
    pub fn clear_read_cache(&mut self) {
        self.read_cache.clear();
    }

    /// Pulisce l'overlay (utile per rollback)
    pub fn clear_overlay(&mut self) {
        self.overlay.clear();
    }
}
