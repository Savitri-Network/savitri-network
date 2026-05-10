use rocksdb::IteratorMode;
use sha2::{Digest, Sha512};

use super::{Storage, RocksDb, CF_ACCOUNTS, CF_CONTRACTS};
use crate::storage::contracts::ContractInfo;
use crate::core::types::Account;

impl Storage<RocksDb> {
    // Compute deterministic state root over accounts and contract storage roots in lexicographic order (DB snapshot)
    //
    // Lo state root include:
    // 1. Account balances (in ordine lessicografico per address)
    // 2. Contract storage roots (in ordine lessicografico per contract address)
    //
    pub fn compute_state_root(&self) -> anyhow::Result<[u8; 64]> {
        // SECURITY: Use a RocksDB snapshot for atomic point-in-time reads.
        // This ensures the state root computation sees a consistent DB state
        // even if concurrent writes occur during iteration.
        let snapshot = self.db.snapshot();

        // Seed = H("STATEv1-LE")
        let seed = Sha512::digest(b"STATEv1-LE");
        let mut root_hasher = Sha512::new();
        root_hasher.update(seed);

        // 1. Aggiungi account balances
        let cf_accounts = self.cf(CF_ACCOUNTS)?;
        let iter_accounts = snapshot.iterator_cf(&cf_accounts, IteratorMode::Start);
        for item in iter_accounts {
            let (k, v): (Box<[u8]>, Box<[u8]>) = item?;
            let acc = Account::decode(&v)?; // treat decode failure as error
            if acc == Account::default() {
                anyhow::bail!("found persisted empty account for key {:?}", k);
            }
            let mut leaf_hasher = Sha512::new();
            // Leaf domain per spec: b"STATE"
            leaf_hasher.update(b"STATE");
            leaf_hasher.update(&k);
            leaf_hasher.update(&acc.balance.to_le_bytes());
            let leaf = leaf_hasher.finalize();
            root_hasher.update(&leaf);
        }

        // 2. Aggiungi contract storage roots (using same snapshot for consistency)
        let cf_contracts = self.cf(CF_CONTRACTS)?;
        let iter_contracts = snapshot.iterator_cf(&cf_contracts, IteratorMode::Start);
        for item in iter_contracts {
            let (k, v): (Box<[u8]>, Box<[u8]>) = item?;
            // Storage slot keys begin with "storage:"
            if k.starts_with(b"storage:") {
                continue; // Skip storage slots, processiamo solo ContractInfo
            }

            // Deserializza ContractInfo
            let contract_info: ContractInfo = match crate::safe_deserialize(&v) {
                Ok(ci) => ci,
                Err(_) => continue, // Skip entries that are not ContractInfo
            };

            // Lo storage_root è già salvato in ContractInfo, ma dobbiamo ricalcolarlo
            // per includere eventuali modifiche non ancora committate
            use crate::contracts::storage::ContractStorage;
            let contract_storage = ContractStorage<RocksDb>::new(contract_info.address.clone())?;
            let storage_root = contract_storage.compute_storage_root(self)?;

            // Aggiungi il contract storage root allo state root
            let mut leaf_hasher = Sha512::new();
            // Leaf domain per contract storage: b"CONTRACT-STORAGE-ROOT"
            leaf_hasher.update(b"CONTRACT-STORAGE-ROOT");
            leaf_hasher.update(&k); // contract address
            leaf_hasher.update(&storage_root); // storage root (32 bytes)
            let leaf = leaf_hasher.finalize();
            root_hasher.update(&leaf);
        }

        let out = root_hasher.finalize();
        let mut root = [0u8; 64];
        root.copy_from_slice(&out);
        Ok(root)
    }

    // Compute state root over an overlay (sorted) merged with RocksDB state
    //
    // Lo state root include:
    // 1. Account balances (con overlay applicato)
    //
    // In futuro, si potrebbe aggiungere un parametro per l'overlay of contracts.
    pub fn compute_state_root_overlay(
        &self,
        overlay: &std::collections::BTreeMap<Vec<u8>, Account>,
    ) -> anyhow::Result<[u8; 64]> {
        // SECURITY: Use a RocksDB snapshot for atomic point-in-time reads.
        let snapshot = self.db.snapshot();

        let seed = Sha512::digest(b"STATEv1-LE");
        let mut root_hasher = Sha512::new();
        root_hasher.update(seed);

        // 1. Aggiungi account balances (con overlay)
        let cf_accounts = self.cf(CF_ACCOUNTS)?;
        use std::cmp::Ordering;
        let mut overlay_iter = overlay.iter().peekable();
        let mut db_iter = snapshot.iterator_cf(&cf_accounts, IteratorMode::Start);
        let mut db_next = db_iter.next();

        loop {
            // Early propagate DB error
            if let Some(Err(e)) = &db_next {
                return Err(anyhow::Error::msg(format!("{:?}", e)));
            }

            let db_key_opt: Option<Vec<u8>> = db_next
                .as_ref()
                .and_then(|r: &Result<_, _>| r.as_ref().ok())
                .map(|(k, _): &(Box<[u8]>, Box<[u8]>)| k.as_ref().to_vec());
            let ov_peek: Option<(Vec<u8>, Account)> = overlay_iter
                .peek()
                .map(|(k, v): (&Vec<u8>, &Account)| (k.to_vec(), Account { balance: v.balance, nonce: v.nonce }));

            enum Step {
                UseDb,
                UseOv,
                UseBoth,
                Done,
            }
            let step = match (db_key_opt.as_ref(), ov_peek.as_ref()) {
                (None, None) => Step::Done,
                (None, Some(_)) => Step::UseOv,
                (Some(_), None) => Step::UseDb,
                (Some(ref dbk), Some(ref (ovk, _))) => match (dbk.as_slice(), ovk.as_slice()).cmp() {
                    Ordering::Less => Step::UseDb,
                    Ordering::Greater => Step::UseOv,
                    Ordering::Equal => Step::UseBoth,
                },
            };

            match step {
                Step::Done => break,
                Step::UseOv => {
                    if let Some((ok, ov)) = overlay_iter
                        .next()
                        .map(|(k, v): (&Vec<u8>, &Account)| (k.to_vec(), Account { balance: v.balance, nonce: v.nonce }))
                    {
                        if ov != Account::default() {
                            let mut leaf = Sha512::new();
                            // Leaf domain per spec: b"STATE"
                            leaf.update(b"STATE");
                            leaf.update(&ok);
                            leaf.update(&ov.balance.to_le_bytes());
                            let leaf = leaf.finalize();
                            root_hasher.update(&leaf);
                        }
                    }
                }
                Step::UseDb => {
                    let item = db_next.take().unwrap();
                    let (k, v) = item?;
                    let acc = Account::decode(&v)?;
                    if acc == Account::default() {
                        anyhow::bail!("found persisted empty account for key {:?}", k);
                    }
                    let mut leaf = Sha512::new();
                    // Leaf domain per spec: b"STATE"
                    leaf.update(b"STATE");
                    leaf.update(&k);
                    leaf.update(&acc.balance.to_le_bytes());
                    let leaf = leaf.finalize();
                    root_hasher.update(&leaf);
                    db_next = db_iter.next();
                }
                Step::UseBoth => {
                    // Overlay overwrites DB
                    if let Some((ok, ov)) = overlay_iter
                        .next()
                        .map(|(k, v): (&Vec<u8>, &Account)| (k.to_vec(), Account { balance: v.balance, nonce: v.nonce }))
                    {
                        if ov != Account::default() {
                            let mut leaf = Sha512::new();
                            // Leaf domain per spec: b"STATE"
                            leaf.update(b"STATE");
                            leaf.update(&ok);
                            leaf.update(&ov.balance.to_le_bytes());
                            let leaf = leaf.finalize();
                            root_hasher.update(&leaf);
                        }
                    }
                    // consume db_next as well
                    let _ = db_next.take().unwrap()?;
                    db_next = db_iter.next();
                }
            }
        }

        // 2. Aggiungi contract storage roots (using same snapshot for consistency)
        let cf_contracts = self.cf(CF_CONTRACTS)?;
        let iter_contracts = snapshot.iterator_cf(&cf_contracts, IteratorMode::Start);
        for item in iter_contracts {
            let (k, v): (Box<[u8]>, Box<[u8]>) = item?;
            if k.starts_with(b"storage:") {
                continue; // Skip storage slots
            }

            // Deserializza ContractInfo
            let contract_info: ContractInfo = match crate::safe_deserialize(&v) {
                Ok(ci) => ci,
                Err(_) => continue, // Skip entries that are not ContractInfo
            };

            use crate::contracts::storage::ContractStorage;
            let contract_storage = ContractStorage<RocksDb>::new(contract_info.address.clone())?;
            let storage_root = contract_storage.compute_storage_root(self)?;

            // Aggiungi il contract storage root allo state root
            let mut leaf_hasher = Sha512::new();
            // Leaf domain per contract storage: b"CONTRACT-STORAGE-ROOT"
            leaf_hasher.update(b"CONTRACT-STORAGE-ROOT");
            leaf_hasher.update(&k); // contract address
            leaf_hasher.update(&storage_root); // storage root (32 bytes)
            let leaf = leaf_hasher.finalize();
            root_hasher.update(&leaf);
        }

        let out = root_hasher.finalize();
        let mut root = [0u8; 64];
        root.copy_from_slice(&out);
        Ok(root)
    }
}
