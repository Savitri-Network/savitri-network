//! Utilities per testing
//!

use crate::storage::Storage;
use crate::types::Account;
use anyhow::Result;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

///
/// La directory viene creata nel sistema temporaneo con un nome unico
/// test eseguiti in parallelo.
///
/// # Esempi
///
/// ```rust,no_run
/// use savitri_node::test_utils::unique_tmp_dir;
///
/// let tmp_dir = unique_tmp_dir("my-test")?;
/// let storage = Storage::new(&tmp_dir)?;
/// ```
pub fn unique_tmp_dir(prefix: &str) -> Result<PathBuf> {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let mut p = std::env::temp_dir();
    p.push(format!("{}-{}", prefix, nanos));
    fs::create_dir_all(&p)?;
    Ok(p)
}

///
/// Creates a temporary directory and initializes Storage.
/// il debugging.
///
/// # Esempi
///
/// ```rust,no_run
/// use savitri_node::test_utils::create_test_storage;
///
/// let (storage, tmp_dir) = create_test_storage("test-storage")?;
/// ```
pub fn create_test_storage(prefix: &str) -> Result<(Storage, PathBuf)> {
    let tmp_dir = unique_tmp_dir(prefix)?;
    let storage = Storage::new(&tmp_dir)?;
    Ok((storage, tmp_dir))
}

///
pub fn create_test_storage_default() -> Result<(Storage, PathBuf)> {
    create_test_storage("savitri-test")
}

/// Helper per pulire directory temporanee dopo i test
///
/// Utile per cleanup esplicito quando necessario, anche se normalmente
/// il sistema operativo pulisce automaticamente le directory temporanee.
pub fn cleanup_temp_dir(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_dir_all(path)?;
    }
    Ok(())
}

/// Creates uno Storage con account pre-inizializzati per i test
///
/// Creates uno Storage e popola alcuni account con balance iniziali
/// per facilitare i test che richiedono account esistenti.
///
/// # Esempi
///
/// ```rust,no_run
/// use savitri_node::test_utils::create_storage_with_accounts;
/// use savitri_node::types::Account;
///
/// let accounts = vec![
///     (b"alice".to_vec(), Account { balance: 1000 }),
///     (b"bob".to_vec(), Account { balance: 500 }),
/// ];
/// let (storage, _tmp_dir) = create_storage_with_accounts("test", accounts)?;
/// ```
pub fn create_storage_with_accounts(
    prefix: &str,
    accounts: Vec<(Vec<u8>, Account)>,
) -> Result<(Storage, PathBuf)> {
    let (storage, tmp_dir) = create_test_storage(prefix)?;
    let mut batch = storage.begin_batch();
    for (addr, account) in accounts {
        batch.put_account(&addr, &account)?;
    }
    batch.commit()?;
    Ok((storage, tmp_dir))
}
