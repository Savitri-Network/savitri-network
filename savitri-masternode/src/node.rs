//!

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Nodo masternode completo
pub struct Masternode {
    storage: Arc<savitri_storage::Storage>,
}

impl Masternode {
    pub fn new(storage: Arc<savitri_storage::Storage>) -> Self {
        Self { storage }
    }

    pub async fn start(&mut self) -> Result<()> {
        // Logica di avvio nodo
        Ok(())
    }

    /// Ferma il masternode
    pub async fn stop(&mut self) -> Result<()> {
        // Logica di stop nodo
        Ok(())
    }
}
