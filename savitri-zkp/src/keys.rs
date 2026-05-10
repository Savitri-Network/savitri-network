//! ZKP key management for trusted setup artifacts.
//!
//! Handles serialization, deserialization, and caching of
//! proving and verification keys.

use anyhow::Result;

/// Storage for ZKP proving and verification keys.
#[derive(Debug, Clone)]
pub struct ZkpKeyStore {
    proving_key: Option<Vec<u8>>,
    verification_key: Option<Vec<u8>>,
}

impl ZkpKeyStore {
    pub fn new() -> Self {
        Self {
            proving_key: None,
            verification_key: None,
        }
    }

    pub fn with_keys(pk: Vec<u8>, vk: Vec<u8>) -> Self {
        Self {
            proving_key: Some(pk),
            verification_key: Some(vk),
        }
    }

    pub fn proving_key(&self) -> Option<&[u8]> {
        self.proving_key.as_deref()
    }

    pub fn verification_key(&self) -> Option<&[u8]> {
        self.verification_key.as_deref()
    }

    pub fn set_proving_key(&mut self, pk: Vec<u8>) {
        self.proving_key = Some(pk);
    }

    pub fn set_verification_key(&mut self, vk: Vec<u8>) {
        self.verification_key = Some(vk);
    }

    /// Save keys to files.
    pub fn save_to_files(
        &self,
        pk_path: &std::path::Path,
        vk_path: &std::path::Path,
    ) -> Result<()> {
        if let Some(pk) = &self.proving_key {
            std::fs::write(pk_path, pk)?;
        }
        if let Some(vk) = &self.verification_key {
            std::fs::write(vk_path, vk)?;
        }
        Ok(())
    }

    /// Load keys from files.
    pub fn load_from_files(
        pk_path: &std::path::Path,
        vk_path: &std::path::Path,
    ) -> Result<Self> {
        let pk = std::fs::read(pk_path)?;
        let vk = std::fs::read(vk_path)?;
        Ok(Self::with_keys(pk, vk))
    }

    /// Generate keys using Arkworks Groth16 trusted setup.
    #[cfg(feature = "arkworks")]
    pub fn generate_arkworks_keys(&mut self) -> Result<()> {
        let prover = crate::prover::ArkworksProver::from_setup()?;
        self.proving_key = Some(prover.proving_key_bytes()?);
        self.verification_key =
            Some(prover.verification_key_bytes().to_vec());
        Ok(())
    }
}

impl Default for ZkpKeyStore {
    fn default() -> Self {
        Self::new()
    }
}
