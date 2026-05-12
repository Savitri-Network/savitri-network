use serde::{Deserialize, Serialize};

/// Transaction structure for the Savitri blockchain
///
/// Represents a transaction that transfers value between accounts.
/// All transactions must be signed and include a nonce for replay protection.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Transaction {
    /// Sender address
    pub from: String,
    /// Receiver address
    pub to: String,
    /// Amount to transfer
    pub amount: u64,
    /// Transaction nonce for replay protection
    pub nonce: u64,
    /// Transaction fee
    #[serde(default)]
    pub fee: u64,
    /// Transaction signature (variable length, typically 64 bytes)
    #[serde(default)]
    pub signature: Vec<u8>,
    /// Transaction data payload
    #[serde(default)]
    pub data: Vec<u8>,
    /// Transaction timestamp
    #[serde(default)]
    pub timestamp: u64,
    /// Public key of the sender
    #[serde(default)]
    pub pubkey: Vec<u8>,
    /// Cryptographic signature (legacy field name)
    #[serde(default)]
    pub sig: Vec<u8>,
    /// Whether signature has been pre-verified
    #[serde(default)]
    pub pre_verified: bool,
}

impl Transaction {
    /// Compute transaction hash using blake3
    pub fn hash(&self) -> [u8; 32] {
        let mut hasher = blake3::Hasher::new();
        hasher.update(self.from.as_bytes());
        hasher.update(self.to.as_bytes());
        hasher.update(&self.amount.to_le_bytes());
        hasher.update(&self.nonce.to_le_bytes());
        hasher.update(&self.fee.to_le_bytes());
        hasher.update(&self.data);
        hasher.update(&self.timestamp.to_le_bytes());
        *hasher.finalize().as_bytes()
    }

    /// Create a new transaction
    pub fn new(from: String, to: String, amount: u64, nonce: u64, fee: u64) -> Self {
        Self {
            from,
            to,
            amount,
            nonce,
            fee,
            signature: vec![0u8; 64],
            data: Vec::new(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            pubkey: Vec::new(),
            sig: Vec::new(),
            pre_verified: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeeLimits {
    /// Fee minimo accettato (in wei, assumendo 18 decimali)
    /// Default: 0.0001 token = 100_000_000_000_000 wei
    pub min_fee: u128,
    /// Fee massimo accettato (in wei, assumendo 18 decimali)
    /// Default: 1.0 token = 1_000_000_000_000_000_000 wei
    pub max_fee: u128,
}

impl FeeLimits {
    /// Creates nuovi limiti fee con valori specificati
    pub fn new(min_fee: u128, max_fee: u128) -> Self {
        Self { min_fee, max_fee }
    }

    pub fn validate(&self, fee: u128) -> bool {
        fee >= self.min_fee && fee <= self.max_fee
    }
}

impl Default for FeeLimits {
    fn default() -> Self {
        // Default secondo PRD:
        // Min fee: 0.0001 token = 100_000_000_000_000 wei (10^14)
        // Max fee: 1.0 token = 1_000_000_000_000_000_000 wei (10^18)
        Self {
            min_fee: 100_000_000_000_000,       // 0.0001 token
            max_fee: 1_000_000_000_000_000_000, // 1.0 token
        }
    }
}

/// Account structure for the Savitri blockchain
///
/// Represents an account with balance and nonce for replay protection.
/// Each account maintains its state independently.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Account {
    /// Account balance in smallest token units
    pub balance: u128,
    /// Account nonce for transaction replay protection
    pub nonce: u64,
}

impl Account {
    /// Encode account to 24-byte array
    ///
    /// # Returns
    /// 24-byte array containing balance (16 bytes) + nonce (8 bytes) in little-endian
    pub fn encode(&self) -> [u8; 24] {
        let mut encoded = [0u8; 24];
        encoded[0..16].copy_from_slice(&self.balance.to_le_bytes());
        encoded[16..24].copy_from_slice(&self.nonce.to_le_bytes());
        encoded
    }

    /// Decode account from 24-byte array
    ///
    /// # Arguments
    /// * `bytes` - 24-byte array containing encoded account data
    ///
    /// # Returns
    /// Decoded Account struct
    pub fn decode(bytes: &[u8]) -> anyhow::Result<Self> {
        // Support both old format (16 bytes: balance only) and new format (24 bytes: balance + nonce)
        if bytes.len() == 24 {
            // New format: 16 bytes balance + 8 bytes nonce
            let mut balance_bytes = [0u8; 16];
            balance_bytes.copy_from_slice(&bytes[0..16]);
            let mut nonce_bytes = [0u8; 8];
            nonce_bytes.copy_from_slice(&bytes[16..24]);
            Ok(Account {
                balance: u128::from_le_bytes(balance_bytes),
                nonce: u64::from_le_bytes(nonce_bytes),
            })
        } else if bytes.len() == 16 {
            // Old format: 16 bytes balance only (nonce = 0)
            let mut balance_bytes = [0u8; 16];
            balance_bytes.copy_from_slice(&bytes[0..16]);
            Ok(Account {
                balance: u128::from_le_bytes(balance_bytes),
                nonce: 0,
            })
        } else {
            anyhow::bail!(
                "invalid account length: {} (expected 16 or 24 bytes)",
                bytes.len()
            );
        }
    }

    // Guardrailed updates: reject underflow/overflow
    /// Credit account with specified amount
    ///
    /// # Arguments
    /// * `amount` - Amount to credit to the account
    ///
    /// # Returns
    /// Ok(()) if successful, error if overflow would occur
    pub fn credit(&mut self, amount: u128) -> anyhow::Result<()> {
        self.balance = self
            .balance
            .checked_add(amount)
            .ok_or_else(|| anyhow::anyhow!("balance overflow"))?;
        Ok(())
    }

    /// Debit account with specified amount
    ///
    /// # Arguments
    /// * `amount` - Amount to debit from the account
    ///
    /// # Returns
    /// Ok(()) if successful, error if insufficient funds or overflow
    pub fn debit(&mut self, amount: u128) -> anyhow::Result<()> {
        self.balance = self
            .balance
            .checked_sub(amount)
            .ok_or_else(|| anyhow::anyhow!("balance underflow"))?;
        Ok(())
    }

    /// Increment account nonce by 1
    ///
    /// # Returns
    /// Ok(()) if successful, error if nonce would overflow
    pub fn increment_nonce(&mut self) -> anyhow::Result<()> {
        self.nonce = self
            .nonce
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("nonce overflow"))?;
        Ok(())
    }

    /// Set the account nonce
    ///
    /// # Arguments
    /// * `nonce` - The new nonce value
    pub fn set_nonce(&mut self, nonce: u64) {
        self.nonce = nonce;
    }
}
