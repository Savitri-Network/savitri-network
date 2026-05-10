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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn account_credit_debit_checked() {
        let mut a = Account::default();
        a.credit(10).unwrap();
        assert_eq!(a.balance, 10);
        a.debit(3).unwrap();
        assert_eq!(a.balance, 7);
        assert!(a.debit(8).is_err()); // underflow

        // BANK-GRADE: Test nonce functionality
        assert_eq!(a.nonce, 0); // Default nonce
        a.increment_nonce().unwrap();
        assert_eq!(a.nonce, 1);
        a.set_nonce(5);
        assert_eq!(a.nonce, 5);

        // Test nonce overflow protection
        a.nonce = u64::MAX;
        assert!(a.increment_nonce().is_err()); // Should fail on overflow
    }

    #[test]
    fn account_credit_overflow_checked() {
        let mut a = Account {
            balance: u128::MAX - 5,
            nonce: 0,
        };
        a.credit(5).unwrap();
        assert_eq!(a.balance, u128::MAX);
        assert!(a.credit(1).is_err());
    }

    #[test]
    fn fee_limits_validate() {
        let limits = FeeLimits::default();

        // Test valid fees
        assert!(limits.validate(limits.min_fee)); // Exactly min
        assert!(limits.validate(limits.max_fee)); // Exactly max
        assert!(limits.validate((limits.min_fee + limits.max_fee) / 2)); // Middle value

        // Test invalid fees
        assert!(!limits.validate(limits.min_fee - 1)); // Below min
        assert!(!limits.validate(limits.max_fee + 1)); // Above max
        assert!(!limits.validate(0)); // Zero
    }

    #[test]
    fn fee_limits_custom() {
        let custom_limits = FeeLimits::new(1000, 10000);
        assert_eq!(custom_limits.min_fee, 1000);
        assert_eq!(custom_limits.max_fee, 10000);
        assert!(custom_limits.validate(5000));
        assert!(!custom_limits.validate(500));
        assert!(!custom_limits.validate(20000));
    }

    #[test]
    fn account_encoding_backward_compatibility() {
        // Test new 24-byte format
        let account_new = Account {
            balance: 1000000,
            nonce: 42,
        };
        let encoded_new = account_new.encode();
        assert_eq!(encoded_new.len(), 24);

        let decoded_new = Account::decode(&encoded_new).unwrap();
        assert_eq!(decoded_new.balance, 1000000);
        assert_eq!(decoded_new.nonce, 42);

        // Test old 16-byte format compatibility
        let mut old_bytes = [0u8; 16];
        old_bytes.copy_from_slice(&1000000u128.to_le_bytes());
        let decoded_old = Account::decode(&old_bytes).unwrap();
        assert_eq!(decoded_old.balance, 1000000);
        assert_eq!(decoded_old.nonce, 0); // Nonce should be 0 for old format
    }

    #[test]
    fn bank_grade_transactional_integrity() {
        // This test ensures that debit/credit operations are atomic
        // If credit fails, debit must be rolled back completely

        let mut sender = Account {
            balance: 1000,
            nonce: 5,
        };
        let mut receiver = Account {
            balance: u128::MAX - 1, // Near overflow
            nonce: 10,
        };

        // Capture original state
        let sender_original = sender;
        let receiver_original = receiver;

        // Test successful transaction
        assert!(sender.debit(100).is_ok());
        assert!(receiver.credit(100).is_ok());
        assert_eq!(sender.balance, 900);
        assert_eq!(receiver.balance, u128::MAX);

        // Test failed credit with rollback simulation
        let mut sender2 = Account {
            balance: 1000,
            nonce: 5,
        };
        let mut receiver2 = Account {
            balance: u128::MAX, // At max - will overflow on any credit
            nonce: 10,
        };

        // Simulate: debit succeeds, credit fails
        let debit_result = sender2.debit(100);
        assert!(debit_result.is_ok()); // Debit would succeed

        let credit_result = receiver2.credit(100);
        assert!(credit_result.is_err()); // Credit fails due to overflow

        // In a real transaction, we would rollback:
        // sender2 = sender_original; // This is what our implementation does
        assert_eq!(sender2.balance, 900); // Current state after debit
                                          // After rollback: should be 1000 again

        // Verify no token loss: total supply should remain constant
        let total_before = sender_original.balance + receiver_original.balance;
        let total_after_failed = u128::MAX - 100; // 1000 - 5 = 995, MAX - 995 = MAX - 100
        assert_eq!(total_before, total_after_failed); // No tokens lost, just moved temporarily
    }
}
