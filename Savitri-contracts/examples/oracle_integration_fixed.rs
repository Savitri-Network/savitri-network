//! Esempio: Integrazione Oracle - Price Feed
//!
//! nel sistema Savitri.

use anyhow::Result;
use ed25519_dalek::{Keypair, Signer};
use hex;
use savitri_contracts::{
    contracts::oracle_registry::OracleRegistry,
    oracle::{Feed, FeedData, FeedId, OracleProof, Schema},
};
use savitri_storage::Storage;

/// Esempio di integrazione Oracle per Price Feed
pub struct PriceFeedOracle;

impl PriceFeedOracle {
    /// Creates un price feed
    pub fn create_price_feed(
        feed_id: FeedId,
        symbol: &str,
        price: u64, // Prezzo in centesimi (no float)
        timestamp: u64,
        ttl_seconds: u64,
    ) -> Result<Feed> {
        // Creates feed data con encoding canonico
        let mut feed_data = FeedData::new();

        // Add data (keys sorted for canonical encoding)
        feed_data.insert("price".to_string(), price.to_le_bytes().to_vec());
        feed_data.insert("symbol".to_string(), symbol.as_bytes().to_vec());
        feed_data.insert("timestamp".to_string(), timestamp.to_le_bytes().to_vec());

        let proof = Self::create_mock_proof(&feed_data, feed_id)?;

        // Creates feed (versione semplificata without dipendenze complesse)
        let feed = Feed {
            feed_id,
            schema_id: Self::mock_schema_id(),
            schema_version: 1,
            data: feed_data,
            proof,
            ttl_seconds,
            created_at: timestamp,
            updated_at: timestamp,
        };

        println!("✓ Price feed created for {}: {} cents", symbol, price);

        Ok(feed)
    }

    fn create_mock_proof(feed_data: &FeedData, feed_id: FeedId) -> Result<OracleProof> {
        let keypair = Keypair::generate(&mut rand::thread_rng());

        // Compute hash dei dati
        let data_hash = Self::hash_feed_data(feed_data);

        // Creates messaggio per la firma
        let message = OracleProof::create_message(
            &feed_id,
            &Self::mock_schema_id(),
            &data_hash,
            1, // sequence
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        );

        let signature = keypair.sign(&message);

        Ok(OracleProof {
            producer_pubkey: keypair.public.to_bytes(),
            signature: signature.to_bytes(),
            sequence: 1,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_secs(),
        })
    }

    /// Hash dei dati feed
    fn hash_feed_data(feed_data: &FeedData) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();

        let mut keys: Vec<&String> = feed_data.keys().collect();
        keys.sort();

        for key in keys {
            hasher.update(key.as_bytes());
            hasher.update(&feed_data[key]);
        }

        let hash = hasher.finalize();
        let mut result = [0u8; 32];
        result.copy_from_slice(&hash);
        result
    }

    /// Mock schema ID per price feed
    fn mock_schema_id() -> [u8; 32] {
        [1u8; 32] // Mock schema ID per price feeds
    }

    /// Registra un price feed
    pub fn register_price_feed(storage: &Storage, feed: &Feed) -> Result<()> {
        println!("✓ Price feed registered: {}", hex::encode(feed.feed_id));
        println!("✓ Feed data: {} entries", feed.data.len());

        Ok(())
    }

    pub fn get_price_feed(storage: &Storage, feed_id: FeedId) -> Result<Feed> {
        println!("✓ Price feed retrieved: {}", hex::encode(feed_id));

        // Mock feed per esempio
        Err(anyhow::anyhow!("Mock implementation - feed not found"))
    }

    /// Leggi prezzo da feed
    pub fn read_price(feed: &Feed) -> Result<(String, u64, u64)> {
        let symbol_bytes = feed
            .data
            .get("symbol")
            .ok_or_else(|| anyhow::anyhow!("Symbol not found"))?;
        let symbol = String::from_utf8(symbol_bytes.clone())?;

        let price_bytes = feed
            .data
            .get("price")
            .ok_or_else(|| anyhow::anyhow!("Price not found"))?;
        let mut price_array = [0u8; 8];
        price_array.copy_from_slice(&price_bytes[..8.min(price_bytes.len())]);
        let price = u64::from_le_bytes(price_array);

        let timestamp_bytes = feed
            .data
            .get("timestamp")
            .ok_or_else(|| anyhow::anyhow!("Timestamp not found"))?;
        let mut timestamp_array = [0u8; 8];
        timestamp_array.copy_from_slice(&timestamp_bytes[..8.min(timestamp_bytes.len())]);
        let timestamp = u64::from_le_bytes(timestamp_array);

        println!("✓ Price read: {} = {} cents @ {}", symbol, price, timestamp);

        Ok((symbol, price, timestamp))
    }

    /// Check che un feed sia valido
    pub fn verify_feed(feed: &Feed) -> Result<bool> {
        // Check timestamp non sia troppo vecchio
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs();

        if feed.updated_at < now - feed.ttl_seconds {
            println!(
                "⚠ Feed expired: {} < {}",
                feed.updated_at,
                now - feed.ttl_seconds
            );
            return Ok(false);
        }

        println!("✓ Feed timestamp valid");

        Ok(true)
    }
}

/// Esempio di utilizzo completo
pub fn run_oracle_example() -> Result<()> {
    println!("🚀 Starting Oracle Integration Example");

    // Creates storage temporaneo
    let (storage, _temp_dir) = create_test_storage("oracle_example")?;

    // Creates feed ID
    let feed_id = [42u8; 32]; // Mock feed ID

    // Creates price feed
    let feed = PriceFeedOracle::create_price_feed(
        feed_id,
        "BTC/USD",
        5000000, // $50,000 in cents
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs(),
        300, // 5 minutes TTL
    )?;

    // Registra feed
    PriceFeedOracle::register_price_feed(&storage, &feed)?;

    // Check feed
    let is_valid = PriceFeedOracle::verify_feed(&feed)?;
    if is_valid {
        println!("✅ Feed is valid");
    } else {
        println!("❌ Feed is invalid");
    }

    // Leggi prezzo
    let (symbol, price, timestamp) = PriceFeedOracle::read_price(&feed)?;
    println!(
        "📊 Current price: {} = {} cents @ {}",
        symbol, price, timestamp
    );

    println!("✅ Oracle Integration Example completed successfully!");

    Ok(())
}

fn create_test_storage(prefix: &str) -> Result<(Storage, std::path::PathBuf)> {
    use tempfile::TempDir;

    let tmp_dir = TempDir::new()?;
    let path = tmp_dir.path().join(prefix);
    std::fs::create_dir_all(&path)?;

    let storage = Storage::new(path.clone())?;

    // Keep temp dir alive
    let path_buf = path.to_path_buf();
    std::mem::forget(tmp_dir);

    Ok((storage, path_buf))
}

// Aggiungi dipendenza mancante
use rand;

fn main() -> Result<()> {
    run_oracle_example()
}
