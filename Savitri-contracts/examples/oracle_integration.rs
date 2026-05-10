//! Esempio: Integrazione Oracle - Price Feed
//!
//! nel sistema Savitri.

use anyhow::Result;
use hex;
use savitri_contracts::{
    contracts::oracle_registry::OracleRegistry,
    oracle::{Feed, FeedData, FeedId, OracleProof, Schema, SchemaType},
};
use savitri_storage::Storage;
use sha3::{Digest, Keccak256};

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

        let proof = OracleProof::mock_sign(&feed_data, feed_id)?;

        // Creates feed
        let feed = Feed::new(
            feed_id,
            Schema::price_schema_id(),
            1, // schema version
            feed_data,
            proof,
            ttl_seconds,
            timestamp,
        )?;

        println!("✓ Price feed created for {}: {} cents", symbol, price);

        Ok(feed)
    }

    /// Registra un price feed
    pub fn register_price_feed(storage: &Storage, feed: &Feed) -> Result<()> {
        let registry = OracleRegistry::new();

        // Registra feed
        registry.register_feed(
            storage,
            feed,
            &Schema::price_schema(),
            &Default::default(), // config
            feed.created_at,
        )?;

        println!("✓ Price feed registered: {}", hex::encode(feed.feed_id));

        Ok(())
    }

    pub fn get_price_feed(storage: &Storage, feed_id: FeedId) -> Result<Feed> {
        let registry = OracleRegistry::new();

        let feed = registry.get_feed(storage, feed_id)?;

        println!("✓ Price feed retrieved: {}", hex::encode(feed_id));

        Ok(feed)
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
        let price = u64::from_le_bytes(price_bytes[..8].try_into().unwrap());

        let timestamp_bytes = feed
            .data
            .get("timestamp")
            .ok_or_else(|| anyhow::anyhow!("Timestamp not found"))?;
        let timestamp = u64::from_le_bytes(timestamp_bytes[..8].try_into().unwrap());

        Ok((symbol, price, timestamp))
    }

    /// Check validità feed
    pub fn is_feed_valid(feed: &Feed, current_time: u64) -> bool {
        // Compute expiration time
        let ttl = if feed.ttl_seconds == 0 {
            3600 // default 1 hour
        } else {
            feed.ttl_seconds
        };

        let expires_at = feed.created_at + ttl;

        // Check TTL
        if current_time > expires_at {
            println!("✗ Feed expired: {} > {}", current_time, expires_at);
            return false;
        }

        // Check timestamp futuro
        if feed.created_at > current_time + 60 {
            // 60s tolerance
            println!("✗ Feed from future: {} > {}", feed.created_at, current_time);
            return false;
        }

        println!("✓ Feed is valid");
        true
    }
}

fn main() -> Result<()> {
    println!("=== Oracle Integration Example - Price Feed ===\n");

    // Setup
    let tmp = tempfile::TempDir::new()?;
    let storage = Storage::new(tmp.path())?;

    // Creates feed ID
    let mut feed_id = [0u8; 32];
    feed_id[..8].copy_from_slice(&b"BTC_PRICE"[..8]);

    // Creates price feed
    let symbol = "BTC";
    let price = 45000_00; // $45,000.00 in centesimi
    let timestamp = 1000000;
    let ttl_seconds = 3600; // 1 hour

    let feed = PriceFeedOracle::create_price_feed(feed_id, symbol, price, timestamp, ttl_seconds)?;

    // Registra feed
    PriceFeedOracle::register_price_feed(&storage, &feed)?;

    let retrieved_feed = PriceFeedOracle::get_price_feed(&storage, feed_id)?;

    // Leggi prezzo
    let (symbol_read, price_read, timestamp_read) = PriceFeedOracle::read_price(&retrieved_feed)?;

    println!("\n=== Feed Data ===");
    println!("Symbol: {}", symbol_read);
    println!(
        "Price: {} cents (${:.2})",
        price_read,
        price_read as f64 / 100.0
    );
    println!("Timestamp: {}", timestamp_read);

    // Check validità
    let current_time = timestamp + 1800; // 30 minutes later
    let is_valid = PriceFeedOracle::is_feed_valid(&retrieved_feed, current_time);
    println!("\nFeed valid at time {}: {}", current_time, is_valid);

    println!("\n✓ Example completed successfully");

    Ok(())
}
