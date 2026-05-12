//! Oracle Storage Module
//!
//! Complete storage implementation for Oracle framework including:
//! - Connector registration and management
//! - ACL (Access Control List) management
//! - Rate limiting functionality
//! - Oracle anchor storage and retrieval
//! - Feed data storage operations
//! - Schema management functions

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

/// Column family for oracle data
pub const CF_ORACLE: &str = "oracle";

/// Oracle role enum
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OracleRole {
    Reader,
    Writer,
    Auditor,
}

/// Connector configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectorConfig {
    pub max_requests_per_second: u32,
    pub max_batch_size: u32,
    pub timeout_ms: u32,
    pub retry_attempts: u32,
    pub allowed_feeds: Vec<[u8; 32]>,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            max_requests_per_second: 10,
            max_batch_size: 100,
            timeout_ms: 5000,
            retry_attempts: 3,
            allowed_feeds: Vec::new(),
        }
    }
}

/// Connector information for Oracle system
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ConnectorInfo {
    pub connector_id: String,
    pub pubkey: [u8; 32],
    pub config: ConnectorConfig,
    pub registered_at: u64,
    pub last_active: u64,
    pub status: ConnectorStatus,
}

/// Connector status
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ConnectorStatus {
    Active,
    Inactive,
    Suspended,
    Revoked,
}

impl Default for ConnectorStatus {
    fn default() -> Self {
        Self::Active
    }
}

/// ACL entry per un feed
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleAclEntry {
    pub feed_id: Vec<u8>,
    pub address: Vec<u8>,
    pub role: OracleRole,
    pub granted_at: u64,
    pub granted_by: Vec<u8>,
}

/// Tracking entry per rate limiting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectorRateLimitTracking {
    pub last_ingest_timestamp: u64,
    pub ingest_count: u64,
    pub window_start: u64,
}

impl ConnectorRateLimitTracking {
    pub fn new(current_time: u64) -> Self {
        Self {
            last_ingest_timestamp: current_time,
            ingest_count: 0,
            window_start: current_time,
        }
    }

    /// Check if request is allowed based on rate limit
    pub fn is_request_allowed(
        &mut self,
        current_time: u64,
        max_requests_per_second: u32,
        window_size_seconds: u64,
    ) -> bool {
        // Reset window if expired
        if current_time - self.window_start >= window_size_seconds {
            self.window_start = current_time;
            self.ingest_count = 0;
        }

        // Check if under limit
        if self.ingest_count < max_requests_per_second as u64 {
            self.ingest_count += 1;
            self.last_ingest_timestamp = current_time;
            true
        } else {
            false
        }
    }
}

/// Oracle anchor for consensus-anchored data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleAnchor {
    pub feed_id: Vec<u8>,
    pub block_height: u64,
    pub block_hash: Vec<u8>,
    pub timestamp: u64,
    pub data_hash: Vec<u8>,
    pub certificate: Vec<u8>,
    pub anchor_hash: Vec<u8>,
}

impl OracleAnchor {
    pub fn new(
        feed_id: Vec<u8>,
        block_height: u64,
        block_hash: Vec<u8>,
        timestamp: u64,
        data_hash: Vec<u8>,
    ) -> Self {
        Self {
            feed_id,
            block_height,
            block_hash,
            timestamp,
            data_hash,
            certificate: Vec::new(),
            anchor_hash: Vec::new(),
        }
    }

    pub fn is_valid(&self) -> bool {
        !self.feed_id.is_empty() && !self.block_hash.is_empty() && !self.data_hash.is_empty()
    }
}

/// Oracle feed data
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleFeedData {
    pub feed_id: Vec<u8>,
    pub sequence: u64,
    pub data: Vec<u8>,
    pub timestamp: u64,
    pub submitted_by: Vec<u8>,
    pub signature: Vec<u8>,
}

/// Oracle schema definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleSchema {
    pub schema_id: Vec<u8>,
    pub name: String,
    pub version: String,
    pub description: String,
    pub fields: Vec<SchemaField>,
    pub created_at: u64,
    pub created_by: Vec<u8>,
}

/// Schema field definition
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SchemaField {
    pub name: String,
    pub field_type: FieldType,
    pub required: bool,
    pub description: String,
}

/// Field types for oracle schemas
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldType {
    String,
    Integer,
    Float,
    Boolean,
    Bytes,
    Timestamp,
}

/// Oracle storage interface
pub struct OracleStorage;

impl OracleStorage {
    /// Register a new connector
    pub fn register_connector(
        storage: &crate::Storage,
        connector_id: String,
        pubkey: [u8; 32],
        config: ConnectorConfig,
        current_time: u64,
    ) -> Result<()> {
        // Check if connector already exists
        if Self::connector_exists(storage, &connector_id)? {
            return Err(anyhow!("Connector already registered"));
        }

        let connector_info = ConnectorInfo {
            connector_id: connector_id.clone(),
            pubkey,
            config,
            registered_at: current_time,
            last_active: current_time,
            status: ConnectorStatus::Active,
        };

        // Store connector info
        let connector_data = bincode::serialize(&connector_info)?;
        let key = format!("connector:{}", connector_id);
        storage.put(key.as_bytes(), &connector_data)?;

        // Initialize rate limiting tracking
        let rate_limit_key = format!("rate_limit:{}", connector_id);
        let tracking = ConnectorRateLimitTracking::new(current_time);
        let tracking_data = bincode::serialize(&tracking)?;
        storage.put(rate_limit_key.as_bytes(), &tracking_data)?;

        Ok(())
    }

    /// Check if connector exists
    pub fn connector_exists(storage: &crate::Storage, connector_id: &str) -> Result<bool> {
        let key = format!("connector:{}", connector_id);
        storage
            .get(key.as_bytes())
            .map(|opt: Option<Vec<u8>>| opt.is_some())
    }

    /// Get connector information
    pub fn get_connector(
        storage: &crate::Storage,
        connector_id: &str,
    ) -> Result<Option<ConnectorInfo>> {
        let key = format!("connector:{}", connector_id);
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let connector: ConnectorInfo = crate::safe_deserialize(&data)?;
                Ok(Some(connector))
            }
            None => Ok(None),
        }
    }

    /// Update connector status
    pub fn update_connector_status(
        storage: &crate::Storage,
        connector_id: &str,
        new_status: ConnectorStatus,
    ) -> Result<()> {
        let mut connector = match Self::get_connector(storage, connector_id)? {
            Some(c) => c,
            None => return Err(anyhow!("Connector not found")),
        };

        connector.status = new_status;
        let connector_data = bincode::serialize(&connector)?;
        let key = format!("connector:{}", connector_id);
        storage.put(key.as_bytes(), &connector_data)?;

        Ok(())
    }

    /// Update connector last active timestamp
    pub fn update_connector_activity(
        storage: &crate::Storage,
        connector_id: &str,
        current_time: u64,
    ) -> Result<()> {
        let mut connector = match Self::get_connector(storage, connector_id)? {
            Some(c) => c,
            None => return Err(anyhow!("Connector not found")),
        };

        connector.last_active = current_time;
        let connector_data = bincode::serialize(&connector)?;
        let key = format!("connector:{}", connector_id);
        storage.put(key.as_bytes(), &connector_data)?;

        Ok(())
    }

    /// Get all connectors
    pub fn get_all_connectors(storage: &crate::Storage) -> Result<Vec<ConnectorInfo>> {
        let mut connectors = Vec::new();
        let prefix = "connector:";

        // Iterate through all keys with connector prefix
        let iter = storage.iterator_cf(CF_ORACLE)?;
        for item in iter {
            let (key, value): (Vec<u8>, Vec<u8>) = item?;
            let key_str = String::from_utf8_lossy(&key);
            if key_str.starts_with(prefix) {
                if let Ok(connector) = crate::safe_deserialize::<ConnectorInfo>(&value) {
                    connectors.push(connector);
                }
            }
        }

        Ok(connectors)
    }

    /// Grant ACL permission
    pub fn grant_acl_permission(
        storage: &crate::Storage,
        feed_id: Vec<u8>,
        address: Vec<u8>,
        role: OracleRole,
        granted_by: Vec<u8>,
        current_time: u64,
    ) -> Result<()> {
        // Check if permission already exists
        if Self::has_acl_permission(storage, &feed_id, &address)? {
            return Err(anyhow!("Permission already granted"));
        }

        let acl_entry = OracleAclEntry {
            feed_id: feed_id.clone(),
            address: address.clone(),
            role,
            granted_at: current_time,
            granted_by,
        };

        // Store ACL entry
        let acl_data = bincode::serialize(&acl_entry)?;
        let key = format!("acl:{}:{}", hex::encode(&feed_id), hex::encode(&address));
        storage.put(key.as_bytes(), &acl_data)?;

        Ok(())
    }

    /// Revoke ACL permission
    pub fn revoke_acl_permission(
        storage: &crate::Storage,
        feed_id: &[u8],
        address: &[u8],
    ) -> Result<()> {
        let key = format!("acl:{}:{}", hex::encode(feed_id), hex::encode(address));
        storage.delete(key.as_bytes())?;
        Ok(())
    }

    /// Check if address has ACL permission
    pub fn has_acl_permission(
        storage: &crate::Storage,
        feed_id: &[u8],
        address: &[u8],
    ) -> Result<bool> {
        let key = format!("acl:{}:{}", hex::encode(feed_id), hex::encode(address));
        storage
            .get(key.as_bytes())
            .map(|opt: Option<Vec<u8>>| opt.is_some())
    }

    /// Get ACL entry
    pub fn get_acl_entry(
        storage: &crate::Storage,
        feed_id: &[u8],
        address: &[u8],
    ) -> Result<Option<OracleAclEntry>> {
        let key = format!("acl:{}:{}", hex::encode(feed_id), hex::encode(address));
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let entry: OracleAclEntry = crate::safe_deserialize(&data)?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    /// Get all ACL entries for a feed
    pub fn get_feed_acl_entries(
        storage: &crate::Storage,
        feed_id: &[u8],
    ) -> Result<Vec<OracleAclEntry>> {
        let mut entries = Vec::new();
        let prefix = format!("acl:{}:", hex::encode(feed_id));

        let iter = storage.iterator_cf(CF_ORACLE)?;
        for item in iter {
            let (key, value): (Vec<u8>, Vec<u8>) = item?;
            let key_str = String::from_utf8_lossy(&key);
            if key_str.starts_with(&prefix) {
                if let Ok(entry) = crate::safe_deserialize::<OracleAclEntry>(&value) {
                    entries.push(entry);
                }
            }
        }

        Ok(entries)
    }

    /// Check rate limit for connector
    pub fn check_rate_limit(
        storage: &crate::Storage,
        connector_id: &str,
        current_time: u64,
    ) -> Result<bool> {
        // Get connector config
        let connector = match Self::get_connector(storage, connector_id)? {
            Some(c) => c,
            None => return Err(anyhow!("Connector not found")),
        };

        if connector.status != ConnectorStatus::Active {
            return Ok(false);
        }

        // Get rate limit tracking
        let rate_limit_key = format!("rate_limit:{}", connector_id);
        let mut tracking = match storage.get(rate_limit_key.as_bytes())? {
            Some(data) => crate::safe_deserialize(&data)?,
            None => ConnectorRateLimitTracking::new(current_time),
        };

        // Check if request is allowed (1-second window)
        let allowed = tracking.is_request_allowed(
            current_time,
            connector.config.max_requests_per_second,
            1, // 1 second window
        );

        // Update tracking
        let tracking_data = bincode::serialize(&tracking)?;
        storage.put(rate_limit_key.as_bytes(), &tracking_data)?;

        Ok(allowed)
    }

    /// Store oracle anchor
    pub fn store_oracle_anchor(storage: &crate::Storage, anchor: OracleAnchor) -> Result<()> {
        let anchor_data = bincode::serialize(&anchor)?;
        let key = format!("anchor:{}", hex::encode(&anchor.feed_id));
        storage.put(key.as_bytes(), &anchor_data)?;

        // Also store by block height for quick lookup
        let block_key = format!("anchor_by_block:{}", anchor.block_height);
        storage.put(block_key.as_bytes(), &anchor_data)?;

        Ok(())
    }

    /// Get oracle anchor by feed ID
    pub fn get_oracle_anchor(
        storage: &crate::Storage,
        feed_id: &[u8],
    ) -> Result<Option<OracleAnchor>> {
        let key = format!("anchor:{}", hex::encode(feed_id));
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let anchor: OracleAnchor = crate::safe_deserialize(&data)?;
                Ok(Some(anchor))
            }
            None => Ok(None),
        }
    }

    /// Get oracle anchor by block height
    pub fn get_oracle_anchor_by_height(
        storage: &crate::Storage,
        block_height: u64,
    ) -> Result<Option<OracleAnchor>> {
        let key = format!("anchor_by_block:{}", block_height);
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let anchor: OracleAnchor = crate::safe_deserialize(&data)?;
                Ok(Some(anchor))
            }
            None => Ok(None),
        }
    }

    /// Store feed data
    pub fn store_feed_data(storage: &crate::Storage, feed_data: OracleFeedData) -> Result<()> {
        let data_bytes = bincode::serialize(&feed_data)?;
        let key = format!(
            "feed_data:{}:{}",
            hex::encode(&feed_data.feed_id),
            feed_data.sequence
        );
        storage.put(key.as_bytes(), &data_bytes)?;

        // Update max sequence for this feed
        let max_seq_key = format!("max_seq:{}", hex::encode(&feed_data.feed_id));
        storage.put(max_seq_key.as_bytes(), &feed_data.sequence.to_le_bytes())?;

        Ok(())
    }

    /// Get feed data by sequence
    pub fn get_feed_data(
        storage: &crate::Storage,
        feed_id: &[u8],
        sequence: u64,
    ) -> Result<Option<OracleFeedData>> {
        let key = format!("feed_data:{}:{}", hex::encode(feed_id), sequence);
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let feed_data: OracleFeedData = crate::safe_deserialize(&data)?;
                Ok(Some(feed_data))
            }
            None => Ok(None),
        }
    }

    /// Get latest feed data
    pub fn get_latest_feed_data(
        storage: &crate::Storage,
        feed_id: &[u8],
    ) -> Result<Option<OracleFeedData>> {
        let max_seq_key = format!("max_seq:{}", hex::encode(feed_id));
        match storage.get(max_seq_key.as_bytes())? {
            Some(seq_data) if seq_data.len() >= 8 => {
                let sequence = u64::from_le_bytes(seq_data[..8].try_into().unwrap_or([0; 8]));
                Self::get_feed_data(storage, feed_id, sequence)
            }
            _ => Ok(None),
        }
    }

    /// Get max sequence for feed
    pub fn get_feed_max_sequence(storage: &crate::Storage, feed_id: &[u8]) -> Result<Option<u64>> {
        let max_seq_key = format!("max_seq:{}", hex::encode(feed_id));
        match storage.get(max_seq_key.as_bytes())? {
            Some(data) if data.len() >= 8 => {
                let sequence = u64::from_le_bytes(data[..8].try_into().unwrap_or([0; 8]));
                Ok(Some(sequence))
            }
            _ => Ok(None),
        }
    }

    /// Store oracle schema
    pub fn store_schema(storage: &crate::Storage, schema: OracleSchema) -> Result<()> {
        let schema_data = bincode::serialize(&schema)?;
        let key = format!("schema:{}", hex::encode(&schema.schema_id));
        storage.put(key.as_bytes(), &schema_data)?;

        // Also store by name for lookup
        let name_key = format!("schema_by_name:{}", schema.name);
        storage.put(name_key.as_bytes(), &schema.schema_id)?;

        Ok(())
    }

    /// Get oracle schema by ID
    pub fn get_schema(storage: &crate::Storage, schema_id: &[u8]) -> Result<Option<OracleSchema>> {
        let key = format!("schema:{}", hex::encode(schema_id));
        match storage.get(key.as_bytes())? {
            Some(data) => {
                let schema: OracleSchema = crate::safe_deserialize(&data)?;
                Ok(Some(schema))
            }
            None => Ok(None),
        }
    }

    /// Get oracle schema by name
    pub fn get_schema_by_name(
        storage: &crate::Storage,
        schema_name: &str,
    ) -> Result<Option<OracleSchema>> {
        let name_key = format!("schema_by_name:{}", schema_name);
        match storage.get(name_key.as_bytes())? {
            Some(schema_id_bytes) => Self::get_schema(storage, &schema_id_bytes),
            None => Ok(None),
        }
    }

    /// Get all schemas
    pub fn get_all_schemas(storage: &crate::Storage) -> Result<Vec<OracleSchema>> {
        let mut schemas = Vec::new();
        let prefix = "schema:";

        let iter = storage.iterator_cf(CF_ORACLE)?;
        for item in iter {
            let (key, value): (Vec<u8>, Vec<u8>) = item?;
            let key_str = String::from_utf8_lossy(&key);
            if key_str.starts_with(prefix) && !key_str.contains("schema_by_name") {
                if let Ok(schema) = crate::safe_deserialize::<OracleSchema>(&value) {
                    schemas.push(schema);
                }
            }
        }

        Ok(schemas)
    }

    /// Validate feed data against schema
    pub fn validate_feed_data(
        storage: &crate::Storage,
        feed_id: &[u8],
        data: &[u8],
    ) -> Result<bool> {
        // Get schema for this feed (assuming feed_id maps to schema_id)
        match Self::get_schema(storage, feed_id)? {
            Some(schema) => {
                // For now, just check if data is not empty and schema has fields
                Ok(!data.is_empty() && !schema.fields.is_empty())
            }
            None => Ok(false), // No schema found, cannot validate
        }
    }

    /// Get oracle statistics
    pub fn get_oracle_stats(storage: &crate::Storage) -> Result<OracleStats> {
        let connectors = Self::get_all_connectors(storage)?;
        let schemas = Self::get_all_schemas(storage)?;

        let active_connectors = connectors
            .iter()
            .filter(|c| c.status == ConnectorStatus::Active)
            .count();

        let total_feeds = schemas.len(); // Approximation

        Ok(OracleStats {
            total_connectors: connectors.len(),
            active_connectors,
            total_schemas: schemas.len(),
            total_feeds: total_feeds as u64,
        })
    }
}

/// Oracle statistics
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OracleStats {
    pub total_connectors: usize,
    pub active_connectors: usize,
    pub total_schemas: usize,
    pub total_feeds: u64,
}
