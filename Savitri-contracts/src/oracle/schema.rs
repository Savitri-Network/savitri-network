
use crate::oracle::types::OracleError;
use hex;
use serde::{Deserialize, Serialize};
use sha2;
use std::collections::BTreeMap;

/// ID schema (32 bytes)
pub type SchemaId = [u8; 32];

/// Versione schema (u32)
pub type SchemaVersion = u32;

/// Tipo di schema
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum SchemaType {
    /// Price feed: asset/value/timestamp
    Price,
    /// Weather feed: location/temp/humidity
    Weather,
    /// Custom schema
    Custom,
}

/// Campo di uno schema
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaField {
    /// Nome of the campo
    pub name: String,
    /// Tipo of the campo (deterministico, no float)
    pub field_type: FieldType,
    /// Se il campo è obbligatorio
    pub required: bool,
}

/// Tipo di campo (deterministico, no float)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum FieldType {
    /// Stringa UTF-8
    String,
    /// Intero con segno a 64 bit
    I64,
    /// Intero without segno a 64 bit
    U64,
    /// Intero without segno a 32 bit
    U32,
    /// Intero without segno a 16 bit
    U16,
    /// Intero without segno a 8 bit
    U8,
    /// Array di bytes (max 1024)
    Bytes,
    /// Booleano
    Bool,
}

impl FieldType {
    pub fn validate_value(&self, value: &[u8]) -> Result<(), OracleError> {
        match self {
            FieldType::String => {
                // Check che sia UTF-8 valido
                std::str::from_utf8(value).map_err(|_| OracleError::SchemaValidationFailed {
                    schema_id: "unknown".to_string(),
                    reason: "Invalid UTF-8 string".to_string(),
                })?;
            }
            FieldType::I64 | FieldType::U64 => {
                if value.len() != 8 {
                    return Err(OracleError::SchemaValidationFailed {
                        schema_id: "unknown".to_string(),
                        reason: format!("Expected 8 bytes for {:?}, got {}", self, value.len()),
                    });
                }
            }
            FieldType::U32 => {
                if value.len() != 4 {
                    return Err(OracleError::SchemaValidationFailed {
                        schema_id: "unknown".to_string(),
                        reason: format!("Expected 4 bytes for {:?}, got {}", self, value.len()),
                    });
                }
            }
            FieldType::U16 => {
                if value.len() != 2 {
                    return Err(OracleError::SchemaValidationFailed {
                        schema_id: "unknown".to_string(),
                        reason: format!("Expected 2 bytes for {:?}, got {}", self, value.len()),
                    });
                }
            }
            FieldType::U8 => {
                if value.len() != 1 {
                    return Err(OracleError::SchemaValidationFailed {
                        schema_id: "unknown".to_string(),
                        reason: format!("Expected 1 byte for {:?}, got {}", self, value.len()),
                    });
                }
            }
            FieldType::Bytes => {
                if value.len() > 1024 {
                    return Err(OracleError::SchemaValidationFailed {
                        schema_id: "unknown".to_string(),
                        reason: format!("Bytes field too long: {} (max 1024)", value.len()),
                    });
                }
            }
            FieldType::Bool => {
                if value.len() != 1 || (value[0] != 0 && value[0] != 1) {
                    return Err(OracleError::SchemaValidationFailed {
                        schema_id: "unknown".to_string(),
                        reason: "Bool must be exactly 1 byte with value 0 or 1".to_string(),
                    });
                }
            }
        }
        Ok(())
    }
}

/// Schema per un feed type
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Schema {
    /// ID schema (hash deterministico)
    pub id: SchemaId,
    /// Versione schema
    pub version: SchemaVersion,
    /// Tipo schema
    pub schema_type: SchemaType,
    /// Nome schema
    pub name: String,
    pub fields: Vec<SchemaField>,
}

impl Schema {
    /// Creates uno schema per price feed
    pub fn price_feed() -> Self {
        let mut id = [0u8; 32];
        // Hash deterministico per price feed schema
        let seed = b"SAVITRI_ORACLE_SCHEMA_PRICE_V1";
        use sha2::{Digest, Sha512};
        let mut hasher = Sha512::new();
        hasher.update(seed);
        let hash = hasher.finalize();
        id.copy_from_slice(&hash[..32]);

        Self {
            id,
            version: 1,
            schema_type: SchemaType::Price,
            name: "PriceFeed".to_string(),
            fields: vec![
                SchemaField {
                    name: "asset".to_string(),
                    field_type: FieldType::String,
                    required: true,
                },
                SchemaField {
                    name: "value".to_string(),
                    field_type: FieldType::U64, // Prezzo in micro-units (no float)
                    required: true,
                },
                SchemaField {
                    name: "timestamp".to_string(),
                    field_type: FieldType::U64,
                    required: true,
                },
            ],
        }
    }

    /// Creates uno schema per weather feed
    pub fn weather_feed() -> Self {
        let mut id = [0u8; 32];
        // Hash deterministico per weather feed schema
        let seed = b"SAVITRI_ORACLE_SCHEMA_WEATHER_V1";
        use sha2::{Digest, Sha512};
        let mut hasher = Sha512::new();
        hasher.update(seed);
        let hash = hasher.finalize();
        id.copy_from_slice(&hash[..32]);

        Self {
            id,
            version: 1,
            schema_type: SchemaType::Weather,
            name: "WeatherFeed".to_string(),
            fields: vec![
                SchemaField {
                    name: "location".to_string(),
                    field_type: FieldType::String,
                    required: true,
                },
                SchemaField {
                    name: "temperature".to_string(),
                    field_type: FieldType::I64, // Temperatura in decimi di grado (no float)
                    required: true,
                },
                SchemaField {
                    name: "humidity".to_string(),
                    field_type: FieldType::U16, // Umidità in per mille (0-1000, no float)
                    required: true,
                },
                SchemaField {
                    name: "timestamp".to_string(),
                    field_type: FieldType::U64,
                    required: true,
                },
            ],
        }
    }

    pub fn validate_data(&self, data: &BTreeMap<String, Vec<u8>>) -> Result<(), OracleError> {
        // Check campi obbligatori
        for field in &self.fields {
            if field.required {
                if !data.contains_key(&field.name) {
                    return Err(OracleError::SchemaValidationFailed {
                        schema_id: hex::encode(self.id),
                        reason: format!("Missing required field: {}", field.name),
                    });
                }
            }
        }

        for (name, value) in data {
            if let Some(field) = self.fields.iter().find(|f| f.name == *name) {
                field.field_type.validate_value(value)?;
            } else {
            }
        }

        Ok(())
    }
}

/// Registry degli schema
#[derive(Debug, Clone)]
pub struct SchemaRegistry {
    schemas: BTreeMap<String, Schema>, // Key: hex(schema_id)
}

impl SchemaRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            schemas: BTreeMap::new(),
        };

        // Registra schema predefiniti
        let price_schema = Schema::price_feed();
        let weather_schema = Schema::weather_feed();

        registry
            .register(price_schema)
            .expect("Failed to register price schema");
        registry
            .register(weather_schema)
            .expect("Failed to register weather schema");

        registry
    }

    /// Registra uno schema
    pub fn register(&mut self, schema: Schema) -> Result<(), OracleError> {
        let key = hex::encode(schema.id);
        if self.schemas.contains_key(&key) {
            return Err(OracleError::SchemaValidationFailed {
                schema_id: key.clone(),
                reason: "Schema already registered".to_string(),
            });
        }
        self.schemas.insert(key, schema);
        Ok(())
    }

    /// Ottiene uno schema per ID
    pub fn get(&self, schema_id: &SchemaId) -> Option<&Schema> {
        let key = hex::encode(schema_id);
        self.schemas.get(&key)
    }

    /// Ottiene uno schema per tipo
    pub fn get_by_type(&self, schema_type: SchemaType) -> Option<&Schema> {
        self.schemas.values().find(|s| s.schema_type == schema_type)
    }
}

impl Default for SchemaRegistry {
    fn default() -> Self {
        Self::new()
    }
}
