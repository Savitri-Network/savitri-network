//! Oracle module for external data feeds

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub mod integration;
pub mod validator;
pub mod config;

pub use integration::*;
pub use validator::*;
pub use config::*;

#[derive(Debug, Clone)]
pub struct PriceOracle {
    prices: Arc<RwLock<HashMap<String, f64>>>,
}

impl PriceOracle {
    pub fn new() -> Self {
        Self {
            prices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn set_price(&self, token: &str, price: f64) {
        let mut prices = self.prices.write().unwrap();
        prices.insert(token.to_string(), price);
    }

    pub fn get_price(&self, token: &str) -> Option<f64> {
        let prices = self.prices.read().unwrap();
        prices.get(token).copied()
    }
}
