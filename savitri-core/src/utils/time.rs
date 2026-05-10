//! Time utilities

use std::time::{SystemTime, UNIX_EPOCH};

/// Get the current Unix timestamp in seconds
///
/// # Returns
/// The current time as seconds since Unix epoch
#[doc(alias = "current_time")]
pub fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
