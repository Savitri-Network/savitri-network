//! Lightweight logging helpers used across the lightnode crate.

/// Flag for PoU-related messages.
pub const FLAG_POU: &str = "POU";
/// Flag for block production / proposal attempt messages.
pub const FLAG_BLOCK_ATTEMPT: &str = "BLOCK_ATTEMPT";
/// Flag for masternode coordination messages.
pub const FLAG_MASTERNODE: &str = "MASTERNODE";
/// Flag for resource-monitoring messages.
pub const FLAG_RESOURCE: &str = "RESOURCE";

/// Prefix a log message with a stable category label.
pub fn flagged_message(flag: &str, message: impl AsRef<str>) -> String {
    format!("[{}] {}", flag, message.as_ref())
}
