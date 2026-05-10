//! Utility functions for Savitri Network
//! 
//! This module provides various utility functions and helpers used throughout
//! the Savitri ecosystem, including type conversions, time utilities, math functions,
//! and serialization helpers.

pub mod convert;
pub mod time;
pub mod math;
pub mod bincode_utils;

// Re-export commonly used functions
pub use convert::{
    bytes_to_hex, hex_to_bytes, bytes_to_hex_prefixed, hex_to_bytes_prefixed,
    str_to_u64, str_to_u128, u64_to_str, u128_to_str,
    bytes_to_u64_le, bytes_to_u64_be, u64_to_bytes_le, u64_to_bytes_be,
    bytes_to_u128_le, bytes_to_u128_be, u128_to_bytes_le, u128_to_bytes_be,
    timestamp_to_datetime, duration_to_ms, ms_to_duration, duration_to_human,
    slice_to_array, array_to_vec, safe_int_convert,
    float_to_fixed, fixed_to_float, percent_to_bps, bps_to_percent,
    wei_to_ether, ether_to_wei
};
pub use time::{
    now_timestamp, now_timestamp_ms, now_timestamp_us, now_timestamp_ns,
    timestamp_to_ms, ms_to_timestamp, ms_to_datetime,
    duration_between, duration_between_ms, is_within_last_seconds, is_within_last_ms,
    add_seconds, add_ms, subtract_seconds, subtract_ms,
    format_duration, format_duration_ms, format_duration_secs,
    now_iso8601, parse_iso8601,
    slot, epoch, perf
};
pub use math::{
    fixed_point, stats, utils,
};
pub use bincode_utils::{
    consensus_bincode, serialize_consensus, deserialize_consensus,
    default_bincode, serialize_default, deserialize_default,
    serialized_size, can_deserialize, serialize_to_hex, deserialize_from_hex,
    batch, compression, versioning
};
