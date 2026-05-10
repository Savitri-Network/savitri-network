//! Utility functions for Savitri Network

pub mod bincode_utils;
pub mod convert;
pub mod math;
pub mod time;

// Re-export all utility functions from convert module
pub use convert::{
    add_ms, add_seconds, array_to_vec, bps_to_percent, bytes_to_hex, bytes_to_hex_prefixed,
    bytes_to_u128_be, bytes_to_u128_le, bytes_to_u64_be, bytes_to_u64_le, duration_between,
    duration_between_ms, epoch, ether_to_wei, fixed_point, fixed_to_float, float_to_fixed,
    format_duration, format_duration_ms, format_duration_secs, hex_to_bytes, hex_to_bytes_prefixed,
    is_within_last_ms, is_within_last_seconds, ms_to_datetime, ms_to_timestamp, now_iso8601,
    now_timestamp, now_timestamp_ms, now_timestamp_ns, now_timestamp_us, parse_iso8601,
    percent_to_bps, perf, safe_int_convert, slice_to_array, slot, stats, str_to_u128, str_to_u64,
    subtract_ms, subtract_seconds, timestamp_to_ms, u128_to_bytes_be, u128_to_bytes_le,
    u128_to_str, u64_to_bytes_be, u64_to_bytes_le, u64_to_str, wei_to_ether,
};

// Re-export bincode utilities
pub use bincode_utils::{consensus_bincode, deserialize_consensus, serialize_consensus};

// Re-export mathematical utilities
pub use math::{basic, constants, financial, power, rounding, trigonometry};
