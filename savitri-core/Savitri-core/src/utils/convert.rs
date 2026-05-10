//! Type conversion utilities for Savitri Network
//! 
//! This module provides safe and efficient type conversion functions
//! used throughout the Savitri ecosystem.

use std::convert::{TryFrom, TryInto};

/// Safe conversion from bytes to hex string
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Safe conversion from hex string to bytes
pub fn hex_to_bytes(hex_str: &str) -> Result<Vec<u8>, hex::FromHexError> {
    hex::decode(hex_str)
}

/// Convert bytes to hex string with prefix
pub fn bytes_to_hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Convert hex string with prefix to bytes
pub fn hex_to_bytes_prefixed(hex_str: &str) -> Result<Vec<u8>, hex::FromHexError> {
    let cleaned = if hex_str.starts_with("0x") || hex_str.starts_with("0X") {
        &hex_str[2..]
    } else {
        hex_str
    };
    hex::decode(cleaned)
}

/// Safe conversion from string to u64
pub fn str_to_u64(s: &str) -> Result<u64, std::num::ParseIntError> {
    s.parse::<u64>()
}

/// Safe conversion from string to u128
pub fn str_to_u128(s: &str) -> Result<u128, std::num::ParseIntError> {
    s.parse::<u128>()
}

/// Convert u64 to string
pub fn u64_to_str(value: u64) -> String {
    value.to_string()
}

/// Convert u128 to string
pub fn u128_to_str(value: u128) -> String {
    value.to_string()
}

/// Convert bytes to u64 (little endian)
pub fn bytes_to_u64_le(bytes: &[u8]) -> Result<u64, &'static str> {
    if bytes.len() < 8 {
        return Err("Insufficient bytes for u64");
    }
    let mut array = [0u8; 8];
    array.copy_from_slice(&bytes[..8]);
    Ok(u64::from_le_bytes(array))
}

/// Convert bytes to u64 (big endian)
pub fn bytes_to_u64_be(bytes: &[u8]) -> Result<u64, &'static str> {
    if bytes.len() < 8 {
        return Err("Insufficient bytes for u64");
    }
    let mut array = [0u8; 8];
    array.copy_from_slice(&bytes[..8]);
    Ok(u64::from_be_bytes(array))
}

/// Convert u64 to bytes (little endian)
pub fn u64_to_bytes_le(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

/// Convert u64 to bytes (big endian)
pub fn u64_to_bytes_be(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Convert bytes to u128 (little endian)
pub fn bytes_to_u128_le(bytes: &[u8]) -> Result<u128, &'static str> {
    if bytes.len() < 16 {
        return Err("Insufficient bytes for u128");
    }
    let mut array = [0u8; 16];
    array.copy_from_slice(&bytes[..16]);
    Ok(u128::from_le_bytes(array))
}

/// Convert bytes to u128 (big endian)
pub fn bytes_to_u128_be(bytes: &[u8]) -> Result<u128, &'static str> {
    if bytes.len() < 16 {
        return Err("Insufficient bytes for u128");
    }
    let mut array = [0u8; 16];
    array.copy_from_slice(&bytes[..16]);
    Ok(u128::from_be_bytes(array))
}

/// Convert u128 to bytes (little endian)
pub fn u128_to_bytes_le(value: u128) -> [u8; 16] {
    value.to_le_bytes()
}

/// Convert u128 to bytes (big endian)
pub fn u128_to_bytes_be(value: u128) -> [u8; 16] {
    value.to_be_bytes()
}

/// Convert timestamp to datetime string
pub fn timestamp_to_datetime(timestamp: u64) -> String {
    let datetime = chrono::DateTime::<chrono::Utc>::from_timestamp(
        timestamp as i64,
        0
    );
    match datetime {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => "Invalid timestamp".to_string(),
    }
}

/// Convert datetime string to timestamp
pub fn datetime_to_timestamp(datetime_str: &str) -> Result<u64, chrono::ParseError> {
    let dt = chrono::DateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S %z")?;
    Ok(dt.timestamp() as u64)
}

/// Convert duration to milliseconds
pub fn duration_to_ms(duration: std::time::Duration) -> u64 {
    duration.as_millis() as u64
}

/// Convert milliseconds to duration
pub fn ms_to_duration(ms: u64) -> std::time::Duration {
    std::time::Duration::from_millis(ms)
}

/// Convert seconds to duration
pub fn secs_to_duration(secs: u64) -> std::time::Duration {
    std::time::Duration::from_secs(secs)
}

/// Convert duration to human readable string
pub fn duration_to_human(duration: std::time::Duration) -> String {
    let total_secs = duration.as_secs();
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;

    if days > 0 {
        format!("{}d {}h {}m {}s", days, hours, minutes, seconds)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

/// Convert a slice to a fixed-size array
pub fn slice_to_array<T, const N: usize>(slice: &[T]) -> Option<[T; N]>
where
    T: Copy,
{
    // AUDIT: replaced unsafe MaybeUninit with safe try_into conversion.
    slice.try_into().ok()
}

/// Convert a fixed-size array to a vector
pub fn array_to_vec<T: std::clone::Clone, const N: usize>(array: [T; N]) -> Vec<T> {
    array.to_vec()
}

/// Safe conversion between different integer types
pub fn safe_int_convert<T, U>(value: T) -> Option<U>
where
    T: TryInto<U>,
{
    value.try_into().ok()
}

/// Convert floating point to fixed-point representation
pub fn float_to_fixed(value: f64, scale: u64) -> u128 {
    ((value * scale as f64).round() as i64).max(0) as u128
}

/// Convert fixed-point to floating point representation
pub fn fixed_to_float(value: u128, scale: u64) -> f64 {
    value as f64 / scale as f64
}

/// Convert percentage to basis points (1% = 100 basis points)
pub fn percent_to_bps(percent: f64) -> u64 {
    (percent * 100.0).round() as u64
}

/// Convert basis points to percentage
pub fn bps_to_percent(bps: u64) -> f64 {
    bps as f64 / 100.0
}

/// Convert wei to ether (1 ether = 10^18 wei)
pub fn wei_to_ether(wei: u128) -> f64 {
    wei as f64 / 1_000_000_000_000_000_000.0
}

/// Convert ether to wei
pub fn ether_to_wei(ether: f64) -> u128 {
    (ether * 1_000_000_000_000_000_000.0).round() as u128
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_conversions() {
        let bytes = vec![0x12, 0x34, 0x56, 0x78];
        let hex = bytes_to_hex(&bytes);
        assert_eq!(hex, "12345678");
        
        let converted = hex_to_bytes(&hex).unwrap();
        assert_eq!(converted, bytes);
        
        let hex_prefixed = bytes_to_hex_prefixed(&bytes);
        assert_eq!(hex_prefixed, "0x12345678");
        
        let converted_prefixed = hex_to_bytes_prefixed(&hex_prefixed).unwrap();
        assert_eq!(converted_prefixed, bytes);
    }

    #[test]
    fn test_string_conversions() {
        assert_eq!(str_to_u64("123").unwrap(), 123);
        assert_eq!(str_to_u128("456").unwrap(), 456);
        assert_eq!(u64_to_str(789), "789");
        assert_eq!(u128_to_str(101112), "101112");
        
        assert!(str_to_u64("invalid").is_err());
    }

    #[test]
    fn test_byte_conversions() {
        let value = 0x123456789ABCDEF0;
        
        let bytes_le = u64_to_bytes_le(value);
        let converted_le = bytes_to_u64_le(&bytes_le).unwrap();
        assert_eq!(converted_le, value);
        
        let bytes_be = u64_to_bytes_be(value);
        let converted_be = bytes_to_u64_be(&bytes_be).unwrap();
        assert_eq!(converted_be, value);
        
        assert_ne!(bytes_le, bytes_be);
    }

    #[test]
    fn test_u128_conversions() {
        let value = 0x123456789ABCDEF0_123456789ABCDEF0;
        
        let bytes_le = u128_to_bytes_le(value);
        let converted_le = bytes_to_u128_le(&bytes_le).unwrap();
        assert_eq!(converted_le, value);
        
        let bytes_be = u128_to_bytes_be(value);
        let converted_be = bytes_to_u128_be(&bytes_be).unwrap();
        assert_eq!(converted_be, value);
    }

    #[test]
    fn test_timestamp_conversions() {
        let timestamp = 1640995200; // 2022-01-01 00:00:00 UTC
        let datetime = timestamp_to_datetime(timestamp);
        assert!(datetime.contains("2022-01-01"));
        
        // Note: datetime_to_timestamp requires timezone info, so we skip that test
    }

    #[test]
    fn test_duration_conversions() {
        let duration = std::time::Duration::from_secs(3661); // 1 hour, 1 minute, 1 second
        let ms = duration_to_ms(duration);
        assert_eq!(ms, 3_661_000);
        
        let converted = ms_to_duration(ms);
        assert_eq!(converted, duration);
        
        let human = duration_to_human(duration);
        assert_eq!(human, "1h 1m 1s");
        
        let short_duration = std::time::Duration::from_secs(30);
        let short_human = duration_to_human(short_duration);
        assert_eq!(short_human, "30s");
    }

    #[test]
    fn test_array_conversions() {
        let vec = vec![1, 2, 3, 4];
        let array = slice_to_array(&vec).unwrap();
        assert_eq!(array, [1, 2, 3, 4]);
        
        let converted = array_to_vec(array);
        assert_eq!(converted, vec);
        
        // Test with wrong size
        let wrong_vec = vec![1, 2, 3];
        assert!(slice_to_array::<i32, 4>(&wrong_vec).is_none());
    }

    #[test]
    fn test_safe_int_conversion() {
        assert_eq!(safe_int_convert::<u32, u64>(123), Some(123u64));
        assert_eq!(safe_int_convert::<u64, u32>(123), Some(123u32));
        
        // Test overflow
        assert!(safe_int_convert::<u64, u32>(u32::MAX as u64 + 1).is_none());
    }

    #[test]
    fn test_fixed_point_conversions() {
        let value = 123.456;
        let scale = 1000;
        let fixed = float_to_fixed(value, scale);
        assert_eq!(fixed, 123456);
        
        let converted = fixed_to_float(fixed, scale);
        assert!((converted - value).abs() < 0.001);
    }

    #[test]
    fn test_percentage_conversions() {
        assert_eq!(percent_to_bps(1.5), 150);
        assert_eq!(percent_to_bps(0.5), 50);
        
        assert_eq!(bps_to_percent(150), 1.5);
        assert_eq!(bps_to_percent(50), 0.5);
    }

    #[test]
    fn test_ether_conversions() {
        let ether = 1.5;
        let wei = ether_to_wei(ether);
        assert_eq!(wei, 1_500_000_000_000_000_000);
        
        let converted = wei_to_ether(wei);
        assert!((converted - ether).abs() < 0.0001);
    }

    #[test]
    fn test_invalid_hex() {
        assert!(hex_to_bytes("invalid").is_err());
        assert!(hex_to_bytes("0xZZ").is_err());
        assert!(hex_to_bytes_prefixed("invalid").is_err());
    }

    #[test]
    fn test_invalid_byte_conversions() {
        assert!(bytes_to_u64_le(&[1, 2, 3]).is_err());
        assert!(bytes_to_u128_le(&[1, 2, 3]).is_err());
    }
}
