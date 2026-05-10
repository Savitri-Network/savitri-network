//! Type conversion utilities

use anyhow::Result;
use std::convert::TryInto;

/// Convert bytes to hex string
pub fn bytes_to_hex(bytes: &[u8]) -> String {
    hex::encode(bytes)
}

/// Convert hex string to bytes
pub fn hex_to_bytes(hex_str: &str) -> Result<Vec<u8>> {
    let hex_str = hex_str.trim_start_matches("0x");
    hex::decode(hex_str).map_err(|e| anyhow::anyhow!("Invalid hex: {}", e))
}

/// Convert bytes to hex string with 0x prefix
pub fn bytes_to_hex_prefixed(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

/// Convert hex string to bytes (with or without 0x prefix)
pub fn hex_to_bytes_prefixed(hex_str: &str) -> Result<Vec<u8>> {
    hex_to_bytes(hex_str)
}

/// Convert string to u64
pub fn str_to_u64(s: &str) -> Result<u64> {
    s.parse::<u64>()
        .map_err(|e| anyhow::anyhow!("Invalid u64: {}", e))
}

/// Convert string to u128
pub fn str_to_u128(s: &str) -> Result<u128> {
    s.parse::<u128>()
        .map_err(|e| anyhow::anyhow!("Invalid u128: {}", e))
}

/// Convert u64 to string
pub fn u64_to_str(n: u64) -> String {
    n.to_string()
}

/// Convert u128 to string
pub fn u128_to_str(n: u128) -> String {
    n.to_string()
}

/// Convert bytes to u64 (little endian)
pub fn bytes_to_u64_le(bytes: &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(anyhow::anyhow!("Insufficient bytes for u64"));
    }
    Ok(u64::from_le_bytes(bytes[..8].try_into().unwrap()))
}

/// Convert bytes to u64 (big endian)
pub fn bytes_to_u64_be(bytes: &[u8]) -> Result<u64> {
    if bytes.len() < 8 {
        return Err(anyhow::anyhow!("Insufficient bytes for u64"));
    }
    Ok(u64::from_be_bytes(bytes[..8].try_into().unwrap()))
}

/// Convert u64 to bytes (little endian)
pub fn u64_to_bytes_le(n: u64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

/// Convert u64 to bytes (big endian)
pub fn u64_to_bytes_be(n: u64) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Convert bytes to u128 (little endian)
pub fn bytes_to_u128_le(bytes: &[u8]) -> Result<u128> {
    if bytes.len() < 16 {
        return Err(anyhow::anyhow!("Insufficient bytes for u128"));
    }
    Ok(u128::from_le_bytes(bytes[..16].try_into().unwrap()))
}

/// Convert bytes to u128 (big endian)
pub fn bytes_to_u128_be(bytes: &[u8]) -> Result<u128> {
    if bytes.len() < 16 {
        return Err(anyhow::anyhow!("Insufficient bytes for u128"));
    }
    Ok(u128::from_be_bytes(bytes[..16].try_into().unwrap()))
}

/// Convert u128 to bytes (little endian)
pub fn u128_to_bytes_le(n: u128) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

/// Convert u128 to bytes (big endian)
pub fn u128_to_bytes_be(n: u128) -> Vec<u8> {
    n.to_be_bytes().to_vec()
}

/// Convert slice to array
pub fn slice_to_array<T, const N: usize>(slice: &[T]) -> Result<[T; N]>
where
    T: Clone + Copy,
{
    if slice.len() < N {
        return Err(anyhow::anyhow!("Slice too short for array"));
    }
    let mut array = [slice[0]; N];
    array.copy_from_slice(&slice[..N]);
    Ok(array)
}

/// Convert array to vector
pub fn array_to_vec<T: Clone, const N: usize>(array: [T; N]) -> Vec<T> {
    array.to_vec()
}

/// Safe integer conversion
pub fn safe_int_convert<T, U>(value: T) -> Result<U>
where
    T: TryInto<U>,
    <T as TryInto<U>>::Error: std::fmt::Display,
{
    value
        .try_into()
        .map_err(|e| anyhow::anyhow!("Conversion failed: {}", e))
}

/// Convert float to fixed point
pub fn float_to_fixed(value: f64, scale: u32) -> i64 {
    (value * 10_f64.powf(scale as f64)) as i64
}

/// Convert fixed point to float
pub fn fixed_to_float(value: i64, scale: u32) -> f64 {
    value as f64 / 10_f64.powf(scale as f64)
}

/// Convert percentage to basis points
pub fn percent_to_bps(percent: f64) -> u16 {
    (percent * 100.0) as u16
}

/// Convert basis points to percentage
pub fn bps_to_percent(bps: u16) -> f64 {
    bps as f64 / 100.0
}

/// Convert wei to ether
pub fn wei_to_ether(wei: u128) -> f64 {
    wei as f64 / 1_000_000_000_000_000_000.0
}

/// Convert ether to wei
pub fn ether_to_wei(ether: f64) -> u128 {
    (ether * 1_000_000_000_000_000_000.0) as u128
}

/// Get current timestamp in seconds
///
/// # Returns
/// The current time as seconds since Unix epoch
#[doc(alias = "current_time_seconds")]
pub fn now_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Get current timestamp in milliseconds
///
/// # Returns
/// The current time as milliseconds since Unix epoch
#[doc(alias = "current_time_ms")]
pub fn now_timestamp_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Get current timestamp in microseconds
///
/// # Returns
/// The current time as microseconds since Unix epoch
#[doc(alias = "current_time_us")]
pub fn now_timestamp_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Get current timestamp in nanoseconds
///
/// # Returns
/// The current time as nanoseconds since Unix epoch
#[doc(alias = "current_time_ns")]
pub fn now_timestamp_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Convert timestamp to milliseconds
pub fn timestamp_to_ms(timestamp: u64) -> u64 {
    timestamp * 1000
}

/// Convert milliseconds to timestamp
pub fn ms_to_timestamp(ms: u64) -> u64 {
    ms / 1000
}

/// Convert milliseconds to datetime
pub fn ms_to_datetime(ms: u64) -> chrono::DateTime<chrono::Utc> {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap())
}

/// Calculate duration between two timestamps
pub fn duration_between(start: u64, end: u64) -> u64 {
    end.saturating_sub(start)
}

/// Calculate duration between two timestamps in milliseconds
pub fn duration_between_ms(start_ms: u64, end_ms: u64) -> u64 {
    end_ms.saturating_sub(start_ms)
}

/// Check if timestamp is within last N seconds
pub fn is_within_last_seconds(timestamp: u64, seconds: u64) -> bool {
    let now = now_timestamp();
    now.saturating_sub(timestamp) <= seconds
}

/// Check if timestamp is within last N milliseconds
pub fn is_within_last_ms(timestamp_ms: u64, ms: u64) -> bool {
    let now = now_timestamp_ms();
    now.saturating_sub(timestamp_ms) <= ms
}

/// Add seconds to timestamp
pub fn add_seconds(timestamp: u64, seconds: u64) -> u64 {
    timestamp.saturating_add(seconds)
}

/// Add milliseconds to timestamp
pub fn add_ms(timestamp_ms: u64, ms: u64) -> u64 {
    timestamp_ms.saturating_add(ms)
}

/// Subtract seconds from timestamp
pub fn subtract_seconds(timestamp: u64, seconds: u64) -> u64 {
    timestamp.saturating_sub(seconds)
}

/// Subtract milliseconds from timestamp
pub fn subtract_ms(timestamp_ms: u64, ms: u64) -> u64 {
    timestamp_ms.saturating_sub(ms)
}

/// Format duration as human readable string
pub fn format_duration(duration: u64) -> String {
    let hours = duration / 3600;
    let minutes = (duration % 3600) / 60;
    let seconds = duration % 60;

    if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, seconds)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, seconds)
    } else {
        format!("{}s", seconds)
    }
}

/// Format duration in milliseconds as human readable string
pub fn format_duration_ms(duration_ms: u64) -> String {
    let seconds = duration_ms / 1000;
    let ms = duration_ms % 1000;

    if seconds > 0 {
        format!("{}s {}ms", seconds, ms)
    } else {
        format!("{}ms", ms)
    }
}

/// Format duration in seconds as human readable string
pub fn format_duration_secs(duration_secs: u64) -> String {
    format_duration(duration_secs)
}

/// Get current time in ISO8601 format
pub fn now_iso8601() -> String {
    chrono::Utc::now().to_rfc3339()
}

/// Parse ISO8601 string to timestamp
pub fn parse_iso8601(iso_str: &str) -> Result<u64> {
    let datetime = chrono::DateTime::parse_from_rfc3339(iso_str)
        .map_err(|e| anyhow::anyhow!("Invalid ISO8601: {}", e))?;
    Ok(datetime.timestamp() as u64)
}

/// Get current slot number
pub fn slot(timestamp: u64, slot_duration: u64) -> u64 {
    timestamp / slot_duration
}

/// Get current epoch number
pub fn epoch(timestamp: u64, epoch_duration: u64) -> u64 {
    timestamp / epoch_duration
}

/// Performance measurement utility
pub fn perf<F, R>(name: &str, f: F) -> R
where
    F: FnOnce() -> R,
{
    let start = std::time::Instant::now();
    let result = f();
    let duration = start.elapsed();
    println!("{}: {:?}", name, duration);
    result
}

/// Fixed point arithmetic
pub mod fixed_point {
    use super::*;

    /// Fixed point number with scale
    ///
    /// A fixed-point number representation that provides precise decimal arithmetic
    /// without the floating-point rounding errors. The number is stored as an integer
    /// value with a specified decimal scale.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct FixedPoint {
        /// The raw value as integer
        pub value: i64,
        /// The decimal scale (number of decimal places)
        pub scale: u32,
    }

    impl FixedPoint {
        /// Create a new fixed point number
        ///
        /// # Arguments
        /// * `value` - The raw integer value
        /// * `scale` - The decimal scale
        #[doc(alias = "new_fixed_point")]
        pub fn new(value: i64, scale: u32) -> Self {
            Self { value, scale }
        }

        /// Create a fixed point number from a float
        ///
        /// # Arguments
        /// * `value` - The float value to convert
        /// * `scale` - The decimal scale
        #[doc(alias = "from_f64")]
        pub fn from_float(value: f64, scale: u32) -> Self {
            Self {
                value: float_to_fixed(value, scale),
                scale,
            }
        }

        /// Convert to float
        ///
        /// # Returns
        /// The float representation
        #[doc(alias = "to_f64")]
        pub fn to_float(&self) -> f64 {
            fixed_to_float(self.value, self.scale)
        }

        /// Add two fixed point numbers
        ///
        /// # Arguments
        /// * `other` - The other fixed point number
        ///
        /// # Returns
        /// The sum if scales match, error otherwise
        #[doc(alias = "+")]
        pub fn add(&self, other: &FixedPoint) -> Result<FixedPoint> {
            if self.scale != other.scale {
                return Err(anyhow::anyhow!("Scale mismatch"));
            }
            Ok(FixedPoint::new(self.value + other.value, self.scale))
        }

        /// Subtract two fixed point numbers
        ///
        /// # Arguments
        /// * `other` - The other fixed point number
        ///
        /// # Returns
        /// The difference if scales match, error otherwise
        #[doc(alias = "-")]
        pub fn sub(&self, other: &FixedPoint) -> Result<FixedPoint> {
            if self.scale != other.scale {
                return Err(anyhow::anyhow!("Scale mismatch"));
            }
            Ok(FixedPoint::new(self.value - other.value, self.scale))
        }

        /// Multiply two fixed point numbers
        ///
        /// # Arguments
        /// * `other` - The other fixed point number
        ///
        /// # Returns
        /// The product if scales match, error otherwise
        #[doc(alias = "*")]
        pub fn mul(&self, other: &FixedPoint) -> Result<FixedPoint> {
            let result_value = (self.value * other.value) / (10_i64.pow(self.scale) as i64);
            Ok(FixedPoint::new(result_value, self.scale))
        }

        /// Divide two fixed point numbers
        ///
        /// # Arguments
        /// * `other` - The other fixed point number
        ///
        /// # Returns
        /// The quotient if scales match, error otherwise
        #[doc(alias = "/")]
        pub fn div(&self, other: &FixedPoint) -> Result<FixedPoint> {
            if other.value == 0 {
                return Err(anyhow::anyhow!("Division by zero"));
            }
            let result_value = (self.value * 10_i64.pow(self.scale) as i64) / other.value;
            Ok(FixedPoint::new(result_value, self.scale))
        }
    }
}

/// Statistics utilities
pub mod stats {
    /// Calculate mean of a slice
    pub fn mean(values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.iter().sum::<f64>() / values.len() as f64
    }

    pub fn median(values: &mut [f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = values.len() / 2;
        if values.len() % 2 == 0 {
            (values[mid - 1] + values[mid]) / 2.0
        } else {
            values[mid]
        }
    }

    /// Calculate standard deviation
    pub fn std_dev(values: &[f64]) -> f64 {
        if values.len() < 2 {
            return 0.0;
        }
        let mean_val = mean(values);
        let variance =
            values.iter().map(|x| (x - mean_val).powi(2)).sum::<f64>() / (values.len() - 1) as f64;
        variance.sqrt()
    }

    /// Calculate percentiles
    pub fn percentile(values: &mut [f64], p: f64) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let index = (p / 100.0 * (values.len() - 1) as f64) as usize;
        values[index.min(values.len() - 1)]
    }
}

/// Maximum allowed size for general deserialization (4 MB).
/// SECURITY (AUDIT-020): Prevents DoS via oversized payloads.
pub const MAX_DESERIALIZE_SIZE: usize = 4 * 1024 * 1024;

/// Consensus serialization utilities
pub mod consensus_bincode {
    use super::*;

    /// Serialize data for consensus
    pub fn serialize_consensus<T: serde::Serialize>(data: &T) -> Result<Vec<u8>> {
        bincode::serialize(data).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))
    }

    /// Deserialize data for consensus with size limit.
    ///
    /// SECURITY (AUDIT-020): Rejects payloads larger than 4 MB.
    pub fn deserialize_consensus<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T> {
        if data.len() > MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Data too large for deserialization: {} bytes (max {})",
                data.len(),
                MAX_DESERIALIZE_SIZE
            );
        }
        bincode::deserialize(data).map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
    }
}

/// Default serialization utilities
pub mod default_bincode {
    use super::*;

    /// Serialize data with default settings
    pub fn serialize_default<T: serde::Serialize>(data: &T) -> Result<Vec<u8>> {
        bincode::serialize(data).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))
    }

    /// Deserialize data with default settings and size limit.
    ///
    /// SECURITY (AUDIT-020): Rejects payloads larger than 4 MB.
    pub fn deserialize_default<T: serde::de::DeserializeOwned>(data: &[u8]) -> Result<T> {
        if data.len() > MAX_DESERIALIZE_SIZE {
            anyhow::bail!(
                "Data too large for deserialization: {} bytes (max {})",
                data.len(),
                MAX_DESERIALIZE_SIZE
            );
        }
        bincode::deserialize(data).map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
    }
}

/// Get serialized size
pub fn serialized_size<T: serde::Serialize>(data: &T) -> Result<usize> {
    let serialized =
        bincode::serialize(data).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))?;
    Ok(serialized.len())
}

/// Check if data can be deserialized (with size limit).
///
/// SECURITY (AUDIT-020): Rejects payloads larger than 4 MB.
pub fn can_deserialize<T: serde::de::DeserializeOwned>(data: &[u8]) -> bool {
    if data.len() > MAX_DESERIALIZE_SIZE {
        return false;
    }
    bincode::deserialize::<T>(data).is_ok()
}

/// Serialize to hex string
pub fn serialize_to_hex<T: serde::Serialize>(data: &T) -> Result<String> {
    let serialized =
        bincode::serialize(data).map_err(|e| anyhow::anyhow!("Serialization failed: {}", e))?;
    Ok(bytes_to_hex_prefixed(&serialized))
}

/// Deserialize from hex string with size limit.
///
/// SECURITY (AUDIT-020): Rejects payloads larger than 4 MB.
pub fn deserialize_from_hex<T: serde::de::DeserializeOwned>(hex_str: &str) -> Result<T> {
    let bytes = hex_to_bytes_prefixed(hex_str)?;
    if bytes.len() > MAX_DESERIALIZE_SIZE {
        anyhow::bail!(
            "Hex data too large for deserialization: {} bytes (max {})",
            bytes.len(),
            MAX_DESERIALIZE_SIZE
        );
    }
    bincode::deserialize(&bytes).map_err(|e| anyhow::anyhow!("Deserialization failed: {}", e))
}

/// Batch processing utilities
pub mod batch {
    /// Process items in batches
    pub fn process_batch<T, R, F>(items: Vec<T>, batch_size: usize, mut f: F) -> Vec<R>
    where
        F: FnMut(&[T]) -> R,
    {
        let mut results = Vec::new();

        for chunk in items.chunks(batch_size) {
            results.push(f(chunk));
        }

        results
    }

    /// Split items into batches
    pub fn split_into_batches<T: Clone>(items: Vec<T>, batch_size: usize) -> Vec<Vec<T>> {
        items
            .chunks(batch_size)
            .map(|chunk| chunk.to_vec())
            .collect()
    }
}

/// Compression utilities
pub mod compression {
    use super::*;

    /// Compress data using simple run-length encoding
    pub fn compress_rle(data: &[u8]) -> Vec<u8> {
        let mut compressed = Vec::new();
        let mut i = 0;

        while i < data.len() {
            let current = data[i];
            let mut count = 1;

            while i + count < data.len() && data[i + count] == current && count < 255 {
                count += 1;
            }

            compressed.push(count as u8);
            compressed.push(current);
            i += count;
        }

        compressed
    }

    /// Decompress data using simple run-length encoding
    pub fn decompress_rle(compressed: &[u8]) -> Result<Vec<u8>> {
        let mut decompressed = Vec::new();
        let mut i = 0;

        while i < compressed.len() {
            if i + 1 >= compressed.len() {
                return Err(anyhow::anyhow!("Invalid compressed data"));
            }

            let count = compressed[i] as usize;
            let value = compressed[i + 1];

            for _ in 0..count {
                decompressed.push(value);
            }

            i += 2;
        }

        Ok(decompressed)
    }
}

/// Versioning utilities
pub mod versioning {
    use super::*;

    /// Parse version string
    pub fn parse_version(version_str: &str) -> Result<(u32, u32, u32)> {
        let parts: Vec<&str> = version_str.split('.').collect();
        if parts.len() != 3 {
            return Err(anyhow::anyhow!("Invalid version format"));
        }

        let major = parts[0].parse::<u32>()?;
        let minor = parts[1].parse::<u32>()?;
        let patch = parts[2].parse::<u32>()?;

        Ok((major, minor, patch))
    }

    /// Compare versions
    pub fn compare_versions(v1: &str, v2: &str) -> Result<i32> {
        let (major1, minor1, patch1) = parse_version(v1)?;
        let (major2, minor2, patch2) = parse_version(v2)?;

        if major1 != major2 {
            return Ok(major1 as i32 - major2 as i32);
        }

        if minor1 != minor2 {
            return Ok(minor1 as i32 - minor2 as i32);
        }

        Ok(patch1 as i32 - patch2 as i32)
    }

    /// Check if version is compatible
    pub fn is_compatible(current: &str, required: &str) -> Result<bool> {
        let comparison = compare_versions(current, required)?;
        Ok(comparison >= 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hex_conversions() {
        let bytes = vec![0x12, 0x34, 0x56, 0x78];
        let hex = bytes_to_hex(&bytes);
        assert_eq!(hex, "12345678");

        let recovered = hex_to_bytes(&hex).unwrap();
        assert_eq!(recovered, bytes);

        let hex_prefixed = bytes_to_hex_prefixed(&bytes);
        assert_eq!(hex_prefixed, "0x12345678");

        let recovered = hex_to_bytes_prefixed(&hex_prefixed).unwrap();
        assert_eq!(recovered, bytes);
    }

    #[test]
    fn test_string_conversions() {
        let num = 12345u64;
        let str_num = u64_to_str(num);
        assert_eq!(str_num, "12345");

        let recovered = str_to_u64(&str_num).unwrap();
        assert_eq!(recovered, num);
    }

    #[test]
    fn test_endian_conversions() {
        let num = 0x123456789ABCDEF0u64;
        let le_bytes = u64_to_bytes_le(num);
        let be_bytes = u64_to_bytes_be(num);

        assert_eq!(bytes_to_u64_le(&le_bytes).unwrap(), num);
        assert_eq!(bytes_to_u64_be(&be_bytes).unwrap(), num);
    }

    #[test]
    fn test_fixed_point() {
        use fixed_point::FixedPoint;

        let fp1 = FixedPoint::from_float(123.456, 3);
        assert_eq!(fp1.to_float(), 123.456);

        let fp2 = FixedPoint::from_float(78.901, 3);
        let sum = fp1.add(&fp2).unwrap();
        assert!((sum.to_float() - 202.357).abs() < 0.001);
    }

    #[test]
    fn test_stats() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(stats::mean(&values), 3.0);

        let mut values_copy = values.clone();
        assert_eq!(stats::median(&mut values_copy), 3.0);

        assert!((stats::std_dev(&values) - 1.581).abs() < 0.001);
    }

    #[test]
    fn test_versioning() {
        use versioning::*;

        let (major, minor, patch) = parse_version("1.2.3").unwrap();
        assert_eq!(major, 1);
        assert_eq!(minor, 2);
        assert_eq!(patch, 3);

        assert_eq!(compare_versions("1.2.3", "1.2.2").unwrap(), 1);
        assert_eq!(compare_versions("1.2.3", "1.2.3").unwrap(), 0);
        assert_eq!(compare_versions("1.2.3", "1.2.4").unwrap(), -1);

        assert!(is_compatible("1.2.3", "1.2.2").unwrap());
        assert!(!is_compatible("1.2.3", "1.2.4").unwrap());
    }

    #[test]
    fn test_compression() {
        let data = vec![1, 1, 1, 2, 2, 3, 3, 3, 3];
        let compressed = compression::compress_rle(&data);
        let decompressed = compression::decompress_rle(&compressed).unwrap();
        assert_eq!(data, decompressed);
    }

    #[test]
    fn test_timestamps() {
        let ts = now_timestamp();
        assert!(ts > 0);

        let ts_ms = now_timestamp_ms();
        assert!(ts_ms > ts * 1000);

        let added = add_seconds(ts, 60);
        assert_eq!(added, ts + 60);

        assert!(is_within_last_seconds(ts, 10));
    }

    #[test]
    fn test_wei_conversions() {
        let wei = 1_000_000_000_000_000_000u128; // 1 ether
        let ether = wei_to_ether(wei);
        assert!((ether - 1.0).abs() < 0.0001);

        let back_to_wei = ether_to_wei(ether);
        assert_eq!(back_to_wei, wei);
    }
}
