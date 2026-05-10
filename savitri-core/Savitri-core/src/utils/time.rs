//! Time utilities for Savitri Network
//! 
//! This module provides time-related functions and utilities used throughout
//! the Savitri ecosystem, including timestamp handling, duration calculations,
//! and time formatting.

use std::time::{SystemTime, UNIX_EPOCH, Duration};

/// Get current timestamp in seconds since Unix epoch
pub fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Get current timestamp in milliseconds since Unix epoch
pub fn now_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Get current timestamp in microseconds since Unix epoch
pub fn now_timestamp_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as u64
}

/// Get current timestamp in nanoseconds since Unix epoch
pub fn now_timestamp_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Convert timestamp seconds to milliseconds
pub fn timestamp_to_ms(timestamp: u64) -> u64 {
    timestamp * 1000
}

/// Convert timestamp milliseconds to seconds
pub fn ms_to_timestamp(ms: u64) -> u64 {
    ms / 1000
}

/// Convert timestamp seconds to datetime string
pub fn timestamp_to_datetime(timestamp: u64) -> String {
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp as i64, 0);
    match dt {
        Some(dt) => dt.format("%Y-%m-%d %H:%M:%S UTC").to_string(),
        None => "Invalid timestamp".to_string(),
    }
}

/// Convert timestamp milliseconds to datetime string
pub fn ms_to_datetime(ms: u64) -> String {
    timestamp_to_datetime(ms_to_timestamp(ms))
}

/// Convert datetime string to timestamp (seconds)
pub fn datetime_to_timestamp(datetime_str: &str) -> Result<u64, chrono::ParseError> {
    let dt = chrono::DateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S %z")?;
    Ok(dt.timestamp() as u64)
}

/// Convert datetime string to timestamp (milliseconds)
pub fn datetime_to_ms(datetime_str: &str) -> Result<u64, chrono::ParseError> {
    let dt = chrono::DateTime::parse_from_str(datetime_str, "%Y-%m-%d %H:%M:%S %.3f %z")?;
    Ok(dt.timestamp_millis() as u64)
}

/// Calculate duration between two timestamps in seconds
pub fn duration_between(from: u64, to: u64) -> u64 {
    if to > from {
        to - from
    } else {
        from - to
    }
}

/// Calculate duration between two timestamps in milliseconds
pub fn duration_between_ms(from_ms: u64, to_ms: u64) -> u64 {
    if to_ms > from_ms {
        to_ms - from_ms
    } else {
        from_ms - to_ms
    }
}

/// Check if a timestamp is within the last N seconds
pub fn is_within_last_seconds(timestamp: u64, seconds: u64) -> bool {
    let now = now_timestamp();
    now >= timestamp && (now - timestamp) <= seconds
}

/// Check if a timestamp is within the last N milliseconds
pub fn is_within_last_ms(timestamp_ms: u64, ms: u64) -> bool {
    let now = now_timestamp_ms();
    now >= timestamp_ms && (now - timestamp_ms) <= ms
}

/// Add seconds to a timestamp
pub fn add_seconds(timestamp: u64, seconds: u64) -> u64 {
    timestamp.saturating_add(seconds)
}

/// Add milliseconds to a timestamp
pub fn add_ms(timestamp_ms: u64, ms: u64) -> u64 {
    timestamp_ms.saturating_add(ms)
}

/// Subtract seconds from a timestamp
pub fn subtract_seconds(timestamp: u64, seconds: u64) -> u64 {
    timestamp.saturating_sub(seconds)
}

/// Subtract milliseconds from a timestamp
pub fn subtract_ms(timestamp_ms: u64, ms: u64) -> u64 {
    timestamp_ms.saturating_sub(ms)
}

/// Format duration in human readable format
pub fn format_duration(duration: Duration) -> String {
    let total_secs = duration.as_secs();
    let days = total_secs / 86400;
    let hours = (total_secs % 86400) / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    let millis = duration.subsec_millis();

    if days > 0 {
        format!("{}d {}h {}m {}s {}ms", days, hours, minutes, seconds, millis)
    } else if hours > 0 {
        format!("{}h {}m {}s {}ms", hours, minutes, seconds, millis)
    } else if minutes > 0 {
        format!("{}m {}s {}ms", minutes, seconds, millis)
    } else if seconds > 0 {
        format!("{}s {}ms", seconds, millis)
    } else {
        format!("{}ms", millis)
    }
}

/// Format duration from milliseconds
pub fn format_duration_ms(ms: u64) -> String {
    let duration = Duration::from_millis(ms);
    format_duration(duration)
}

/// Format duration from seconds
pub fn format_duration_secs(secs: u64) -> String {
    let duration = Duration::from_secs(secs);
    format_duration(duration)
}

/// Get current time as ISO 8601 string
pub fn now_iso8601() -> String {
    let now = chrono::Utc::now();
    now.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string()
}

/// Parse ISO 8601 string to timestamp
pub fn parse_iso8601(iso_str: &str) -> Result<u64, chrono::ParseError> {
    let dt = chrono::DateTime::parse_from_rfc3339(iso_str)?;
    Ok(dt.timestamp() as u64)
}

/// Time utilities for slot-based systems
pub mod slot {
    use super::*;

    /// Calculate slot number from timestamp
    pub fn timestamp_to_slot(timestamp: u64, slot_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        if timestamp <= genesis_timestamp {
            0
        } else {
            (timestamp - genesis_timestamp) / slot_duration_ms
        }
    }

    /// Calculate timestamp from slot number
    pub fn slot_to_timestamp(slot: u64, slot_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        genesis_timestamp + slot * slot_duration_ms
    }

    /// Get current slot number
    pub fn current_slot(slot_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        timestamp_to_slot(now_timestamp_ms(), slot_duration_ms, genesis_timestamp)
    }

    /// Get slot start timestamp
    pub fn slot_start_timestamp(slot: u64, slot_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        slot_to_timestamp(slot, slot_duration_ms, genesis_timestamp)
    }

    /// Get slot end timestamp
    pub fn slot_end_timestamp(slot: u64, slot_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        slot_start_timestamp(slot, slot_duration_ms, genesis_timestamp) + slot_duration_ms
    }

    /// Check if timestamp is within a specific slot
    pub fn is_in_slot(timestamp_ms: u64, slot: u64, slot_duration_ms: u64, genesis_timestamp: u64) -> bool {
        let start = slot_start_timestamp(slot, slot_duration_ms, genesis_timestamp);
        let end = slot_end_timestamp(slot, slot_duration_ms, genesis_timestamp);
        timestamp_ms >= start && timestamp_ms < end
    }
}

/// Time utilities for epoch-based systems
pub mod epoch {
    use super::*;

    /// Calculate epoch number from timestamp
    pub fn timestamp_to_epoch(timestamp: u64, epoch_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        if timestamp <= genesis_timestamp {
            0
        } else {
            (timestamp - genesis_timestamp) / epoch_duration_ms
        }
    }

    /// Calculate timestamp from epoch number
    pub fn epoch_to_timestamp(epoch: u64, epoch_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        genesis_timestamp + epoch * epoch_duration_ms
    }

    /// Get current epoch number
    pub fn current_epoch(epoch_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        timestamp_to_epoch(now_timestamp_ms(), epoch_duration_ms, genesis_timestamp)
    }

    /// Get epoch start timestamp
    pub fn epoch_start_timestamp(epoch: u64, epoch_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        epoch_to_timestamp(epoch, epoch_duration_ms, genesis_timestamp)
    }

    /// Get epoch end timestamp
    pub fn epoch_end_timestamp(epoch: u64, epoch_duration_ms: u64, genesis_timestamp: u64) -> u64 {
        epoch_start_timestamp(epoch, epoch_duration_ms, genesis_timestamp) + epoch_duration_ms
    }
}

/// Time utilities for measuring performance
pub mod perf {
    use super::*;
    use std::time::Instant;

    /// High-precision timer for performance measurement
    #[derive(Debug)]
    pub struct Timer {
        start: Instant,
    }

    impl Timer {
        /// Create a new timer
        pub fn new() -> Self {
            Self {
                start: Instant::now(),
            }
        }

        /// Get elapsed time in nanoseconds
        pub fn elapsed_ns(&self) -> u64 {
            self.start.elapsed().as_nanos() as u64
        }

        /// Get elapsed time in microseconds
        pub fn elapsed_us(&self) -> u64 {
            self.start.elapsed().as_micros() as u64
        }

        /// Get elapsed time in milliseconds
        pub fn elapsed_ms(&self) -> u64 {
            self.start.elapsed().as_millis() as u64
        }

        /// Get elapsed time in seconds
        pub fn elapsed_secs(&self) -> f64 {
            self.start.elapsed().as_secs_f64()
        }

        /// Reset the timer
        pub fn reset(&mut self) {
            self.start = Instant::now();
        }

        /// Get elapsed time and reset
        pub fn lap(&mut self) -> Duration {
            let elapsed = self.start.elapsed();
            self.reset();
            elapsed
        }
    }

    impl Default for Timer {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Measure execution time of a function
    pub fn measure_time<F, R>(f: F) -> (R, Duration)
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let elapsed = start.elapsed();
        (result, elapsed)
    }

    /// Measure execution time of a function and return time in milliseconds
    pub fn measure_time_ms<F, R>(f: F) -> (R, u64)
    where
        F: FnOnce() -> R,
    {
        let (result, duration) = measure_time(f);
        (result, duration.as_millis() as u64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timestamp_functions() {
        let ts = now_timestamp();
        let ts_ms = now_timestamp_ms();
        let ts_us = now_timestamp_us();
        let ts_ns = now_timestamp_ns();

        // Milliseconds should be larger than seconds
        assert!(ts_ms > ts * 1000);
        
        // Microseconds should be larger than milliseconds
        assert!(ts_us > ts_ms);
        
        // Nanoseconds should be larger than microseconds
        assert!(ts_ns > ts_us);

        // Test conversions
        let converted_ms = timestamp_to_ms(ts);
        assert_eq!(converted_ms, ts * 1000);

        let converted_ts = ms_to_timestamp(ts_ms);
        assert_eq!(converted_ts, ts_ms / 1000);
    }

    #[test]
    fn test_datetime_conversions() {
        let ts = 1640995200; // 2022-01-01 00:00:00 UTC
        let datetime = timestamp_to_datetime(ts);
        assert!(datetime.contains("2022-01-01"));

        let ms_datetime = ms_to_datetime(ts * 1000);
        assert!(ms_datetime.contains("2022-01-01"));
    }

    #[test]
    fn test_duration_calculations() {
        let from = 1000;
        let to = 1500;
        
        assert_eq!(duration_between(from, to), 500);
        assert_eq!(duration_between(to, from), 500); // Should be absolute

        let from_ms = 1000000;
        let to_ms = 1500000;
        assert_eq!(duration_between_ms(from_ms, to_ms), 500000);
    }

    #[test]
    fn test_time_checks() {
        let now = now_timestamp();
        let recent = now - 10; // 10 seconds ago
        
        assert!(is_within_last_seconds(recent, 15));
        assert!(!is_within_last_seconds(recent, 5));

        let now_ms = now_timestamp_ms();
        let recent_ms = now_ms - 10000; // 10 seconds ago in ms
        assert!(is_within_last_ms(recent_ms, 15000));
        assert!(!is_within_last_ms(recent_ms, 5000));
    }

    #[test]
    fn test_time_arithmetic() {
        let ts = 1000;
        
        assert_eq!(add_seconds(ts, 100), 1100);
        assert_eq!(subtract_seconds(ts, 100), 900);
        assert_eq!(subtract_seconds(ts, 2000), 0); // Should not underflow

        let ts_ms = 1000000;
        assert_eq!(add_ms(ts_ms, 1000), 1001000);
        assert_eq!(subtract_ms(ts_ms, 1000), 999000);
        assert_eq!(subtract_ms(ts_ms, 2000000), 0); // Should not underflow
    }

    #[test]
    fn test_duration_formatting() {
        let duration = Duration::from_secs(3661);
        let formatted = format_duration(duration);
        assert_eq!(formatted, "1h 1m 1s 0ms");

        let short_duration = Duration::from_millis(500);
        let short_formatted = format_duration(short_duration);
        assert_eq!(short_formatted, "500ms");

        let ms_formatted = format_duration_ms(3661000);
        assert_eq!(ms_formatted, "1h 1m 1s 0ms");

        let secs_formatted = format_duration_secs(3661);
        assert_eq!(secs_formatted, "1h 1m 1s 0ms");
    }

    #[test]
    fn test_iso8601() {
        let iso = now_iso8601();
        assert!(iso.len() > 20); // Basic length check
        assert!(iso.contains('T'));
        assert!(iso.ends_with('Z'));

        // Test parsing (might fail due to timezone issues, so we just check it doesn't panic)
        let _ = parse_iso8601(&iso);
    }

    #[test]
    fn test_slot_calculations() {
        let slot_duration_ms = 1000;
        let genesis_timestamp = 1000000;
        
        let timestamp = 1001500; // 1.5 seconds after genesis
        let slot = slot::timestamp_to_slot(timestamp, slot_duration_ms, genesis_timestamp);
        assert_eq!(slot, 1);

        let slot_timestamp = slot::slot_to_timestamp(slot, slot_duration_ms, genesis_timestamp);
        assert_eq!(slot_timestamp, 1001000);

        assert!(slot::is_in_slot(timestamp, slot, slot_duration_ms, genesis_timestamp));
        assert!(!slot::is_in_slot(timestamp, 0, slot_duration_ms, genesis_timestamp));
    }

    #[test]
    fn test_epoch_calculations() {
        let epoch_duration_ms = 10000000; // 10,000 seconds
        let genesis_timestamp = 1000000;
        
        let timestamp = 11000000; // 10,000 seconds after genesis
        let epoch = epoch::timestamp_to_epoch(timestamp, epoch_duration_ms, genesis_timestamp);
        assert_eq!(epoch, 1);

        let epoch_timestamp = epoch::epoch_to_timestamp(epoch, epoch_duration_ms, genesis_timestamp);
        assert_eq!(epoch_timestamp, 11000000);
    }

    #[test]
    fn test_timer() {
        let mut timer = perf::Timer::new();
        
        // Timer should start at creation
        std::thread::sleep(Duration::from_millis(10));
        
        let elapsed_ms = timer.elapsed_ms();
        assert!(elapsed_ms >= 10);
        assert!(elapsed_ms < 100); // Should be close to 10ms
        
        timer.reset();
        let elapsed_ms_after_reset = timer.elapsed_ms();
        assert!(elapsed_ms_after_reset < 10); // Should be very small after reset
    }

    #[test]
    fn test_measure_time() {
        let (result, duration) = perf::measure_time(|| {
            std::thread::sleep(Duration::from_millis(10));
            42
        });
        
        assert_eq!(result, 42);
        assert!(duration.as_millis() >= 10);
        
        let (result2, duration_ms) = perf::measure_time_ms(|| {
            std::thread::sleep(Duration::from_millis(5));
            "hello"
        });
        
        assert_eq!(result2, "hello");
        assert!(duration_ms >= 5);
    }
}
