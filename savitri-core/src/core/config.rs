///
/// All configuration types should implement this trait to ensure
/// configuration is valid before node startup.
use anyhow::Result;

pub trait ValidateConfig {
    /// Validate the configuration
    ///
    /// Returns Ok(()) if configuration is valid, Err with descriptive
    ///
    /// Validation should check:
    /// - Required fields are present and non-empty
    /// - Numeric values are within valid ranges
    /// - File paths exist (if required)
    /// - String formats are valid (e.g., peer IDs, addresses)
    /// - Relationships between fields are consistent
    fn validate(&self) -> Result<()>;
}

pub fn validate_port(port: u16, field_name: &str) -> Result<()> {
    if port == 0 {
        anyhow::bail!(
            "configuration field '{}' must be greater than zero",
            field_name
        );
    }
    // port is u16, so it cannot exceed 65535 by definition
    Ok(())
}

pub fn validate_path_exists(path: &std::path::Path, field_name: &str) -> Result<()> {
    if path.as_os_str().is_empty() {
        anyhow::bail!("configuration field '{}' must not be empty", field_name);
    }
    if !path.exists() {
        anyhow::bail!(
            "configuration field '{}' path does not exist: {}",
            field_name,
            path.display()
        );
    }
    Ok(())
}

pub fn validate_non_empty(value: &str, field_name: &str) -> Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("configuration field '{}' must not be empty", field_name);
    }
    Ok(())
}

pub fn validate_positive<T>(value: T, field_name: &str) -> Result<()>
where
    T: PartialOrd + Default + std::fmt::Display + PartialOrd<T>,
{
    if value <= T::default() {
        anyhow::bail!(
            "configuration field '{}' must be greater than zero",
            field_name
        );
    }
    Ok(())
}

pub fn validate_array_non_empty<T: AsRef<str>>(array: &[T], field_name: &str) -> Result<()> {
    for (idx, entry) in array.iter().enumerate() {
        if entry.as_ref().trim().is_empty() {
            anyhow::bail!("{} entry at index {} is empty", field_name, idx);
        }
    }
    Ok(())
}

pub fn validate_array_not_empty<T>(array: &[T], field_name: &str) -> Result<()> {
    if array.is_empty() {
        anyhow::bail!("{} must not be empty", field_name);
    }
    Ok(())
}

pub fn validate_range<T: PartialOrd + std::fmt::Display>(
    value: T,
    min: T,
    max: T,
    field_name: &str,
) -> Result<()> {
    if value < min || value > max {
        anyhow::bail!(
            "configuration field '{}' must be between {} and {} (got {})",
            field_name,
            min,
            max,
            value
        );
    }
    Ok(())
}
