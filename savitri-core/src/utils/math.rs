//! Mathematical utilities

use anyhow::Result;
use std::f64::consts;

/// Re-export fixed point arithmetic from convert module
pub use crate::utils::convert::fixed_point;

/// Re-export statistical functions from convert module  
pub use crate::utils::convert::stats;

/// Re-export basic math conversion functions from convert module
pub use crate::utils::convert::{bps_to_percent, fixed_to_float, float_to_fixed, percent_to_bps};

/// Mathematical constants
pub mod constants {
    /// Pi constant (π)
    pub const PI: f64 = std::f64::consts::PI;

    /// Euler's number (e)
    pub const E: f64 = std::f64::consts::E;

    /// Golden ratio (φ)
    pub const GOLDEN_RATIO: f64 = 1.618033988749895;

    /// Square root of 2
    pub const SQRT_2: f64 = std::f64::consts::SQRT_2;

    /// Natural logarithm of 2
    pub const LN_2: f64 = std::f64::consts::LN_2;

    /// Base-10 logarithm of 2
    pub const LOG10_2: f64 = std::f64::consts::LOG10_2;
}

/// Basic mathematical operations
pub mod basic {
    use std::ops::Neg;

    /// Calculate the absolute value of a number
    #[inline]
    pub fn abs<T: PartialOrd + Default + Neg<Output = T>>(x: T) -> T {
        if x < T::default() {
            -x
        } else {
            x
        }
    }

    /// Calculate the minimum of two values
    #[inline]
    pub fn min<T: PartialOrd>(a: T, b: T) -> T {
        if a < b {
            a
        } else {
            b
        }
    }

    /// Calculate the maximum of two values
    #[inline]
    pub fn max<T: PartialOrd>(a: T, b: T) -> T {
        if a > b {
            a
        } else {
            b
        }
    }

    /// Clamp a value between min and max
    #[inline]
    pub fn clamp<T: PartialOrd>(value: T, min: T, max: T) -> T {
        if value < min {
            min
        } else if value > max {
            max
        } else {
            value
        }
    }
}

/// Power and logarithmic functions
pub mod power {
    use super::*;

    /// Calculate x raised to the power of y (x^y)
    pub fn pow(x: f64, y: f64) -> f64 {
        x.powf(y)
    }

    /// Calculate x raised to an integer power
    pub fn powi(x: f64, y: i32) -> f64 {
        x.powi(y)
    }

    /// Calculate square root of x
    pub fn sqrt(x: f64) -> Result<f64> {
        if x < 0.0 {
            return Err(anyhow::anyhow!(
                "Cannot calculate square root of negative number"
            ));
        }
        Ok(x.sqrt())
    }

    /// Calculate cube root of x
    pub fn cbrt(x: f64) -> f64 {
        x.cbrt()
    }

    /// Calculate natural logarithm (base e)
    pub fn ln(x: f64) -> Result<f64> {
        if x <= 0.0 {
            return Err(anyhow::anyhow!(
                "Cannot calculate logarithm of non-positive number"
            ));
        }
        Ok(x.ln())
    }

    /// Calculate base-10 logarithm
    pub fn log10(x: f64) -> Result<f64> {
        if x <= 0.0 {
            return Err(anyhow::anyhow!(
                "Cannot calculate logarithm of non-positive number"
            ));
        }
        Ok(x.log10())
    }

    /// Calculate base-2 logarithm
    pub fn log2(x: f64) -> Result<f64> {
        if x <= 0.0 {
            return Err(anyhow::anyhow!(
                "Cannot calculate logarithm of non-positive number"
            ));
        }
        Ok(x.log2())
    }

    /// Calculate exponential function (e^x)
    pub fn exp(x: f64) -> f64 {
        x.exp()
    }
}

/// Trigonometric functions
pub mod trigonometry {
    use super::*;

    /// Calculate sine of angle (in radians)
    pub fn sin(x: f64) -> f64 {
        x.sin()
    }

    /// Calculate cosine of angle (in radians)
    pub fn cos(x: f64) -> f64 {
        x.cos()
    }

    /// Calculate tangent of angle (in radians)
    pub fn tan(x: f64) -> f64 {
        x.tan()
    }

    /// Calculate arcsine (inverse sine)
    pub fn asin(x: f64) -> Result<f64> {
        if x < -1.0 || x > 1.0 {
            return Err(anyhow::anyhow!(
                "Domain error: arcsine input must be in [-1, 1]"
            ));
        }
        Ok(x.asin())
    }

    /// Calculate arccosine (inverse cosine)
    pub fn acos(x: f64) -> Result<f64> {
        if x < -1.0 || x > 1.0 {
            return Err(anyhow::anyhow!(
                "Domain error: arccosine input must be in [-1, 1]"
            ));
        }
        Ok(x.acos())
    }

    /// Calculate arctangent (inverse tangent)
    pub fn atan(x: f64) -> f64 {
        x.atan()
    }

    /// Convert degrees to radians
    pub fn deg_to_rad(degrees: f64) -> f64 {
        degrees * consts::PI / 180.0
    }

    /// Convert radians to degrees
    pub fn rad_to_deg(radians: f64) -> f64 {
        radians * 180.0 / consts::PI
    }
}

/// Rounding functions
pub mod rounding {
    /// Round to nearest integer
    pub fn round(x: f64) -> f64 {
        x.round()
    }

    /// Round down to nearest integer (floor)
    pub fn floor(x: f64) -> f64 {
        x.floor()
    }

    /// Round up to nearest integer (ceiling)
    pub fn ceil(x: f64) -> f64 {
        x.ceil()
    }

    /// Truncate decimal part (toward zero)
    pub fn trunc(x: f64) -> f64 {
        x.trunc()
    }
}

/// Financial calculations
pub mod financial {
    /// Calculate compound interest
    ///
    /// # Arguments
    /// * `principal` - Initial amount
    /// * `rate` - Annual interest rate (as decimal, e.g., 0.05 for 5%)
    /// * `periods` - Number of compounding periods
    /// * `compounds_per_period` - How many times to compound per period
    pub fn compound_interest(
        principal: f64,
        rate: f64,
        periods: u32,
        compounds_per_period: u32,
    ) -> f64 {
        let total_compounds = periods as f64 * compounds_per_period as f64;
        let period_rate = rate / compounds_per_period as f64;
        principal * (1.0 + period_rate).powf(total_compounds)
    }

    /// Calculate simple interest
    pub fn simple_interest(principal: f64, rate: f64, periods: u32) -> f64 {
        principal * (1.0 + rate * periods as f64)
    }

    /// Calculate present value of future cash flow
    pub fn present_value(future_value: f64, rate: f64, periods: u32) -> f64 {
        future_value / (1.0 + rate).powf(periods as f64)
    }

    /// Calculate future value with compound interest
    pub fn future_value(present_value: f64, rate: f64, periods: u32) -> f64 {
        present_value * (1.0 + rate).powf(periods as f64)
    }
}

