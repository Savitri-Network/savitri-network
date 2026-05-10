# Savitri Core Test Execution Guide

## 📋 Overview

This guide provides comprehensive instructions for executing tests and benchmarks for the Savitri Core library. All tests are designed to validate core blockchain functionality including cryptography, types, metrics, and utilities.

---

## 🚀 Quick Start

### Prerequisites
- Rust stable toolchain
- Windows/Linux x86_64 environment
- Sufficient disk space for test data
- Release build configuration

### Environment Setup
```bash
# Navigate to core directory
cd savitri-core

# Verify dependencies
cargo check --release

# Run all tests
cargo test --tests --release
```

---

## 🧪 Test Execution Commands

### **All Tests**
```bash
# Run all test suites
cargo test --tests --release

# Run with detailed output
cargo test --tests --release -- --nocapture

# Run library unit tests only
cargo test --lib --release

# Run integration tests only
cargo test --test lib_tests --release
```

### **Library Unit Tests**
```bash
# All library unit tests
cargo test --lib --release

# Specific module tests
cargo test core::types::tests --release
cargo test crypto::hash::tests --release
cargo test metrics::provider::tests --release
cargo test utils::math::tests --release

# Individual test
cargo test test_basic_hashing --release
```

### **Integration Tests**
```bash
# All integration tests
cargo test --test lib_tests --release

# With output
cargo test --test lib_tests --release -- --nocapture

# Specific integration test
cargo test test_cryptography --release
cargo test test_metrics --release
cargo test test_monolith --release
```

### **Module-Specific Tests**
```bash
# Core types tests
cargo test core::types::tests --release

# Cryptography tests
cargo test crypto:: --release

# Metrics tests
cargo test metrics:: --release

# Utility tests
cargo test utils:: --release
```

---

## ⚡ Benchmark Execution

### **Current Status: COMPILATION ERRORS**
```bash
# Note: Benchmarks currently fail to compile
# See "Troubleshooting" section for fixes

# Attempt to run benchmarks (will fail)
cargo bench --release

# After applying fixes (see troubleshooting)
cargo bench
```

### **Benchmark Issues**
- **Type Errors**: FixedPoint arithmetic expects `u128` but receives `&u128`
- **Unused Imports**: `Throughput` and `std::time::Instant` not used
- **Missing Dereference**: Variables need explicit dereferencing

---

## 📊 Test Categories

### **1. Core Types Tests (5 tests)**
**Purpose**: Validate fundamental blockchain types
**Module**: `src/core/types.rs`
**Duration**: ~0.01s
**Coverage**: Account, transaction, fee limits

**Key Tests**:
- `account_credit_overflow_checked` - Overflow protection
- `account_encoding_backward_compatibility` - Format compatibility
- `bank_grade_transactional_integrity` - Transaction validation
- `fee_limits_custom` - Custom fee limit validation
- `fee_limits_validate` - Fee limit enforcement

### **2. Cryptography Tests (45+ tests)**
**Purpose**: Validate cryptographic operations
**Modules**: `src/crypto/`
**Duration**: ~0.05s
**Coverage**: Hashing, encryption, signatures, keys

**Key Tests**:
- `test_basic_hashing` - SHA-256 hash operations
- `test_deterministic_behavior` - Consistent hash results
- `test_domain_separation` - Hash domain isolation
- `test_merkle_root` - Merkle tree operations
- `test_simple_xor_cipher` - XOR encryption
- `test_password_encryption` - Password-based encryption
- `test_keypair_generation` - Ed25519 key generation
- `test_signature_verification` - Digital signature validation
- `test_key_derivation` - Hierarchical key derivation

### **3. Metrics Tests (20+ tests)**
**Purpose**: Validate metrics collection and export
**Modules**: `src/metrics/`
**Duration**: ~0.03s
**Coverage**: Provider, exporter, manifest

**Key Tests**:
- `test_counter_increment` - Counter metric operations
- `test_gauge_set` - Gauge metric operations
- `test_json_export` - JSON format export
- `test_prometheus_export` - Prometheus format export
- `test_manifest_generation` - Metrics manifest creation
- `test_health_checker` - Health status monitoring

### **4. Utility Tests (50+ tests)**
**Purpose**: Validate utility functions
**Modules**: `src/utils/`
**Duration**: ~0.08s
**Coverage**: Math, conversions, time, serialization

**Key Tests**:
- `test_fixed_point_conversions` - Fixed-point arithmetic
- `test_hex_conversions` - Hexadecimal encoding/decoding
- `test_timestamp_conversions` - Unix timestamp operations
- `test_iso8601` - ISO8601 datetime formatting
- `test_slot_calculations` - Blockchain slot timing
- `test_basic_serialization` - Bincode serialization
- `test_compression` - Data compression utilities

### **5. Integration Tests (15 tests)**
**Purpose**: Validate cross-module functionality
**File**: `tests/lib_tests.rs`
**Duration**: ~0.07s
**Coverage**: End-to-end functionality

**Key Tests**:
- `test_basic_types` - Core type integration
- `test_cryptography` - End-to-end crypto operations
- `test_metrics` - Complete metrics pipeline
- `test_monolith` - Block processing integration
- `test_math_fixed_point` - Fixed-point math integration
- `test_slot_scheduler` - Time-based operations
- `test_transaction_root` - Transaction root calculation

---

## 🔍 Troubleshooting

### **Common Issues**

#### **1. Benchmark Compilation Errors**
```bash
Error: mismatched types in benches/math_performance.rs

Solution: Fix type errors by adding dereference operators
# See "Benchmark Fixes" section below
```

#### **2. Test Timeouts**
```bash
Error: test timeout

Solution: Run tests individually or increase timeout
cargo test test_name --release -- --test-threads=1
```

#### **3. Permission Issues**
```bash
Error: Permission denied creating temp directory

Solution: Run with appropriate permissions
# Windows: Run as Administrator
# Linux: Use appropriate user permissions
```

#### **4. Documentation Warnings**
```bash
Warning: missing documentation for a struct field

Solution: Add documentation comments or ignore warnings
#[allow(missing_docs)] // For individual items
```

### **Benchmark Fixes**

#### **Type Error Fixes**
```rust
// File: benches/math_performance.rs
// Lines 264-268: Add dereference operators

// BEFORE (broken):
let weighted_sum = fixed_point::mul(availability, fixed_point::from_string("0.3").unwrap())
                    + fixed_point::mul(latency, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(integrity, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(reputation, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(participation, fixed_point::from_string("0.1").unwrap());

// AFTER (fixed):
let weighted_sum = fixed_point::mul(*availability, fixed_point::from_string("0.3").unwrap())
                    + fixed_point::mul(*latency, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(*integrity, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(*reputation, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(*participation, fixed_point::from_string("0.1").unwrap());
```

#### **Unused Import Fixes**
```rust
// File: benches/math_performance.rs
// Line 1: Remove unused imports

// BEFORE:
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};

// AFTER:
use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};

// Line 3: Remove unused import
// REMOVE: use std::time::Instant;
```

### **Debug Commands**

#### **Verbose Output**
```bash
# Maximum verbosity
cargo test --tests --release -- --nocapture --exact

# Backtrace on failure
cargo test --tests --release -- --nocapture --backtrace

# Individual test with debug
cargo test test_name --release -- --nocapture --exact
```

#### **Module-Specific Debugging**
```bash
# Debug specific module
cargo test crypto::hash::tests --release -- --nocapture

# Debug with filtering
cargo test hash --release -- --nocapture
```

---

## 📈 Performance Interpretation

### **Test Duration Guidelines**
- **Unit Tests**: < 0.20s (excellent)
- **Integration Tests**: < 0.10s (good)
- **Total Execution**: < 1.00s (excellent)
- **Per Test Average**: < 0.005s (excellent)

### **Success Criteria**
- **All Tests Pass**: 100% success rate
- **No Memory Leaks**: Clean resource management
- **Consistent Performance**: < 10% variance between runs
- **Error Handling**: Graceful error recovery

### **Benchmark Metrics** (when fixed)
- **Math Operations**: Fixed-point arithmetic performance
- **Cryptographic Operations**: Hash and signature performance
- **Serialization**: Bincode performance metrics
- **Memory Usage**: Allocation and deallocation efficiency

---

## 🔧 Advanced Usage

### **Custom Test Configuration**
```bash
# Run specific test patterns
cargo test crypto --release
cargo test hash --release
cargo test metrics --release

# Exclude certain tests
cargo test --release -- --skip test_slow

# Run tests in single thread
cargo test --release -- --test-threads=1

# Run with specific features
cargo test --features some-feature --release
```

### **Integration with CI/CD**
```bash
# CI-friendly commands
cargo test --tests --release --quiet

# Generate test reports
cargo test --tests --release -- --format json | tee test_results.json

# Performance regression detection (when benchmarks fixed)
cargo bench -- --save-baseline baseline
```

### **Development Workflow**
```bash
# Fast development cycle
cargo test --lib --release                    # Quick unit tests
cargo test --test lib_tests --release         # Integration tests
cargo bench                                    # Performance tests (when fixed)

# Full validation
cargo test --tests --release && cargo bench   # Complete test suite
```

---

## 📊 Result Analysis

### **Test Output Interpretation**
```
running 139 tests
test core::types::tests::account_credit_overflow_checked ... ok
test crypto::hash::tests::test_basic_hashing ... ok
test result: ok. 139 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.17s
```

**Key Metrics**:
- `running X tests`: Number of tests executed
- `ok`: Test passed successfully
- `test result`: Summary of results
- `finished in X.XXs`: Total execution time

### **Integration Test Output**
```
running 15 tests
test test_basic_types ... ok
test test_cryptography ... ok
test result: ok. 15 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.07s
```

### **Benchmark Output** (when fixed)
```
math_performance  time:   [1.2345 µs 1.2356 µs 1.2367 µs]
Found 2 outliers among 100 measurements (2.00%)
```

---

## 📝 Best Practices

### **Before Running Tests**
1. **Clean Build**: Start with clean build environment
2. **Check Dependencies**: Verify all dependencies are available
3. **Resource Availability**: Ensure sufficient disk/memory
4. **Environment Setup**: Verify test environment configuration

### **During Test Execution**
1. **Monitor Resources**: Watch memory and CPU usage
2. **Check Logs**: Review test output for warnings
3. **Validate Results**: Ensure expected test counts
4. **Record Performance**: Note execution times

### **After Test Completion**
1. **Review Results**: Analyze test outcomes
2. **Check Coverage**: Verify test coverage completeness
3. **Document Issues**: Record any problems found
4. **Archive Results**: Save test reports for comparison

### **Benchmark Best Practices** (when fixed)
1. **Warm-up**: Allow system to warm up before benchmarks
2. **Multiple Runs**: Run benchmarks multiple times for consistency
3. **Baseline Comparison**: Compare with previous results
4. **Environment Control**: Keep test environment consistent

---

## 🚨 Emergency Procedures

### **Test Failures**
1. **Isolate Issue**: Run failing test individually
2. **Check Environment**: Verify test environment
3. **Review Code**: Check recent changes
4. **Consult Logs**: Review detailed error messages

### **Performance Regression**
1. **Baseline Comparison**: Compare with previous results
2. **Environment Check**: Verify test environment consistency
3. **Profiling**: Use profiling tools to identify bottlenecks
4. **Rollback**: Consider rolling back problematic changes

### **Benchmark Failures**
1. **Compilation Check**: Verify benchmarks compile
2. **Type Validation**: Check for type mismatches
3. **Import Cleanup**: Remove unused imports
4. **Dereference Check**: Ensure proper variable dereferencing

### **System Issues**
1. **Resource Cleanup**: Clean up temporary files
2. **Process Termination**: Kill hanging processes
3. **System Restart**: Restart if necessary
4. **Environment Reset**: Reset test environment

---

## 📞 Support and Resources

### **Documentation**
- **Raw Results**: `Results/RAW_TEST_RESULTS.md`
- **Analysis Report**: `Results/CORE_TEST_REPORT.md`
- **Code Documentation**: Inline Rust documentation
- **API Reference**: Generated documentation via `cargo doc`

### **Troubleshooting Resources**
- **Rust Documentation**: https://doc.rust-lang.org/
- **Cargo Book**: https://doc.rust-lang.org/cargo/
- **Criterion.rs**: https://bheisler.github.io/criterion.rs/book/

### **Community Support**
- **Issues**: GitHub issue tracker
- **Discussions**: GitHub discussions
- **Documentation**: Project README and guides

---

## 📋 Quick Reference

### **Essential Commands**
```bash
# All tests
cargo test --tests --release

# Unit tests only
cargo test --lib --release

# Integration tests only
cargo test --test lib_tests --release

# Benchmarks (when fixed)
cargo bench

# Specific module
cargo test crypto:: --release
```

### **Common Issues & Solutions**
- **Benchmark fails**: Fix type errors in math_performance.rs
- **Documentation warnings**: Add docs or use #[allow(missing_docs)]
- **Test timeouts**: Run with --test-threads=1
- **Permission errors**: Run as administrator

### **Performance Expectations**
- **Total test time**: < 1 second
- **Unit test time**: < 0.2 seconds
- **Integration test time**: < 0.1 seconds
- **Per test average**: < 0.005 seconds

---

**Last Updated**: 18-01-2026  
**Test Environment**: Windows Release Build  
**Guide Version**: 1.0  
**Status**: Production Ready (with benchmark fixes pending)

---

⚠️ **Important Note**: Benchmarks currently fail to compile due to type errors. See the "Benchmark Fixes" section for detailed solutions before attempting to run performance tests.

---

*This guide provides comprehensive instructions for executing and troubleshooting Savitri Core tests and benchmarks. For the most up-to-date information, refer to the raw test results and analysis reports.*
