# Savitri Storage Test Execution Guide

## 📋 Overview

This guide provides comprehensive instructions for executing tests and benchmarks for the Savitri Storage layer. All tests are designed to validate production readiness and performance characteristics.

---

## 🚀 Quick Start

### Prerequisites
- Rust stable toolchain
- Windows/Linux x86_64 environment
- Sufficient disk space for test data
- Release build configuration

### Environment Setup
```bash
# Navigate to storage directory
cd savitri-storage

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

# Run specific test file
cargo test --test unit_tests --release
```

### **Unit Tests**
```bash
# Library unit tests
cargo test --lib --release

# Specific unit test file
cargo test --test unit_tests --release

# Individual test
cargo test test_storage_basic_operations --release
```

### **Integration Tests**
```bash
# All integration tests
cargo test --test integration_tests --release

# With output
cargo test --test integration_tests --release -- --nocapture

# Specific test
cargo test test_multi_threaded_access --release
```

### **Performance Tests**
```bash
# All performance tests
cargo test --test performance_tests --release

# Specific performance test
cargo test test_storage_write_performance --release
```

### **Stress Tests**
```bash
# All stress tests
cargo test --test stress_tests --release

# With timing information
cargo test --test stress_tests --release -- --nocapture

# Specific stress test
cargo test test_high_volume_writes --release
```

### **Comprehensive Tests**
```bash
# All comprehensive scenarios
cargo test --test comprehensive_tests --release

# Specific scenario
cargo test test_blockchain_scenario --release
```

---

## ⚡ Benchmark Execution

### **Enterprise Stress Benchmark**
```bash
# Run enterprise stress benchmark
cargo bench --bench enterprise_stress_test

# With release optimization
cargo bench --bench enterprise_stress_test -- --release

# Generate HTML report
cargo bench --bench enterprise_stress_test -- --output-format html
```

### **Storage Benchmarks**
```bash
# Note: storage_bench.rs needs fixes for rocksdb feature
# Current status: Compilation errors due to missing dependencies

# Alternative: Use enterprise_stress_test
cargo bench --bench enterprise_stress_test
```

---

## 📊 Test Categories

### **1. Unit Tests (8 tests)**
**Purpose**: Validate individual component functionality
**Files**: `src/lib.rs`, `tests/unit_tests.rs`
**Duration**: ~0.00s
**Coverage**: Core storage operations, FL storage, error handling

**Key Tests**:
- `test_fl_storage` - FL storage basic functionality
- `test_basic_storage` - Core storage operations
- `test_concurrent_operations` - Thread safety
- `test_error_handling` - Error recovery

### **2. Integration Tests (5 tests)**
**Purpose**: Validate component interaction
**File**: `tests/integration_tests.rs`
**Duration**: ~0.01s
**Coverage**: Cross-component functionality

**Key Tests**:
- `test_storage_fl_integration` - Storage/FL integration
- `test_multi_threaded_access` - Multi-threading safety
- `test_large_data_operations` - Large data handling

### **3. Performance Tests (6 tests)**
**Purpose**: Validate performance characteristics
**File**: `tests/performance_tests.rs`
**Duration**: ~0.03s
**Coverage**: Throughput and latency

**Key Tests**:
- `test_storage_write_performance` - Write throughput
- `test_storage_read_performance` - Read throughput
- `test_concurrent_performance` - Concurrent performance

### **4. Stress Tests (6 tests)**
**Purpose**: Validate system under load
**File**: `tests/stress_tests.rs`
**Duration**: ~0.43s
**Coverage**: High-load scenarios

**Key Tests**:
- `test_high_volume_writes` - High-volume stress
- `test_memory_pressure` - Memory pressure handling
- `test_concurrent_stress` - Concurrent stress

### **5. Comprehensive Tests (4 tests)**
**Purpose**: Validate real-world scenarios
**File**: `tests/comprehensive_tests.rs`
**Duration**: ~0.13s
**Coverage**: Production scenarios

**Key Tests**:
- `test_blockchain_scenario` - Blockchain workload
- `test_federated_learning_scenario` - ML workload
- `test_disaster_recovery_scenario` - Recovery testing

---

## 🔍 Troubleshooting

### **Common Issues**

#### **1. RocksDB Feature Missing**
```bash
Error: no `Storage` in the root

Solution: Enable rocksdb feature
cargo test --features rocksdb --release
```

#### **2. Benchmark Compilation Errors**
```bash
Error: unresolved imports `savitri_storage::Storage`

Solution: Fix imports in storage_bench.rs
# Current status: Known issue, use enterprise_stress_test instead
```

#### **3. Test Timeouts**
```bash
Error: test timeout

Solution: Increase timeout or run individual tests
cargo test test_name --release -- --test-threads=1
```

#### **4. Permission Issues**
```bash
Error: Permission denied creating temp directory

Solution: Run with appropriate permissions
# Windows: Run as Administrator
# Linux: Use appropriate user permissions
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

#### **Performance Profiling**
```bash
# With timing information
cargo test --tests --release -- --nocapture

# Memory usage tracking
cargo test --tests --release --features memory-profiling
```

---

## 📈 Performance Interpretation

### **Test Duration Guidelines**
- **Unit Tests**: < 0.01s (excellent)
- **Integration Tests**: < 0.05s (good)
- **Performance Tests**: < 0.10s (acceptable)
- **Stress Tests**: < 1.00s (good)
- **Comprehensive Tests**: < 0.50s (acceptable)

### **Benchmark Metrics**
- **Enterprise Stress**: ~1.0ns (excellent)
- **Throughput**: Measure ops/sec
- **Latency**: Measure response time
- **Memory**: Track memory usage

### **Success Criteria**
- **All Tests Pass**: 100% success rate
- **No Memory Leaks**: Clean resource management
- **Consistent Performance**: < 10% variance
- **Error Handling**: Graceful error recovery

---

## 🔧 Advanced Usage

### **Custom Test Configuration**
```bash
# Run specific test patterns
cargo test test_performance --release

# Exclude certain tests
cargo test --release -- --skip test_stress

# Run tests in single thread
cargo test --release -- --test-threads=1

# Run with specific features
cargo test --features rocksdb --release
```

### **Benchmark Customization**
```bash
# Custom benchmark parameters
cargo bench --bench enterprise_stress_test -- --sample-size 200

# Generate different output formats
cargo bench --bench enterprise_stress_test -- --output-format json

# Warm-up and measurement time
cargo bench --bench enterprise_stress_test -- --warm-up-time 5 --measurement-time 30
```

### **Integration with CI/CD**
```bash
# CI-friendly commands
cargo test --tests --release --quiet

# Generate test reports
cargo test --tests --release -- --format json | tee test_results.json

# Performance regression detection
cargo bench --bench enterprise_stress_test -- --save-baseline baseline
```

---

## 📊 Result Analysis

### **Test Output Interpretation**
```
running 6 tests
test test_storage_basic_operations ... ok
test test_concurrent_operations ... ok
test result: ok. 6 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s
```

**Key Metrics**:
- `running X tests`: Number of tests executed
- `ok`: Test passed successfully
- `test result`: Summary of results
- `finished in X.XXs`: Total execution time

### **Benchmark Output Interpretation**
```
enterprise_stress_test  time:   [997.73 ps 1.0220 ns 1.0471 ns]
Found 1 outliers among 100 measurements (1.00%)
```

**Key Metrics**:
- `time: [min median max]`: Performance range
- `outliers`: Statistical outliers detected
- `measurements`: Sample size for statistical validity

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

### **System Issues**
1. **Resource Cleanup**: Clean up temporary files
2. **Process Termination**: Kill hanging processes
3. **System Restart**: Restart if necessary
4. **Environment Reset**: Reset test environment

---

## 📞 Support and Resources

### **Documentation**
- **Raw Results**: `Results/RAW_TEST_RESULTS.md`
- **Analysis Report**: `Results/STORAGE_TEST_REPORT.md`
- **Code Documentation**: Inline Rust documentation

### **Troubleshooting Resources**
- **Rust Documentation**: https://doc.rust-lang.org/
- **Cargo Book**: https://doc.rust-lang.org/cargo/
- **Criterion.rs**: https://bheisler.github.io/criterion.rs/book/

### **Community Support**
- **Issues**: GitHub issue tracker
- **Discussions**: GitHub discussions
- **Documentation**: Project README and guides

---

**Last Updated**: 18-01-2026  
**Test Environment**: Windows Release Build  
**Guide Version**: 1.0  
**Status**: Production Ready

---

*This guide provides comprehensive instructions for executing and troubleshooting Savitri Storage tests and benchmarks. For the most up-to-date information, refer to the raw test results and analysis reports.*
