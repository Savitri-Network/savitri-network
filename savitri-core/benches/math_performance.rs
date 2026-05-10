use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use savitri_core::utils::math::{fixed_point, stats, utils};

fn bench_math_operations(c: &mut Criterion) {
    let a = fixed_point::from_string("123.456789").unwrap();
    let b = fixed_point::from_string("789.012345").unwrap();

    c.bench_function("mul_fixed_point", |bencher| {
        bencher.iter(|| {
            black_box(fixed_point::mul(black_box(a), black_box(b)));
        });
    });

    c.bench_function("div_fixed_point", |bencher| {
        bencher.iter(|| {
            black_box(fixed_point::div(black_box(a), black_box(b)));
        });
    });

    c.bench_function("sqrt_fixed_point", |bencher| {
        bencher.iter(|| {
            black_box(fixed_point::sqrt(black_box(a)));
        });
    });

    // Benchmark con throughput
    c.bench_function("mul_throughput", |bencher| {
        bencher.iter(|| {
            black_box(fixed_point::mul(black_box(a), black_box(b)));
        });
    });
}

fn bench_statistics(c: &mut Criterion) {
    let values: Vec<fixed_point::FixedPoint> = (1..=1000)
        .map(|i| fixed_point::from_string(&format!("{}.{}", i, i % 1000)).unwrap())
        .collect();

    c.bench_function("mean_1000_values", |bencher| {
        bencher.iter(|| {
            black_box(stats::mean(black_box(&values)));
        });
    });

    c.bench_function("std_deviation_1000_values", |bencher| {
        bencher.iter(|| {
            black_box(stats::std_deviation(black_box(&values)));
        });
    });

    c.bench_function("quartiles_1000_values", |bencher| {
        let mut values_clone = values.clone();
        bencher.iter(|| {
            black_box(stats::quartiles(black_box(&mut values_clone)));
        });
    });

    // Benchmark con dataset più grandi
    let large_values: Vec<fixed_point::FixedPoint> = (1..=10000)
        .map(|i| fixed_point::from_string(&format!("{}.{}", i, i % 10000)).unwrap())
        .collect();

    c.bench_function("mean_10000_values", |bencher| {
        bencher.iter(|| {
            black_box(stats::mean(black_box(&large_values)));
        });
    });
}

fn bench_parsing(c: &mut Criterion) {
    let test_strings = vec![
        "123.456789",
        "0.000000000000000001",
        "999999.999999999999",
        "1.0",
        ".45",
        "123.",
    ];

    for (i, s) in test_strings.iter().enumerate() {
        c.bench_with_input(
            BenchmarkId::new("parse_fixed_point", format!("string_{}", i)),
            s,
            |bencher, input| {
                bencher.iter(|| {
                    black_box(fixed_point::from_string(black_box(input)).unwrap());
                });
            },
        );
    }

    // Benchmark parsing throughput
    c.bench_function("parse_throughput", |bencher| {
        bencher.iter(|| {
            for s in &test_strings {
                black_box(fixed_point::from_string(black_box(s)).unwrap());
            }
        });
    });
}

fn bench_blockchain_operations(c: &mut Criterion) {
    let gas_prices: Vec<fixed_point::FixedPoint> = (1..=100)
        .map(|i| fixed_point::from_string(&format!("{}.{}", 20 + i % 20, i % 100)).unwrap())
        .collect();

    c.bench_function("blockchain_gas_price_stats", |bencher| {
        let mut prices = gas_prices.clone();
        bencher.iter(|| {
            let avg = stats::mean(black_box(&prices));
            let vol = stats::std_deviation(black_box(&prices));
            let (q1, q2, q3) = stats::quartiles(black_box(&mut prices));
            black_box((avg, vol, (q1, q2, q3)));
        });
    });

    // Test di interesse composto
    let principal = fixed_point::from_string("1000.0").unwrap();
    let rate = fixed_point::from_string("0.05").unwrap();

    c.bench_function("compound_interest_12_periods", |bencher| {
        bencher.iter(|| {
            black_box(utils::compound_interest(
                black_box(principal),
                black_box(rate),
                12,
            ));
        });
    });

    let validator_scores: Vec<fixed_point::FixedPoint> = (1..=100)
        .map(|i| fixed_point::from_string(&format!("0.{}", i % 100)).unwrap())
        .collect();

    c.bench_function("pou_score_calculation", |bencher| {
        bencher.iter(|| {
            let availability = black_box(validator_scores[0]);
            let latency = black_box(validator_scores[1]);
            let integrity = black_box(validator_scores[2]);
            let reputation = black_box(validator_scores[3]);
            let participation = black_box(validator_scores[4]);

            let weighted_sum =
                fixed_point::mul(availability, fixed_point::from_string("0.3").unwrap())
                    + fixed_point::mul(latency, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(integrity, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(reputation, fixed_point::from_string("0.2").unwrap())
                    + fixed_point::mul(participation, fixed_point::from_string("0.1").unwrap());

            black_box(weighted_sum);
        });
    });
}

fn bench_u256_conversions(c: &mut Criterion) {
    let test_values: Vec<fixed_point::FixedPoint> = (1..=1000)
        .map(|i| fixed_point::from_string(&format!("{}.{}", i, i % 1000)).unwrap())
        .collect();

    c.bench_function("to_u256_conversion", |bencher| {
        bencher.iter(|| {
            for &val in &test_values {
                black_box(fixed_point::to_u256(black_box(val)));
            }
        });
    });

    let u256_values: Vec<ethnum::U256> = test_values
        .iter()
        .map(|&val| fixed_point::to_u256(val))
        .collect();

    c.bench_function("to_u128_conversion", |bencher| {
        bencher.iter(|| {
            for &val in &u256_values {
                black_box(fixed_point::to_u128(black_box(val)));
            }
        });
    });

    // Benchmark conversion throughput
    c.bench_function("conversion_throughput", |bencher| {
        bencher.iter(|| {
            for &val in &test_values {
                let u256_val = black_box(fixed_point::to_u256(val));
                black_box(fixed_point::to_u128(u256_val));
            }
        });
    });
}

fn bench_memory_usage(c: &mut Criterion) {
    // Test memory allocation patterns
    c.bench_function("large_dataset_allocation", |bencher| {
        bencher.iter(|| {
            let values: Vec<fixed_point::FixedPoint> = (1..=10000)
                .map(|i| fixed_point::from_string(&format!("{}.{}", i, i % 10000)).unwrap())
                .collect();
            black_box(stats::mean(black_box(&values)));
        });
    });

    // Test stack vs heap allocation
    c.bench_function("stack_vs_heap_small", |bencher| {
        bencher.iter(|| {
            // Stack allocation (small dataset)
            let values = [
                fixed_point::from_string("1.0").unwrap(),
                fixed_point::from_string("2.0").unwrap(),
                fixed_point::from_string("3.0").unwrap(),
                fixed_point::from_string("4.0").unwrap(),
                fixed_point::from_string("5.0").unwrap(),
            ];
            black_box(stats::mean(black_box(&values)));
        });
    });

    c.bench_function("stack_vs_heap_large", |bencher| {
        bencher.iter(|| {
            // Heap allocation (large dataset)
            let values: Vec<fixed_point::FixedPoint> = (1..=1000)
                .map(|i| fixed_point::from_string(&format!("{}.{}", i, i % 1000)).unwrap())
                .collect();
            black_box(stats::mean(black_box(&values)));
        });
    });
}

fn bench_real_world_scenarios(c: &mut Criterion) {
    let transactions: Vec<fixed_point::FixedPoint> = (1..=1000)
        .map(|i| fixed_point::from_string(&format!("{}.{}", 20 + i % 50, i % 1000)).unwrap())
        .collect();

    c.bench_function("block_processing_1000_tx", |bencher| {
        bencher.iter(|| {
            let gas_prices = black_box(&transactions);
            let avg_gas_price = stats::mean(gas_prices);
            let gas_volatility = stats::std_deviation(gas_prices);
            let (q1, q2, q3) = stats::quartiles(&mut gas_prices.to_vec());

            let base_reward = fixed_point::from_string("2.0").unwrap();
            let fee_reward = stats::mean(gas_prices);
            let block_reward = base_reward + fee_reward;

            black_box((avg_gas_price, gas_volatility, (q1, q2, q3), block_reward));
        });
    });

    let validator_data: Vec<(
        fixed_point::FixedPoint,
        fixed_point::FixedPoint,
        fixed_point::FixedPoint,
        fixed_point::FixedPoint,
        fixed_point::FixedPoint,
    )> = (1..=100)
        .map(|i| {
            (
                fixed_point::from_string(&format!("0.{}", 80 + i % 20)).unwrap(), // availability
                fixed_point::from_string(&format!("0.{}", 70 + i % 30)).unwrap(), // latency
                fixed_point::from_string(&format!("0.{}", 90 + i % 10)).unwrap(), // integrity
                fixed_point::from_string(&format!("0.{}", 85 + i % 15)).unwrap(), // reputation
                fixed_point::from_string(&format!("0.{}", 75 + i % 25)).unwrap(), // participation
            )
        })
        .collect();

    c.bench_function("pou_validation_100_validators", |bencher| {
        bencher.iter(|| {
            let mut total_score = fixed_point::from_string("0.0").unwrap();
            for (availability, latency, integrity, reputation, participation) in
                black_box(&validator_data)
            {
                let weighted_sum =
                    fixed_point::mul(*availability, fixed_point::from_string("0.3").unwrap())
                        + fixed_point::mul(*latency, fixed_point::from_string("0.2").unwrap())
                        + fixed_point::mul(*integrity, fixed_point::from_string("0.2").unwrap())
                        + fixed_point::mul(*reputation, fixed_point::from_string("0.2").unwrap())
                        + fixed_point::mul(
                            *participation,
                            fixed_point::from_string("0.1").unwrap(),
                        );
                total_score = total_score + weighted_sum;
            }
            black_box(total_score);
        });
    });
}

fn bench_determinism_validation(c: &mut Criterion) {
    // Check che i calcoli siano deterministici
    let test_cases = vec![
        "1.5",
        "0.000000000000000001",
        "999999.999999999999",
        "123.456789",
        "0.1",
    ];

    c.bench_function("deterministic_round_trip", |bencher| {
        bencher.iter(|| {
            for test_case in &test_cases {
                let parsed = black_box(fixed_point::from_string(test_case).unwrap());
                let back_to_string = black_box(fixed_point::to_string(parsed));
                assert_eq!(back_to_string, *test_case);
            }
        });
    });

    let a = fixed_point::from_string("123.456789").unwrap();
    let b = fixed_point::from_string("789.012345").unwrap();

    c.bench_function("deterministic_math_ops", |bencher| {
        bencher.iter(|| {
            let mul_result = black_box(fixed_point::mul(black_box(a), black_box(b)));
            let div_result = black_box(fixed_point::div(black_box(a), black_box(b)));
            let sqrt_result = black_box(fixed_point::sqrt(black_box(a)));

            assert!(mul_result > 0);
            assert!(div_result > 0);
            assert!(sqrt_result > 0);

            black_box((mul_result, div_result, sqrt_result));
        });
    });
}

criterion_group!(
    math_performance,
    bench_math_operations,
    bench_statistics,
    bench_parsing,
    bench_blockchain_operations,
    bench_u256_conversions,
    bench_memory_usage,
    bench_real_world_scenarios,
    bench_determinism_validation
);

criterion_main!(math_performance);
