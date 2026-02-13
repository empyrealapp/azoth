//! Benchmarks for transaction throughput and latency
//!
//! Run with: cargo bench --bench transaction_throughput

use azoth::prelude::*;
use azoth::typed_values::{TypedValue, U256};
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use std::sync::Arc;
use std::thread;

fn bench_non_conflicting_transactions(c: &mut Criterion) {
    let mut group = c.benchmark_group("non_conflicting_transactions");

    for num_threads in [1, 2, 4, 8, 16].iter() {
        group.throughput(Throughput::Elements(*num_threads as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter_batched(
                    || {
                        let temp_dir = tempfile::tempdir().unwrap();
                        let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

                        // Initialize accounts
                        for i in 0..num_threads {
                            let key = format!("account_{}", i);
                            Transaction::new(&db)
                                .keys(vec![key.as_bytes().to_vec()])
                                .execute(|ctx| {
                                    ctx.set(
                                        key.as_bytes(),
                                        &TypedValue::U256(U256::from(1000u64)),
                                    )?;
                                    Ok(())
                                })
                                .unwrap();
                        }
                        (temp_dir, db)
                    },
                    |(_temp_dir, db)| {
                        let handles: Vec<_> = (0..num_threads)
                            .map(|i| {
                                let db = Arc::clone(&db);
                                thread::spawn(move || {
                                    let key = format!("account_{}", i);
                                    Transaction::new(&db)
                                        .keys(vec![key.as_bytes().to_vec()])
                                        .execute(|ctx| {
                                            ctx.set(
                                                key.as_bytes(),
                                                &TypedValue::U256(U256::from(900u64)),
                                            )?;
                                            ctx.log_bytes(b"withdraw")?;
                                            Ok(())
                                        })
                                        .unwrap();
                                })
                            })
                            .collect();

                        for handle in handles {
                            handle.join().unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_single_transaction_latency(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();

    // Initialize
    Transaction::new(&db)
        .keys(vec![b"key".to_vec()])
        .execute(|ctx| {
            ctx.set(b"key", &TypedValue::U256(U256::from(1000u64)))?;
            Ok(())
        })
        .unwrap();

    c.benchmark_group("single_transaction_latency")
        .bench_function("with_preflight", |b| {
            b.iter(|| {
                Transaction::new(&db)
                    .keys(vec![b"key".to_vec()])
                    .validate(|ctx| {
                        let _val = ctx.get(b"key")?;
                        Ok(())
                    })
                    .execute(|ctx| {
                        ctx.set(b"key", &TypedValue::U256(U256::from(999u64)))?;
                        ctx.log_bytes(b"update")?;
                        Ok(())
                    })
                    .unwrap();
            });
        });
}

fn bench_batch_sizes(c: &mut Criterion) {
    let mut group = c.benchmark_group("batch_event_generation");

    for batch_size in [10, 100, 1000].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &batch_size| {
                let temp_dir = tempfile::tempdir().unwrap();
                let db = AzothDb::open(temp_dir.path()).unwrap();

                b.iter(|| {
                    for _ in 0..batch_size {
                        Transaction::new(&db)
                            .execute(|ctx| {
                                ctx.log_bytes(black_box(b"test_event"))?;
                                Ok(())
                            })
                            .unwrap();
                    }
                });
            },
        );
    }
    group.finish();
}

fn bench_read_heavy_workload(c: &mut Criterion) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();

    // Initialize 100 keys
    for i in 0..100 {
        let key = format!("key_{}", i);
        Transaction::new(&db)
            .keys(vec![key.as_bytes().to_vec()])
            .execute(|ctx| {
                ctx.set(key.as_bytes(), &TypedValue::U64(i))?;
                Ok(())
            })
            .unwrap();
    }

    c.benchmark_group("read_heavy_workload")
        .bench_function("read_only", |b| {
            b.iter(|| {
                for i in 0..100 {
                    let key = format!("key_{}", i);
                    Transaction::new(&db)
                        .keys(vec![key.as_bytes().to_vec()])
                        .validate(|ctx| {
                            let _val = ctx.get(key.as_bytes())?;
                            Ok(())
                        })
                        .execute(|_ctx| Ok(()))
                        .unwrap();
                }
            });
        });
}

/// Benchmarks comparing conflicting vs non-conflicting key throughput.
/// Non-conflicting keys (different stripes) enable parallel preflight; conflicting (same stripe) serialize.
fn bench_parallelism_conflicting_vs_non_conflicting(c: &mut Criterion) {
    let mut group = c.benchmark_group("parallelism");
    group.sample_size(20);

    for num_threads in [4, 8, 16].iter() {
        // Non-conflicting: each thread uses unique keys
        group.bench_with_input(
            BenchmarkId::new("non_conflicting_with_preflight", num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter_batched(
                    || {
                        let temp_dir = tempfile::tempdir().unwrap();
                        let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());
                        for i in 0..num_threads {
                            let key = format!("nc_key_{}", i);
                            Transaction::new(&db)
                                .keys(vec![key.as_bytes().to_vec()])
                                .execute(|ctx| {
                                    ctx.set(
                                        key.as_bytes(),
                                        &TypedValue::U64(1000),
                                    )?;
                                    Ok(())
                                })
                                .unwrap();
                        }
                        (temp_dir, db)
                    },
                    |(_temp_dir, db)| {
                        let handles: Vec<_> = (0..num_threads)
                            .map(|i| {
                                let db = Arc::clone(&db);
                                thread::spawn(move || {
                                    let key = format!("nc_key_{}", i);
                                    for _ in 0..50 {
                                        Transaction::new(&db)
                                            .keys(vec![key.as_bytes().to_vec()])
                                            .validate(|ctx| {
                                                let _ = ctx.get(key.as_bytes())?;
                                                Ok(())
                                            })
                                            .execute(|ctx| {
                                                let v = match ctx.get(key.as_bytes())? {
                                                    TypedValue::U64(x) => x,
                                                    _ => 0,
                                                };
                                                ctx.set(
                                                    key.as_bytes(),
                                                    &TypedValue::U64(v.wrapping_add(1)),
                                                )?;
                                                Ok(())
                                            })
                                            .unwrap();
                                    }
                                })
                            })
                            .collect();
                        for handle in handles {
                            handle.join().unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );

        // Conflicting: all threads compete for same key
        group.bench_with_input(
            BenchmarkId::new("conflicting_with_preflight", num_threads),
            num_threads,
            |b, &num_threads| {
                b.iter_batched(
                    || {
                        let temp_dir = tempfile::tempdir().unwrap();
                        let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());
                        Transaction::new(&db)
                            .keys(vec![b"hot_key".to_vec()])
                            .execute(|ctx| {
                                ctx.set(b"hot_key", &TypedValue::U64(1_000_000))?;
                                Ok(())
                            })
                            .unwrap();
                        (temp_dir, db)
                    },
                    |(_temp_dir, db)| {
                        let handles: Vec<_> = (0..num_threads)
                            .map(|_| {
                                let db = Arc::clone(&db);
                                thread::spawn(move || {
                                    for _ in 0..50 {
                                        Transaction::new(&db)
                                            .keys(vec![b"hot_key".to_vec()])
                                            .validate(|ctx| {
                                                let v = match ctx.get(b"hot_key")? {
                                                    TypedValue::U64(x) => x,
                                                    _ => 0,
                                                };
                                                if v == 0 {
                                                    return Err(AzothError::PreflightFailed(
                                                        "depleted".into(),
                                                    ));
                                                }
                                                Ok(())
                                            })
                                            .execute(|ctx| {
                                                let v = match ctx.get(b"hot_key")? {
                                                    TypedValue::U64(x) => x,
                                                    _ => 0,
                                                };
                                                ctx.set(
                                                    b"hot_key",
                                                    &TypedValue::U64(v - 1),
                                                )?;
                                                Ok(())
                                            })
                                            .ok();
                                    }
                                })
                            })
                            .collect();
                        for handle in handles {
                            handle.join().unwrap();
                        }
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_non_conflicting_transactions,
    bench_single_transaction_latency,
    bench_batch_sizes,
    bench_read_heavy_workload,
    bench_parallelism_conflicting_vs_non_conflicting
);
criterion_main!(benches);
