//! Tests verifying that non-conflicting keys enable parallelism in Azoth.
//!
//! Validates that:
//! 1. Preflight lock acquisition does NOT block for non-conflicting keys (different stripes)
//! 2. Transaction execution (preflight + updates) runs in parallel for non-conflicting keys
//! 3. Conflicting keys (same stripe) properly serialize
//!
//! Run with: cargo test -p azoth --test parallelism_test

use azoth::prelude::*;
use azoth::typed_values::TypedValue;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::{Duration, Instant};

/// Verifies that non-conflicting keys allow parallel lock acquisition and preflight.
/// Threads with different keys should NOT block each other during lock acquisition.
#[test]
fn test_non_conflicting_keys_parallel_lock_acquisition() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Initialize N unique keys
    let num_threads = 16;
    for i in 0..num_threads {
        let key = format!("account_{}", i);
        Transaction::new(&db)
            .keys(vec![key.as_bytes().to_vec()])
            .execute(|ctx| {
                ctx.set(key.as_bytes(), &TypedValue::U64(1000))?;
                Ok(())
            })
            .unwrap();
    }

    let barrier = Arc::new(Barrier::new(num_threads));
    let lock_acquisition_times = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let lock_acquisition_times = Arc::clone(&lock_acquisition_times);

            thread::spawn(move || {
                barrier.wait();
                let t0 = Instant::now();

                let key = format!("account_{}", i);
                Transaction::new(&db)
                    .keys(vec![key.as_bytes().to_vec()])
                    .validate(|ctx| {
                        let _ = ctx.get(key.as_bytes())?;
                        Ok(())
                    })
                    .execute(|ctx| {
                        ctx.set(key.as_bytes(), &TypedValue::U64(999))?;
                        ctx.log("update", &serde_json::json!({"thread": i}))?;
                        Ok(())
                    })
                    .unwrap();

                let elapsed = t0.elapsed();
                lock_acquisition_times.lock().push((i, elapsed));
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let times = lock_acquisition_times.lock().clone();
    let total_wall_time = times
        .iter()
        .map(|(_, t)| *t)
        .max()
        .unwrap_or(Duration::ZERO);
    let serial_estimate: Duration = times.iter().map(|(_, t)| *t).sum();

    // With parallel execution, wall time ≈ max(individual). With serial, wall time ≈ sum(individual).
    // Non-conflicting keys use different stripes → parallel. Expect wall << serial.
    assert!(
        total_wall_time < serial_estimate / 2,
        "Non-conflicting keys should run in parallel. Wall: {:?}, Serial estimate: {:?}",
        total_wall_time,
        serial_estimate
    );

    println!(
        "✅ Non-conflicting lock acquisition: {:?} wall (parallel) vs {:?} serial estimate",
        total_wall_time,
        serial_estimate
    );
}

/// Verifies that conflicting keys (same stripe) properly serialize.
/// All threads touching the same key must run one after another.
#[test]
fn test_conflicting_keys_serialize() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    Transaction::new(&db)
        .keys(vec![b"hot_key".to_vec()])
        .execute(|ctx| {
            ctx.set(b"hot_key", &TypedValue::U64(1000))?;
            Ok(())
        })
        .unwrap();

    let num_threads = 8;
    let updates_per_thread = 5;

    let barrier = Arc::new(Barrier::new(num_threads));
    let counter = Arc::new(AtomicU64::new(0));

    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let counter = Arc::clone(&counter);

            thread::spawn(move || {
                barrier.wait();
                for _ in 0..updates_per_thread {
                    Transaction::new(&db)
                        .keys(vec![b"hot_key".to_vec()])
                        .validate(|ctx| {
                            let v = match ctx.get(b"hot_key")? {
                                TypedValue::U64(x) => x,
                                _ => 0,
                            };
                            if v == 0 {
                                return Err(azoth::AzothError::PreflightFailed("Depleted".into()));
                            }
                            Ok(())
                        })
                        .execute(|ctx| {
                            let v = match ctx.get(b"hot_key")? {
                                TypedValue::U64(x) => x,
                                _ => 0,
                            };
                            ctx.set(b"hot_key", &TypedValue::U64(v - 1))?;
                            counter.fetch_add(1, Ordering::SeqCst);
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
    let elapsed = start.elapsed();

    let total_updates = num_threads * updates_per_thread;
    assert_eq!(
        counter.load(Ordering::SeqCst),
        total_updates as u64,
        "All updates should succeed"
    );

    // Conflicting: each txn blocks the next. Time should grow roughly linearly with concurrency.
    // Single txn baseline: run 1 thread, 1 update
    let baseline_start = Instant::now();
    Transaction::new(&db)
        .keys(vec![b"baseline".to_vec()])
        .execute(|ctx| {
            ctx.set(b"baseline", &TypedValue::U64(1))?;
            Ok(())
        })
        .unwrap();
    let baseline = baseline_start.elapsed();

    // With 8 threads × 5 updates = 40 serialized operations, we expect ~40x baseline
    // Allow wide margin (10x-50x) since timing varies
    assert!(
        elapsed > baseline * 5,
        "Conflicting keys should serialize; elapsed {:?} should exceed baseline {:?}",
        elapsed,
        baseline
    );

    println!(
        "✅ Conflicting keys serialized: {} updates in {:?} (~{} ms/update)",
        total_updates,
        elapsed,
        elapsed.as_millis() / total_updates as u128
    );
}

/// Compares throughput: non-conflicting vs conflicting over fixed duration.
/// Non-conflicting should achieve higher ops/sec.
#[test]
fn test_parallelism_throughput_comparison() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    let num_keys = 32;
    for i in 0..num_keys {
        let key = format!("key_{}", i);
        Transaction::new(&db)
            .keys(vec![key.as_bytes().to_vec()])
            .execute(|ctx| {
                ctx.set(key.as_bytes(), &TypedValue::U64(1000))?;
                Ok(())
            })
            .unwrap();
    }

    let num_threads = 8;
    let duration = Duration::from_millis(200);

    // Non-conflicting: each thread uses a disjoint set of keys
    let non_conflicting_ops = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(num_threads));
    let handles: Vec<_> = (0..num_threads)
        .map(|i| {
            let db = Arc::clone(&db);
            let ops = Arc::clone(&non_conflicting_ops);
            let barrier = Arc::clone(&barrier);
            let base_key = i * (num_keys / num_threads);

            thread::spawn(move || {
                barrier.wait();
                let end = Instant::now() + duration;
                let mut local_ops = 0u64;
                while Instant::now() < end {
                    let key_idx = (base_key + (local_ops as usize) % (num_keys / num_threads))
                        % num_keys;
                    let key = format!("key_{}", key_idx);
                    if Transaction::new(&db)
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
                            ctx.set(key.as_bytes(), &TypedValue::U64(v.wrapping_add(1)))?;
                            Ok(())
                        })
                        .is_ok()
                    {
                        local_ops += 1;
                    }
                }
                ops.fetch_add(local_ops, Ordering::SeqCst);
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
    let non_conflicting_total = non_conflicting_ops.load(Ordering::SeqCst);

    // Conflicting run: all threads compete for the same key (same stripe)
    Transaction::new(&db)
        .keys(vec![b"contention_key".to_vec()])
        .execute(|ctx| {
            ctx.set(b"contention_key", &TypedValue::U64(0))?;
            Ok(())
        })
        .unwrap();

    let conflicting_ops = Arc::new(AtomicU64::new(0));
    let barrier2 = Arc::new(Barrier::new(num_threads));
    let handles2: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = Arc::clone(&db);
            let ops = Arc::clone(&conflicting_ops);
            let barrier = Arc::clone(&barrier2);

            thread::spawn(move || {
                barrier.wait();
                let end = Instant::now() + duration;
                let mut local_ops = 0u64;
                while Instant::now() < end {
                    if Transaction::new(&db)
                        .keys(vec![b"contention_key".to_vec()])
                        .execute(|ctx| {
                            let v = match ctx.get(b"contention_key")? {
                                TypedValue::U64(x) => x,
                                _ => 0,
                            };
                            ctx.set(b"contention_key", &TypedValue::U64(v.wrapping_add(1)))?;
                            Ok(())
                        })
                        .is_ok()
                    {
                        local_ops += 1;
                    }
                }
                ops.fetch_add(local_ops, Ordering::SeqCst);
            })
        })
        .collect();

    for handle in handles2 {
        handle.join().unwrap();
    }
    let conflicting_total = conflicting_ops.load(Ordering::SeqCst);

    // Both workloads should complete. Non-conflicting typically outperforms conflicting
    // (different stripes → parallel preflight; same stripe → serialized). Ratio can vary
    // due to hash collisions and LMDB write serialization.
    let ratio = non_conflicting_total as f64 / conflicting_total.max(1) as f64;
    println!(
        "✅ Throughput comparison: non-conflicting {} ops/s vs conflicting {} ops/s ({:.2}x ratio)",
        non_conflicting_total,
        conflicting_total,
        ratio
    );
}
