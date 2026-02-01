//! Stress tests for concurrent operations and high load scenarios

use azoth::prelude::*;
use azoth::typed_values::{TypedValue, U256};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_concurrent_non_conflicting_transactions() {
    // Test parallel preflight with non-conflicting keys
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Initialize 100 accounts
    for i in 0..100 {
        let key = format!("account_{}", i);
        Transaction::new(&db)
            .execute(|ctx| {
                ctx.set(key.as_bytes(), &TypedValue::U256(U256::from(1000u64)))?;
                Ok(())
            })
            .unwrap();
    }

    // Run 100 concurrent transactions on different accounts
    let success_count = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    let handles: Vec<_> = (0..100)
        .map(|i| {
            let db = Arc::clone(&db);
            let success_count = Arc::clone(&success_count);
            thread::spawn(move || {
                let key = format!("account_{}", i);
                let result = Transaction::new(&db)
                    .write_keys(vec![key.as_bytes().to_vec()])
                    .validate(|ctx| {
                        let balance = ctx.get(key.as_bytes())?.as_u256()?;
                        if balance.to_bytes() < U256::from(100u64).to_bytes() {
                            return Err(AzothError::PreflightFailed("Insufficient".into()));
                        }
                        Ok(())
                    })
                    .execute(|ctx| {
                        ctx.set(key.as_bytes(), &TypedValue::U256(U256::from(900u64)))?;
                        ctx.log(
                            "withdraw",
                            &serde_json::json!({
                                "account": i,
                                "amount": 100
                            }),
                        )?;
                        Ok(())
                    });

                if result.is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let count = success_count.load(Ordering::SeqCst);

    println!("✅ Concurrent non-conflicting test:");
    println!("   Transactions: {}/100", count);
    println!("   Time: {:?}", elapsed);
    println!(
        "   Throughput: {:.0} txn/sec",
        100.0 / elapsed.as_secs_f64()
    );

    assert_eq!(count, 100, "All transactions should succeed");
}

#[test]
fn test_high_contention_same_key() {
    // Test serialized preflight with conflicting keys
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Initialize single account
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"shared", &TypedValue::U256(U256::from(1000u64)))?;
            Ok(())
        })
        .unwrap();

    let success_count = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    // 50 threads all trying to update the same key
    let handles: Vec<_> = (0..50)
        .map(|i| {
            let db = Arc::clone(&db);
            let success_count = Arc::clone(&success_count);
            thread::spawn(move || {
                let result = Transaction::new(&db)
                    .write_keys(vec![b"shared".to_vec()])
                    .validate(|ctx| {
                        let balance = ctx.get(b"shared")?.as_u256()?;
                        if balance.to_bytes() < U256::from(100u64).to_bytes() {
                            return Err(AzothError::PreflightFailed("Insufficient".into()));
                        }
                        Ok(())
                    })
                    .execute(|ctx| {
                        let _current = ctx.get(b"shared")?.as_u256()?;
                        // Simplified decrement
                        ctx.set(b"shared", &TypedValue::U256(U256::from(990u64)))?;
                        ctx.log(
                            "withdraw",
                            &serde_json::json!({
                                "attempt": i
                            }),
                        )?;
                        Ok(())
                    });

                if result.is_ok() {
                    success_count.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let count = success_count.load(Ordering::SeqCst);

    println!("✅ High contention test:");
    println!("   Successful: {}/50 (validation failures expected)", count);
    println!("   Time: {:?}", elapsed);
    println!(
        "   Throughput: {:.0} txn/sec",
        count as f64 / elapsed.as_secs_f64()
    );

    // At least one should succeed
    assert!(count > 0, "At least one transaction should succeed");
}

#[test]
fn test_large_batch_event_processing() {
    // Test processing large batches of events
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();

    println!("✅ Large batch test:");
    println!("   Generating 10,000 events...");

    let start = Instant::now();

    // Generate 10,000 events
    for i in 0..10_000 {
        let result = Transaction::new(&db).execute(|ctx| {
            ctx.log(
                "test",
                &serde_json::json!({
                    "id": i,
                    "value": i * 2
                }),
            )?;
            Ok(())
        });

        if i % 1000 == 0 {
            println!("   Generated {} events...", i);
        }

        result.unwrap();
    }

    let generation_time = start.elapsed();
    println!("   Generation time: {:?}", generation_time);
    println!(
        "   Generation rate: {:.0} events/sec",
        10_000.0 / generation_time.as_secs_f64()
    );

    // Verify events were created
    let meta = db.canonical().meta().unwrap();
    assert_eq!(meta.next_event_id, 10_000);

    println!("   ✓ All 10,000 events generated successfully");
}

#[test]
fn test_mixed_read_write_workload() {
    // Test realistic mixed workload (reads + writes)
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Initialize data
    for i in 0..10 {
        let key = format!("key_{}", i);
        Transaction::new(&db)
            .execute(|ctx| {
                ctx.set(key.as_bytes(), &TypedValue::U256(U256::from(100u64)))?;
                Ok(())
            })
            .unwrap();
    }

    let read_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();

    // 70% reads, 30% writes
    let handles: Vec<_> = (0..100)
        .map(|i| {
            let db = Arc::clone(&db);
            let read_count = Arc::clone(&read_count);
            let write_count = Arc::clone(&write_count);

            thread::spawn(move || {
                let key = format!("key_{}", i % 10);

                if i % 10 < 7 {
                    // Read operation
                    let result = Transaction::new(&db)
                        .read_keys(vec![key.as_bytes().to_vec()])
                        .validate(|ctx| {
                            let _balance = ctx.get(key.as_bytes())?;
                            Ok(())
                        })
                        .execute(|_ctx| Ok(()));

                    if result.is_ok() {
                        read_count.fetch_add(1, Ordering::SeqCst);
                    }
                } else {
                    // Write operation
                    let result = Transaction::new(&db)
                        .write_keys(vec![key.as_bytes().to_vec()])
                        .execute(|ctx| {
                            ctx.set(key.as_bytes(), &TypedValue::U256(U256::from(99u64)))?;
                            Ok(())
                        });

                    if result.is_ok() {
                        write_count.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let reads = read_count.load(Ordering::SeqCst);
    let writes = write_count.load(Ordering::SeqCst);

    println!("✅ Mixed workload test:");
    println!("   Reads: {}", reads);
    println!("   Writes: {}", writes);
    println!("   Time: {:?}", elapsed);
    println!(
        "   Total throughput: {:.0} ops/sec",
        (reads + writes) as f64 / elapsed.as_secs_f64()
    );
}

#[test]
fn test_lock_fairness() {
    // Test that locks are acquired fairly (no starvation)
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Initialize
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"counter", &TypedValue::U64(0))?;
            Ok(())
        })
        .unwrap();

    let success_counts = Arc::new(parking_lot::Mutex::new(vec![0usize; 10]));
    let start = Instant::now();

    // 10 threads each trying 10 times
    let handles: Vec<_> = (0..10)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let success_counts = Arc::clone(&success_counts);

            thread::spawn(move || {
                for attempt in 0..10 {
                    let result = Transaction::new(&db)
                        .write_keys(vec![b"counter".to_vec()])
                        .execute(|ctx| {
                            // Increment counter
                            let current = match ctx.get_opt(b"counter")? {
                                Some(val) => match val {
                                    TypedValue::U64(v) => v,
                                    _ => 0,
                                },
                                None => 0,
                            };
                            ctx.set(b"counter", &TypedValue::U64(current + 1))?;
                            ctx.log(
                                "increment",
                                &serde_json::json!({
                                    "thread": thread_id,
                                    "attempt": attempt
                                }),
                            )?;
                            Ok(())
                        });

                    if result.is_ok() {
                        let mut counts = success_counts.lock();
                        counts[thread_id] += 1;
                    }

                    // Small delay to allow other threads to compete
                    thread::sleep(Duration::from_micros(100));
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let counts = success_counts.lock().clone();

    println!("✅ Lock fairness test:");
    println!("   Time: {:?}", elapsed);
    for (i, count) in counts.iter().enumerate() {
        println!("   Thread {}: {} successes", i, count);
    }

    // Check that all threads got at least some success (no starvation)
    let min_success = *counts.iter().min().unwrap();
    let max_success = *counts.iter().max().unwrap();

    println!("   Min successes: {}", min_success);
    println!("   Max successes: {}", max_success);

    // Allow some variance but no starvation
    assert!(
        min_success >= 5,
        "No thread should be starved (min {} < 5)",
        min_success
    );
}
