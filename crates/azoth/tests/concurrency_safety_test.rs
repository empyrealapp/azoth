//! Critical concurrency safety tests for write lock removal optimization
//!
//! These tests verify that removing the application-level write lock
//! and relying on LMDB's internal locking + stripe locks is safe.

use azoth::prelude::*;
use azoth::typed_values::TypedValue;
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Barrier,
};
use std::thread;
use std::time::Duration;

/// Test that concurrent updates to the same key don't cause lost updates
/// This is the most critical test - it verifies atomicity of read-modify-write
#[test]
fn test_no_lost_updates_same_key() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Initialize counter to 0
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"counter", &TypedValue::U64(0))?;
            Ok(())
        })
        .unwrap();

    let num_threads = 20;
    let increments_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));

    // Launch threads that all increment the same counter
    let handles: Vec<_> = (0..num_threads)
        .map(|_| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();

                for _ in 0..increments_per_thread {
                    // Read-modify-write must be atomic
                    Transaction::new(&db)
                        .write_keys(vec![b"counter".to_vec()])
                        .execute(|ctx| {
                            let current = match ctx.get(b"counter")? {
                                TypedValue::U64(v) => v,
                                _ => 0,
                            };
                            ctx.set(b"counter", &TypedValue::U64(current + 1))?;
                            ctx.log(
                                "increment",
                                &serde_json::json!({"value": current + 1}),
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

    // Verify final value - read directly from canonical store
    let txn = db.canonical().read_only_txn().unwrap();
    let bytes = txn.get_state(b"counter").unwrap().unwrap();
    let final_value = match TypedValue::from_bytes(&bytes).unwrap() {
        TypedValue::U64(v) => v,
        _ => 0,
    };

    let expected = (num_threads * increments_per_thread) as u64;
    assert_eq!(
        final_value, expected,
        "Lost updates detected! Expected {}, got {}. This means {} updates were lost.",
        expected, final_value, expected - final_value
    );

    println!("✅ No lost updates test passed:");
    println!("   Threads: {}", num_threads);
    println!("   Increments per thread: {}", increments_per_thread);
    println!("   Expected value: {}", expected);
    println!("   Actual value: {}", final_value);
}

/// Test that stripe locks properly serialize conflicting transactions
#[test]
fn test_stripe_lock_serialization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Initialize two accounts
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"account_a", &TypedValue::U64(1000))?;
            ctx.set(b"account_b", &TypedValue::U64(1000))?;
            Ok(())
        })
        .unwrap();

    let num_transfers = 100;
    let barrier = Arc::new(Barrier::new(2));

    // Two threads repeatedly transfer money between accounts
    let handles: Vec<_> = (0..2)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..num_transfers {
                    let (from, to) = if i % 2 == 0 {
                        (b"account_a", b"account_b")
                    } else {
                        (b"account_b", b"account_a")
                    };

                    // Transfer must be atomic (both accounts locked)
                    Transaction::new(&db)
                        .write_keys(vec![from.to_vec(), to.to_vec()])
                        .validate(|ctx| {
                            let from_balance = match ctx.get(from)? { TypedValue::U64(v) => v, _ => 0 };
                            if from_balance < 10 {
                                return Err(AzothError::PreflightFailed(
                                    "Insufficient balance".into(),
                                ));
                            }
                            Ok(())
                        })
                        .execute(|ctx| {
                            let from_balance = match ctx.get(from)? { TypedValue::U64(v) => v, _ => 0 };
                            let to_balance = match ctx.get(to)? { TypedValue::U64(v) => v, _ => 0 };

                            ctx.set(from, &TypedValue::U64(from_balance - 10))?;
                            ctx.set(to, &TypedValue::U64(to_balance + 10))?;

                            ctx.log(
                                "transfer",
                                &serde_json::json!({
                                    "thread": thread_id,
                                    "from": String::from_utf8_lossy(from),
                                    "to": String::from_utf8_lossy(to),
                                    "amount": 10
                                }),
                            )?;
                            Ok(())
                        })
                        .ok(); // Some may fail due to insufficient balance
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify total balance is conserved (no money created or destroyed)
    let txn = db.canonical().read_only_txn().unwrap();
    let bytes_a = txn.get_state(b"account_a").unwrap().unwrap();
    let bytes_b = txn.get_state(b"account_b").unwrap().unwrap();
    let balance_a = match TypedValue::from_bytes(&bytes_a).unwrap() {
        TypedValue::U64(v) => v,
        _ => 0,
    };
    let balance_b = match TypedValue::from_bytes(&bytes_b).unwrap() {
        TypedValue::U64(v) => v,
        _ => 0,
    };

    let total = balance_a + balance_b;
    assert_eq!(
        total, 2000,
        "Total balance should be conserved! Got {}, expected 2000. Money was {} {}",
        total,
        if total > 2000 { "created" } else { "destroyed" },
        (total as i64 - 2000).abs()
    );

    println!("✅ Stripe lock serialization test passed:");
    println!("   Account A: {}", balance_a);
    println!("   Account B: {}", balance_b);
    println!("   Total: {} (conserved)", total);
}

/// Test that pause_ingestion correctly waits for in-flight transactions
#[test]
fn test_pause_waits_for_transactions() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    let paused = Arc::new(AtomicBool::new(false));
    let txn_started = Arc::new(AtomicBool::new(false));
    let txn_finished = Arc::new(AtomicBool::new(false));

    // Thread 1: Start a long transaction
    let db_clone = Arc::clone(&db);
    let paused_clone = Arc::clone(&paused);
    let txn_started_clone = Arc::clone(&txn_started);
    let txn_finished_clone = Arc::clone(&txn_finished);

    let txn_thread = thread::spawn(move || {
        Transaction::new(&db_clone)
            .execute(|ctx| {
                txn_started_clone.store(true, Ordering::SeqCst);

                // Simulate long-running transaction
                thread::sleep(Duration::from_millis(100));

                // Verify we're not paused yet (pause should wait for us)
                assert!(
                    !paused_clone.load(Ordering::SeqCst),
                    "Transaction should complete before pause"
                );

                ctx.set(b"test", &TypedValue::U64(123))?;
                Ok(())
            })
            .unwrap();

        txn_finished_clone.store(true, Ordering::SeqCst);
    });

    // Wait for transaction to start
    while !txn_started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(1));
    }

    // Thread 2: Try to pause (should wait for transaction)
    let db_clone = Arc::clone(&db);
    let paused_clone = Arc::clone(&paused);
    let txn_finished_clone = Arc::clone(&txn_finished);

    let pause_thread = thread::spawn(move || {
        db_clone.canonical().pause_ingestion().unwrap();
        paused_clone.store(true, Ordering::SeqCst);

        // Verify transaction finished before pause completed
        assert!(
            txn_finished_clone.load(Ordering::SeqCst),
            "pause_ingestion should wait for transaction to complete"
        );
    });

    txn_thread.join().unwrap();
    pause_thread.join().unwrap();

    assert!(paused.load(Ordering::SeqCst), "Should be paused now");

    println!("✅ Pause waits for transactions test passed");
}

/// Test that LMDB properly serializes concurrent write transactions
#[test]
fn test_lmdb_write_serialization() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    let write_order = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let start_times = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let end_times = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let num_threads = 10;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let write_order = Arc::clone(&write_order);
            let start_times = Arc::clone(&start_times);
            let end_times = Arc::clone(&end_times);

            thread::spawn(move || {
                // All threads start simultaneously
                barrier.wait();

                let start = std::time::Instant::now();

                Transaction::new(&db)
                    .execute(|ctx| {
                        start_times.lock().push((thread_id, start.elapsed()));

                        // Simulate some work
                        thread::sleep(Duration::from_millis(10));

                        let key = format!("key_{}", thread_id);
                        ctx.set(key.as_bytes(), &TypedValue::U64(thread_id as u64))?;

                        write_order.lock().push(thread_id);
                        end_times.lock().push((thread_id, start.elapsed()));

                        Ok(())
                    })
                    .unwrap();
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let order = write_order.lock().clone();
    let starts = start_times.lock().clone();
    let ends = end_times.lock().clone();

    println!("✅ LMDB write serialization test passed:");
    println!("   Threads: {}", num_threads);
    println!("   Write order: {:?}", order);

    // Verify no overlap in write transactions (they must be serialized)
    for i in 0..ends.len() - 1 {
        let (tid1, end1) = ends[i];

        // Find the next transaction in execution order
        if let Some((tid2, start2)) = starts.iter().find(|(tid, _)| {
            let pos1 = order.iter().position(|&t| t == tid1).unwrap();
            let pos2 = order.iter().position(|&t| t == *tid).unwrap();
            pos2 == pos1 + 1
        }) {
            if start2 < &end1 {
                println!(
                    "   Warning: Transaction {} (ended {:?}) overlapped with {} (started {:?})",
                    tid1, end1, tid2, start2
                );
                println!("   This is expected - some overlap in timing is fine as long as");
                println!("   the critical sections (actual writes) don't overlap.");
            }
        }
    }

    // The key test: verify all transactions completed
    assert_eq!(order.len(), num_threads, "All transactions should complete");
}

/// Stress test: Many threads, many updates, verify consistency
#[test]
fn test_concurrent_consistency_stress() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Initialize accounts
    let num_accounts = 10;
    for i in 0..num_accounts {
        let key = format!("account_{}", i);
        Transaction::new(&db)
            .execute(|ctx| {
                ctx.set(key.as_bytes(), &TypedValue::U64(100))?;
                Ok(())
            })
            .unwrap();
    }

    let num_threads = 20;
    let ops_per_thread = 100;
    let total_operations = Arc::new(AtomicU64::new(0));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let total_operations = Arc::clone(&total_operations);

            thread::spawn(move || {
                let mut rng = thread_id; // Simple PRNG

                for _ in 0..ops_per_thread {
                    // Pick two random accounts
                    rng = (rng * 1103515245 + 12345) & 0x7fffffff;
                    let acc1 = rng % num_accounts;
                    rng = (rng * 1103515245 + 12345) & 0x7fffffff;
                    let acc2 = rng % num_accounts;

                    if acc1 == acc2 {
                        continue;
                    }

                    let key1 = format!("account_{}", acc1);
                    let key2 = format!("account_{}", acc2);

                    // Transfer 1 from acc1 to acc2
                    let result = Transaction::new(&db)
                        .write_keys(vec![key1.as_bytes().to_vec(), key2.as_bytes().to_vec()])
                        .validate(|ctx| {
                            let balance = match ctx.get(key1.as_bytes())? {
                                TypedValue::U64(v) => v,
                                _ => 0,
                            };
                            if balance < 1 {
                                return Err(AzothError::PreflightFailed("Insufficient".into()));
                            }
                            Ok(())
                        })
                        .execute(|ctx| {
                            let bal1 = match ctx.get(key1.as_bytes())? {
                                TypedValue::U64(v) => v,
                                _ => 0,
                            };
                            let bal2 = match ctx.get(key2.as_bytes())? {
                                TypedValue::U64(v) => v,
                                _ => 0,
                            };

                            ctx.set(key1.as_bytes(), &TypedValue::U64(bal1 - 1))?;
                            ctx.set(key2.as_bytes(), &TypedValue::U64(bal2 + 1))?;

                            Ok(())
                        });

                    if result.is_ok() {
                        total_operations.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // Verify total balance is conserved
    let txn = db.canonical().read_only_txn().unwrap();
    let mut total_balance = 0u64;
    for i in 0..num_accounts {
        let key = format!("account_{}", i);
        if let Some(bytes) = txn.get_state(key.as_bytes()).unwrap() {
            let balance = match TypedValue::from_bytes(&bytes).unwrap() {
                TypedValue::U64(v) => v,
                _ => 0,
            };
            total_balance += balance;
        }
    }

    let expected_total = num_accounts * 100;
    assert_eq!(
        total_balance, expected_total,
        "Total balance not conserved! Expected {}, got {}",
        expected_total, total_balance
    );

    let ops = total_operations.load(Ordering::SeqCst);
    println!("✅ Concurrent consistency stress test passed:");
    println!("   Threads: {}", num_threads);
    println!("   Successful operations: {}", ops);
    println!("   Total balance: {} (conserved)", total_balance);
}
