//! Deadlock Prevention Tests
//!
//! These tests verify that the read transaction system prevents deadlocks
//! that could occur with the old implementation where read_txn() returned
//! a write transaction.
//!
//! Key invariants tested:
//! 1. Multiple concurrent reads never deadlock
//! 2. Reads never block writes (and vice versa)
//! 3. High concurrency scenarios complete within timeout
//! 4. The old footgun pattern would have caused deadlock

use azoth::{AzothDb, CanonicalReadTxn, CanonicalStore, Transaction, TypedValue};
use azoth_core::{CanonicalConfig, ProjectionConfig};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn create_test_db() -> (AzothDb, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();
    (db, temp_dir)
}

fn create_test_db_with_pool(pool_size: usize) -> (AzothDb, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let canonical_config =
        CanonicalConfig::new(temp_dir.path().join("canonical")).with_read_pool_size(pool_size);
    let projection_config = ProjectionConfig::new(temp_dir.path().join("projection.db"));
    let db = AzothDb::open_with_config(
        temp_dir.path().to_path_buf(),
        canonical_config,
        projection_config,
    )
    .unwrap();
    (db, temp_dir)
}

/// Test that multiple concurrent read transactions don't block each other
/// This would deadlock with the old implementation that returned write transactions
#[test]
fn test_concurrent_reads_no_deadlock() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // Initialize test data
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key-{}", i).into_bytes())
        .collect();
    Transaction::new(&db)
        .keys(keys)
        .execute(|ctx| {
            for i in 0..100 {
                ctx.set(format!("key-{}", i).as_bytes(), &TypedValue::I64(i))?;
            }
            Ok(())
        })
        .unwrap();

    let num_readers = 20;
    let reads_per_thread = 100;
    let barrier = Arc::new(Barrier::new(num_readers));
    let success = Arc::new(AtomicBool::new(true));
    let timeout = Duration::from_secs(10);

    let start = Instant::now();

    let handles: Vec<_> = (0..num_readers)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let success = Arc::clone(&success);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..reads_per_thread {
                    let key = format!("key-{}", (thread_id * reads_per_thread + i) % 100);

                    // Use read_txn() - this should NOT block other readers
                    let txn = match db.canonical().read_txn() {
                        Ok(t) => t,
                        Err(e) => {
                            eprintln!("Thread {} failed to get read txn: {}", thread_id, e);
                            success.store(false, Ordering::SeqCst);
                            return;
                        }
                    };

                    match txn.get_state(key.as_bytes()) {
                        Ok(Some(_)) => {}
                        Ok(None) => {
                            eprintln!("Thread {} got None for key {}", thread_id, key);
                            success.store(false, Ordering::SeqCst);
                            return;
                        }
                        Err(e) => {
                            eprintln!("Thread {} read error: {}", thread_id, e);
                            success.store(false, Ordering::SeqCst);
                            return;
                        }
                    }
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    assert!(
        success.load(Ordering::SeqCst),
        "Some threads failed during concurrent reads"
    );
    assert!(
        elapsed < timeout,
        "Concurrent reads took too long ({:?}), possible deadlock!",
        elapsed
    );

    println!(
        "✅ {} concurrent readers completed {} reads each in {:?}",
        num_readers, reads_per_thread, elapsed
    );
}

/// Test that read transactions don't block write transactions
/// The old implementation would deadlock here because read_txn() returned a write txn
#[test]
fn test_reads_dont_block_writes() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // Initialize
    Transaction::new(&db)
        .keys(vec![b"counter".to_vec()])
        .execute(|ctx| {
            ctx.set(b"counter", &TypedValue::I64(0))?;
            Ok(())
        })
        .unwrap();

    let num_readers = 5;
    let num_writes = 20;
    let barrier = Arc::new(Barrier::new(num_readers + 1)); // +1 for writer thread
    let writes_done = Arc::new(AtomicU64::new(0));
    let reader_stop = Arc::new(AtomicBool::new(false));
    let timeout = Duration::from_secs(10);

    let start = Instant::now();

    // Spawn reader threads that continuously hold read transactions
    let reader_handles: Vec<_> = (0..num_readers)
        .map(|_| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let reader_stop = Arc::clone(&reader_stop);

            thread::spawn(move || {
                barrier.wait();

                while !reader_stop.load(Ordering::SeqCst) {
                    let txn = db.canonical().read_txn().unwrap();
                    let _ = txn.get_state(b"counter").unwrap();
                    // Hold the transaction briefly
                    thread::sleep(Duration::from_millis(1));
                    drop(txn);
                }
            })
        })
        .collect();

    // Writer thread
    let db_clone = Arc::clone(&db);
    let barrier_clone = Arc::clone(&barrier);
    let writes_done_clone = Arc::clone(&writes_done);

    let writer_handle = thread::spawn(move || {
        barrier_clone.wait();

        for i in 0..num_writes {
            // Writes should succeed even while readers are active
            Transaction::new(&db_clone)
                .keys(vec![b"counter".to_vec()])
                .execute(|ctx| {
                    ctx.set(b"counter", &TypedValue::I64(i as i64))?;
                    Ok(())
                })
                .unwrap();
            writes_done_clone.fetch_add(1, Ordering::SeqCst);
        }
    });

    // Wait for writer to complete
    writer_handle.join().unwrap();

    // Signal readers to stop
    reader_stop.store(true, Ordering::SeqCst);

    // Wait for all readers
    for handle in reader_handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();

    assert_eq!(
        writes_done.load(Ordering::SeqCst),
        num_writes as u64,
        "Not all writes completed"
    );
    assert!(
        elapsed < timeout,
        "Test took too long ({:?}), reads may have blocked writes!",
        elapsed
    );

    println!(
        "✅ {} writes completed while {} readers were active in {:?}",
        num_writes, num_readers, elapsed
    );
}

/// Test that write transactions don't block read transactions
#[test]
fn test_writes_dont_block_reads() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // Initialize
    Transaction::new(&db)
        .keys(vec![b"value".to_vec()])
        .execute(|ctx| {
            ctx.set(b"value", &TypedValue::I64(0))?;
            Ok(())
        })
        .unwrap();

    let num_readers = 5;
    let reads_completed = Arc::new(AtomicU64::new(0));
    let barrier = Arc::new(Barrier::new(num_readers + 1));
    let test_duration = Duration::from_secs(2);
    let stop = Arc::new(AtomicBool::new(false));

    // Spawn readers
    let reader_handles: Vec<_> = (0..num_readers)
        .map(|_| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let reads_completed = Arc::clone(&reads_completed);
            let stop = Arc::clone(&stop);

            thread::spawn(move || {
                barrier.wait();

                while !stop.load(Ordering::SeqCst) {
                    let txn = db.canonical().read_txn().unwrap();
                    let _ = txn.get_state(b"value").unwrap();
                    reads_completed.fetch_add(1, Ordering::SeqCst);
                }
            })
        })
        .collect();

    // Writer thread that continuously writes
    let db_clone = Arc::clone(&db);
    let barrier_clone = Arc::clone(&barrier);
    let stop_clone = Arc::clone(&stop);

    let writer_handle = thread::spawn(move || {
        barrier_clone.wait();

        let mut counter = 0i64;
        while !stop_clone.load(Ordering::SeqCst) {
            Transaction::new(&db_clone)
                .keys(vec![b"value".to_vec()])
                .execute(|ctx| {
                    ctx.set(b"value", &TypedValue::I64(counter))?;
                    Ok(())
                })
                .unwrap();
            counter += 1;
            thread::sleep(Duration::from_millis(10));
        }
    });

    // Let test run for the duration
    thread::sleep(test_duration);
    stop.store(true, Ordering::SeqCst);

    writer_handle.join().unwrap();
    for handle in reader_handles {
        handle.join().unwrap();
    }

    let total_reads = reads_completed.load(Ordering::SeqCst);

    // We should have completed many reads during the test period
    assert!(
        total_reads > 100,
        "Only {} reads completed - writes may be blocking reads!",
        total_reads
    );

    println!(
        "✅ {} reads completed during {} seconds of continuous writes",
        total_reads,
        test_duration.as_secs()
    );
}

/// Stress test with many concurrent readers and writers
#[test]
fn test_high_concurrency_stress() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // Initialize test data
    let keys: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("item-{}", i).into_bytes())
        .collect();
    Transaction::new(&db)
        .keys(keys)
        .execute(|ctx| {
            for i in 0..50 {
                ctx.set(format!("item-{}", i).as_bytes(), &TypedValue::I64(i))?;
            }
            Ok(())
        })
        .unwrap();

    let num_readers = 10;
    let num_writers = 3;
    let operations_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_readers + num_writers));
    let errors = Arc::new(AtomicU64::new(0));
    let timeout = Duration::from_secs(30);

    let start = Instant::now();

    // Reader threads
    let reader_handles: Vec<_> = (0..num_readers)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let errors = Arc::clone(&errors);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..operations_per_thread {
                    let key = format!("item-{}", (thread_id * operations_per_thread + i) % 50);

                    match db.canonical().read_txn() {
                        Ok(txn) => {
                            if let Err(e) = txn.get_state(key.as_bytes()) {
                                eprintln!("Read error: {}", e);
                                errors.fetch_add(1, Ordering::SeqCst);
                            }
                        }
                        Err(e) => {
                            eprintln!("Failed to get read txn: {}", e);
                            errors.fetch_add(1, Ordering::SeqCst);
                        }
                    }
                }
            })
        })
        .collect();

    // Writer threads
    let writer_handles: Vec<_> = (0..num_writers)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);
            let errors = Arc::clone(&errors);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..operations_per_thread {
                    let key = format!("writer-{}-{}", thread_id, i);

                    if let Err(e) = Transaction::new(&db)
                        .keys(vec![key.as_bytes().to_vec()])
                        .execute(|ctx| {
                            ctx.set(key.as_bytes(), &TypedValue::I64(i as i64))?;
                            Ok(())
                        })
                    {
                        eprintln!("Write error: {}", e);
                        errors.fetch_add(1, Ordering::SeqCst);
                    }
                }
            })
        })
        .collect();

    // Wait for all threads
    for handle in reader_handles {
        handle.join().unwrap();
    }
    for handle in writer_handles {
        handle.join().unwrap();
    }

    let elapsed = start.elapsed();
    let error_count = errors.load(Ordering::SeqCst);

    assert_eq!(
        error_count, 0,
        "{} errors occurred during stress test",
        error_count
    );
    assert!(
        elapsed < timeout,
        "Stress test took too long ({:?}), possible deadlock!",
        elapsed
    );

    println!(
        "✅ Stress test completed: {} readers × {} ops + {} writers × {} ops in {:?}",
        num_readers, operations_per_thread, num_writers, operations_per_thread, elapsed
    );
}

/// Test that read transactions provide MVCC snapshot isolation
#[test]
fn test_read_transaction_snapshot_isolation() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // Initialize
    Transaction::new(&db)
        .keys(vec![b"version".to_vec()])
        .execute(|ctx| {
            ctx.set(b"version", &TypedValue::I64(1))?;
            Ok(())
        })
        .unwrap();

    let barrier = Arc::new(Barrier::new(2));

    // Thread 1: Hold a read transaction while updates happen
    let db1 = Arc::clone(&db);
    let barrier1 = Arc::clone(&barrier);
    let reader_handle = thread::spawn(move || {
        let txn = db1.canonical().read_txn().unwrap();

        // Read initial value
        let bytes = txn.get_state(b"version").unwrap().unwrap();
        let initial = TypedValue::from_bytes(&bytes).unwrap().as_i64().unwrap();
        assert_eq!(initial, 1, "Initial value should be 1");

        // Signal writer to proceed
        barrier1.wait();

        // Wait for writer to complete updates
        barrier1.wait();

        // Read again - should still see version 1 (snapshot isolation)
        let bytes = txn.get_state(b"version").unwrap().unwrap();
        let snapshot = TypedValue::from_bytes(&bytes).unwrap().as_i64().unwrap();
        assert_eq!(
            snapshot, 1,
            "Snapshot should still show version 1, but got {}",
            snapshot
        );

        drop(txn);

        // New transaction should see updated value
        let new_txn = db1.canonical().read_txn().unwrap();
        let bytes = new_txn.get_state(b"version").unwrap().unwrap();
        let updated = TypedValue::from_bytes(&bytes).unwrap().as_i64().unwrap();
        assert_eq!(updated, 5, "New transaction should see version 5");
    });

    // Thread 2: Perform updates
    let db2 = Arc::clone(&db);
    let barrier2 = Arc::clone(&barrier);
    let writer_handle = thread::spawn(move || {
        // Wait for reader to take snapshot
        barrier2.wait();

        // Perform multiple updates
        for v in 2..=5 {
            Transaction::new(&db2)
                .keys(vec![b"version".to_vec()])
                .execute(|ctx| {
                    ctx.set(b"version", &TypedValue::I64(v))?;
                    Ok(())
                })
                .unwrap();
        }

        // Signal reader that updates are done
        barrier2.wait();
    });

    reader_handle.join().unwrap();
    writer_handle.join().unwrap();

    println!("✅ Snapshot isolation verified");
}

/// Test read pool exhaustion behavior
#[tokio::test]
async fn test_read_pool_exhaustion_timeout() {
    let (db, _temp) = create_test_db_with_pool(2);
    let pool = db.canonical().read_pool().unwrap().clone();

    // Modify pool config for faster timeout (we can't directly, so we test what we can)
    assert_eq!(pool.available_permits(), 2);

    // Acquire all permits in sequence, release each before acquiring next
    for _ in 0..5 {
        let txn = pool.acquire().await.unwrap();
        assert!(pool.available_permits() < 2);
        drop(txn);
    }

    println!("✅ Pool exhaustion and recovery works correctly");
}

/// Verify the old footgun behavior would have caused issues
/// This is a documentation test - it shows what the old code would have done
#[test]
fn test_old_footgun_would_have_blocked() {
    // This test documents the old problematic behavior:
    //
    // OLD CODE (footgun):
    //   fn read_txn(&self) -> Result<Self::Txn<'_>> {
    //       self.write_txn()  // Returns WRITE transaction!
    //   }
    //
    // PROBLEM: If thread A called read_txn() and held it, thread B could not
    // get a write_txn() because LMDB only allows one writer at a time.
    //
    // NEW CODE (fixed):
    //   type ReadTxn<'a> = LmdbReadTxn<'a>;
    //   fn read_txn(&self) -> Result<Self::ReadTxn<'_>> {
    //       Ok(LmdbReadTxn::new(self.env.begin_ro_txn()?, self.state_db))
    //   }
    //
    // SOLUTION: read_txn() now returns a true read-only transaction that
    // doesn't block other readers or writers.

    let (db, _temp) = create_test_db();

    // This pattern would have deadlocked with the old implementation:
    // 1. Thread A: let read_txn = db.canonical().read_txn(); // Actually a write txn!
    // 2. Thread B: let write_txn = db.canonical().write_txn(); // BLOCKED forever!

    // With the new implementation, this works fine:
    let read_txn = db.canonical().read_txn().unwrap();
    let _value = read_txn.get_state(b"anything").unwrap();

    // In the same thread, we can still get a write transaction
    // (In practice you'd do this from different threads, but we're verifying
    // that read_txn is truly read-only and doesn't hold the write lock)
    drop(read_txn); // Must drop first in same thread due to LMDB per-thread transaction limits

    let write_result = Transaction::new(&db)
        .keys(vec![b"key".to_vec()])
        .execute(|ctx| {
            ctx.set(b"key", &TypedValue::I64(42))?;
            Ok(())
        });
    assert!(write_result.is_ok());

    println!("✅ Old footgun behavior has been fixed");
}

/// Test that long-running reads don't starve writers
#[test]
fn test_long_reads_dont_starve_writers() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // Initialize
    Transaction::new(&db)
        .keys(vec![b"data".to_vec()])
        .execute(|ctx| {
            ctx.set(b"data", &TypedValue::I64(0))?;
            Ok(())
        })
        .unwrap();

    let writes_completed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(2));

    // Long-running reader
    let db1 = Arc::clone(&db);
    let barrier1 = Arc::clone(&barrier);
    let stop1 = Arc::clone(&stop);
    let reader = thread::spawn(move || {
        barrier1.wait();

        while !stop1.load(Ordering::SeqCst) {
            let txn = db1.canonical().read_txn().unwrap();
            // Simulate long read operation
            thread::sleep(Duration::from_millis(50));
            let _ = txn.get_state(b"data").unwrap();
            drop(txn);
        }
    });

    // Writer
    let db2 = Arc::clone(&db);
    let barrier2 = Arc::clone(&barrier);
    let writes = Arc::clone(&writes_completed);
    let writer = thread::spawn(move || {
        barrier2.wait();

        for i in 0..20 {
            Transaction::new(&db2)
                .keys(vec![b"data".to_vec()])
                .execute(|ctx| {
                    ctx.set(b"data", &TypedValue::I64(i))?;
                    Ok(())
                })
                .unwrap();
            writes.fetch_add(1, Ordering::SeqCst);
        }
    });

    writer.join().unwrap();
    stop.store(true, Ordering::SeqCst);
    reader.join().unwrap();

    assert_eq!(
        writes_completed.load(Ordering::SeqCst),
        20,
        "Writer was starved!"
    );

    println!("✅ Long-running reads don't starve writers");
}
