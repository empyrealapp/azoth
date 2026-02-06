//! Integration tests for read pool functionality
//!
//! Tests concurrent read operations using the read pool system.

use azoth::{AzothDb, CanonicalReadTxn, CanonicalStore, Transaction, TypedValue};
use azoth_core::{CanonicalConfig, ProjectionConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn create_test_db_with_read_pool(pool_size: usize) -> (AzothDb, TempDir) {
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

fn create_test_db_without_pool() -> (AzothDb, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();
    (db, temp_dir)
}

#[test]
fn test_read_txn_trait_works() {
    let (db, _temp) = create_test_db_without_pool();

    // Write some test data
    Transaction::new(&db)
        .keys(vec![b"key1".to_vec(), b"key2".to_vec()])
        .execute(|ctx| {
            ctx.set(b"key1", &TypedValue::I64(100))?;
            ctx.set(b"key2", &TypedValue::I64(200))?;
            Ok(())
        })
        .unwrap();

    // Read using the trait method (not the deprecated read_only_txn)
    let txn = db.canonical().read_txn().unwrap();
    let value = txn.get_state(b"key1").unwrap().unwrap();
    let typed = TypedValue::from_bytes(&value).unwrap();
    assert_eq!(typed.as_i64().unwrap(), 100);

    let value2 = txn.get_state(b"key2").unwrap().unwrap();
    let typed2 = TypedValue::from_bytes(&value2).unwrap();
    assert_eq!(typed2.as_i64().unwrap(), 200);

    // Non-existent key
    assert!(txn.get_state(b"nonexistent").unwrap().is_none());
}

#[test]
fn test_read_pool_enabled() {
    let (db, _temp) = create_test_db_with_read_pool(4);

    // Verify pool is enabled
    assert!(db.canonical().has_read_pool());
    let pool = db.canonical().read_pool().unwrap();
    assert_eq!(pool.available_permits(), 4);
}

#[test]
fn test_read_pool_disabled_by_default() {
    let (db, _temp) = create_test_db_without_pool();

    // Verify pool is not enabled by default
    assert!(!db.canonical().has_read_pool());
    assert!(db.canonical().read_pool().is_none());
}

#[test]
fn test_concurrent_reads_without_pool() {
    let (db, _temp) = create_test_db_without_pool();
    let db = Arc::new(db);

    // Write test data
    let keys: Vec<Vec<u8>> = (0..100)
        .map(|i| format!("key-{}", i).into_bytes())
        .collect();
    Transaction::new(&db)
        .keys(keys)
        .execute(|ctx| {
            for i in 0..100 {
                let key = format!("key-{}", i);
                ctx.set(key.as_bytes(), &TypedValue::I64(i))?;
            }
            Ok(())
        })
        .unwrap();

    let num_threads = 10;
    let reads_per_thread = 50;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            let db = Arc::clone(&db);
            let barrier = Arc::clone(&barrier);

            thread::spawn(move || {
                barrier.wait();

                for i in 0..reads_per_thread {
                    let key_idx = (thread_id * reads_per_thread + i) % 100;
                    let key = format!("key-{}", key_idx);

                    // Use the trait method for reading
                    let txn = db.canonical().read_txn().unwrap();
                    let bytes = txn.get_state(key.as_bytes()).unwrap().unwrap();
                    let value = TypedValue::from_bytes(&bytes).unwrap();
                    assert_eq!(value.as_i64().unwrap(), key_idx as i64);
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}

#[tokio::test]
async fn test_lmdb_read_pool_concurrent_async() {
    let (db, _temp) = create_test_db_with_read_pool(4);
    let db = Arc::new(db);

    // Write test data using execute_blocking (we're in async context)
    let keys: Vec<Vec<u8>> = (0..10)
        .map(|i| format!("async-key-{}", i).into_bytes())
        .collect();
    Transaction::new(&db)
        .keys(keys)
        .execute_blocking(|ctx| {
            for i in 0..10 {
                let key = format!("async-key-{}", i);
                ctx.set(key.as_bytes(), &TypedValue::I64(i * 100))?;
            }
            Ok(())
        })
        .unwrap();

    let pool = db.canonical().read_pool().unwrap().clone();

    // Spawn multiple async tasks using the pool
    let mut handles = Vec::new();
    for i in 0..10 {
        let pool = pool.clone();
        handles.push(tokio::spawn(async move {
            let key = format!("async-key-{}", i % 10);
            let txn = pool.acquire().await.unwrap();
            let bytes = txn.get_state(key.as_bytes()).unwrap().unwrap();
            let value = TypedValue::from_bytes(&bytes).unwrap();
            assert_eq!(value.as_i64().unwrap(), (i % 10) * 100);
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_lmdb_read_pool_exhaustion_and_recovery() {
    let (db, _temp) = create_test_db_with_read_pool(2);
    let db = Arc::new(db);

    // Write test data using execute_blocking (we're in async context)
    Transaction::new(&db)
        .keys(vec![b"test-key".to_vec()])
        .execute_blocking(|ctx| {
            ctx.set(b"test-key", &TypedValue::I64(42))?;
            Ok(())
        })
        .unwrap();

    let pool = db.canonical().read_pool().unwrap().clone();

    // Verify initial permits
    assert_eq!(pool.available_permits(), 2);

    // Acquire and use one slot, then release it
    {
        let txn1 = pool.acquire().await.unwrap();
        assert_eq!(pool.available_permits(), 1);

        // Read using acquired transaction
        let bytes1 = txn1.get_state(b"test-key").unwrap().unwrap();
        let value1 = TypedValue::from_bytes(&bytes1).unwrap();
        assert_eq!(value1.as_i64().unwrap(), 42);
    }
    // Permit should be released
    assert_eq!(pool.available_permits(), 2);

    // Acquire again to verify pool is still working
    {
        let txn2 = pool.acquire().await.unwrap();
        assert_eq!(pool.available_permits(), 1);

        let bytes2 = txn2.get_state(b"test-key").unwrap().unwrap();
        let value2 = TypedValue::from_bytes(&bytes2).unwrap();
        assert_eq!(value2.as_i64().unwrap(), 42);
    }
    assert_eq!(pool.available_permits(), 2);
}

#[test]
fn test_read_txn_doesnt_block_write() {
    let (db, _temp) = create_test_db_without_pool();
    let db = Arc::new(db);

    // Write initial data
    Transaction::new(&db)
        .keys(vec![b"counter".to_vec()])
        .execute(|ctx| {
            ctx.set(b"counter", &TypedValue::I64(0))?;
            Ok(())
        })
        .unwrap();

    // Spawn a thread that holds a read transaction
    let db_clone = Arc::clone(&db);
    let barrier = Arc::new(Barrier::new(2));
    let barrier_clone = Arc::clone(&barrier);

    let read_handle = thread::spawn(move || {
        // Hold a read transaction
        let read_txn = db_clone.canonical().read_txn().unwrap();

        // Verify read works
        let bytes = read_txn.get_state(b"counter").unwrap().unwrap();
        let value = TypedValue::from_bytes(&bytes).unwrap();
        assert_eq!(value.as_i64().unwrap(), 0);

        // Signal that we're holding the read txn
        barrier_clone.wait();

        // Wait for write to complete
        barrier_clone.wait();

        // Read transaction still sees old value (MVCC snapshot)
        let bytes_again = read_txn.get_state(b"counter").unwrap().unwrap();
        let value_again = TypedValue::from_bytes(&bytes_again).unwrap();
        assert_eq!(value_again.as_i64().unwrap(), 0);
    });

    // Wait for read thread to acquire transaction
    barrier.wait();

    // Write while another thread holds read - should NOT deadlock
    // (This would have deadlocked with the old footgun implementation)
    let write_result = Transaction::new(&db)
        .keys(vec![b"counter".to_vec()])
        .execute(|ctx| {
            ctx.set(b"counter", &TypedValue::I64(1))?;
            Ok(())
        });
    assert!(write_result.is_ok());

    // Signal read thread that write is done
    barrier.wait();

    // Wait for read thread to finish
    read_handle.join().unwrap();

    // New read sees updated value
    let new_txn = db.canonical().read_txn().unwrap();
    let new_bytes = new_txn.get_state(b"counter").unwrap().unwrap();
    let new_value = TypedValue::from_bytes(&new_bytes).unwrap();
    assert_eq!(new_value.as_i64().unwrap(), 1);
}

#[test]
fn test_sqlite_read_pool() {
    // SQLite projection store also supports read pool
    // Check that it can be configured
    let temp_dir = tempfile::tempdir().unwrap();
    let canonical_config = CanonicalConfig::new(temp_dir.path().join("canonical"));
    let projection_config =
        ProjectionConfig::new(temp_dir.path().join("projection.db")).with_read_pool_size(4);

    let db = AzothDb::open_with_config(
        temp_dir.path().to_path_buf(),
        canonical_config,
        projection_config,
    )
    .unwrap();

    assert!(db.projection().has_read_pool());
    let pool = db.projection().read_pool().unwrap();
    assert_eq!(pool.pool_size(), 4);
}

#[tokio::test]
async fn test_sqlite_read_pool_query() {
    let temp_dir = tempfile::tempdir().unwrap();
    let canonical_config = CanonicalConfig::new(temp_dir.path().join("canonical"));
    let projection_config =
        ProjectionConfig::new(temp_dir.path().join("projection.db")).with_read_pool_size(2);

    let db = AzothDb::open_with_config(
        temp_dir.path().to_path_buf(),
        canonical_config,
        projection_config,
    )
    .unwrap();

    // Create a test table
    db.execute(|conn| {
        conn.execute(
            "CREATE TABLE test_items (id INTEGER PRIMARY KEY, value TEXT)",
            [],
        )
        .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?;
        conn.execute("INSERT INTO test_items (id, value) VALUES (1, 'hello')", [])
            .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?;
        conn.execute("INSERT INTO test_items (id, value) VALUES (2, 'world')", [])
            .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?;
        Ok(())
    })
    .unwrap();

    let pool = db.projection().read_pool().unwrap().clone();

    // Query using pool
    let conn = pool.acquire().await.unwrap();
    let value: String = conn
        .query_row("SELECT value FROM test_items WHERE id = ?1", [1], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(value, "hello");

    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM test_items", [], |row| row.get(0))
        .unwrap();
    assert_eq!(count, 2);
}
