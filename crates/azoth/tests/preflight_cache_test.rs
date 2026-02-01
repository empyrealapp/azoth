//! Integration tests for preflight cache functionality

use azoth::{AzothDb, Transaction, TypedValue};
use azoth_core::{error::Result, CanonicalConfig, ProjectionConfig};
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn create_test_db_with_cache(cache_enabled: bool) -> (AzothDb, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let canonical_config = CanonicalConfig::new(temp_dir.path().join("canonical"))
        .with_preflight_cache(cache_enabled)
        .with_preflight_cache_size(1000)
        .with_preflight_cache_ttl(60);
    let projection_config = ProjectionConfig::new(temp_dir.path().join("projection.db"));
    let db = AzothDb::open_with_config(
        temp_dir.path().to_path_buf(),
        canonical_config,
        projection_config,
    )
    .unwrap();
    (db, temp_dir)
}

#[test]
fn test_preflight_cache_hot_keys() -> Result<()> {
    let (db, _temp) = create_test_db_with_cache(true);

    // Initialize a hot key
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"balance", &TypedValue::I64(1000))?;
            Ok(())
        })?;

    // Read the same key multiple times during preflight
    // First read should be a cache miss, subsequent reads should be cache hits
    for i in 0..100 {
        Transaction::new(&db)
            .read_keys(vec![b"balance".to_vec()])
            .preflight(|ctx| {
                let balance = ctx.get(b"balance")?.as_i64()?;
                assert_eq!(balance, 1000);
                Ok(())
            })
            .execute(|ctx| {
                // Don't modify anything, just validate cache works
                ctx.set(b"counter", &TypedValue::I64(i))?;
                Ok(())
            })?;
    }

    // Verify cache stats (cache should have entries)
    let stats = db.canonical().preflight_cache().stats();
    assert!(stats.enabled);
    assert!(stats.size > 0);

    Ok(())
}

#[test]
fn test_preflight_cache_invalidation() -> Result<()> {
    let (db, _temp) = create_test_db_with_cache(true);

    // Initialize a key
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"value", &TypedValue::I64(100))?;
            Ok(())
        })?;

    // Read it (should be cached)
    Transaction::new(&db)
        .read_keys(vec![b"value".to_vec()])
        .preflight(|ctx| {
            let value = ctx.get(b"value")?.as_i64()?;
            assert_eq!(value, 100);
            Ok(())
        })
        .execute(|_| Ok(()))?;

    // Modify the key (should invalidate cache)
    Transaction::new(&db)
        .write_keys(vec![b"value".to_vec()])
        .execute(|ctx| {
            ctx.set(b"value", &TypedValue::I64(200))?;
            Ok(())
        })?;

    // Read it again (should read fresh value, not cached old value)
    Transaction::new(&db)
        .read_keys(vec![b"value".to_vec()])
        .preflight(|ctx| {
            let value = ctx.get(b"value")?.as_i64()?;
            assert_eq!(value, 200, "Cache should have been invalidated");
            Ok(())
        })
        .execute(|_| Ok(()))?;

    Ok(())
}

#[test]
fn test_preflight_cache_concurrent() -> Result<()> {
    let (db, _temp) = create_test_db_with_cache(true);
    let db = Arc::new(db);

    // Initialize keys
    Transaction::new(&db)
        .execute(|ctx| {
            for i in 0..10 {
                let key = format!("key-{}", i);
                ctx.set(key.as_bytes(), &TypedValue::I64(i))?;
            }
            Ok(())
        })?;

    // Spawn multiple threads doing concurrent reads
    let mut handles = vec![];
    let barrier = Arc::new(Barrier::new(10));

    for thread_id in 0..10 {
        let db_clone = Arc::clone(&db);
        let barrier_clone = Arc::clone(&barrier);

        let handle = thread::spawn(move || -> Result<()> {
            // Wait for all threads to be ready
            barrier_clone.wait();

            // Each thread reads multiple keys
            for i in 0..10 {
                let key = format!("key-{}", i);
                Transaction::new(&db_clone)
                    .read_keys(vec![key.as_bytes().to_vec()])
                    .preflight(|ctx| {
                        let value = ctx.get(key.as_bytes())?.as_i64()?;
                        assert_eq!(value, i);
                        Ok(())
                    })
                    .execute(|ctx| {
                        // Write a thread-specific counter
                        let counter_key = format!("counter-{}", thread_id);
                        ctx.set(counter_key.as_bytes(), &TypedValue::I64(i))?;
                        Ok(())
                    })?;
            }

            Ok(())
        });

        handles.push(handle);
    }

    // Wait for all threads to complete
    for handle in handles {
        handle.join().unwrap()?;
    }

    // Verify all values are correct
    let txn = db.canonical().read_only_txn()?;
    for i in 0..10 {
        let key = format!("key-{}", i);
        let bytes = txn.get_state(key.as_bytes())?.unwrap();
        let value = TypedValue::from_bytes(&bytes)?.as_i64()?;
        assert_eq!(value, i);
    }

    Ok(())
}

#[test]
fn test_preflight_cache_non_existent_keys() -> Result<()> {
    let (db, _temp) = create_test_db_with_cache(true);

    // Try to get a non-existent key (should cache the None result)
    let result = Transaction::new(&db)
        .read_keys(vec![b"nonexistent".to_vec()])
        .preflight(|ctx| {
            let value = ctx.get_opt(b"nonexistent")?;
            assert!(value.is_none(), "Key should not exist");
            Ok(())
        })
        .execute(|_| Ok(()));

    assert!(result.is_ok());

    // Try again - should use cached None
    let result = Transaction::new(&db)
        .read_keys(vec![b"nonexistent".to_vec()])
        .preflight(|ctx| {
            let value = ctx.get_opt(b"nonexistent")?;
            assert!(value.is_none(), "Key should still not exist");
            Ok(())
        })
        .execute(|_| Ok(()));

    assert!(result.is_ok());

    // Now create the key
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"nonexistent", &TypedValue::I64(42))?;
            Ok(())
        })?;

    // Read it - should see the new value (cache invalidated)
    Transaction::new(&db)
        .read_keys(vec![b"nonexistent".to_vec()])
        .preflight(|ctx| {
            let value = ctx.get_opt(b"nonexistent")?;
            assert!(value.is_some(), "Key should now exist");
            assert_eq!(value.unwrap().as_i64()?, 42);
            Ok(())
        })
        .execute(|_| Ok(()))?;

    Ok(())
}

#[test]
fn test_preflight_cache_disabled() -> Result<()> {
    let (db, _temp) = create_test_db_with_cache(false);

    // Initialize a key
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"value", &TypedValue::I64(100))?;
            Ok(())
        })?;

    // Read it multiple times
    for _ in 0..10 {
        Transaction::new(&db)
            .read_keys(vec![b"value".to_vec()])
            .preflight(|ctx| {
                let value = ctx.get(b"value")?.as_i64()?;
                assert_eq!(value, 100);
                Ok(())
            })
            .execute(|_| Ok(()))?;
    }

    // Cache should be empty (disabled)
    let stats = db.canonical().preflight_cache().stats();
    assert!(!stats.enabled);
    assert_eq!(stats.size, 0);

    Ok(())
}

#[test]
fn test_preflight_cache_exists_method() -> Result<()> {
    let (db, _temp) = create_test_db_with_cache(true);

    // Initialize a key
    Transaction::new(&db)
        .execute(|ctx| {
            ctx.set(b"exists_key", &TypedValue::I64(123))?;
            Ok(())
        })?;

    // Check existence (should cache the result)
    Transaction::new(&db)
        .read_keys(vec![b"exists_key".to_vec()])
        .preflight(|ctx| {
            assert!(ctx.exists(b"exists_key")?, "Key should exist");
            assert!(!ctx.exists(b"missing_key")?, "Key should not exist");
            Ok(())
        })
        .execute(|_| Ok(()))?;

    // Check again - should use cache
    Transaction::new(&db)
        .read_keys(vec![b"exists_key".to_vec()])
        .preflight(|ctx| {
            assert!(ctx.exists(b"exists_key")?, "Key should still exist");
            Ok(())
        })
        .execute(|_| Ok(()))?;

    Ok(())
}
