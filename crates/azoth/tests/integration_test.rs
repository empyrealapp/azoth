//! Integration tests for axiom database

use azoth::prelude::*;
use std::sync::Arc;
use tempfile::TempDir;

/// Helper to create a test database
fn create_test_db() -> (AzothDb, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();
    (db, temp_dir)
}

#[test]
fn test_basic_open_and_close() {
    let (db, _temp) = create_test_db();

    // Should be able to get metadata
    assert!(db.canonical().meta().is_ok());
    assert!(db.projection().get_cursor().is_ok());

    // Close should work
    db.close().unwrap();
}

#[test]
fn test_write_and_read_state() {
    let (db, _temp) = create_test_db();

    // Write state
    let mut txn = db.canonical().write_txn().unwrap();
    txn.put_state(b"key1", b"value1").unwrap();
    txn.put_state(b"key2", b"value2").unwrap();
    let commit_info = txn.commit().unwrap();

    assert_eq!(commit_info.state_keys_written, 2);

    // Read state back
    let txn = db.canonical().write_txn().unwrap();
    assert_eq!(txn.get_state(b"key1").unwrap(), Some(b"value1".to_vec()));
    assert_eq!(txn.get_state(b"key2").unwrap(), Some(b"value2".to_vec()));
    assert_eq!(txn.get_state(b"nonexistent").unwrap(), None);
}

#[test]
fn test_append_events() {
    let (db, _temp) = create_test_db();

    // Append single event
    let mut txn = db.canonical().write_txn().unwrap();
    let event_id = txn.append_event(b"event1").unwrap();
    assert_eq!(event_id, 0);
    txn.commit().unwrap();

    // Append multiple events
    let mut txn = db.canonical().write_txn().unwrap();
    let (first, last) = txn
        .append_events(&[b"event2".to_vec(), b"event3".to_vec()])
        .unwrap();
    assert_eq!(first, 1);
    assert_eq!(last, 2);
    txn.commit().unwrap();

    // Verify next_event_id
    let meta = db.canonical().meta().unwrap();
    assert_eq!(meta.next_event_id, 3);
}

#[test]
fn test_atomic_state_and_events() {
    let (db, _temp) = create_test_db();

    // Write state and events in same transaction
    let mut txn = db.canonical().write_txn().unwrap();
    txn.put_state(b"balance", b"100").unwrap();
    txn.append_event(b"deposit:50").unwrap();
    txn.append_event(b"withdraw:20").unwrap();
    let commit_info = txn.commit().unwrap();

    assert_eq!(commit_info.state_keys_written, 1);
    assert_eq!(commit_info.events_written, 2);

    // Verify both committed
    let txn = db.canonical().write_txn().unwrap();
    assert_eq!(txn.get_state(b"balance").unwrap(), Some(b"100".to_vec()));

    let meta = db.canonical().meta().unwrap();
    assert_eq!(meta.next_event_id, 2);
}

#[test]
fn test_event_iteration() {
    let (db, _temp) = create_test_db();

    // Write events
    let mut txn = db.canonical().write_txn().unwrap();
    txn.append_events(&[b"event1".to_vec(), b"event2".to_vec(), b"event3".to_vec()])
        .unwrap();
    txn.commit().unwrap();

    // Iterate all events
    let mut iter = db.canonical().iter_events(0, None).unwrap();
    let events: Vec<_> = std::iter::from_fn(|| iter.next().transpose())
        .collect::<Result<_>>()
        .unwrap();

    assert_eq!(events.len(), 3);
    assert_eq!(events[0].0, 0);
    assert_eq!(events[0].1, b"event1");
    assert_eq!(events[1].0, 1);
    assert_eq!(events[2].0, 2);

    // Iterate range
    let mut iter = db.canonical().iter_events(1, Some(3)).unwrap();
    let events: Vec<_> = std::iter::from_fn(|| iter.next().transpose())
        .collect::<Result<_>>()
        .unwrap();

    assert_eq!(events.len(), 2);
    assert_eq!(events[0].0, 1);
    assert_eq!(events[1].0, 2);
}

#[test]
fn test_projector_basic() {
    let (db, _temp) = create_test_db();

    // Write events
    let mut txn = db.canonical().write_txn().unwrap();
    txn.append_event(b"test:event1").unwrap();
    txn.append_event(b"test:event2").unwrap();
    txn.commit().unwrap();

    // Initial cursor should be u64::MAX (represents -1 from i64, no events processed)
    assert_eq!(db.projection().get_cursor().unwrap(), u64::MAX);

    // Run projector
    let stats = db.projector().run_once().unwrap();
    assert_eq!(stats.events_applied, 2);

    // Cursor should advance
    assert_eq!(db.projection().get_cursor().unwrap(), 1);

    // Running again should do nothing (caught up)
    let stats = db.projector().run_once().unwrap();
    assert_eq!(stats.events_applied, 0);
}

#[test]
fn test_projector_lag() {
    let (db, _temp) = create_test_db();

    // No lag initially
    assert_eq!(db.projector().get_lag().unwrap(), 0);

    // Write events
    let mut txn = db.canonical().write_txn().unwrap();
    txn.append_event(b"test:event1").unwrap();
    txn.append_event(b"test:event2").unwrap();
    txn.append_event(b"test:event3").unwrap();
    txn.commit().unwrap();

    // Should have lag
    assert_eq!(db.projector().get_lag().unwrap(), 3);

    // Run projector
    db.projector().run_once().unwrap();

    // Lag should be reduced or zero
    assert_eq!(db.projector().get_lag().unwrap(), 0);
}

#[test]
fn test_seal() {
    let (db, _temp) = create_test_db();

    // Write some events
    let mut txn = db.canonical().write_txn().unwrap();
    txn.append_event(b"event1").unwrap();
    txn.append_event(b"event2").unwrap();
    txn.commit().unwrap();

    // Seal
    let sealed_id = db.canonical().seal().unwrap();
    assert_eq!(sealed_id, 1); // Last event ID

    // Metadata should reflect seal
    let meta = db.canonical().meta().unwrap();
    assert_eq!(meta.sealed_event_id, Some(1));

    // Cannot write after seal
    assert!(db.canonical().write_txn().is_err());
}

#[test]
fn test_pause_and_resume() {
    let (db, _temp) = create_test_db();

    // Initially not paused
    assert!(!db.canonical().is_paused());

    // Can write
    assert!(db.canonical().write_txn().is_ok());

    // Pause
    db.canonical().pause_ingestion().unwrap();
    assert!(db.canonical().is_paused());

    // Cannot write when paused
    assert!(db.canonical().write_txn().is_err());

    // Resume
    db.canonical().resume_ingestion().unwrap();
    assert!(!db.canonical().is_paused());

    // Can write again
    assert!(db.canonical().write_txn().is_ok());
}

#[test]
fn test_backup_and_restore() {
    let (db, temp1) = create_test_db();

    // Write some data
    let mut txn = db.canonical().write_txn().unwrap();
    txn.put_state(b"key", b"value").unwrap();
    txn.append_event(b"event1").unwrap();
    txn.append_event(b"event2").unwrap();
    txn.commit().unwrap();

    // Run projector
    db.projector().run_once().unwrap();

    // Backup
    let backup_dir = temp1.path().join("backup");
    db.backup_to(&backup_dir).unwrap();

    // Verify backup manifest exists
    assert!(backup_dir.join("manifest.json").exists());
    assert!(backup_dir.join("canonical").exists());
    assert!(backup_dir.join("projection.db").exists());

    // Restore to new location
    let temp2 = tempfile::tempdir().unwrap();
    let restored_db = AzothDb::restore_from(&backup_dir, &temp2.path().to_path_buf()).unwrap();

    // Verify restored metadata
    let meta = restored_db.canonical().meta().unwrap();
    assert_eq!(meta.sealed_event_id, Some(1));
    assert_eq!(meta.next_event_id, 2); // Should have 2 events (IDs 0 and 1)

    // Verify cursor was restored correctly
    let cursor = restored_db.projection().get_cursor().unwrap();
    assert_eq!(cursor, 1);

    // Note: The restored canonical is sealed, so we can't write new transactions
    // This is expected behavior - backups preserve the sealed state
}

#[test]
fn test_delete_state() {
    let (db, _temp) = create_test_db();

    // Write and delete
    let mut txn = db.canonical().write_txn().unwrap();
    txn.put_state(b"key", b"value").unwrap();
    txn.del_state(b"key").unwrap();
    let commit_info = txn.commit().unwrap();

    assert_eq!(commit_info.state_keys_written, 1);
    assert_eq!(commit_info.state_keys_deleted, 1);

    // Key should be gone
    let txn = db.canonical().write_txn().unwrap();
    assert_eq!(txn.get_state(b"key").unwrap(), None);
}

#[test]
fn test_multiple_transactions() {
    let (db, _temp) = create_test_db();

    // Transaction 1
    let mut txn = db.canonical().write_txn().unwrap();
    txn.put_state(b"key1", b"value1").unwrap();
    txn.append_event(b"event1").unwrap();
    txn.commit().unwrap();

    // Transaction 2
    let mut txn = db.canonical().write_txn().unwrap();
    txn.put_state(b"key2", b"value2").unwrap();
    txn.append_event(b"event2").unwrap();
    txn.commit().unwrap();

    // Both should be committed
    let txn = db.canonical().write_txn().unwrap();
    assert!(txn.get_state(b"key1").unwrap().is_some());
    assert!(txn.get_state(b"key2").unwrap().is_some());

    let meta = db.canonical().meta().unwrap();
    assert_eq!(meta.next_event_id, 2);
}

#[test]
fn test_transaction_abort() {
    let (db, _temp) = create_test_db();

    // Create transaction but abort
    let mut txn = db.canonical().write_txn().unwrap();
    txn.put_state(b"key", b"value").unwrap();
    txn.abort();

    // State should not be written
    let txn = db.canonical().write_txn().unwrap();
    assert_eq!(txn.get_state(b"key").unwrap(), None);
}

#[test]
fn test_event_id_monotonicity() {
    let (db, _temp) = create_test_db();

    // Write events in multiple transactions
    for i in 0..10 {
        let mut txn = db.canonical().write_txn().unwrap();
        let event_id = txn
            .append_event(&format!("event{}", i).into_bytes())
            .unwrap();
        assert_eq!(event_id, i);
        txn.commit().unwrap();
    }

    // Verify all events are in order
    let mut iter = db.canonical().iter_events(0, None).unwrap();
    for i in 0..10 {
        let (id, _) = iter.next().unwrap().unwrap();
        assert_eq!(id, i);
    }
}

#[test]
fn test_schema_version() {
    let (db, _temp) = create_test_db();

    // Initial version should be 0 (no migrations applied)
    assert_eq!(db.projection().schema_version().unwrap(), 0);

    // Migrate to version 1
    db.projection().migrate(1).unwrap();
    assert_eq!(db.projection().schema_version().unwrap(), 1);

    // Migrating to same version is a no-op
    db.projection().migrate(1).unwrap();
    assert_eq!(db.projection().schema_version().unwrap(), 1);
}

#[tokio::test]
async fn test_projector_wakes_on_event_notification() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Spawn the projector with a very long poll interval (10s).
    // If notification works, it will wake up far sooner.
    let db2 = Arc::clone(&db);
    let projector_handle = tokio::spawn(async move { db2.projector().run_continuous().await });

    // Give the projector task time to start and enter its await
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // Write events
    {
        let mut txn = db.canonical().write_txn().unwrap();
        txn.append_event(b"test:event1").unwrap();
        txn.append_event(b"test:event2").unwrap();
        txn.commit().unwrap();
    }

    // The notification should wake the projector almost immediately.
    // Wait up to 500ms -- if we had to rely on the 10s poll this would time out.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_millis(500);
    loop {
        if db.projector().get_lag().unwrap() == 0 {
            break;
        }
        if tokio::time::Instant::now() > deadline {
            panic!("Projector did not catch up within 500ms -- notification may not be working");
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    assert_eq!(db.projection().get_cursor().unwrap(), 1);

    // Shut down
    db.projector().shutdown();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), projector_handle).await;
}

#[test]
fn test_write_batch_atomic_commit() {
    let (db, _temp) = create_test_db();

    // Build a batch with multiple puts, a delete, and an event
    let mut batch = db.write_batch();
    batch.put(b"wb:key1", b"value1");
    batch.put(b"wb:key2", b"value2");
    batch.put(b"wb:key3", b"value3");
    batch.delete(b"wb:key3"); // delete within same batch
    batch.append_event(b"batch_write:3_keys");

    assert_eq!(batch.len(), 5);

    let info = batch.commit().unwrap();
    assert_eq!(info.state_keys_written, 3);
    assert_eq!(info.state_keys_deleted, 1);
    assert_eq!(info.events_written, 1);

    // Verify state (scope the read txn so LMDB's single-reader-per-thread slot is freed)
    {
        let txn = db.canonical().read_txn().unwrap();
        assert_eq!(txn.get_state(b"wb:key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(txn.get_state(b"wb:key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(txn.get_state(b"wb:key3").unwrap(), None); // deleted
    }

    // Verify event
    let meta = db.canonical().meta().unwrap();
    assert_eq!(meta.next_event_id, 1);
}

#[test]
fn test_write_batch_empty_is_noop() {
    let (db, _temp) = create_test_db();

    let batch = db.write_batch();
    assert!(batch.is_empty());

    // Committing an empty batch should succeed with zero-stat info
    let info = batch.commit().unwrap();
    assert_eq!(info.events_written, 0);
    assert_eq!(info.state_keys_written, 0);
}

#[tokio::test]
async fn test_write_batch_commit_async() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();

    let mut batch = db.write_batch();
    batch.put(b"async:key", b"async_value");
    batch.append_event(b"async_write:1");

    let info = batch.commit_async().await.unwrap();
    assert_eq!(info.state_keys_written, 1);
    assert_eq!(info.events_written, 1);

    // Verify
    let txn = db.canonical().read_txn().unwrap();
    assert_eq!(
        txn.get_state(b"async:key").unwrap(),
        Some(b"async_value".to_vec())
    );
}

#[tokio::test]
async fn test_submit_write_async() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();

    // Use submit_write to do a multi-op transaction asynchronously
    db.submit_write(|txn| {
        txn.put_state(b"sw:key1", b"val1")?;
        txn.put_state(b"sw:key2", b"val2")?;
        txn.append_event(b"submit_write:2")?;
        Ok(())
    })
    .await
    .unwrap();

    // Verify state was committed (scope the read txn to free the reader slot)
    {
        let read = db.canonical().read_txn().unwrap();
        assert_eq!(read.get_state(b"sw:key1").unwrap(), Some(b"val1".to_vec()));
        assert_eq!(read.get_state(b"sw:key2").unwrap(), Some(b"val2".to_vec()));
    }

    // Verify event
    let meta = db.canonical().meta().unwrap();
    assert_eq!(meta.next_event_id, 1);
}
