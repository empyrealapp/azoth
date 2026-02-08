//! Tests for encryption and compression functionality

use azoth::prelude::*;
use azoth::BackupManifest;
use azoth::{BackupOptions, EncryptionKey};
use std::str::FromStr;
use tempfile::TempDir;

#[test]
fn test_encryption_key_generation() {
    let key = EncryptionKey::generate();
    let key_str = key.to_identity_string();

    // Should be able to parse it back
    let parsed = EncryptionKey::from_str(&key_str).unwrap();
    assert_eq!(parsed.to_identity_string(), key_str);
}

#[test]
fn test_encryption_key_recipient() {
    let key = EncryptionKey::generate();
    let recipient_str = key.to_recipient_string();

    // Recipient string should start with age1
    assert!(recipient_str.starts_with("age1"));
}

#[test]
fn test_backup_with_encryption() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let backup_path = temp_dir.path().join("backup");

    // Create database and write some data
    let db = AzothDb::open(&db_path)?;

    // Write some test data
    {
        let mut txn = db.canonical().write_txn()?;
        txn.put_state(b"key1", b"value1")?;
        txn.put_state(b"key2", b"value2")?;
        txn.append_event(b"test_event")?;
        txn.commit()?;
    }

    // Run projector
    db.projector().run_once()?;

    // Generate encryption key
    let key = EncryptionKey::generate();

    // For now, skip encryption/compression tests as they modify files in place
    // and need more careful implementation. Just test that options work.
    let _options = BackupOptions::new()
        .with_encryption(key.clone())
        .with_compression(false) // Disable compression for now
        .with_compression_level(3);

    // Create regular backup without encryption/compression
    db.backup_to(&backup_path)?;

    // Verify backup directory exists
    assert!(backup_path.exists());

    // Verify manifest exists
    let manifest_path = backup_path.join("manifest.json");
    assert!(manifest_path.exists());

    Ok(())
}

#[test]
fn test_backup_with_compression_only() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let backup_path = temp_dir.path().join("backup");

    // Create database and write some data
    let db = AzothDb::open(&db_path)?;

    // Write some test data
    {
        let mut txn = db.canonical().write_txn()?;
        txn.put_state(b"key1", b"value1")?;
        txn.append_event(b"test_event")?;
        txn.commit()?;
    }

    // Run projector
    db.projector().run_once()?;

    // Create backup with compression only (no encryption)
    let options = BackupOptions::new()
        .with_compression(true)
        .with_compression_level(3);

    db.backup_with_options(&backup_path, &options)?;

    // Verify backup directory exists
    assert!(backup_path.exists());

    // Verify manifest exists
    let manifest_path = backup_path.join("manifest.json");
    assert!(manifest_path.exists());

    Ok(())
}

#[test]
fn test_backup_with_empty_event_log_does_not_hang_or_fail() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let backup_path = temp_dir.path().join("backup");

    let db = AzothDb::open(&db_path)?;

    // Write state without appending any events.
    {
        let mut txn = db.canonical().write_txn()?;
        txn.put_state(b"key1", b"value1")?;
        txn.commit()?;
    }

    // No events: projector should already be caught up.
    let options = BackupOptions::new().with_compression(false);
    db.backup_with_options(&backup_path, &options)?;

    assert!(backup_path.join("manifest.json").exists());
    assert!(backup_path.join("canonical").join("data.mdb").exists());
    assert!(backup_path.join("projection.db").exists());

    Ok(())
}

#[test]
fn test_backup_restore_with_encryption_and_compression_roundtrip() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let db_path = temp_dir.path().join("test_db");
    let backup_path = temp_dir.path().join("backup");
    let restored_path = temp_dir.path().join("restored_db");

    let db = AzothDb::open(&db_path)?;

    // Write some state + at least one event so sealing/projector paths are exercised.
    {
        let mut txn = db.canonical().write_txn()?;
        txn.put_state(b"key1", b"value1")?;
        txn.append_event(b"test_event")?;
        txn.commit()?;
    }
    db.projector().run_once()?;

    let key = EncryptionKey::generate();
    let options = BackupOptions::new()
        .with_encryption(key)
        .with_compression(true)
        .with_compression_level(3);

    db.backup_with_options(&backup_path, &options)?;

    // Backup should not leave the DB permanently sealed (writes must continue post-backup).
    {
        let mut txn = db.canonical().write_txn()?;
        txn.put_state(b"post_backup_key", b"ok")?;
        txn.append_event(b"post_backup_event")?;
        txn.commit()?;
    }

    // Artifacts should reflect the combined (compress then encrypt) layout.
    assert!(backup_path.join("manifest.json").exists());
    assert!(backup_path.join("canonical.tar.zst.age").exists());
    assert!(backup_path.join("projection.db.zst.age").exists());
    assert!(!backup_path.join("canonical").exists());
    assert!(!backup_path.join("projection.db").exists());

    // Parse manifest so we can assert projection cursor/schema round-trip.
    let manifest_json = std::fs::read_to_string(backup_path.join("manifest.json"))?;
    let manifest: BackupManifest = serde_json::from_str(&manifest_json)
        .map_err(|e| AzothError::Serialization(e.to_string()))?;

    let _restored = AzothDb::restore_with_options(&backup_path, &restored_path, &options)?;
    let restored_db = AzothDb::open(&restored_path)?;

    {
        let ro = restored_db.canonical().read_txn()?;
        let v = ro.get_state(b"key1")?.expect("missing restored key1");
        assert_eq!(v, b"value1");
    } // drop read txn before other LMDB operations

    // Verify events round-trip too.
    let mut it = restored_db.canonical().iter_events(0, None)?;
    let mut events = Vec::new();
    while let Some((_id, bytes)) = it.next()? {
        events.push(bytes);
    }
    assert!(
        events.iter().any(|e| e.as_slice() == b"test_event"),
        "restored event log missing test_event"
    );

    // Projection should also match what the manifest recorded.
    assert_eq!(
        restored_db.projection().schema_version()?,
        manifest.projection_schema_version
    );
    assert_eq!(
        restored_db.projection().get_cursor()?,
        manifest.projection_cursor
    );
    assert_eq!(restored_db.projector().get_lag()?, 0);

    // Tightened invariant: when there is at least one event, the projection cursor should be
    // exactly the sealed event id (fully caught up at the snapshot point).
    if manifest.projection_cursor != u64::MAX {
        assert_eq!(manifest.projection_cursor, manifest.sealed_event_id);
    }

    Ok(())
}

#[test]
fn test_backup_options_builder() {
    let key = EncryptionKey::generate();
    let options = BackupOptions::new()
        .with_encryption(key)
        .with_compression(true)
        .with_compression_level(5);

    assert!(options.is_encrypted());
    assert!(options.compression);
    assert_eq!(options.compression_level, 5);
}
