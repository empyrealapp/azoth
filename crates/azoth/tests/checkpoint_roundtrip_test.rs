//! Regression tests for checkpoint create/restore with backup options.

use azoth::checkpoint::{CheckpointConfig, CheckpointManager, LocalStorage};
use azoth::prelude::*;
use azoth::BackupManifest;
use std::io::Read;
use std::sync::Arc;
use tempfile::TempDir;

fn read_manifest_from_tar(tar_path: &std::path::Path) -> Result<BackupManifest> {
    let f = std::fs::File::open(tar_path)?;
    let mut ar = tar::Archive::new(f);
    for entry in ar.entries().map_err(AzothError::Io)? {
        let mut entry = entry.map_err(AzothError::Io)?;
        let path = entry
            .path()
            .map_err(AzothError::Io)?
            .to_string_lossy()
            .to_string();
        if path.ends_with("manifest.json") {
            let mut s = String::new();
            entry.read_to_string(&mut s).map_err(AzothError::Io)?;
            let manifest: BackupManifest =
                serde_json::from_str(&s).map_err(|e| AzothError::Serialization(e.to_string()))?;
            return Ok(manifest);
        }
    }
    Err(AzothError::Restore("manifest.json not found in checkpoint tar".into()))
}

#[tokio::test(flavor = "current_thread")]
async fn test_checkpoint_create_restore_with_encryption_and_compression() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let storage_dir = TempDir::new()?;

    let db_path = temp_dir.path().join("db");
    let restore_path = temp_dir.path().join("restored_db");

    let db = Arc::new(
        AzothDb::open(&db_path)
            .map_err(|e| AzothError::Internal(format!("AzothDb::open failed: {}", e)))?,
    );

    // Write some state + an event.
    {
        let mut txn = db
            .canonical()
            .write_txn()
            .map_err(|e| AzothError::Internal(format!("write_txn failed: {}", e)))?;
        txn.put_state(b"key1", b"value1")
            .map_err(|e| AzothError::Internal(format!("put_state failed: {}", e)))?;
        txn.append_event(b"test_event")
            .map_err(|e| AzothError::Internal(format!("append_event failed: {}", e)))?;
        txn.commit()
            .map_err(|e| AzothError::Internal(format!("commit failed: {}", e)))?;
    }
    db.projector()
        .run_once()
        .map_err(|e| AzothError::Internal(format!("projector.run_once failed: {}", e)))?;

    let key = EncryptionKey::generate();
    let config = CheckpointConfig::new()
        .with_encryption(key)
        .with_compression(true)
        .with_compression_level(3);

    let storage = LocalStorage::new(storage_dir.path().to_path_buf());
    let manager = CheckpointManager::new(db.clone(), storage, config);

    let meta = manager
        .create_checkpoint()
        .await
        .map_err(|e| AzothError::Internal(format!("create_checkpoint failed: {}", e)))?;

    // Parse manifest from the stored checkpoint tar so we can assert projection cursor/schema.
    let manifest = read_manifest_from_tar(storage_dir.path().join(&meta.id).as_path())
        .map_err(|e| AzothError::Internal(format!("read_manifest_from_tar failed: {}", e)))?;

    manager
        .restore_checkpoint(&meta.id, restore_path.as_path())
        .await
        .map_err(|e| AzothError::Internal(format!("restore_checkpoint failed: {}", e)))?;

    let restored_db = AzothDb::open(&restore_path)
        .map_err(|e| AzothError::Internal(format!("AzothDb::open(restored) failed: {}", e)))?;
    {
        let ro = restored_db
            .canonical()
            .read_txn()
            .map_err(|e| AzothError::Internal(format!("restored read_txn failed: {}", e)))?;
        let v = ro
            .get_state(b"key1")
            .map_err(|e| AzothError::Internal(format!("restored get_state failed: {}", e)))?
            .expect("missing restored key1");
        assert_eq!(v, b"value1");
    } // drop read txn before other LMDB operations

    let mut it = restored_db.canonical().iter_events(0, None)?;
    let mut events = Vec::new();
    while let Some((_id, bytes)) = it.next()? {
        events.push(bytes);
    }
    assert!(
        events.iter().any(|e| e.as_slice() == b"test_event"),
        "restored event log missing test_event"
    );

    assert_eq!(
        restored_db.projection().schema_version()?,
        manifest.projection_schema_version
    );
    assert_eq!(restored_db.projection().get_cursor()?, manifest.projection_cursor);
    assert_eq!(restored_db.projector().get_lag()?, 0);

    if manifest.projection_cursor != u64::MAX {
        assert_eq!(manifest.projection_cursor, manifest.sealed_event_id);
    }

    Ok(())
}
