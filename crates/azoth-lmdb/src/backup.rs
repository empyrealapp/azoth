use azoth_core::{
    error::{AzothError, Result},
    event_log::EventLog,
    traits::CanonicalStore,
    types::BackupInfo,
    CanonicalConfig,
};
use std::path::Path;

use crate::store::LmdbCanonicalStore;

fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        let dest = dst.join(entry.file_name());
        let ty = entry.file_type()?;
        if ty.is_dir() {
            copy_dir_recursive(&path, &dest)?;
        } else if ty.is_file() {
            std::fs::copy(&path, &dest)?;
        }
    }
    Ok(())
}

/// Create a backup of the LMDB store
///
/// Creates a consistent snapshot of the database. Since the store should be
/// sealed before backup (enforced by requiring sealed_event_id), the database
/// is in a consistent state and safe to copy.
///
/// Note: This uses file copy for now. The lmdb-rs crate (v0.8) doesn't expose
/// the env_copy API. Future versions could use heed or upgrade to lmdb-rkv
/// which may provide better hot backup support.
pub fn backup_to(store: &LmdbCanonicalStore, dir: &Path) -> Result<BackupInfo> {
    // Create backup directory
    std::fs::create_dir_all(dir)?;

    // Ensure the file-based event log is durably flushed before we snapshot files.
    store.event_log.sync()?;

    // Get sealed event ID (enforces that store is sealed = consistent state)
    let meta = store.meta()?;
    let sealed_event_id = match (meta.next_event_id, meta.sealed_event_id) {
        // Empty event log: there is nothing to seal, but the state is still consistent.
        (0, _) => 0,
        (_, Some(id)) => id,
        _ => return Err(AzothError::Backup("Store must be sealed before backup".into())),
    };

    // Copy LMDB data file
    // Since the store is sealed, the database is in a consistent state
    // and we can safely copy the file
    let src_dir = &store.config_path;
    let src_data = src_dir.join("data.mdb");

    if !src_data.exists() {
        return Err(AzothError::Backup("Source data file not found".into()));
    }

    let backup_data = dir.join("data.mdb");
    std::fs::copy(&src_data, &backup_data)?;

    // Also copy lock file if it exists (though typically not needed for restore)
    let src_lock = src_dir.join("lock.mdb");
    if src_lock.exists() {
        let backup_lock = dir.join("lock.mdb");
        let _ = std::fs::copy(&src_lock, &backup_lock); // Don't fail if lock copy fails
    }

    // Copy file-based event log directory (required for event sourcing restore).
    let src_event_log = src_dir.join("event-log");
    if src_event_log.exists() {
        let backup_event_log = dir.join("event-log");
        copy_dir_recursive(&src_event_log, &backup_event_log)?;
    }

    // Get size
    let size_bytes = std::fs::metadata(&backup_data)?.len();

    Ok(BackupInfo {
        sealed_event_id,
        path: dir.display().to_string(),
        timestamp: chrono::Utc::now().to_rfc3339(),
        size_bytes,
    })
}

/// Restore from a backup
pub fn restore_from(dir: &Path, cfg: CanonicalConfig) -> Result<LmdbCanonicalStore> {
    // Verify backup exists
    let backup_data = dir.join("data.mdb");
    if !backup_data.exists() {
        return Err(AzothError::Restore("Backup data file not found".into()));
    }

    // Create target directory
    std::fs::create_dir_all(&cfg.path)?;

    // Copy backup files to target
    let dst_data = cfg.path.join("data.mdb");
    std::fs::copy(&backup_data, &dst_data)?;

    // Restore event log directory if present.
    let backup_event_log = dir.join("event-log");
    if backup_event_log.exists() {
        let dst_event_log = cfg.path.join("event-log");
        copy_dir_recursive(&backup_event_log, &dst_event_log)?;
    }

    // Open the restored store
    LmdbCanonicalStore::open(cfg)
}
