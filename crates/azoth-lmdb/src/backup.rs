use azoth_core::{
    error::{AzothError, Result},
    traits::CanonicalStore,
    types::BackupInfo,
    CanonicalConfig,
};
use std::path::Path;

use crate::store::LmdbCanonicalStore;

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

    // Get sealed event ID (enforces that store is sealed = consistent state)
    let meta = store.meta()?;
    let sealed_event_id = meta
        .sealed_event_id
        .ok_or_else(|| AzothError::Backup("Store must be sealed before backup".into()))?;

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

    // Open the restored store
    LmdbCanonicalStore::open(cfg)
}
