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
/// Uses simple file copy for now
/// Production version would use LMDB's env.copy() API
pub fn backup_to(store: &LmdbCanonicalStore, dir: &Path) -> Result<BackupInfo> {
    // Create backup directory
    std::fs::create_dir_all(dir)?;

    // Get sealed event ID (should be sealed before backup)
    let meta = store.meta()?;
    let sealed_event_id = meta
        .sealed_event_id
        .ok_or_else(|| AzothError::Backup("Store must be sealed before backup".into()))?;

    // Copy LMDB data files
    // Note: This is simplified - production would use env.copy_to_path()
    let src_dir = &store.config_path;
    let src_data = src_dir.join("data.mdb");

    if src_data.exists() {
        let backup_data = dir.join("data.mdb");
        std::fs::copy(&src_data, &backup_data)?;

        // Get size
        let size_bytes = std::fs::metadata(&backup_data)?.len();

        Ok(BackupInfo {
            sealed_event_id,
            path: dir.display().to_string(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            size_bytes,
        })
    } else {
        Err(AzothError::Backup("Source data file not found".into()))
    }
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
