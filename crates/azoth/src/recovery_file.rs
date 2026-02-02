//! Local recovery file management
//!
//! Creates `.latest_backup` JSON files for easy restoration in dev/test environments.
//!
//! Recovery files provide a simple way to track the most recent backup without
//! needing to query external systems like IPFS or blockchain registries.

use crate::checkpoint::CheckpointMetadata;
use crate::{AzothError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Recovery file format
///
/// This file is written locally alongside the database and contains
/// information needed to restore from the most recent backup.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecoveryFile {
    /// Main backup ID (CID, S3 key, filename, etc.)
    pub main_backup_id: String,

    /// ISO 8601 timestamp when backup was created
    pub timestamp: String,

    /// Sealed event ID at time of backup
    pub sealed_event_id: u64,

    /// Total backup size in bytes
    pub size_bytes: u64,

    /// Storage type (ipfs, s3, local, etc.)
    pub storage_type: String,

    /// Optional per-capability or per-database backup IDs
    ///
    /// For multi-database systems, this can track individual database backups.
    /// Key = database/capability name, Value = backup ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub component_backups: Option<HashMap<String, String>>,

    /// Optional mnemonic fingerprint for validation
    ///
    /// Can be used to verify the recovery file matches expected encryption key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mnemonic_fingerprint: Option<String>,

    /// Optional human-readable name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Recovery file manager
///
/// Manages reading and writing of `.latest_backup` JSON files.
///
/// # Example
///
/// ```no_run
/// use azoth::recovery_file::RecoveryFileManager;
/// use azoth::checkpoint::CheckpointMetadata;
/// use std::path::PathBuf;
/// use chrono::Utc;
///
/// let manager = RecoveryFileManager::new(PathBuf::from("/data"));
///
/// // Write recovery file after creating backup
/// let metadata = CheckpointMetadata {
///     id: "QmXyz...".to_string(),
///     timestamp: Utc::now(),
///     sealed_event_id: 12345,
///     size_bytes: 1_000_000,
///     name: None,
///     storage_type: "ipfs".to_string(),
/// };
/// manager.write(&metadata).unwrap();
///
/// // Read recovery file for restoration
/// if manager.exists() {
///     let recovery = manager.read().unwrap();
///     println!("Latest backup: {}", recovery.main_backup_id);
/// }
/// ```
pub struct RecoveryFileManager {
    base_path: PathBuf,
    filename: String,
}

impl RecoveryFileManager {
    /// Create a new recovery file manager
    ///
    /// # Arguments
    ///
    /// * `base_path` - Directory where recovery file will be stored
    pub fn new(base_path: PathBuf) -> Self {
        Self {
            base_path,
            filename: ".latest_backup".to_string(),
        }
    }

    /// Create with custom filename
    ///
    /// Useful if you want to track multiple backup streams or use a different naming convention.
    pub fn with_filename(base_path: PathBuf, filename: String) -> Self {
        Self {
            base_path,
            filename,
        }
    }

    /// Get the full path to the recovery file
    fn file_path(&self) -> PathBuf {
        self.base_path.join(&self.filename)
    }

    /// Write recovery file from checkpoint metadata
    ///
    /// Creates or overwrites the recovery file with the latest backup information.
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - Directory doesn't exist and can't be created
    /// - File can't be written
    /// - Serialization fails
    pub fn write(&self, metadata: &CheckpointMetadata) -> Result<()> {
        // Ensure directory exists
        std::fs::create_dir_all(&self.base_path)?;

        let recovery = RecoveryFile {
            main_backup_id: metadata.id.clone(),
            timestamp: metadata.timestamp.to_string(),
            sealed_event_id: metadata.sealed_event_id,
            size_bytes: metadata.size_bytes,
            storage_type: metadata.storage_type.clone(),
            component_backups: None,
            mnemonic_fingerprint: None,
            name: metadata.name.clone(),
        };

        let path = self.file_path();
        let json = serde_json::to_string_pretty(&recovery)
            .map_err(|e| AzothError::Serialization(e.to_string()))?;
        std::fs::write(&path, json)?;

        tracing::info!("Wrote recovery file to {}", path.display());
        Ok(())
    }

    /// Write recovery file with custom component backups
    ///
    /// Useful for multi-database systems where you want to track individual database backups.
    pub fn write_with_components(
        &self,
        metadata: &CheckpointMetadata,
        component_backups: HashMap<String, String>,
    ) -> Result<()> {
        std::fs::create_dir_all(&self.base_path)?;

        let recovery = RecoveryFile {
            main_backup_id: metadata.id.clone(),
            timestamp: metadata.timestamp.to_string(),
            sealed_event_id: metadata.sealed_event_id,
            size_bytes: metadata.size_bytes,
            storage_type: metadata.storage_type.clone(),
            component_backups: Some(component_backups),
            mnemonic_fingerprint: None,
            name: metadata.name.clone(),
        };

        let path = self.file_path();
        let json = serde_json::to_string_pretty(&recovery)
            .map_err(|e| AzothError::Serialization(e.to_string()))?;
        std::fs::write(&path, json)?;

        tracing::info!(
            "Wrote recovery file with {} components to {}",
            recovery
                .component_backups
                .as_ref()
                .map(|c| c.len())
                .unwrap_or(0),
            path.display()
        );
        Ok(())
    }

    /// Read recovery file
    ///
    /// # Errors
    ///
    /// Returns error if:
    /// - File doesn't exist
    /// - File can't be read
    /// - JSON is malformed
    pub fn read(&self) -> Result<RecoveryFile> {
        let path = self.file_path();
        let json = std::fs::read_to_string(&path)?;
        let recovery: RecoveryFile =
            serde_json::from_str(&json).map_err(|e| AzothError::Serialization(e.to_string()))?;
        Ok(recovery)
    }

    /// Check if recovery file exists
    pub fn exists(&self) -> bool {
        self.file_path().exists()
    }

    /// Delete recovery file
    ///
    /// Returns `Ok(())` even if file doesn't exist.
    pub fn delete(&self) -> Result<()> {
        let path = self.file_path();
        if path.exists() {
            std::fs::remove_file(&path)?;
            tracing::info!("Deleted recovery file at {}", path.display());
        }
        Ok(())
    }

    /// Get recovery file path
    pub fn path(&self) -> &Path {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use tempfile::TempDir;

    #[test]
    fn test_write_and_read() {
        let temp_dir = TempDir::new().unwrap();
        let manager = RecoveryFileManager::new(temp_dir.path().to_path_buf());

        let metadata = CheckpointMetadata {
            id: "QmTest123".to_string(),
            timestamp: Utc::now(),
            sealed_event_id: 12345,
            size_bytes: 1_000_000,
            name: Some("test-backup".to_string()),
            storage_type: "ipfs".to_string(),
        };

        // Write
        manager.write(&metadata).unwrap();
        assert!(manager.exists());

        // Read
        let recovery = manager.read().unwrap();
        assert_eq!(recovery.main_backup_id, "QmTest123");
        assert_eq!(recovery.sealed_event_id, 12345);
        assert_eq!(recovery.size_bytes, 1_000_000);
        assert_eq!(recovery.name, Some("test-backup".to_string()));
    }

    #[test]
    fn test_write_with_components() {
        let temp_dir = TempDir::new().unwrap();
        let manager = RecoveryFileManager::new(temp_dir.path().to_path_buf());

        let metadata = CheckpointMetadata {
            id: "QmMain".to_string(),
            timestamp: Utc::now(),
            sealed_event_id: 100,
            size_bytes: 2_000_000,
            name: None,
            storage_type: "ipfs".to_string(),
        };

        let mut components = HashMap::new();
        components.insert("database1".to_string(), "QmDb1".to_string());
        components.insert("database2".to_string(), "QmDb2".to_string());

        manager
            .write_with_components(&metadata, components)
            .unwrap();

        let recovery = manager.read().unwrap();
        assert_eq!(recovery.component_backups.as_ref().unwrap().len(), 2);
        assert_eq!(
            recovery
                .component_backups
                .as_ref()
                .unwrap()
                .get("database1"),
            Some(&"QmDb1".to_string())
        );
    }

    #[test]
    fn test_custom_filename() {
        let temp_dir = TempDir::new().unwrap();
        let manager = RecoveryFileManager::with_filename(
            temp_dir.path().to_path_buf(),
            "custom_recovery.json".to_string(),
        );

        let metadata = CheckpointMetadata {
            id: "QmCustom".to_string(),
            timestamp: Utc::now(),
            sealed_event_id: 999,
            size_bytes: 500_000,
            name: None,
            storage_type: "local".to_string(),
        };

        manager.write(&metadata).unwrap();
        assert!(temp_dir.path().join("custom_recovery.json").exists());
    }

    #[test]
    fn test_delete() {
        let temp_dir = TempDir::new().unwrap();
        let manager = RecoveryFileManager::new(temp_dir.path().to_path_buf());

        let metadata = CheckpointMetadata {
            id: "QmDelete".to_string(),
            timestamp: Utc::now(),
            sealed_event_id: 555,
            size_bytes: 100_000,
            name: None,
            storage_type: "local".to_string(),
        };

        manager.write(&metadata).unwrap();
        assert!(manager.exists());

        manager.delete().unwrap();
        assert!(!manager.exists());

        // Delete again should be OK
        manager.delete().unwrap();
    }
}
