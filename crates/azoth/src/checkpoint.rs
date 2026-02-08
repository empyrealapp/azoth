//! Checkpoint system for periodic backups with pluggable storage backends
//!
//! This module provides a trait-based checkpoint system that allows you to
//! implement different storage backends (IPFS, S3, local filesystem, etc.)
//!
//! # Example
//!
//! ```no_run
//! use azoth::checkpoint::{CheckpointStorage, LocalStorage};
//! use azoth::prelude::*;
//! use std::path::PathBuf;
//!
//! # #[tokio::main]
//! # async fn main() -> Result<()> {
//! // Use local filesystem storage
//! let storage = LocalStorage::new(PathBuf::from("./checkpoints"));
//!
//! // Or implement your own storage backend
//! // struct S3Storage { /* ... */ }
//! // impl CheckpointStorage for S3Storage { /* ... */ }
//! # Ok(())
//! # }
//! ```

use crate::{AzothDb, AzothError, BackupManifest, BackupOptions, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Metadata about a checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Unique identifier for this checkpoint (CID, S3 key, filename, etc.)
    pub id: String,
    /// Timestamp when checkpoint was created
    pub timestamp: DateTime<Utc>,
    /// Sealed event ID at the time of checkpoint
    pub sealed_event_id: u64,
    /// Size of the checkpoint in bytes
    pub size_bytes: u64,
    /// Optional human-readable name
    pub name: Option<String>,
    /// Storage backend type (for restoration purposes)
    pub storage_type: String,
}

/// Trait for checkpoint storage backends
///
/// Implement this trait to add support for different storage systems
/// (IPFS, S3, Azure Blob, local filesystem, etc.)
#[async_trait]
pub trait CheckpointStorage: Send + Sync {
    /// Upload a checkpoint archive to storage
    ///
    /// Returns a unique identifier for the uploaded checkpoint
    async fn upload(&self, path: &Path, metadata: &CheckpointMetadata) -> Result<String>;

    /// Download a checkpoint archive from storage
    ///
    /// Downloads the checkpoint identified by `id` and saves it to `path`
    async fn download(&self, id: &str, path: &Path) -> Result<()>;

    /// Delete a checkpoint from storage (optional)
    async fn delete(&self, id: &str) -> Result<()> {
        let _ = id;
        Err(AzothError::Config(
            "Delete operation not supported by this storage backend".to_string(),
        ))
    }

    /// List all available checkpoints (optional)
    async fn list(&self) -> Result<Vec<CheckpointMetadata>> {
        Err(AzothError::Config(
            "List operation not supported by this storage backend".to_string(),
        ))
    }

    /// Get the storage type identifier
    fn storage_type(&self) -> &str;
}

/// Local filesystem storage backend
pub struct LocalStorage {
    base_path: PathBuf,
}

impl LocalStorage {
    /// Create a new local filesystem storage backend
    pub fn new(base_path: PathBuf) -> Self {
        Self { base_path }
    }
}

#[async_trait]
impl CheckpointStorage for LocalStorage {
    async fn upload(&self, path: &Path, metadata: &CheckpointMetadata) -> Result<String> {
        std::fs::create_dir_all(&self.base_path)?;

        let filename = format!(
            "{}-{}.tar",
            metadata.timestamp.format("%Y%m%d-%H%M%S"),
            &metadata.id
        );
        let dest_path = self.base_path.join(&filename);

        std::fs::copy(path, &dest_path)?;

        // Also save metadata
        let metadata_path = self.base_path.join(format!("{}.json", &filename));
        let metadata_json = serde_json::to_string_pretty(metadata)
            .map_err(|e| AzothError::Serialization(e.to_string()))?;
        std::fs::write(metadata_path, metadata_json)?;

        Ok(filename)
    }

    async fn download(&self, id: &str, path: &Path) -> Result<()> {
        let src_path = self.base_path.join(id);
        if !src_path.exists() {
            return Err(AzothError::NotFound(format!(
                "Checkpoint not found: {}",
                id
            )));
        }

        std::fs::copy(&src_path, path)?;
        Ok(())
    }

    async fn delete(&self, id: &str) -> Result<()> {
        let checkpoint_path = self.base_path.join(id);
        let metadata_path = self.base_path.join(format!("{}.json", id));

        if checkpoint_path.exists() {
            std::fs::remove_file(&checkpoint_path)?;
        }

        if metadata_path.exists() {
            std::fs::remove_file(&metadata_path)?;
        }

        Ok(())
    }

    async fn list(&self) -> Result<Vec<CheckpointMetadata>> {
        let mut checkpoints = Vec::new();

        if !self.base_path.exists() {
            return Ok(checkpoints);
        }

        for entry in std::fs::read_dir(&self.base_path)? {
            let entry = entry?;
            let path = entry.path();

            if path.extension().and_then(|s| s.to_str()) == Some("json") {
                let metadata_json = std::fs::read_to_string(&path)?;
                let metadata: CheckpointMetadata = serde_json::from_str(&metadata_json)
                    .map_err(|e| AzothError::Serialization(e.to_string()))?;
                checkpoints.push(metadata);
            }
        }

        // Sort by timestamp (newest first)
        checkpoints.sort_by(|a, b| b.timestamp.cmp(&a.timestamp));

        Ok(checkpoints)
    }

    fn storage_type(&self) -> &str {
        "local"
    }
}

/// Configuration for checkpoint creation
#[derive(Clone)]
pub struct CheckpointConfig {
    /// Backup options (encryption, compression)
    pub backup_options: BackupOptions,
    /// Temporary directory for creating checkpoints (defaults to system temp)
    pub temp_dir: Option<PathBuf>,
}

impl CheckpointConfig {
    /// Create new checkpoint config with defaults
    pub fn new() -> Self {
        Self {
            backup_options: BackupOptions::new(),
            temp_dir: None,
        }
    }

    /// Set encryption key
    pub fn with_encryption(mut self, key: crate::EncryptionKey) -> Self {
        self.backup_options = self.backup_options.with_encryption(key);
        self
    }

    /// Enable or disable compression
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.backup_options = self.backup_options.with_compression(enabled);
        self
    }

    /// Set compression level (0-9)
    pub fn with_compression_level(mut self, level: u32) -> Self {
        self.backup_options = self.backup_options.with_compression_level(level);
        self
    }

    /// Set temporary directory for checkpoints
    pub fn with_temp_dir(mut self, dir: PathBuf) -> Self {
        self.temp_dir = Some(dir);
        self
    }
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Checkpoint manager for creating and restoring checkpoints
pub struct CheckpointManager<S: CheckpointStorage> {
    db: Arc<AzothDb>,
    storage: Arc<S>,
    config: CheckpointConfig,
}

impl<S: CheckpointStorage> CheckpointManager<S> {
    /// Create a new checkpoint manager
    pub fn new(db: Arc<AzothDb>, storage: S, config: CheckpointConfig) -> Self {
        Self {
            db,
            storage: Arc::new(storage),
            config,
        }
    }

    /// Create a checkpoint and upload to storage
    ///
    /// Returns the checkpoint metadata including the storage identifier
    pub async fn create_checkpoint(&self) -> Result<CheckpointMetadata> {
        // Create temporary directory for backup
        let temp_dir = if let Some(ref dir) = self.config.temp_dir {
            std::fs::create_dir_all(dir)?;
            tempfile::tempdir_in(dir)?
        } else {
            tempfile::tempdir()?
        };

        tracing::info!("Creating checkpoint in {}", temp_dir.path().display());

        // Backup database to temp directory
        self.db
            .backup_with_options(temp_dir.path(), &self.config.backup_options)?;

        // Create a separate directory for the archive so we never tar "into" the directory
        // we're archiving (which can include the tar file itself and cause runaway growth).
        let archive_dir = if let Some(ref dir) = self.config.temp_dir {
            std::fs::create_dir_all(dir)?;
            tempfile::tempdir_in(dir)?
        } else {
            tempfile::tempdir()?
        };

        // Create tar archive
        let timestamp = Utc::now();
        let temp_id = format!("checkpoint-{}", timestamp.format("%Y%m%d-%H%M%S"));
        let archive_path = archive_dir.path().join(format!("{}.tar", &temp_id));

        // Create archive with all files in backup directory
        self.create_archive(temp_dir.path(), &archive_path)?;

        // Get sealed event ID from manifest
        let manifest_path = temp_dir.path().join("manifest.json");
        let manifest_json = std::fs::read_to_string(&manifest_path)?;
        let manifest: BackupManifest = serde_json::from_str(&manifest_json)
            .map_err(|e| AzothError::Serialization(e.to_string()))?;

        let sealed_event_id = manifest.sealed_event_id;
        let size_bytes = std::fs::metadata(&archive_path)?.len();

        // Create metadata
        let metadata = CheckpointMetadata {
            id: temp_id.clone(),
            timestamp,
            sealed_event_id,
            size_bytes,
            name: None,
            storage_type: self.storage.storage_type().to_string(),
        };

        // Upload to storage
        tracing::info!(
            "Uploading checkpoint to {} storage",
            self.storage.storage_type()
        );
        let storage_id = self.storage.upload(&archive_path, &metadata).await?;

        // Update metadata with storage ID
        let final_metadata = CheckpointMetadata {
            id: storage_id.clone(),
            ..metadata
        };

        tracing::info!(
            "Checkpoint uploaded successfully: id={}, size={} bytes",
            storage_id,
            size_bytes
        );

        Ok(final_metadata)
    }

    /// Restore from a checkpoint
    ///
    /// Downloads the checkpoint from storage and restores the database
    pub async fn restore_checkpoint(&self, checkpoint_id: &str, target_path: &Path) -> Result<()> {
        // Create temp directory for download
        let temp_dir = tempfile::tempdir()?;
        let archive_path = temp_dir.path().join("checkpoint.tar");

        tracing::info!("Downloading checkpoint: {}", checkpoint_id);

        // Download from storage
        self.storage.download(checkpoint_id, &archive_path).await?;

        // Extract archive
        let extract_dir = temp_dir.path().join("extracted");
        std::fs::create_dir_all(&extract_dir)?;

        let archive_file = File::open(&archive_path)?;
        let mut archive = tar::Archive::new(archive_file);
        archive.unpack(&extract_dir).map_err(AzothError::Io)?;

        // Restore database
        tracing::info!("Restoring database to {}", target_path.display());
        AzothDb::restore_with_options(extract_dir.as_path(), target_path, &self.config.backup_options)?;

        tracing::info!("Checkpoint restored successfully");
        Ok(())
    }

    /// List all available checkpoints
    pub async fn list_checkpoints(&self) -> Result<Vec<CheckpointMetadata>> {
        self.storage.list().await
    }

    /// Delete a checkpoint
    pub async fn delete_checkpoint(&self, checkpoint_id: &str) -> Result<()> {
        self.storage.delete(checkpoint_id).await
    }

    /// Create a final checkpoint on shutdown
    ///
    /// This creates a checkpoint with a "shutdown" marker in the metadata.
    /// Call this before closing the database to ensure a recovery point exists.
    pub async fn shutdown_checkpoint(&self) -> Result<CheckpointMetadata> {
        tracing::info!("Creating shutdown checkpoint...");

        let mut metadata = self.create_checkpoint().await?;

        // Mark this as a shutdown checkpoint
        metadata.name = Some("shutdown".to_string());

        tracing::info!("Shutdown checkpoint created successfully: {}", metadata.id);
        Ok(metadata)
    }

    fn create_archive(&self, backup_dir: &Path, output_path: &Path) -> Result<()> {
        let output_file = File::create(output_path)?;
        let mut tar_builder = tar::Builder::new(output_file);

        // Add all files from backup directory
        tar_builder
            .append_dir_all(".", backup_dir)
            .map_err(AzothError::Io)?;

        tar_builder.finish().map_err(AzothError::Io)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_local_storage() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let storage = LocalStorage::new(temp_dir.path().to_path_buf());

        // Create a test file
        let test_file = temp_dir.path().join("test.tar");
        std::fs::write(&test_file, b"test data")?;

        let metadata = CheckpointMetadata {
            id: "test-checkpoint".to_string(),
            timestamp: Utc::now(),
            sealed_event_id: 42,
            size_bytes: 9,
            name: Some("Test Checkpoint".to_string()),
            storage_type: "local".to_string(),
        };

        // Upload
        let id = storage.upload(&test_file, &metadata).await?;
        assert!(id.ends_with(".tar"));

        // List
        let checkpoints = storage.list().await?;
        assert_eq!(checkpoints.len(), 1);
        assert_eq!(checkpoints[0].sealed_event_id, 42);

        // Download
        let download_path = temp_dir.path().join("downloaded.tar");
        storage.download(&id, &download_path).await?;

        let downloaded_data = std::fs::read(&download_path)?;
        assert_eq!(downloaded_data, b"test data");

        // Delete
        storage.delete(&id).await?;
        let checkpoints = storage.list().await?;
        assert_eq!(checkpoints.len(), 0);

        Ok(())
    }
}
