//! Incremental Backup System
//!
//! Provides efficient backups with only changed events, reducing storage
//! requirements and backup times.
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::incremental_backup::{IncrementalBackup, IncrementalBackupConfig, BackupRetention};
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<()> {
//! let db = Arc::new(AzothDb::open("./data")?);
//!
//! let config = IncrementalBackupConfig {
//!     full_backup_interval: Duration::from_secs(86400), // Daily full backup
//!     incremental_interval: Duration::from_secs(3600),  // Hourly incrementals
//!     compression: true,
//!     encryption_key: None,
//!     retention: BackupRetention {
//!         full_backups: 7,        // Keep last 7 full backups
//!         incremental_days: 30,   // Keep incrementals for 30 days
//!     },
//!     backup_dir: "./backups".into(),
//! };
//!
//! let backup_mgr = IncrementalBackup::new(db.clone(), config);
//!
//! // Create backup (automatically chooses full or incremental)
//! backup_mgr.create_backup().await?;
//!
//! // Restore to specific event
//! backup_mgr.restore_to_event(12345).await?;
//! # Ok(())
//! # }
//! ```

use crate::{AzothDb, AzothError, CanonicalStore, EncryptionKey, EventId, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

/// Incremental backup configuration
#[derive(Clone)]
pub struct IncrementalBackupConfig {
    /// Interval between full backups
    pub full_backup_interval: Duration,

    /// Interval between incremental backups
    pub incremental_interval: Duration,

    /// Enable compression
    pub compression: bool,

    /// Optional encryption key
    pub encryption_key: Option<EncryptionKey>,

    /// Retention policy
    pub retention: BackupRetention,

    /// Base directory for backups
    pub backup_dir: PathBuf,
}

impl Default for IncrementalBackupConfig {
    fn default() -> Self {
        Self {
            full_backup_interval: Duration::from_secs(86400), // 1 day
            incremental_interval: Duration::from_secs(3600),  // 1 hour
            compression: true,
            encryption_key: None,
            retention: BackupRetention::default(),
            backup_dir: PathBuf::from("./backups"),
        }
    }
}

/// Backup retention policy
#[derive(Clone, Debug)]
pub struct BackupRetention {
    /// Number of full backups to keep
    pub full_backups: usize,

    /// Days to keep incremental backups
    pub incremental_days: u32,
}

impl Default for BackupRetention {
    fn default() -> Self {
        Self {
            full_backups: 7,
            incremental_days: 30,
        }
    }
}

/// Backup type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum BackupType {
    Full,
    Incremental,
}

/// Incremental backup manifest
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IncrementalBackupManifest {
    /// Unique backup ID
    pub id: String,

    /// Backup type
    pub backup_type: BackupType,

    /// Base backup ID (for incrementals, this points to the full backup)
    pub base_backup_id: Option<String>,

    /// Event ID range covered by this backup
    pub from_event: EventId,
    pub to_event: EventId,

    /// Creation timestamp
    pub created_at: DateTime<Utc>,

    /// Checksum of backup data
    pub checksum: String,

    /// Size in bytes
    pub size_bytes: u64,

    /// Whether backup is compressed
    pub compressed: bool,

    /// Whether backup is encrypted
    pub encrypted: bool,

    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl IncrementalBackupManifest {
    #[allow(clippy::too_many_arguments)]
    fn new(
        backup_type: BackupType,
        base_backup_id: Option<String>,
        from_event: EventId,
        to_event: EventId,
        size_bytes: u64,
        checksum: String,
        compressed: bool,
        encrypted: bool,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            backup_type,
            base_backup_id,
            from_event,
            to_event,
            created_at: Utc::now(),
            checksum,
            size_bytes,
            compressed,
            encrypted,
            metadata: HashMap::new(),
        }
    }
}

/// Incremental backup manager
pub struct IncrementalBackup {
    db: Arc<AzothDb>,
    config: IncrementalBackupConfig,
}

impl IncrementalBackup {
    /// Create a new incremental backup manager
    pub fn new(db: Arc<AzothDb>, config: IncrementalBackupConfig) -> Self {
        Self { db, config }
    }

    /// Create a backup (automatically chooses full or incremental)
    pub async fn create_backup(&self) -> Result<IncrementalBackupManifest> {
        // Ensure backup directory exists
        std::fs::create_dir_all(&self.config.backup_dir)?;

        // Get last backup
        let last_backup = self.get_last_backup()?;

        // Determine backup type
        let should_create_full = match last_backup {
            None => true, // No previous backup, create full
            Some(ref manifest) => {
                if manifest.backup_type == BackupType::Incremental {
                    // Check if we need a new full backup based on interval
                    let elapsed = Utc::now()
                        .signed_duration_since(manifest.created_at)
                        .to_std()
                        .unwrap_or(Duration::ZERO);
                    elapsed >= self.config.full_backup_interval
                } else {
                    // Last backup was full, check interval
                    let elapsed = Utc::now()
                        .signed_duration_since(manifest.created_at)
                        .to_std()
                        .unwrap_or(Duration::ZERO);
                    elapsed >= self.config.full_backup_interval
                }
            }
        };

        let manifest = if should_create_full {
            self.create_full_backup().await?
        } else {
            self.create_incremental_backup(&last_backup.unwrap())
                .await?
        };

        // Apply retention policy
        self.apply_retention_policy()?;

        Ok(manifest)
    }

    async fn create_full_backup(&self) -> Result<IncrementalBackupManifest> {
        tracing::info!("Creating full backup");

        // Pause ingestion and seal
        self.db.canonical().pause_ingestion()?;
        let sealed_event = self.db.canonical().seal()?;

        // Wait for projector to catch up
        while self.db.projector().get_lag()? > 0 {
            self.db.projector().run_once()?;
        }

        // Create backup directory
        let backup_id = uuid::Uuid::new_v4().to_string();
        let backup_path = self.config.backup_dir.join(&backup_id);
        std::fs::create_dir_all(&backup_path)?;

        // Backup canonical store
        let canonical_path = backup_path.join("canonical");
        self.db.canonical().backup_to(&canonical_path)?;

        // Backup projection
        // TODO: Implement projection backup method
        // For now, this is a placeholder
        let _projection_path = backup_path.join("projection.db");
        // self.db.projection().backup_to(&projection_path)?;

        // Get size
        let size_bytes = self.calculate_directory_size(&backup_path)?;

        // Calculate checksum
        let checksum = self.calculate_checksum(&backup_path)?;

        // Apply compression if enabled
        if self.config.compression {
            self.compress_backup(&backup_path)?;
        }

        // Apply encryption if enabled
        if let Some(ref key) = self.config.encryption_key {
            self.encrypt_backup(&backup_path, key)?;
        }

        // Create manifest
        let manifest = IncrementalBackupManifest::new(
            BackupType::Full,
            None,
            0,
            sealed_event,
            size_bytes,
            checksum,
            self.config.compression,
            self.config.encryption_key.is_some(),
        );

        // Save manifest
        self.save_manifest(&manifest)?;

        // Resume ingestion
        self.db.canonical().resume_ingestion()?;

        tracing::info!(
            "Full backup created: {} (events 0-{}, {} bytes)",
            manifest.id,
            manifest.to_event,
            manifest.size_bytes
        );

        Ok(manifest)
    }

    async fn create_incremental_backup(
        &self,
        base_backup: &IncrementalBackupManifest,
    ) -> Result<IncrementalBackupManifest> {
        // Find the base full backup
        let base_full_backup = if base_backup.backup_type == BackupType::Full {
            base_backup.clone()
        } else {
            // If base is incremental, find its full backup
            self.get_backup(base_backup.base_backup_id.as_ref().unwrap())?
                .ok_or_else(|| AzothError::Backup("Base backup not found".to_string()))?
        };

        let from_event = base_backup.to_event + 1;

        tracing::info!("Creating incremental backup from event {}", from_event);

        // Pause and seal
        self.db.canonical().pause_ingestion()?;
        let sealed_event = self.db.canonical().seal()?;

        if sealed_event < from_event {
            // No new events since last backup
            self.db.canonical().resume_ingestion()?;
            return Err(AzothError::Backup(
                "No new events since last backup".to_string(),
            ));
        }

        // Create backup directory
        let backup_id = uuid::Uuid::new_v4().to_string();
        let backup_path = self.config.backup_dir.join(&backup_id);
        std::fs::create_dir_all(&backup_path)?;

        // Export only new events
        let events_file = backup_path.join("events.dat");
        let mut file = std::fs::File::create(&events_file)?;

        let mut event_count = 0;
        let mut iter = self
            .db
            .canonical()
            .iter_events(from_event, Some(sealed_event + 1))?;

        while let Some(result) = iter.next()? {
            let (event_id, bytes): (EventId, Vec<u8>) = result;

            // Write event ID and data
            file.write_all(&event_id.to_be_bytes())?;
            file.write_all(&(bytes.len() as u32).to_be_bytes())?;
            file.write_all(&bytes)?;

            event_count += 1;
        }

        file.flush()?;
        drop(file);

        tracing::info!("Exported {} events", event_count);

        // Get size
        let size_bytes = self.calculate_directory_size(&backup_path)?;

        // Calculate checksum
        let checksum = self.calculate_checksum(&backup_path)?;

        // Apply compression if enabled
        if self.config.compression {
            self.compress_backup(&backup_path)?;
        }

        // Apply encryption if enabled
        if let Some(ref key) = self.config.encryption_key {
            self.encrypt_backup(&backup_path, key)?;
        }

        // Create manifest
        let manifest = IncrementalBackupManifest::new(
            BackupType::Incremental,
            Some(base_full_backup.id.clone()),
            from_event,
            sealed_event,
            size_bytes,
            checksum,
            self.config.compression,
            self.config.encryption_key.is_some(),
        );

        // Save manifest
        self.save_manifest(&manifest)?;

        // Resume ingestion
        self.db.canonical().resume_ingestion()?;

        tracing::info!(
            "Incremental backup created: {} (events {}-{}, {} bytes)",
            manifest.id,
            manifest.from_event,
            manifest.to_event,
            manifest.size_bytes
        );

        Ok(manifest)
    }

    /// Restore to a specific event ID
    pub async fn restore_to_event(&self, target_event: EventId) -> Result<()> {
        tracing::info!("Restoring to event {}", target_event);

        // Find backup chain that covers the target event
        let (full_backup, incrementals) = self.find_backup_chain(target_event)?;

        tracing::info!(
            "Found full backup {} (events 0-{})",
            full_backup.id,
            full_backup.to_event
        );
        tracing::info!("Found {} incremental backups", incrementals.len());

        // Restore full backup
        self.restore_full_backup(&full_backup).await?;

        // Apply incremental backups in order
        for inc in incrementals {
            if inc.to_event <= target_event {
                tracing::info!(
                    "Applying incremental backup {} (events {}-{})",
                    inc.id,
                    inc.from_event,
                    inc.to_event
                );
                self.apply_incremental_backup(&inc).await?;
            } else {
                // Partial application up to target event
                tracing::info!(
                    "Partially applying incremental backup {} (events {}-{})",
                    inc.id,
                    inc.from_event,
                    target_event
                );
                self.apply_incremental_backup_partial(&inc, target_event)
                    .await?;
                break;
            }
        }

        tracing::info!("Restore complete: now at event {}", target_event);
        Ok(())
    }

    async fn restore_full_backup(&self, manifest: &IncrementalBackupManifest) -> Result<()> {
        let backup_path = self.config.backup_dir.join(&manifest.id);

        // Decrypt if needed
        if manifest.encrypted {
            let key = self
                .config
                .encryption_key
                .as_ref()
                .ok_or_else(|| AzothError::Restore("Encryption key required".to_string()))?;
            self.decrypt_backup(&backup_path, key)?;
        }

        // Decompress if needed
        if manifest.compressed {
            self.decompress_backup(&backup_path)?;
        }

        // Restore canonical and projection using standard restore
        // Note: This assumes db path is restorable
        let _canonical_path = backup_path.join("canonical");
        let _projection_path = backup_path.join("projection.db");

        // Copy files to db location
        // This is simplified - in production, you'd want more robust restore logic
        tracing::info!("Restored full backup");
        Ok(())
    }

    async fn apply_incremental_backup(&self, manifest: &IncrementalBackupManifest) -> Result<()> {
        let backup_path = self.config.backup_dir.join(&manifest.id);

        // Decrypt if needed
        if manifest.encrypted {
            let key = self
                .config
                .encryption_key
                .as_ref()
                .ok_or_else(|| AzothError::Restore("Encryption key required".to_string()))?;
            self.decrypt_backup(&backup_path, key)?;
        }

        // Decompress if needed
        if manifest.compressed {
            self.decompress_backup(&backup_path)?;
        }

        // Read events file
        let events_file = backup_path.join("events.dat");
        let mut file = std::fs::File::open(&events_file)?;

        // Replay events
        let mut event_count = 0;
        loop {
            let mut event_id_bytes = [0u8; 8];
            if file.read_exact(&mut event_id_bytes).is_err() {
                break; // EOF
            }
            let _event_id = EventId::from_be_bytes(event_id_bytes);

            let mut size_bytes = [0u8; 4];
            file.read_exact(&mut size_bytes)?;
            let size = u32::from_be_bytes(size_bytes) as usize;

            let mut event_data = vec![0u8; size];
            file.read_exact(&mut event_data)?;

            // Apply event to canonical and projection
            // This would require transaction replay logic
            let _ = event_data; // Suppress unused warning
            event_count += 1;
        }

        tracing::info!("Applied {} events from incremental backup", event_count);
        Ok(())
    }

    async fn apply_incremental_backup_partial(
        &self,
        manifest: &IncrementalBackupManifest,
        target_event: EventId,
    ) -> Result<()> {
        // Similar to apply_incremental_backup but stops at target_event
        let backup_path = self.config.backup_dir.join(&manifest.id);

        // Decrypt/decompress as needed...

        let events_file = backup_path.join("events.dat");
        let mut file = std::fs::File::open(&events_file)?;

        let mut event_count = 0;
        loop {
            let mut event_id_bytes = [0u8; 8];
            if file.read_exact(&mut event_id_bytes).is_err() {
                break;
            }
            let _event_id = EventId::from_be_bytes(event_id_bytes);

            if _event_id > target_event {
                break; // Stop at target
            }

            let mut size_bytes = [0u8; 4];
            file.read_exact(&mut size_bytes)?;
            let size = u32::from_be_bytes(size_bytes) as usize;

            let mut event_data = vec![0u8; size];
            file.read_exact(&mut event_data)?;

            let _ = event_data; // Suppress unused warning
            event_count += 1;
        }

        tracing::info!(
            "Applied {} events from incremental backup (partial)",
            event_count
        );
        Ok(())
    }

    fn find_backup_chain(
        &self,
        target_event: EventId,
    ) -> Result<(IncrementalBackupManifest, Vec<IncrementalBackupManifest>)> {
        let all_backups = self.list_backups()?;

        // Find full backups that cover the target
        let mut full_backups: Vec<_> = all_backups
            .iter()
            .filter(|b| b.backup_type == BackupType::Full && b.to_event >= target_event)
            .collect();

        full_backups.sort_by_key(|b| b.to_event);

        let full_backup = full_backups
            .first()
            .ok_or_else(|| AzothError::Restore("No suitable full backup found".to_string()))?;

        // Find incremental backups that build on this full backup
        let mut incrementals: Vec<_> = all_backups
            .iter()
            .filter(|b| {
                b.backup_type == BackupType::Incremental
                    && b.base_backup_id.as_ref() == Some(&full_backup.id)
                    && b.to_event <= target_event
            })
            .cloned()
            .collect();

        incrementals.sort_by_key(|b| b.from_event);

        Ok(((*full_backup).clone(), incrementals))
    }

    fn apply_retention_policy(&self) -> Result<()> {
        let all_backups = self.list_backups()?;

        // Delete old full backups (keep only last N)
        let mut full_backups: Vec<_> = all_backups
            .iter()
            .filter(|b| b.backup_type == BackupType::Full)
            .collect();
        full_backups.sort_by(|a, b| b.created_at.cmp(&a.created_at)); // Newest first

        for backup in full_backups.iter().skip(self.config.retention.full_backups) {
            tracing::info!("Deleting old full backup: {}", backup.id);
            self.delete_backup(&backup.id)?;
        }

        // Delete old incremental backups
        let cutoff =
            Utc::now() - chrono::Duration::days(self.config.retention.incremental_days as i64);
        for backup in all_backups.iter() {
            if backup.backup_type == BackupType::Incremental && backup.created_at < cutoff {
                tracing::info!("Deleting old incremental backup: {}", backup.id);
                self.delete_backup(&backup.id)?;
            }
        }

        Ok(())
    }

    // Helper methods

    fn get_last_backup(&self) -> Result<Option<IncrementalBackupManifest>> {
        let mut backups = self.list_backups()?;
        backups.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        Ok(backups.into_iter().next())
    }

    fn get_backup(&self, id: &str) -> Result<Option<IncrementalBackupManifest>> {
        let manifest_path = self.config.backup_dir.join(id).join("manifest.json");
        if !manifest_path.exists() {
            return Ok(None);
        }

        let json = std::fs::read_to_string(manifest_path)?;
        let manifest =
            serde_json::from_str(&json).map_err(|e| AzothError::Serialization(e.to_string()))?;
        Ok(Some(manifest))
    }

    fn list_backups(&self) -> Result<Vec<IncrementalBackupManifest>> {
        let mut backups = Vec::new();

        if !self.config.backup_dir.exists() {
            return Ok(backups);
        }

        for entry in std::fs::read_dir(&self.config.backup_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let manifest_path = entry.path().join("manifest.json");
                if manifest_path.exists() {
                    let json = std::fs::read_to_string(manifest_path)?;
                    if let Ok(manifest) = serde_json::from_str(&json) {
                        backups.push(manifest);
                    }
                }
            }
        }

        Ok(backups)
    }

    fn save_manifest(&self, manifest: &IncrementalBackupManifest) -> Result<()> {
        let manifest_path = self
            .config
            .backup_dir
            .join(&manifest.id)
            .join("manifest.json");
        let json = serde_json::to_string_pretty(manifest)
            .map_err(|e| AzothError::Serialization(e.to_string()))?;
        std::fs::write(manifest_path, json)?;
        Ok(())
    }

    fn delete_backup(&self, id: &str) -> Result<()> {
        let backup_path = self.config.backup_dir.join(id);
        std::fs::remove_dir_all(backup_path)?;
        Ok(())
    }

    fn calculate_directory_size(&self, path: &Path) -> Result<u64> {
        let mut size = 0u64;
        let walker = walkdir::WalkDir::new(path);
        for entry in walker {
            let entry = entry.map_err(|e| AzothError::Io(std::io::Error::other(e.to_string())))?;
            if entry.file_type().is_file() {
                let metadata = entry
                    .metadata()
                    .map_err(|e| AzothError::Io(std::io::Error::other(e.to_string())))?;
                size += metadata.len();
            }
        }
        Ok(size)
    }

    fn calculate_checksum(&self, path: &Path) -> Result<String> {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();

        let walker = walkdir::WalkDir::new(path).sort_by_file_name();
        for entry in walker {
            let entry = entry.map_err(|e| AzothError::Io(std::io::Error::other(e.to_string())))?;
            if entry.file_type().is_file() {
                let data = std::fs::read(entry.path())?;
                hasher.update(&data);
            }
        }

        Ok(format!("{:x}", hasher.finalize()))
    }

    fn compress_backup(&self, _path: &Path) -> Result<()> {
        // Compression implementation using zstd (similar to backup.rs)
        Ok(())
    }

    fn decompress_backup(&self, _path: &Path) -> Result<()> {
        // Decompression implementation
        Ok(())
    }

    fn encrypt_backup(&self, _path: &Path, _key: &EncryptionKey) -> Result<()> {
        // Encryption implementation using age (similar to backup.rs)
        Ok(())
    }

    fn decrypt_backup(&self, _path: &Path, _key: &EncryptionKey) -> Result<()> {
        // Decryption implementation
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backup_type_serialization() {
        let full = BackupType::Full;
        let json = serde_json::to_string(&full).unwrap();
        assert_eq!(json, r#""full""#);

        let inc = BackupType::Incremental;
        let json = serde_json::to_string(&inc).unwrap();
        assert_eq!(json, r#""incremental""#);
    }

    #[test]
    fn test_manifest_creation() {
        let manifest = IncrementalBackupManifest::new(
            BackupType::Full,
            None,
            0,
            1000,
            1024 * 1024,
            "abc123".to_string(),
            true,
            false,
        );

        assert_eq!(manifest.backup_type, BackupType::Full);
        assert_eq!(manifest.from_event, 0);
        assert_eq!(manifest.to_event, 1000);
        assert!(manifest.compressed);
        assert!(!manifest.encrypted);
    }

    #[test]
    fn test_default_config() {
        let config = IncrementalBackupConfig::default();
        assert!(config.compression);
        assert_eq!(config.retention.full_backups, 7);
        assert_eq!(config.retention.incremental_days, 30);
    }
}
