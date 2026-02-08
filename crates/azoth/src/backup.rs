//! Backup and restore with optional encryption and compression
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::backup::{BackupOptions, EncryptionKey};
//!
//! # fn main() -> Result<()> {
//! let db = AzothDb::open("./data")?;
//!
//! // Backup with encryption and compression
//! let key = EncryptionKey::generate();
//! let options = BackupOptions::new()
//!     .with_encryption(key)
//!     .with_compression(true);
//!
//! db.backup_with_options("./backup", &options)?;
//!
//! // Restore
//! let restored = AzothDb::restore_with_options("./backup", "./restored", &options)?;
//! # Ok(())
//! # }
//! ```

use crate::{AzothDb, AzothError, BackupManifest, ProjectionStore, Result};
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

/// Encryption key for backup encryption using age
///
/// Uses the age encryption format for simplicity and security
#[derive(Clone)]
pub struct EncryptionKey {
    identity: age::x25519::Identity,
}

impl EncryptionKey {
    /// Generate a new random encryption key
    pub fn generate() -> Self {
        let identity = age::x25519::Identity::generate();
        Self { identity }
    }

    /// Get the age identity string (secret key)
    ///
    /// Warning: This exposes the secret key. Store it securely.
    pub fn to_identity_string(&self) -> String {
        use age::secrecy::ExposeSecret;
        self.identity.to_string().expose_secret().to_string()
    }

    /// Get the public recipient key
    pub fn to_recipient(&self) -> age::x25519::Recipient {
        self.identity.to_public()
    }

    /// Get the public recipient key as string
    pub fn to_recipient_string(&self) -> String {
        self.identity.to_public().to_string()
    }

    /// Create from recipient string (for encryption only)
    pub fn from_recipient_str(s: &str) -> Result<Self> {
        // This creates a dummy identity that can only be used for the recipient
        // We'll store the recipient in the identity field as a workaround
        let _recipient = s
            .parse::<age::x25519::Recipient>()
            .map_err(|e| AzothError::Config(format!("Invalid age recipient: {}", e)))?;

        // For encryption-only use, we generate a dummy identity and only use the recipient
        // This is a limitation - ideally we'd have separate types for encrypt vs decrypt keys
        // For now, just return error - users should use from_str for full keys
        Err(AzothError::Config(
            "Use from_str() with full identity for encryption/decryption".into(),
        ))
    }
}

impl FromStr for EncryptionKey {
    type Err = AzothError;

    fn from_str(s: &str) -> Result<Self> {
        let identity = s
            .parse::<age::x25519::Identity>()
            .map_err(|e| AzothError::Config(format!("Invalid age identity: {}", e)))?;
        Ok(Self { identity })
    }
}

/// Backup options
#[derive(Clone, Default)]
pub struct BackupOptions {
    /// Optional encryption key
    pub encryption: Option<EncryptionKey>,

    /// Enable compression (gzip)
    pub compression: bool,

    /// Compression level (0-9, default 6)
    pub compression_level: u32,
}

impl BackupOptions {
    /// Create new backup options with defaults
    pub fn new() -> Self {
        Self {
            encryption: None,
            compression: false,
            compression_level: 6,
        }
    }

    /// Enable encryption with the given key
    pub fn with_encryption(mut self, key: EncryptionKey) -> Self {
        self.encryption = Some(key);
        self
    }

    /// Enable or disable compression
    pub fn with_compression(mut self, enabled: bool) -> Self {
        self.compression = enabled;
        self
    }

    /// Set compression level (0-9)
    pub fn with_compression_level(mut self, level: u32) -> Self {
        self.compression_level = level.min(9);
        self
    }

    /// Check if encryption is enabled
    pub fn is_encrypted(&self) -> bool {
        self.encryption.is_some()
    }
}

impl AzothDb {
    /// Backup with custom options (encryption and compression)
    pub fn backup_with_options<P: AsRef<Path>>(
        &self,
        dir: P,
        options: &BackupOptions,
    ) -> Result<()> {
        use crate::CanonicalStore;

        let backup_dir = dir.as_ref();
        std::fs::create_dir_all(backup_dir)?;

        let canonical = self.canonical().clone();
        struct IngestionGuard {
            canonical: Arc<crate::LmdbCanonicalStore>,
        }
        impl Drop for IngestionGuard {
            fn drop(&mut self) {
                if let Err(e) = self.canonical.clear_seal() {
                    tracing::error!("Failed to clear seal after backup: {}", e);
                }
                if let Err(e) = self.canonical.resume_ingestion() {
                    tracing::error!("Failed to resume ingestion after backup: {}", e);
                }
            }
        }

        // Pause ingestion
        canonical.pause_ingestion()?;
        // Ensure we always resume (even on errors).
        let _guard = IngestionGuard { canonical };

        // Seal
        let sealed_id = self.canonical().seal()?;
        tracing::info!("Sealed canonical at event {}", sealed_id);

        // Catch up projector
        while self.projector().get_lag()? > 0 {
            self.projector().run_once()?;
        }
        tracing::info!("Projector caught up");

        // Backup canonical
        let canonical_dir = backup_dir.join("canonical");
        self.canonical().backup_to(&canonical_dir)?;

        // Backup projection
        let projection_path = backup_dir.join("projection.db");
        self.projection().backup_to(&projection_path)?;

        // Compression must run before encryption for the combined case.
        // Otherwise we delete/mutate source paths and the next stage can't find its inputs.
        if options.compression {
            tracing::info!("Compressing backup...");
            compress_directory(&canonical_dir, options.compression_level)?;
            compress_file(&projection_path, options.compression_level)?;
        }

        // Apply encryption if enabled.
        //
        // - no compression: encrypt files in canonical/ in-place + projection.db -> projection.db.age
        // - compression: encrypt compressed artifacts:
        //   - canonical.tar.zst -> canonical.tar.zst.age
        //   - projection.db.zst -> projection.db.zst.age
        if let Some(ref key) = options.encryption {
            tracing::info!("Encrypting backup...");
            if options.compression {
                let canonical_archive = canonical_dir.with_extension("tar.zst");
                let projection_archive = projection_path.with_extension("db.zst");
                encrypt_file(&canonical_archive, key)?;
                encrypt_file(&projection_archive, key)?;
            } else {
                encrypt_backup(&canonical_dir, key)?;
                encrypt_file(&projection_path, key)?;
            }
        }

        // Write manifest
        let cursor = self.projection().get_cursor()?;
        let manifest = BackupManifest::new(
            sealed_id,
            "lmdb".to_string(),
            "sqlite".to_string(),
            cursor,
            1,
            self.projection().schema_version()?,
        );

        let manifest_path = backup_dir.join("manifest.json");
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| AzothError::Serialization(e.to_string()))?;
        std::fs::write(&manifest_path, manifest_json)?;

        tracing::info!("Backup complete at {}", backup_dir.display());
        Ok(())
    }

    /// Restore from backup with custom options
    pub fn restore_with_options<P: AsRef<Path>, Q: AsRef<Path>>(
        backup_dir: P,
        target_path: Q,
        options: &BackupOptions,
    ) -> Result<Self> {
        let backup_dir = backup_dir.as_ref();
        let target_path = target_path.as_ref();

        // Read manifest
        let manifest_path = backup_dir.join("manifest.json");
        let manifest_json = std::fs::read_to_string(&manifest_path)?;
        let _manifest: BackupManifest = serde_json::from_str(&manifest_json)
            .map_err(|e| AzothError::Serialization(e.to_string()))?;

        let canonical_dir = backup_dir.join("canonical");
        let projection_file = backup_dir.join("projection.db");

        // Restore must mirror backup ordering.
        //
        // - encryption + compression: decrypt *.age -> *.zst, then decompress
        // - compression only: decompress
        // - encryption only: decrypt canonical/*.age and projection.db.age
        if options.compression {
            if let Some(ref key) = options.encryption {
                tracing::info!("Decrypting backup artifacts...");
                let canonical_archive_age = canonical_dir.with_extension("tar.zst.age");
                let projection_file_age = projection_file.with_extension("db.zst.age");
                decrypt_file(&canonical_archive_age, key)?;
                decrypt_file(&projection_file_age, key)?;
            }

            tracing::info!("Decompressing backup artifacts...");
            decompress_directory(&canonical_dir)?;
            decompress_file(&projection_file)?;
        } else if let Some(ref key) = options.encryption {
            tracing::info!("Decrypting backup...");
            decrypt_backup(&canonical_dir, key)?;
            let projection_file_age = projection_file.with_extension("db.age");
            decrypt_file(&projection_file_age, key)?;
        }

        // Restore using standard method
        Self::restore_from(backup_dir, target_path)
    }
}

// Encryption functions using age

fn encrypt_backup(dir: &Path, key: &EncryptionKey) -> Result<()> {
    // Encrypt all files in the directory
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() {
            encrypt_file(&path, key)?;
        }
    }
    Ok(())
}

fn encrypt_file(path: &Path, key: &EncryptionKey) -> Result<()> {
    // Read original file
    let plaintext = std::fs::read(path)?;

    // Encrypt with age
    let recipient = key.to_recipient();
    let encryptor = age::Encryptor::with_recipients(vec![Box::new(recipient)])
        .expect("We provided a recipient");

    let mut encrypted = vec![];
    let mut writer = encryptor
        .wrap_output(&mut encrypted)
        .map_err(|e| AzothError::Encryption(format!("Failed to wrap output: {}", e)))?;
    writer
        .write_all(&plaintext)
        .map_err(|e| AzothError::Encryption(format!("Failed to write: {}", e)))?;
    writer
        .finish()
        .map_err(|e| AzothError::Encryption(format!("Failed to finish: {}", e)))?;

    // Write encrypted file with .age extension
    let encrypted_path = path.with_extension(format!(
        "{}.age",
        path.extension().and_then(|s| s.to_str()).unwrap_or("dat")
    ));
    std::fs::write(&encrypted_path, encrypted)?;

    // Remove original file
    std::fs::remove_file(path)?;

    Ok(())
}

fn decrypt_backup(dir: &Path, key: &EncryptionKey) -> Result<()> {
    // Decrypt all .age files in the directory
    let entries = std::fs::read_dir(dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|s| s.to_str()) == Some("age") {
            decrypt_file(&path, key)?;
        }
    }
    Ok(())
}

fn decrypt_file(path: &Path, key: &EncryptionKey) -> Result<()> {
    // Read encrypted file
    let encrypted = std::fs::read(path)?;

    // Decrypt with age
    let decryptor = match age::Decryptor::new(&encrypted[..])
        .map_err(|e| AzothError::Encryption(format!("Failed to create decryptor: {}", e)))?
    {
        age::Decryptor::Recipients(d) => d,
        _ => {
            return Err(AzothError::Encryption(
                "Unexpected decryptor type".to_string(),
            ))
        }
    };

    let mut decrypted = vec![];
    let mut reader = decryptor
        .decrypt(std::iter::once(&key.identity as &dyn age::Identity))
        .map_err(|e| AzothError::Encryption(format!("Failed to decrypt: {}", e)))?;
    reader
        .read_to_end(&mut decrypted)
        .map_err(|e| AzothError::Encryption(format!("Failed to read: {}", e)))?;

    // Determine original filename (remove .age extension)
    let original_path = if let Some(stem) = path.file_stem() {
        path.with_file_name(stem)
    } else {
        path.with_extension("")
    };

    // Write decrypted file
    std::fs::write(&original_path, decrypted)?;

    // Remove encrypted file
    std::fs::remove_file(path)?;

    Ok(())
}

// Compression functions using zstd

fn compress_directory(dir: &Path, level: u32) -> Result<()> {
    // Create tar.zst archive of directory
    let archive_path = dir.with_extension("tar.zst");
    let archive_file = File::create(&archive_path)?;
    let encoder = zstd::Encoder::new(archive_file, level as i32)?;
    let mut tar_builder = tar::Builder::new(encoder);

    // Add the directory (as a directory) so decompression recreates the original layout.
    // If we archive with "." as the prefix, extraction would spill files into the parent
    // directory rather than recreating `dir/`, which breaks restore layout expectations.
    let dir_name = dir
        .file_name()
        .ok_or_else(|| AzothError::Config("Cannot get directory name".to_string()))?;
    tar_builder
        .append_dir_all(dir_name, dir)
        .map_err(AzothError::Io)?;

    // Finish and flush
    let encoder = tar_builder.into_inner().map_err(AzothError::Io)?;
    encoder.finish()?;

    // Remove original directory
    std::fs::remove_dir_all(dir)?;

    Ok(())
}

fn compress_file(path: &Path, level: u32) -> Result<()> {
    // Read original file
    let data = std::fs::read(path)?;

    // Compress with zstd
    let compressed = zstd::encode_all(&data[..], level as i32)?;

    // Write compressed file with .zst extension
    let compressed_path = path.with_extension(format!(
        "{}.zst",
        path.extension().and_then(|s| s.to_str()).unwrap_or("dat")
    ));
    std::fs::write(&compressed_path, compressed)?;

    // Remove original file
    std::fs::remove_file(path)?;

    Ok(())
}

fn decompress_directory(dir_path: &Path) -> Result<()> {
    // Look for tar.zst archive
    let archive_path = dir_path.with_extension("tar.zst");
    if !archive_path.exists() {
        return Err(AzothError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Archive not found: {}", archive_path.display()),
        )));
    }

    // Decompress and extract tar archive
    let archive_file = File::open(&archive_path)?;
    let decoder = zstd::Decoder::new(archive_file)?;
    let mut tar_archive = tar::Archive::new(decoder);

    // Extract to parent directory
    let parent = dir_path
        .parent()
        .ok_or_else(|| AzothError::Config("Cannot get parent directory".to_string()))?;
    tar_archive.unpack(parent).map_err(AzothError::Io)?;

    // Remove archive file
    std::fs::remove_file(&archive_path)?;

    Ok(())
}

fn decompress_file(path: &Path) -> Result<()> {
    // Find the .zst file
    let compressed_path = if path.extension().and_then(|s| s.to_str()) == Some("zst") {
        path.to_path_buf()
    } else {
        path.with_extension(format!(
            "{}.zst",
            path.extension().and_then(|s| s.to_str()).unwrap_or("dat")
        ))
    };

    if !compressed_path.exists() {
        return Err(AzothError::Io(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Compressed file not found: {}", compressed_path.display()),
        )));
    }

    // Read and decompress
    let compressed = std::fs::read(&compressed_path)?;
    let decompressed = zstd::decode_all(&compressed[..])?;

    // Determine original filename (remove .zst extension)
    let original_path = if let Some(stem) = compressed_path.file_stem() {
        compressed_path.with_file_name(stem)
    } else {
        path.to_path_buf()
    };

    // Write decompressed file
    std::fs::write(&original_path, decompressed)?;

    // Remove compressed file
    std::fs::remove_file(&compressed_path)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_key_generation() {
        let key = EncryptionKey::generate();
        let key_str = key.to_identity_string();
        assert!(!key_str.is_empty());

        // Should be able to parse it back
        let parsed = EncryptionKey::from_str(&key_str).unwrap();
        assert_eq!(parsed.to_identity_string(), key_str);
    }

    #[test]
    fn test_encryption_key_recipient() {
        let key = EncryptionKey::generate();
        let recipient_str = key.to_recipient_string();
        assert!(recipient_str.starts_with("age1"));
    }

    #[test]
    fn test_backup_options() {
        let options = BackupOptions::new()
            .with_encryption(EncryptionKey::generate())
            .with_compression(true)
            .with_compression_level(9);

        assert!(options.is_encrypted());
        assert!(options.compression);
        assert_eq!(options.compression_level, 9);
    }
}
