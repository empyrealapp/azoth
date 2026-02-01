//! Unified Axiom database interface
//!
//! Provides a single entry point for managing canonical store, projection store,
//! and projector together.

use crate::{
    CanonicalConfig, CanonicalStore, LmdbCanonicalStore, MigrationManager, ProjectionConfig,
    ProjectionStore, Projector, ProjectorConfig, Result, SqliteProjectionStore,
};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Unified Axiom database
///
/// Bundles together canonical store, projection store, and projector
/// for easy management.
pub struct AzothDb {
    canonical: Arc<LmdbCanonicalStore>,
    projection: Arc<SqliteProjectionStore>,
    projector: Projector<LmdbCanonicalStore, SqliteProjectionStore>,
    base_path: PathBuf,
}

impl AzothDb {
    /// Open an Axiom database at the given path
    ///
    /// Creates subdirectories:
    /// - `{path}/canonical/` - LMDB canonical store
    /// - `{path}/projection.db` - SQLite projection store
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let base_path = path.as_ref().to_path_buf();

        // Create canonical config
        let canonical_path = base_path.join("canonical");
        let canonical_config = CanonicalConfig::new(canonical_path);

        // Create projection config
        let projection_path = base_path.join("projection.db");
        let projection_config = ProjectionConfig::new(projection_path);

        Self::open_with_config(base_path, canonical_config, projection_config)
    }

    /// Open with custom configurations
    pub fn open_with_config(
        base_path: PathBuf,
        canonical_config: CanonicalConfig,
        projection_config: ProjectionConfig,
    ) -> Result<Self> {
        // Open stores
        let canonical = Arc::new(LmdbCanonicalStore::open(canonical_config)?);
        let projection = Arc::new(SqliteProjectionStore::open(projection_config)?);

        // Create projector
        let projector_config = ProjectorConfig::default();
        let projector = Projector::new(canonical.clone(), projection.clone(), projector_config);

        Ok(Self {
            canonical,
            projection,
            projector,
            base_path,
        })
    }

    /// Open with custom projector config
    pub fn open_with_projector_config(
        base_path: PathBuf,
        canonical_config: CanonicalConfig,
        projection_config: ProjectionConfig,
        projector_config: ProjectorConfig,
    ) -> Result<Self> {
        let canonical = Arc::new(LmdbCanonicalStore::open(canonical_config)?);
        let projection = Arc::new(SqliteProjectionStore::open(projection_config)?);
        let projector = Projector::new(canonical.clone(), projection.clone(), projector_config);

        Ok(Self {
            canonical,
            projection,
            projector,
            base_path,
        })
    }

    /// Get reference to canonical store
    pub fn canonical(&self) -> &Arc<LmdbCanonicalStore> {
        &self.canonical
    }

    /// Get reference to projection store
    pub fn projection(&self) -> &Arc<SqliteProjectionStore> {
        &self.projection
    }

    /// Get reference to projector
    pub fn projector(&self) -> &Projector<LmdbCanonicalStore, SqliteProjectionStore> {
        &self.projector
    }

    /// Get the base path
    pub fn base_path(&self) -> &Path {
        &self.base_path
    }

    /// Run migrations on the projection store
    pub fn migrate(&self, manager: &MigrationManager) -> Result<()> {
        manager.run(&self.projection)
    }

    /// Backup the entire database to a directory
    ///
    /// Follows the safe backup workflow:
    /// 1. Pause ingestion
    /// 2. Seal canonical
    /// 3. Run projector to catch up
    /// 4. Backup both stores
    /// 5. Resume ingestion
    pub fn backup_to<P: AsRef<Path>>(&self, dir: P) -> Result<()> {
        use crate::CanonicalStore;

        let backup_dir = dir.as_ref();
        std::fs::create_dir_all(backup_dir)?;

        // Pause ingestion
        self.canonical.pause_ingestion()?;

        // Seal
        let sealed_id = self.canonical.seal()?;
        tracing::info!("Sealed canonical at event {}", sealed_id);

        // Catch up projector
        while self.projector.get_lag()? > 0 {
            self.projector.run_once()?;
        }
        tracing::info!("Projector caught up");

        // Backup canonical
        let canonical_dir = backup_dir.join("canonical");
        self.canonical.backup_to(&canonical_dir)?;

        // Backup projection
        let projection_path = backup_dir.join("projection.db");
        self.projection.backup_to(&projection_path)?;

        // Write manifest
        let cursor = self.projection.get_cursor()?;
        let manifest = crate::BackupManifest::new(
            sealed_id,
            "lmdb".to_string(),
            "sqlite".to_string(),
            cursor,
            1, // canonical schema version
            self.projection.schema_version()?,
        );

        let manifest_path = backup_dir.join("manifest.json");
        let manifest_json = serde_json::to_string_pretty(&manifest)
            .map_err(|e| crate::AzothError::Serialization(e.to_string()))?;
        std::fs::write(&manifest_path, manifest_json)?;

        // Resume ingestion
        self.canonical.resume_ingestion()?;

        tracing::info!("Backup complete at {}", backup_dir.display());
        Ok(())
    }

    /// Restore from a backup directory
    pub fn restore_from<P: AsRef<Path>>(backup_dir: P, target_path: P) -> Result<Self> {
        use crate::CanonicalStore;

        let backup_dir = backup_dir.as_ref();
        let target_path = target_path.as_ref();

        // Read manifest
        let manifest_path = backup_dir.join("manifest.json");
        let manifest_json = std::fs::read_to_string(&manifest_path)?;
        let _manifest: crate::BackupManifest = serde_json::from_str(&manifest_json)
            .map_err(|e| crate::AzothError::Serialization(e.to_string()))?;

        // Prepare configs
        let canonical_dir = backup_dir.join("canonical");
        let projection_file = backup_dir.join("projection.db");

        let target_canonical = target_path.join("canonical");
        let target_projection = target_path.join("projection.db");

        let canonical_config = CanonicalConfig::new(target_canonical.clone());
        let projection_config = ProjectionConfig::new(target_projection.clone());

        // Restore canonical
        let canonical = Arc::new(LmdbCanonicalStore::restore_from(
            &canonical_dir,
            canonical_config,
        )?);

        // Restore projection
        let projection = Arc::new(SqliteProjectionStore::restore_from(
            &projection_file,
            projection_config,
        )?);

        // Create projector
        let projector = Projector::new(
            canonical.clone(),
            projection.clone(),
            ProjectorConfig::default(),
        );

        Ok(Self {
            canonical,
            projection,
            projector,
            base_path: target_path.to_path_buf(),
        })
    }

    /// Execute an async SQL query on the projection store
    ///
    /// This is a convenience wrapper around `projection().query_async()`.
    ///
    /// # Example
    /// ```ignore
    /// let balance: i64 = db.query_async(|conn| {
    ///     conn.query_row("SELECT balance FROM accounts WHERE id = ?1", [1], |row| row.get(0))
    /// }).await?;
    /// ```
    pub async fn query_async<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        self.projection.query_async(f).await
    }

    /// Execute a synchronous SQL query on the projection store
    ///
    /// # Example
    /// ```ignore
    /// let balance: i64 = db.query(|conn| {
    ///     conn.query_row("SELECT balance FROM accounts WHERE id = ?1", [1], |row| row.get(0))
    /// })?;
    /// ```
    pub fn query<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<R>,
    {
        self.projection.query(f)
    }

    /// Execute arbitrary SQL statements asynchronously
    ///
    /// # Example
    /// ```ignore
    /// db.execute_async(|conn| {
    ///     conn.execute("CREATE TABLE balances (id INTEGER PRIMARY KEY, amount INTEGER)", [])?;
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn execute_async<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<()> + Send + 'static,
    {
        self.projection.execute_async(f).await
    }

    /// Execute arbitrary SQL statements synchronously
    ///
    /// # Example
    /// ```ignore
    /// db.execute(|conn| {
    ///     conn.execute("CREATE TABLE balances (id INTEGER PRIMARY KEY, amount INTEGER)", [])?;
    ///     Ok(())
    /// })?;
    /// ```
    pub fn execute<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&rusqlite::Connection) -> Result<()>,
    {
        self.projection.execute(f)
    }

    /// Execute a SQL transaction
    ///
    /// # Example
    /// ```ignore
    /// db.transaction(|tx| {
    ///     tx.execute("INSERT INTO accounts (id, balance) VALUES (?1, ?2)", params![1, 100])?;
    ///     tx.execute("INSERT INTO accounts (id, balance) VALUES (?1, ?2)", params![2, 200])?;
    ///     Ok(())
    /// })?;
    /// ```
    pub fn transaction<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&rusqlite::Transaction) -> Result<()>,
    {
        self.projection.transaction(f)
    }

    /// Execute a SQL transaction asynchronously
    pub async fn transaction_async<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&rusqlite::Transaction) -> Result<()> + Send + 'static,
    {
        self.projection.transaction_async(f).await
    }

    /// Scan state keys with a prefix
    ///
    /// Returns an iterator over (key, value) pairs with keys starting with the prefix.
    ///
    /// # Example
    /// ```ignore
    /// use azoth_core::traits::StateIter;
    ///
    /// let mut iter = db.scan_prefix(b"user:")?;
    /// while let Some((key, value)) = iter.next()? {
    ///     println!("Key: {:?}, Value: {:?}", key, value);
    /// }
    /// ```
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Box<dyn azoth_core::traits::StateIter>> {
        use azoth_core::traits::CanonicalStore;
        self.canonical.scan_prefix(prefix)
    }

    /// Iterate state keys in a range
    ///
    /// # Example
    /// ```ignore
    /// use azoth_core::traits::StateIter;
    ///
    /// let mut iter = db.range(b"user:a", Some(b"user:z"))?;
    /// while let Some((key, value)) = iter.next()? {
    ///     println!("Key: {:?}, Value: {:?}", key, value);
    /// }
    /// ```
    pub fn range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Box<dyn azoth_core::traits::StateIter>> {
        use azoth_core::traits::CanonicalStore;
        self.canonical.range(start, end)
    }

    /// Prepare database for shutdown
    ///
    /// This method should be called before closing the database to ensure
    /// all pending operations complete. It:
    /// 1. Pauses ingestion
    /// 2. Seals the canonical store
    /// 3. Runs projector to catch up
    ///
    /// After calling this, you can create a final checkpoint using
    /// `CheckpointManager::shutdown_checkpoint()`, then call `close()`.
    ///
    /// # Example
    /// ```ignore
    /// // Prepare for shutdown
    /// db.prepare_shutdown()?;
    ///
    /// // Create final checkpoint (async)
    /// checkpoint_manager.shutdown_checkpoint().await?;
    ///
    /// // Close database
    /// db.close()?;
    /// ```
    pub fn prepare_shutdown(&self) -> Result<()> {
        use crate::CanonicalStore;

        tracing::info!("Preparing database for shutdown...");

        // 1. Pause ingestion
        self.canonical.pause_ingestion()?;
        tracing::info!("Ingestion paused");

        // 2. Seal the canonical store
        let sealed_id = self.canonical.seal()?;
        tracing::info!("Canonical store sealed at event {}", sealed_id);

        // 3. Run projector to catch up
        tracing::info!("Running projector to catch up...");
        while self.projector.get_lag()? > 0 {
            self.projector.run_once()?;
        }
        tracing::info!("Projector caught up");

        tracing::info!("Database prepared for shutdown");
        Ok(())
    }

    /// Close the database (ensures clean shutdown)
    ///
    /// For graceful shutdown with checkpoint, call `shutdown()` first.
    pub fn close(self) -> Result<()> {
        use crate::{CanonicalStore, ProjectionStore};

        // Drop projector first
        drop(self.projector);

        // Close stores
        Arc::try_unwrap(self.projection)
            .map_err(|_| crate::AzothError::InvalidState("Projection store still in use".into()))?
            .close()?;

        Arc::try_unwrap(self.canonical)
            .map_err(|_| crate::AzothError::InvalidState("Canonical store still in use".into()))?
            .close()?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_axiom_db_open() {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = AzothDb::open(temp_dir.path()).unwrap();

        // Should be able to access components
        assert!(db.canonical().meta().is_ok());
        assert!(db.projection().get_cursor().is_ok());
    }
}
