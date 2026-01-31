use azoth_core::{
    error::{AzothError, Result},
    traits::ProjectionStore,
    types::EventId,
    ProjectionConfig,
};
use rusqlite::{Connection, OpenFlags};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::schema;
use crate::txn::SimpleProjectionTxn;

/// SQLite-backed projection store
pub struct SqliteProjectionStore {
    conn: Arc<Mutex<Connection>>,
    config: ProjectionConfig,
}

impl SqliteProjectionStore {
    /// Get the underlying connection (for migrations and custom queries)
    ///
    /// Returns an Arc to the Mutex-protected SQLite connection.
    /// Users should lock the mutex to access the connection.
    pub fn conn(&self) -> &Arc<Mutex<Connection>> {
        &self.conn
    }

    /// Initialize schema if needed
    fn init_schema(conn: &Connection) -> Result<()> {
        // Create projection_meta table
        conn.execute(
            "CREATE TABLE IF NOT EXISTS projection_meta (
                id INTEGER PRIMARY KEY CHECK (id = 0),
                last_applied_event_id INTEGER NOT NULL DEFAULT -1,
                schema_version INTEGER NOT NULL,
                updated_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Insert default row if not exists (-1 means no events processed yet)
        conn.execute(
            "INSERT OR IGNORE INTO projection_meta (id, last_applied_event_id, schema_version)
             VALUES (0, -1, 1)",
            [],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }

    /// Configure SQLite connection
    fn configure_connection(conn: &Connection, cfg: &ProjectionConfig) -> Result<()> {
        // Enable WAL mode
        if cfg.wal_mode {
            conn.pragma_update(None, "journal_mode", "WAL")
                .map_err(|e| AzothError::Config(e.to_string()))?;
        }

        // Set synchronous mode
        let sync_mode = match cfg.synchronous {
            azoth_core::config::SynchronousMode::Full => "FULL",
            azoth_core::config::SynchronousMode::Normal => "NORMAL",
            azoth_core::config::SynchronousMode::Off => "OFF",
        };
        conn.pragma_update(None, "synchronous", sync_mode)
            .map_err(|e| AzothError::Config(e.to_string()))?;

        // Enable foreign keys
        conn.pragma_update(None, "foreign_keys", "ON")
            .map_err(|e| AzothError::Config(e.to_string()))?;

        // Set cache size
        conn.pragma_update(None, "cache_size", cfg.cache_size)
            .map_err(|e| AzothError::Config(e.to_string()))?;

        Ok(())
    }
}

impl ProjectionStore for SqliteProjectionStore {
    type Txn<'a> = SimpleProjectionTxn<'a>;

    fn open(cfg: ProjectionConfig) -> Result<Self> {
        // Create parent directory if needed
        if let Some(parent) = cfg.path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Open connection
        let conn = Connection::open_with_flags(
            &cfg.path,
            OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Configure connection
        Self::configure_connection(&conn, &cfg)?;

        // Initialize schema
        Self::init_schema(&conn)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
            config: cfg,
        })
    }

    fn close(&self) -> Result<()> {
        // SQLite connection closes automatically on drop
        Ok(())
    }

    fn begin_txn(&self) -> Result<Self::Txn<'_>> {
        // Begin exclusive transaction using SimpleProjectionTxn which actually works
        let guard = self.conn.lock().unwrap();
        SimpleProjectionTxn::new(guard)
    }

    fn get_cursor(&self) -> Result<EventId> {
        let conn = self.conn.lock().unwrap();
        let cursor: i64 = conn
            .query_row(
                "SELECT last_applied_event_id FROM projection_meta WHERE id = 0",
                [],
                |row| row.get(0),
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(cursor as EventId)
    }

    fn migrate(&self, target_version: u32) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        schema::migrate(&conn, target_version)
    }

    fn backup_to(&self, path: &Path) -> Result<()> {
        // Checkpoint WAL to flush all changes to the main database file
        {
            let conn = self.conn.lock().unwrap();
            // Execute checkpoint with full iteration of results
            let mut stmt = conn
                .prepare("PRAGMA wal_checkpoint(RESTART)")
                .map_err(|e| AzothError::Projection(e.to_string()))?;
            let mut rows = stmt
                .query([])
                .map_err(|e| AzothError::Projection(e.to_string()))?;
            // Consume all rows to ensure checkpoint completes
            while let Ok(Some(_)) = rows.next() {}
        }

        // Get source path
        let src_path = &self.config.path;

        // Copy database file (now includes all changes from WAL)
        std::fs::copy(src_path, path)?;

        Ok(())
    }

    fn restore_from(path: &Path, cfg: ProjectionConfig) -> Result<Self> {
        // Copy backup file to target location
        std::fs::copy(path, &cfg.path)?;

        // Open the restored database
        Self::open(cfg)
    }

    fn schema_version(&self) -> Result<u32> {
        let conn = self.conn.lock().unwrap();
        let version: i64 = conn
            .query_row(
                "SELECT schema_version FROM projection_meta WHERE id = 0",
                [],
                |row| row.get(0),
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(version as u32)
    }
}
