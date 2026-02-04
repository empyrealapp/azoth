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
        // schema_version starts at 0 so that migrations starting from version 1 will be applied
        conn.execute(
            "INSERT OR IGNORE INTO projection_meta (id, last_applied_event_id, schema_version)
             VALUES (0, -1, 0)",
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

    /// Execute a read-only SQL query asynchronously
    ///
    /// This method runs the query on a separate thread to avoid blocking,
    /// making it safe to call from async contexts.
    ///
    /// # Example
    /// ```ignore
    /// let balance: i64 = store.query_async(|conn| {
    ///     conn.query_row("SELECT balance FROM accounts WHERE id = ?1", [account_id], |row| row.get(0))
    /// }).await?;
    /// ```
    pub async fn query_async<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Connection) -> Result<R> + Send + 'static,
        R: Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn_guard = conn.lock().unwrap();
            f(&conn_guard)
        })
        .await
        .map_err(|e| AzothError::Projection(format!("Query task failed: {}", e)))?
    }

    /// Execute a read-only SQL query synchronously
    ///
    /// This is a convenience method for non-async contexts.
    /// For async contexts, prefer `query_async`.
    ///
    /// # Example
    /// ```ignore
    /// let balance: i64 = store.query(|conn| {
    ///     conn.query_row("SELECT balance FROM accounts WHERE id = ?1", [account_id], |row| row.get(0))
    /// })?;
    /// ```
    pub fn query<F, R>(&self, f: F) -> Result<R>
    where
        F: FnOnce(&Connection) -> Result<R>,
    {
        let conn_guard = self.conn.lock().unwrap();
        f(&conn_guard)
    }

    /// Execute arbitrary SQL statements (DDL/DML) asynchronously
    ///
    /// Useful for creating tables, indexes, or performing bulk updates.
    ///
    /// # Example
    /// ```ignore
    /// store.execute_async(|conn| {
    ///     conn.execute("CREATE TABLE IF NOT EXISTS balances (id INTEGER PRIMARY KEY, amount INTEGER)", [])?;
    ///     conn.execute("CREATE INDEX IF NOT EXISTS idx_amount ON balances(amount)", [])?;
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn execute_async<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&Connection) -> Result<()> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn_guard = conn.lock().unwrap();
            f(&conn_guard)
        })
        .await
        .map_err(|e| AzothError::Projection(format!("Execute task failed: {}", e)))?
    }

    /// Execute arbitrary SQL statements (DDL/DML) synchronously
    ///
    /// # Example
    /// ```ignore
    /// store.execute(|conn| {
    ///     conn.execute("CREATE TABLE IF NOT EXISTS balances (id INTEGER PRIMARY KEY, amount INTEGER)", [])?;
    ///     Ok(())
    /// })?;
    /// ```
    pub fn execute<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&Connection) -> Result<()>,
    {
        let conn_guard = self.conn.lock().unwrap();
        f(&conn_guard)
    }

    /// Execute a transaction with multiple SQL statements
    ///
    /// The closure receives a transaction object and can execute multiple
    /// statements atomically. If the closure returns an error, the transaction
    /// is rolled back.
    ///
    /// # Example
    /// ```ignore
    /// store.transaction(|tx| {
    ///     tx.execute("INSERT INTO accounts (id, balance) VALUES (?1, ?2)", params![1, 100])?;
    ///     tx.execute("INSERT INTO accounts (id, balance) VALUES (?1, ?2)", params![2, 200])?;
    ///     Ok(())
    /// })?;
    /// ```
    pub fn transaction<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&rusqlite::Transaction) -> Result<()>,
    {
        let mut conn_guard = self.conn.lock().unwrap();
        let tx = conn_guard
            .transaction()
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        f(&tx)?;

        tx.commit()
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }

    /// Execute a transaction asynchronously
    pub async fn transaction_async<F>(&self, f: F) -> Result<()>
    where
        F: FnOnce(&rusqlite::Transaction) -> Result<()> + Send + 'static,
    {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn_guard = conn.lock().unwrap();
            let tx = conn_guard
                .transaction()
                .map_err(|e| AzothError::Projection(e.to_string()))?;

            f(&tx)?;

            tx.commit()
                .map_err(|e| AzothError::Projection(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| AzothError::Projection(format!("Transaction task failed: {}", e)))?
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
