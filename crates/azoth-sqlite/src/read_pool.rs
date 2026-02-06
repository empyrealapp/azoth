//! SQLite Read Connection Pool
//!
//! Provides a pool of read-only SQLite connections for concurrent reads.
//! Unlike LMDB, SQLite requires separate connections for true concurrency.

use azoth_core::{
    error::{AzothError, Result},
    ReadPoolConfig,
};
use rusqlite::{Connection, OpenFlags};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};

/// A pooled read-only connection for SQLite
///
/// This wraps a SQLite read-only connection with automatic permit release
/// when the connection is returned to the pool.
pub struct PooledSqliteConnection<'a> {
    conn: std::sync::MutexGuard<'a, Connection>,
    _permit: SemaphorePermit<'a>,
}

impl<'a> PooledSqliteConnection<'a> {
    /// Execute a read-only query
    ///
    /// # Example
    /// ```ignore
    /// let conn = pool.acquire().await?;
    /// let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))?;
    /// ```
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, f: F) -> Result<T>
    where
        P: rusqlite::Params,
        F: FnOnce(&rusqlite::Row<'_>) -> rusqlite::Result<T>,
    {
        self.conn
            .query_row(sql, params, f)
            .map_err(|e| AzothError::Projection(e.to_string()))
    }

    /// Prepare a statement for execution
    pub fn prepare(&self, sql: &str) -> Result<rusqlite::Statement<'_>> {
        self.conn
            .prepare(sql)
            .map_err(|e| AzothError::Projection(e.to_string()))
    }

    /// Get direct access to the underlying connection
    ///
    /// Use this for complex queries that need the full rusqlite API.
    pub fn connection(&self) -> &Connection {
        &self.conn
    }
}

/// SQLite Read Connection Pool
///
/// Manages a pool of read-only SQLite connections for concurrent reads.
/// Each connection is opened with `SQLITE_OPEN_READ_ONLY` flag.
///
/// # Example
///
/// ```ignore
/// let pool = SqliteReadPool::new(&db_path, ReadPoolConfig::enabled(4))?;
///
/// // Acquire a pooled connection
/// let conn = pool.acquire().await?;
/// let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))?;
/// // Connection is returned to pool when `conn` is dropped
/// ```
pub struct SqliteReadPool {
    connections: Vec<Mutex<Connection>>,
    semaphore: Semaphore,
    acquire_timeout: Duration,
    enabled: bool,
    db_path: PathBuf,
}

impl SqliteReadPool {
    /// Create a new read pool with the given configuration
    ///
    /// Opens `pool_size` read-only connections to the database.
    pub fn new(db_path: &Path, config: ReadPoolConfig) -> Result<Self> {
        let pool_size = if config.enabled { config.pool_size } else { 1 };
        let mut connections = Vec::with_capacity(pool_size);

        for _ in 0..pool_size {
            let conn = Connection::open_with_flags(
                db_path,
                OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

            connections.push(Mutex::new(conn));
        }

        Ok(Self {
            connections,
            semaphore: Semaphore::new(pool_size),
            acquire_timeout: Duration::from_millis(config.acquire_timeout_ms),
            enabled: config.enabled,
            db_path: db_path.to_path_buf(),
        })
    }

    /// Acquire a pooled read-only connection
    ///
    /// Waits up to the configured timeout for a connection to become available.
    /// Returns an error if the timeout is exceeded.
    pub async fn acquire(&self) -> Result<PooledSqliteConnection<'_>> {
        let permit = tokio::time::timeout(self.acquire_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| {
                AzothError::Timeout(format!(
                    "Read pool acquire timeout after {:?}",
                    self.acquire_timeout
                ))
            })?
            .map_err(|e| AzothError::Internal(format!("Semaphore closed: {}", e)))?;

        // Find an available connection (the permit ensures one is available)
        for conn in &self.connections {
            if let Ok(guard) = conn.try_lock() {
                return Ok(PooledSqliteConnection {
                    conn: guard,
                    _permit: permit,
                });
            }
        }

        // This shouldn't happen if semaphore is working correctly
        Err(AzothError::Internal(
            "No available connection despite having permit".into(),
        ))
    }

    /// Try to acquire a pooled read-only connection without waiting
    ///
    /// Returns `None` if no connection is immediately available.
    pub fn try_acquire(&self) -> Result<Option<PooledSqliteConnection<'_>>> {
        match self.semaphore.try_acquire() {
            Ok(permit) => {
                for conn in &self.connections {
                    if let Ok(guard) = conn.try_lock() {
                        return Ok(Some(PooledSqliteConnection {
                            conn: guard,
                            _permit: permit,
                        }));
                    }
                }
                // No connection available, permit will be dropped
                Ok(None)
            }
            Err(_) => Ok(None),
        }
    }

    /// Get the number of available connections in the pool
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Check if pooling is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Get the database path
    pub fn db_path(&self) -> &Path {
        &self.db_path
    }

    /// Get the pool size
    pub fn pool_size(&self) -> usize {
        self.connections.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_db() -> (TempDir, PathBuf) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("test.db");

        // Create the database with some test data
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", [])
            .unwrap();
        conn.execute("INSERT INTO test (id, value) VALUES (1, 'hello')", [])
            .unwrap();
        conn.execute("INSERT INTO test (id, value) VALUES (2, 'world')", [])
            .unwrap();
        drop(conn);

        (temp_dir, db_path)
    }

    #[tokio::test]
    async fn test_pool_acquire_release() {
        let (_temp_dir, db_path) = create_test_db();
        let config = ReadPoolConfig::enabled(2);
        let pool = SqliteReadPool::new(&db_path, config).unwrap();

        assert_eq!(pool.available_permits(), 2);

        // Acquire first connection
        let conn1 = pool.acquire().await.unwrap();
        assert_eq!(pool.available_permits(), 1);

        // Acquire second connection
        let conn2 = pool.acquire().await.unwrap();
        assert_eq!(pool.available_permits(), 0);

        // Try acquire should fail now
        assert!(pool.try_acquire().unwrap().is_none());

        // Drop first connection - should release permit
        drop(conn1);
        assert_eq!(pool.available_permits(), 1);

        // Drop second connection
        drop(conn2);
        assert_eq!(pool.available_permits(), 2);
    }

    #[tokio::test]
    async fn test_pool_query() {
        let (_temp_dir, db_path) = create_test_db();
        let config = ReadPoolConfig::enabled(2);
        let pool = SqliteReadPool::new(&db_path, config).unwrap();

        let conn = pool.acquire().await.unwrap();
        let value: String = conn
            .query_row("SELECT value FROM test WHERE id = ?1", [1], |row| {
                row.get(0)
            })
            .unwrap();
        assert_eq!(value, "hello");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM test", [], |row| row.get(0))
            .unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn test_try_acquire() {
        let (_temp_dir, db_path) = create_test_db();
        let config = ReadPoolConfig::enabled(1);
        let pool = SqliteReadPool::new(&db_path, config).unwrap();

        // First try should succeed
        let conn = pool.try_acquire().unwrap();
        assert!(conn.is_some());

        // Second try should return None
        assert!(pool.try_acquire().unwrap().is_none());

        // After drop, try should succeed again
        drop(conn);
        assert!(pool.try_acquire().unwrap().is_some());
    }
}
