//! Extension loading and initialization

use crate::types::VectorConfig;
use azoth_core::{error::AzothError, Result};
use rusqlite::Connection;
use std::path::Path;

/// Load the sqlite-vector extension
///
/// The extension binary should be available at the given path.
/// Pre-built binaries can be downloaded from:
/// https://github.com/sqliteai/sqlite-vector/releases
///
/// # Platform-specific defaults
///
/// If no path is provided, defaults to:
/// - Linux: `./libsqlite_vector.so`
/// - macOS: `./libsqlite_vector.dylib`
/// - Windows: `./sqlite_vector.dll`
///
/// # Safety
///
/// This function uses unsafe code to load the extension. The extension must be
/// a valid SQLite extension and should be from a trusted source.
pub fn load_vector_extension(conn: &Connection, path: Option<&Path>) -> Result<()> {
    let ext_path = path.unwrap_or_else(|| {
        #[cfg(target_os = "linux")]
        {
            Path::new("./libsqlite_vector.so")
        }
        #[cfg(target_os = "macos")]
        {
            Path::new("./libsqlite_vector.dylib")
        }
        #[cfg(target_os = "windows")]
        {
            Path::new("./sqlite_vector.dll")
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos", target_os = "windows")))]
        {
            Path::new("./libsqlite_vector.so")
        }
    });

    unsafe {
        let _guard = rusqlite::LoadExtensionGuard::new(conn)
            .map_err(|e| AzothError::Projection(format!("Failed to enable extensions: {}", e)))?;

        conn.load_extension(ext_path, None)
            .map_err(|e| {
                AzothError::Projection(format!(
                    "Failed to load vector extension from {}: {}",
                    ext_path.display(),
                    e
                ))
            })?;
    }

    tracing::info!("Loaded sqlite-vector extension successfully");
    Ok(())
}

/// Extend SqliteProjectionStore with vector support
pub trait VectorExtension {
    /// Load the sqlite-vector extension
    ///
    /// # Example
    ///
    /// ```no_run
    /// use azoth::prelude::*;
    /// use azoth_vector::VectorExtension;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = AzothDb::open("./data")?;
    /// db.projection().load_vector_extension(None)?;
    /// # Ok(())
    /// # }
    /// ```
    fn load_vector_extension(&self, path: Option<&Path>) -> Result<()>;

    /// Initialize a vector column
    ///
    /// Must be called after creating the table with a BLOB column.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use azoth::prelude::*;
    /// use azoth_vector::{VectorExtension, VectorConfig};
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = AzothDb::open("./data")?;
    /// db.projection().load_vector_extension(None)?;
    ///
    /// // Create table with BLOB column
    /// db.projection().execute(|conn| {
    ///     conn.execute(
    ///         "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, vector BLOB)",
    ///         [],
    ///     )?;
    ///     Ok(())
    /// })?;
    ///
    /// // Initialize vector column
    /// db.projection().vector_init("embeddings", "vector", VectorConfig::default())?;
    /// # Ok(())
    /// # }
    /// ```
    fn vector_init(&self, table: &str, column: &str, config: VectorConfig) -> Result<()>;

    /// Check if vector extension is loaded
    ///
    /// # Example
    ///
    /// ```no_run
    /// use azoth::prelude::*;
    /// use azoth_vector::VectorExtension;
    ///
    /// # fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let db = AzothDb::open("./data")?;
    ///
    /// if !db.projection().has_vector_support() {
    ///     db.projection().load_vector_extension(None)?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    fn has_vector_support(&self) -> bool;

    /// Get the version of the sqlite-vector extension
    fn vector_version(&self) -> Result<String>;
}

impl VectorExtension for azoth_sqlite::SqliteProjectionStore {
    fn load_vector_extension(&self, path: Option<&Path>) -> Result<()> {
        let conn = self.conn().lock().unwrap();
        load_vector_extension(&conn, path)
    }

    fn vector_init(&self, table: &str, column: &str, config: VectorConfig) -> Result<()> {
        let conn = self.conn().lock().unwrap();
        let config_str = config.to_config_string();

        conn.execute(
            &format!(
                "SELECT vector_init('{}', '{}', ?)",
                table.replace('\'', "''"),
                column.replace('\'', "''")
            ),
            [&config_str],
        )
        .map_err(|e| {
            AzothError::Projection(format!(
                "Failed to init vector column {}.{}: {}",
                table, column, e
            ))
        })?;

        tracing::info!(
            "Initialized vector column {}.{} ({})",
            table,
            column,
            &config_str
        );
        Ok(())
    }

    fn has_vector_support(&self) -> bool {
        let conn = self.conn().lock().unwrap();
        let result = conn.prepare("SELECT vector_version()");
        result.is_ok()
    }

    fn vector_version(&self) -> Result<String> {
        let conn = self.conn().lock().unwrap();
        let version: String = conn
            .query_row("SELECT vector_version()", [], |row| row.get(0))
            .map_err(|e| {
                AzothError::Projection(format!("Failed to get vector version: {}", e))
            })?;
        Ok(version)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extension_not_loaded() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let conn = Connection::open(&db_path).unwrap();

        // Should fail without extension
        let result = conn.prepare("SELECT vector_version()");
        assert!(result.is_err());
    }

    // Note: We can't easily test extension loading without the actual .so/.dylib file
    // Integration tests with the extension binary should be in tests/
}
