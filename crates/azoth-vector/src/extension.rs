//! Extension loading and initialization

use crate::search::validate_sql_identifier;
use crate::types::VectorConfig;
use azoth_core::{error::AzothError, Result};
use rusqlite::Connection;
use std::path::Path;

/// Load the sqlite-vector extension
///
/// The extension binary should be available at the given path.
/// Pre-built binaries can be downloaded from:
/// <https://github.com/sqliteai/sqlite-vector/releases>
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
/// This function uses `unsafe` because `rusqlite::Connection::load_extension`
/// calls `sqlite3_load_extension`, which loads a native shared library (`.so` / `.dylib` / `.dll`)
/// into the current process. Loading an untrusted library can execute arbitrary code.
///
/// **Requirements for safe usage:**
/// - The extension binary **must** come from a trusted, verified source (e.g., official releases).
/// - The `path` argument should **never** be derived from user-supplied input.
/// - Consider validating file checksums before loading in production deployments.
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

    // Reject paths that contain path-traversal components to prevent loading
    // arbitrary libraries from unexpected locations.
    validate_extension_path(ext_path)?;

    unsafe {
        let _guard = rusqlite::LoadExtensionGuard::new(conn)
            .map_err(|e| AzothError::Projection(format!("Failed to enable extensions: {}", e)))?;

        conn.load_extension(ext_path, None).map_err(|e| {
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

/// Validate that an extension path does not contain path-traversal components
/// or point through symlinks.
///
/// Rejects paths containing `..` components and paths that are symlinks,
/// as these could be used to load arbitrary shared libraries.
fn validate_extension_path(path: &Path) -> Result<()> {
    // Reject ".." components anywhere in the path
    for component in path.components() {
        if let std::path::Component::ParentDir = component {
            return Err(AzothError::Config(format!(
                "Extension path '{}' contains '..' component. \
                 Path traversal is not allowed for extension loading.",
                path.display()
            )));
        }
    }

    // If the file exists, reject symlinks
    if path.exists() && path.symlink_metadata()
        .map(|m| m.file_type().is_symlink())
        .unwrap_or(false)
    {
        return Err(AzothError::Config(format!(
            "Extension path '{}' is a symbolic link. \
             Symlinks are not allowed for extension loading.",
            path.display()
        )));
    }

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
    /// # fn example() -> Result<()> {
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
    /// # fn example() -> Result<()> {
    /// # let db = AzothDb::open("./data")?;
    /// # db.projection().load_vector_extension(None)?;
    /// // Create table with BLOB column
    /// # db.projection().execute(|conn: &rusqlite::Connection| {
    /// #     conn.execute(
    /// #         "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, vector BLOB)",
    /// #         [],
    /// #     ).map_err(|e| azoth::AzothError::Projection(e.to_string()))?;
    /// #     Ok(())
    /// # })?;
    /// // Initialize vector column
    /// # db.projection().vector_init("embeddings", "vector", VectorConfig::default())?;
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
    /// # fn example() -> Result<()> {
    /// let db = AzothDb::open("./data")?;
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
        let conn = self.conn().lock();
        load_vector_extension(&conn, path)
    }

    fn vector_init(&self, table: &str, column: &str, config: VectorConfig) -> Result<()> {
        // Validate identifiers to prevent SQL injection (same check used in VectorSearch::new)
        validate_sql_identifier(table, "Table")?;
        validate_sql_identifier(column, "Column")?;

        let conn = self.conn().lock();
        let config_str = config.to_config_string();

        conn.query_row(
            &format!("SELECT vector_init('{table}', '{column}', ?)"),
            [&config_str],
            |_row| Ok(()),
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
        let conn = self.conn().lock();
        let result = conn.prepare("SELECT vector_version()");
        result.is_ok()
    }

    fn vector_version(&self) -> Result<String> {
        let conn = self.conn().lock();
        let version: String = conn
            .query_row("SELECT vector_version()", [], |row| row.get(0))
            .map_err(|e| AzothError::Projection(format!("Failed to get vector version: {}", e)))?;
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
