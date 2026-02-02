//! Migration helpers for creating vector tables

use crate::types::VectorConfig;
use azoth_core::{error::AzothError, Result};
use rusqlite::Connection;

/// Helper to create a table with a vector column
///
/// This creates the table and initializes the vector column in one step.
///
/// # Example
///
/// ```no_run
/// use azoth::prelude::*;
/// use azoth_vector::{create_vector_table, VectorConfig};
/// use rusqlite::Connection;
///
/// struct CreateEmbeddingsTable;
///
/// impl azoth::Migration for CreateEmbeddingsTable {
///     fn version(&self) -> u32 { 2 }
///     fn name(&self) -> &str { "create_embeddings_table" }
///
///     fn up(&self, conn: &Connection) -> Result<()> {
///         create_vector_table(
///             conn,
///             "embeddings",
///             "id INTEGER PRIMARY KEY, text TEXT, vector BLOB",
///             "vector",
///             VectorConfig::default(),
///         )?;
///         Ok(())
///     }
///
///     fn down(&self, conn: &Connection) -> Result<()> {
///         conn.execute("DROP TABLE embeddings", [])
///             .map_err(|e| azoth::AzothError::Projection(e.to_string()))?;
///         Ok(())
///     }
/// }
/// ```
pub fn create_vector_table(
    conn: &Connection,
    table_name: &str,
    schema: &str,
    vector_column: &str,
    config: VectorConfig,
) -> Result<()> {
    // Validate table name and column name (prevent SQL injection)
    if !is_valid_identifier(table_name) {
        return Err(AzothError::InvalidState(format!(
            "Invalid table name: {}",
            table_name
        )));
    }
    if !is_valid_identifier(vector_column) {
        return Err(AzothError::InvalidState(format!(
            "Invalid column name: {}",
            vector_column
        )));
    }

    // Create table
    conn.execute(&format!("CREATE TABLE {} ({})", table_name, schema), [])
        .map_err(|e| AzothError::Projection(format!("Failed to create table: {}", e)))?;

    // Initialize vector column (SELECT returns a row)
    let config_str = config.to_config_string();
    conn.query_row(
        &format!(
            "SELECT vector_init('{}', '{}', ?)",
            table_name.replace('\'', "''"),
            vector_column.replace('\'', "''")
        ),
        [&config_str],
        |_row| Ok(()),
    )
    .map_err(|e| AzothError::Projection(format!("Failed to init vector column: {}", e)))?;

    tracing::info!(
        "Created table {} with vector column {} ({})",
        table_name,
        vector_column,
        &config_str
    );

    Ok(())
}

/// Helper to add a vector column to an existing table
///
/// This adds a BLOB column and initializes it for vector search.
///
/// # Example
///
/// ```no_run
/// use azoth::prelude::*;
/// use azoth_vector::{add_vector_column, VectorConfig};
/// use rusqlite::Connection;
///
/// struct AddVectorToExistingTable;
///
/// impl azoth::Migration for AddVectorToExistingTable {
///     fn version(&self) -> u32 { 3 }
///     fn name(&self) -> &str { "add_vector_column" }
///
///     fn up(&self, conn: &Connection) -> Result<()> {
///         add_vector_column(
///             conn,
///             "documents",
///             "embedding",
///             VectorConfig::default(),
///         )?;
///         Ok(())
///     }
///
///     fn down(&self, conn: &Connection) -> Result<()> {
///         // Note: SQLite doesn't support DROP COLUMN in older versions
///         // You may need to recreate the table
///         Ok(())
///     }
/// }
/// ```
pub fn add_vector_column(
    conn: &Connection,
    table_name: &str,
    column_name: &str,
    config: VectorConfig,
) -> Result<()> {
    // Validate identifiers
    if !is_valid_identifier(table_name) {
        return Err(AzothError::InvalidState(format!(
            "Invalid table name: {}",
            table_name
        )));
    }
    if !is_valid_identifier(column_name) {
        return Err(AzothError::InvalidState(format!(
            "Invalid column name: {}",
            column_name
        )));
    }

    // Add BLOB column
    conn.execute(
        &format!("ALTER TABLE {} ADD COLUMN {} BLOB", table_name, column_name),
        [],
    )
    .map_err(|e| AzothError::Projection(format!("Failed to add column: {}", e)))?;

    // Initialize vector column
    let config_str = config.to_config_string();
    conn.execute(
        &format!("SELECT vector_init('{}', '{}', ?)", table_name, column_name),
        [&config_str],
    )
    .map_err(|e| AzothError::Projection(format!("Failed to init vector column: {}", e)))?;

    tracing::info!(
        "Added vector column {}.{} ({})",
        table_name,
        column_name,
        &config_str
    );

    Ok(())
}

/// Validate SQL identifier (table/column name)
///
/// Allows alphanumeric, underscore, and must start with letter/underscore
fn is_valid_identifier(name: &str) -> bool {
    if name.is_empty() {
        return false;
    }

    let first_char = name.chars().next().unwrap();
    if !first_char.is_ascii_alphabetic() && first_char != '_' {
        return false;
    }

    name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_identifier() {
        assert!(is_valid_identifier("table_name"));
        assert!(is_valid_identifier("_private"));
        assert!(is_valid_identifier("Table123"));
        assert!(is_valid_identifier("_"));

        assert!(!is_valid_identifier(""));
        assert!(!is_valid_identifier("123table"));
        assert!(!is_valid_identifier("table-name"));
        assert!(!is_valid_identifier("table.name"));
        assert!(!is_valid_identifier("table name"));
        assert!(!is_valid_identifier("table'name"));
    }

    #[test]
    fn test_create_table_invalid_name() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let conn = Connection::open(&db_path).unwrap();

        let result = create_vector_table(
            &conn,
            "invalid-name",
            "id INTEGER",
            "vector",
            VectorConfig::default(),
        );

        assert!(result.is_err());
    }

    // Full integration tests with vector extension in tests/ directory
}
