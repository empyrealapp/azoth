//! Vector similarity search API

use crate::types::{DistanceMetric, SearchResult, Vector};
use azoth_core::Result;
use azoth_sqlite::SqliteProjectionStore;
use rusqlite::params;
use std::sync::Arc;

/// Validate that a SQL identifier (table or column name) is safe.
///
/// Only allows `[a-zA-Z_][a-zA-Z0-9_]*` to prevent SQL injection via
/// identifier manipulation. Returns an error if the identifier is invalid.
fn validate_sql_identifier(name: &str, kind: &str) -> Result<()> {
    if name.is_empty() {
        return Err(azoth_core::error::AzothError::Config(format!(
            "{} name must not be empty",
            kind
        )));
    }
    if name.len() > 128 {
        return Err(azoth_core::error::AzothError::Config(format!(
            "{} name must be 128 characters or fewer, got {}",
            kind,
            name.len()
        )));
    }
    let mut chars = name.chars();
    let first = chars.next().unwrap(); // safe: name is non-empty
    if !first.is_ascii_alphabetic() && first != '_' {
        return Err(azoth_core::error::AzothError::Config(format!(
            "{} name '{}' must start with a letter or underscore",
            kind, name
        )));
    }
    for c in chars {
        if !c.is_ascii_alphanumeric() && c != '_' {
            return Err(azoth_core::error::AzothError::Config(format!(
                "{} name '{}' contains invalid character '{}'. \
                 Only ASCII alphanumeric and underscore are allowed.",
                kind, name, c
            )));
        }
    }
    Ok(())
}

/// Vector search builder
///
/// Provides k-NN search with optional filtering and custom distance metrics.
///
/// # Example
///
/// ```no_run
/// use azoth::prelude::*;
/// use azoth_vector::{VectorSearch, Vector, DistanceMetric};
///
/// # async fn example() -> Result<()> {
/// let db = AzothDb::open("./data")?;
///
/// let query = Vector::new(vec![0.1, 0.2, 0.3]);
/// let search = VectorSearch::new(db.projection().clone(), "embeddings", "vector")?
///     .distance_metric(DistanceMetric::Cosine);
///
/// let results = search.knn(&query, 10).await?;
/// # Ok(())
/// # }
/// ```
pub struct VectorSearch {
    projection: Arc<SqliteProjectionStore>,
    table: String,
    column: String,
    distance_metric: DistanceMetric,
}

impl VectorSearch {
    /// Create a new vector search builder
    ///
    /// # Arguments
    ///
    /// * `projection` - The SQLite projection store
    /// * `table` - Table name containing the vector column (must be a valid SQL identifier)
    /// * `column` - Vector column name (must be a valid SQL identifier, initialized with vector_init)
    ///
    /// # Errors
    ///
    /// Returns an error if `table` or `column` contain characters other than
    /// ASCII alphanumeric and underscore, or don't start with a letter/underscore.
    pub fn new(
        projection: Arc<SqliteProjectionStore>,
        table: impl Into<String>,
        column: impl Into<String>,
    ) -> Result<Self> {
        let table = table.into();
        let column = column.into();
        validate_sql_identifier(&table, "Table")?;
        validate_sql_identifier(&column, "Column")?;
        Ok(Self {
            projection,
            table,
            column,
            distance_metric: DistanceMetric::Cosine,
        })
    }

    /// Set the distance metric
    ///
    /// Default is Cosine similarity.
    pub fn distance_metric(mut self, metric: DistanceMetric) -> Self {
        self.distance_metric = metric;
        self
    }

    /// Perform k-nearest neighbors search
    ///
    /// Returns up to `k` results ordered by similarity (closest first).
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use azoth_vector::{VectorSearch, Vector};
    /// # async fn example(search: VectorSearch) -> Result<(), Box<dyn std::error::Error>> {
    /// let query_vector = Vector::new(vec![0.1, 0.2, 0.3]);
    /// let results = search.knn(&query_vector, 10).await?;
    ///
    /// for result in results {
    ///     println!("Row {}: distance = {}", result.rowid, result.distance);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn knn(&self, query: &Vector, k: usize) -> Result<Vec<SearchResult>> {
        // Table and column are validated at construction time via validate_sql_identifier
        let table = self.table.clone();
        let column = self.column.clone();
        let query_json = query.to_json();
        let k_i64 = k as i64;

        self.projection
            .query_async(move |conn| {
                let sql = format!(
                    "SELECT rowid, distance
                     FROM vector_quantize_scan('{table}', '{column}', ?, ?)
                     ORDER BY distance ASC",
                );

                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?;

                let results = stmt
                    .query_map(params![query_json, k_i64], |row| {
                        Ok(SearchResult {
                            rowid: row.get(0)?,
                            distance: row.get(1)?,
                        })
                    })
                    .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?;

                Ok(results)
            })
            .await
    }

    /// Search with distance threshold
    ///
    /// Returns all results within the given distance threshold, up to `k` results.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use azoth_vector::{VectorSearch, Vector};
    /// # async fn example(search: VectorSearch) -> Result<(), Box<dyn std::error::Error>> {
    /// let query = Vector::new(vec![0.1, 0.2, 0.3]);
    /// // Only return results with cosine distance < 0.3 (similarity > 0.7)
    /// let results = search.threshold(&query, 0.3, 100).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn threshold(
        &self,
        query: &Vector,
        max_distance: f32,
        k: usize,
    ) -> Result<Vec<SearchResult>> {
        let results = self.knn(query, k).await?;
        Ok(results
            .into_iter()
            .filter(|r| r.distance <= max_distance)
            .collect())
    }

    /// Search with custom SQL filter
    ///
    /// Allows filtering results by additional columns in the table.
    ///
    /// # Safety (SQL Injection)
    ///
    /// The `filter` string is interpolated into the WHERE clause of the query.
    /// **Always use `?` placeholders** for values and pass them via `filter_params`.
    /// Never interpolate user input directly into the filter string.
    ///
    /// Table and column identifiers are validated at `VectorSearch::new()` time
    /// to prevent identifier injection.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use azoth_vector::{VectorSearch, Vector};
    /// # async fn example(search: VectorSearch) -> Result<(), Box<dyn std::error::Error>> {
    /// let query = Vector::new(vec![0.1, 0.2, 0.3]);
    ///
    /// // GOOD: parameterized filter
    /// let results = search
    ///     .knn_filtered(&query, 10, "t.category = ?", vec!["tech".to_string()])
    ///     .await?;
    ///
    /// // BAD (DO NOT DO THIS): interpolating user input
    /// // let results = search
    /// //     .knn_filtered(&query, 10, &format!("t.category = '{}'", user_input), vec![])
    /// //     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn knn_filtered(
        &self,
        query: &Vector,
        k: usize,
        filter: &str,
        filter_params: Vec<String>,
    ) -> Result<Vec<SearchResult>> {
        // Table and column are validated at construction time via validate_sql_identifier
        let table = self.table.clone();
        let column = self.column.clone();
        let query_json = query.to_json();
        let k_i64 = k as i64;
        let filter = filter.to_string();

        self.projection
            .query_async(move |conn| {
                let sql = format!(
                    "SELECT v.rowid, v.distance
                     FROM vector_quantize_scan('{table}', '{column}', ?, ?) AS v
                     JOIN {table} AS t ON v.rowid = t.rowid
                     WHERE {filter}
                     ORDER BY v.distance ASC",
                );

                let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> =
                    vec![Box::new(query_json), Box::new(k_i64)];
                for p in filter_params {
                    params_vec.push(Box::new(p));
                }

                let mut stmt = conn
                    .prepare(&sql)
                    .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?;

                let results = stmt
                    .query_map(rusqlite::params_from_iter(params_vec.iter()), |row| {
                        Ok(SearchResult {
                            rowid: row.get(0)?,
                            distance: row.get(1)?,
                        })
                    })
                    .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?
                    .collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))?;

                Ok(results)
            })
            .await
    }

    /// Get multiple results by rowids and include their distances from query
    ///
    /// Useful for retrieving full records after search.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use azoth_vector::{VectorSearch, Vector};
    /// # use azoth_core::Result;
    /// # async fn example(search: VectorSearch) -> Result<()> {
    /// let query = Vector::new(vec![0.1, 0.2, 0.3]);
    /// let results = search.knn(&query, 10).await?;
    ///
    /// // Get full records
    /// for result in results {
    ///     let record: String = search.projection()
    ///         .query(|conn: &rusqlite::Connection| {
    ///             conn.query_row(
    ///                 "SELECT content FROM embeddings WHERE rowid = ?",
    ///                 [result.rowid],
    ///                 |row: &rusqlite::Row| row.get(0),
    ///             ).map_err(|e| azoth_core::error::AzothError::Projection(e.to_string()))
    ///         })?;
    ///     println!("Distance: {}, Content: {}", result.distance, record);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn projection(&self) -> &Arc<SqliteProjectionStore> {
        &self.projection
    }

    /// Get the table name
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Get the column name
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Get the distance metric
    pub fn distance_metric_value(&self) -> DistanceMetric {
        self.distance_metric
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use azoth_core::traits::ProjectionStore;

    fn make_store() -> Arc<SqliteProjectionStore> {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let config = azoth_core::ProjectionConfig {
            path: db_path.clone(),
            wal_mode: true,
            synchronous: azoth_core::config::SynchronousMode::Normal,
            cache_size: -2000,
            schema_version: 1,
            read_pool: azoth_core::config::ReadPoolConfig::default(),
        };

        // Leak the tempdir so it lives long enough for the test
        std::mem::forget(dir);
        Arc::new(azoth_sqlite::SqliteProjectionStore::open(config).unwrap())
    }

    #[test]
    fn test_search_builder() {
        let store = make_store();

        let search = VectorSearch::new(store.clone(), "test", "vector")
            .unwrap()
            .distance_metric(DistanceMetric::L2);

        assert_eq!(search.table(), "test");
        assert_eq!(search.column(), "vector");
        assert_eq!(search.distance_metric_value(), DistanceMetric::L2);
    }

    #[test]
    fn test_identifier_validation_rejects_injection() {
        let store = make_store();

        // SQL injection in table name should be rejected
        let result = VectorSearch::new(store.clone(), "x; DROP TABLE y; --", "vector");
        assert!(result.is_err());

        // SQL injection in column name should be rejected
        let result = VectorSearch::new(store.clone(), "test", "v'; DROP TABLE y; --");
        assert!(result.is_err());

        // Empty names should be rejected
        let result = VectorSearch::new(store.clone(), "", "vector");
        assert!(result.is_err());

        // Names starting with digits should be rejected
        let result = VectorSearch::new(store.clone(), "123table", "vector");
        assert!(result.is_err());

        // Valid identifiers should work
        let result = VectorSearch::new(store.clone(), "my_table", "embedding_col");
        assert!(result.is_ok());

        let result = VectorSearch::new(store.clone(), "_private", "_col");
        assert!(result.is_ok());
    }

    // Full integration tests with vector extension in tests/ directory
}
