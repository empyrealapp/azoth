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
pub(crate) fn validate_sql_identifier(name: &str, kind: &str) -> Result<()> {
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

/// A bound parameter value for SQL queries.
///
/// Used internally by [`VectorFilter`] to hold typed values that are bound
/// via parameterized queries, preventing SQL injection.
#[derive(Clone, Debug)]
pub enum FilterValue {
    /// String parameter
    String(String),
    /// 64-bit integer parameter
    I64(i64),
    /// 64-bit float parameter
    F64(f64),
}

impl FilterValue {
    /// Convert into a boxed `ToSql` trait object for rusqlite parameter binding.
    pub fn to_boxed_sql(self) -> Box<dyn rusqlite::ToSql> {
        match self {
            Self::String(s) => Box::new(s),
            Self::I64(i) => Box::new(i),
            Self::F64(f) => Box::new(f),
        }
    }
}

/// A single condition in a vector search filter.
#[derive(Clone, Debug)]
struct FilterCondition {
    /// SQL fragment, e.g. `t.category = ?` or `t.in_stock = ?`
    sql: String,
    /// Bound parameter values (one per `?` placeholder in `sql`)
    params: Vec<FilterValue>,
}

/// Type-safe filter builder for vector search queries.
///
/// All column names are validated as safe SQL identifiers and all values are
/// bound via parameterized queries, eliminating SQL injection by construction.
///
/// # Example
///
/// ```
/// use azoth_vector::VectorFilter;
///
/// let filter = VectorFilter::new()
///     .eq("category", "electronics")
///     .eq_i64("in_stock", 1)
///     .gt("price", "9.99");
///
/// let (sql, params) = filter.to_sql().unwrap();
/// assert_eq!(sql, "t.category = ? AND t.in_stock = ? AND t.price > ?");
/// assert_eq!(params.len(), 3);
/// ```
#[derive(Clone, Debug, Default)]
pub struct VectorFilter {
    conditions: Vec<FilterCondition>,
}

impl VectorFilter {
    /// Create an empty filter (matches all rows).
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a string equality condition: `t.<column> = ?`
    pub fn eq(self, column: &str, value: impl Into<String>) -> Self {
        self.add_op(column, "=", FilterValue::String(value.into()))
    }

    /// Add a string inequality condition: `t.<column> != ?`
    pub fn neq(self, column: &str, value: impl Into<String>) -> Self {
        self.add_op(column, "!=", FilterValue::String(value.into()))
    }

    /// Add a string greater-than condition: `t.<column> > ?`
    pub fn gt(self, column: &str, value: impl Into<String>) -> Self {
        self.add_op(column, ">", FilterValue::String(value.into()))
    }

    /// Add a string greater-or-equal condition: `t.<column> >= ?`
    pub fn gte(self, column: &str, value: impl Into<String>) -> Self {
        self.add_op(column, ">=", FilterValue::String(value.into()))
    }

    /// Add a string less-than condition: `t.<column> < ?`
    pub fn lt(self, column: &str, value: impl Into<String>) -> Self {
        self.add_op(column, "<", FilterValue::String(value.into()))
    }

    /// Add a string less-or-equal condition: `t.<column> <= ?`
    pub fn lte(self, column: &str, value: impl Into<String>) -> Self {
        self.add_op(column, "<=", FilterValue::String(value.into()))
    }

    /// Add a LIKE condition: `t.<column> LIKE ?`
    pub fn like(self, column: &str, pattern: impl Into<String>) -> Self {
        self.add_op(column, "LIKE", FilterValue::String(pattern.into()))
    }

    /// Add an integer equality condition: `t.<column> = ?`
    pub fn eq_i64(self, column: &str, value: i64) -> Self {
        self.add_op(column, "=", FilterValue::I64(value))
    }

    /// Add an integer greater-than condition: `t.<column> > ?`
    pub fn gt_i64(self, column: &str, value: i64) -> Self {
        self.add_op(column, ">", FilterValue::I64(value))
    }

    /// Add an integer greater-or-equal condition: `t.<column> >= ?`
    pub fn gte_i64(self, column: &str, value: i64) -> Self {
        self.add_op(column, ">=", FilterValue::I64(value))
    }

    /// Add an integer less-than condition: `t.<column> < ?`
    pub fn lt_i64(self, column: &str, value: i64) -> Self {
        self.add_op(column, "<", FilterValue::I64(value))
    }

    /// Add an integer less-or-equal condition: `t.<column> <= ?`
    pub fn lte_i64(self, column: &str, value: i64) -> Self {
        self.add_op(column, "<=", FilterValue::I64(value))
    }

    /// Add a float equality condition: `t.<column> = ?`
    pub fn eq_f64(self, column: &str, value: f64) -> Self {
        self.add_op(column, "=", FilterValue::F64(value))
    }

    /// Add a float greater-than condition: `t.<column> > ?`
    pub fn gt_f64(self, column: &str, value: f64) -> Self {
        self.add_op(column, ">", FilterValue::F64(value))
    }

    /// Add a float less-than condition: `t.<column> < ?`
    pub fn lt_f64(self, column: &str, value: f64) -> Self {
        self.add_op(column, "<", FilterValue::F64(value))
    }

    /// Internal helper: validate column and push a condition.
    fn add_op(mut self, column: &str, op: &str, value: FilterValue) -> Self {
        self.conditions.push(FilterCondition {
            // We store validated column + op; validation happens in to_sql()
            sql: format!("t.{column} {op} ?"),
            params: vec![value],
        });
        self
    }

    /// Emit the WHERE clause and its bound parameters.
    ///
    /// Returns `("1 = 1", [])` for an empty filter (matches all rows).
    ///
    /// # Errors
    ///
    /// Returns `AzothError::Config` if any column name fails identifier validation.
    pub fn to_sql(&self) -> Result<(String, Vec<FilterValue>)> {
        if self.conditions.is_empty() {
            return Ok(("1 = 1".to_string(), Vec::new()));
        }

        // Validate all column names before emitting SQL
        for cond in &self.conditions {
            // Extract column name from `t.<col> <op> ?`
            let col_name = cond
                .sql
                .strip_prefix("t.")
                .and_then(|rest| rest.split_whitespace().next())
                .unwrap_or("");
            validate_sql_identifier(col_name, "Filter column")?;
        }

        let sql_parts: Vec<&str> = self.conditions.iter().map(|c| c.sql.as_str()).collect();
        let sql = sql_parts.join(" AND ");

        let params: Vec<FilterValue> = self
            .conditions
            .iter()
            .flat_map(|c| c.params.clone())
            .collect();

        Ok((sql, params))
    }
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

    /// Search with structured filter conditions
    ///
    /// Allows filtering results by additional columns in the table using a
    /// type-safe [`VectorFilter`] builder. All column names are validated as
    /// safe SQL identifiers, and all values are bound via parameterized queries,
    /// preventing SQL injection by construction.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use azoth_vector::{VectorSearch, Vector, VectorFilter};
    /// # async fn example(search: VectorSearch) -> Result<(), Box<dyn std::error::Error>> {
    /// let query = Vector::new(vec![0.1, 0.2, 0.3]);
    ///
    /// let filter = VectorFilter::new()
    ///     .eq("category", "tech")
    ///     .eq_i64("in_stock", 1);
    ///
    /// let results = search.knn_filtered(&query, 10, &filter).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn knn_filtered(
        &self,
        query: &Vector,
        k: usize,
        filter: &VectorFilter,
    ) -> Result<Vec<SearchResult>> {
        let (where_clause, filter_params) = filter.to_sql()?;

        let table = self.table.clone();
        let column = self.column.clone();
        let query_json = query.to_json();
        let k_i64 = k as i64;

        self.projection
            .query_async(move |conn| {
                let sql = format!(
                    "SELECT v.rowid, v.distance
                     FROM vector_quantize_scan('{table}', '{column}', ?, ?) AS v
                     JOIN {table} AS t ON v.rowid = t.rowid
                     WHERE {where_clause}
                     ORDER BY v.distance ASC",
                );

                let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> =
                    vec![Box::new(query_json), Box::new(k_i64)];
                for p in filter_params {
                    params_vec.push(p.to_boxed_sql());
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
