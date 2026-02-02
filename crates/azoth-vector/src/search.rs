//! Vector similarity search API

use crate::types::{DistanceMetric, SearchResult, Vector};
use azoth_core::Result;
use azoth_sqlite::SqliteProjectionStore;
use rusqlite::params;
use std::sync::Arc;

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
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let db = AzothDb::open("./data")?;
///
/// let query = Vector::new(vec![0.1, 0.2, 0.3]);
/// let search = VectorSearch::new(db.projection(), "embeddings", "vector")
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
    /// * `table` - Table name containing the vector column
    /// * `column` - Vector column name (must be initialized with vector_init)
    pub fn new(
        projection: Arc<SqliteProjectionStore>,
        table: impl Into<String>,
        column: impl Into<String>,
    ) -> Self {
        Self {
            projection,
            table: table.into(),
            column: column.into(),
            distance_metric: DistanceMetric::Cosine,
        }
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
        let table = self.table.clone();
        let column = self.column.clone();
        let query_json = query.to_json();
        let k_i64 = k as i64;

        self.projection
            .query_async(move |conn| {
                let sql = format!(
                    "SELECT rowid, distance
                     FROM vector_quantize_scan('{}', '{}', ?, ?)
                     ORDER BY distance ASC",
                    table.replace('\'', "''"),
                    column.replace('\'', "''")
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
    /// # Example
    ///
    /// ```no_run
    /// # use azoth_vector::{VectorSearch, Vector};
    /// # async fn example(search: VectorSearch) -> Result<(), Box<dyn std::error::Error>> {
    /// let query = Vector::new(vec![0.1, 0.2, 0.3]);
    ///
    /// // Only search within a specific category
    /// let results = search
    ///     .knn_filtered(&query, 10, "category = ?", vec!["tech".to_string()])
    ///     .await?;
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
        let table = self.table.clone();
        let column = self.column.clone();
        let query_json = query.to_json();
        let k_i64 = k as i64;
        let filter = filter.to_string();

        self.projection
            .query_async(move |conn| {
                let sql = format!(
                    "SELECT v.rowid, v.distance
                     FROM vector_quantize_scan('{}', '{}', ?, ?) AS v
                     JOIN {} AS t ON v.rowid = t.rowid
                     WHERE {}
                     ORDER BY v.distance ASC",
                    table.replace('\'', "''"),
                    column.replace('\'', "''"),
                    table.replace('\'', "''"),
                    filter
                );

                let mut params_vec: Vec<Box<dyn rusqlite::ToSql>> = vec![
                    Box::new(query_json),
                    Box::new(k_i64),
                ];
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
    /// # async fn example(search: VectorSearch) -> Result<(), Box<dyn std::error::Error>> {
    /// let query = Vector::new(vec![0.1, 0.2, 0.3]);
    /// let results = search.knn(&query, 10).await?;
    ///
    /// // Get full records
    /// for result in results {
    ///     let record: String = search.projection
    ///         .query(|conn| {
    ///             conn.query_row(
    ///                 "SELECT content FROM embeddings WHERE rowid = ?",
    ///                 [result.rowid],
    ///                 |row| row.get(0),
    ///             )
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

    #[test]
    fn test_search_builder() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let config = azoth_core::ProjectionConfig {
            path: db_path.clone(),
            wal_mode: true,
            synchronous: azoth_core::config::SynchronousMode::Normal,
            cache_size: -2000,
            schema_version: 1,
        };

        let store = Arc::new(azoth_sqlite::SqliteProjectionStore::open(config).unwrap());

        let search = VectorSearch::new(store.clone(), "test", "vector")
            .distance_metric(DistanceMetric::L2);

        assert_eq!(search.table(), "test");
        assert_eq!(search.column(), "vector");
        assert_eq!(search.distance_metric_value(), DistanceMetric::L2);
    }

    // Full integration tests with vector extension in tests/ directory
}
