//! Integration tests for azoth-vector
//!
//! These tests require the sqlite-vector extension to be available.
//! Download from: https://github.com/sqliteai/sqlite-vector/releases

use azoth_core::ProjectionConfig;
use azoth_sqlite::SqliteProjectionStore;
use azoth_vector::{DistanceMetric, Vector, VectorConfig, VectorExtension, VectorSearch};
use std::sync::Arc;
use tempfile::tempdir;

#[cfg(feature = "has-vector-extension")]
mod with_extension {
    use super::*;

    fn setup_db() -> Arc<SqliteProjectionStore> {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let config = ProjectionConfig {
            path: db_path,
            ..Default::default()
        };

        let store = Arc::new(SqliteProjectionStore::open(config).unwrap());

        // Load extension
        store.load_vector_extension(None).unwrap();

        store
    }

    #[test]
    fn test_extension_loaded() {
        let store = setup_db();
        assert!(store.has_vector_support());

        let version = store.vector_version().unwrap();
        assert!(!version.is_empty());
        println!("sqlite-vector version: {}", version);
    }

    #[test]
    fn test_vector_init() {
        let store = setup_db();

        // Create table
        store
            .execute(|conn| {
                conn.execute(
                    "CREATE TABLE test_vectors (id INTEGER PRIMARY KEY, vector BLOB)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();

        // Initialize vector column
        store
            .vector_init(
                "test_vectors",
                "vector",
                VectorConfig {
                    vector_type: azoth_vector::VectorType::Float32,
                    dimension: 3,
                    distance_metric: DistanceMetric::Cosine,
                },
            )
            .unwrap();
    }

    #[tokio::test]
    async fn test_basic_search() {
        let store = setup_db();

        // Create table
        store
            .execute(|conn| {
                conn.execute(
                    "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, vector BLOB, label TEXT)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();

        // Initialize vector column (3D for simplicity)
        store
            .vector_init(
                "embeddings",
                "vector",
                VectorConfig {
                    vector_type: azoth_vector::VectorType::Float32,
                    dimension: 3,
                    distance_metric: DistanceMetric::Cosine,
                },
            )
            .unwrap();

        // Insert test vectors
        let vectors = vec![
            (Vector::new(vec![1.0, 0.0, 0.0]), "x-axis"),
            (Vector::new(vec![0.0, 1.0, 0.0]), "y-axis"),
            (Vector::new(vec![0.0, 0.0, 1.0]), "z-axis"),
            (Vector::new(vec![0.7, 0.7, 0.0]), "xy-plane"),
        ];

        for (i, (vector, label)) in vectors.iter().enumerate() {
            store
                .transaction(|txn| {
                    txn.execute(
                        "INSERT INTO embeddings (id, vector, label) VALUES (?, ?, ?)",
                        rusqlite::params![i as i64 + 1, vector.to_blob(), label],
                    )?;
                    Ok(())
                })
                .unwrap();
        }

        // Search for vector close to x-axis
        let query = Vector::new(vec![0.9, 0.1, 0.0]);
        let search = VectorSearch::new(store.clone(), "embeddings", "vector");
        let results = search.knn(&query, 2).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].rowid, 1); // Should be x-axis (closest)

        println!("Search results:");
        for result in &results {
            let label: String = store
                .query(|conn| {
                    conn.query_row(
                        "SELECT label FROM embeddings WHERE rowid = ?",
                        [result.rowid],
                        |row| row.get(0),
                    )
                })
                .unwrap();
            println!("  {} - distance: {}", label, result.distance);
        }
    }

    #[tokio::test]
    async fn test_filtered_search() {
        let store = setup_db();

        // Create table with category
        store
            .execute(|conn| {
                conn.execute(
                    "CREATE TABLE items (
                        id INTEGER PRIMARY KEY,
                        vector BLOB,
                        category TEXT,
                        in_stock INTEGER
                    )",
                    [],
                )?;
                Ok(())
            })
            .unwrap();

        store
            .vector_init(
                "items",
                "vector",
                VectorConfig {
                    vector_type: azoth_vector::VectorType::Float32,
                    dimension: 3,
                    distance_metric: DistanceMetric::Cosine,
                },
            )
            .unwrap();

        // Insert items
        let items = vec![
            (vec![1.0, 0.0, 0.0], "electronics", 1),
            (vec![0.9, 0.1, 0.0], "electronics", 1),
            (vec![0.0, 1.0, 0.0], "books", 1),
            (vec![0.8, 0.2, 0.0], "electronics", 0), // out of stock
        ];

        for (i, (vector, category, in_stock)) in items.iter().enumerate() {
            let v = Vector::new(vector.clone());
            store
                .transaction(|txn| {
                    txn.execute(
                        "INSERT INTO items (id, vector, category, in_stock) VALUES (?, ?, ?, ?)",
                        rusqlite::params![i as i64 + 1, v.to_blob(), category, in_stock],
                    )?;
                    Ok(())
                })
                .unwrap();
        }

        // Search only in electronics that are in stock
        let query = Vector::new(vec![0.95, 0.05, 0.0]);
        let search = VectorSearch::new(store.clone(), "items", "vector");

        let results = search
            .knn_filtered(
                &query,
                10,
                "category = ? AND in_stock = 1",
                vec!["electronics".to_string()],
            )
            .await
            .unwrap();

        // Should only get 2 results (both electronics in stock)
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].rowid, 1);
        assert_eq!(results[1].rowid, 2);
    }

    #[tokio::test]
    async fn test_threshold_search() {
        let store = setup_db();

        store
            .execute(|conn| {
                conn.execute(
                    "CREATE TABLE docs (id INTEGER PRIMARY KEY, vector BLOB)",
                    [],
                )?;
                Ok(())
            })
            .unwrap();

        store
            .vector_init(
                "docs",
                "vector",
                VectorConfig {
                    vector_type: azoth_vector::VectorType::Float32,
                    dimension: 3,
                    distance_metric: DistanceMetric::Cosine,
                },
            )
            .unwrap();

        // Insert vectors with varying distances
        let vectors = vec![
            vec![1.0, 0.0, 0.0],   // distance ~0.1 from query
            vec![0.5, 0.5, 0.0],   // distance ~0.3 from query
            vec![0.0, 1.0, 0.0],   // distance ~0.9 from query
        ];

        for (i, vector) in vectors.iter().enumerate() {
            let v = Vector::new(vector.clone());
            store
                .transaction(|txn| {
                    txn.execute(
                        "INSERT INTO docs (id, vector) VALUES (?, ?)",
                        rusqlite::params![i as i64 + 1, v.to_blob()],
                    )?;
                    Ok(())
                })
                .unwrap();
        }

        let query = Vector::new(vec![0.9, 0.1, 0.0]);
        let search = VectorSearch::new(store.clone(), "docs", "vector");

        // Only get results within distance 0.4
        let results = search.threshold(&query, 0.4, 10).await.unwrap();

        // Should only get 2 closest results
        assert!(results.len() <= 2);
    }

    #[test]
    fn test_vector_blob_roundtrip() {
        let original = Vector::new(vec![0.1, 0.2, 0.3, 0.4, 0.5]);
        let blob = original.to_blob();
        let decoded = Vector::from_blob(&blob).unwrap();

        assert_eq!(original.dimension, decoded.dimension);
        for (a, b) in original.data.iter().zip(decoded.data.iter()) {
            assert!((a - b).abs() < 1e-6);
        }
    }
}

#[cfg(not(feature = "has-vector-extension"))]
#[test]
fn test_extension_not_available() {
    println!("sqlite-vector extension not available for testing");
    println!("Download from: https://github.com/sqliteai/sqlite-vector/releases");
    println!("Run tests with: cargo test --features has-vector-extension");
}
