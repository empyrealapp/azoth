//! Integration tests for azoth-vector
//!
//! These tests require the sqlite-vector extension. With feature `has-vector-extension`:
//! - Set `SQLITE_VECTOR_EXTENSION_PATH` to a pre-downloaded extension, or
//! - On macOS, the extension is downloaded automatically from GitHub releases.
//!
//! Download manually: <https://github.com/sqliteai/sqlite-vector/releases>

#[cfg(feature = "has-vector-extension")]
use std::path::PathBuf;

/// Resolve path to the sqlite-vector extension for loading.
/// 1. Use SQLITE_VECTOR_EXTENSION_PATH env if set.
/// 2. Use ./libsqlite_vector.{dylib,so,dll} if it exists.
/// 3. On macOS, download from GitHub releases and return path to the binary.
#[cfg(feature = "has-vector-extension")]
fn get_extension_path() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("SQLITE_VECTOR_EXTENSION_PATH") {
        let p = PathBuf::from(path);
        if p.exists() {
            return Some(p);
        }
    }

    let default_name = {
        #[cfg(target_os = "macos")]
        {
            "vector.dylib"
        }
        #[cfg(target_os = "linux")]
        {
            "vector.so"
        }
        #[cfg(target_os = "windows")]
        {
            "vector.dll"
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
        {
            "vector.so"
        }
    };

    let current_dir = PathBuf::from("./").join(default_name);
    if current_dir.exists() {
        return Some(current_dir);
    }

    #[cfg(target_os = "macos")]
    {
        download_extension_macos()
    }

    #[cfg(not(target_os = "macos"))]
    {
        None
    }
}

#[cfg(all(feature = "has-vector-extension", target_os = "macos"))]
fn download_extension_macos() -> Option<PathBuf> {
    use std::fs;
    use std::io::Read;

    const VERSION: &str = "0.9.70";
    const ZIP_URL: &str = "https://github.com/sqliteai/sqlite-vector/releases/download/0.9.70/vector-apple-xcframework-0.9.70.zip";

    let out_dir = std::env::var("OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/sqlite_vector_ext"));
    let cache_marker = out_dir.join(".ok");
    let framework_vector = out_dir
        .join("vector.xcframework")
        .join("macos-arm64_x86_64")
        .join("vector.framework")
        .join("vector");

    if cache_marker.exists() && framework_vector.exists() {
        return Some(framework_vector);
    }

    let _ = fs::create_dir_all(&out_dir);

    let resp = ureq::get(ZIP_URL).call().ok()?;
    let zip_bytes: Vec<u8> = resp.into_reader().bytes().filter_map(|b| b.ok()).collect();
    let mut archive = zip::ZipArchive::new(std::io::Cursor::new(zip_bytes)).ok()?;

    for i in 0..archive.len() {
        let mut entry = archive.by_index(i).ok()?;
        let name = entry.name().to_string();
        let out_path = out_dir.join(&name);

        if name.ends_with('/') {
            let _ = fs::create_dir_all(&out_path);
        } else {
            if let Some(parent) = out_path.parent() {
                let _ = fs::create_dir_all(parent);
            }
            let mut buf = Vec::new();
            let _ = entry.read_to_end(&mut buf);
            let _ = fs::write(&out_path, buf);
        }
    }

    if framework_vector.exists() {
        let _ = fs::write(cache_marker, VERSION);
        Some(framework_vector)
    } else {
        // Fallback: find any "vector" binary inside the extracted tree
        find_vector_binary_in_dir(&out_dir)
    }
}

#[cfg(all(feature = "has-vector-extension", target_os = "macos"))]
fn find_vector_binary_in_dir(dir: &std::path::Path) -> Option<PathBuf> {
    use std::fs;
    if dir.is_dir() {
        for entry in fs::read_dir(dir).ok()? {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.is_dir() {
                if let Some(p) = find_vector_binary_in_dir(&path) {
                    return Some(p);
                }
            } else if path.file_name().and_then(|n| n.to_str()) == Some("vector") {
                return Some(path);
            }
        }
    }
    None
}

#[cfg(feature = "has-vector-extension")]
mod with_extension {
    use super::*;
    use azoth_core::error::AzothError;
    use azoth_core::{ProjectionConfig, ProjectionStore};
    use azoth_sqlite::SqliteProjectionStore;
    use azoth_vector::{
        DistanceMetric, Vector, VectorConfig, VectorExtension, VectorFilter, VectorSearch,
    };
    use std::sync::Arc;
    use tempfile::tempdir;

    fn setup_db() -> Arc<SqliteProjectionStore> {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("test.db");

        let config = ProjectionConfig::new(db_path);

        let store = Arc::new(SqliteProjectionStore::open(config).unwrap());

        let ext_path = get_extension_path();
        store
            .load_vector_extension(ext_path.as_deref())
            .unwrap_or_else(|e| panic!("Failed to load vector extension (set SQLITE_VECTOR_EXTENSION_PATH or run on macOS for auto-download): {}", e));

        store
    }

    fn map_rusqlite(e: rusqlite::Error) -> AzothError {
        AzothError::Projection(e.to_string())
    }

    /// Build quantization table so vector_quantize_scan() can be used.
    fn run_vector_quantize(store: &SqliteProjectionStore, table: &str, column: &str) {
        store
            .execute(|conn| {
                let sql = format!(
                    "SELECT vector_quantize('{}', '{}')",
                    table.replace('\'', "''"),
                    column.replace('\'', "''")
                );
                conn.query_row(&sql, [], |_row| Ok(()))
                    .map_err(map_rusqlite)
            })
            .unwrap();
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

        store
            .execute(|conn| {
                conn.execute(
                    "CREATE TABLE test_vectors (id INTEGER PRIMARY KEY, vector BLOB)",
                    [],
                )
                .map_err(map_rusqlite)?;
                Ok(())
            })
            .unwrap();

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

        store
            .execute(|conn| {
                conn.execute(
                    "CREATE TABLE embeddings (id INTEGER PRIMARY KEY, vector BLOB, label TEXT)",
                    [],
                )
                .map_err(map_rusqlite)?;
                Ok(())
            })
            .unwrap();

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
                    )
                    .map_err(map_rusqlite)?;
                    Ok(())
                })
                .unwrap();
        }

        run_vector_quantize(&store, "embeddings", "vector");

        let query = Vector::new(vec![0.9, 0.1, 0.0]);
        let search = VectorSearch::new(store.clone(), "embeddings", "vector").unwrap();
        let results = search.knn(&query, 2).await.unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].rowid, 1);

        println!("Search results:");
        for result in &results {
            let label: String = store
                .query(|conn| {
                    conn.query_row(
                        "SELECT label FROM embeddings WHERE rowid = ?",
                        [result.rowid],
                        |row| row.get(0),
                    )
                    .map_err(map_rusqlite)
                })
                .unwrap();
            println!("  {} - distance: {}", label, result.distance);
        }
    }

    #[tokio::test]
    async fn test_filtered_search() {
        let store = setup_db();

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
                )
                .map_err(map_rusqlite)?;
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

        let items = vec![
            (vec![1.0, 0.0, 0.0], "electronics", 1),
            (vec![0.9, 0.1, 0.0], "electronics", 1),
            (vec![0.0, 1.0, 0.0], "books", 1),
            (vec![0.8, 0.2, 0.0], "electronics", 0),
        ];

        for (i, (vector, category, in_stock)) in items.iter().enumerate() {
            let v = Vector::new(vector.clone());
            store
                .transaction(|txn| {
                    txn.execute(
                        "INSERT INTO items (id, vector, category, in_stock) VALUES (?, ?, ?, ?)",
                        rusqlite::params![i as i64 + 1, v.to_blob(), category, in_stock],
                    )
                    .map_err(map_rusqlite)?;
                    Ok(())
                })
                .unwrap();
        }

        run_vector_quantize(&store, "items", "vector");

        let query = Vector::new(vec![0.95, 0.05, 0.0]);
        let search = VectorSearch::new(store.clone(), "items", "vector").unwrap();

        let filter = VectorFilter::new()
            .eq("category", "electronics")
            .eq_i64("in_stock", 1);

        let results = search.knn_filtered(&query, 10, &filter).await.unwrap();

        assert_eq!(results.len(), 2);
        let rowids: std::collections::HashSet<_> = results.iter().map(|r| r.rowid).collect();
        assert!(
            rowids.contains(&1) && rowids.contains(&2),
            "expected rowids 1 and 2, got {:?}",
            rowids
        );
    }

    #[tokio::test]
    async fn test_threshold_search() {
        let store = setup_db();

        store
            .execute(|conn| {
                conn.execute(
                    "CREATE TABLE docs (id INTEGER PRIMARY KEY, vector BLOB)",
                    [],
                )
                .map_err(map_rusqlite)?;
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

        let vectors = vec![
            vec![1.0, 0.0, 0.0],
            vec![0.5, 0.5, 0.0],
            vec![0.0, 1.0, 0.0],
        ];

        for (i, vector) in vectors.iter().enumerate() {
            let v = Vector::new(vector.clone());
            store
                .transaction(|txn| {
                    txn.execute(
                        "INSERT INTO docs (id, vector) VALUES (?, ?)",
                        rusqlite::params![i as i64 + 1, v.to_blob()],
                    )
                    .map_err(map_rusqlite)?;
                    Ok(())
                })
                .unwrap();
        }

        run_vector_quantize(&store, "docs", "vector");

        let query = Vector::new(vec![0.9, 0.1, 0.0]);
        let search = VectorSearch::new(store.clone(), "docs", "vector").unwrap();

        let results = search.threshold(&query, 0.4, 10).await.unwrap();

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
    println!("sqlite-vector extension not enabled for testing");
    println!("Download from: https://github.com/sqliteai/sqlite-vector/releases");
    println!("Run tests with: cargo test -p azoth-vector --features has-vector-extension");
}
