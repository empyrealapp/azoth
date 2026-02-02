//! Vector similarity search extension for Azoth using sqlite-vector
//!
//! This crate provides vector similarity search capabilities for Azoth applications
//! using the [sqlite-vector](https://github.com/sqliteai/sqlite-vector) extension.
//!
//! # Features
//!
//! - Multiple vector types (Float32, Float16, Int8, 1-bit)
//! - Multiple distance metrics (L2, Cosine, Dot Product, Hamming)
//! - Fast k-NN search with filtering
//! - SIMD-optimized for modern CPUs
//! - No preindexing required
//! - Migration helpers for easy setup
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth_vector::{VectorExtension, Vector, VectorSearch, VectorConfig, DistanceMetric};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Initialize Azoth with vector support
//! let db = AzothDb::open("./data")?;
//! db.projection().load_vector_extension(None)?;
//!
//! // Initialize vector column
//! db.projection().vector_init(
//!     "embeddings",
//!     "vector",
//!     VectorConfig::default(),
//! )?;
//!
//! // Insert vectors
//! let vector = Vector::new(vec![0.1, 0.2, 0.3]);
//! db.projection().transaction(|txn| {
//!     txn.execute(
//!         "INSERT INTO embeddings (id, vector) VALUES (?, ?)",
//!         rusqlite::params![1, vector.to_blob()],
//!     )?;
//!     Ok(())
//! })?;
//!
//! // Search for similar vectors
//! let query = Vector::new(vec![0.15, 0.25, 0.35]);
//! let search = VectorSearch::new(db.projection(), "embeddings", "vector");
//! let results = search.knn(&query, 10).await?;
//! # Ok(())
//! # }
//! ```

pub mod extension;
pub mod migration;
pub mod search;
pub mod types;

pub use extension::VectorExtension;
pub use migration::{add_vector_column, create_vector_table};
pub use search::VectorSearch;
pub use types::{DistanceMetric, SearchResult, Vector, VectorConfig, VectorType};
