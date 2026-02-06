//! SQLite-backed projection store implementation
//!
//! Provides a queryable SQL database for derived projections from events.
//!
//! Key features:
//! - Cursor tracking for event application
//! - Schema migrations
//! - WAL mode for better concurrency
//! - Atomic batch application
//! - Optional read connection pooling for concurrent reads

pub mod read_pool;
pub mod schema;
pub mod store;
pub mod txn;

pub use read_pool::{PooledSqliteConnection, SqliteReadPool};
pub use store::SqliteProjectionStore;
pub use txn::SimpleProjectionTxn;
