//! SQLite-backed projection store implementation
//!
//! Provides a queryable SQL database for derived projections from events.
//!
//! Key features:
//! - Cursor tracking for event application
//! - Schema migrations
//! - WAL mode for better concurrency
//! - Atomic batch application

pub mod schema;
pub mod store;
pub mod txn;

pub use store::SqliteProjectionStore;
pub use txn::SimpleProjectionTxn;
