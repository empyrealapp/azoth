//! LMDB-backed canonical store implementation
//!
//! Provides a transactional KV store + append-only event log using LMDB.
//!
//! Key features:
//! - Atomic commits over state + events
//! - Big-endian event ID encoding for proper sort order
//! - Stripe locking for concurrent preflight validation
//! - Hot copy backup support
//! - Single-writer semantics (enforced by mutex)
//! - Optional read transaction pooling for concurrent reads

pub mod backup;
pub mod dead_letter_queue;
pub mod iter;
pub mod keys;
pub mod preflight_cache;
pub mod read_pool;
pub mod state_iter;
pub mod store;
pub mod txn;

pub use dead_letter_queue::DeadLetterQueue;
pub use preflight_cache::EvictionPolicy;
pub use read_pool::{LmdbReadPool, PooledLmdbReadTxn};
pub use store::LmdbCanonicalStore;
pub use txn::{LmdbReadTxn, LmdbWriteTxn};
