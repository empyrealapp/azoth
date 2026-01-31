//! File-based event log implementation
//!
//! Provides a high-performance append-only event log using sequential file writes.
//! This is much faster than LMDB for event storage and doesn't block the canonical
//! store's write operations.
//!
//! Features:
//! - Fast sequential writes (no ACID overhead)
//! - Memory-mapped reads for iteration
//! - Automatic log rotation based on size
//! - EventId allocation via atomic counter + file sync
//! - Multiple concurrent readers, single writer

mod store;

pub use store::{FileEventLog, FileEventLogConfig};
