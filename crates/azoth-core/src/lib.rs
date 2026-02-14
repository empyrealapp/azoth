//! Axiom Core: Traits and types for the axiom storage subsystem
//!
//! This crate defines the core abstractions for a TEE-safe, high-performance
//! storage layer with:
//! - Canonical store (LMDB): Transactional KV + append-only event log
//! - Projection store (SQLite): Queryable SQL tables derived from events
//! - Projector: Consumes events and updates SQL projections
//! - Backup/restore: Deterministic snapshotting with manifest
//!
//! Key features:
//! - Three-phase transactions: Async preflight → Fast sync commit → Async projection
//! - Stripe locking: Parallel preflight validation for non-conflicting keys
//! - Pausable ingestion: Safe backups with no partial state
//! - Deterministic replay: Any projection can be rebuilt from events

pub mod config;
pub mod error;
pub mod event_log;
pub mod lock_manager;
pub mod observe;
pub mod traits;
pub mod types;

pub use config::{CanonicalConfig, ProjectionConfig, ProjectorConfig, ReadPoolConfig};
pub use error::{AzothError, Result};
pub use event_log::{EventLog, EventLogIterator, EventLogStats};
pub use lock_manager::LockManager;
pub use traits::{
    CanonicalReadTxn, CanonicalStore, CanonicalTxn, DecodedEvent, EventApplier, EventDecoder,
    EventIter, PreflightResult, ProjectionStore, ProjectionTxn,
};
pub use types::{BackupInfo, BackupManifest, CanonicalMeta, CommitInfo, EventBytes, EventId};
