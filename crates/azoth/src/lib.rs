//! Axiom: A TEE-safe, high-performance storage subsystem
//!
//! Axiom provides a complete storage solution with:
//! - **Canonical store**: Transactional KV + append-only event log (LMDB)
//! - **Projection store**: Queryable SQL tables derived from events (SQLite)
//! - **Projector**: Event processing with batching and backpressure
//! - **Migrations**: Schema versioning and evolution
//! - **Event handlers**: Extensible event processing pipeline
//!
//! # Quick Start
//!
//! ```no_run
//! use azoth::prelude::*;
//!
//! # fn main() -> Result<()> {
//! // Open database
//! let db = AzothDb::open("./data")?;
//!
//! // Write state + events atomically
//! let mut txn = db.canonical().write_txn()?;
//! txn.put_state(b"key", b"value")?;
//! txn.append_event(b"event_data")?;
//! txn.commit()?;
//!
//! // Run projector
//! db.projector().run_once()?;
//! # Ok(())
//! # }
//! ```

pub mod backup;
pub mod checkpoint;
pub mod circuit_breaker;
pub mod db;
pub mod dead_letter_queue;
pub mod dlq_replayer;
pub mod event_format;
pub mod event_handler;
pub mod event_processor;
pub mod incremental_backup;
pub mod ipfs;
pub mod ipfs_storage;
pub mod migration;
#[cfg(feature = "onchain")]
pub mod onchain_registry;
pub mod prelude;
pub mod recovery_file;
pub mod transaction;
pub mod typed_values;

// Re-export core types
pub use azoth_core::{
    config::{
        CanonicalConfig, ProjectionConfig, ProjectorConfig, ReadPoolConfig, SyncMode,
        SynchronousMode,
    },
    error::{AzothError, Result},
    traits::{
        CanonicalReadTxn, CanonicalStore, CanonicalTxn, DecodedEvent, EventApplier, EventDecoder,
        EventIter, PreflightResult, ProjectionStore, ProjectionTxn, StateIter,
    },
    types::{BackupInfo, BackupManifest, CanonicalMeta, CommitInfo, EventBytes, EventId},
    LockManager,
};

// Re-export implementations
pub use azoth_lmdb::{LmdbCanonicalStore, LmdbReadPool, LmdbReadTxn, PooledLmdbReadTxn};
pub use azoth_projector::{Projector, ProjectorStats};
pub use azoth_sqlite::{PooledSqliteConnection, SqliteProjectionStore, SqliteReadPool};

// Re-export main types from this crate
pub use backup::{BackupOptions, EncryptionKey};
pub use checkpoint::{
    CheckpointConfig, CheckpointManager, CheckpointMetadata, CheckpointStorage, LocalStorage,
};
pub use circuit_breaker::{
    BreakerMetrics, BreakerMetricsSnapshot, BreakerState, CircuitBreaker, CircuitBreakerConfig,
};
pub use db::AzothDb;
pub use dead_letter_queue::{DeadLetterQueue, FailedEvent};
pub use dlq_replayer::{
    BackoffStrategy, DlqMetrics, DlqMetricsSnapshot, DlqReplayConfig, DlqReplayer, ReplayPriority,
};
pub use event_format::{Event, EventCodec, EventTypeRegistry, JsonCodec, MsgPackCodec};
pub use event_handler::{BatchConfig, BatchEvent, EventHandler, EventHandlerRegistry};
pub use event_processor::{
    ErrorAction, ErrorStrategy, EventProcessor, EventProcessorBuilder, ShutdownHandle,
};
pub use incremental_backup::{
    BackupRetention, BackupType, IncrementalBackup, IncrementalBackupConfig,
    IncrementalBackupManifest,
};
pub use ipfs::{IpfsClient, IpfsProvider};
pub use ipfs_storage::IpfsStorage;
pub use migration::{
    FileMigration, Migration, MigrationHistoryEntry, MigrationInfo, MigrationManager,
};
pub use transaction::{PreflightContext, Transaction, TransactionContext};
pub use typed_values::{Array, Set, TypedValue, I256, U256};
