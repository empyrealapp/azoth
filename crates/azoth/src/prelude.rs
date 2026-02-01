//! Axiom Prelude
//!
//! Import this to get all commonly used types and traits:
//!
//! ```
//! use azoth::prelude::*;
//! ```

// Core types
pub use crate::{
    AzothDb, AzothError, BackupInfo, CanonicalMeta, CommitInfo, EventBytes, EventId, Result,
};

// Configs
pub use crate::{CanonicalConfig, ProjectionConfig, ProjectorConfig, SyncMode, SynchronousMode};

// Traits
pub use crate::{
    CanonicalStore, CanonicalTxn, DecodedEvent, EventApplier, EventDecoder, ProjectionStore,
    ProjectionTxn,
};

// Implementations
pub use crate::{LmdbCanonicalStore, Projector, SqliteProjectionStore};

// Event handling
pub use crate::{EventHandler, EventHandlerRegistry};

// DLQ and Replay
pub use crate::{
    BackoffStrategy, DeadLetterQueue, DlqMetrics, DlqReplayConfig, DlqReplayer, FailedEvent,
    ReplayPriority,
};

// Circuit Breaker
pub use crate::{
    BreakerMetrics, BreakerMetricsSnapshot, BreakerState, CircuitBreaker, CircuitBreakerConfig,
};

// Migrations
pub use crate::{Migration, MigrationManager};

// Transaction API
pub use crate::{PreflightContext, Transaction, TransactionContext};

// Backup
pub use crate::{BackupOptions, EncryptionKey};

// Incremental Backup
pub use crate::{
    BackupRetention, BackupType, IncrementalBackup, IncrementalBackupConfig,
    IncrementalBackupManifest,
};

// Event format
pub use crate::{Event, EventCodec, EventTypeRegistry, JsonCodec, MsgPackCodec};

// Re-export common external deps
pub use anyhow;
pub use serde::{Deserialize, Serialize};
pub use std::sync::Arc;
pub use tracing;
