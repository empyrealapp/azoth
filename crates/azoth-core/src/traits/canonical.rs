use crate::error::Result;
use crate::lock_manager::LockManager;
use crate::types::{BackupInfo, CanonicalMeta, CommitInfo, EventId};
use std::path::Path;

/// Preflight validation result
///
/// Tracks keys accessed during preflight validation for stripe locking
#[derive(Debug, Clone)]
pub struct PreflightResult {
    /// Whether validation passed
    pub valid: bool,

    /// Validation errors (if any)
    pub errors: Vec<String>,

    /// Keys read during preflight
    pub read_keys: Vec<Vec<u8>>,

    /// Keys to be written
    pub write_keys: Vec<Vec<u8>>,
}

impl PreflightResult {
    pub fn success() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            read_keys: Vec::new(),
            write_keys: Vec::new(),
        }
    }

    pub fn failure(error: String) -> Self {
        Self {
            valid: false,
            errors: vec![error],
            read_keys: Vec::new(),
            write_keys: Vec::new(),
        }
    }

    pub fn with_keys(mut self, read_keys: Vec<Vec<u8>>, write_keys: Vec<Vec<u8>>) -> Self {
        self.read_keys = read_keys;
        self.write_keys = write_keys;
        self
    }
}

/// Iterator over events in the canonical store
pub trait EventIter: Send {
    /// Get the next event
    ///
    /// Returns None when iteration is complete
    fn next(&mut self) -> Result<Option<(EventId, Vec<u8>)>>;
}

/// Transaction for canonical store operations
///
/// Supports three-phase commit:
/// 1. Async preflight validation (with stripe locking)
/// 2. Fast sync state updates and event appends
/// 3. Atomic commit
///
/// Note: Not required to be Send, as some backends (LMDB) have thread-affine transactions
pub trait CanonicalTxn {
    /// Phase 1: Preflight validation (async with stripe locking)
    ///
    /// Returns keys that will be accessed and validation result.
    /// This phase can run concurrently with other non-conflicting transactions.
    fn preflight(&mut self) -> Result<PreflightResult> {
        // Default implementation: no preflight validation
        Ok(PreflightResult::success())
    }

    /// Phase 2: Read state (fast, single-writer)
    fn get_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Phase 2: Write state (fast, single-writer)
    fn put_state(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Phase 2: Delete state (fast, single-writer)
    fn del_state(&mut self, key: &[u8]) -> Result<()>;

    /// Phase 2: Iterate over all state entries
    ///
    /// Returns a vector of (key, value) pairs.
    /// Note: This performs a full scan and should be used sparingly.
    /// Default implementation returns empty vector (not all backends support iteration).
    fn iter_state(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok(Vec::new())
    }

    /// Phase 3: Append single event (fast, single-writer)
    fn append_event(&mut self, event: &[u8]) -> Result<EventId>;

    /// Phase 3: Append multiple events (fast, single-writer)
    fn append_events(&mut self, events: &[Vec<u8>]) -> Result<(EventId, EventId)>;

    /// Commit transaction (phases 2+3 succeed atomically)
    ///
    /// Note: Phase 1 (preflight) is separate and uses stripe locks
    fn commit(self) -> Result<CommitInfo>;

    /// Abort transaction
    fn abort(self);
}

/// Canonical store: transactional KV + append-only event log
///
/// Provides:
/// - Atomic commits over state + events
/// - Stripe locking for concurrent preflight validation
/// - Sequential event iteration
/// - Seal mechanism for deterministic snapshots
/// - Pausable ingestion for safe backups
pub trait CanonicalStore: Send + Sync {
    type Txn<'a>: CanonicalTxn
    where
        Self: 'a;

    /// Open a canonical store
    fn open(cfg: crate::config::CanonicalConfig) -> Result<Self>
    where
        Self: Sized;

    /// Close the store
    fn close(&self) -> Result<()>;

    /// Begin a read-only transaction
    fn read_txn(&self) -> Result<Self::Txn<'_>>;

    /// Begin a write transaction
    ///
    /// Returns error if store is paused or sealed
    fn write_txn(&self) -> Result<Self::Txn<'_>>;

    /// Iterate events in a range
    ///
    /// - `from`: Starting event ID (inclusive)
    /// - `to`: Optional ending event ID (exclusive)
    fn iter_events(&self, from: EventId, to: Option<EventId>) -> Result<Box<dyn EventIter>>;

    /// Seal the store at the current event ID
    ///
    /// Returns the sealed event ID. No future commits will change state
    /// below or at this event ID.
    fn seal(&self) -> Result<EventId>;

    /// Get the lock manager for stripe locking
    fn lock_manager(&self) -> &LockManager;

    /// Pause ingestion (stop accepting new writes)
    ///
    /// Waits for in-flight transactions to complete
    fn pause_ingestion(&self) -> Result<()>;

    /// Resume ingestion (allow new writes)
    fn resume_ingestion(&self) -> Result<()>;

    /// Check if ingestion is paused
    fn is_paused(&self) -> bool;

    /// Create a backup
    fn backup_to(&self, dir: &Path) -> Result<BackupInfo>;

    /// Restore from a backup
    fn restore_from(dir: &Path, cfg: crate::config::CanonicalConfig) -> Result<Self>
    where
        Self: Sized;

    /// Get store metadata
    fn meta(&self) -> Result<CanonicalMeta>;
}
