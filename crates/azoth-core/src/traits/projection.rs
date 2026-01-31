use crate::error::Result;
use crate::types::EventId;
use std::path::Path;

/// Transaction for projection store operations
///
/// Note: Not required to be Send, as some backends have thread-affine transactions
pub trait ProjectionTxn {
    /// Apply a single event to the projection
    fn apply_event(&mut self, id: EventId, bytes: &[u8]) -> Result<()>;

    /// Apply a batch of events to the projection
    fn apply_batch(&mut self, events: &[(EventId, Vec<u8>)]) -> Result<()> {
        for (id, bytes) in events {
            self.apply_event(*id, bytes)?;
        }
        Ok(())
    }

    /// Commit transaction and update cursor
    fn commit(self: Box<Self>, new_cursor: EventId) -> Result<()>;

    /// Rollback transaction
    fn rollback(self: Box<Self>);
}

/// Projection store: SQL database for derived tables
///
/// Provides:
/// - Schema management and migrations
/// - Cursor tracking for event application
/// - Transactional batch application
/// - Backup and restore
pub trait ProjectionStore: Send + Sync {
    type Txn<'a>: ProjectionTxn
    where
        Self: 'a;

    /// Open a projection store
    fn open(cfg: crate::config::ProjectionConfig) -> Result<Self>
    where
        Self: Sized;

    /// Close the store
    fn close(&self) -> Result<()>;

    /// Begin a transaction
    fn begin_txn(&self) -> Result<Self::Txn<'_>>;

    /// Get the current cursor (last applied event ID)
    fn get_cursor(&self) -> Result<EventId>;

    /// Run migrations to target schema version
    fn migrate(&self, target_version: u32) -> Result<()>;

    /// Create a backup
    fn backup_to(&self, path: &Path) -> Result<()>;

    /// Restore from a backup
    fn restore_from(path: &Path, cfg: crate::config::ProjectionConfig) -> Result<Self>
    where
        Self: Sized;

    /// Get the schema version
    fn schema_version(&self) -> Result<u32>;
}
