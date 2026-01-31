//! Event log trait and types
//!
//! Defines the interface for event storage backends (file-based, LMDB, etc.)

use crate::error::Result;
use crate::types::EventId;

/// Iterator over events
pub trait EventLogIterator: Iterator<Item = Result<(EventId, Vec<u8>)>> + Send {}

impl<T: Iterator<Item = Result<(EventId, Vec<u8>)>> + Send> EventLogIterator for T {}

/// Event log storage backend
///
/// Provides fast append-only event storage separate from the canonical KV store.
/// This enables:
/// - Higher write throughput (no ACID overhead)
/// - Event rotation and archival
/// - Multiple concurrent writers (with file locking)
pub trait EventLog: Send + Sync {
    /// Append an event with a pre-allocated EventId
    ///
    /// This should be fast - just append bytes to a file.
    /// EventId must be provided by the caller (allocated from canonical store).
    fn append_with_id(&self, event_id: EventId, event_bytes: &[u8]) -> Result<()>;

    /// Append multiple events in a batch with pre-allocated EventIds
    ///
    /// The first event will have event_id = first_event_id,
    /// subsequent events will have first_event_id + 1, first_event_id + 2, etc.
    fn append_batch_with_ids(&self, first_event_id: EventId, events: &[Vec<u8>]) -> Result<()>;

    /// Get the next EventId that will be assigned
    fn next_event_id(&self) -> Result<EventId>;

    /// Iterate over events in a range
    ///
    /// Returns events from `start` (inclusive) to `end` (exclusive).
    /// If `end` is None, iterates to the latest event.
    fn iter_range(&self, start: EventId, end: Option<EventId>)
        -> Result<Box<dyn EventLogIterator>>;

    /// Get a single event by ID
    fn get(&self, event_id: EventId) -> Result<Option<Vec<u8>>>;

    /// Delete events up to (and including) the given EventId
    ///
    /// Used after archival to free up space.
    /// Returns the number of events deleted.
    fn delete_range(&self, start: EventId, end: EventId) -> Result<usize>;

    /// Rotate the current log file
    ///
    /// Closes the current file and starts a new one.
    /// Returns the path to the rotated file.
    fn rotate(&self) -> Result<std::path::PathBuf>;

    /// Get the oldest event ID still in storage
    fn oldest_event_id(&self) -> Result<EventId>;

    /// Get the newest event ID in storage
    fn newest_event_id(&self) -> Result<EventId>;

    /// Sync all writes to disk
    fn sync(&self) -> Result<()>;

    /// Get statistics about the event log
    fn stats(&self) -> Result<EventLogStats>;
}

/// Statistics about the event log
#[derive(Debug, Clone)]
pub struct EventLogStats {
    /// Total number of events in storage
    pub event_count: u64,

    /// Oldest event ID still in storage
    pub oldest_event_id: EventId,

    /// Newest event ID in storage
    pub newest_event_id: EventId,

    /// Total bytes used by event storage
    pub total_bytes: u64,

    /// Number of active log files
    pub file_count: usize,
}
