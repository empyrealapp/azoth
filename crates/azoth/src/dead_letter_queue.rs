//! Dead Letter Queue
//!
//! Stores failed events for later analysis and replay.

use crate::{AzothError, EventId, Result};
use rusqlite::{params, Connection};
use std::sync::Arc;

/// Failed event record
#[derive(Debug, Clone)]
pub struct FailedEvent {
    pub id: i64,
    pub event_id: EventId,
    pub event_bytes: Vec<u8>,
    pub error_message: String,
    pub failed_at: String,
    pub retry_count: i32,
}

/// Dead Letter Queue for storing failed events
pub struct DeadLetterQueue {
    conn: Arc<Connection>,
}

impl DeadLetterQueue {
    /// Create a new dead letter queue
    pub fn new(conn: Arc<Connection>) -> Result<Self> {
        let dlq = Self { conn };
        dlq.init()?;
        Ok(dlq)
    }

    /// Initialize the DLQ table
    fn init(&self) -> Result<()> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS dead_letter_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER NOT NULL,
                    event_bytes BLOB NOT NULL,
                    error_message TEXT NOT NULL,
                    failed_at TEXT NOT NULL DEFAULT (datetime('now')),
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    last_retry_at TEXT
                )",
                [],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Index for querying by event_id
        self.conn
            .execute(
                "CREATE INDEX IF NOT EXISTS idx_dlq_event_id
                 ON dead_letter_queue(event_id)",
                [],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }

    /// Add a failed event to the dead letter queue
    pub fn add(&self, event_id: EventId, event_bytes: &[u8], error: &AzothError) -> Result<i64> {
        let _id = self
            .conn
            .execute(
                "INSERT INTO dead_letter_queue (event_id, event_bytes, error_message)
                 VALUES (?1, ?2, ?3)",
                params![event_id, event_bytes, error.to_string()],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(self.conn.last_insert_rowid())
    }

    /// Get a failed event by ID
    pub fn get(&self, id: i64) -> Result<Option<FailedEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, event_id, event_bytes, error_message, failed_at, retry_count
                 FROM dead_letter_queue
                 WHERE id = ?1",
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        let result = stmt.query_row([id], |row| {
            Ok(FailedEvent {
                id: row.get(0)?,
                event_id: row.get(1)?,
                event_bytes: row.get(2)?,
                error_message: row.get(3)?,
                failed_at: row.get(4)?,
                retry_count: row.get(5)?,
            })
        });

        match result {
            Ok(event) => Ok(Some(event)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(AzothError::Projection(e.to_string())),
        }
    }

    /// Get all failed events
    pub fn list(&self, limit: usize) -> Result<Vec<FailedEvent>> {
        let mut stmt = self
            .conn
            .prepare(
                "SELECT id, event_id, event_bytes, error_message, failed_at, retry_count
                 FROM dead_letter_queue
                 ORDER BY failed_at DESC
                 LIMIT ?1",
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        let events = stmt
            .query_map([limit], |row| {
                Ok(FailedEvent {
                    id: row.get(0)?,
                    event_id: row.get(1)?,
                    event_bytes: row.get(2)?,
                    error_message: row.get(3)?,
                    failed_at: row.get(4)?,
                    retry_count: row.get(5)?,
                })
            })
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        events
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| AzothError::Projection(e.to_string()))
    }

    /// Mark an event for retry
    pub fn mark_retry(&self, id: i64) -> Result<()> {
        self.conn
            .execute(
                "UPDATE dead_letter_queue
                 SET retry_count = retry_count + 1,
                     last_retry_at = datetime('now')
                 WHERE id = ?1",
                [id],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }

    /// Remove a failed event from the queue
    pub fn remove(&self, id: i64) -> Result<()> {
        self.conn
            .execute("DELETE FROM dead_letter_queue WHERE id = ?1", [id])
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }

    /// Get count of failed events
    pub fn count(&self) -> Result<usize> {
        let count: i64 = self
            .conn
            .query_row("SELECT COUNT(*) FROM dead_letter_queue", [], |row| {
                row.get(0)
            })
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(count as usize)
    }

    /// Clear all failed events
    pub fn clear(&self) -> Result<()> {
        self.conn
            .execute("DELETE FROM dead_letter_queue", [])
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }
}
