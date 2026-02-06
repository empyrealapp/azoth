use azoth_core::{
    error::{AzothError, Result},
    traits::ProjectionTxn,
    types::EventId,
};
use rusqlite::Connection;
use std::sync::MutexGuard;

// Projection transaction that works with Connection directly
pub struct SimpleProjectionTxn<'a> {
    conn: MutexGuard<'a, Connection>,
    in_txn: bool,
}

impl<'a> SimpleProjectionTxn<'a> {
    pub fn new(conn: MutexGuard<'a, Connection>) -> Result<Self> {
        conn.execute("BEGIN IMMEDIATE TRANSACTION", [])
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(Self { conn, in_txn: true })
    }
}

impl<'a> ProjectionTxn for SimpleProjectionTxn<'a> {
    fn apply_event(&mut self, _id: EventId, _bytes: &[u8]) -> Result<()> {
        // Applications implement their own event application logic
        Ok(())
    }

    fn commit(mut self: Box<Self>, new_cursor: EventId) -> Result<()> {
        if self.in_txn {
            // Update cursor
            self.conn
                .execute(
                    "UPDATE projection_meta SET last_applied_event_id = ?1, updated_at = datetime('now') WHERE id = 0",
                    [new_cursor as i64],
                )
                .map_err(|e| AzothError::Projection(e.to_string()))?;

            self.conn
                .execute("COMMIT", [])
                .map_err(|e| AzothError::Projection(e.to_string()))?;

            self.in_txn = false;
        }
        Ok(())
    }

    fn rollback(mut self: Box<Self>) {
        if self.in_txn {
            let _ = self.conn.execute("ROLLBACK", []);
            self.in_txn = false;
        }
    }
}

impl<'a> Drop for SimpleProjectionTxn<'a> {
    fn drop(&mut self) {
        if self.in_txn {
            let _ = self.conn.execute("ROLLBACK", []);
        }
    }
}
