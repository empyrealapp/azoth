use azoth_core::{
    error::{AzothError, Result},
    event_log::EventLog,
    traits::{CanonicalTxn, PreflightResult},
    types::{CommitInfo, EventId},
};
use azoth_file_log::FileEventLog;
use lmdb::{Cursor, Database, RwTransaction, Transaction, WriteFlags};
use std::sync::Arc;

use crate::keys::meta_keys;

/// Write transaction for LMDB canonical store
///
/// State updates go to LMDB for transactional integrity.
/// Events are buffered and written to FileEventLog on commit.
pub struct LmdbWriteTxn<'a> {
    txn: Option<RwTransaction<'a>>,
    state_db: Database,
    #[allow(dead_code)]
    events_db: Database, // Deprecated: kept for backward compatibility
    meta_db: Database,
    event_log: Arc<FileEventLog>, // NEW: File-based event storage
    pending_events: Vec<Vec<u8>>, // NEW: Events to write on commit
    stats: TxnStats,
}

/// Transaction statistics
struct TxnStats {
    state_keys_written: usize,
    state_keys_deleted: usize,
    events_written: usize,
    first_event_id: Option<EventId>,
    last_event_id: Option<EventId>,
}

impl<'a> LmdbWriteTxn<'a> {
    pub fn new(
        txn: RwTransaction<'a>,
        state_db: Database,
        events_db: Database,
        meta_db: Database,
        event_log: Arc<FileEventLog>,
    ) -> Self {
        Self {
            txn: Some(txn),
            state_db,
            events_db,
            meta_db,
            event_log,
            pending_events: Vec::new(),
            stats: TxnStats {
                state_keys_written: 0,
                state_keys_deleted: 0,
                events_written: 0,
                first_event_id: None,
                last_event_id: None,
            },
        }
    }

    /// Get next event ID and increment it
    fn allocate_event_ids(&mut self, count: usize) -> Result<EventId> {
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| AzothError::InvalidState("Transaction already committed".into()))?;

        // Read current next_event_id
        let next_event_id = match txn.get(self.meta_db, &meta_keys::NEXT_EVENT_ID) {
            Ok(bytes) => {
                let id_str = std::str::from_utf8(bytes)
                    .map_err(|e| AzothError::Serialization(e.to_string()))?;
                id_str
                    .parse::<EventId>()
                    .map_err(|e| AzothError::Serialization(e.to_string()))?
            }
            Err(lmdb::Error::NotFound) => 0,
            Err(e) => return Err(AzothError::Transaction(e.to_string())),
        };

        // Update next_event_id
        let new_next_event_id = next_event_id + count as u64;
        txn.put(
            self.meta_db,
            &meta_keys::NEXT_EVENT_ID,
            &new_next_event_id.to_string(),
            WriteFlags::empty(),
        )
        .map_err(|e| AzothError::Transaction(e.to_string()))?;

        Ok(next_event_id)
    }
}

impl<'a> CanonicalTxn for LmdbWriteTxn<'a> {
    fn preflight(&mut self) -> Result<PreflightResult> {
        // Default implementation: no preflight validation
        // Applications can override this by wrapping the transaction
        Ok(PreflightResult::success())
    }

    fn get_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| AzothError::InvalidState("Transaction already committed".into()))?;

        match txn.get(self.state_db, &key) {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(AzothError::Transaction(e.to_string())),
        }
    }

    fn put_state(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| AzothError::InvalidState("Transaction already committed".into()))?;

        txn.put(self.state_db, &key, &value, WriteFlags::empty())
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        self.stats.state_keys_written += 1;
        Ok(())
    }

    fn del_state(&mut self, key: &[u8]) -> Result<()> {
        let txn = self
            .txn
            .as_mut()
            .ok_or_else(|| AzothError::InvalidState("Transaction already committed".into()))?;

        match txn.del(self.state_db, &key, None) {
            Ok(()) => {
                self.stats.state_keys_deleted += 1;
                Ok(())
            }
            Err(lmdb::Error::NotFound) => Ok(()), // Idempotent
            Err(e) => Err(AzothError::Transaction(e.to_string())),
        }
    }

    fn iter_state(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let txn = self
            .txn
            .as_ref()
            .ok_or_else(|| AzothError::InvalidState("Transaction already committed".into()))?;

        let mut results = Vec::new();
        let mut cursor = txn.open_ro_cursor(self.state_db)
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        for (key, value) in cursor.iter() {
            results.push((key.to_vec(), value.to_vec()));
        }

        Ok(results)
    }

    fn append_event(&mut self, event: &[u8]) -> Result<EventId> {
        // Allocate EventId from metadata
        let event_id = self.allocate_event_ids(1)?;

        // Buffer event for writing on commit
        self.pending_events.push(event.to_vec());

        self.stats.events_written += 1;
        if self.stats.first_event_id.is_none() {
            self.stats.first_event_id = Some(event_id);
        }
        self.stats.last_event_id = Some(event_id);

        Ok(event_id)
    }

    fn append_events(&mut self, events: &[Vec<u8>]) -> Result<(EventId, EventId)> {
        if events.is_empty() {
            return Err(AzothError::InvalidState("No events to append".into()));
        }

        // Allocate EventId range from metadata
        let first_event_id = self.allocate_event_ids(events.len())?;
        let last_event_id = first_event_id + events.len() as u64 - 1;

        // Buffer events for writing on commit
        self.pending_events.extend_from_slice(events);

        self.stats.events_written += events.len();
        if self.stats.first_event_id.is_none() {
            self.stats.first_event_id = Some(first_event_id);
        }
        self.stats.last_event_id = Some(last_event_id);

        Ok((first_event_id, last_event_id))
    }

    fn commit(mut self) -> Result<CommitInfo> {
        // Update timestamp before committing
        {
            let txn = self
                .txn
                .as_mut()
                .ok_or_else(|| AzothError::InvalidState("Transaction already committed".into()))?;

            txn.put(
                self.meta_db,
                &meta_keys::UPDATED_AT,
                &chrono::Utc::now().to_rfc3339(),
                WriteFlags::empty(),
            )
            .map_err(|e| AzothError::Transaction(e.to_string()))?;
        }

        // Phase 1: Commit LMDB state transaction
        let txn = self.txn.take().unwrap();
        txn.commit()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        // Phase 2: Write events to file-based log
        // This happens AFTER state commit succeeds
        // If this fails, it's a critical error (should rarely happen)
        if !self.pending_events.is_empty() {
            let first_event_id = self
                .stats
                .first_event_id
                .ok_or_else(|| AzothError::InvalidState("Missing first_event_id".into()))?;

            // Write events with pre-allocated EventIds from LMDB
            self.event_log
                .append_batch_with_ids(first_event_id, &self.pending_events)?;
        }

        Ok(CommitInfo {
            events_written: self.stats.events_written,
            first_event_id: self.stats.first_event_id,
            last_event_id: self.stats.last_event_id,
            state_keys_written: self.stats.state_keys_written,
            state_keys_deleted: self.stats.state_keys_deleted,
        })
    }

    fn abort(mut self) {
        if let Some(txn) = self.txn.take() {
            txn.abort();
        }
    }
}

impl<'a> Drop for LmdbWriteTxn<'a> {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            txn.abort();
        }
    }
}
