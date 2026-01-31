use azoth_core::{
    error::{AzothError, Result},
    traits::EventIter,
    types::EventId,
};
use lmdb::{Database, Environment, Transaction};
use std::sync::Arc;

use crate::keys::event_id_to_key;

/// Iterator over events in LMDB
///
/// Note: This holds an Environment reference and creates read transactions on demand
pub struct LmdbEventIter {
    env: Arc<Environment>,
    db: Database,
    current: EventId,
    to: Option<EventId>,
}

impl LmdbEventIter {
    pub fn new(env: Arc<Environment>, db: Database, from: EventId, to: Option<EventId>) -> Self {
        Self {
            env,
            db,
            current: from,
            to,
        }
    }
}

impl EventIter for LmdbEventIter {
    fn next(&mut self) -> Result<Option<(EventId, Vec<u8>)>> {
        // Check if we've exceeded the upper bound
        if let Some(to) = self.to {
            if self.current >= to {
                return Ok(None);
            }
        }

        // Create a read transaction for this operation
        let txn = self
            .env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        // Try to get the current event
        let key = event_id_to_key(self.current);
        match txn.get(self.db, &key) {
            Ok(bytes) => {
                let event_id = self.current;
                self.current += 1;
                Ok(Some((event_id, bytes.to_vec())))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(AzothError::Transaction(e.to_string())),
        }
    }
}
