use azoth_core::{error::Result, traits::StateIter, AzothError};
use lmdb::{Cursor, Database, Environment, Transaction};
use std::sync::Arc;

/// LMDB state iterator for range queries
///
/// This collects all results upfront to avoid lifetime issues with LMDB cursors
pub struct LmdbStateIter {
    results: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
}

impl LmdbStateIter {
    /// Create a new state iterator starting from a key
    pub fn new(
        env: Arc<Environment>,
        db: Database,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<Self> {
        let mut results = Vec::new();

        // Create a read-only transaction and collect all results
        let txn = env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let mut cursor = txn
            .open_ro_cursor(db)
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        // Position cursor at start key or first key >= start
        let iter = if !start.is_empty() {
            cursor.iter_from(start)
        } else {
            cursor.iter_start()
        };

        // Collect all matching entries
        for (key, value) in iter {
            let key_bytes: &[u8] = key;
            let value_bytes: &[u8] = value;
            // Check if we've exceeded the end key
            if let Some(end_key) = end {
                if key_bytes >= end_key {
                    break;
                }
            }
            results.push((key_bytes.to_vec(), value_bytes.to_vec()));
        }

        Ok(Self { results, index: 0 })
    }

    /// Create a prefix iterator
    pub fn with_prefix(env: Arc<Environment>, db: Database, prefix: &[u8]) -> Result<Self> {
        // Calculate end key for prefix (prefix with last byte incremented)
        let end = if !prefix.is_empty() {
            let mut end_bytes = prefix.to_vec();
            // Try to increment the last byte
            if let Some(last) = end_bytes.last_mut() {
                if *last == 255 {
                    // Can't increment, use None for end
                    None
                } else {
                    *last += 1;
                    Some(end_bytes)
                }
            } else {
                None
            }
        } else {
            None
        };

        Self::new(env, db, prefix, end.as_deref())
    }
}

impl StateIter for LmdbStateIter {
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        if self.index < self.results.len() {
            let result = self.results[self.index].clone();
            self.index += 1;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}
