use azoth_core::{error::Result, traits::StateIter, AzothError};
use lmdb::{Cursor, Database, Environment, Transaction};
use std::sync::Arc;

/// LMDB state iterator with chunked loading for constant memory usage
///
/// This iterator fetches data in chunks instead of materializing the entire range,
/// preventing OOM on large ranges while maintaining correctness.
pub struct LmdbStateIter {
    env: Arc<Environment>,
    db: Database,
    current_key: Vec<u8>,                   // Resume point for next chunk
    end_key: Option<Vec<u8>>,               // Upper bound (exclusive)
    chunk_size: usize,                      // Items per chunk
    current_chunk: Vec<(Vec<u8>, Vec<u8>)>, // Current chunk data
    chunk_index: usize,                     // Position within current chunk
    finished: bool,                         // No more data to fetch
}

impl LmdbStateIter {
    /// Create a new state iterator starting from a key
    pub fn new(
        env: Arc<Environment>,
        db: Database,
        start: &[u8],
        end: Option<&[u8]>,
        chunk_size: usize,
    ) -> Result<Self> {
        Ok(Self {
            env,
            db,
            current_key: start.to_vec(),
            end_key: end.map(|k| k.to_vec()),
            chunk_size,
            current_chunk: Vec::new(),
            chunk_index: 0,
            finished: false,
        })
    }

    /// Create a prefix iterator
    pub fn with_prefix(
        env: Arc<Environment>,
        db: Database,
        prefix: &[u8],
        chunk_size: usize,
    ) -> Result<Self> {
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

        Self::new(env, db, prefix, end.as_deref(), chunk_size)
    }

    /// Fetch the next chunk of data
    fn fetch_next_chunk(&mut self) -> Result<()> {
        self.current_chunk.clear();
        self.chunk_index = 0;

        if self.finished {
            return Ok(());
        }

        // Create short-lived read transaction
        let txn = self
            .env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let mut cursor = txn
            .open_ro_cursor(self.db)
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        // Position cursor at resume point
        let iter = if !self.current_key.is_empty() {
            cursor.iter_from(&self.current_key)
        } else {
            cursor.iter_start()
        };

        let mut count = 0;
        for (key, value) in iter {
            let key_bytes: &[u8] = key;
            let value_bytes: &[u8] = value;

            // Check end boundary
            if let Some(end) = &self.end_key {
                if key_bytes >= end.as_slice() {
                    self.finished = true;
                    break;
                }
            }

            self.current_chunk
                .push((key_bytes.to_vec(), value_bytes.to_vec()));
            count += 1;

            if count >= self.chunk_size {
                // Set resume point for next chunk
                // We need to advance past this key, so increment the last byte
                let mut next_key = key_bytes.to_vec();

                // Try to increment the key by adding 1 to the last byte
                // If we overflow, we need to carry to the previous byte
                let mut i = next_key.len();
                let mut carry = true;

                while carry && i > 0 {
                    i -= 1;
                    if next_key[i] < 255 {
                        next_key[i] += 1;
                        carry = false;
                    } else {
                        next_key[i] = 0;
                    }
                }

                // If we still have carry, we overflowed the entire key
                // Append a 0 byte to create the next key
                if carry {
                    next_key.push(0);
                }

                self.current_key = next_key;
                break;
            }
        }

        // If we got fewer items than chunk_size, we're done
        if count < self.chunk_size {
            self.finished = true;
        }

        Ok(())
    }
}

impl StateIter for LmdbStateIter {
    fn next(&mut self) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
        // Refill chunk when exhausted
        if self.chunk_index >= self.current_chunk.len() {
            self.fetch_next_chunk()?;
        }

        if self.chunk_index < self.current_chunk.len() {
            let result = self.current_chunk[self.chunk_index].clone();
            self.chunk_index += 1;
            Ok(Some(result))
        } else {
            Ok(None)
        }
    }
}
