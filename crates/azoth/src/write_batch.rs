//! First-class write batch API for atomic multi-operation commits.
//!
//! `WriteBatch` collects state mutations and event appends, then applies
//! them in a single LMDB transaction on [`commit`](WriteBatch::commit).
//! This makes batch intent explicit and enables future Azoth-level
//! optimizations (e.g. deferred commit coalescing).
//!
//! # Example
//!
//! ```ignore
//! use azoth::WriteBatch;
//!
//! let mut batch = WriteBatch::new(&db);
//! batch.put(b"user:1:name", b"Alice");
//! batch.put(b"user:1:balance", b"100");
//! batch.append_event(b"user_created:{\"id\":1}");
//! let info = batch.commit()?;
//!
//! // Or commit asynchronously (wraps spawn_blocking):
//! let info = batch.commit_async().await?;
//! ```

use crate::{AzothDb, Result};
use azoth_core::traits::{CanonicalStore, CanonicalTxn};
use azoth_core::types::CommitInfo;

/// A buffered operation in the batch.
enum BatchOp {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    AppendEvent { data: Vec<u8> },
}

/// Collects state mutations and event appends, then commits them
/// atomically in a single LMDB transaction.
///
/// Operations are buffered in memory until [`commit`](Self::commit) or
/// [`commit_async`](Self::commit_async) is called. If the batch is
/// dropped without committing, no changes are applied.
pub struct WriteBatch<'a> {
    db: &'a AzothDb,
    ops: Vec<BatchOp>,
}

impl<'a> WriteBatch<'a> {
    /// Create a new empty write batch.
    pub fn new(db: &'a AzothDb) -> Self {
        Self {
            db,
            ops: Vec::new(),
        }
    }

    /// Buffer a key-value put operation.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        });
        self
    }

    /// Buffer a key deletion.
    pub fn delete(&mut self, key: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Delete {
            key: key.to_vec(),
        });
        self
    }

    /// Buffer an event append.
    pub fn append_event(&mut self, data: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::AppendEvent {
            data: data.to_vec(),
        });
        self
    }

    /// Return the number of buffered operations.
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Return `true` if the batch contains no operations.
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Apply all buffered operations in a single LMDB transaction.
    ///
    /// Returns [`CommitInfo`] with statistics about the committed
    /// transaction. If any operation fails, the entire batch is rolled
    /// back (LMDB's standard atomic commit guarantee).
    pub fn commit(self) -> Result<CommitInfo> {
        let mut txn = self.db.canonical().write_txn()?;

        for op in &self.ops {
            match op {
                BatchOp::Put { key, value } => {
                    txn.put_state(key, value)?;
                }
                BatchOp::Delete { key } => {
                    txn.del_state(key)?;
                }
                BatchOp::AppendEvent { data } => {
                    txn.append_event(data)?;
                }
            }
        }

        txn.commit()
    }

    /// Apply all buffered operations asynchronously.
    ///
    /// Wraps [`commit`](Self::commit) inside `spawn_blocking` so it is
    /// safe to call from an async context without blocking the runtime.
    pub async fn commit_async(self) -> Result<CommitInfo> {
        let ops = self.ops;
        let canonical = self.db.canonical().clone();

        tokio::task::spawn_blocking(move || {
            let mut txn = canonical.write_txn()?;

            for op in &ops {
                match op {
                    BatchOp::Put { key, value } => {
                        txn.put_state(key, value)?;
                    }
                    BatchOp::Delete { key } => {
                        txn.del_state(key)?;
                    }
                    BatchOp::AppendEvent { data } => {
                        txn.append_event(data)?;
                    }
                }
            }

            txn.commit()
        })
        .await
        .map_err(|e| azoth_core::error::AzothError::Internal(format!("Task join error: {}", e)))?
    }
}
