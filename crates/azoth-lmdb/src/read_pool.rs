//! LMDB Read Transaction Pool
//!
//! Provides a pool of read-only transaction slots for concurrent reads.
//! LMDB natively supports concurrent readers (up to `max_readers` configured),
//! so this pool primarily manages concurrency limits and provides a clean API.

use azoth_core::{
    error::{AzothError, Result},
    ReadPoolConfig,
};
use lmdb::{Database, Environment, RoTransaction, Transaction};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};

/// A pooled read-only transaction for LMDB
///
/// This wraps an LMDB read-only transaction with automatic permit release
/// when the transaction is dropped.
pub struct PooledLmdbReadTxn<'a> {
    txn: RoTransaction<'a>,
    state_db: Database,
    _permit: SemaphorePermit<'a>,
}

impl<'a> PooledLmdbReadTxn<'a> {
    /// Get state value by key
    pub fn get_state(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.txn.get(self.state_db, &key) {
            Ok(bytes) => Ok(Some(bytes.to_vec())),
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(AzothError::Transaction(e.to_string())),
        }
    }

    /// Check if a key exists
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        Ok(self.get_state(key)?.is_some())
    }
}

/// LMDB Read Transaction Pool
///
/// Manages a pool of read-only transaction slots using a semaphore.
/// This limits concurrent read transactions and provides timeout support.
///
/// # Example
///
/// ```ignore
/// let pool = LmdbReadPool::new(env.clone(), state_db, ReadPoolConfig::enabled(4));
///
/// // Acquire a pooled read transaction
/// let txn = pool.acquire().await?;
/// let value = txn.get_state(b"key")?;
/// // Transaction and permit are released when `txn` is dropped
/// ```
pub struct LmdbReadPool {
    env: Arc<Environment>,
    state_db: Database,
    semaphore: Arc<Semaphore>,
    acquire_timeout: Duration,
    enabled: bool,
}

impl LmdbReadPool {
    /// Create a new read pool with the given configuration
    pub fn new(env: Arc<Environment>, state_db: Database, config: ReadPoolConfig) -> Self {
        let pool_size = if config.enabled { config.pool_size } else { 1 };
        Self {
            env,
            state_db,
            semaphore: Arc::new(Semaphore::new(pool_size)),
            acquire_timeout: Duration::from_millis(config.acquire_timeout_ms),
            enabled: config.enabled,
        }
    }

    /// Acquire a pooled read-only transaction
    ///
    /// Waits up to the configured timeout for a slot to become available.
    /// Returns an error if the timeout is exceeded.
    pub async fn acquire(&self) -> Result<PooledLmdbReadTxn<'_>> {
        let permit = tokio::time::timeout(self.acquire_timeout, self.semaphore.acquire())
            .await
            .map_err(|_| {
                AzothError::Timeout(format!(
                    "Read pool acquire timeout after {:?}",
                    self.acquire_timeout
                ))
            })?
            .map_err(|e| AzothError::Internal(format!("Semaphore closed: {}", e)))?;

        let txn = self
            .env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        Ok(PooledLmdbReadTxn {
            txn,
            state_db: self.state_db,
            _permit: permit,
        })
    }

    /// Try to acquire a pooled read-only transaction without waiting
    ///
    /// Returns `None` if no slot is immediately available.
    pub fn try_acquire(&self) -> Result<Option<PooledLmdbReadTxn<'_>>> {
        match self.semaphore.try_acquire() {
            Ok(permit) => {
                let txn = self
                    .env
                    .begin_ro_txn()
                    .map_err(|e| AzothError::Transaction(e.to_string()))?;

                Ok(Some(PooledLmdbReadTxn {
                    txn,
                    state_db: self.state_db,
                    _permit: permit,
                }))
            }
            Err(_) => Ok(None),
        }
    }

    /// Acquire a pooled read-only transaction (blocking)
    ///
    /// This is a synchronous version that blocks the current thread using
    /// exponential backoff (1ms, 2ms, 4ms, ... capped at 32ms) up to the
    /// configured `acquire_timeout`.
    ///
    /// Prefer `acquire()` in async contexts.
    pub fn acquire_blocking(&self) -> Result<PooledLmdbReadTxn<'_>> {
        let deadline = std::time::Instant::now() + self.acquire_timeout;
        let mut backoff_ms = 1u64;
        const MAX_BACKOFF_MS: u64 = 32;

        let permit = loop {
            match self.semaphore.try_acquire() {
                Ok(permit) => break permit,
                Err(_) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(AzothError::Timeout(format!(
                            "LMDB read pool acquire timeout after {:?}",
                            self.acquire_timeout
                        )));
                    }
                    std::thread::sleep(Duration::from_millis(backoff_ms));
                    backoff_ms = (backoff_ms * 2).min(MAX_BACKOFF_MS);
                }
            }
        };

        let txn = self
            .env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        Ok(PooledLmdbReadTxn {
            txn,
            state_db: self.state_db,
            _permit: permit,
        })
    }

    /// Get the number of available slots in the pool
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Check if pooling is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lmdb::{DatabaseFlags, Environment, EnvironmentFlags, WriteFlags};
    use tempfile::TempDir;

    fn create_test_env() -> (TempDir, Arc<Environment>, Database) {
        let temp_dir = TempDir::new().unwrap();
        let mut builder = Environment::new();
        builder.set_max_dbs(1);
        builder.set_max_readers(10);
        builder.set_flags(EnvironmentFlags::empty());
        let env = builder.open(temp_dir.path()).unwrap();
        let db = env.create_db(Some("test"), DatabaseFlags::empty()).unwrap();
        (temp_dir, Arc::new(env), db)
    }

    #[tokio::test]
    async fn test_pool_acquire_release() {
        let (_temp_dir, env, db) = create_test_env();
        let config = ReadPoolConfig::enabled(2);
        let pool = LmdbReadPool::new(env, db, config);

        assert_eq!(pool.available_permits(), 2);

        // Acquire and release one transaction
        {
            let txn1 = pool.acquire().await.unwrap();
            assert_eq!(pool.available_permits(), 1);
            drop(txn1);
        }
        assert_eq!(pool.available_permits(), 2);

        // Acquire again - should work
        {
            let txn2 = pool.acquire().await.unwrap();
            assert_eq!(pool.available_permits(), 1);
            drop(txn2);
        }
        assert_eq!(pool.available_permits(), 2);
    }

    #[test]
    fn test_try_acquire() {
        let (_temp_dir, env, db) = create_test_env();
        let config = ReadPoolConfig::enabled(1);
        let pool = LmdbReadPool::new(env, db, config);

        // First try should succeed
        let txn = pool.try_acquire().unwrap();
        assert!(txn.is_some());

        // Second try should return None (semaphore exhausted)
        assert!(pool.try_acquire().unwrap().is_none());

        // After drop, try should succeed again
        drop(txn);
        assert!(pool.try_acquire().unwrap().is_some());
    }

    #[test]
    fn test_pool_get_state() {
        let (_temp_dir, env, db) = create_test_env();

        // Write some data first using the same environment
        {
            let mut txn = env.begin_rw_txn().unwrap();
            txn.put(db, b"key1", b"value1", WriteFlags::empty())
                .unwrap();
            txn.commit().unwrap();
        }

        let config = ReadPoolConfig::enabled(2);
        let pool = LmdbReadPool::new(env, db, config);

        let txn = pool.try_acquire().unwrap().unwrap();
        let value = txn.get_state(b"key1").unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        let missing = txn.get_state(b"nonexistent").unwrap();
        assert!(missing.is_none());
    }
}
