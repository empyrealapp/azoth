//! Stripe-based lock manager with deadlock prevention
//!
//! Provides per-key locking with automatic sorting to prevent deadlocks.
//! Keys are hashed to stripes, and locks are acquired in stripe index order.

use crate::error::{AzothError, Result};
use parking_lot::{Mutex, MutexGuard};
use std::collections::BTreeSet;
use std::time::Duration;
use xxhash_rust::xxh3::xxh3_64;

/// Default lock acquisition timeout (5 seconds)
pub const DEFAULT_LOCK_TIMEOUT_MS: u64 = 5000;

/// Lock manager for stripe locking during preflight phase
///
/// Provides per-key locking with configurable number of stripes.
/// Non-conflicting keys map to different stripes and can be locked concurrently.
///
/// # Deadlock Prevention
///
/// This implementation prevents deadlocks through two mechanisms:
///
/// 1. **Automatic Sorting**: When acquiring multiple locks via `acquire_keys()`,
///    stripes are always acquired in ascending index order. This prevents the
///    classic A-B/B-A deadlock scenario.
///
/// 2. **Timeout-Based Acquisition**: Uses `try_lock_for()` with a configurable
///    timeout instead of blocking indefinitely. If a lock cannot be acquired
///    within the timeout, returns `LockTimeout` error.
///
/// # Example
///
/// ```ignore
/// let lm = LockManager::new(256, Duration::from_secs(5));
///
/// // Acquire locks on multiple keys - automatically sorted to prevent deadlock
/// let _guard = lm.acquire_keys(&[b"key_b", b"key_a"])?;
/// // Locks acquired in stripe order, not key order
/// ```
pub struct LockManager {
    stripes: Vec<Mutex<()>>,
    num_stripes: usize,
    default_timeout: Duration,
}

/// Guard that holds multiple stripe locks
///
/// Locks are held in sorted stripe order and released in reverse order
/// when dropped (via RAII).
pub struct MultiLockGuard<'a> {
    // Stored in sorted stripe order; dropped in reverse order automatically
    _guards: Vec<MutexGuard<'a, ()>>,
}

impl LockManager {
    /// Create a new lock manager with the specified number of stripes
    ///
    /// # Arguments
    ///
    /// * `num_stripes` - Number of lock stripes (common values: 256, 512, 1024).
    ///   More stripes = less contention but more memory.
    /// * `default_timeout` - Default timeout for lock acquisition.
    ///
    /// # Panics
    ///
    /// Panics if `num_stripes` is 0.
    pub fn new(num_stripes: usize, default_timeout: Duration) -> Self {
        assert!(num_stripes > 0, "num_stripes must be positive");
        let stripes = (0..num_stripes).map(|_| Mutex::new(())).collect();

        Self {
            stripes,
            num_stripes,
            default_timeout,
        }
    }

    /// Create a lock manager with default timeout
    pub fn with_stripes(num_stripes: usize) -> Self {
        Self::new(num_stripes, Duration::from_millis(DEFAULT_LOCK_TIMEOUT_MS))
    }

    /// Hash a key to determine its stripe index
    fn stripe_index(&self, key: &[u8]) -> usize {
        let hash = xxh3_64(key);
        (hash as usize) % self.num_stripes
    }

    /// Acquire exclusive locks on all keys in a deadlock-free manner
    ///
    /// This method:
    /// 1. Computes stripe indices for all keys
    /// 2. Deduplicates stripes (multiple keys may map to same stripe)
    /// 3. Sorts stripe indices for consistent global ordering
    /// 4. Acquires locks in sorted order with timeout
    ///
    /// # Arguments
    ///
    /// * `keys` - Keys to acquire locks for
    ///
    /// # Returns
    ///
    /// A `MultiLockGuard` that holds all acquired locks. Locks are released
    /// when the guard is dropped.
    ///
    /// # Errors
    ///
    /// Returns `LockTimeout` if any lock cannot be acquired within the timeout.
    ///
    /// # Deadlock Safety
    ///
    /// Because locks are always acquired in sorted stripe order, two threads
    /// acquiring locks on keys [A, B] and [B, A] will both attempt to acquire
    /// locks in the same order, preventing deadlock.
    pub fn acquire_keys<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<MultiLockGuard<'_>> {
        self.acquire_keys_with_timeout(keys, self.default_timeout)
    }

    /// Acquire exclusive locks with a custom timeout
    pub fn acquire_keys_with_timeout<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
        timeout: Duration,
    ) -> Result<MultiLockGuard<'_>> {
        // Early return for empty keys
        if keys.is_empty() {
            return Ok(MultiLockGuard {
                _guards: Vec::new(),
            });
        }

        // Compute stripe indices and deduplicate using BTreeSet (automatically sorted)
        let stripe_indices: BTreeSet<usize> =
            keys.iter().map(|k| self.stripe_index(k.as_ref())).collect();

        // Acquire locks in sorted order
        let mut guards = Vec::with_capacity(stripe_indices.len());

        for stripe_idx in stripe_indices {
            match self.stripes[stripe_idx].try_lock_for(timeout) {
                Some(guard) => guards.push(guard),
                None => {
                    // Failed to acquire lock - drop all acquired locks and return error
                    // Guards are dropped automatically when `guards` goes out of scope
                    return Err(AzothError::LockTimeout {
                        timeout_ms: timeout.as_millis() as u64,
                    });
                }
            }
        }

        Ok(MultiLockGuard { _guards: guards })
    }

    /// Acquire a single lock (convenience method)
    ///
    /// Prefer `acquire_keys()` for multiple keys to ensure deadlock safety.
    pub fn lock(&self, key: &[u8]) -> Result<MutexGuard<'_, ()>> {
        let idx = self.stripe_index(key);
        self.stripes[idx]
            .try_lock_for(self.default_timeout)
            .ok_or(AzothError::LockTimeout {
                timeout_ms: self.default_timeout.as_millis() as u64,
            })
    }

    /// Get the number of stripes
    pub fn num_stripes(&self) -> usize {
        self.num_stripes
    }

    /// Get the default timeout
    pub fn default_timeout(&self) -> Duration {
        self.default_timeout
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_lock_manager_basic() {
        let lm = LockManager::with_stripes(256);
        assert_eq!(lm.num_stripes(), 256);

        // Should be able to acquire locks
        let _guard = lm.acquire_keys(&[b"key1", b"key2"]).unwrap();
    }

    #[test]
    fn test_stripe_distribution() {
        let lm = LockManager::with_stripes(256);

        // Different keys should (usually) map to different stripes
        let idx1 = lm.stripe_index(b"key1");
        let idx2 = lm.stripe_index(b"key2");
        let idx3 = lm.stripe_index(b"key3");

        assert!(idx1 < 256);
        assert!(idx2 < 256);
        assert!(idx3 < 256);

        // Same key should always map to same stripe
        assert_eq!(idx1, lm.stripe_index(b"key1"));
    }

    #[test]
    fn test_empty_keys() {
        let lm = LockManager::with_stripes(256);
        let _guard = lm.acquire_keys::<&[u8]>(&[]).unwrap();
    }

    #[test]
    fn test_duplicate_keys_deduplicated() {
        let lm = LockManager::with_stripes(256);

        // Same key multiple times should only lock once
        let _guard = lm.acquire_keys(&[b"key1", b"key1", b"key1"]).unwrap();
    }

    #[test]
    fn test_sorted_acquisition_prevents_deadlock() {
        // This test verifies that unsorted key order doesn't cause deadlock
        let lm = Arc::new(LockManager::with_stripes(256));

        let lm1 = lm.clone();
        let lm2 = lm.clone();

        // Thread 1: keys in order [a, b]
        let h1 = thread::spawn(move || {
            for _ in 0..100 {
                let _guard = lm1.acquire_keys(&[b"key_a", b"key_b"]).unwrap();
                // Hold briefly
                thread::sleep(Duration::from_micros(10));
            }
        });

        // Thread 2: keys in REVERSE order [b, a]
        // Without sorted acquisition, this would deadlock
        let h2 = thread::spawn(move || {
            for _ in 0..100 {
                let _guard = lm2.acquire_keys(&[b"key_b", b"key_a"]).unwrap();
                thread::sleep(Duration::from_micros(10));
            }
        });

        // Both should complete without deadlock
        h1.join().unwrap();
        h2.join().unwrap();
    }

    #[test]
    fn test_timeout_works() {
        let lm = Arc::new(LockManager::new(1, Duration::from_millis(50)));

        // Acquire the only stripe
        let _guard = lm.acquire_keys(&[b"any_key"]).unwrap();

        // Try to acquire from another thread - should timeout
        let lm2 = lm.clone();
        let handle = thread::spawn(move || {
            // Check if result is a timeout error (don't return guard across threads)
            matches!(
                lm2.acquire_keys(&[b"another_key"]),
                Err(AzothError::LockTimeout { .. })
            )
        });

        let timed_out = handle.join().unwrap();
        assert!(timed_out, "Should have timed out");
    }

    #[test]
    fn test_concurrent_different_stripes() {
        // Keys on different stripes should be acquired concurrently
        let lm = Arc::new(LockManager::with_stripes(256));

        let handles: Vec<_> = (0..10)
            .map(|i| {
                let lm = lm.clone();
                thread::spawn(move || {
                    let key = format!("unique_key_{}", i);
                    let _guard = lm.acquire_keys(&[key.as_bytes()]).unwrap();
                    thread::sleep(Duration::from_millis(10));
                })
            })
            .collect();

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
