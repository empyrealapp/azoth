use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use xxhash_rust::xxh3::xxh3_64;

/// Lock manager for stripe locking during preflight phase
///
/// Provides per-key locking with configurable number of stripes.
/// Non-conflicting keys map to different stripes and can be locked concurrently.
pub struct LockManager {
    stripes: Vec<Arc<RwLock<()>>>,
    num_stripes: usize,
}

impl LockManager {
    /// Create a new lock manager with the specified number of stripes
    ///
    /// Common values: 256, 512, 1024
    /// More stripes = less contention but more memory
    pub fn new(num_stripes: usize) -> Self {
        assert!(num_stripes > 0, "num_stripes must be positive");
        let stripes = (0..num_stripes)
            .map(|_| Arc::new(RwLock::new(())))
            .collect();

        Self {
            stripes,
            num_stripes,
        }
    }

    /// Hash a key to determine its stripe index
    fn stripe_index(&self, key: &[u8]) -> usize {
        let hash = xxh3_64(key);
        (hash as usize) % self.num_stripes
    }

    /// Acquire read lock for a key (blocking)
    ///
    /// Multiple readers can hold locks on the same stripe concurrently.
    /// Used during preflight when reading state.
    ///
    /// This method blocks until the lock is acquired.
    pub fn read_lock(&self, key: &[u8]) -> RwLockReadGuard<'_, ()> {
        let idx = self.stripe_index(key);
        self.stripes[idx].read().expect("Lock poisoned")
    }

    /// Acquire write lock for a key (blocking)
    ///
    /// Only one writer can hold a lock on a stripe at a time.
    /// Used during preflight when validating writes.
    ///
    /// This method blocks until the lock is acquired.
    pub fn write_lock(&self, key: &[u8]) -> RwLockWriteGuard<'_, ()> {
        let idx = self.stripe_index(key);
        self.stripes[idx].write().expect("Lock poisoned")
    }

    /// Get the number of stripes
    pub fn num_stripes(&self) -> usize {
        self.num_stripes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_lock_manager_basic() {
        let lm = LockManager::new(256);
        assert_eq!(lm.num_stripes(), 256);

        // Should be able to acquire locks
        let _lock1 = lm.read_lock(b"key1");
        let _lock2 = lm.read_lock(b"key2");
    }

    #[test]
    fn test_stripe_distribution() {
        let lm = LockManager::new(256);

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
    fn test_concurrent_readers() {
        let lm = Arc::new(LockManager::new(256));

        // Multiple readers on same key should work
        let lm1 = lm.clone();
        let lm2 = lm.clone();

        let h1 = thread::spawn(move || {
            let _lock = lm1.read_lock(b"same_key");
            thread::sleep(Duration::from_millis(10));
        });

        let h2 = thread::spawn(move || {
            let _lock = lm2.read_lock(b"same_key");
            thread::sleep(Duration::from_millis(10));
        });

        // Both should complete without blocking
        h1.join().unwrap();
        h2.join().unwrap();
    }
}
