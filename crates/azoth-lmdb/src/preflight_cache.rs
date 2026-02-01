//! In-memory cache for preflight validation reads.
//!
//! This module provides an optional cache to speed up preflight validation by caching
//! frequently accessed state keys. The cache is global, thread-safe, and supports:
//! - LRU eviction when capacity is reached
//! - TTL-based expiration for stale entries
//! - Invalidation on transaction commit for modified keys

use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// A cached value, representing either a found value or a non-existent key.
#[derive(Clone, Debug)]
pub enum CachedValue {
    /// Key exists with this value (raw bytes)
    Some(Vec<u8>),
    /// Key does not exist
    None,
}

/// An entry in the preflight cache.
#[derive(Clone, Debug)]
struct CacheEntry {
    value: CachedValue,
    inserted_at: Instant,
}

impl CacheEntry {
    fn new(value: CachedValue) -> Self {
        Self {
            value,
            inserted_at: Instant::now(),
        }
    }

    fn is_expired(&self, ttl: Duration) -> bool {
        self.inserted_at.elapsed() > ttl
    }
}

/// Thread-safe in-memory cache for preflight reads.
///
/// Uses DashMap for lock-free concurrent access and tracks insertion order
/// for LRU eviction.
pub struct PreflightCache {
    cache: Arc<DashMap<Vec<u8>, CacheEntry>>,
    capacity: usize,
    ttl: Duration,
    enabled: bool,
    /// Track insertion order for LRU eviction (key, inserted_at)
    lru_tracker: Arc<DashMap<Vec<u8>, Instant>>,
}

impl PreflightCache {
    /// Create a new preflight cache with the given configuration.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of entries (default: 10,000)
    /// * `ttl_secs` - Time-to-live for entries in seconds (default: 60)
    /// * `enabled` - Whether the cache is enabled (default: true)
    pub fn new(capacity: usize, ttl_secs: u64, enabled: bool) -> Self {
        Self {
            cache: Arc::new(DashMap::with_capacity(capacity)),
            capacity,
            ttl: Duration::from_secs(ttl_secs),
            enabled,
            lru_tracker: Arc::new(DashMap::with_capacity(capacity)),
        }
    }

    /// Create a disabled cache (all operations are no-ops).
    pub fn disabled() -> Self {
        Self::new(0, 0, false)
    }

    /// Get a value from the cache if it exists and hasn't expired.
    ///
    /// Returns `None` if the cache is disabled, the key is not in the cache,
    /// or the entry has expired.
    pub fn get(&self, key: &[u8]) -> Option<CachedValue> {
        if !self.enabled {
            return None;
        }

        let entry = self.cache.get(key)?;

        // Check if expired
        if entry.is_expired(self.ttl) {
            drop(entry); // Release read lock
            self.cache.remove(key);
            self.lru_tracker.remove(key);
            return None;
        }

        Some(entry.value.clone())
    }

    /// Insert a value into the cache.
    ///
    /// If the cache is at capacity, evicts the least recently used entry.
    /// This is a no-op if the cache is disabled.
    pub fn insert(&self, key: Vec<u8>, value: CachedValue) {
        if !self.enabled {
            return;
        }

        // Check if we need to evict
        if self.cache.len() >= self.capacity && !self.cache.contains_key(&key) {
            self.evict_lru();
        }

        let now = Instant::now();
        self.cache.insert(key.clone(), CacheEntry::new(value));
        self.lru_tracker.insert(key, now);
    }

    /// Invalidate (remove) specific keys from the cache.
    ///
    /// Used when keys are modified during transaction commit.
    pub fn invalidate_keys(&self, keys: &[Vec<u8>]) {
        if !self.enabled {
            return;
        }

        for key in keys {
            self.cache.remove(key);
            self.lru_tracker.remove(key);
        }
    }

    /// Evict the least recently used entry from the cache.
    fn evict_lru(&self) {
        if let Some(oldest_key) = self.find_oldest_key() {
            self.cache.remove(&oldest_key);
            self.lru_tracker.remove(&oldest_key);
        }
    }

    /// Find the key with the oldest insertion time.
    fn find_oldest_key(&self) -> Option<Vec<u8>> {
        self.lru_tracker
            .iter()
            .min_by_key(|entry| *entry.value())
            .map(|entry| entry.key().clone())
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        if !self.enabled {
            return;
        }

        self.cache.clear();
        self.lru_tracker.clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.len(),
            capacity: self.capacity,
            enabled: self.enabled,
        }
    }
}

/// Statistics about the cache state.
#[derive(Debug, Clone)]
pub struct CacheStats {
    /// Current number of entries in the cache
    pub size: usize,
    /// Maximum capacity
    pub capacity: usize,
    /// Whether the cache is enabled
    pub enabled: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_cache_basic_operations() {
        let cache = PreflightCache::new(100, 60, true);

        // Cache miss
        assert!(cache.get(b"key1").is_none());

        // Insert and retrieve
        cache.insert(b"key1".to_vec(), CachedValue::Some(b"value1".to_vec()));
        match cache.get(b"key1") {
            Some(CachedValue::Some(v)) => assert_eq!(v, b"value1"),
            _ => panic!("Expected Some(value1)"),
        }

        // Insert None (non-existent key)
        cache.insert(b"key2".to_vec(), CachedValue::None);
        match cache.get(b"key2") {
            Some(CachedValue::None) => {}
            _ => panic!("Expected None value"),
        }
    }

    #[test]
    fn test_cache_eviction() {
        let cache = PreflightCache::new(3, 60, true);

        // Fill cache to capacity
        cache.insert(b"key1".to_vec(), CachedValue::Some(b"value1".to_vec()));
        thread::sleep(Duration::from_millis(10));
        cache.insert(b"key2".to_vec(), CachedValue::Some(b"value2".to_vec()));
        thread::sleep(Duration::from_millis(10));
        cache.insert(b"key3".to_vec(), CachedValue::Some(b"value3".to_vec()));

        assert_eq!(cache.stats().size, 3);

        // Insert one more, should evict key1 (oldest)
        thread::sleep(Duration::from_millis(10));
        cache.insert(b"key4".to_vec(), CachedValue::Some(b"value4".to_vec()));

        // key1 should be evicted
        assert!(cache.get(b"key1").is_none());
        // key2, key3, key4 should still be present
        assert!(cache.get(b"key2").is_some());
        assert!(cache.get(b"key3").is_some());
        assert!(cache.get(b"key4").is_some());
    }

    #[test]
    fn test_cache_invalidation() {
        let cache = PreflightCache::new(100, 60, true);

        cache.insert(b"key1".to_vec(), CachedValue::Some(b"value1".to_vec()));
        cache.insert(b"key2".to_vec(), CachedValue::Some(b"value2".to_vec()));
        cache.insert(b"key3".to_vec(), CachedValue::Some(b"value3".to_vec()));

        // Invalidate key2
        cache.invalidate_keys(&[b"key2".to_vec()]);

        assert!(cache.get(b"key1").is_some());
        assert!(cache.get(b"key2").is_none());
        assert!(cache.get(b"key3").is_some());
    }

    #[test]
    fn test_cache_ttl_expiration() {
        let cache = PreflightCache::new(100, 1, true); // 1 second TTL

        cache.insert(b"key1".to_vec(), CachedValue::Some(b"value1".to_vec()));

        // Should be available immediately
        assert!(cache.get(b"key1").is_some());

        // Wait for expiration
        thread::sleep(Duration::from_secs(2));

        // Should be expired now
        assert!(cache.get(b"key1").is_none());
    }

    #[test]
    fn test_cache_disabled() {
        let cache = PreflightCache::disabled();

        // All operations should be no-ops
        cache.insert(b"key1".to_vec(), CachedValue::Some(b"value1".to_vec()));
        assert!(cache.get(b"key1").is_none());

        let stats = cache.stats();
        assert!(!stats.enabled);
        assert_eq!(stats.size, 0);
    }

    #[test]
    fn test_cache_clear() {
        let cache = PreflightCache::new(100, 60, true);

        cache.insert(b"key1".to_vec(), CachedValue::Some(b"value1".to_vec()));
        cache.insert(b"key2".to_vec(), CachedValue::Some(b"value2".to_vec()));

        assert_eq!(cache.stats().size, 2);

        cache.clear();

        assert_eq!(cache.stats().size, 0);
        assert!(cache.get(b"key1").is_none());
        assert!(cache.get(b"key2").is_none());
    }

    #[test]
    fn test_cache_concurrent_access() {
        let cache = Arc::new(PreflightCache::new(1000, 60, true));
        let mut handles = vec![];

        // Spawn multiple threads doing concurrent reads/writes
        for i in 0..10 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key-{}-{}", i, j);
                    let value = format!("value-{}-{}", i, j);
                    cache_clone.insert(key.as_bytes().to_vec(), CachedValue::Some(value.as_bytes().to_vec()));

                    // Try to read it back
                    cache_clone.get(key.as_bytes());
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Cache should have entries (up to capacity)
        let stats = cache.stats();
        assert!(stats.size > 0);
        assert!(stats.size <= stats.capacity);
    }
}
