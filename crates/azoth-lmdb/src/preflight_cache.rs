//! In-memory cache for preflight validation reads.
//!
//! This module provides an optional cache to speed up preflight validation by caching
//! frequently accessed state keys. The cache is global, thread-safe, and supports:
//! - **FIFO eviction** (default) – oldest insertion is evicted first
//! - **LRU eviction** – least-recently-*read* key is evicted first
//! - TTL-based expiration for stale entries
//! - Invalidation on transaction commit for modified keys
//!
//! ## Choosing an eviction policy
//!
//! | Policy | Best for | Trade-off |
//! |--------|----------|-----------|
//! | `Fifo` | Write-heavy, short-lived keys (e.g. event processing) | Simpler, lower overhead |
//! | `Lru`  | Read-heavy, hot-key workloads (e.g. bank balances)    | Slightly more overhead per `get` |

use dashmap::DashMap;
use parking_lot::Mutex;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Eviction policy for the preflight cache.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum EvictionPolicy {
    /// First-In, First-Out: evict the oldest inserted key.
    #[default]
    Fifo,
    /// Least-Recently-Used: evict the key that hasn't been read the longest.
    Lru,
}

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

// ---------------------------------------------------------------------------
// LRU tracking with a doubly-linked list (intrusive, index-based)
// ---------------------------------------------------------------------------

/// Index into the LRU node slab.
type NodeIdx = usize;

/// A node in the LRU doubly-linked list.
struct LruNode {
    key: Vec<u8>,
    prev: Option<NodeIdx>,
    next: Option<NodeIdx>,
}

/// Minimal doubly-linked list for O(1) promote / evict-tail.
///
/// Stored behind a single `Mutex` alongside a key→index map so that
/// `promote` and `evict` are O(1) amortised.
struct LruList {
    nodes: Vec<LruNode>,
    /// Free-list of recycled node slots.
    free: Vec<NodeIdx>,
    head: Option<NodeIdx>,
    tail: Option<NodeIdx>,
    /// Reverse map: key → node index (for O(1) promote on read).
    index: HashMap<Vec<u8>, NodeIdx>,
}

impl LruList {
    fn with_capacity(cap: usize) -> Self {
        Self {
            nodes: Vec::with_capacity(cap),
            free: Vec::new(),
            head: None,
            tail: None,
            index: HashMap::with_capacity(cap),
        }
    }

    /// Push a key to the front (most-recently-used). Returns the node index.
    fn push_front(&mut self, key: Vec<u8>) -> NodeIdx {
        let idx = if let Some(free_idx) = self.free.pop() {
            self.nodes[free_idx] = LruNode {
                key: key.clone(),
                prev: None,
                next: self.head,
            };
            free_idx
        } else {
            let idx = self.nodes.len();
            self.nodes.push(LruNode {
                key: key.clone(),
                prev: None,
                next: self.head,
            });
            idx
        };

        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(idx);
        }
        self.head = Some(idx);
        if self.tail.is_none() {
            self.tail = Some(idx);
        }
        self.index.insert(key, idx);
        idx
    }

    /// Move an existing node to the front (most-recently-used).
    fn promote(&mut self, key: &[u8]) {
        let Some(&idx) = self.index.get(key) else {
            return;
        };
        if self.head == Some(idx) {
            return; // already at front
        }
        self.detach(idx);
        // Re-attach at head
        self.nodes[idx].prev = None;
        self.nodes[idx].next = self.head;
        if let Some(old_head) = self.head {
            self.nodes[old_head].prev = Some(idx);
        }
        self.head = Some(idx);
        if self.tail.is_none() {
            self.tail = Some(idx);
        }
    }

    /// Evict the tail (least-recently-used). Returns the evicted key.
    fn evict_tail(&mut self) -> Option<Vec<u8>> {
        let tail_idx = self.tail?;
        let key = self.nodes[tail_idx].key.clone();
        self.detach(tail_idx);
        self.index.remove(&key);
        self.free.push(tail_idx);
        Some(key)
    }

    /// Remove a specific key from the list.
    fn remove(&mut self, key: &[u8]) {
        if let Some(idx) = self.index.remove(key) {
            self.detach(idx);
            self.free.push(idx);
        }
    }

    /// Clear the entire list.
    fn clear(&mut self) {
        self.nodes.clear();
        self.free.clear();
        self.head = None;
        self.tail = None;
        self.index.clear();
    }

    /// Detach a node from the linked list (does NOT remove from index/free-list).
    fn detach(&mut self, idx: NodeIdx) {
        let prev = self.nodes[idx].prev;
        let next = self.nodes[idx].next;

        if let Some(p) = prev {
            self.nodes[p].next = next;
        } else {
            self.head = next;
        }
        if let Some(n) = next {
            self.nodes[n].prev = prev;
        } else {
            self.tail = prev;
        }
        self.nodes[idx].prev = None;
        self.nodes[idx].next = None;
    }
}

/// Thread-safe in-memory cache for preflight reads.
///
/// Uses `DashMap` for lock-free concurrent value access and a secondary
/// structure (FIFO queue or LRU list) for bounded eviction.
pub struct PreflightCache {
    cache: Arc<DashMap<Vec<u8>, CacheEntry>>,
    capacity: usize,
    ttl: Duration,
    enabled: bool,
    policy: EvictionPolicy,
    /// FIFO queue (used when policy == Fifo).
    fifo_queue: Arc<Mutex<VecDeque<Vec<u8>>>>,
    /// LRU list (used when policy == Lru).
    lru_list: Arc<Mutex<LruList>>,
}

impl PreflightCache {
    /// Create a new preflight cache with the given configuration.
    ///
    /// Uses the default FIFO eviction policy.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of entries (default: 10,000)
    /// * `ttl_secs` - Time-to-live for entries in seconds (default: 60)
    /// * `enabled` - Whether the cache is enabled (default: true)
    pub fn new(capacity: usize, ttl_secs: u64, enabled: bool) -> Self {
        Self::with_policy(capacity, ttl_secs, enabled, EvictionPolicy::Fifo)
    }

    /// Create a new preflight cache with an explicit eviction policy.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of entries
    /// * `ttl_secs` - Time-to-live for entries in seconds
    /// * `enabled` - Whether the cache is enabled
    /// * `policy`  - Eviction policy (`Fifo` or `Lru`)
    pub fn with_policy(
        capacity: usize,
        ttl_secs: u64,
        enabled: bool,
        policy: EvictionPolicy,
    ) -> Self {
        Self {
            cache: Arc::new(DashMap::with_capacity(capacity)),
            capacity,
            ttl: Duration::from_secs(ttl_secs),
            enabled,
            policy,
            fifo_queue: Arc::new(Mutex::new(VecDeque::with_capacity(capacity))),
            lru_list: Arc::new(Mutex::new(LruList::with_capacity(capacity))),
        }
    }

    /// Create a disabled cache (all operations are no-ops).
    pub fn disabled() -> Self {
        Self::new(0, 0, false)
    }

    /// Get a value from the cache if it exists and hasn't expired.
    ///
    /// When LRU eviction is active, this promotes the key to
    /// most-recently-used.
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
            return None;
        }

        let value = entry.value.clone();
        drop(entry);

        // LRU: promote on read
        if self.policy == EvictionPolicy::Lru {
            self.lru_list.lock().promote(key);
        }

        Some(value)
    }

    /// Insert a value into the cache.
    ///
    /// If the cache is at capacity, evicts according to the active policy.
    /// This is a no-op if the cache is disabled.
    pub fn insert(&self, key: Vec<u8>, value: CachedValue) {
        if !self.enabled {
            return;
        }

        let is_new = !self.cache.contains_key(&key);

        if self.cache.len() >= self.capacity && is_new {
            self.evict_one();
        }

        self.cache.insert(key.clone(), CacheEntry::new(value));

        match self.policy {
            EvictionPolicy::Fifo => {
                self.fifo_queue.lock().push_back(key);
            }
            EvictionPolicy::Lru => {
                let mut lru = self.lru_list.lock();
                // Remove old entry if updating, then push to front.
                if !is_new {
                    lru.remove(&key);
                }
                lru.push_front(key);
            }
        }
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
            if self.policy == EvictionPolicy::Lru {
                self.lru_list.lock().remove(key);
            }
        }
    }

    /// Evict one key from the cache.
    fn evict_one(&self) {
        match self.policy {
            EvictionPolicy::Fifo => self.evict_fifo(),
            EvictionPolicy::Lru => self.evict_lru(),
        }
    }

    /// FIFO eviction: pop from front of queue, skip stale duplicates.
    fn evict_fifo(&self) {
        let mut queue = self.fifo_queue.lock();
        while let Some(key) = queue.pop_front() {
            if self.cache.remove(&key).is_some() {
                break;
            }
        }
    }

    /// LRU eviction: evict the tail (least-recently-used).
    fn evict_lru(&self) {
        let mut lru = self.lru_list.lock();
        if let Some(key) = lru.evict_tail() {
            self.cache.remove(&key);
        }
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        if !self.enabled {
            return;
        }

        self.cache.clear();
        self.fifo_queue.lock().clear();
        self.lru_list.lock().clear();
    }

    /// Get cache statistics.
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            size: self.cache.len(),
            capacity: self.capacity,
            enabled: self.enabled,
            policy: self.policy,
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
    /// Active eviction policy
    pub policy: EvictionPolicy,
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
                    cache_clone.insert(
                        key.as_bytes().to_vec(),
                        CachedValue::Some(value.as_bytes().to_vec()),
                    );

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

    // -----------------------------------------------------------------------
    // LRU-specific tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_lru_eviction_keeps_hot_keys() {
        let cache = PreflightCache::with_policy(3, 60, true, EvictionPolicy::Lru);

        cache.insert(b"a".to_vec(), CachedValue::Some(b"1".to_vec()));
        cache.insert(b"b".to_vec(), CachedValue::Some(b"2".to_vec()));
        cache.insert(b"c".to_vec(), CachedValue::Some(b"3".to_vec()));

        // Read "a" to make it most-recently-used
        assert!(cache.get(b"a").is_some());

        // Insert "d" — should evict "b" (least-recently-used), NOT "a"
        cache.insert(b"d".to_vec(), CachedValue::Some(b"4".to_vec()));

        assert!(cache.get(b"a").is_some(), "Hot key 'a' should survive");
        assert!(cache.get(b"b").is_none(), "'b' should be evicted (LRU)");
        assert!(cache.get(b"c").is_some());
        assert!(cache.get(b"d").is_some());
    }

    #[test]
    fn test_lru_eviction_order() {
        let cache = PreflightCache::with_policy(2, 60, true, EvictionPolicy::Lru);

        cache.insert(b"x".to_vec(), CachedValue::Some(b"1".to_vec()));
        cache.insert(b"y".to_vec(), CachedValue::Some(b"2".to_vec()));

        // Both present
        assert!(cache.get(b"x").is_some());
        assert!(cache.get(b"y").is_some());

        // Insert "z" — "x" was read before "y", so "x" is LRU after both
        // reads. But "y" was read after "x" above, so the actual LRU is "x".
        // Wait—let's be explicit: read order was x then y. After reads:
        // MRU → y → x → LRU. So evicting should remove "x".
        cache.insert(b"z".to_vec(), CachedValue::Some(b"3".to_vec()));

        assert!(cache.get(b"x").is_none(), "'x' should be evicted");
        assert!(cache.get(b"y").is_some());
        assert!(cache.get(b"z").is_some());
    }

    #[test]
    fn test_lru_invalidation() {
        let cache = PreflightCache::with_policy(100, 60, true, EvictionPolicy::Lru);

        cache.insert(b"a".to_vec(), CachedValue::Some(b"1".to_vec()));
        cache.insert(b"b".to_vec(), CachedValue::Some(b"2".to_vec()));

        cache.invalidate_keys(&[b"a".to_vec()]);

        assert!(cache.get(b"a").is_none());
        assert!(cache.get(b"b").is_some());
        assert_eq!(cache.stats().size, 1);
    }

    #[test]
    fn test_lru_stats_show_policy() {
        let fifo = PreflightCache::new(10, 60, true);
        assert_eq!(fifo.stats().policy, EvictionPolicy::Fifo);

        let lru = PreflightCache::with_policy(10, 60, true, EvictionPolicy::Lru);
        assert_eq!(lru.stats().policy, EvictionPolicy::Lru);
    }
}
