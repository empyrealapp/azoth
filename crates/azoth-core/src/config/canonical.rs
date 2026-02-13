use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for read connection pooling
///
/// When enabled, maintains a pool of read-only connections/transactions
/// for concurrent read access without blocking writes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReadPoolConfig {
    /// Whether pooling is enabled (default: false, opt-in)
    #[serde(default)]
    pub enabled: bool,

    /// Number of read connections/slots in the pool (default: 4)
    ///
    /// For LMDB: controls concurrent read transaction slots
    /// For SQLite: controls number of read-only connections
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,

    /// Timeout in milliseconds when acquiring a pooled connection (default: 5000)
    ///
    /// If no connection is available within this time, an error is returned.
    #[serde(default = "default_acquire_timeout")]
    pub acquire_timeout_ms: u64,
}

impl Default for ReadPoolConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            pool_size: default_pool_size(),
            acquire_timeout_ms: default_acquire_timeout(),
        }
    }
}

impl ReadPoolConfig {
    /// Create a new enabled read pool configuration
    pub fn enabled(pool_size: usize) -> Self {
        Self {
            enabled: true,
            pool_size,
            acquire_timeout_ms: default_acquire_timeout(),
        }
    }

    /// Set the acquire timeout
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.acquire_timeout_ms = timeout_ms;
        self
    }
}

fn default_pool_size() -> usize {
    4
}

fn default_acquire_timeout() -> u64 {
    5000
}

/// Configuration for canonical store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalConfig {
    /// Path to the canonical store directory
    pub path: PathBuf,

    /// Maximum map size for LMDB (in bytes)
    /// Default: 10GB
    #[serde(default = "default_map_size")]
    pub map_size: usize,

    /// Sync mode for durability
    #[serde(default)]
    pub sync_mode: SyncMode,

    /// Number of stripes for lock manager
    /// Default: 256
    #[serde(default = "default_stripe_count")]
    pub stripe_count: usize,

    /// Maximum number of readers (LMDB specific)
    /// Default: 126
    #[serde(default = "default_max_readers")]
    pub max_readers: u32,

    /// Use read-only transactions for preflight (default: true)
    ///
    /// When enabled, preflight validation uses concurrent read-only transactions
    /// instead of write transactions, improving throughput and reducing contention.
    #[serde(default = "default_true")]
    pub preflight_read_only: bool,

    /// Chunk size for state iteration (default: 1000)
    ///
    /// State iterators fetch data in chunks to maintain constant memory usage.
    /// Larger chunks improve throughput but use more memory temporarily.
    #[serde(default = "default_chunk_size")]
    pub state_iter_chunk_size: usize,

    /// Enable in-memory cache for preflight validation (default: true)
    ///
    /// When enabled, frequently accessed state keys are cached in memory during
    /// preflight validation, reducing LMDB reads for hot keys.
    #[serde(default = "default_true")]
    pub preflight_cache_enabled: bool,

    /// Maximum number of entries in the preflight cache (default: 10,000)
    ///
    /// Each entry uses approximately 120 bytes + value size.
    /// Default of 10,000 entries ≈ 1-6 MB memory overhead.
    #[serde(default = "default_preflight_cache_size")]
    pub preflight_cache_size: usize,

    /// Time-to-live for preflight cache entries in seconds (default: 60)
    ///
    /// Entries older than this will be evicted on access.
    #[serde(default = "default_preflight_cache_ttl")]
    pub preflight_cache_ttl_secs: u64,

    /// Read pool configuration (optional, disabled by default)
    ///
    /// When enabled, maintains a pool of read-only connections for
    /// concurrent read access without blocking writes.
    #[serde(default)]
    pub read_pool: ReadPoolConfig,

    /// Lock acquisition timeout in milliseconds (default: 5000)
    ///
    /// When acquiring stripe locks for transaction preflight, if a lock
    /// cannot be acquired within this timeout, the transaction fails with
    /// `LockTimeout` error instead of blocking indefinitely.
    #[serde(default = "default_lock_timeout")]
    pub lock_timeout_ms: u64,

    /// Maximum size of a single event payload in bytes (default: 4MB)
    #[serde(default = "default_event_max_size")]
    pub event_max_size_bytes: usize,

    /// Maximum total size for a single event batch append in bytes (default: 64MB)
    #[serde(default = "default_event_batch_max_bytes")]
    pub event_batch_max_bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Full durability – calls `fsync()` on every commit.
    ///
    /// Guarantees that committed data survives power loss and OS crashes.
    /// This is the safest option but has the highest write latency (~2-5x slower
    /// than `NoMetaSync`).
    Full,

    /// Skips syncing the LMDB meta-page on each commit (default).
    ///
    /// Data pages are still synced, so committed data is durable against process
    /// crashes. In the rare event of an OS crash or power failure, the last
    /// transaction _may_ be lost, but the database will remain consistent.
    /// This is a good balance of durability and performance for most workloads.
    #[default]
    NoMetaSync,

    /// Disables `fsync()` entirely – the OS page cache decides when to flush.
    ///
    /// **WARNING**: This is the fastest mode but offers no durability guarantees
    /// beyond normal process lifetime. A power failure or OS crash can lose an
    /// unbounded number of recent transactions or, in the worst case, corrupt the
    /// database file. Only use this for ephemeral, reproducible, or test workloads.
    NoSync,
}

fn default_map_size() -> usize {
    10 * 1024 * 1024 * 1024 // 10GB
}

fn default_stripe_count() -> usize {
    256
}

fn default_max_readers() -> u32 {
    126
}

fn default_true() -> bool {
    true
}

fn default_chunk_size() -> usize {
    1000
}

fn default_preflight_cache_size() -> usize {
    10_000
}

fn default_preflight_cache_ttl() -> u64 {
    60
}

fn default_lock_timeout() -> u64 {
    5000
}

fn default_event_max_size() -> usize {
    4 * 1024 * 1024
}

fn default_event_batch_max_bytes() -> usize {
    64 * 1024 * 1024
}

impl CanonicalConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            map_size: default_map_size(),
            sync_mode: SyncMode::default(),
            stripe_count: default_stripe_count(),
            max_readers: default_max_readers(),
            preflight_read_only: default_true(),
            state_iter_chunk_size: default_chunk_size(),
            preflight_cache_enabled: default_true(),
            preflight_cache_size: default_preflight_cache_size(),
            preflight_cache_ttl_secs: default_preflight_cache_ttl(),
            read_pool: ReadPoolConfig::default(),
            lock_timeout_ms: default_lock_timeout(),
            event_max_size_bytes: default_event_max_size(),
            event_batch_max_bytes: default_event_batch_max_bytes(),
        }
    }

    pub fn with_map_size(mut self, map_size: usize) -> Self {
        self.map_size = map_size;
        self
    }

    pub fn with_sync_mode(mut self, sync_mode: SyncMode) -> Self {
        self.sync_mode = sync_mode;
        self
    }

    pub fn with_stripe_count(mut self, stripe_count: usize) -> Self {
        self.stripe_count = stripe_count;
        self
    }

    pub fn with_preflight_cache(mut self, enabled: bool) -> Self {
        self.preflight_cache_enabled = enabled;
        self
    }

    pub fn with_preflight_cache_size(mut self, size: usize) -> Self {
        self.preflight_cache_size = size;
        self
    }

    pub fn with_preflight_cache_ttl(mut self, ttl_secs: u64) -> Self {
        self.preflight_cache_ttl_secs = ttl_secs;
        self
    }

    /// Configure read connection pooling
    pub fn with_read_pool(mut self, config: ReadPoolConfig) -> Self {
        self.read_pool = config;
        self
    }

    /// Enable read pooling with the specified pool size
    pub fn with_read_pool_size(mut self, pool_size: usize) -> Self {
        self.read_pool = ReadPoolConfig::enabled(pool_size);
        self
    }

    /// Set lock acquisition timeout in milliseconds
    ///
    /// This controls how long to wait when acquiring stripe locks for
    /// transaction preflight. Default is 5000ms.
    pub fn with_lock_timeout(mut self, timeout_ms: u64) -> Self {
        self.lock_timeout_ms = timeout_ms;
        self
    }

    /// Set maximum event payload size in bytes.
    pub fn with_event_max_size(mut self, size_bytes: usize) -> Self {
        self.event_max_size_bytes = size_bytes;
        self
    }

    /// Set maximum event batch size in bytes.
    pub fn with_event_batch_max_bytes(mut self, size_bytes: usize) -> Self {
        self.event_batch_max_bytes = size_bytes;
        self
    }
}
