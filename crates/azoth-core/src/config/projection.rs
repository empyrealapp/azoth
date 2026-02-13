use super::canonical::ReadPoolConfig;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Configuration for projection store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectionConfig {
    /// Path to the SQLite database file
    pub path: PathBuf,

    /// Enable WAL mode
    /// Default: true
    #[serde(default = "default_wal_mode")]
    pub wal_mode: bool,

    /// SQLite synchronous mode
    #[serde(default)]
    pub synchronous: SynchronousMode,

    /// Target schema version for migrations
    /// Default: 1
    #[serde(default = "default_schema_version")]
    pub schema_version: u32,

    /// SQLite cache size (in pages, negative = KB)
    /// Default: -64000 (64MB)
    #[serde(default = "default_cache_size")]
    pub cache_size: i32,

    /// Read pool configuration (enabled by default with 4 connections).
    ///
    /// Maintains a pool of read-only connections for concurrent read access
    /// without blocking writes. Disable with `ReadPoolConfig::default()` if
    /// you need single-connection behavior.
    #[serde(default = "default_read_pool")]
    pub read_pool: ReadPoolConfig,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SynchronousMode {
    /// Full fsync (safest, slowest)
    Full,
    /// fsync at critical moments (good balance)
    #[default]
    Normal,
    /// No fsync (fastest, least safe)
    Off,
}

fn default_wal_mode() -> bool {
    true
}

fn default_schema_version() -> u32 {
    1
}

fn default_cache_size() -> i32 {
    -64000 // 64MB
}

fn default_read_pool() -> ReadPoolConfig {
    ReadPoolConfig::enabled(4)
}

impl ProjectionConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            wal_mode: default_wal_mode(),
            synchronous: SynchronousMode::default(),
            schema_version: default_schema_version(),
            cache_size: default_cache_size(),
            read_pool: default_read_pool(),
        }
    }

    pub fn with_synchronous(mut self, synchronous: SynchronousMode) -> Self {
        self.synchronous = synchronous;
        self
    }

    pub fn with_wal_mode(mut self, wal_mode: bool) -> Self {
        self.wal_mode = wal_mode;
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
}
