use serde::{Deserialize, Serialize};
use std::path::PathBuf;

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
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// Full durability (fsync on every commit)
    Full,
    /// No metadata sync (faster, usually safe)
    #[default]
    NoMetaSync,
    /// No sync at all (fastest, least safe)
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

impl CanonicalConfig {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            map_size: default_map_size(),
            sync_mode: SyncMode::default(),
            stripe_count: default_stripe_count(),
            max_readers: default_max_readers(),
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
}
