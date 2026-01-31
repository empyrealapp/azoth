use serde::{Deserialize, Serialize};

/// Configuration for the projector
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectorConfig {
    /// Maximum number of events per batch
    /// Default: 1000
    #[serde(default = "default_batch_events_max")]
    pub batch_events_max: usize,

    /// Maximum bytes per batch
    /// Default: 4MB
    #[serde(default = "default_batch_bytes_max")]
    pub batch_bytes_max: usize,

    /// Maximum apply latency in milliseconds
    /// Projector tries to commit at least every N ms
    /// Default: 100ms
    #[serde(default = "default_max_apply_latency_ms")]
    pub max_apply_latency_ms: u64,

    /// Poll interval when caught up (milliseconds)
    /// Default: 10ms
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Maximum lag before throttling (number of events)
    /// Default: 100000
    #[serde(default = "default_max_lag_before_throttle")]
    pub max_lag_before_throttle: u64,
}

fn default_batch_events_max() -> usize {
    1000
}

fn default_batch_bytes_max() -> usize {
    4 * 1024 * 1024 // 4MB
}

fn default_max_apply_latency_ms() -> u64 {
    100
}

fn default_poll_interval_ms() -> u64 {
    10
}

fn default_max_lag_before_throttle() -> u64 {
    100_000
}

impl Default for ProjectorConfig {
    fn default() -> Self {
        Self {
            batch_events_max: default_batch_events_max(),
            batch_bytes_max: default_batch_bytes_max(),
            max_apply_latency_ms: default_max_apply_latency_ms(),
            poll_interval_ms: default_poll_interval_ms(),
            max_lag_before_throttle: default_max_lag_before_throttle(),
        }
    }
}

impl ProjectorConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_batch_events_max(mut self, max: usize) -> Self {
        self.batch_events_max = max;
        self
    }

    pub fn with_batch_bytes_max(mut self, max: usize) -> Self {
        self.batch_bytes_max = max;
        self
    }

    pub fn with_max_apply_latency_ms(mut self, ms: u64) -> Self {
        self.max_apply_latency_ms = ms;
        self
    }
}
