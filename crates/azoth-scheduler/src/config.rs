//! Scheduler configuration.

use std::time::Duration;

/// Configuration for the scheduler.
#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// How often to poll for due tasks.
    pub poll_interval: Duration,
    /// Maximum number of tasks to execute concurrently.
    pub max_concurrent_tasks: usize,
    /// Default maximum number of retries for failed tasks.
    pub default_max_retries: u32,
    /// Default timeout in seconds for task execution.
    pub default_timeout_secs: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            poll_interval: Duration::from_secs(1),
            max_concurrent_tasks: 10,
            default_max_retries: 3,
            default_timeout_secs: 300,
        }
    }
}

impl SchedulerConfig {
    /// Create a new configuration with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the poll interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set the maximum concurrent tasks.
    pub fn with_max_concurrent_tasks(mut self, max: usize) -> Self {
        self.max_concurrent_tasks = max;
        self
    }

    /// Set the default maximum retries.
    pub fn with_default_max_retries(mut self, retries: u32) -> Self {
        self.default_max_retries = retries;
        self
    }

    /// Set the default timeout in seconds.
    pub fn with_default_timeout_secs(mut self, timeout: u64) -> Self {
        self.default_timeout_secs = timeout;
        self
    }
}
