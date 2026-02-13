//! Automatic Dead Letter Queue Replay
//!
//! Provides automatic retry of failed events with configurable backoff strategies
//! and replay priorities.
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::dlq_replayer::{DlqReplayer, DlqReplayConfig, BackoffStrategy, ReplayPriority};
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<()> {
//! let db = Arc::new(AzothDb::open("./data")?);
//! let conn = Arc::new(
//!     rusqlite::Connection::open("./data/projection.db")
//!         .map_err(|e| AzothError::Projection(e.to_string()))?
//! );
//! let dlq = Arc::new(DeadLetterQueue::new(conn.clone())?);
//! let registry = Arc::new(EventHandlerRegistry::new());
//!
//! let config = DlqReplayConfig {
//!     enabled: true,
//!     check_interval: Duration::from_secs(60),
//!     max_retries: 5,
//!     backoff: BackoffStrategy::Exponential {
//!         initial: Duration::from_secs(10),
//!         max: Duration::from_secs(3600),
//!     },
//!     min_age: Duration::from_secs(5),
//!     batch_size: 100,
//!     priority: ReplayPriority::ByRetryCount,
//!     stop_on_consecutive_failures: Some(10),
//! };
//!
//! let replayer = Arc::new(DlqReplayer::new(dlq, registry, config));
//!
//! // Run replayer (in a real application, run this in a dedicated thread)
//! // Note: replayer.run() must be called from a thread that owns the Connection
//! // since rusqlite::Connection is not Send
//! # Ok(())
//! # }
//! ```

use crate::{AzothError, DeadLetterQueue, EventHandlerRegistry, FailedEvent, Result};
use rusqlite::params;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// Automatic DLQ replay configuration
#[derive(Clone, Debug)]
pub struct DlqReplayConfig {
    /// Enable automatic replay
    pub enabled: bool,

    /// Check interval - how often to poll for failed events
    pub check_interval: Duration,

    /// Max retry attempts before permanent failure
    pub max_retries: usize,

    /// Backoff strategy
    pub backoff: BackoffStrategy,

    /// Age threshold - only retry events older than this
    /// This prevents immediate retry of events that just failed
    pub min_age: Duration,

    /// Max events to replay per batch
    pub batch_size: usize,

    /// Replay priority (FIFO, LIFO, by error type)
    pub priority: ReplayPriority,

    /// Stop conditions - stop after N consecutive failures
    pub stop_on_consecutive_failures: Option<usize>,
}

impl Default for DlqReplayConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval: Duration::from_secs(60),
            max_retries: 5,
            backoff: BackoffStrategy::Exponential {
                initial: Duration::from_secs(10),
                max: Duration::from_secs(3600),
            },
            min_age: Duration::from_secs(5),
            batch_size: 100,
            priority: ReplayPriority::ByRetryCount,
            stop_on_consecutive_failures: Some(10),
        }
    }
}

/// Backoff strategy for retry delays
#[derive(Clone, Debug)]
pub enum BackoffStrategy {
    /// Fixed delay between retries
    Fixed(Duration),

    /// Exponential: delay * 2^attempt (capped at max)
    Exponential { initial: Duration, max: Duration },

    /// Fibonacci: delay follows fibonacci sequence (capped at max)
    Fibonacci { initial: Duration, max: Duration },
}

impl BackoffStrategy {
    /// Calculate delay for a given retry attempt
    pub fn calculate(&self, attempt: usize) -> Duration {
        match self {
            BackoffStrategy::Fixed(delay) => *delay,
            BackoffStrategy::Exponential { initial, max } => {
                let multiplier = 2u64.saturating_pow(attempt as u32);
                let delay = initial.saturating_mul(multiplier as u32);
                delay.min(*max)
            }
            BackoffStrategy::Fibonacci { initial, max } => {
                let fib = Self::fibonacci(attempt);
                let delay = initial.saturating_mul(fib as u32);
                delay.min(*max)
            }
        }
    }

    fn fibonacci(n: usize) -> u64 {
        match n {
            0 => 1,
            1 => 1,
            _ => {
                let mut a = 1u64;
                let mut b = 1u64;
                for _ in 2..=n {
                    let c = a.saturating_add(b);
                    a = b;
                    b = c;
                }
                b
            }
        }
    }
}

/// Replay priority determines order of event replay
#[derive(Clone, Debug)]
pub enum ReplayPriority {
    /// First In First Out (oldest first)
    FIFO,

    /// Last In First Out (newest first)
    LIFO,

    /// By retry count (fewer retries first - give newer failures priority)
    ByRetryCount,

    /// By error type (specific errors first)
    ByErrorType(Vec<String>),
}

impl ReplayPriority {
    /// Generate the ORDER BY clause for this priority.
    ///
    /// For `ByErrorType`, error type strings are validated against a strict
    /// allowlist (`[a-zA-Z0-9_ -.]`) to prevent SQL injection via CASE/LIKE expressions.
    fn order_by_clause(&self) -> Result<String> {
        match self {
            ReplayPriority::FIFO => Ok("failed_at ASC".to_string()),
            ReplayPriority::LIFO => Ok("failed_at DESC".to_string()),
            ReplayPriority::ByRetryCount => Ok("retry_count ASC, failed_at ASC".to_string()),
            ReplayPriority::ByErrorType(types) => {
                // Validate each error type to prevent SQL injection.
                // Only alphanumeric, underscore, dash, dot, and space are allowed.
                for t in types {
                    if t.is_empty() || t.len() > 128 {
                        return Err(AzothError::Config(format!(
                            "ByErrorType string must be 1-128 characters, got length {}",
                            t.len()
                        )));
                    }
                    if !t.chars().all(|c| {
                        c.is_alphanumeric() || c == '_' || c == '-' || c == '.' || c == ' '
                    }) {
                        return Err(AzothError::Config(format!(
                            "ByErrorType string '{}' contains disallowed characters. \
                             Only alphanumeric, underscore, dash, dot, and space are permitted.",
                            t
                        )));
                    }
                }

                // Safe to interpolate after validation
                let cases = types
                    .iter()
                    .enumerate()
                    .map(|(i, t)| format!("WHEN error_message LIKE '%{}%' THEN {}", t, i))
                    .collect::<Vec<_>>()
                    .join(" ");
                Ok(format!("CASE {} ELSE 999 END, failed_at ASC", cases))
            }
        }
    }
}

/// DLQ replay metrics
#[derive(Default)]
pub struct DlqMetrics {
    /// Total successful replays
    pub successes: AtomicU64,

    /// Total failed replays
    pub failures: AtomicU64,

    /// Events moved to permanent failure
    pub permanent_failures: AtomicU64,

    /// Last check timestamp
    pub last_check: AtomicU64,
}

impl DlqMetrics {
    fn record_success(&self, _retry_count: i32) {
        self.successes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self, _retry_count: i32) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_permanent_failure(&self) {
        self.permanent_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn update_last_check(&self) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_check.store(now, Ordering::Relaxed);
    }

    /// Get metrics snapshot
    pub fn snapshot(&self) -> DlqMetricsSnapshot {
        DlqMetricsSnapshot {
            successes: self.successes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            permanent_failures: self.permanent_failures.load(Ordering::Relaxed),
            last_check: self.last_check.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of DLQ metrics
#[derive(Debug, Clone)]
pub struct DlqMetricsSnapshot {
    pub successes: u64,
    pub failures: u64,
    pub permanent_failures: u64,
    pub last_check: u64,
}

/// Automatic DLQ replayer
pub struct DlqReplayer {
    dlq: Arc<DeadLetterQueue>,
    registry: Arc<EventHandlerRegistry>,
    config: DlqReplayConfig,
    shutdown: Arc<AtomicBool>,
    metrics: Arc<DlqMetrics>,
}

impl DlqReplayer {
    /// Create a new DLQ replayer
    pub fn new(
        dlq: Arc<DeadLetterQueue>,
        registry: Arc<EventHandlerRegistry>,
        config: DlqReplayConfig,
    ) -> Self {
        Self {
            dlq,
            registry,
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
            metrics: Arc::new(DlqMetrics::default()),
        }
    }

    /// Get metrics
    pub fn metrics(&self) -> &Arc<DlqMetrics> {
        &self.metrics
    }

    /// Signal shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Check if replayer is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Start automatic replay loop
    pub async fn run(self: Arc<Self>) -> Result<()> {
        if !self.config.enabled {
            tracing::info!("DLQ replayer is disabled");
            return Ok(());
        }

        tracing::info!(
            "DLQ replayer started (check interval: {:?})",
            self.config.check_interval
        );

        while !self.shutdown.load(Ordering::Relaxed) {
            match self.run_replay_cycle().await {
                Ok(_) => {}
                Err(e) => {
                    tracing::error!("DLQ replay cycle error: {}", e);
                }
            }

            self.metrics.update_last_check();
            tokio::time::sleep(self.config.check_interval).await;
        }

        tracing::info!("DLQ replayer shutdown");
        Ok(())
    }

    async fn run_replay_cycle(&self) -> Result<()> {
        // Get eligible events (respecting min_age, max_retries)
        let failed_events = self.get_eligible_events()?;

        if failed_events.is_empty() {
            return Ok(());
        }

        tracing::debug!("Found {} eligible events for replay", failed_events.len());

        let mut consecutive_failures = 0;

        for event in failed_events {
            // Calculate backoff delay
            let delay = self.config.backoff.calculate(event.retry_count as usize);

            // Check if enough time has passed since last retry
            if !self.should_retry_now(&event, delay)? {
                continue;
            }

            // Attempt replay
            tracing::debug!(
                "Replaying event {} (attempt {}/{})",
                event.event_id,
                event.retry_count + 1,
                self.config.max_retries
            );

            match self.replay_event(&event).await {
                Ok(_) => {
                    // Success! Remove from DLQ
                    self.dlq.remove(event.id)?;
                    self.metrics.record_success(event.retry_count);
                    consecutive_failures = 0;

                    tracing::info!(
                        "Successfully replayed event {} after {} retries",
                        event.event_id,
                        event.retry_count
                    );
                }
                Err(e) => {
                    // Failure - update retry count
                    self.dlq.mark_retry(event.id)?;
                    self.metrics.record_failure(event.retry_count);
                    consecutive_failures += 1;

                    tracing::warn!(
                        "Failed to replay event {}: {} (retry {}/{})",
                        event.event_id,
                        e,
                        event.retry_count + 1,
                        self.config.max_retries
                    );

                    // Check if we should stop
                    if let Some(max) = self.config.stop_on_consecutive_failures {
                        if consecutive_failures >= max {
                            tracing::warn!(
                                "Stopping DLQ replay after {} consecutive failures",
                                consecutive_failures
                            );
                            return Ok(());
                        }
                    }

                    // Check if event exceeded max retries
                    if event.retry_count + 1 >= self.config.max_retries as i32 {
                        tracing::error!(
                            "Event {} exceeded max retries ({}), marking as permanently failed",
                            event.event_id,
                            self.config.max_retries
                        );
                        self.move_to_permanent_failure(&event)?;
                    }
                }
            }
        }

        Ok(())
    }

    fn get_eligible_events(&self) -> Result<Vec<FailedEvent>> {
        let order_by = self.config.priority.order_by_clause()?;
        let min_age_secs = self.config.min_age.as_secs();

        let query = format!(
            "SELECT id, event_id, event_bytes, error_message, failed_at, retry_count
             FROM dead_letter_queue
             WHERE retry_count < ?
             AND datetime(COALESCE(last_retry_at, failed_at)) <= datetime('now', '-{} seconds')
             ORDER BY {}
             LIMIT ?",
            min_age_secs, order_by
        );

        // Get connection from DLQ
        let conn = self.dlq.connection();
        let mut stmt = conn
            .prepare(&query)
            .map_err(|e: rusqlite::Error| AzothError::Projection(e.to_string()))?;

        let events = stmt
            .query_map(
                params![self.config.max_retries as i32, self.config.batch_size],
                |row: &rusqlite::Row| {
                    Ok(FailedEvent {
                        id: row.get(0)?,
                        event_id: row.get(1)?,
                        event_bytes: row.get(2)?,
                        error_message: row.get(3)?,
                        failed_at: row.get(4)?,
                        retry_count: row.get(5)?,
                    })
                },
            )
            .map_err(|e: rusqlite::Error| AzothError::Projection(e.to_string()))?;

        events
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e: rusqlite::Error| AzothError::Projection(e.to_string()))
    }

    fn should_retry_now(&self, event: &FailedEvent, delay: Duration) -> Result<bool> {
        // If this is the first retry, check failed_at
        // Otherwise, check last_retry_at
        let conn = self.dlq.connection();

        let last_attempt: String = conn
            .query_row(
                "SELECT COALESCE(last_retry_at, failed_at) FROM dead_letter_queue WHERE id = ?",
                [event.id],
                |row| row.get(0),
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Parse timestamp
        use chrono::{DateTime, Utc};
        let last_time = DateTime::parse_from_rfc3339(&last_attempt)
            .or_else(|_| {
                // Try SQLite datetime format
                chrono::NaiveDateTime::parse_from_str(&last_attempt, "%Y-%m-%d %H:%M:%S")
                    .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc).into())
            })
            .map_err(|e| {
                AzothError::Projection(format!("Failed to parse timestamp {}: {}", last_attempt, e))
            })?;

        let elapsed = Utc::now().signed_duration_since(last_time);
        let elapsed_duration = Duration::from_secs(elapsed.num_seconds().max(0) as u64);

        Ok(elapsed_duration >= delay)
    }

    async fn replay_event(&self, event: &FailedEvent) -> Result<()> {
        // Get connection from DLQ (it uses the projection connection)
        let conn = self.dlq.connection();

        // Process event through registry
        self.registry
            .process(conn.as_ref(), event.event_id, &event.event_bytes)
    }

    fn move_to_permanent_failure(&self, event: &FailedEvent) -> Result<()> {
        let conn = self.dlq.connection();

        // Create permanent failures table if it doesn't exist
        conn.execute(
            "CREATE TABLE IF NOT EXISTS permanent_failures (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id INTEGER NOT NULL,
                event_bytes BLOB NOT NULL,
                error_message TEXT NOT NULL,
                failed_at TEXT NOT NULL,
                retry_count INTEGER NOT NULL,
                marked_permanent_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Move to permanent failures
        conn.execute(
            "INSERT INTO permanent_failures (event_id, event_bytes, error_message, failed_at, retry_count)
             SELECT event_id, event_bytes, error_message, failed_at, retry_count
             FROM dead_letter_queue
             WHERE id = ?",
            [event.id],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Remove from DLQ
        self.dlq.remove(event.id)?;

        self.metrics.record_permanent_failure();

        Ok(())
    }

    /// Get count of permanent failures
    pub fn permanent_failure_count(&self) -> Result<usize> {
        let conn = self.dlq.connection();

        // Check if table exists
        let exists: bool = conn
            .query_row(
                "SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='permanent_failures'",
                [],
                |row| row.get(0),
            )
            .unwrap_or(false);

        if !exists {
            return Ok(0);
        }

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM permanent_failures", [], |row| {
                row.get(0)
            })
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(count as usize)
    }

    /// Clear permanent failures
    pub fn clear_permanent_failures(&self) -> Result<()> {
        let conn = self.dlq.connection();
        conn.execute("DELETE FROM permanent_failures", [])
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backoff_fixed() {
        let backoff = BackoffStrategy::Fixed(Duration::from_secs(10));
        assert_eq!(backoff.calculate(0), Duration::from_secs(10));
        assert_eq!(backoff.calculate(5), Duration::from_secs(10));
        assert_eq!(backoff.calculate(100), Duration::from_secs(10));
    }

    #[test]
    fn test_backoff_exponential() {
        let backoff = BackoffStrategy::Exponential {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(60),
        };

        assert_eq!(backoff.calculate(0), Duration::from_secs(1)); // 1 * 2^0 = 1
        assert_eq!(backoff.calculate(1), Duration::from_secs(2)); // 1 * 2^1 = 2
        assert_eq!(backoff.calculate(2), Duration::from_secs(4)); // 1 * 2^2 = 4
        assert_eq!(backoff.calculate(3), Duration::from_secs(8)); // 1 * 2^3 = 8
        assert_eq!(backoff.calculate(10), Duration::from_secs(60)); // capped at max
    }

    #[test]
    fn test_backoff_fibonacci() {
        let backoff = BackoffStrategy::Fibonacci {
            initial: Duration::from_secs(1),
            max: Duration::from_secs(100),
        };

        assert_eq!(backoff.calculate(0), Duration::from_secs(1)); // fib(0) = 1
        assert_eq!(backoff.calculate(1), Duration::from_secs(1)); // fib(1) = 1
        assert_eq!(backoff.calculate(2), Duration::from_secs(2)); // fib(2) = 2
        assert_eq!(backoff.calculate(3), Duration::from_secs(3)); // fib(3) = 3
        assert_eq!(backoff.calculate(4), Duration::from_secs(5)); // fib(4) = 5
        assert_eq!(backoff.calculate(5), Duration::from_secs(8)); // fib(5) = 8
    }

    #[test]
    fn test_replay_priority_order_by() {
        let priority = ReplayPriority::FIFO;
        assert_eq!(priority.order_by_clause().unwrap(), "failed_at ASC");

        let priority = ReplayPriority::LIFO;
        assert_eq!(priority.order_by_clause().unwrap(), "failed_at DESC");

        let priority = ReplayPriority::ByRetryCount;
        assert_eq!(
            priority.order_by_clause().unwrap(),
            "retry_count ASC, failed_at ASC"
        );
    }

    #[test]
    fn test_replay_priority_by_error_type_validation() {
        // Valid error types should work
        let priority = ReplayPriority::ByErrorType(vec![
            "timeout".to_string(),
            "connection_error".to_string(),
        ]);
        assert!(priority.order_by_clause().is_ok());

        // SQL injection attempt should be rejected
        let priority =
            ReplayPriority::ByErrorType(vec!["'; DROP TABLE dead_letter_queue; --".to_string()]);
        assert!(priority.order_by_clause().is_err());

        // Empty string should be rejected
        let priority = ReplayPriority::ByErrorType(vec!["".to_string()]);
        assert!(priority.order_by_clause().is_err());
    }

    #[test]
    fn test_default_config() {
        let config = DlqReplayConfig::default();
        assert!(config.enabled);
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.batch_size, 100);
    }
}
