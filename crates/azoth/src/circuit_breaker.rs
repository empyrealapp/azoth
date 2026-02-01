//! Circuit Breaker Pattern
//!
//! Prevents cascading failures during errors by automatically stopping
//! request processing when failure rate exceeds thresholds.
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
//! use std::time::Duration;
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<()> {
//! let config = CircuitBreakerConfig {
//!     failure_threshold: 5,
//!     window: Duration::from_secs(60),
//!     timeout: Duration::from_secs(30),
//!     half_open_requests: 3,
//! };
//!
//! let breaker = Arc::new(CircuitBreaker::new("projection-processor".to_string(), config));
//!
//! // Use the circuit breaker to protect operations
//! match breaker.call(|| {
//!     // Your operation that might fail
//!     Ok(())
//! }).await {
//!     Ok(result) => println!("Success: {:?}", result),
//!     Err(e) => println!("Failed or circuit open: {}", e),
//! }
//! # Ok(())
//! # }
//! ```

use crate::{AzothError, Result};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Circuit breaker configuration
#[derive(Clone, Debug)]
pub struct CircuitBreakerConfig {
    /// Failure threshold before opening circuit
    pub failure_threshold: usize,

    /// Time window for counting failures
    pub window: Duration,

    /// How long to keep circuit open before trying half-open
    pub timeout: Duration,

    /// Number of test requests in half-open state before closing
    pub half_open_requests: usize,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            window: Duration::from_secs(60),
            timeout: Duration::from_secs(30),
            half_open_requests: 3,
        }
    }
}

/// Circuit breaker state
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BreakerState {
    /// Circuit is closed, requests are allowed
    Closed,
    /// Circuit is open, requests are blocked
    Open,
    /// Circuit is half-open, testing if system recovered
    HalfOpen,
}

/// Internal state for circuit breaker
struct BreakerInternalState {
    state: BreakerState,
    failures: VecDeque<Instant>,
    opened_at: Option<Instant>,
    half_open_successes: usize,
    half_open_failures: usize,
}

impl BreakerInternalState {
    fn new() -> Self {
        Self {
            state: BreakerState::Closed,
            failures: VecDeque::new(),
            opened_at: None,
            half_open_successes: 0,
            half_open_failures: 0,
        }
    }

    fn clean_old_failures(&mut self, window: Duration) {
        let now = Instant::now();
        while let Some(&failure_time) = self.failures.front() {
            if now.duration_since(failure_time) > window {
                self.failures.pop_front();
            } else {
                break;
            }
        }
    }
}

/// Circuit breaker metrics
#[derive(Debug, Default)]
pub struct BreakerMetrics {
    /// Total successful calls
    pub successes: AtomicU64,

    /// Total failed calls
    pub failures: AtomicU64,

    /// Total rejected calls (circuit open)
    pub rejections: AtomicU64,

    /// Number of times circuit opened
    pub opens: AtomicU64,

    /// Number of times circuit closed
    pub closes: AtomicU64,
}

impl BreakerMetrics {
    fn record_success(&self) {
        self.successes.fetch_add(1, Ordering::Relaxed);
    }

    fn record_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    fn record_rejection(&self) {
        self.rejections.fetch_add(1, Ordering::Relaxed);
    }

    fn record_open(&self) {
        self.opens.fetch_add(1, Ordering::Relaxed);
    }

    fn record_close(&self) {
        self.closes.fetch_add(1, Ordering::Relaxed);
    }

    /// Get a snapshot of metrics
    pub fn snapshot(&self) -> BreakerMetricsSnapshot {
        BreakerMetricsSnapshot {
            successes: self.successes.load(Ordering::Relaxed),
            failures: self.failures.load(Ordering::Relaxed),
            rejections: self.rejections.load(Ordering::Relaxed),
            opens: self.opens.load(Ordering::Relaxed),
            closes: self.closes.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of circuit breaker metrics
#[derive(Debug, Clone)]
pub struct BreakerMetricsSnapshot {
    pub successes: u64,
    pub failures: u64,
    pub rejections: u64,
    pub opens: u64,
    pub closes: u64,
}

impl BreakerMetricsSnapshot {
    /// Get success rate (0.0 - 1.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.successes + self.failures;
        if total == 0 {
            1.0
        } else {
            self.successes as f64 / total as f64
        }
    }

    /// Get failure rate (0.0 - 1.0)
    pub fn failure_rate(&self) -> f64 {
        1.0 - self.success_rate()
    }

    /// Get rejection rate (relative to total attempts)
    pub fn rejection_rate(&self) -> f64 {
        let total = self.successes + self.failures + self.rejections;
        if total == 0 {
            0.0
        } else {
            self.rejections as f64 / total as f64
        }
    }
}

/// Circuit breaker for protecting operations from cascading failures
pub struct CircuitBreaker {
    name: String,
    config: CircuitBreakerConfig,
    state: Arc<Mutex<BreakerInternalState>>,
    metrics: Arc<BreakerMetrics>,
}

impl CircuitBreaker {
    /// Create a new circuit breaker
    pub fn new(name: String, config: CircuitBreakerConfig) -> Self {
        Self {
            name,
            config,
            state: Arc::new(Mutex::new(BreakerInternalState::new())),
            metrics: Arc::new(BreakerMetrics::default()),
        }
    }

    /// Get the breaker name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the current state
    pub async fn get_state(&self) -> BreakerState {
        let state = self.state.lock().await;
        state.state.clone()
    }

    /// Get metrics
    pub fn metrics(&self) -> &Arc<BreakerMetrics> {
        &self.metrics
    }

    /// Execute a function through the circuit breaker
    pub async fn call<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce() -> Result<T>,
    {
        // Check current state and decide whether to allow the call
        let mut state = self.state.lock().await;

        match state.state {
            BreakerState::Closed => {
                // Clean old failures
                state.clean_old_failures(self.config.window);

                // Circuit is closed, allow the call
                drop(state); // Release lock before calling f

                match f() {
                    Ok(result) => {
                        self.on_success().await;
                        Ok(result)
                    }
                    Err(e) => {
                        self.on_failure().await;
                        Err(e)
                    }
                }
            }

            BreakerState::Open => {
                // Check if timeout elapsed
                if let Some(opened_at) = state.opened_at {
                    if opened_at.elapsed() >= self.config.timeout {
                        // Transition to half-open
                        tracing::info!(
                            "Circuit breaker '{}': transitioning to half-open",
                            self.name
                        );
                        state.state = BreakerState::HalfOpen;
                        state.half_open_successes = 0;
                        state.half_open_failures = 0;
                        drop(state);

                        // Try the call in half-open state (inline instead of recursive)
                        match f() {
                            Ok(result) => {
                                self.on_half_open_success().await;
                                Ok(result)
                            }
                            Err(e) => {
                                self.on_half_open_failure().await;
                                Err(e)
                            }
                        }
                    } else {
                        // Circuit still open, reject the call
                        self.metrics.record_rejection();
                        Err(AzothError::CircuitBreakerOpen)
                    }
                } else {
                    // No opened_at timestamp, reject
                    self.metrics.record_rejection();
                    Err(AzothError::CircuitBreakerOpen)
                }
            }

            BreakerState::HalfOpen => {
                // Circuit is half-open, testing recovery
                drop(state);

                match f() {
                    Ok(result) => {
                        self.on_half_open_success().await;
                        Ok(result)
                    }
                    Err(e) => {
                        self.on_half_open_failure().await;
                        Err(e)
                    }
                }
            }
        }
    }

    async fn on_success(&self) {
        self.metrics.record_success();
    }

    async fn on_failure(&self) {
        self.metrics.record_failure();

        let mut state = self.state.lock().await;

        // Record failure
        state.failures.push_back(Instant::now());
        state.clean_old_failures(self.config.window);

        // Check if we should open the circuit
        if state.failures.len() >= self.config.failure_threshold {
            tracing::warn!(
                "Circuit breaker '{}': opening circuit ({} failures in {:?})",
                self.name,
                state.failures.len(),
                self.config.window
            );

            state.state = BreakerState::Open;
            state.opened_at = Some(Instant::now());
            self.metrics.record_open();
        }
    }

    async fn on_half_open_success(&self) {
        self.metrics.record_success();

        let mut state = self.state.lock().await;
        state.half_open_successes += 1;

        // Check if we have enough successes to close the circuit
        if state.half_open_successes >= self.config.half_open_requests {
            tracing::info!(
                "Circuit breaker '{}': closing circuit after {} successful requests",
                self.name,
                state.half_open_successes
            );

            state.state = BreakerState::Closed;
            state.failures.clear();
            state.opened_at = None;
            self.metrics.record_close();
        }
    }

    async fn on_half_open_failure(&self) {
        self.metrics.record_failure();

        let mut state = self.state.lock().await;
        state.half_open_failures += 1;

        // Any failure in half-open state reopens the circuit
        tracing::warn!(
            "Circuit breaker '{}': reopening circuit after failure in half-open state",
            self.name
        );

        state.state = BreakerState::Open;
        state.opened_at = Some(Instant::now());
        state.half_open_successes = 0;
        state.half_open_failures = 0;
        self.metrics.record_open();
    }

    /// Manually reset the circuit breaker to closed state
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        tracing::info!(
            "Circuit breaker '{}': manually reset to closed state",
            self.name
        );
        state.state = BreakerState::Closed;
        state.failures.clear();
        state.opened_at = None;
        state.half_open_successes = 0;
        state.half_open_failures = 0;
    }

    /// Manually trip (open) the circuit breaker
    pub async fn trip(&self) {
        let mut state = self.state.lock().await;
        tracing::warn!(
            "Circuit breaker '{}': manually tripped to open state",
            self.name
        );
        state.state = BreakerState::Open;
        state.opened_at = Some(Instant::now());
        self.metrics.record_open();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_circuit_breaker_closed() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            window: Duration::from_secs(60),
            timeout: Duration::from_secs(5),
            half_open_requests: 2,
        };

        let breaker = CircuitBreaker::new("test".to_string(), config);

        // Should start closed
        assert_eq!(breaker.get_state().await, BreakerState::Closed);

        // Successful calls should work
        let result = breaker.call(|| Ok::<_, AzothError>(42)).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn test_circuit_breaker_opens_on_failures() {
        let config = CircuitBreakerConfig {
            failure_threshold: 3,
            window: Duration::from_secs(60),
            timeout: Duration::from_secs(5),
            half_open_requests: 2,
        };

        let breaker = CircuitBreaker::new("test".to_string(), config);

        // Fail 3 times to open the circuit
        for _ in 0..3 {
            let _ = breaker
                .call(|| Err::<(), _>(AzothError::Projection("test error".to_string())))
                .await;
        }

        // Circuit should now be open
        assert_eq!(breaker.get_state().await, BreakerState::Open);

        // Next call should be rejected
        let result = breaker.call(|| Ok::<_, AzothError>(42)).await;
        assert!(matches!(result, Err(AzothError::CircuitBreakerOpen)));
    }

    #[tokio::test]
    async fn test_circuit_breaker_half_open_recovery() {
        let config = CircuitBreakerConfig {
            failure_threshold: 2,
            window: Duration::from_secs(60),
            timeout: Duration::from_millis(100), // Short timeout for testing
            half_open_requests: 2,
        };

        let breaker = CircuitBreaker::new("test".to_string(), config);

        // Fail to open circuit
        for _ in 0..2 {
            let _ = breaker
                .call(|| Err::<(), _>(AzothError::Projection("test error".to_string())))
                .await;
        }

        assert_eq!(breaker.get_state().await, BreakerState::Open);

        // Wait for timeout
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Next call should transition to half-open
        let _ = breaker.call(|| Ok::<_, AzothError>(1)).await;
        let state = breaker.get_state().await;
        assert!(state == BreakerState::HalfOpen || state == BreakerState::Closed);
    }

    #[tokio::test]
    async fn test_manual_reset() {
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new("test".to_string(), config);

        // Trip the breaker
        breaker.trip().await;
        assert_eq!(breaker.get_state().await, BreakerState::Open);

        // Reset it
        breaker.reset().await;
        assert_eq!(breaker.get_state().await, BreakerState::Closed);
    }

    #[tokio::test]
    async fn test_metrics() {
        let config = CircuitBreakerConfig::default();
        let breaker = CircuitBreaker::new("test".to_string(), config);

        // Record some successes
        for _ in 0..5 {
            let _ = breaker.call(|| Ok::<_, AzothError>(())).await;
        }

        // Record some failures
        for _ in 0..2 {
            let _ = breaker
                .call(|| Err::<(), _>(AzothError::Projection("error".to_string())))
                .await;
        }

        let metrics = breaker.metrics().snapshot();
        assert_eq!(metrics.successes, 5);
        assert_eq!(metrics.failures, 2);
        assert!(metrics.success_rate() > 0.7);
    }
}
