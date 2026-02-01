//! Example: Circuit Breaker Pattern
//!
//! Demonstrates how to use circuit breakers to prevent cascading failures
//! during error conditions.

use azoth::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Simulated unreliable service
struct UnreliableService {
    call_count: Arc<AtomicU32>,
    failure_rate: f64,
}

impl UnreliableService {
    fn new(failure_rate: f64) -> Self {
        Self {
            call_count: Arc::new(AtomicU32::new(0)),
            failure_rate,
        }
    }

    fn call(&self) -> Result<String> {
        let count = self.call_count.fetch_add(1, Ordering::SeqCst);

        // Simulate failures based on failure rate
        let should_fail = (count as f64 * 0.37) % 1.0 < self.failure_rate;

        if should_fail {
            Err(AzothError::Projection(format!(
                "Service call {} failed",
                count
            )))
        } else {
            Ok(format!("Success {}", count))
        }
    }

    fn reset(&self) {
        self.call_count.store(0, Ordering::SeqCst);
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    tracing::info!("=== Circuit Breaker Pattern Demo ===\n");

    // Create an unreliable service (50% failure rate)
    let service = Arc::new(UnreliableService::new(0.5));

    // Configure circuit breaker
    let config = CircuitBreakerConfig {
        failure_threshold: 3,            // Open after 3 failures
        window: Duration::from_secs(60), // Count failures in 60s window
        timeout: Duration::from_secs(5), // Keep open for 5s
        half_open_requests: 2,           // Need 2 successes to close
    };

    let breaker = Arc::new(CircuitBreaker::new("example-service".to_string(), config));

    tracing::info!("Configuration:");
    tracing::info!("  Failure threshold: 3");
    tracing::info!("  Failure window: 60s");
    tracing::info!("  Circuit timeout: 5s");
    tracing::info!("  Half-open requests: 2\n");

    tracing::info!("=== Phase 1: Normal Operation ===\n");

    // Make some calls that will succeed and fail
    for i in 0..10 {
        let service_clone = service.clone();
        let result = breaker.call(move || service_clone.call()).await;

        match result {
            Ok(msg) => tracing::info!("Call {}: {}", i + 1, msg),
            Err(AzothError::CircuitBreakerOpen) => {
                tracing::warn!("Call {}: Circuit breaker is OPEN (rejected)", i + 1);
            }
            Err(e) => tracing::warn!("Call {}: Failed - {}", i + 1, e),
        }
    }

    let metrics = breaker.metrics().snapshot();
    let state = breaker.get_state().await;

    tracing::info!("\n--- State after Phase 1 ---");
    tracing::info!("Circuit state: {:?}", state);
    tracing::info!("Successes: {}", metrics.successes);
    tracing::info!("Failures: {}", metrics.failures);
    tracing::info!("Rejections: {}", metrics.rejections);
    tracing::info!("Success rate: {:.1}%", metrics.success_rate() * 100.0);

    if state == BreakerState::Open {
        tracing::info!("\n=== Phase 2: Circuit Open ===\n");
        tracing::info!("The circuit is now OPEN. Requests will be rejected immediately.");
        tracing::info!("This prevents cascading failures and gives the service time to recover.\n");

        // Try a few more calls - they should be rejected immediately
        for i in 0..3 {
            let service_clone = service.clone();
            let result = breaker.call(move || service_clone.call()).await;

            match result {
                Ok(_) => tracing::info!("Call {}: Success", i + 1),
                Err(AzothError::CircuitBreakerOpen) => {
                    tracing::warn!("Call {}: REJECTED (circuit open)", i + 1);
                }
                Err(e) => tracing::warn!("Call {}: Failed - {}", i + 1, e),
            }
        }

        tracing::info!("\n=== Phase 3: Waiting for Timeout ===\n");
        tracing::info!("Waiting 5 seconds for circuit to transition to half-open...\n");
        tokio::time::sleep(Duration::from_secs(5)).await;

        tracing::info!("=== Phase 4: Half-Open Recovery ===\n");

        // Improve service reliability for recovery
        service.reset();
        let better_service = Arc::new(UnreliableService::new(0.1)); // Only 10% failure rate

        tracing::info!("Service has recovered (10% failure rate).");
        tracing::info!("Circuit will test with a few requests...\n");

        // Try calls in half-open state
        for i in 0..5 {
            let service_clone = better_service.clone();
            let result = breaker.call(move || service_clone.call()).await;

            let state = breaker.get_state().await;

            match result {
                Ok(msg) => tracing::info!("Call {}: {} (state: {:?})", i + 1, msg, state),
                Err(AzothError::CircuitBreakerOpen) => {
                    tracing::warn!("Call {}: REJECTED (state: {:?})", i + 1, state);
                }
                Err(e) => tracing::warn!("Call {}: Failed - {} (state: {:?})", i + 1, e, state),
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    let final_metrics = breaker.metrics().snapshot();
    let final_state = breaker.get_state().await;

    tracing::info!("\n=== Final Statistics ===");
    tracing::info!("Circuit state: {:?}", final_state);
    tracing::info!("Total successes: {}", final_metrics.successes);
    tracing::info!("Total failures: {}", final_metrics.failures);
    tracing::info!("Total rejections: {}", final_metrics.rejections);
    tracing::info!("Times opened: {}", final_metrics.opens);
    tracing::info!("Times closed: {}", final_metrics.closes);
    tracing::info!("Success rate: {:.1}%", final_metrics.success_rate() * 100.0);
    tracing::info!(
        "Rejection rate: {:.1}%",
        final_metrics.rejection_rate() * 100.0
    );

    tracing::info!("\n=== Benefits of Circuit Breaker ===");
    tracing::info!("✓ Fast failure: Immediate rejection when circuit is open");
    tracing::info!("✓ Resource protection: Prevents overwhelming a failing service");
    tracing::info!("✓ Automatic recovery: Tests service health and closes when recovered");
    tracing::info!("✓ Cascading failure prevention: Stops errors from propagating");
    tracing::info!("✓ Observable: Clear metrics on failures, rejections, and recovery");

    Ok(())
}
