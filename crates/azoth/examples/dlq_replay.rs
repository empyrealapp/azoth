//! Example: Automatic Dead Letter Queue Replay
//!
//! Demonstrates how to set up automatic retry of failed events with
//! configurable backoff strategies and replay priorities.

use azoth::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use std::time::Duration;

// Example event handler that fails sometimes
struct UnreliableHandler {
    fail_count: Arc<AtomicU32>,
    fail_threshold: u32,
}

impl UnreliableHandler {
    fn new(fail_threshold: u32) -> Self {
        Self {
            fail_count: Arc::new(AtomicU32::new(0)),
            fail_threshold,
        }
    }
}

impl EventHandler for UnreliableHandler {
    fn handle(&self, _conn: &rusqlite::Connection, event_id: EventId, data: &[u8]) -> Result<()> {
        let count = self.fail_count.fetch_add(1, Ordering::SeqCst);

        // Simulate failures for the first N attempts
        if count < self.fail_threshold {
            tracing::warn!(
                "Handler failing on purpose (attempt {}/{})",
                count + 1,
                self.fail_threshold
            );
            return Err(AzothError::Projection(format!(
                "Simulated failure for event {} (attempt {})",
                event_id,
                count + 1
            )));
        }

        // After threshold, succeed
        tracing::info!(
            "Successfully processed event {} on attempt {}",
            event_id,
            count + 1
        );
        let msg = String::from_utf8_lossy(data);
        tracing::info!("Event data: {}", msg);
        Ok(())
    }

    fn event_type(&self) -> &str {
        "test_event"
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Clean up any existing data
    let _ = std::fs::remove_dir_all("./tmp/dlq_example");

    // Open database
    let db = Arc::new(AzothDb::open("./tmp/dlq_example")?);
    let conn = Arc::new(rusqlite::Connection::open(
        "./tmp/dlq_example/projection.db",
    )?);

    // Create DLQ
    let dlq = Arc::new(DeadLetterQueue::new(conn.clone())?);

    // Register handler that will fail 2 times before succeeding
    let mut registry = EventHandlerRegistry::new();
    registry.register(Box::new(UnreliableHandler::new(2)));
    let registry = Arc::new(registry);

    tracing::info!("=== Step 1: Write some events ===");

    // Write some test events
    for i in 0..5 {
        let mut txn = db.canonical().write_txn()?;
        let event_data = format!("Test event {}", i);
        txn.append_event(event_data.as_bytes())?;
        txn.commit()?;
    }

    tracing::info!("Wrote 5 events");

    tracing::info!("\n=== Step 2: Process events (will fail and go to DLQ) ===");

    // Process events - they will fail and go to DLQ
    let processor = crate::EventProcessor::builder(db.clone())
        .with_handler(Box::new(UnreliableHandler::new(2)))
        .with_error_strategy(ErrorStrategy::DeadLetterQueue)
        .with_dead_letter_queue(dlq.clone())
        .with_batch_size(10)
        .build(conn.clone());

    let mut processor = processor;
    processor.process_batch()?;

    // Check DLQ
    let dlq_count = dlq.count()?;
    tracing::info!("Events in DLQ: {}", dlq_count);

    let failed_events = dlq.list(10)?;
    for event in &failed_events {
        tracing::info!(
            "  Event {}: {} (retries: {})",
            event.event_id,
            event.error_message,
            event.retry_count
        );
    }

    tracing::info!("\n=== Step 3: Set up automatic DLQ replay ===");

    // Configure DLQ replayer with exponential backoff
    let config = DlqReplayConfig {
        enabled: true,
        check_interval: Duration::from_secs(2), // Check every 2 seconds
        max_retries: 5,
        backoff: BackoffStrategy::Exponential {
            initial: Duration::from_millis(100), // Start with 100ms
            max: Duration::from_secs(10),        // Cap at 10s
        },
        min_age: Duration::from_millis(50), // Only retry events older than 50ms
        batch_size: 10,
        priority: ReplayPriority::ByRetryCount, // Process events with fewer retries first
        stop_on_consecutive_failures: Some(5),
    };

    let replayer = Arc::new(DlqReplayer::new(dlq.clone(), registry.clone(), config));

    tracing::info!("Starting DLQ replayer...");
    tracing::info!("Backoff: exponential (100ms - 10s)");
    tracing::info!("Priority: by retry count (fewer retries first)");
    tracing::info!("Max retries: 5");

    // Start replayer in background
    let replayer_handle = {
        let replayer = replayer.clone();
        tokio::spawn(async move { replayer.run().await })
    };

    // Wait for replay to complete
    tracing::info!("\n=== Step 4: Wait for automatic replay ===");
    tokio::time::sleep(Duration::from_secs(10)).await;

    // Check final state
    let final_dlq_count = dlq.count()?;
    let permanent_failures = replayer.permanent_failure_count()?;
    let metrics = replayer.metrics().snapshot();

    tracing::info!("\n=== Final Results ===");
    tracing::info!("Events remaining in DLQ: {}", final_dlq_count);
    tracing::info!("Permanent failures: {}", permanent_failures);
    tracing::info!("Successful replays: {}", metrics.successes);
    tracing::info!("Failed replays: {}", metrics.failures);
    tracing::info!("Total permanent failures: {}", metrics.permanent_failures);

    // Shutdown replayer
    replayer.shutdown();
    let _ = replayer_handle.await;

    tracing::info!("\n=== Demonstration of different backoff strategies ===");

    // Show different backoff strategies
    let strategies = vec![
        ("Fixed (5s)", BackoffStrategy::Fixed(Duration::from_secs(5))),
        (
            "Exponential (1s - 60s)",
            BackoffStrategy::Exponential {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(60),
            },
        ),
        (
            "Fibonacci (1s - 100s)",
            BackoffStrategy::Fibonacci {
                initial: Duration::from_secs(1),
                max: Duration::from_secs(100),
            },
        ),
    ];

    for (name, strategy) in strategies {
        tracing::info!("\n{}", name);
        for attempt in 0..6 {
            let delay = strategy.calculate(attempt);
            tracing::info!("  Attempt {}: wait {}s", attempt, delay.as_secs());
        }
    }

    tracing::info!("\n=== Demonstration of replay priorities ===");

    let priorities = vec![
        ("FIFO (oldest first)", ReplayPriority::FIFO),
        ("LIFO (newest first)", ReplayPriority::LIFO),
        ("By retry count", ReplayPriority::ByRetryCount),
        (
            "By error type",
            ReplayPriority::ByErrorType(vec!["timeout".to_string(), "network".to_string()]),
        ),
    ];

    for (name, priority) in priorities {
        tracing::info!("{}: ORDER BY {}", name, priority.order_by_clause());
    }

    // Cleanup
    let _ = std::fs::remove_dir_all("./tmp/dlq_example");

    Ok(())
}
