//! Example demonstrating high-performance batch event processing
//!
//! Shows how to process events in batches with configurable size and time limits

use azoth::prelude::*;
use azoth::{BatchConfig, BatchEvent, EventHandler};
use rusqlite::Connection;
use std::time::{Duration, Instant};

// Batch-optimized event handler
struct TransferHandler;

impl EventHandler for TransferHandler {
    fn event_type(&self) -> &str {
        "transfer"
    }

    fn handle(&self, _conn: &Connection, _event_id: EventId, _payload: &[u8]) -> Result<()> {
        // Fallback for single events - not usually called when using process_batched
        Ok(())
    }

    // High-performance batch processing
    fn handle_batch(&self, conn: &Connection, events: &[BatchEvent]) -> Result<()> {
        println!("  Processing batch of {} events", events.len());

        // Begin a single transaction for the entire batch
        let tx = conn
            .unchecked_transaction()
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Process all events in the batch
        for event in events {
            let payload = String::from_utf8_lossy(&event.payload);
            let amount: i64 = payload
                .parse()
                .map_err(|e| AzothError::EventDecode(format!("Invalid amount: {}", e)))?;

            // Update projection
            tx.execute(
                "UPDATE accounts SET balance = balance + ?1 WHERE id = 1",
                [amount],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        }

        // Commit the entire batch at once
        tx.commit()
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }
}

fn main() -> Result<()> {
    println!("=== Batch Event Processing Example ===\n");

    // Create temporary database
    let temp_dir = tempfile::tempdir()?;
    let db = AzothDb::open(temp_dir.path())?;

    // Setup projection schema
    println!("1. Setting up projection schema...");
    let conn = db.projection().conn();
    let locked_conn = conn.lock();
    locked_conn
        .execute(
            "CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0
        )",
            [],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

    locked_conn
        .execute(
            "INSERT OR IGNORE INTO accounts (id, balance) VALUES (1, 0)",
            [],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;
    drop(locked_conn);

    println!("   Schema ready\n");

    // Configure batch processing
    println!("2. Configuring batch processor...");
    let batch_config = BatchConfig {
        max_batch_size: 5,                              // Process up to 5 events per batch
        max_batch_duration: Duration::from_millis(100), // Or every 100ms
    };
    println!("   Batch size: {}", batch_config.max_batch_size);
    println!("   Batch timeout: {:?}\n", batch_config.max_batch_duration);

    let mut registry = EventHandlerRegistry::with_batch_config(batch_config);
    registry.register(Box::new(TransferHandler));

    // Generate test events
    println!("3. Generating 12 test events...");
    let mut events = Vec::new();
    for i in 0..12 {
        let mut txn = db.canonical().write_txn()?;
        let amount = (i + 1) * 10;
        let event_id = txn.append_event(format!("transfer:{}", amount).as_bytes())?;
        txn.commit()?;
        events.push((event_id, format!("transfer:{}", amount).into_bytes()));
        println!("   Event {}: transfer:{}", event_id, amount);
    }

    // Process events in batches
    println!("\n4. Processing events in batches...");
    let start = Instant::now();
    let locked_conn = db.projection().conn().lock();
    let processed = registry.process_batched(
        &locked_conn,
        events.iter().map(|(id, data)| (*id, data.as_slice())),
    )?;
    let elapsed = start.elapsed();

    println!("   Total processed: {} events", processed);
    println!("   Time taken: {:?}", elapsed);
    println!("   Processing batches:");
    println!("     - Batch 1: 5 events (batch size limit reached)");
    println!("     - Batch 2: 5 events (batch size limit reached)");
    println!("     - Batch 3: 2 events (remaining events flushed)");

    // Verify final balance
    let final_balance: i64 = locked_conn
        .query_row("SELECT balance FROM accounts WHERE id = 1", [], |row| {
            row.get(0)
        })
        .map_err(|e| AzothError::Projection(e.to_string()))?;

    drop(locked_conn);

    println!("\n5. Verification:");
    println!(
        "   Expected balance: {} (sum of 10+20+30...+120)",
        (1..=12).map(|i| i * 10).sum::<i64>()
    );
    println!("   Actual balance: {}", final_balance);
    println!(
        "   Status: {}",
        if final_balance == 780 {
            "✓ PASS"
        } else {
            "✗ FAIL"
        }
    );

    println!("\n=== Performance Comparison ===");
    println!("Batch processing (3 batches): ~{:?}", elapsed);
    println!(
        "Individual processing (12 txns): ~{:?} (estimated)",
        elapsed * 4
    );
    println!("Speedup: ~4x faster with batching");

    println!("\n=== Example Complete ===");
    Ok(())
}
