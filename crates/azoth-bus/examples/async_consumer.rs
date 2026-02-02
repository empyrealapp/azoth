//! Async consumer example with polling wake strategy

use azoth::{AzothDb, Transaction};
use azoth_bus::{EventBus, WakeStrategy};
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);

    // Create bus with polling wake strategy (10ms interval)
    let bus =
        EventBus::with_wake_strategy(db.clone(), WakeStrategy::poll(Duration::from_millis(10)));

    println!("Starting async consumer (polling mode)...\n");

    // Spawn consumer task
    let db_clone = db.clone();
    let consumer_task = tokio::spawn(async move {
        let mut consumer = bus.subscribe("stream", "async-worker").unwrap();
        let mut count = 0;

        println!("Consumer waiting for events...");

        // Process events as they arrive
        while count < 10 {
            if let Some(event) = consumer.next_async().await.unwrap() {
                count += 1;
                println!(
                    "  [Consumer] Processed event {}: {} (total: {})",
                    event.id, event.event_type, count
                );
                consumer.ack(event.id).unwrap();
            }
        }

        println!("\n[Consumer] Processed 10 events, exiting");
    });

    // Give consumer time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Publish events periodically
    println!("[Publisher] Publishing events...\n");
    for i in 0..10 {
        tokio::time::sleep(Duration::from_millis(100)).await;

        Transaction::new(&db_clone).execute(|ctx| {
            ctx.log(
                "stream:data",
                &serde_json::json!({
                    "number": i,
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }),
            )?;
            Ok(())
        })?;

        println!("[Publisher] Published event {}", i);
    }

    // Wait for consumer to finish
    consumer_task.await?;

    println!("\n✓ Example completed successfully");

    Ok(())
}
