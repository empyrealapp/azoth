//! Real-time async consumer example with notification-based wake strategy
//!
//! This example demonstrates the real-time publish/subscribe pattern where
//! consumers wake instantly when events are published - no polling required.
//!
//! Key features:
//! - `EventBus::with_notifications()` - enables instant wake-up
//! - `bus.publish_async()` - atomically commits event AND notifies consumers (async-safe)
//! - `consumer.next_async()` - waits efficiently for new events
//! - `consumer.ack_async()` - acknowledges events (async-safe)

use azoth::AzothDb;
use azoth_bus::{Consumer, EventBus};
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);

    // Create bus with notification-based wake strategy
    // When using `publish_async()`, consumers wake INSTANTLY - no polling!
    let bus = EventBus::with_notifications(db.clone());

    println!("=== Real-Time Pub/Sub Demo ===\n");
    println!("This example shows instant event delivery using notifications.");
    println!("Consumers wake immediately when bus.publish_async() is called.\n");

    // Create consumers BEFORE entering the async runtime
    // This is necessary because Consumer::new() uses Transaction which
    // panics when called from within a tokio runtime
    let consumers: Vec<Consumer> = (1..=3)
        .map(|i| {
            let name = format!("worker-{}", i);
            bus.subscribe("tasks", &name)
                .expect("Failed to create consumer")
        })
        .collect();

    // Now start the tokio runtime and run the async portion
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async move { run_demo(bus, consumers).await })?;

    Ok(())
}

async fn run_demo(
    bus: EventBus,
    consumers: Vec<Consumer>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Spawn consumer tasks
    let mut consumer_tasks = vec![];

    for mut consumer in consumers {
        let task = tokio::spawn(async move {
            let consumer_name = consumer.name().to_string();
            println!("[{}] Started, waiting for events...", consumer_name);

            let mut count = 0;
            while count < 5 {
                // next_async() waits efficiently using tokio::sync::Notify
                // When bus.publish_async() is called, this wakes INSTANTLY
                match consumer.next_async().await {
                    Ok(Some(event)) => {
                        count += 1;

                        // Parse the payload (it's Vec<u8>)
                        let payload: serde_json::Value =
                            serde_json::from_slice(&event.payload).unwrap_or_default();

                        println!(
                            "[{}] Received event {} - {} (total: {})",
                            consumer_name,
                            event.id,
                            payload.get("data").and_then(|v| v.as_str()).unwrap_or("?"),
                            count
                        );

                        // Acknowledge using async version (safe in async context)
                        if let Err(e) = consumer.ack_async(event.id).await {
                            eprintln!("[{}] Failed to ack: {}", consumer_name, e);
                        }

                        // Simulate some work
                        tokio::time::sleep(Duration::from_millis(20)).await;
                    }
                    Ok(None) => break,
                    Err(e) => {
                        eprintln!("[{}] Error: {}", consumer_name, e);
                        break;
                    }
                }
            }

            println!("[{}] Completed {} events", consumer_name, count);
        });

        consumer_tasks.push(task);
    }

    // Give consumers time to start and begin waiting
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish events using async API (safe in async context)
    println!("\n[Publisher] Starting to publish 15 events...\n");

    for i in 0..15 {
        // Use bus.publish_async() - this atomically:
        // 1. Commits the event to the log (via spawn_blocking)
        // 2. Notifies ALL waiting consumers instantly
        match bus
            .publish_async(
                "tasks", // stream name
                "job",   // event type (becomes "tasks:job")
                serde_json::json!({
                    "id": i,
                    "data": format!("Task {}", i),
                    "timestamp": chrono::Utc::now().to_rfc3339()
                }),
            )
            .await
        {
            Ok(event_id) => {
                println!("[Publisher] Published event {} (id: {})", i, event_id);
            }
            Err(e) => {
                eprintln!("[Publisher] Failed to publish: {}", e);
            }
        }

        // Small delay between publishes
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    println!("\n[Publisher] All events published!");

    // Wait for all consumers to finish (with timeout)
    let timeout = tokio::time::timeout(Duration::from_secs(5), async {
        for task in consumer_tasks {
            let _ = task.await;
        }
    });

    if timeout.await.is_err() {
        println!("\n[Warning] Consumers timed out");
    }

    println!("\n=== Demo Complete ===");
    println!("All consumers received events in real-time without polling!");

    Ok(())
}
