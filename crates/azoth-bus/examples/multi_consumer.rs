//! Multi-consumer example showing independent cursors

use azoth::{AzothDb, Transaction};
use azoth_bus::EventBus;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);
    let bus = EventBus::new(db.clone());

    // Publish events
    println!("Publishing 10 events...");
    Transaction::new(&db).execute(|ctx| {
        for i in 0..10 {
            ctx.log(
                "stream:event",
                &serde_json::json!({
                    "number": i,
                    "data": format!("Event {}", i)
                }),
            )?;
        }
        Ok(())
    })?;

    // Create two consumers with independent cursors
    let mut consumer1 = bus.subscribe("stream", "fast-processor")?;
    let mut consumer2 = bus.subscribe("stream", "slow-processor")?;

    // Consumer 1 processes all events
    println!("\nConsumer 1 (fast) processing all events:");
    let mut count1 = 0;
    while let Some(event) = consumer1.next()? {
        println!("  C1 processed event {}", event.id);
        consumer1.ack(event.id)?;
        count1 += 1;
    }
    println!("Consumer 1 processed {} events", count1);

    // Consumer 2 processes only first 5 events
    println!("\nConsumer 2 (slow) processing first 5 events:");
    let mut count2 = 0;
    while let Some(event) = consumer2.next()? {
        println!("  C2 processed event {}", event.id);
        consumer2.ack(event.id)?;
        count2 += 1;
        if count2 >= 5 {
            break;
        }
    }
    println!("Consumer 2 processed {} events", count2);

    // Show lag for each consumer
    println!("\n--- Consumer Status ---");
    let info1 = bus.consumer_info("stream", "fast-processor")?;
    let info2 = bus.consumer_info("stream", "slow-processor")?;

    println!("Consumer 1: position={}, lag={}", info1.position, info1.lag);
    println!("Consumer 2: position={}, lag={}", info2.position, info2.lag);

    // Publish more events
    println!("\nPublishing 5 more events...");
    Transaction::new(&db).execute(|ctx| {
        for i in 10..15 {
            ctx.log(
                "stream:event",
                &serde_json::json!({
                    "number": i,
                    "data": format!("Event {}", i)
                }),
            )?;
        }
        Ok(())
    })?;

    // Consumer 2 resumes and processes remaining events
    println!("\nConsumer 2 resuming...");
    while let Some(event) = consumer2.next()? {
        println!("  C2 processed event {}", event.id);
        consumer2.ack(event.id)?;
        count2 += 1;
    }

    let info2_final = bus.consumer_info("stream", "slow-processor")?;
    println!(
        "\nConsumer 2 final status: position={}, lag={}",
        info2_final.position, info2_final.lag
    );
    println!("Total events processed by consumer 2: {}", count2);

    Ok(())
}
