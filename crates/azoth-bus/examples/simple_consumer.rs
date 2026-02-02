//! Simple consumer example demonstrating basic pub/sub usage

use azoth::{AzothDb, Transaction};
use azoth_bus::{EventBus, EventFilter};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a temporary database
    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);

    // Create the event bus
    let bus = EventBus::new(db.clone());

    // Publish some events to different streams
    println!("Publishing events...");
    Transaction::new(&db).execute(|ctx| {
        ctx.log(
            "knowledge:doc_created",
            &serde_json::json!({
                "id": "doc1",
                "title": "Getting Started"
            }),
        )?;
        ctx.log(
            "knowledge:doc_updated",
            &serde_json::json!({
                "id": "doc1",
                "content": "Welcome to Azoth!"
            }),
        )?;
        ctx.log(
            "bus:message_sent",
            &serde_json::json!({
                "to": "agent1",
                "message": "Hello"
            }),
        )?;
        ctx.log(
            "knowledge:doc_deleted",
            &serde_json::json!({
                "id": "doc1"
            }),
        )?;
        Ok(())
    })?;

    // Subscribe to the "knowledge" stream
    // This automatically filters to only "knowledge:*" events
    let mut consumer = bus.subscribe("knowledge", "doc-processor")?;

    println!("\nProcessing knowledge events:");
    while let Some(event) = consumer.next()? {
        println!(
            "  Event {}: {} - {} bytes",
            event.id,
            event.event_type,
            event.payload.len()
        );

        // Acknowledge the event to advance the cursor
        consumer.ack(event.id)?;

        // Check lag
        let info = bus.consumer_info("knowledge", "doc-processor")?;
        println!("    Position: {}, Lag: {}", info.position, info.lag);
    }

    println!("\nCaught up!");

    // Demonstrate additional filtering
    println!("\n--- Additional Filtering Example ---");

    // Publish more events
    Transaction::new(&db).execute(|ctx| {
        ctx.log("knowledge:doc_created", &serde_json::json!({"id": "doc2"}))?;
        ctx.log(
            "knowledge:index_updated",
            &serde_json::json!({"count": 100}),
        )?;
        ctx.log("knowledge:doc_created", &serde_json::json!({"id": "doc3"}))?;
        Ok(())
    })?;

    // Create a consumer that only sees "knowledge:doc_*" events
    let mut filtered_consumer = bus
        .subscribe("knowledge", "doc-only-processor")?
        .with_filter(EventFilter::prefix("knowledge:doc_"));

    println!("Processing only doc events:");
    while let Some(event) = filtered_consumer.next()? {
        println!("  Event {}: {}", event.id, event.event_type);
        filtered_consumer.ack(event.id)?;
    }

    // Show consumer statistics
    println!("\n--- Consumer Statistics ---");
    let consumers = bus.list_consumers("knowledge")?;
    for info in consumers {
        println!(
            "Consumer '{}': position={}, lag={}",
            info.name, info.position, info.lag
        );
    }

    Ok(())
}
