//! Async consumer example with notification-based wake strategy

use azoth::{AzothDb, Transaction};
use azoth_bus::EventBus;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);

    // Create bus with notification-based wake strategy
    // Note: In a real application, you would trigger notifications when
    // events are published. For now, this demonstrates the API.
    let bus = EventBus::with_notifications(db.clone());

    println!("Starting async consumer (notification mode)...\n");
    println!("Note: This example uses polling internally since we don't have");
    println!("commit hooks yet, but demonstrates the notification API.\n");

    // Spawn multiple consumers
    let mut tasks = vec![];

    for i in 1..=3 {
        let bus_clone = bus.clone();
        let task = tokio::spawn(async move {
            let consumer_name = format!("worker-{}", i);
            let mut consumer = bus_clone.subscribe("tasks", &consumer_name).unwrap();

            println!("[{}] Started, waiting for events...", consumer_name);

            let mut count = 0;
            while count < 5 {
                // This will use the notification strategy
                // With WakeStrategy::Notify, it waits on tokio::sync::Notify
                // With WakeStrategy::Poll, it checks periodically
                if let Some(event) = consumer.next_async().await.unwrap() {
                    count += 1;
                    println!(
                        "[{}] Processed event {} (count: {})",
                        consumer_name, event.id, count
                    );
                    consumer.ack(event.id).unwrap();

                    // Simulate work
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }

            println!("[{}] Completed", consumer_name);
        });

        tasks.push(task);
    }

    // Give consumers time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Publish events
    println!("\n[Publisher] Publishing 15 events...\n");
    for i in 0..15 {
        Transaction::new(&db).execute(|ctx| {
            ctx.log(
                "tasks:job",
                &serde_json::json!({
                    "id": i,
                    "data": format!("Task {}", i)
                }),
            )?;
            Ok(())
        })?;

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Wait for all consumers
    for task in tasks {
        task.await?;
    }

    println!("\n✓ All consumers completed");

    Ok(())
}
