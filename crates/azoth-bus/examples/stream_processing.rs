//! Stream Processing Pipeline Example
//!
//! This example demonstrates using azoth-bus as a simple actor framework
//! for message passing and stream processing pipelines.
//!
//! Pipeline:
//!   [Generator] --numbers--> [Doubler] --doubled--> [Printer]
//!
//! Features demonstrated:
//! - Multi-stream message passing
//! - Transform and forward pattern
//! - Immediate wake-up (real-time) processing
//! - Buffered consumption (process when ready)

use azoth::AzothDb;
use azoth_bus::{Consumer, EventBus};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Stream Processing Pipeline Demo ===\n");
    println!("Pipeline: Generator -> Doubler -> Printer\n");

    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);

    // Create bus with notifications for real-time processing
    let bus = EventBus::with_notifications(db.clone());

    // Create consumers BEFORE entering async runtime
    // (Consumer creation uses sync transactions)
    let doubler_consumer = bus.subscribe("numbers", "doubler")?;
    let printer_consumer = bus.subscribe("doubled", "printer")?;

    // Flag to signal shutdown
    let running = Arc::new(AtomicBool::new(true));

    // Run the async pipeline
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(run_pipeline(
        bus,
        doubler_consumer,
        printer_consumer,
        running,
    ))?;

    println!("\n=== Pipeline Complete ===");
    Ok(())
}

async fn run_pipeline(
    bus: EventBus,
    doubler_consumer: Consumer,
    printer_consumer: Consumer,
    running: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error>> {
    // Spawn the Doubler agent (consumes from "numbers", publishes to "doubled")
    let bus_doubler = bus.clone();
    let running_doubler = running.clone();
    let doubler_handle =
        tokio::spawn(
            async move { doubler_agent(bus_doubler, doubler_consumer, running_doubler).await },
        );

    // Spawn the Printer agent (consumes from "doubled", prints results)
    let running_printer = running.clone();
    let printer_handle =
        tokio::spawn(async move { printer_agent(printer_consumer, running_printer).await });

    // Give agents time to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Generator: publish numbers to the "numbers" stream
    println!("[Generator] Publishing numbers 1-10...\n");

    for i in 1..=10 {
        bus.publish_async(
            "numbers",
            "value",
            serde_json::json!({
                "value": i,
                "source": "generator"
            }),
        )
        .await?;

        println!("[Generator] Published: {}", i);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Signal completion
    bus.publish_async("numbers", "done", serde_json::json!({}))
        .await?;
    println!("\n[Generator] Done!");

    // Wait a bit for pipeline to drain
    tokio::time::sleep(Duration::from_millis(500)).await;
    running.store(false, Ordering::SeqCst);

    // Wait for agents to finish
    let _ = tokio::time::timeout(Duration::from_secs(2), doubler_handle).await;
    let _ = tokio::time::timeout(Duration::from_secs(2), printer_handle).await;

    Ok(())
}

/// Doubler Agent: Consumes numbers, multiplies by 2, publishes to "doubled" stream
async fn doubler_agent(
    bus: EventBus,
    mut consumer: Consumer,
    running: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("[Doubler] Started, waiting for numbers...");

    while running.load(Ordering::SeqCst) {
        // Wait for next event with timeout
        let event = tokio::time::timeout(Duration::from_millis(100), consumer.next_async()).await;

        match event {
            Ok(Ok(Some(event))) => {
                // Check for done signal
                if event.event_type == "numbers:done" {
                    println!("[Doubler] Received done signal");
                    // Forward done signal
                    bus.publish_async("doubled", "done", serde_json::json!({}))
                        .await?;
                    consumer.ack_async(event.id).await?;
                    break;
                }

                // Parse the value
                let payload: serde_json::Value =
                    serde_json::from_slice(&event.payload).unwrap_or_default();

                if let Some(value) = payload.get("value").and_then(|v| v.as_i64()) {
                    let doubled = value * 2;

                    // Transform and forward to next stream
                    bus.publish_async(
                        "doubled",
                        "value",
                        serde_json::json!({
                            "original": value,
                            "doubled": doubled,
                            "source": "doubler"
                        }),
                    )
                    .await?;

                    println!("[Doubler] {} -> {}", value, doubled);
                }

                // Acknowledge after processing
                consumer.ack_async(event.id).await?;
            }
            Ok(Ok(None)) => {
                // No more events
                break;
            }
            Ok(Err(e)) => {
                eprintln!("[Doubler] Error: {}", e);
                break;
            }
            Err(_) => {
                // Timeout - continue polling
            }
        }
    }

    println!("[Doubler] Stopped");
    Ok(())
}

/// Printer Agent: Consumes doubled values and prints them
async fn printer_agent(
    mut consumer: Consumer,
    running: Arc<AtomicBool>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("[Printer] Started, waiting for doubled values...");

    let mut total = 0i64;
    let mut count = 0;

    while running.load(Ordering::SeqCst) {
        let event = tokio::time::timeout(Duration::from_millis(100), consumer.next_async()).await;

        match event {
            Ok(Ok(Some(event))) => {
                // Check for done signal
                if event.event_type == "doubled:done" {
                    println!("[Printer] Received done signal");
                    consumer.ack_async(event.id).await?;
                    break;
                }

                let payload: serde_json::Value =
                    serde_json::from_slice(&event.payload).unwrap_or_default();

                if let Some(doubled) = payload.get("doubled").and_then(|v| v.as_i64()) {
                    let original = payload
                        .get("original")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    println!("[Printer] Received: {} (original: {})", doubled, original);
                    total += doubled;
                    count += 1;
                }

                consumer.ack_async(event.id).await?;
            }
            Ok(Ok(None)) => break,
            Ok(Err(e)) => {
                eprintln!("[Printer] Error: {}", e);
                break;
            }
            Err(_) => {
                // Timeout - continue
            }
        }
    }

    println!(
        "\n[Printer] Summary: Processed {} values, total = {}",
        count, total
    );
    println!("[Printer] Stopped");
    Ok(())
}
