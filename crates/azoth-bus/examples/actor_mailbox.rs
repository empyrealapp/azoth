//! Actor Mailbox Example
//!
//! This example demonstrates using azoth-bus as an actor mailbox system
//! where messages accumulate and actors process them when ready.
//!
//! Features demonstrated:
//! - Buffered message consumption (not real-time)
//! - Multiple actors with independent mailboxes
//! - Actors can be offline and catch up later
//! - Message durability (survives restarts)

use azoth::AzothDb;
use azoth_bus::EventBus;
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Actor Mailbox Demo ===\n");

    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);

    // Use default (polling) wake strategy - no immediate notifications
    // Messages accumulate in the mailbox until the actor reads them
    let bus = EventBus::new(db.clone());

    // ==========================================================
    // Phase 1: Send messages to actors (actors are "offline")
    // ==========================================================
    println!("Phase 1: Sending messages while actors are offline...\n");

    // Send messages to Actor A
    for i in 1..=5 {
        bus.publish(
            "actor-a",
            "task",
            &serde_json::json!({
                "task_id": i,
                "priority": if i <= 2 { "high" } else { "normal" }
            }),
        )?;
        println!("  -> Sent task {} to actor-a", i);
    }

    // Send messages to Actor B
    for i in 1..=3 {
        bus.publish(
            "actor-b",
            "request",
            &serde_json::json!({
                "request_id": i,
                "data": format!("Request data {}", i)
            }),
        )?;
        println!("  -> Sent request {} to actor-b", i);
    }

    println!("\nMessages are now durably stored, waiting for actors.\n");

    // ==========================================================
    // Phase 2: Actor A comes online and processes its mailbox
    // ==========================================================
    println!("Phase 2: Actor A comes online...\n");

    let mut actor_a = bus.subscribe("actor-a", "worker")?;

    // Check mailbox status
    let info = bus.consumer_info("actor-a", "worker")?;
    println!("[Actor A] Mailbox has {} messages waiting", info.lag);

    // Process all waiting messages
    while let Some(event) = actor_a.next()? {
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap_or_default();

        let task_id = payload.get("task_id").and_then(|v| v.as_i64()).unwrap_or(0);
        let priority = payload
            .get("priority")
            .and_then(|v| v.as_str())
            .unwrap_or("?");

        println!(
            "[Actor A] Processing task {} (priority: {})",
            task_id, priority
        );

        // Simulate some work
        std::thread::sleep(std::time::Duration::from_millis(50));

        actor_a.ack(event.id)?;
    }

    println!("[Actor A] Mailbox empty, going idle.\n");

    // ==========================================================
    // Phase 3: More messages arrive while Actor A is "idle"
    // ==========================================================
    println!("Phase 3: New messages arrive...\n");

    bus.publish(
        "actor-a",
        "task",
        &serde_json::json!({
            "task_id": 6,
            "priority": "urgent"
        }),
    )?;
    println!("  -> Sent urgent task 6 to actor-a");

    bus.publish(
        "actor-a",
        "task",
        &serde_json::json!({
            "task_id": 7,
            "priority": "normal"
        }),
    )?;
    println!("  -> Sent task 7 to actor-a\n");

    // ==========================================================
    // Phase 4: Actor A wakes up and processes new messages
    // ==========================================================
    println!("Phase 4: Actor A checks for new messages...\n");

    let info = bus.consumer_info("actor-a", "worker")?;
    println!("[Actor A] {} new messages waiting", info.lag);

    while let Some(event) = actor_a.next()? {
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap_or_default();

        let task_id = payload.get("task_id").and_then(|v| v.as_i64()).unwrap_or(0);
        let priority = payload
            .get("priority")
            .and_then(|v| v.as_str())
            .unwrap_or("?");

        println!(
            "[Actor A] Processing task {} (priority: {})",
            task_id, priority
        );
        actor_a.ack(event.id)?;
    }

    println!("[Actor A] Done.\n");

    // ==========================================================
    // Phase 5: Actor B processes its mailbox (independent cursor)
    // ==========================================================
    println!("Phase 5: Actor B comes online...\n");

    let mut actor_b = bus.subscribe("actor-b", "handler")?;

    let info = bus.consumer_info("actor-b", "handler")?;
    println!("[Actor B] Mailbox has {} messages waiting", info.lag);

    while let Some(event) = actor_b.next()? {
        let payload: serde_json::Value = serde_json::from_slice(&event.payload).unwrap_or_default();

        let request_id = payload
            .get("request_id")
            .and_then(|v| v.as_i64())
            .unwrap_or(0);
        let data = payload.get("data").and_then(|v| v.as_str()).unwrap_or("?");

        println!("[Actor B] Handling request {}: {}", request_id, data);
        actor_b.ack(event.id)?;
    }

    println!("[Actor B] Done.\n");

    // ==========================================================
    // Summary
    // ==========================================================
    println!("=== Summary ===");
    println!("- Messages were stored durably even while actors were offline");
    println!("- Each actor has its own independent cursor (mailbox position)");
    println!("- Actors process messages at their own pace");
    println!("- New messages accumulate until actors read them");
    println!("- This pattern supports: work queues, task distribution, decoupled services");

    Ok(())
}
