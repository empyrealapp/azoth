//! Retention policy example demonstrating automatic event cleanup

use azoth::{AzothDb, CanonicalStore, Transaction};
use azoth_bus::{EventBus, RetentionManager, RetentionPolicy};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);

    // Create bus and retention manager
    let bus = EventBus::new(db.clone());
    let mgr = RetentionManager::new(db.clone());

    println!("=== Retention Policy Demo ===\n");

    // Publish 100 events to the stream
    println!("Publishing 100 events...");
    Transaction::new(&db).execute(|ctx| {
        for i in 0..100 {
            ctx.log(
                "logs:entry",
                &serde_json::json!({
                    "id": i,
                    "message": format!("Log entry {}", i)
                }),
            )?;
        }
        Ok(())
    })?;

    let meta = db.canonical().meta()?;
    println!("Total events in log: {}\n", meta.next_event_id);

    // Create a slow consumer that processes only 30 events
    println!("Creating slow consumer...");
    let mut slow_consumer = bus.subscribe("logs", "slow-processor")?;

    for _ in 0..30 {
        if let Some(event) = slow_consumer.next()? {
            slow_consumer.ack(event.id)?;
        }
    }

    let info = bus.consumer_info("logs", "slow-processor")?;
    println!(
        "Slow consumer position: {}, lag: {}\n",
        info.position, info.lag
    );

    // Set retention policy to keep only last 50 events
    println!("Setting retention policy: KeepCount(50)");
    mgr.set_retention("logs", RetentionPolicy::KeepCount(50))?;

    // Run compaction
    println!("Running compaction...");
    let stats = mgr.compact("logs")?;

    println!("\nCompaction Results:");
    println!(
        "  Policy cutoff: {} (keep events >= this)",
        stats.policy_cutoff
    );
    println!("  Min consumer position: {:?}", stats.min_consumer_position);
    println!(
        "  Actual cutoff: {} (respects slow consumer)",
        stats.actual_cutoff
    );
    println!("  Events deleted: {} (not implemented yet)", stats.deleted);

    println!("\nExplanation:");
    println!("- Policy wants to keep last 50 events (cutoff at event 50)");
    println!("- But slow consumer is at position 29");
    println!("- So compaction would stop at event 29 to protect the consumer");
    println!("- Events 0-28 would be safe to delete (if deletion was implemented)");

    // Create a fast consumer
    println!("\n--- Fast Consumer Example ---\n");
    let mut fast_consumer = bus.subscribe("logs", "fast-processor")?;

    for _ in 0..90 {
        if let Some(event) = fast_consumer.next()? {
            fast_consumer.ack(event.id)?;
        }
    }

    let fast_info = bus.consumer_info("logs", "fast-processor")?;
    println!(
        "Fast consumer position: {}, lag: {}",
        fast_info.position, fast_info.lag
    );

    // Run compaction again
    println!("\nRunning compaction again...");
    let stats2 = mgr.compact("logs")?;

    println!("\nCompaction Results:");
    println!("  Policy cutoff: {}", stats2.policy_cutoff);
    println!(
        "  Min consumer position: {:?} (slowest is still at 29)",
        stats2.min_consumer_position
    );
    println!("  Actual cutoff: {}", stats2.actual_cutoff);

    // Demonstrate different retention policies
    println!("\n--- Different Retention Policies ---\n");

    println!("KeepAll: Never delete events");
    mgr.set_retention("logs", RetentionPolicy::KeepAll)?;
    let policy = mgr.get_retention("logs")?;
    println!("  Current policy: {:?}", policy);

    println!("\nKeepCount(N): Keep last N events globally");
    mgr.set_retention("logs", RetentionPolicy::KeepCount(20))?;
    let policy = mgr.get_retention("logs")?;
    println!("  Current policy: {:?}", policy);

    println!("\nKeepDays(N): Keep events from last N days");
    mgr.set_retention("logs", RetentionPolicy::KeepDays(7))?;
    let policy = mgr.get_retention("logs")?;
    println!("  Current policy: {:?} (not yet implemented)", policy);

    println!("\n✓ Retention demo completed");

    Ok(())
}
