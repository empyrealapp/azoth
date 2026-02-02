//! Consumer group example demonstrating load-balanced consumption

use azoth::{AzothDb, Transaction};
use azoth_bus::EventBus;
use std::sync::Arc;
use std::time::Duration;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp.path())?);

    // Create bus and consumer group
    let bus = EventBus::new(db.clone());
    let group = bus.consumer_group("tasks", "workers");

    println!("=== Consumer Group Demo ===\n");

    // Publish 20 task events
    println!("Publishing 20 task events...");
    Transaction::new(&db).execute(|ctx| {
        for i in 0..20 {
            ctx.log(
                "tasks:process",
                &serde_json::json!({
                    "task_id": i,
                    "data": format!("Task {}", i)
                }),
            )?;
        }
        Ok(())
    })?;

    println!("Published 20 events\n");

    // Create 3 worker members
    println!("Creating 3 workers...");
    let mut worker1 = group.join("worker-1")?;
    let mut worker2 = group.join("worker-2")?;
    let mut worker3 = group.join("worker-3")?;

    // List members
    let members = group.list_members()?;
    println!("Active members: {:?}\n", members);

    // Simulate parallel consumption
    println!("--- Workers claiming and processing tasks ---\n");

    let mut w1_count = 0;
    let mut w2_count = 0;
    let mut w3_count = 0;

    loop {
        let mut claimed_any = false;

        // Worker 1
        if let Some(claimed) = worker1.claim_next()? {
            println!(
                "Worker 1 claimed task {} (lease expires at {})",
                claimed.event.id, claimed.lease_expires_at
            );
            // Simulate processing
            std::thread::sleep(Duration::from_millis(10));
            worker1.release(claimed.event.id, true)?;
            w1_count += 1;
            claimed_any = true;
        }

        // Worker 2
        if let Some(claimed) = worker2.claim_next()? {
            println!("Worker 2 claimed task {}", claimed.event.id);
            std::thread::sleep(Duration::from_millis(10));
            worker2.release(claimed.event.id, true)?;
            w2_count += 1;
            claimed_any = true;
        }

        // Worker 3
        if let Some(claimed) = worker3.claim_next()? {
            println!("Worker 3 claimed task {}", claimed.event.id);
            std::thread::sleep(Duration::from_millis(10));
            worker3.release(claimed.event.id, true)?;
            w3_count += 1;
            claimed_any = true;
        }

        if !claimed_any {
            break; // All caught up
        }
    }

    println!("\n--- Processing Summary ---");
    println!("Worker 1 processed: {} tasks", w1_count);
    println!("Worker 2 processed: {} tasks", w2_count);
    println!("Worker 3 processed: {} tasks", w3_count);
    println!("Total processed: {}", w1_count + w2_count + w3_count);

    // Demonstrate nack and reclaim
    println!("\n--- Nack and Reclaim Demo ---\n");

    // Publish more tasks
    Transaction::new(&db).execute(|ctx| {
        for i in 20..25 {
            ctx.log(
                "tasks:process",
                &serde_json::json!({
                    "task_id": i,
                    "data": format!("Task {}", i)
                }),
            )?;
        }
        Ok(())
    })?;

    println!("Published 5 more tasks\n");

    // Worker 1 claims and nacks a task
    if let Some(claimed) = worker1.claim_next()? {
        println!(
            "Worker 1 claimed task {} but failed to process (nack)",
            claimed.event.id
        );
        worker1.release(claimed.event.id, false)?; // Nack!
    }

    // Worker 2 should get the nacked task
    if let Some(claimed) = worker2.claim_next()? {
        println!(
            "Worker 2 reclaimed task {} (was nacked by worker 1)",
            claimed.event.id
        );
        worker2.release(claimed.event.id, true)?;
    }

    // Demonstrate expired claim cleanup
    println!("\n--- Expired Claim Cleanup Demo ---\n");

    // Create a group with very short lease
    let short_lease_group =
        bus.consumer_group_with_lease("tasks", "short-lease-workers", Duration::from_millis(10));

    let mut worker = short_lease_group.join("worker-timeout")?;

    // Publish a task
    Transaction::new(&db).execute(|ctx| {
        ctx.log(
            "tasks:timeout",
            &serde_json::json!({
                "task_id": 100,
                "data": "Timeout task"
            }),
        )?;
        Ok(())
    })?;

    // Claim but don't release (simulate worker crash)
    if let Some(claimed) = worker.claim_next()? {
        println!(
            "Worker claimed task {} but crashed (not releasing)",
            claimed.event.id
        );
        // Intentionally not releasing

        // Wait for lease to expire
        std::thread::sleep(Duration::from_millis(20));

        // Cleanup expired claims
        let cleaned = worker.cleanup_expired_claims()?;
        println!("Cleaned up {} expired claims", cleaned);

        // Try to claim again - should get the same task from reclaim list
        if let Some(reclaimed) = worker.claim_next()? {
            println!("Worker reclaimed task {} after timeout", reclaimed.event.id);
            worker.release(reclaimed.event.id, true)?;
        }
    }

    println!("\n✓ Consumer group demo completed");

    Ok(())
}
