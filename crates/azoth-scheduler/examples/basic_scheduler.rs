//! Basic scheduler example
//!
//! Demonstrates:
//! - Immediate task execution
//! - Interval-based scheduling
//! - Task handlers and events

use azoth::AzothDb;
use azoth_scheduler::prelude::*;
use rusqlite::Connection;
use std::sync::Arc;
use std::time::Duration;

// Simple task handler that prints a message
struct PrintHandler;

impl TaskHandler for PrintHandler {
    fn task_type(&self) -> &str {
        "print"
    }

    fn execute(&self, ctx: &TaskContext, payload: &[u8]) -> Result<TaskEvent> {
        let message = String::from_utf8_lossy(payload);
        println!(
            "[Task {}] Execution #{}: {}",
            ctx.task_id, ctx.execution_attempt, message
        );

        Ok(TaskEvent {
            event_type: "message_printed".to_string(),
            payload: payload.to_vec(),
        })
    }
}

// Task handler that demonstrates work
struct WorkHandler;

impl TaskHandler for WorkHandler {
    fn task_type(&self) -> &str {
        "work"
    }

    fn execute(&self, ctx: &TaskContext, _payload: &[u8]) -> Result<TaskEvent> {
        println!(
            "[Task {}] Starting work (execution #{})",
            ctx.task_id, ctx.execution_attempt
        );

        // Simulate some work
        std::thread::sleep(Duration::from_millis(500));

        println!("[Task {}] Work completed", ctx.task_id);

        Ok(TaskEvent {
            event_type: "work_completed".to_string(),
            payload: serde_json::to_vec(&serde_json::json!({
                "task_id": ctx.task_id,
                "execution_id": ctx.execution_id,
            }))
            .unwrap(),
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("azoth_scheduler=debug,basic_scheduler=debug")
        .init();

    println!("=== Azoth Scheduler Example ===\n");

    // Setup database and projection
    let db = Arc::new(AzothDb::open("./data/scheduler-example")?);
    #[allow(clippy::arc_with_non_send_sync)]
    let conn = Arc::new(Connection::open("./data/scheduler-example/projection.db")?);

    // Create scheduler
    let mut scheduler = Scheduler::builder(db.clone())
        .with_task_handler(PrintHandler)
        .with_task_handler(WorkHandler)
        .with_poll_interval(Duration::from_secs(1))
        .with_max_concurrent_tasks(5)
        .build(conn)?;

    println!("Scheduler created. Scheduling tasks...\n");

    // Schedule an immediate task
    scheduler.schedule_task(
        ScheduleTaskRequest::builder("welcome")
            .task_type("print")
            .immediate()
            .payload(b"Welcome to Azoth Scheduler!".to_vec())
            .build()?,
    )?;

    // Schedule an interval task
    scheduler.schedule_task(
        ScheduleTaskRequest::builder("heartbeat")
            .task_type("print")
            .interval(3) // Every 3 seconds
            .payload(b"Heartbeat...".to_vec())
            .build()?,
    )?;

    // Schedule a work task
    scheduler.schedule_task(
        ScheduleTaskRequest::builder("background-work")
            .task_type("work")
            .interval(5) // Every 5 seconds
            .payload(vec![])
            .build()?,
    )?;

    println!("Tasks scheduled. Running for 30 seconds (for demo)...\n");

    // Run scheduler with timeout for demo
    tokio::select! {
        res = scheduler.run() => {
            res?;
        }
        _ = tokio::time::sleep(Duration::from_secs(30)) => {
            println!("\nDemo timeout reached, stopping scheduler...");
            scheduler.shutdown();
        }
    }

    println!("Scheduler stopped.");
    Ok(())
}
