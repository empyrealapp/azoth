//! Cron scheduler example
//!
//! Demonstrates cron-based scheduling with realistic tasks

use azoth::AzothDb;
use azoth_scheduler::prelude::*;
use chrono::Utc;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Serialize, Deserialize)]
struct ReportConfig {
    report_type: String,
    recipients: Vec<String>,
}

// Report generation handler
struct ReportHandler;

impl TaskHandler for ReportHandler {
    fn task_type(&self) -> &str {
        "generate_report"
    }

    fn execute(&self, ctx: &TaskContext, payload: &[u8]) -> Result<TaskEvent> {
        let config: ReportConfig = serde_json::from_slice(payload)
            .map_err(|e| SchedulerError::HandlerError(format!("Invalid payload: {}", e)))?;

        println!(
            "[{}] Generating {} report for {} recipients...",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            config.report_type,
            config.recipients.len()
        );

        // Simulate report generation
        std::thread::sleep(Duration::from_millis(500));

        println!(
            "[{}] Report {} generated successfully",
            Utc::now().format("%Y-%m-%d %H:%M:%S"),
            ctx.execution_id
        );

        Ok(TaskEvent {
            event_type: "report_generated".to_string(),
            payload: serde_json::to_vec(&serde_json::json!({
                "report_type": config.report_type,
                "execution_id": ctx.execution_id,
                "generated_at": Utc::now().to_rfc3339(),
            }))
            .unwrap(),
        })
    }

    fn validate(&self, payload: &[u8]) -> Result<()> {
        serde_json::from_slice::<ReportConfig>(payload)
            .map_err(|e| SchedulerError::InvalidTask(format!("Invalid report config: {}", e)))?;
        Ok(())
    }
}

// Cleanup handler
struct CleanupHandler;

impl TaskHandler for CleanupHandler {
    fn task_type(&self) -> &str {
        "cleanup"
    }

    fn execute(&self, _ctx: &TaskContext, _payload: &[u8]) -> Result<TaskEvent> {
        println!(
            "[{}] Running cleanup tasks...",
            Utc::now().format("%Y-%m-%d %H:%M:%S")
        );

        std::thread::sleep(Duration::from_millis(300));

        println!(
            "[{}] Cleanup completed",
            Utc::now().format("%Y-%m-%d %H:%M:%S")
        );

        Ok(TaskEvent {
            event_type: "cleanup_completed".to_string(),
            payload: vec![],
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter("azoth_scheduler=info,cron_tasks=info")
        .init();

    println!("=== Azoth Scheduler - Cron Example ===\n");

    // Setup database and projection
    let db = Arc::new(AzothDb::open("./data/cron-example")?);
    let conn = Arc::new(Connection::open("./data/cron-example/projection.db")?);

    // Create scheduler
    let mut scheduler = Scheduler::builder(db.clone())
        .with_task_handler(ReportHandler)
        .with_task_handler(CleanupHandler)
        .with_poll_interval(Duration::from_secs(1))
        .build(conn)?;

    println!("Scheduler created. Setting up cron tasks...\n");

    // Daily report at midnight (for demo, runs every minute)
    scheduler.schedule_task(
        ScheduleTaskRequest::builder("daily-report")
            .task_type("generate_report")
            .cron("0 * * * * *") // Every minute (use "0 0 0 * * *" for midnight daily)
            .payload(
                serde_json::to_vec(&ReportConfig {
                    report_type: "Daily Summary".to_string(),
                    recipients: vec!["admin@example.com".to_string()],
                })
                .unwrap(),
            )
            .build()?,
    )?;
    println!("✓ Scheduled: Daily report (every minute for demo)");

    // Weekly report on Sundays (for demo, runs every 2 minutes)
    scheduler.schedule_task(
        ScheduleTaskRequest::builder("weekly-report")
            .task_type("generate_report")
            .cron("0 */2 * * * *") // Every 2 minutes (use "0 0 0 * * 0" for Sunday midnight)
            .payload(
                serde_json::to_vec(&ReportConfig {
                    report_type: "Weekly Analytics".to_string(),
                    recipients: vec![
                        "admin@example.com".to_string(),
                        "team@example.com".to_string(),
                    ],
                })
                .unwrap(),
            )
            .build()?,
    )?;
    println!("✓ Scheduled: Weekly report (every 2 minutes for demo)");

    // Hourly cleanup (for demo, runs every 30 seconds)
    scheduler.schedule_task(
        ScheduleTaskRequest::builder("hourly-cleanup")
            .task_type("cleanup")
            .cron("*/30 * * * * *") // Every 30 seconds (6-field cron with seconds)
            .payload(vec![])
            .build()?,
    )?;
    println!("✓ Scheduled: Hourly cleanup (every 30 seconds for demo)");

    println!("\nScheduler running for 60 seconds (for demo)...\n");

    // Run scheduler with timeout for demo
    tokio::select! {
        res = scheduler.run() => {
            res?;
        }
        _ = tokio::time::sleep(Duration::from_secs(60)) => {
            println!("\nDemo timeout reached, stopping scheduler...");
            scheduler.shutdown();
        }
    }

    println!("Scheduler stopped.");
    Ok(())
}
