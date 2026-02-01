//! Azoth Scheduler
//!
//! A cron-like task scheduling system for Azoth projects.
//!
//! # Overview
//!
//! The scheduler provides task scheduling capabilities with:
//! - Cron expressions for recurring tasks
//! - Interval-based scheduling
//! - One-time tasks at specific times
//! - Immediate execution (job queue)
//! - Configurable concurrency
//! - Automatic retries with exponential backoff
//!
//! # Architecture
//!
//! The scheduler follows Azoth's event-sourcing architecture:
//!
//! 1. **Schedule Events**: Tasks are scheduled by writing events to the canonical store
//! 2. **Projection**: A SQLite projection tracks task state and next run times
//! 3. **Scheduler Loop**: Continuously polls for due tasks
//! 4. **Task Handlers**: Execute tasks and produce events
//! 5. **Event Handlers**: Process task result events
//!
//! # Example
//!
//! ```ignore
//! use azoth_scheduler::prelude::*;
//! use std::sync::Arc;
//!
//! // Define a task handler
//! struct ReportHandler;
//!
//! impl TaskHandler for ReportHandler {
//!     fn task_type(&self) -> &str {
//!         "generate_report"
//!     }
//!
//!     fn execute(&self, ctx: &TaskContext, payload: &[u8]) -> Result<TaskEvent> {
//!         // Execute task logic
//!         Ok(TaskEvent {
//!             event_type: "report_generated".to_string(),
//!             payload: vec![],
//!         })
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     let db = Arc::new(AzothDb::open("./data")?);
//!     let conn = Arc::new(Connection::open("./projection.db")?);
//!
//!     // Create scheduler
//!     let scheduler = Scheduler::builder(db.clone())
//!         .with_task_handler(ReportHandler)
//!         .with_poll_interval(Duration::from_secs(1))
//!         .build(conn)?;
//!
//!     // Schedule a task
//!     scheduler.schedule_task(
//!         ScheduleTaskRequest::builder("daily-report")
//!             .task_type("generate_report")
//!             .cron("0 0 * * *")  // Daily at midnight
//!             .payload(vec![])
//!             .build()?
//!     )?;
//!
//!     // Run scheduler
//!     scheduler.run().await?;
//!     Ok(())
//! }
//! ```

pub mod config;
pub mod error;
pub mod event_applier;
pub mod events;
pub mod execution;
pub mod projection;
pub mod schedule;
pub mod scheduler;
pub mod task_handler;

pub mod prelude;

// Re-export main types
pub use config::SchedulerConfig;
pub use error::{Result, SchedulerError};
pub use event_applier::SchedulerEventApplier;
pub use events::SchedulerEvent;
pub use projection::{DueTask, ScheduleProjection, ScheduledTask, TaskFilter};
pub use schedule::Schedule;
pub use scheduler::{ScheduleTaskRequest, ScheduleTaskRequestBuilder, Scheduler, SchedulerBuilder};
pub use task_handler::{TaskContext, TaskEvent, TaskHandler, TaskHandlerRegistry};
