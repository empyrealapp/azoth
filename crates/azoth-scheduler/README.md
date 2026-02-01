# Azoth Scheduler

A cron-like task scheduling system for Azoth projects, built on event-sourcing principles.

## Features

- **Flexible Scheduling**
  - Cron expressions for complex recurring tasks
  - Simple interval-based scheduling
  - One-time execution at specific timestamps
  - Immediate execution (job queue functionality)

- **Robust Execution**
  - Configurable concurrency limits
  - Automatic retries with exponential backoff
  - Timeout support per task
  - Task cancellation

- **Event-Sourced**
  - All schedule operations are events in the canonical log
  - Task executions produce events for EventHandlers
  - Full audit trail of all task activity
  - Durable state across restarts

## Architecture

```
┌─────────────────────┐
│  Schedule Events    │  TaskScheduled, TaskExecuted, TaskCancelled
│  (Canonical Store)  │  Written via Transaction API
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  Scheduler Loop     │  Continuously checks for due tasks
│  (like Projector)   │  - Polls schedule projection
└──────────┬──────────┘  - Executes via TaskHandler
           │              - Writes result events
           ▼
┌─────────────────────┐
│ Schedule Projection │  SQLite table with next_run_time index
│   (SQLite)          │  Tracks task state, retries, history
└─────────────────────┘
           │
           ▼
┌─────────────────────┐
│   TaskHandler       │  Produces event for EventHandler
│   (User Code)       │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│  EventHandler       │  Processes task result event
│  (User Code)        │  Updates projections, side effects
└─────────────────────┘
```

## Usage

### Basic Example

```rust
use azoth::AzothDb;
use azoth_scheduler::prelude::*;
use std::sync::Arc;
use std::time::Duration;

// Define a task handler
struct ReportHandler;

impl TaskHandler for ReportHandler {
    fn task_type(&self) -> &str {
        "generate_report"
    }

    fn execute(&self, ctx: &TaskContext, payload: &[u8]) -> Result<TaskEvent> {
        // Your task logic here
        println!("Generating report...");

        Ok(TaskEvent {
            event_type: "report_generated".to_string(),
            payload: vec![],
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let db = Arc::new(AzothDb::open("./data")?);
    let conn = Arc::new(Connection::open("./projection.db")?);

    // Create scheduler
    let scheduler = Scheduler::builder(db.clone())
        .with_task_handler(ReportHandler)
        .with_poll_interval(Duration::from_secs(1))
        .build(conn)?;

    // Schedule tasks
    scheduler.schedule_task(
        ScheduleTaskRequest::builder("daily-report")
            .task_type("generate_report")
            .cron("0 0 0 * * *")  // Daily at midnight (6-field: sec min hour day month dow)
            .payload(vec![])
            .build()?
    )?;

    // Run scheduler
    scheduler.run().await?;
    Ok(())
}
```

### Schedule Types

#### Cron Expression
```rust
scheduler.schedule_task(
    ScheduleTaskRequest::builder("task-id")
        .task_type("my_task")
        .cron("0 0 0 * * *")  // Daily at midnight
        .payload(vec![])
        .build()?
)?;
```

Cron format: `second minute hour day_of_month month day_of_week` (6-field with seconds)
- `0 0 0 * * *` - Daily at midnight
- `0 0 */6 * * *` - Every 6 hours
- `0 0 9 * * 1-5` - Weekdays at 9 AM
- `*/30 * * * * *` - Every 30 seconds

#### Interval
```rust
scheduler.schedule_task(
    ScheduleTaskRequest::builder("task-id")
        .task_type("my_task")
        .interval(300)  // Every 5 minutes (300 seconds)
        .payload(vec![])
        .build()?
)?;
```

#### One-Time
```rust
let run_at = Utc::now().timestamp() + 3600; // 1 hour from now

scheduler.schedule_task(
    ScheduleTaskRequest::builder("task-id")
        .task_type("my_task")
        .one_time(run_at)
        .payload(vec![])
        .build()?
)?;
```

#### Immediate
```rust
scheduler.schedule_task(
    ScheduleTaskRequest::builder("task-id")
        .task_type("my_task")
        .immediate()
        .payload(vec![])
        .build()?
)?;
```

### Task Handlers

Task handlers execute the actual work and produce events:

```rust
struct EmailHandler;

impl TaskHandler for EmailHandler {
    fn task_type(&self) -> &str {
        "send_email"
    }

    fn execute(&self, ctx: &TaskContext, payload: &[u8]) -> Result<TaskEvent> {
        let email: EmailData = serde_json::from_slice(payload)?;

        // Send email
        send_email(&email)?;

        // Return event to be processed by EventHandlers
        Ok(TaskEvent {
            event_type: "email_sent".to_string(),
            payload: serde_json::to_vec(&EmailSentEvent {
                email_id: email.id,
                sent_at: Utc::now(),
            })?,
        })
    }

    fn validate(&self, payload: &[u8]) -> Result<()> {
        serde_json::from_slice::<EmailData>(payload)
            .map_err(|e| SchedulerError::InvalidTask(e.to_string()))?;
        Ok(())
    }
}
```

### Configuration

```rust
let scheduler = Scheduler::builder(db)
    .with_task_handler(MyHandler)
    .with_poll_interval(Duration::from_secs(1))      // How often to check for due tasks
    .with_max_concurrent_tasks(10)                    // Max parallel executions
    .with_default_max_retries(3)                      // Retries on failure
    .with_default_timeout_secs(300)                   // 5 minute timeout
    .build(conn)?;
```

### Task Management

```rust
// Cancel a task
scheduler.cancel_task("task-id", "No longer needed")?;

// Get task info
let task = scheduler.get_task("task-id")?;

// List all tasks
let tasks = scheduler.list_tasks(&TaskFilter::new())?;

// List only enabled tasks
let tasks = scheduler.list_tasks(&TaskFilter::new().enabled(true))?;
```

## Retry Behavior

When a task fails:
1. The failure is recorded with a `TaskExecuted` event (success: false)
2. The task's retry count is incremented
3. If retry count < max retries:
   - Task is rescheduled with exponential backoff (1min, 2min, 4min, etc.)
4. If retry count >= max retries:
   - Task is disabled
   - Check execution history to diagnose

## Examples

Run the included examples:

```bash
# Basic scheduler with immediate and interval tasks
cargo run --example basic_scheduler

# Cron-based scheduling
cargo run --example cron_tasks
```

## Testing

```bash
cargo test --package azoth-scheduler
```

## Design Rationale

**Why event-triggered tasks?**
- Fully event-sourced (all executions in event log)
- Testable and debuggable (inspect events)
- Distributed processing (handlers can run anywhere)
- Consistent with Azoth's architecture

**Why projection-based state?**
- Fast queries by next_run_time
- SQL queries for task management
- Durable across restarts

**Why poll-based timing?**
- Simple implementation
- Works with multiple schedulers (future feature)
- Minimum latency = poll interval (acceptable at 1 second)
