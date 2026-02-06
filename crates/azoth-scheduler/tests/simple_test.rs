use azoth::AzothDb;
use azoth_scheduler::prelude::*;
use rusqlite::Connection;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tempfile::TempDir;

// Test task handler that counts executions
struct CounterHandler {
    counter: Arc<AtomicUsize>,
}

impl TaskHandler for CounterHandler {
    fn task_type(&self) -> &str {
        "counter"
    }

    fn execute(&self, _ctx: &TaskContext, _payload: &[u8]) -> Result<TaskEvent> {
        self.counter.fetch_add(1, Ordering::SeqCst);
        Ok(TaskEvent {
            event_type: "counter_incremented".to_string(),
            payload: vec![],
        })
    }
}

fn setup() -> (Arc<AzothDb>, Arc<Connection>, TempDir, TempDir) {
    let data_dir = tempfile::tempdir().unwrap();
    let db_dir = tempfile::tempdir().unwrap();

    let db = Arc::new(AzothDb::open(data_dir.path()).unwrap());
    #[allow(clippy::arc_with_non_send_sync)]
    let conn = Arc::new(Connection::open(db_dir.path().join("projection.db")).unwrap());

    (db, conn, data_dir, db_dir)
}

#[tokio::test]
async fn test_schedule_and_get_task() {
    let (db, conn, _data_dir, _db_dir) = setup();
    let counter = Arc::new(AtomicUsize::new(0));

    let scheduler = Scheduler::builder(db.clone())
        .with_task_handler(CounterHandler {
            counter: counter.clone(),
        })
        .build(conn.clone())
        .unwrap();

    // Schedule an immediate task
    scheduler
        .schedule_task(
            ScheduleTaskRequest::builder("test-immediate")
                .task_type("counter")
                .immediate()
                .payload(vec![])
                .build()
                .unwrap(),
        )
        .unwrap();

    // Need to apply the event to the projection
    let mut applier = SchedulerEventApplier::new(db.canonical().clone(), conn.clone()).unwrap();
    let applied = applier.run_once().unwrap();
    println!("Applied {} events", applied);

    // Verify task exists
    let task = scheduler.get_task("test-immediate").unwrap();
    if task.is_none() {
        println!("Task not found!");
    }
    assert!(
        task.is_some(),
        "Task should exist after applying schedule event"
    );
    assert_eq!(task.unwrap().task_id, "test-immediate");
}

#[tokio::test]
async fn test_schedule_validation() {
    let (db, conn, _data_dir, _db_dir) = setup();

    let scheduler = Scheduler::builder(db.clone())
        .with_task_handler(CounterHandler {
            counter: Arc::new(AtomicUsize::new(0)),
        })
        .build(conn)
        .unwrap();

    // Invalid cron expression (using old 5-field format which this library doesn't support)
    let result = scheduler.schedule_task(
        ScheduleTaskRequest::builder("test")
            .task_type("counter")
            .cron("0 0 * * *") // 5-field cron, but library expects 6
            .payload(vec![])
            .build()
            .unwrap(),
    );
    assert!(result.is_err());

    // Invalid interval
    let result = scheduler.schedule_task(
        ScheduleTaskRequest::builder("test")
            .task_type("counter")
            .interval(0)
            .payload(vec![])
            .build()
            .unwrap(),
    );
    assert!(result.is_err());

    // Handler not found
    let result = scheduler.schedule_task(
        ScheduleTaskRequest::builder("test")
            .task_type("nonexistent")
            .immediate()
            .payload(vec![])
            .build()
            .unwrap(),
    );
    assert!(result.is_err());
}

#[test]
fn test_schedule_types() {
    use chrono::Utc;

    // Test cron schedule (6-field format: sec min hour day month dow)
    let schedule = Schedule::Cron {
        expression: "0 0 0 * * *".to_string(),
    };
    assert!(schedule.validate().is_ok());
    assert!(schedule.is_recurring());

    // Test interval schedule
    let schedule = Schedule::Interval { seconds: 300 };
    assert!(schedule.validate().is_ok());
    assert!(schedule.is_recurring());

    // Test one-time schedule
    let future = Utc::now().timestamp() + 3600;
    let schedule = Schedule::OneTime { run_at: future };
    assert!(schedule.validate().is_ok());
    assert!(!schedule.is_recurring());

    // Test immediate schedule
    let schedule = Schedule::Immediate;
    assert!(schedule.validate().is_ok());
    assert!(!schedule.is_recurring());
}

#[tokio::test]
async fn test_task_cancellation() {
    let (db, conn, _data_dir, _db_dir) = setup();

    let scheduler = Scheduler::builder(db.clone())
        .with_task_handler(CounterHandler {
            counter: Arc::new(AtomicUsize::new(0)),
        })
        .build(conn.clone())
        .unwrap();

    // Schedule a task
    scheduler
        .schedule_task(
            ScheduleTaskRequest::builder("test-cancel")
                .task_type("counter")
                .immediate()
                .payload(vec![])
                .build()
                .unwrap(),
        )
        .unwrap();

    // Apply the schedule event
    let mut applier = SchedulerEventApplier::new(db.canonical().clone(), conn.clone()).unwrap();
    applier.run_once().unwrap();

    // Verify task exists
    let task = scheduler.get_task("test-cancel").unwrap();
    assert!(task.is_some());

    // Cancel the task
    scheduler.cancel_task("test-cancel", "test").unwrap();

    // Apply the cancel event
    applier.run_once().unwrap();

    // Verify task is gone
    let task = scheduler.get_task("test-cancel").unwrap();
    assert!(task.is_none());
}
