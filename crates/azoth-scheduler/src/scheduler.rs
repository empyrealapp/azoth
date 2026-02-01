//! Main scheduler implementation.

use crate::config::SchedulerConfig;
use crate::error::{Result, SchedulerError};
use crate::events::SchedulerEvent;
use crate::execution::execute_task;
use crate::projection::{ScheduleProjection, ScheduledTask, TaskFilter};
use crate::schedule::Schedule;
use crate::task_handler::{TaskHandler, TaskHandlerRegistry};
use azoth::AzothDb;
use azoth_core::traits::{CanonicalStore, CanonicalTxn};
use chrono::Utc;
use rusqlite::Connection;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Main scheduler struct.
///
/// The scheduler continuously polls for due tasks and executes them
/// using registered task handlers. Tasks are executed concurrently
/// with a configurable limit.
pub struct Scheduler {
    db: Arc<AzothDb>,
    projection: Arc<ScheduleProjection>,
    handler_registry: Arc<TaskHandlerRegistry>,
    config: SchedulerConfig,
    shutdown: Arc<AtomicBool>,
    concurrent_tasks: Arc<AtomicUsize>,
}

impl Scheduler {
    /// Create a new scheduler builder.
    pub fn builder(db: Arc<AzothDb>) -> SchedulerBuilder {
        SchedulerBuilder::new(db)
    }

    /// Run the scheduler loop.
    ///
    /// This polls for due tasks and executes them until shutdown is signaled.
    /// Task execution happens in separate tokio tasks and does not block
    /// the scheduler loop.
    ///
    /// Takes `&mut self` to ensure exclusive access to the SQLite connection.
    pub async fn run(&mut self) -> Result<()> {
        info!("Scheduler starting");

        while !self.shutdown.load(Ordering::SeqCst) {
            let now = Utc::now().timestamp();

            // Get due tasks
            let due_tasks = match self.projection.get_due_tasks(now) {
                Ok(tasks) => tasks,
                Err(e) => {
                    error!("Failed to get due tasks: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                }
            };

            if due_tasks.is_empty() {
                // No due tasks - sleep until next task or poll interval
                let next_wake = self
                    .projection
                    .get_next_wake_time()
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| {
                        Utc::now() + chrono::Duration::from_std(self.config.poll_interval).unwrap()
                    });

                let sleep_duration = (next_wake.timestamp() - Utc::now().timestamp()).max(0) as u64;
                let sleep_duration =
                    Duration::from_secs(sleep_duration).min(self.config.poll_interval);

                debug!("No due tasks, sleeping for {:?}", sleep_duration);
                tokio::time::sleep(sleep_duration).await;
                continue;
            }

            debug!("Found {} due tasks", due_tasks.len());

            // Execute tasks with concurrency limit
            for task in due_tasks {
                // Wait if at concurrency limit
                while self.concurrent_tasks.load(Ordering::SeqCst)
                    >= self.config.max_concurrent_tasks
                {
                    debug!("At concurrency limit, waiting");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                // Spawn task execution (non-blocking)
                let db = self.db.clone();
                let handler_registry = self.handler_registry.clone();
                let concurrent_tasks = self.concurrent_tasks.clone();

                concurrent_tasks.fetch_add(1, Ordering::SeqCst);

                tokio::spawn(async move {
                    let result = execute_task(db, handler_registry, task).await;
                    concurrent_tasks.fetch_sub(1, Ordering::SeqCst);

                    if let Err(e) = result {
                        error!("Task execution failed: {}", e);
                    }
                });
            }

            // Small sleep to avoid busy loop
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Wait for all tasks to complete before shutting down
        info!("Scheduler shutting down, waiting for tasks to complete");
        while self.concurrent_tasks.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        info!("Scheduler stopped");
        Ok(())
    }

    /// Signal graceful shutdown.
    pub fn shutdown(&self) {
        info!("Shutdown signal received");
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Schedule a task.
    ///
    /// This writes a `TaskScheduled` event to the canonical store,
    /// which will be applied to the projection by the event processor.
    pub fn schedule_task(&self, request: ScheduleTaskRequest) -> Result<()> {
        // Validate schedule
        request.schedule.validate()?;

        // Validate handler exists
        if !self.handler_registry.has(&request.task_type) {
            return Err(SchedulerError::HandlerNotFound(request.task_type));
        }

        // Validate payload with handler
        let handler = self.handler_registry.get(&request.task_type)?;
        handler.validate(&request.payload)?;

        // Write event
        let event = SchedulerEvent::TaskScheduled {
            task_id: request.task_id,
            task_type: request.task_type,
            schedule: request.schedule,
            payload: request.payload,
            max_retries: request.max_retries,
            timeout_secs: request.timeout_secs,
        };

        let mut txn = self.db.canonical().write_txn()?;
        txn.append_event(&serde_json::to_vec(&event)?)?;
        txn.commit()?;

        Ok(())
    }

    /// Cancel a task.
    ///
    /// This writes a `TaskCancelled` event to the canonical store.
    pub fn cancel_task(&self, task_id: &str, reason: &str) -> Result<()> {
        let event = SchedulerEvent::TaskCancelled {
            task_id: task_id.to_string(),
            reason: reason.to_string(),
        };

        let mut txn = self.db.canonical().write_txn()?;
        txn.append_event(&serde_json::to_vec(&event)?)?;
        txn.commit()?;

        Ok(())
    }

    /// Get a task by ID.
    pub fn get_task(&self, task_id: &str) -> Result<Option<ScheduledTask>> {
        self.projection.get_task(task_id)
    }

    /// List all tasks.
    pub fn list_tasks(&self, filter: &TaskFilter) -> Result<Vec<ScheduledTask>> {
        self.projection.list_tasks(filter)
    }

    /// Get the number of currently executing tasks.
    pub fn concurrent_tasks(&self) -> usize {
        self.concurrent_tasks.load(Ordering::SeqCst)
    }
}

impl Clone for Scheduler {
    fn clone(&self) -> Self {
        Self {
            db: self.db.clone(),
            projection: self.projection.clone(),
            handler_registry: self.handler_registry.clone(),
            config: self.config.clone(),
            shutdown: self.shutdown.clone(),
            concurrent_tasks: self.concurrent_tasks.clone(),
        }
    }
}

/// Builder for creating a scheduler.
pub struct SchedulerBuilder {
    db: Arc<AzothDb>,
    handlers: Vec<Arc<dyn TaskHandler>>,
    config: SchedulerConfig,
}

impl SchedulerBuilder {
    /// Create a new scheduler builder.
    pub fn new(db: Arc<AzothDb>) -> Self {
        Self {
            db,
            handlers: Vec::new(),
            config: SchedulerConfig::default(),
        }
    }

    /// Register a task handler.
    pub fn with_task_handler(mut self, handler: impl TaskHandler + 'static) -> Self {
        self.handlers.push(Arc::new(handler));
        self
    }

    /// Set the poll interval.
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.config = self.config.with_poll_interval(interval);
        self
    }

    /// Set the maximum concurrent tasks.
    pub fn with_max_concurrent_tasks(mut self, max: usize) -> Self {
        self.config = self.config.with_max_concurrent_tasks(max);
        self
    }

    /// Set the default maximum retries.
    pub fn with_default_max_retries(mut self, retries: u32) -> Self {
        self.config = self.config.with_default_max_retries(retries);
        self
    }

    /// Set the default timeout in seconds.
    pub fn with_default_timeout_secs(mut self, timeout: u64) -> Self {
        self.config = self.config.with_default_timeout_secs(timeout);
        self
    }

    /// Build the scheduler.
    ///
    /// Requires a SQLite connection for the projection store.
    #[allow(clippy::arc_with_non_send_sync)]
    pub fn build(self, projection_conn: Arc<Connection>) -> Result<Scheduler> {
        // Create projection
        let projection = Arc::new(ScheduleProjection::new(projection_conn));
        projection.init_schema()?;

        // Create handler registry
        let mut handler_registry = TaskHandlerRegistry::new();
        for handler in self.handlers {
            handler_registry.register(handler);
        }

        Ok(Scheduler {
            db: self.db,
            projection,
            handler_registry: Arc::new(handler_registry),
            config: self.config,
            shutdown: Arc::new(AtomicBool::new(false)),
            concurrent_tasks: Arc::new(AtomicUsize::new(0)),
        })
    }
}

/// Request to schedule a task.
#[derive(Debug, Clone)]
pub struct ScheduleTaskRequest {
    /// Unique identifier for the task.
    pub task_id: String,
    /// Type of task (used to look up handler).
    pub task_type: String,
    /// Schedule configuration.
    pub schedule: Schedule,
    /// Task payload (serialized task data).
    pub payload: Vec<u8>,
    /// Maximum number of retries on failure.
    pub max_retries: u32,
    /// Timeout in seconds.
    pub timeout_secs: u64,
}

impl ScheduleTaskRequest {
    /// Create a new request builder.
    pub fn builder(task_id: impl Into<String>) -> ScheduleTaskRequestBuilder {
        ScheduleTaskRequestBuilder {
            task_id: task_id.into(),
            task_type: None,
            schedule: None,
            payload: Vec::new(),
            max_retries: 3,
            timeout_secs: 300,
        }
    }
}

/// Builder for schedule task requests.
pub struct ScheduleTaskRequestBuilder {
    task_id: String,
    task_type: Option<String>,
    schedule: Option<Schedule>,
    payload: Vec<u8>,
    max_retries: u32,
    timeout_secs: u64,
}

impl ScheduleTaskRequestBuilder {
    /// Set the task type.
    pub fn task_type(mut self, task_type: impl Into<String>) -> Self {
        self.task_type = Some(task_type.into());
        self
    }

    /// Set a cron schedule.
    pub fn cron(mut self, expression: impl Into<String>) -> Self {
        self.schedule = Some(Schedule::Cron {
            expression: expression.into(),
        });
        self
    }

    /// Set an interval schedule.
    pub fn interval(mut self, seconds: u64) -> Self {
        self.schedule = Some(Schedule::Interval { seconds });
        self
    }

    /// Set a one-time schedule.
    pub fn one_time(mut self, run_at: i64) -> Self {
        self.schedule = Some(Schedule::OneTime { run_at });
        self
    }

    /// Set immediate execution.
    pub fn immediate(mut self) -> Self {
        self.schedule = Some(Schedule::Immediate);
        self
    }

    /// Set the schedule directly.
    pub fn schedule(mut self, schedule: Schedule) -> Self {
        self.schedule = Some(schedule);
        self
    }

    /// Set the payload.
    pub fn payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    /// Set the maximum retries.
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set the timeout in seconds.
    pub fn timeout_secs(mut self, timeout: u64) -> Self {
        self.timeout_secs = timeout;
        self
    }

    /// Build the request.
    pub fn build(self) -> Result<ScheduleTaskRequest> {
        let task_type = self
            .task_type
            .ok_or_else(|| SchedulerError::InvalidTask("task_type is required".into()))?;
        let schedule = self
            .schedule
            .ok_or_else(|| SchedulerError::InvalidTask("schedule is required".into()))?;

        Ok(ScheduleTaskRequest {
            task_id: self.task_id,
            task_type,
            schedule,
            payload: self.payload,
            max_retries: self.max_retries,
            timeout_secs: self.timeout_secs,
        })
    }
}
