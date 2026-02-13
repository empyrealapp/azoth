//! Schedule projection for task state management.

use crate::error::{Result, SchedulerError};
use crate::events::SchedulerEvent;
use crate::schedule::Schedule;
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, OptionalExtension};
use std::sync::Arc;

/// Projection that maintains task schedules in SQLite.
pub struct ScheduleProjection {
    conn: Arc<Connection>,
}

impl ScheduleProjection {
    /// Create a new schedule projection.
    pub fn new(conn: Arc<Connection>) -> Self {
        Self { conn }
    }

    /// Initialize the database schema.
    pub fn init_schema(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS scheduled_tasks (
                task_id TEXT PRIMARY KEY,
                task_type TEXT NOT NULL,
                schedule_type TEXT NOT NULL,
                schedule_data TEXT NOT NULL,
                payload BLOB NOT NULL,
                enabled INTEGER NOT NULL DEFAULT 1,
                next_run_time INTEGER NOT NULL,
                last_run_time INTEGER,
                last_execution_id TEXT,
                retry_count INTEGER NOT NULL DEFAULT 0,
                max_retries INTEGER NOT NULL DEFAULT 3,
                timeout_secs INTEGER NOT NULL DEFAULT 300,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_next_run_time
                ON scheduled_tasks(next_run_time, enabled);

            CREATE TABLE IF NOT EXISTS task_executions (
                execution_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                triggered_event_id INTEGER NOT NULL,
                started_at INTEGER NOT NULL,
                completed_at INTEGER,
                success INTEGER,
                error TEXT,
                retry_attempt INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY (task_id) REFERENCES scheduled_tasks(task_id)
            );

            CREATE INDEX IF NOT EXISTS idx_task_executions_task_id
                ON task_executions(task_id, started_at DESC);
            "#,
        )?;
        Ok(())
    }

    /// Get tasks that are due to run.
    pub fn get_due_tasks(&self, now: i64) -> Result<Vec<DueTask>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT task_id, task_type, payload, retry_count, max_retries, timeout_secs
            FROM scheduled_tasks
            WHERE enabled = 1 AND next_run_time <= ?
            ORDER BY next_run_time ASC
            "#,
        )?;

        let tasks = stmt
            .query_map(params![now], |row| {
                Ok(DueTask {
                    task_id: row.get(0)?,
                    task_type: row.get(1)?,
                    payload: row.get(2)?,
                    retry_count: row.get(3)?,
                    max_retries: row.get(4)?,
                    timeout_secs: row.get(5)?,
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(tasks)
    }

    /// Get the next wake time (earliest next_run_time).
    pub fn get_next_wake_time(&self) -> Result<Option<DateTime<Utc>>> {
        let next: Option<i64> = self
            .conn
            .query_row(
                r#"
                SELECT next_run_time
                FROM scheduled_tasks
                WHERE enabled = 1
                ORDER BY next_run_time ASC
                LIMIT 1
                "#,
                [],
                |row| row.get(0),
            )
            .optional()?;

        Ok(next.and_then(|ts| DateTime::from_timestamp(ts, 0)))
    }

    /// Get a task by ID.
    pub fn get_task(&self, task_id: &str) -> Result<Option<ScheduledTask>> {
        let task: Option<ScheduledTask> = self
            .conn
            .query_row(
                r#"
                SELECT task_id, task_type, schedule_type, schedule_data, payload,
                       enabled, next_run_time, last_run_time, last_execution_id,
                       retry_count, max_retries, timeout_secs, created_at, updated_at
                FROM scheduled_tasks
                WHERE task_id = ?
                "#,
                params![task_id],
                |row| {
                    Ok(ScheduledTask {
                        task_id: row.get(0)?,
                        task_type: row.get(1)?,
                        schedule: serde_json::from_str(&row.get::<_, String>(3)?).unwrap(),
                        payload: row.get(4)?,
                        enabled: row.get(5)?,
                        next_run_time: row
                            .get::<_, Option<i64>>(6)?
                            .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                        last_run_time: row
                            .get::<_, Option<i64>>(7)?
                            .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                        last_execution_id: row.get(8)?,
                        retry_count: row.get(9)?,
                        max_retries: row.get(10)?,
                        timeout_secs: row.get(11)?,
                        created_at: DateTime::from_timestamp(row.get(12)?, 0).unwrap(),
                        updated_at: DateTime::from_timestamp(row.get(13)?, 0).unwrap(),
                    })
                },
            )
            .optional()?;

        Ok(task)
    }

    /// List all tasks with optional filtering.
    ///
    /// All filter values are passed as bound parameters to prevent SQL injection.
    pub fn list_tasks(&self, filter: &TaskFilter) -> Result<Vec<ScheduledTask>> {
        let mut query = String::from(
            r#"
            SELECT task_id, task_type, schedule_type, schedule_data, payload,
                   enabled, next_run_time, last_run_time, last_execution_id,
                   retry_count, max_retries, timeout_secs, created_at, updated_at
            FROM scheduled_tasks
            WHERE 1=1
            "#,
        );

        // Collect bound parameters to prevent SQL injection
        let mut bound_params: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();

        if let Some(enabled) = filter.enabled {
            query.push_str(" AND enabled = ?");
            bound_params.push(Box::new(if enabled { 1_i32 } else { 0_i32 }));
        }

        if let Some(task_type) = &filter.task_type {
            query.push_str(" AND task_type = ?");
            bound_params.push(Box::new(task_type.clone()));
        }

        query.push_str(" ORDER BY created_at DESC");

        let mut stmt = self.conn.prepare(&query)?;
        let tasks = stmt
            .query_map(rusqlite::params_from_iter(bound_params.iter()), |row| {
                Ok(ScheduledTask {
                    task_id: row.get(0)?,
                    task_type: row.get(1)?,
                    schedule: serde_json::from_str(&row.get::<_, String>(3)?).unwrap(),
                    payload: row.get(4)?,
                    enabled: row.get(5)?,
                    next_run_time: row
                        .get::<_, Option<i64>>(6)?
                        .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                    last_run_time: row
                        .get::<_, Option<i64>>(7)?
                        .and_then(|ts| DateTime::from_timestamp(ts, 0)),
                    last_execution_id: row.get(8)?,
                    retry_count: row.get(9)?,
                    max_retries: row.get(10)?,
                    timeout_secs: row.get(11)?,
                    created_at: DateTime::from_timestamp(row.get(12)?, 0).unwrap(),
                    updated_at: DateTime::from_timestamp(row.get(13)?, 0).unwrap(),
                })
            })?
            .collect::<std::result::Result<Vec<_>, _>>()?;

        Ok(tasks)
    }

    /// Apply an event to the projection.
    pub fn apply_event(&self, event: &SchedulerEvent) -> Result<()> {
        match event {
            SchedulerEvent::TaskScheduled {
                task_id,
                task_type,
                schedule,
                payload,
                max_retries,
                timeout_secs,
            } => {
                let now = Utc::now();
                let next_run_time = schedule.next_run_time(now)?;

                let schedule_type = match schedule {
                    Schedule::Cron { .. } => "cron",
                    Schedule::Interval { .. } => "interval",
                    Schedule::OneTime { .. } => "one_time",
                    Schedule::Immediate => "immediate",
                };

                let schedule_data = serde_json::to_string(schedule)?;

                self.conn.execute(
                    r#"
                    INSERT INTO scheduled_tasks
                    (task_id, task_type, schedule_type, schedule_data, payload,
                     enabled, next_run_time, retry_count, max_retries, timeout_secs,
                     created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, 1, ?, 0, ?, ?, ?, ?)
                    ON CONFLICT(task_id) DO UPDATE SET
                        task_type = excluded.task_type,
                        schedule_type = excluded.schedule_type,
                        schedule_data = excluded.schedule_data,
                        payload = excluded.payload,
                        enabled = 1,
                        next_run_time = excluded.next_run_time,
                        retry_count = 0,
                        max_retries = excluded.max_retries,
                        timeout_secs = excluded.timeout_secs,
                        updated_at = excluded.updated_at
                    "#,
                    params![
                        task_id,
                        task_type,
                        schedule_type,
                        schedule_data,
                        payload,
                        next_run_time.map(|t| t.timestamp()),
                        max_retries,
                        timeout_secs,
                        now.timestamp(),
                        now.timestamp(),
                    ],
                )?;
            }
            SchedulerEvent::TaskExecuted {
                task_id,
                execution_id,
                triggered_event_id,
                started_at,
                completed_at,
                success,
                error,
            } => {
                // Get the current task to determine next run time
                let task = self
                    .get_task(task_id)?
                    .ok_or_else(|| SchedulerError::TaskNotFound(task_id.clone()))?;

                let retry_attempt = task.retry_count;

                // Record execution
                self.conn.execute(
                    r#"
                    INSERT INTO task_executions
                    (execution_id, task_id, triggered_event_id, started_at,
                     completed_at, success, error, retry_attempt)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    "#,
                    params![
                        execution_id,
                        task_id,
                        triggered_event_id,
                        started_at,
                        completed_at,
                        if *success { 1 } else { 0 },
                        error,
                        retry_attempt,
                    ],
                )?;

                // Update task state
                if *success {
                    // Calculate next run time for recurring tasks
                    let next_run_time = if task.schedule.is_recurring() {
                        let last_run =
                            DateTime::from_timestamp(*started_at, 0).ok_or_else(|| {
                                SchedulerError::InvalidSchedule("Invalid timestamp".into())
                            })?;
                        task.schedule.next_run_time(last_run)?
                    } else {
                        None
                    };

                    self.conn.execute(
                        r#"
                        UPDATE scheduled_tasks
                        SET last_run_time = ?,
                            last_execution_id = ?,
                            next_run_time = ?,
                            retry_count = 0,
                            updated_at = ?
                        WHERE task_id = ?
                        "#,
                        params![
                            started_at,
                            execution_id,
                            next_run_time.map(|t| t.timestamp()),
                            Utc::now().timestamp(),
                            task_id,
                        ],
                    )?;
                } else {
                    // Increment retry count or disable if max retries reached
                    let new_retry_count = task.retry_count + 1;
                    let enabled = if new_retry_count >= task.max_retries {
                        0
                    } else {
                        1
                    };

                    // Calculate next retry time (exponential backoff)
                    let next_run_time = if enabled == 1 {
                        let backoff_seconds = 2_i64.pow(new_retry_count) * 60; // 1min, 2min, 4min, etc.
                        Some(Utc::now() + chrono::Duration::seconds(backoff_seconds))
                    } else {
                        None
                    };

                    self.conn.execute(
                        r#"
                        UPDATE scheduled_tasks
                        SET retry_count = ?,
                            enabled = ?,
                            next_run_time = ?,
                            updated_at = ?
                        WHERE task_id = ?
                        "#,
                        params![
                            new_retry_count,
                            enabled,
                            next_run_time.map(|t| t.timestamp()),
                            Utc::now().timestamp(),
                            task_id,
                        ],
                    )?;
                }
            }
            SchedulerEvent::TaskCancelled { task_id, .. } => {
                self.conn.execute(
                    "DELETE FROM scheduled_tasks WHERE task_id = ?",
                    params![task_id],
                )?;
            }
        }

        Ok(())
    }
}

/// A task that is due to run.
#[derive(Debug, Clone)]
pub struct DueTask {
    /// Task identifier.
    pub task_id: String,
    /// Task type.
    pub task_type: String,
    /// Task payload.
    pub payload: Vec<u8>,
    /// Current retry count.
    pub retry_count: u32,
    /// Maximum retries.
    pub max_retries: u32,
    /// Timeout in seconds.
    pub timeout_secs: u64,
}

/// A scheduled task.
#[derive(Debug, Clone)]
pub struct ScheduledTask {
    /// Task identifier.
    pub task_id: String,
    /// Task type.
    pub task_type: String,
    /// Schedule configuration.
    pub schedule: Schedule,
    /// Task payload.
    pub payload: Vec<u8>,
    /// Whether the task is enabled.
    pub enabled: bool,
    /// Next scheduled run time.
    pub next_run_time: Option<DateTime<Utc>>,
    /// Last run time.
    pub last_run_time: Option<DateTime<Utc>>,
    /// Last execution ID.
    pub last_execution_id: Option<String>,
    /// Current retry count.
    pub retry_count: u32,
    /// Maximum retries.
    pub max_retries: u32,
    /// Timeout in seconds.
    pub timeout_secs: u64,
    /// When the task was created.
    pub created_at: DateTime<Utc>,
    /// When the task was last updated.
    pub updated_at: DateTime<Utc>,
}

/// Filter for listing tasks.
#[derive(Debug, Clone, Default)]
pub struct TaskFilter {
    /// Filter by enabled status.
    pub enabled: Option<bool>,
    /// Filter by task type.
    pub task_type: Option<String>,
}

impl TaskFilter {
    /// Create a new empty filter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Filter by enabled status.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }

    /// Filter by task type.
    pub fn task_type(mut self, task_type: impl Into<String>) -> Self {
        self.task_type = Some(task_type.into());
        self
    }
}
