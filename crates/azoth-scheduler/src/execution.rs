//! Task execution logic.

use crate::error::Result;
use crate::events::SchedulerEvent;
use crate::projection::DueTask;
use crate::task_handler::{TaskContext, TaskHandlerRegistry};
use azoth::AzothDb;
use azoth_core::traits::{CanonicalStore, CanonicalTxn};
use chrono::Utc;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Execute a single task.
///
/// This function spawns a blocking task to execute the handler,
/// then writes the result event to the canonical store.
pub async fn execute_task(
    db: Arc<AzothDb>,
    handler_registry: Arc<TaskHandlerRegistry>,
    due_task: DueTask,
) -> Result<()> {
    let execution_id = uuid::Uuid::new_v4().to_string();
    let started_at = Utc::now().timestamp();

    debug!(
        task_id = %due_task.task_id,
        execution_id = %execution_id,
        "Starting task execution"
    );

    // Get handler
    let handler = handler_registry.get(&due_task.task_type)?;

    // Create context
    let ctx = TaskContext {
        task_id: due_task.task_id.clone(),
        execution_id: execution_id.clone(),
        scheduled_at: Utc::now(),
        execution_attempt: due_task.retry_count,
    };

    // Clone what we need for the blocking task
    let task_id = due_task.task_id.clone();
    let payload = due_task.payload.clone();
    let timeout_secs = due_task.timeout_secs;

    // Execute with timeout in a blocking task
    let result = tokio::time::timeout(
        Duration::from_secs(timeout_secs),
        tokio::task::spawn_blocking(move || handler.execute(&ctx, &payload)),
    )
    .await;

    let completed_at = Utc::now().timestamp();

    // Process result and write events
    match result {
        Ok(Ok(Ok(task_event))) => {
            // Success - write both the task event and execution event
            info!(
                task_id = %task_id,
                execution_id = %execution_id,
                event_type = %task_event.event_type,
                "Task executed successfully"
            );

            // Write task event to canonical store
            let mut txn = db.canonical().write_txn()?;
            let event_id = txn.append_event(&task_event.payload)?;

            // Write TaskExecuted event
            let executed_event = SchedulerEvent::TaskExecuted {
                task_id,
                execution_id,
                triggered_event_id: event_id,
                started_at,
                completed_at,
                success: true,
                error: None,
            };
            txn.append_event(&serde_json::to_vec(&executed_event)?)?;
            txn.commit()?;

            Ok(())
        }
        Ok(Ok(Err(handler_error))) => {
            // Handler returned an error
            error!(
                task_id = %task_id,
                execution_id = %execution_id,
                error = %handler_error,
                "Task handler returned error"
            );

            let executed_event = SchedulerEvent::TaskExecuted {
                task_id,
                execution_id,
                triggered_event_id: 0,
                started_at,
                completed_at,
                success: false,
                error: Some(handler_error.to_string()),
            };

            let mut txn = db.canonical().write_txn()?;
            txn.append_event(&serde_json::to_vec(&executed_event)?)?;
            txn.commit()?;

            Ok(())
        }
        Ok(Err(join_error)) => {
            // Task panicked
            error!(
                task_id = %task_id,
                execution_id = %execution_id,
                error = %join_error,
                "Task handler panicked"
            );

            let executed_event = SchedulerEvent::TaskExecuted {
                task_id,
                execution_id,
                triggered_event_id: 0,
                started_at,
                completed_at,
                success: false,
                error: Some(format!("Task panicked: {}", join_error)),
            };

            let mut txn = db.canonical().write_txn()?;
            txn.append_event(&serde_json::to_vec(&executed_event)?)?;
            txn.commit()?;

            Ok(())
        }
        Err(_timeout) => {
            // Execution timed out
            error!(
                task_id = %task_id,
                execution_id = %execution_id,
                timeout_secs = timeout_secs,
                "Task execution timed out"
            );

            let executed_event = SchedulerEvent::TaskExecuted {
                task_id,
                execution_id,
                triggered_event_id: 0,
                started_at,
                completed_at,
                success: false,
                error: Some(format!("Task timed out after {} seconds", timeout_secs)),
            };

            let mut txn = db.canonical().write_txn()?;
            txn.append_event(&serde_json::to_vec(&executed_event)?)?;
            txn.commit()?;

            Ok(())
        }
    }
}
