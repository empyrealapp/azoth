//! Task handler trait and registry.

use crate::error::{Result, SchedulerError};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;

/// Context provided to task handlers during execution.
#[derive(Debug, Clone)]
pub struct TaskContext {
    /// Unique identifier for the task.
    pub task_id: String,
    /// Unique identifier for this execution.
    pub execution_id: String,
    /// When this execution was scheduled to run.
    pub scheduled_at: DateTime<Utc>,
    /// Number of attempts (0 for first attempt).
    pub execution_attempt: u32,
}

/// Event produced by a task handler.
#[derive(Debug, Clone)]
pub struct TaskEvent {
    /// Type of event (used to route to event handlers).
    pub event_type: String,
    /// Serialized event payload.
    pub payload: Vec<u8>,
}

/// Trait implemented by task handlers.
///
/// Task handlers execute scheduled tasks and produce events
/// that are written to the canonical event log.
pub trait TaskHandler: Send + Sync {
    /// Returns the task type this handler processes.
    fn task_type(&self) -> &str;

    /// Execute the task with the given context and payload.
    ///
    /// Returns a `TaskEvent` that will be written to the canonical log.
    /// The event will then be processed by registered event handlers.
    fn execute(&self, ctx: &TaskContext, payload: &[u8]) -> Result<TaskEvent>;

    /// Validate the task payload (optional).
    ///
    /// Called when scheduling a task to ensure the payload is valid.
    /// Default implementation always succeeds.
    fn validate(&self, _payload: &[u8]) -> Result<()> {
        Ok(())
    }
}

/// Registry of task handlers.
#[derive(Clone)]
pub struct TaskHandlerRegistry {
    handlers: Arc<HashMap<String, Arc<dyn TaskHandler>>>,
}

impl TaskHandlerRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(HashMap::new()),
        }
    }

    /// Register a task handler.
    pub fn register(&mut self, handler: Arc<dyn TaskHandler>) {
        let task_type = handler.task_type().to_string();
        Arc::make_mut(&mut self.handlers).insert(task_type, handler);
    }

    /// Get a handler by task type.
    pub fn get(&self, task_type: &str) -> Result<Arc<dyn TaskHandler>> {
        self.handlers
            .get(task_type)
            .cloned()
            .ok_or_else(|| SchedulerError::HandlerNotFound(task_type.to_string()))
    }

    /// Check if a handler is registered for a task type.
    pub fn has(&self, task_type: &str) -> bool {
        self.handlers.contains_key(task_type)
    }

    /// Get all registered task types.
    pub fn task_types(&self) -> Vec<String> {
        self.handlers.keys().cloned().collect()
    }
}

impl Default for TaskHandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
