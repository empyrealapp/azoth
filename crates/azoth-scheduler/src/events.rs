//! Event types for the scheduler.

use crate::schedule::Schedule;
use serde::{Deserialize, Serialize};

/// Events emitted by the scheduler.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SchedulerEvent {
    /// A task was scheduled.
    TaskScheduled {
        /// Unique identifier for the task.
        task_id: String,
        /// Type of task (used to look up handler).
        task_type: String,
        /// Schedule configuration.
        schedule: Schedule,
        /// Task payload (serialized task data).
        payload: Vec<u8>,
        /// Maximum number of retries on failure.
        max_retries: u32,
        /// Timeout in seconds.
        timeout_secs: u64,
    },
    /// A task was executed.
    TaskExecuted {
        /// Task identifier.
        task_id: String,
        /// Unique execution identifier.
        execution_id: String,
        /// Event ID of the event triggered by this execution (0 if failed).
        triggered_event_id: u64,
        /// When execution started (Unix timestamp).
        started_at: i64,
        /// When execution completed (Unix timestamp).
        completed_at: i64,
        /// Whether execution succeeded.
        success: bool,
        /// Error message if execution failed.
        error: Option<String>,
    },
    /// A task was cancelled.
    TaskCancelled {
        /// Task identifier.
        task_id: String,
        /// Reason for cancellation.
        reason: String,
    },
}

impl SchedulerEvent {
    /// Returns the task ID for this event.
    pub fn task_id(&self) -> &str {
        match self {
            Self::TaskScheduled { task_id, .. }
            | Self::TaskExecuted { task_id, .. }
            | Self::TaskCancelled { task_id, .. } => task_id,
        }
    }
}
