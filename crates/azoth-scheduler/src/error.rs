//! Error types for the scheduler.

use thiserror::Error;

/// Result type for scheduler operations.
pub type Result<T> = std::result::Result<T, SchedulerError>;

/// Errors that can occur in the scheduler.
#[derive(Debug, Error)]
pub enum SchedulerError {
    /// Database error.
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),

    /// Azoth core error.
    #[error("Azoth error: {0}")]
    Azoth(#[from] azoth_core::error::AzothError),

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Invalid schedule.
    #[error("Invalid schedule: {0}")]
    InvalidSchedule(String),

    /// Task handler not found.
    #[error("Task handler not found: {0}")]
    HandlerNotFound(String),

    /// Task handler error.
    #[error("Task handler error: {0}")]
    HandlerError(String),

    /// Task execution timeout.
    #[error("Task execution timeout")]
    Timeout,

    /// Task not found.
    #[error("Task not found: {0}")]
    TaskNotFound(String),

    /// Invalid task configuration.
    #[error("Invalid task configuration: {0}")]
    InvalidTask(String),

    /// Cron parsing error.
    #[error("Cron parsing error: {0}")]
    CronParse(#[from] cron::error::Error),

    /// Other error.
    #[error("{0}")]
    Other(String),
}
