use std::io;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AzothError {
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Store is sealed at event {0}")]
    Sealed(u64),

    #[error("Store is paused, ingestion suspended")]
    Paused,

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Backup error: {0}")]
    Backup(String),

    #[error("Restore error: {0}")]
    Restore(String),

    #[error("Encryption error: {0}")]
    Encryption(String),

    #[error("Projection error: {0}")]
    Projection(String),

    #[error("Event decoding error: {0}")]
    EventDecode(String),

    #[error("Preflight validation error: {0}")]
    PreflightFailed(String),

    #[error("Lock error: {0}")]
    Lock(String),

    #[error("Not found: {0}")]
    NotFound(String),

    #[error("Invalid state: {0}")]
    InvalidState(String),

    #[error("Timeout: {0}")]
    Timeout(String),

    #[error(
        "Lock acquisition timed out after {timeout_ms}ms (possible deadlock or high contention)"
    )]
    LockTimeout { timeout_ms: u64 },

    #[error("Attempted to access undeclared key '{key}' - all keys must be declared via keys() before access")]
    UndeclaredKeyAccess { key: String },

    #[error("Circuit breaker is open, rejecting request")]
    CircuitBreakerOpen,

    #[error("Event log write failed after state commit (events saved to dead letter queue): {0}")]
    EventLogWriteFailed(String),

    #[error("Key too large ({size} bytes, max {max}): {context}")]
    KeyTooLarge {
        size: usize,
        max: usize,
        context: String,
    },

    #[error("Value too large ({size} bytes, max {max}): {context}")]
    ValueTooLarge {
        size: usize,
        max: usize,
        context: String,
    },

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, AzothError>;

impl AzothError {
    /// Wrap this error with additional context.
    ///
    /// The context string is prepended to the error message, producing a
    /// chain like `"during balance update: Transaction error: ..."`.
    ///
    /// # Example
    /// ```ignore
    /// db.write_txn()
    ///     .map_err(|e| e.context("during balance update"))?;
    /// ```
    pub fn context(self, msg: impl Into<String>) -> Self {
        let ctx = msg.into();
        AzothError::Internal(format!("{}: {}", ctx, self))
    }
}

/// Extension trait to add `.context()` on `Result<T, AzothError>`.
///
/// Mirrors the ergonomics of `anyhow::Context`.
pub trait ResultExt<T> {
    /// If the result is `Err`, wrap the error with additional context.
    fn context(self, msg: impl Into<String>) -> Result<T>;

    /// If the result is `Err`, wrap the error with a lazily-evaluated context.
    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T>;
}

impl<T> ResultExt<T> for Result<T> {
    fn context(self, msg: impl Into<String>) -> Result<T> {
        self.map_err(|e| e.context(msg))
    }

    fn with_context<F: FnOnce() -> String>(self, f: F) -> Result<T> {
        self.map_err(|e| e.context(f()))
    }
}

// Custom Error Types:
//
// Azoth supports custom error types through the `#[from] anyhow::Error` variant.
// Any error implementing `std::error::Error + Send + Sync + 'static` can be
// converted to `AzothError::Other`.
//
// For better control, implement `From<YourError> for AzothError` directly.
//
// Example:
//
// use thiserror::Error;
//
// #[derive(Error, Debug)]
// pub enum MyAppError {
//     #[error("Business logic error: {0}")]
//     BusinessLogic(String),
//
//     #[error("Authorization error: {0}")]
//     Unauthorized(String),
//
//     #[error(transparent)]
//     Azoth(#[from] AzothError),
// }
//
// impl From<MyAppError> for AzothError {
//     fn from(err: MyAppError) -> Self {
//         match err {
//             MyAppError::Azoth(e) => e,
//             other => AzothError::Other(other.into()),
//         }
//     }
// }
