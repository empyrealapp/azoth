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

    #[error("Other error: {0}")]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, AzothError>;

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
