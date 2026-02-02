use thiserror::Error;

#[derive(Error, Debug)]
pub enum BusError {
    #[error("Azoth error: {0}")]
    Azoth(#[from] azoth_core::error::AzothError),

    #[error("Consumer not found: {0}")]
    ConsumerNotFound(String),

    #[error("Invalid consumer state: {0}")]
    InvalidState(String),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),
}

pub type Result<T> = std::result::Result<T, BusError>;
