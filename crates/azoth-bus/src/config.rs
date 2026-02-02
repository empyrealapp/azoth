use serde::{Deserialize, Serialize};

/// Configuration for a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Stream name
    pub name: String,

    /// Optional retention policy
    pub retention: Option<RetentionPolicy>,
}

/// Retention policy for events in a stream
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionPolicy {
    /// Keep all events (no cleanup)
    KeepAll,

    /// Keep events for N days
    KeepDays(u64),

    /// Keep last N events
    KeepCount(u64),
}

/// Metadata stored for each consumer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerMetadata {
    /// Consumer name
    pub name: String,

    /// Stream name
    pub stream: String,

    /// When consumer was created
    pub created_at: chrono::DateTime<chrono::Utc>,

    /// Last acknowledgment timestamp
    pub last_ack_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl ConsumerMetadata {
    pub fn new(stream: String, name: String) -> Self {
        Self {
            name,
            stream,
            created_at: chrono::Utc::now(),
            last_ack_at: None,
        }
    }
}
