use serde::{Deserialize, Serialize};

/// Event identifier - strictly monotonic u64
pub type EventId = u64;

/// Raw event bytes (opaque to storage layer)
pub type EventBytes = Vec<u8>;

/// Information about a committed transaction
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommitInfo {
    /// Number of events written in this commit
    pub events_written: usize,

    /// First event ID in this commit (if any)
    pub first_event_id: Option<EventId>,

    /// Last event ID in this commit (if any)
    pub last_event_id: Option<EventId>,

    /// Number of state keys written
    pub state_keys_written: usize,

    /// Number of state keys deleted
    pub state_keys_deleted: usize,
}

impl CommitInfo {
    pub fn empty() -> Self {
        Self {
            events_written: 0,
            first_event_id: None,
            last_event_id: None,
            state_keys_written: 0,
            state_keys_deleted: 0,
        }
    }
}
