use crate::types::event::EventId;
use serde::{Deserialize, Serialize};

/// Metadata about the canonical store state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CanonicalMeta {
    /// Next event ID to be assigned
    pub next_event_id: EventId,

    /// Sealed event ID (None if not sealed)
    pub sealed_event_id: Option<EventId>,

    /// Schema version of the canonical store
    pub schema_version: u32,

    /// Creation timestamp (ISO 8601)
    pub created_at: String,

    /// Last updated timestamp (ISO 8601)
    pub updated_at: String,
}

impl CanonicalMeta {
    pub fn new() -> Self {
        let now = chrono::Utc::now().to_rfc3339();
        Self {
            next_event_id: 0,
            sealed_event_id: None,
            schema_version: 1,
            created_at: now.clone(),
            updated_at: now,
        }
    }
}

impl Default for CanonicalMeta {
    fn default() -> Self {
        Self::new()
    }
}

/// Information about a backup operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupInfo {
    /// Sealed event ID at backup time
    pub sealed_event_id: EventId,

    /// Path where backup was written
    pub path: String,

    /// Backup timestamp (ISO 8601)
    pub timestamp: String,

    /// Size of backup in bytes
    pub size_bytes: u64,
}
