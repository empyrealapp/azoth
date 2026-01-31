use crate::types::event::EventId;
use serde::{Deserialize, Serialize};

/// Backup manifest for deterministic restore
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupManifest {
    /// Sealed event ID at backup time
    pub sealed_event_id: EventId,

    /// Canonical backend type (e.g., "lmdb")
    pub canonical_backend: String,

    /// Canonical backend version
    pub canonical_version: String,

    /// Projection backend type (e.g., "sqlite")
    pub projection_backend: String,

    /// Projection cursor at backup time
    pub projection_cursor: EventId,

    /// Canonical schema version
    pub canonical_schema_version: u32,

    /// Projection schema version
    pub projection_schema_version: u32,

    /// Backup timestamp (ISO 8601)
    pub timestamp: String,
}

impl BackupManifest {
    pub fn new(
        sealed_event_id: EventId,
        canonical_backend: String,
        projection_backend: String,
        projection_cursor: EventId,
        canonical_schema_version: u32,
        projection_schema_version: u32,
    ) -> Self {
        Self {
            sealed_event_id,
            canonical_backend,
            canonical_version: env!("CARGO_PKG_VERSION").to_string(),
            projection_backend,
            projection_cursor,
            canonical_schema_version,
            projection_schema_version,
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }
}
