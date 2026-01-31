use crate::error::Result;
use crate::traits::projection::ProjectionTxn;
use crate::types::EventId;

/// Decoded event representation
///
/// Applications define their own event types and implement decoding
#[derive(Debug, Clone)]
pub struct DecodedEvent {
    pub id: EventId,
    pub event_type: String,
    pub payload: Vec<u8>,
}

/// Event decoder: convert raw bytes to structured events
pub trait EventDecoder: Send + Sync {
    /// Decode raw event bytes
    fn decode(&self, id: EventId, bytes: &[u8]) -> Result<DecodedEvent>;
}

/// Event applier: apply events to projection store
///
/// Note: Takes a mutable reference to the concrete transaction type,
/// not a trait object, to avoid object-safety issues
pub trait EventApplier<T: ProjectionTxn>: Send + Sync {
    /// Apply a decoded event to the projection
    fn apply(&self, txn: &mut T, event: &DecodedEvent) -> Result<()>;
}
