//! Event format with structured payloads
//!
//! Events consist of:
//! - **Name**: Event type identifier (e.g., "withdraw", "deposit")
//! - **Payload**: Structured data (JSON or MessagePack)
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::event_format::{Event, EventCodec, JsonCodec};
//! use serde::{Serialize, Deserialize};
//!
//! # fn main() -> Result<()> {
//! #[derive(Serialize, Deserialize)]
//! struct WithdrawPayload {
//!     amount: i64,
//!     account_id: u64,
//! }
//!
//! let codec = JsonCodec;
//! let event = Event::new("withdraw", WithdrawPayload {
//!     amount: 50,
//!     account_id: 1,
//! });
//!
//! // Encode to bytes
//! let bytes = codec.encode(&event)?;
//!
//! // Decode from bytes
//! let decoded: Event<WithdrawPayload> = codec.decode(&bytes)?;
//! # Ok(())
//! # }
//! ```

use crate::{AzothError, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// Structured event with name and typed payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event<T> {
    /// Event name/type (e.g., "deposit", "withdraw")
    pub name: String,

    /// Structured payload
    pub payload: T,

    /// Optional metadata
    #[serde(default)]
    pub metadata: EventMetadata,
}

/// Event metadata
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventMetadata {
    /// Timestamp (ISO 8601)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timestamp: Option<String>,

    /// Correlation ID for tracing
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    /// Custom fields
    #[serde(flatten)]
    pub custom: std::collections::HashMap<String, serde_json::Value>,
}

impl<T> Event<T> {
    /// Create a new event with the given name and payload
    pub fn new(name: impl Into<String>, payload: T) -> Self {
        Self {
            name: name.into(),
            payload,
            metadata: EventMetadata::default(),
        }
    }

    /// Add a timestamp
    pub fn with_timestamp(mut self) -> Self {
        self.metadata.timestamp = Some(chrono::Utc::now().to_rfc3339());
        self
    }

    /// Add a correlation ID
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.metadata.correlation_id = Some(id.into());
        self
    }

    /// Add custom metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.custom.insert(key.into(), value);
        self
    }
}

/// Event codec trait for encoding/decoding events
pub trait EventCodec: Send + Sync {
    /// Encode an event to bytes
    fn encode<T: Serialize>(&self, event: &Event<T>) -> Result<Vec<u8>>;

    /// Decode an event from bytes
    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<Event<T>>;

    /// Get the codec name (for manifest)
    fn name(&self) -> &str;
}

/// JSON codec (human-readable, larger size)
pub struct JsonCodec;

impl EventCodec for JsonCodec {
    fn encode<T: Serialize>(&self, event: &Event<T>) -> Result<Vec<u8>> {
        serde_json::to_vec(event).map_err(|e| AzothError::Serialization(e.to_string()))
    }

    fn decode<T: DeserializeOwned>(&self, bytes: &[u8]) -> Result<Event<T>> {
        serde_json::from_slice(bytes).map_err(|e| AzothError::Serialization(e.to_string()))
    }

    fn name(&self) -> &str {
        "json"
    }
}

/// MessagePack codec (binary, compact)
///
/// Note: Requires `rmp-serde` feature (not yet added)
pub struct MsgPackCodec;

impl EventCodec for MsgPackCodec {
    fn encode<T: Serialize>(&self, _event: &Event<T>) -> Result<Vec<u8>> {
        // TODO: Implement with rmp-serde
        // rmp_serde::to_vec(event)
        //     .map_err(|e| AzothError::Serialization(e.to_string()))
        Err(AzothError::InvalidState(
            "MessagePack codec not yet implemented - add rmp-serde dependency".into(),
        ))
    }

    fn decode<T: DeserializeOwned>(&self, _bytes: &[u8]) -> Result<Event<T>> {
        // TODO: Implement with rmp-serde
        // rmp_serde::from_slice(bytes)
        //     .map_err(|e| AzothError::Serialization(e.to_string()))
        Err(AzothError::InvalidState(
            "MessagePack codec not yet implemented - add rmp-serde dependency".into(),
        ))
    }

    fn name(&self) -> &str {
        "msgpack"
    }
}

/// Helper to encode an event with the default codec (JSON)
pub fn encode<T: Serialize>(name: &str, payload: &T) -> Result<Vec<u8>> {
    let event = Event::new(name, payload);
    JsonCodec.encode(&event)
}

/// Helper to decode an event with the default codec (JSON)
pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<Event<T>> {
    JsonCodec.decode(bytes)
}

/// Event type registry for dynamic dispatch
///
/// Maps event names to handler functions
#[allow(clippy::type_complexity)]
pub struct EventTypeRegistry {
    handlers: std::collections::HashMap<
        String,
        Box<dyn Fn(&serde_json::Value) -> Result<()> + Send + Sync>,
    >,
}

impl EventTypeRegistry {
    pub fn new() -> Self {
        Self {
            handlers: std::collections::HashMap::new(),
        }
    }

    /// Register a handler for an event type
    pub fn register<T, F>(&mut self, event_name: impl Into<String>, handler: F)
    where
        T: DeserializeOwned,
        F: Fn(&T) -> Result<()> + Send + Sync + 'static,
    {
        let handler = Box::new(move |value: &serde_json::Value| {
            let payload: T = serde_json::from_value(value.clone())
                .map_err(|e| AzothError::EventDecode(e.to_string()))?;
            handler(&payload)
        });

        self.handlers.insert(event_name.into(), handler);
    }

    /// Process an event by name
    pub fn process(&self, name: &str, payload: &serde_json::Value) -> Result<()> {
        let handler = self
            .handlers
            .get(name)
            .ok_or_else(|| AzothError::EventDecode(format!("No handler for event '{}'", name)))?;

        handler(payload)
    }
}

impl Default for EventTypeRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    struct TestPayload {
        amount: i64,
        account: String,
    }

    #[test]
    fn test_event_creation() {
        let event = Event::new(
            "deposit",
            TestPayload {
                amount: 100,
                account: "alice".to_string(),
            },
        )
        .with_timestamp()
        .with_correlation_id("req-123");

        assert_eq!(event.name, "deposit");
        assert_eq!(event.payload.amount, 100);
        assert!(event.metadata.timestamp.is_some());
        assert_eq!(event.metadata.correlation_id, Some("req-123".to_string()));
    }

    #[test]
    fn test_json_codec() {
        let codec = JsonCodec;
        let event = Event::new(
            "withdraw",
            TestPayload {
                amount: 50,
                account: "bob".to_string(),
            },
        );

        // Encode
        let bytes = codec.encode(&event).unwrap();

        // Decode
        let decoded: Event<TestPayload> = codec.decode(&bytes).unwrap();

        assert_eq!(decoded.name, "withdraw");
        assert_eq!(decoded.payload.amount, 50);
        assert_eq!(decoded.payload.account, "bob");
    }

    #[test]
    fn test_helper_functions() {
        let payload = TestPayload {
            amount: 75,
            account: "charlie".to_string(),
        };

        // Encode
        let bytes = encode("transfer", &payload).unwrap();

        // Decode
        let decoded: Event<TestPayload> = decode(&bytes).unwrap();

        assert_eq!(decoded.name, "transfer");
        assert_eq!(decoded.payload.amount, 75);
    }

    #[test]
    fn test_event_type_registry() {
        let mut registry = EventTypeRegistry::new();

        // Register handler
        registry.register::<TestPayload, _>("deposit", |payload| {
            assert_eq!(payload.amount, 100);
            Ok(())
        });

        // Process event
        let payload_json = serde_json::json!({
            "amount": 100,
            "account": "alice"
        });

        registry.process("deposit", &payload_json).unwrap();
    }

    #[test]
    fn test_unknown_event_type() {
        let registry = EventTypeRegistry::new();

        let result = registry.process("unknown", &serde_json::json!({}));
        assert!(result.is_err());
    }
}
