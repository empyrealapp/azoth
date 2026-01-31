//! Event handler system
//!
//! Provides a registry for event handlers that process events and update projections.
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::{EventHandler, EventHandlerRegistry};
//! use rusqlite::Connection;
//!
//! // Define an event handler
//! struct DepositHandler;
//!
//! impl EventHandler for DepositHandler {
//!     fn event_type(&self) -> &str {
//!         "deposit"
//!     }
//!
//!     fn handle(&self, conn: &Connection, event_id: EventId, payload: &[u8]) -> Result<()> {
//!         // Parse payload
//!         let amount: i64 = String::from_utf8_lossy(payload).parse()
//!             .map_err(|e: std::num::ParseIntError| AzothError::EventDecode(e.to_string()))?;
//!
//!         // Update projection
//!         conn.execute(
//!             "UPDATE accounts SET balance = balance + ?1 WHERE id = 1",
//!             [amount],
//!         ).map_err(|e| AzothError::Projection(e.to_string()))?;
//!
//!         Ok(())
//!     }
//! }
//!
//! // Register handler
//! let mut registry = EventHandlerRegistry::new();
//! registry.register(Box::new(DepositHandler));
//! ```

use crate::{AzothError, EventId, Result};
use rusqlite::Connection;
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Represents a single event in a batch
#[derive(Clone)]
pub struct BatchEvent {
    pub event_id: EventId,
    pub payload: Vec<u8>,
}

/// Batch processing configuration
#[derive(Clone, Debug)]
pub struct BatchConfig {
    /// Maximum number of events per batch
    pub max_batch_size: usize,
    /// Maximum time to wait before processing a partial batch
    pub max_batch_duration: Duration,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 1000,
            max_batch_duration: Duration::from_millis(100),
        }
    }
}

/// Event handler trait
///
/// Implement this to define how events are processed and applied to projections.
/// For high-performance applications, implement `handle_batch` instead of `handle`.
pub trait EventHandler: Send + Sync {
    /// The event type this handler processes (e.g., "deposit", "withdraw")
    fn event_type(&self) -> &str;

    /// Handle a single event and update the projection
    ///
    /// # Arguments
    /// - `conn`: SQLite connection to the projection database
    /// - `event_id`: The ID of the event being processed
    /// - `payload`: Raw event payload bytes
    ///
    /// Note: For better performance, implement `handle_batch` instead.
    fn handle(&self, conn: &Connection, event_id: EventId, payload: &[u8]) -> Result<()>;

    /// Handle a batch of events (optional, high-performance)
    ///
    /// If implemented, this will be used instead of calling `handle` repeatedly.
    /// Process all events in a single database transaction for better performance.
    ///
    /// # Arguments
    /// - `conn`: SQLite connection to the projection database
    /// - `events`: Batch of events to process
    ///
    /// Default implementation: calls `handle` for each event individually.
    fn handle_batch(&self, conn: &Connection, events: &[BatchEvent]) -> Result<()> {
        // Default: process one at a time
        for event in events {
            self.handle(conn, event.event_id, &event.payload)?;
        }
        Ok(())
    }

    /// Optional: validate event before processing
    ///
    /// Called during preflight phase. Return an error if the event is invalid.
    fn validate(&self, _payload: &[u8]) -> Result<()> {
        Ok(())
    }
}

/// Event handler registry
///
/// Manages a collection of event handlers and routes events to the appropriate handler.
/// Supports batch processing for improved performance.
pub struct EventHandlerRegistry {
    handlers: HashMap<String, Box<dyn EventHandler>>,
    batch_config: BatchConfig,
}

impl EventHandlerRegistry {
    /// Create a new empty registry with default batch configuration
    pub fn new() -> Self {
        Self {
            handlers: HashMap::new(),
            batch_config: BatchConfig::default(),
        }
    }

    /// Create a new registry with custom batch configuration
    pub fn with_batch_config(batch_config: BatchConfig) -> Self {
        Self {
            handlers: HashMap::new(),
            batch_config,
        }
    }

    /// Get the batch configuration
    pub fn batch_config(&self) -> &BatchConfig {
        &self.batch_config
    }

    /// Set the batch configuration
    pub fn set_batch_config(&mut self, config: BatchConfig) {
        self.batch_config = config;
    }

    /// Register an event handler
    ///
    /// Panics if a handler for the same event type is already registered.
    pub fn register(&mut self, handler: Box<dyn EventHandler>) {
        let event_type = handler.event_type().to_string();
        if self.handlers.contains_key(&event_type) {
            panic!("Handler for event type '{}' already registered", event_type);
        }
        self.handlers.insert(event_type, handler);
    }

    /// Try to register a handler, returning error if already registered
    pub fn try_register(&mut self, handler: Box<dyn EventHandler>) -> Result<()> {
        let event_type = handler.event_type().to_string();
        if self.handlers.contains_key(&event_type) {
            return Err(AzothError::InvalidState(format!(
                "Handler for event type '{}' already registered",
                event_type
            )));
        }
        self.handlers.insert(event_type, handler);
        Ok(())
    }

    /// Get a handler by event type
    pub fn get(&self, event_type: &str) -> Option<&dyn EventHandler> {
        self.handlers.get(event_type).map(|h| h.as_ref())
    }

    /// Process an event using the appropriate handler
    ///
    /// Event format: `event_type:payload`
    /// Example: `deposit:100`, `withdraw:50`
    pub fn process(&self, conn: &Connection, event_id: EventId, event_bytes: &[u8]) -> Result<()> {
        // Parse event type from bytes
        let event_str = std::str::from_utf8(event_bytes)
            .map_err(|e| AzothError::EventDecode(format!("Invalid UTF-8: {}", e)))?;

        let (event_type, payload) = event_str.split_once(':').ok_or_else(|| {
            AzothError::EventDecode("Event must be in format 'type:payload'".into())
        })?;

        // Get handler
        let handler = self.get(event_type).ok_or_else(|| {
            AzothError::EventDecode(format!("No handler for event type '{}'", event_type))
        })?;

        // Process event
        handler.handle(conn, event_id, payload.as_bytes())
    }

    /// Validate an event (preflight check)
    pub fn validate(&self, event_bytes: &[u8]) -> Result<()> {
        let event_str = std::str::from_utf8(event_bytes)
            .map_err(|e| AzothError::EventDecode(format!("Invalid UTF-8: {}", e)))?;

        let (event_type, payload) = event_str.split_once(':').ok_or_else(|| {
            AzothError::EventDecode("Event must be in format 'type:payload'".into())
        })?;

        let handler = self.get(event_type).ok_or_else(|| {
            AzothError::EventDecode(format!("No handler for event type '{}'", event_type))
        })?;

        handler.validate(payload.as_bytes())
    }

    /// Process multiple events in batches for improved performance
    ///
    /// Groups events by type and processes each group in batches according to the batch configuration.
    /// Batches are flushed when either:
    /// - The batch size limit is reached
    /// - The batch duration limit is exceeded
    ///
    /// # Arguments
    /// - `conn`: SQLite connection to use for all handlers
    /// - `events`: Iterator of (event_id, event_bytes) tuples
    ///
    /// # Returns
    /// Number of events successfully processed
    pub fn process_batched<'a, I>(&self, conn: &Connection, events: I) -> Result<usize>
    where
        I: IntoIterator<Item = (EventId, &'a [u8])>,
    {
        // Group events by type
        let mut event_groups: HashMap<String, Vec<BatchEvent>> = HashMap::new();
        let mut last_flush = Instant::now();
        let mut total_processed = 0;

        for (event_id, event_bytes) in events {
            // Parse event type
            let event_str = std::str::from_utf8(event_bytes)
                .map_err(|e| AzothError::EventDecode(format!("Invalid UTF-8: {}", e)))?;

            let (event_type, payload) = event_str.split_once(':').ok_or_else(|| {
                AzothError::EventDecode("Event must be in format 'type:payload'".into())
            })?;

            // Add to appropriate group
            let batch = event_groups.entry(event_type.to_string()).or_default();

            batch.push(BatchEvent {
                event_id,
                payload: payload.as_bytes().to_vec(),
            });

            // Check if we should flush any batches
            let should_flush_size = batch.len() >= self.batch_config.max_batch_size;
            let should_flush_time = last_flush.elapsed() >= self.batch_config.max_batch_duration;

            if should_flush_size || should_flush_time {
                // Flush all batches that have reached their limits
                for (event_type, events) in event_groups.iter_mut() {
                    if !events.is_empty()
                        && (events.len() >= self.batch_config.max_batch_size || should_flush_time)
                    {
                        let handler = self.get(event_type).ok_or_else(|| {
                            AzothError::EventDecode(format!(
                                "No handler for event type '{}'",
                                event_type
                            ))
                        })?;

                        handler.handle_batch(conn, events)?;
                        total_processed += events.len();
                        events.clear();
                    }
                }
                last_flush = Instant::now();
            }
        }

        // Flush any remaining events
        for (event_type, events) in event_groups.iter() {
            if !events.is_empty() {
                let handler = self.get(event_type).ok_or_else(|| {
                    AzothError::EventDecode(format!("No handler for event type '{}'", event_type))
                })?;

                handler.handle_batch(conn, events)?;
                total_processed += events.len();
            }
        }

        Ok(total_processed)
    }

    /// List all registered event types
    pub fn event_types(&self) -> Vec<&str> {
        self.handlers.keys().map(|s| s.as_str()).collect()
    }
}

impl Default for EventHandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper macro to create a simple event handler
///
/// # Example
///
/// ```ignore
/// simple_handler!(DepositHandler, "deposit", |conn, event_id, payload| {
///     let amount: i64 = String::from_utf8_lossy(payload).parse()?;
///     conn.execute("UPDATE accounts SET balance = balance + ?1", [amount])?;
///     Ok(())
/// });
/// ```
#[macro_export]
macro_rules! simple_handler {
    ($name:ident, $event_type:expr, |$conn:ident, $event_id:ident, $payload:ident| $body:block) => {
        struct $name;

        impl $crate::EventHandler for $name {
            fn event_type(&self) -> &str {
                $event_type
            }

            fn handle(
                &self,
                $conn: &rusqlite::Connection,
                $event_id: $crate::EventId,
                $payload: &[u8],
            ) -> $crate::Result<()> {
                $body
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    struct TestHandler;

    impl EventHandler for TestHandler {
        fn event_type(&self) -> &str {
            "test"
        }

        fn handle(&self, _conn: &Connection, _event_id: EventId, _payload: &[u8]) -> Result<()> {
            Ok(())
        }
    }

    struct CountingHandler {
        call_count: Arc<Mutex<usize>>,
        batch_sizes: Arc<Mutex<Vec<usize>>>,
    }

    impl CountingHandler {
        fn new() -> Self {
            Self {
                call_count: Arc::new(Mutex::new(0)),
                batch_sizes: Arc::new(Mutex::new(Vec::new())),
            }
        }

        #[allow(dead_code)]
        fn get_call_count(&self) -> usize {
            *self.call_count.lock().unwrap()
        }

        fn get_batch_sizes(&self) -> Vec<usize> {
            self.batch_sizes.lock().unwrap().clone()
        }
    }

    impl EventHandler for CountingHandler {
        fn event_type(&self) -> &str {
            "count"
        }

        fn handle(&self, _conn: &Connection, _event_id: EventId, _payload: &[u8]) -> Result<()> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;
            Ok(())
        }

        fn handle_batch(&self, _conn: &Connection, events: &[BatchEvent]) -> Result<()> {
            let mut count = self.call_count.lock().unwrap();
            *count += 1;

            let mut sizes = self.batch_sizes.lock().unwrap();
            sizes.push(events.len());

            Ok(())
        }
    }

    #[test]
    fn test_registry() {
        let mut registry = EventHandlerRegistry::new();
        registry.register(Box::new(TestHandler));

        assert!(registry.get("test").is_some());
        assert!(registry.get("other").is_none());
        assert_eq!(registry.event_types(), vec!["test"]);
    }

    #[test]
    fn test_batch_processing() {
        let handler = Arc::new(CountingHandler::new());
        let handler_clone = Arc::clone(&handler);

        let mut registry = EventHandlerRegistry::with_batch_config(BatchConfig {
            max_batch_size: 3,
            max_batch_duration: Duration::from_secs(1),
        });

        registry.register(Box::new(CountingHandler {
            call_count: handler.call_count.clone(),
            batch_sizes: handler.batch_sizes.clone(),
        }));

        // Create a mock connection (won't actually use it)
        let temp_db = tempfile::NamedTempFile::new().unwrap();
        let conn = Connection::open(temp_db.path()).unwrap();

        // Create events
        let events: Vec<(EventId, Vec<u8>)> = vec![
            (0, b"count:1".to_vec()),
            (1, b"count:2".to_vec()),
            (2, b"count:3".to_vec()),
            (3, b"count:4".to_vec()),
            (4, b"count:5".to_vec()),
        ];

        // Process in batches
        let processed = registry
            .process_batched(
                &conn,
                events.iter().map(|(id, data)| (*id, data.as_slice())),
            )
            .unwrap();

        assert_eq!(processed, 5);

        // Should have called handle_batch twice: once for 3 events, once for 2
        let batch_sizes = handler_clone.get_batch_sizes();
        assert_eq!(batch_sizes.len(), 2); // Two batch calls
        assert_eq!(batch_sizes[0], 3); // First batch: 3 events
        assert_eq!(batch_sizes[1], 2); // Second batch: 2 events
    }

    #[test]
    fn test_batch_config() {
        let config = BatchConfig {
            max_batch_size: 500,
            max_batch_duration: Duration::from_millis(50),
        };

        let registry = EventHandlerRegistry::with_batch_config(config.clone());
        assert_eq!(registry.batch_config().max_batch_size, 500);
        assert_eq!(
            registry.batch_config().max_batch_duration,
            Duration::from_millis(50)
        );
    }
}
