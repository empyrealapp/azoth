use crate::{
    config::ConsumerMetadata, consumer::Consumer, consumer_group::ConsumerGroup, error::Result,
    notification::WakeStrategy,
};
use azoth::{typed_values::TypedValue, AsyncTransaction, AzothDb, Transaction};
use azoth_core::traits::canonical::{CanonicalReadTxn, CanonicalStore};
use azoth_core::EventId;
use serde::{Deserialize, Serialize};
use std::panic;
use std::sync::Arc;
use std::time::Duration;

/// Information about a consumer
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerInfo {
    /// Stream name
    pub stream: String,

    /// Consumer name
    pub name: String,

    /// Current cursor position
    pub position: u64,

    /// Number of events behind the head
    pub lag: u64,

    /// Last acknowledgment timestamp
    pub last_ack: Option<chrono::DateTime<chrono::Utc>>,
}

/// Main event bus API
#[derive(Clone)]
pub struct EventBus {
    db: Arc<AzothDb>,
    wake_strategy: WakeStrategy,
}

impl EventBus {
    /// Create a new event bus with default polling wake strategy
    pub fn new(db: Arc<AzothDb>) -> Self {
        Self {
            db,
            wake_strategy: WakeStrategy::default(),
        }
    }

    /// Create a new event bus with a specific wake strategy
    pub fn with_wake_strategy(db: Arc<AzothDb>, wake_strategy: WakeStrategy) -> Self {
        Self { db, wake_strategy }
    }

    /// Enable async notifications (uses tokio::sync::Notify)
    pub fn with_notifications(db: Arc<AzothDb>) -> Self {
        Self {
            db,
            wake_strategy: WakeStrategy::notify(),
        }
    }

    /// Subscribe to a stream, creating or resuming a named consumer
    ///
    /// The consumer automatically filters to events with the stream prefix.
    /// For example, subscribing to "knowledge" will only see "knowledge:*" events.
    pub fn subscribe(
        &self,
        stream: impl Into<String>,
        name: impl Into<String>,
    ) -> Result<Consumer> {
        Consumer::new(
            self.db.clone(),
            stream.into(),
            name.into(),
            self.wake_strategy.clone(),
        )
    }

    /// Create or access a consumer group for load-balanced consumption
    ///
    /// Consumer groups allow multiple members to process events in parallel
    /// without duplicates. Each event is claimed by exactly one member.
    pub fn consumer_group(
        &self,
        stream: impl Into<String>,
        group_name: impl Into<String>,
    ) -> ConsumerGroup {
        ConsumerGroup::new(
            self.db.clone(),
            stream.into(),
            group_name.into(),
            Duration::from_secs(30), // Default 30s lease
        )
    }

    /// Create a consumer group with a custom lease duration
    pub fn consumer_group_with_lease(
        &self,
        stream: impl Into<String>,
        group_name: impl Into<String>,
        lease_duration: Duration,
    ) -> ConsumerGroup {
        ConsumerGroup::new(
            self.db.clone(),
            stream.into(),
            group_name.into(),
            lease_duration,
        )
    }

    /// Get information about a consumer
    pub fn consumer_info(&self, stream: &str, name: &str) -> Result<ConsumerInfo> {
        let cursor_key = format!("bus:consumer:{}:{}:cursor", stream, name).into_bytes();
        let meta_key = format!("bus:consumer:{}:{}:meta", stream, name).into_bytes();

        // Get cursor (last acked event ID) and last ack timestamp
        let (cursor_opt, last_ack) = {
            let txn = self.db.canonical().read_txn()?;

            // Get cursor (last acked event ID)
            let cursor_opt = match txn.get_state(&cursor_key)? {
                Some(bytes) => {
                    let value = TypedValue::from_bytes(&bytes)?;
                    Some(value.as_i64()? as u64)
                }
                None => None,
            };

            // Get metadata for last ack timestamp
            let last_ack = match txn.get_state(&meta_key)? {
                Some(bytes) => {
                    let value = TypedValue::from_bytes(&bytes)?;
                    let meta_bytes = match value {
                        TypedValue::Bytes(b) => b,
                        _ => {
                            return Err(crate::error::BusError::InvalidState(
                                "Consumer metadata must be bytes".into(),
                            ))
                        }
                    };
                    let meta: ConsumerMetadata = serde_json::from_slice(&meta_bytes)?;
                    meta.last_ack_at
                }
                None => None,
            };

            (cursor_opt, last_ack)
        };

        // Calculate position (count of events processed) and lag
        let meta = self.db.canonical().meta()?;
        let head = meta.next_event_id;

        // Position = count of events processed
        // If cursor = Some(N), we've processed N+1 events (events 0 through N)
        // If cursor = None, we've processed 0 events
        let position = match cursor_opt {
            Some(cursor) => cursor + 1,
            None => 0,
        };

        // Lag = number of unprocessed events
        // Next event to read is at position (same as event ID)
        let lag = head.saturating_sub(position);

        Ok(ConsumerInfo {
            stream: stream.to_string(),
            name: name.to_string(),
            position,
            lag,
            last_ack,
        })
    }

    /// List all consumers for a stream
    pub fn list_consumers(&self, stream: &str) -> Result<Vec<ConsumerInfo>> {
        let prefix = format!("bus:consumer:{}:", stream).into_bytes();

        // LMDB has a bug where cursor operations can panic on empty ranges
        // We use catch_unwind to handle this gracefully
        let result = panic::catch_unwind(panic::AssertUnwindSafe(|| {
            let mut consumers = Vec::new();
            let mut seen_names = std::collections::HashSet::new();

            // Scan for all consumer metadata keys (these exist even if no events processed)
            let mut iter = self.db.canonical().scan_prefix(&prefix)?;

            // Handle LMDB cursor issues gracefully
            loop {
                match iter.next() {
                    Ok(Some((key, _value))) => {
                        let key_str = String::from_utf8_lossy(&key);

                        // Parse key: bus:consumer:{stream}:{name}:meta
                        if key_str.ends_with(":meta") {
                            let parts: Vec<&str> = key_str.split(':').collect();
                            if parts.len() >= 4 {
                                let name = parts[3];

                                // Avoid duplicates
                                if !seen_names.contains(name) {
                                    seen_names.insert(name.to_string());

                                    // Get consumer info
                                    if let Ok(info) = self.consumer_info(stream, name) {
                                        consumers.push(info);
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => break, // End of iteration
                    Err(_) => break,   // LMDB cursor error - stop gracefully
                }
            }

            Ok(consumers)
        }));

        match result {
            Ok(Ok(consumers)) => Ok(consumers),
            Ok(Err(e)) => Err(e),
            Err(_) => {
                // LMDB panicked (e.g., cursor error on empty range)
                // Return empty list
                Ok(Vec::new())
            }
        }
    }

    /// Get the underlying database
    pub fn db(&self) -> &Arc<AzothDb> {
        &self.db
    }

    /// Get the wake strategy (useful for manual notifications)
    pub fn wake_strategy(&self) -> &WakeStrategy {
        &self.wake_strategy
    }

    /// Publish an event to a stream and notify waiting consumers
    ///
    /// This is the primary method for real-time stream processing. It:
    /// 1. Atomically commits the event to the log
    /// 2. Immediately notifies all consumers waiting on this stream
    ///
    /// The event type is automatically prefixed with the stream name.
    /// For example, `bus.publish("orders", "created", &data)` logs an event
    /// with type `"orders:created"`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use azoth_bus::EventBus;
    ///
    /// let bus = EventBus::with_notifications(db);
    ///
    /// // Publish event - consumers wake instantly
    /// let event_id = bus.publish("orders", "created", &json!({
    ///     "order_id": "12345",
    ///     "amount": 99.99
    /// }))?;
    /// ```
    pub fn publish<T: serde::Serialize>(
        &self,
        stream: &str,
        event_type: &str,
        payload: &T,
    ) -> Result<EventId> {
        let full_type = format!("{}:{}", stream, event_type);

        let commit_info = Transaction::new(&self.db).execute(|ctx| {
            ctx.log(&full_type, payload)?;
            Ok(())
        })?;

        // Notify waiting consumers
        self.wake_strategy.notify_stream(stream);

        // Return the event ID from commit info
        commit_info
            .first_event_id
            .ok_or_else(|| crate::error::BusError::InvalidState("No event ID returned".into()))
    }

    /// Publish multiple events atomically to a stream
    ///
    /// All events are committed in a single transaction, then consumers
    /// are notified. Returns the range of event IDs (first, last).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let (first_id, last_id) = bus.publish_batch("orders", &[
    ///     ("created", json!({"id": 1})),
    ///     ("created", json!({"id": 2})),
    ///     ("updated", json!({"id": 1, "status": "paid"})),
    /// ])?;
    /// ```
    pub fn publish_batch<T: serde::Serialize>(
        &self,
        stream: &str,
        events: &[(&str, T)],
    ) -> Result<(EventId, EventId)> {
        if events.is_empty() {
            return Err(crate::error::BusError::InvalidState(
                "Cannot publish empty batch".into(),
            ));
        }

        let prefixed_events: Vec<(String, &T)> = events
            .iter()
            .map(|(t, p)| (format!("{}:{}", stream, t), p))
            .collect();

        // We need to log each event individually since log_many expects owned tuples
        let commit_info = Transaction::new(&self.db).execute(|ctx| {
            for (event_type, payload) in &prefixed_events {
                ctx.log(event_type, payload)?;
            }
            Ok(())
        })?;

        // Notify waiting consumers
        self.wake_strategy.notify_stream(stream);

        // Return the event ID range from commit info
        let first = commit_info
            .first_event_id
            .ok_or_else(|| crate::error::BusError::InvalidState("No event ID returned".into()))?;
        let last = commit_info
            .last_event_id
            .ok_or_else(|| crate::error::BusError::InvalidState("No event ID returned".into()))?;

        Ok((first, last))
    }

    /// Publish raw bytes as an event (for advanced use cases)
    ///
    /// Unlike `publish()`, this does not format the event type.
    /// The caller is responsible for the full event format.
    pub fn publish_raw(&self, stream: &str, event_bytes: &[u8]) -> Result<EventId> {
        let commit_info = Transaction::new(&self.db).execute(|ctx| {
            ctx.log_bytes(event_bytes)?;
            Ok(())
        })?;

        // Notify waiting consumers
        self.wake_strategy.notify_stream(stream);

        commit_info
            .first_event_id
            .ok_or_else(|| crate::error::BusError::InvalidState("No event ID returned".into()))
    }

    /// Publish an event asynchronously (safe to call from async context)
    ///
    /// This is the async-safe version of `publish()`. It uses `spawn_blocking`
    /// internally to avoid blocking the Tokio runtime.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Safe to call from async context
    /// let event_id = bus.publish_async("orders", "created", json!({
    ///     "order_id": "12345"
    /// })).await?;
    /// ```
    pub async fn publish_async<T: serde::Serialize + Send + 'static>(
        &self,
        stream: &str,
        event_type: &str,
        payload: T,
    ) -> Result<EventId> {
        let full_type = format!("{}:{}", stream, event_type);
        let stream_name = stream.to_string();
        let wake_strategy = self.wake_strategy.clone();

        let commit_info = AsyncTransaction::new(self.db.clone())
            .execute(move |ctx| {
                ctx.log(&full_type, &payload)?;
                Ok(())
            })
            .await?;

        // Notify waiting consumers
        wake_strategy.notify_stream(&stream_name);

        // Return the event ID from commit info
        commit_info
            .first_event_id
            .ok_or_else(|| crate::error::BusError::InvalidState("No event ID returned".into()))
    }

    /// Publish multiple events atomically (async-safe version)
    ///
    /// All events are committed in a single transaction, then consumers
    /// are notified. Returns the range of event IDs (first, last).
    pub async fn publish_batch_async<T: serde::Serialize + Send + 'static>(
        &self,
        stream: &str,
        events: Vec<(String, T)>,
    ) -> Result<(EventId, EventId)> {
        if events.is_empty() {
            return Err(crate::error::BusError::InvalidState(
                "Cannot publish empty batch".into(),
            ));
        }

        let stream_prefix = stream.to_string();
        let stream_name = stream.to_string();
        let wake_strategy = self.wake_strategy.clone();

        let commit_info = AsyncTransaction::new(self.db.clone())
            .execute(move |ctx| {
                for (event_type, payload) in events {
                    let full_type = format!("{}:{}", stream_prefix, event_type);
                    ctx.log(&full_type, &payload)?;
                }
                Ok(())
            })
            .await?;

        // Notify waiting consumers
        wake_strategy.notify_stream(&stream_name);

        // Return the event ID range from commit info
        let first = commit_info
            .first_event_id
            .ok_or_else(|| crate::error::BusError::InvalidState("No event ID returned".into()))?;
        let last = commit_info
            .last_event_id
            .ok_or_else(|| crate::error::BusError::InvalidState("No event ID returned".into()))?;

        Ok((first, last))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use azoth::Transaction;
    use tempfile::TempDir;

    fn test_db() -> (Arc<AzothDb>, TempDir) {
        let temp = TempDir::new().unwrap();
        let db = AzothDb::open(temp.path()).unwrap();
        (Arc::new(db), temp)
    }

    fn publish_events(db: &AzothDb, stream: &str, count: usize) -> Result<()> {
        Transaction::new(db).execute(|ctx| {
            for i in 0..count {
                let event_type = format!("{}:event{}", stream, i);
                let data = format!("data{}", i);
                ctx.log(&event_type, &data)?;
            }
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn test_subscribe() {
        let (db, _temp) = test_db();
        let bus = EventBus::new(db.clone());

        let consumer = bus.subscribe("test", "c1").unwrap();
        assert_eq!(consumer.stream(), "test");
        assert_eq!(consumer.name(), "c1");
    }

    #[test]
    fn test_consumer_info() {
        let (db, _temp) = test_db();
        let bus = EventBus::new(db.clone());

        publish_events(&db, "test", 10).unwrap();

        let mut consumer = bus.subscribe("test", "c1").unwrap();

        // Initial state
        let info = bus.consumer_info("test", "c1").unwrap();
        assert_eq!(info.position, 0);
        assert_eq!(info.lag, 10);

        // After reading and acking 5 events
        for _ in 0..5 {
            if let Some(event) = consumer.next().unwrap() {
                consumer.ack(event.id).unwrap();
            }
        }

        let info = bus.consumer_info("test", "c1").unwrap();
        assert_eq!(info.position, 5);
        assert_eq!(info.lag, 5);
        assert!(info.last_ack.is_some());
    }

    #[test]
    fn test_list_consumers() {
        let (db, _temp) = test_db();
        let bus = EventBus::new(db.clone());

        // Create multiple consumers
        bus.subscribe("test", "c1").unwrap();
        bus.subscribe("test", "c2").unwrap();
        bus.subscribe("other", "c3").unwrap();

        let consumers = bus.list_consumers("test").unwrap();
        assert_eq!(consumers.len(), 2);

        let names: Vec<String> = consumers.iter().map(|c| c.name.clone()).collect();
        assert!(names.contains(&"c1".to_string()));
        assert!(names.contains(&"c2".to_string()));
    }

    #[test]
    fn test_lag_calculation() {
        let (db, _temp) = test_db();
        let bus = EventBus::new(db.clone());

        publish_events(&db, "test", 100).unwrap();

        let mut c1 = bus.subscribe("test", "c1").unwrap();
        let mut c2 = bus.subscribe("test", "c2").unwrap();

        // c1 processes 50 events
        for _ in 0..50 {
            if let Some(event) = c1.next().unwrap() {
                c1.ack(event.id).unwrap();
            }
        }

        // c2 processes 30 events
        for _ in 0..30 {
            if let Some(event) = c2.next().unwrap() {
                c2.ack(event.id).unwrap();
            }
        }

        let info1 = bus.consumer_info("test", "c1").unwrap();
        let info2 = bus.consumer_info("test", "c2").unwrap();

        assert_eq!(info1.lag, 50);
        assert_eq!(info2.lag, 70);
    }

    #[test]
    fn test_publish() {
        let (db, _temp) = test_db();
        let bus = EventBus::new(db.clone());

        // Publish using the bus API
        let event_id = bus
            .publish("orders", "created", &serde_json::json!({"id": 123}))
            .unwrap();

        assert_eq!(event_id, 0);

        // Consumer should see the event with prefixed type
        let mut consumer = bus.subscribe("orders", "c1").unwrap();
        let event = consumer.next().unwrap().unwrap();

        assert_eq!(event.event_type, "orders:created");
        assert!(String::from_utf8_lossy(&event.payload).contains("123"));
    }

    #[test]
    fn test_publish_batch() {
        let (db, _temp) = test_db();
        let bus = EventBus::new(db.clone());

        // Publish batch using the bus API
        let events = vec![
            ("created", serde_json::json!({"id": 1})),
            ("created", serde_json::json!({"id": 2})),
            ("updated", serde_json::json!({"id": 1, "status": "paid"})),
        ];

        let (first_id, last_id) = bus.publish_batch("orders", &events).unwrap();

        assert_eq!(first_id, 0);
        assert_eq!(last_id, 2);

        // Consumer should see all events
        let mut consumer = bus.subscribe("orders", "c1").unwrap();

        let event1 = consumer.next().unwrap().unwrap();
        assert_eq!(event1.event_type, "orders:created");
        consumer.ack(event1.id).unwrap();

        let event2 = consumer.next().unwrap().unwrap();
        assert_eq!(event2.event_type, "orders:created");
        consumer.ack(event2.id).unwrap();

        let event3 = consumer.next().unwrap().unwrap();
        assert_eq!(event3.event_type, "orders:updated");
        consumer.ack(event3.id).unwrap();

        assert!(consumer.next().unwrap().is_none());
    }

    #[test]
    fn test_publish_notifies_consumers() {
        let (db, _temp) = test_db();

        // Create bus with notifications enabled
        let bus = EventBus::with_notifications(db.clone());

        // Create consumer first
        let mut consumer = bus.subscribe("orders", "c1").unwrap();

        // No events initially
        assert!(consumer.next().unwrap().is_none());

        // Publish an event - this should trigger notification
        let event_id = bus
            .publish("orders", "created", &serde_json::json!({"test": true}))
            .unwrap();

        // Consumer should now see the event
        let event = consumer.next().unwrap().unwrap();
        assert_eq!(event.id, event_id);
        assert_eq!(event.event_type, "orders:created");
    }

    #[test]
    fn test_publish_raw() {
        let (db, _temp) = test_db();
        let bus = EventBus::new(db.clone());

        // Publish raw bytes
        let raw_event = b"custom:raw_event:{\"data\":\"test\"}";
        let event_id = bus.publish_raw("custom", raw_event).unwrap();

        assert_eq!(event_id, 0);
    }

    #[test]
    fn test_wake_strategy_getter() {
        let (db, _temp) = test_db();

        let bus = EventBus::with_notifications(db.clone());

        // Should be able to get wake strategy for manual notifications
        let wake = bus.wake_strategy();
        wake.notify_stream("test"); // Should not panic
    }
}
