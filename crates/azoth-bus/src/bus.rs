use crate::{
    config::ConsumerMetadata, consumer::Consumer, consumer_group::ConsumerGroup, error::Result,
    notification::WakeStrategy,
};
use azoth::{typed_values::TypedValue, AzothDb};
use azoth_core::traits::canonical::CanonicalStore;
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
            let txn = self.db.canonical().read_only_txn()?;

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
}
