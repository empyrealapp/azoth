use crate::{
    config::ConsumerMetadata,
    error::{BusError, Result},
    filter::{Event, EventFilter, EventFilterTrait},
    notification::WakeStrategy,
};
use azoth::{typed_values::TypedValue, AsyncTransaction, AzothDb, Transaction};
use azoth_core::{
    traits::canonical::{CanonicalReadTxn, CanonicalStore},
    EventId,
};
use std::sync::Arc;

/// A consumer of events from a stream
pub struct Consumer {
    db: Arc<AzothDb>,
    stream: String,
    name: String,
    filter: EventFilter,
    cursor_key: Vec<u8>,
    meta_key: Vec<u8>,
    wake_strategy: WakeStrategy,
}

impl Consumer {
    /// Create a new consumer
    pub fn new(
        db: Arc<AzothDb>,
        stream: String,
        name: String,
        wake_strategy: WakeStrategy,
    ) -> Result<Self> {
        let cursor_key = format!("bus:consumer:{}:{}:cursor", stream, name).into_bytes();
        let meta_key = format!("bus:consumer:{}:{}:meta", stream, name).into_bytes();

        // Initialize metadata if this is a new consumer
        let meta = ConsumerMetadata::new(stream.clone(), name.clone());
        let meta_bytes = serde_json::to_vec(&meta)?;

        Transaction::new(&db)
            .keys(vec![meta_key.clone()])
            .execute(|ctx| {
                // Only write if not exists
                if ctx.get_opt(&meta_key)?.is_none() {
                    ctx.set(&meta_key, &TypedValue::Bytes(meta_bytes))?;
                }
                Ok(())
            })?;

        // Auto-filter to stream prefix
        let stream_filter = EventFilter::prefix(format!("{}:", stream));

        Ok(Self {
            db,
            stream,
            name,
            filter: stream_filter,
            cursor_key,
            meta_key,
            wake_strategy,
        })
    }

    /// Add an additional filter on top of the stream filter
    pub fn with_filter(mut self, filter: EventFilter) -> Self {
        self.filter = self.filter.and(filter);
        self
    }

    /// Get the current cursor position (last acknowledged event ID)
    ///
    /// Returns None if no events have been acknowledged yet.
    pub fn position(&self) -> Result<Option<u64>> {
        let txn = self.db.canonical().read_txn()?;
        match txn.get_state(&self.cursor_key)? {
            Some(bytes) => {
                let value = TypedValue::from_bytes(&bytes)?;
                Ok(Some(value.as_i64()? as u64))
            }
            None => Ok(None),
        }
    }

    /// Seek to read from a specific event ID
    ///
    /// The next call to `next()` will return the event at `event_id`.
    /// If `event_id` is 0, resets to the beginning.
    pub fn seek(&mut self, event_id: u64) -> Result<()> {
        if event_id == 0 {
            // Reset to beginning - delete cursor
            Transaction::new(&self.db)
                .keys(vec![self.cursor_key.clone()])
                .execute(|ctx| {
                    ctx.delete(&self.cursor_key)?;
                    Ok(())
                })?;
        } else {
            // Set cursor to event_id - 1 so next() reads from event_id
            Transaction::new(&self.db)
                .keys(vec![self.cursor_key.clone()])
                .execute(|ctx| {
                    ctx.set(&self.cursor_key, &TypedValue::I64((event_id - 1) as i64))?;
                    Ok(())
                })?;
        }
        Ok(())
    }

    /// Read the next event (blocking poll)
    ///
    /// Returns the next unprocessed event that matches the filter.
    /// Call `ack()` to advance the cursor after processing.
    ///
    /// Note: This method is intentionally not implementing Iterator
    /// because it returns Result and requires error handling.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Event>> {
        let cursor = self.position()?;
        // Start from cursor + 1, or 0 if no cursor (never processed anything)
        let start_id = match cursor {
            None => 0,
            Some(n) => n + 1,
        };
        let mut iter = self.db.canonical().iter_events(start_id, None)?;

        while let Some((id, bytes)) = iter.next()? {
            let event = Event::decode(id, &bytes)?;

            if self.filter.matches(&event) {
                return Ok(Some(event));
            }
        }

        Ok(None)
    }

    /// Acknowledge processing of an event (update cursor)
    pub fn ack(&mut self, event_id: EventId) -> Result<()> {
        let meta_key = self.meta_key.clone();
        let cursor_key = self.cursor_key.clone();

        Transaction::new(&self.db)
            .keys(vec![cursor_key.clone(), meta_key.clone()])
            .execute(|ctx| {
                // Update cursor
                ctx.set(&cursor_key, &TypedValue::I64(event_id as i64))?;

                // Update last ack timestamp in metadata
                if let Some(meta_value) = ctx.get_opt(&meta_key)? {
                    let meta_bytes = match meta_value {
                        TypedValue::Bytes(b) => b,
                        _ => {
                            return Err(azoth_core::AzothError::InvalidState(
                                "Consumer metadata must be bytes".into(),
                            ))
                        }
                    };
                    let mut meta: ConsumerMetadata = serde_json::from_slice(&meta_bytes)
                        .map_err(|e| azoth_core::AzothError::Serialization(e.to_string()))?;
                    meta.last_ack_at = Some(chrono::Utc::now());

                    let updated_bytes = serde_json::to_vec(&meta)
                        .map_err(|e| azoth_core::AzothError::Serialization(e.to_string()))?;
                    ctx.set(&meta_key, &TypedValue::Bytes(updated_bytes))?;
                }

                Ok(())
            })?;
        Ok(())
    }

    /// Acknowledge processing of an event asynchronously (safe for async context)
    ///
    /// This is the async-safe version of `ack()`. Use this when calling from
    /// async code to avoid blocking the runtime.
    pub async fn ack_async(&mut self, event_id: EventId) -> Result<()> {
        let meta_key = self.meta_key.clone();
        let cursor_key = self.cursor_key.clone();

        AsyncTransaction::new(self.db.clone())
            .keys(vec![cursor_key.clone(), meta_key.clone()])
            .execute(move |ctx| {
                // Update cursor
                ctx.set(&cursor_key, &TypedValue::I64(event_id as i64))?;

                // Update last ack timestamp in metadata
                if let Some(meta_value) = ctx.get_opt(&meta_key)? {
                    let meta_bytes = match meta_value {
                        TypedValue::Bytes(b) => b,
                        _ => {
                            return Err(azoth_core::AzothError::InvalidState(
                                "Consumer metadata must be bytes".into(),
                            ))
                        }
                    };
                    let mut meta: ConsumerMetadata = serde_json::from_slice(&meta_bytes)
                        .map_err(|e| azoth_core::AzothError::Serialization(e.to_string()))?;
                    meta.last_ack_at = Some(chrono::Utc::now());

                    let updated_bytes = serde_json::to_vec(&meta)
                        .map_err(|e| azoth_core::AzothError::Serialization(e.to_string()))?;
                    ctx.set(&meta_key, &TypedValue::Bytes(updated_bytes))?;
                }

                Ok(())
            })
            .await?;
        Ok(())
    }

    /// Get consumer metadata
    pub fn metadata(&self) -> Result<ConsumerMetadata> {
        let txn = self.db.canonical().read_txn()?;
        match txn.get_state(&self.meta_key)? {
            Some(bytes) => {
                let value = TypedValue::from_bytes(&bytes)?;
                let meta_bytes = match value {
                    TypedValue::Bytes(b) => b,
                    _ => {
                        return Err(BusError::InvalidState(
                            "Consumer metadata must be bytes".into(),
                        ))
                    }
                };
                Ok(serde_json::from_slice(&meta_bytes)?)
            }
            None => Err(BusError::ConsumerNotFound(format!(
                "{}:{}",
                self.stream, self.name
            ))),
        }
    }

    /// Get the stream name
    pub fn stream(&self) -> &str {
        &self.stream
    }

    /// Get the consumer name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Read the next event asynchronously (non-blocking)
    ///
    /// This method will wait for new events using the configured wake strategy.
    /// With polling (default), it checks periodically. With notifications, it
    /// waits for explicit wake-up signals.
    ///
    /// Returns the next unprocessed event that matches the filter.
    /// Call `ack()` to advance the cursor after processing.
    pub async fn next_async(&mut self) -> Result<Option<Event>> {
        loop {
            // Try to get an event
            if let Some(event) = self.next()? {
                return Ok(Some(event));
            }

            // No event available, wait for wake signal
            self.wake_strategy.wait(&self.stream).await;
        }
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

    fn publish_event(db: &AzothDb, event_type: &str, data: &str) -> Result<()> {
        Transaction::new(db).execute(|ctx| {
            ctx.log(event_type, &data)?;
            Ok(())
        })?;
        Ok(())
    }

    fn test_consumer(db: Arc<AzothDb>, stream: &str, name: &str) -> Result<Consumer> {
        Consumer::new(
            db,
            stream.to_string(),
            name.to_string(),
            WakeStrategy::default(),
        )
    }

    #[test]
    fn test_consumer_creation() {
        let (db, _temp) = test_db();
        let consumer = test_consumer(db.clone(), "test", "c1").unwrap();

        assert_eq!(consumer.stream(), "test");
        assert_eq!(consumer.name(), "c1");
        assert_eq!(consumer.position().unwrap(), None); // No events processed yet
    }

    #[test]
    fn test_consumer_next_with_stream_filter() {
        let (db, _temp) = test_db();

        // Publish events to different streams
        publish_event(&db, "test:event1", "data1").unwrap();
        publish_event(&db, "other:event2", "data2").unwrap();
        publish_event(&db, "test:event3", "data3").unwrap();

        // Consumer should only see "test:" events
        let mut consumer = test_consumer(db.clone(), "test", "c1").unwrap();

        let event1 = consumer.next().unwrap().unwrap();
        assert_eq!(event1.event_type, "test:event1");
        consumer.ack(event1.id).unwrap();

        let event2 = consumer.next().unwrap().unwrap();
        assert_eq!(event2.event_type, "test:event3");
        consumer.ack(event2.id).unwrap();

        assert!(consumer.next().unwrap().is_none());
    }

    #[test]
    fn test_consumer_with_additional_filter() {
        let (db, _temp) = test_db();

        publish_event(&db, "test:doc_updated", "data1").unwrap();
        publish_event(&db, "test:index_updated", "data2").unwrap();
        publish_event(&db, "test:doc_deleted", "data3").unwrap();

        // Filter further to only "test:doc_" events
        let mut consumer = test_consumer(db.clone(), "test", "c1")
            .unwrap()
            .with_filter(EventFilter::prefix("test:doc_"));

        let event1 = consumer.next().unwrap().unwrap();
        assert_eq!(event1.event_type, "test:doc_updated");
        consumer.ack(event1.id).unwrap();

        let event2 = consumer.next().unwrap().unwrap();
        assert_eq!(event2.event_type, "test:doc_deleted");
        consumer.ack(event2.id).unwrap();

        assert!(consumer.next().unwrap().is_none());
    }

    #[test]
    fn test_consumer_ack() {
        let (db, _temp) = test_db();

        publish_event(&db, "test:event1", "data1").unwrap();
        publish_event(&db, "test:event2", "data2").unwrap();

        let mut consumer = test_consumer(db.clone(), "test", "c1").unwrap();

        let event1 = consumer.next().unwrap().unwrap();
        consumer.ack(event1.id).unwrap();

        assert_eq!(consumer.position().unwrap(), Some(event1.id));

        // After restart, should resume from acknowledged position
        let mut consumer2 = test_consumer(db.clone(), "test", "c1").unwrap();
        let event2 = consumer2.next().unwrap().unwrap();
        assert_eq!(event2.event_type, "test:event2");
    }

    #[test]
    fn test_consumer_seek() {
        let (db, _temp) = test_db();

        publish_event(&db, "test:event1", "data1").unwrap();
        publish_event(&db, "test:event2", "data2").unwrap();
        publish_event(&db, "test:event3", "data3").unwrap();

        let mut consumer = test_consumer(db.clone(), "test", "c1").unwrap();

        // Seek to read from event ID 1 (test:event2, which is the second event)
        consumer.seek(1).unwrap();

        let event = consumer.next().unwrap().unwrap();
        assert_eq!(event.event_type, "test:event2");
        assert_eq!(event.id, 1);
    }

    #[test]
    fn test_independent_consumers() {
        let (db, _temp) = test_db();

        publish_event(&db, "test:event1", "data1").unwrap();
        publish_event(&db, "test:event2", "data2").unwrap();
        publish_event(&db, "test:event3", "data3").unwrap();

        let mut c1 = test_consumer(db.clone(), "test", "c1").unwrap();
        let mut c2 = test_consumer(db.clone(), "test", "c2").unwrap();

        // c1 reads and acks first event (ID 0)
        let e1 = c1.next().unwrap().unwrap();
        assert_eq!(e1.id, 0);
        c1.ack(e1.id).unwrap();

        // c2 reads first event (ID 0), acks it, then reads and acks second event (ID 1)
        let e1 = c2.next().unwrap().unwrap();
        assert_eq!(e1.id, 0);
        c2.ack(e1.id).unwrap();

        let e2 = c2.next().unwrap().unwrap();
        assert_eq!(e2.id, 1);
        c2.ack(e2.id).unwrap();

        // c1 acked event 0, c2 acked event 1
        assert_eq!(c1.position().unwrap(), Some(0));
        assert_eq!(c2.position().unwrap(), Some(1));
    }

    #[test]
    fn test_consumer_catches_new_events() {
        let (db, _temp) = test_db();

        // Create consumer first (no events yet)
        let mut consumer = test_consumer(db.clone(), "test", "c1").unwrap();

        // Consumer should see no events initially
        assert!(consumer.next().unwrap().is_none());

        // Publish events AFTER consumer is created
        publish_event(&db, "test:event1", "data1").unwrap();
        publish_event(&db, "test:event2", "data2").unwrap();

        // Consumer should now see the new events
        let event1 = consumer.next().unwrap();
        assert!(
            event1.is_some(),
            "Consumer should catch events published after creation"
        );
        let event1 = event1.unwrap();
        assert_eq!(event1.event_type, "test:event1");
        consumer.ack(event1.id).unwrap();

        let event2 = consumer.next().unwrap().unwrap();
        assert_eq!(event2.event_type, "test:event2");
        consumer.ack(event2.id).unwrap();

        assert!(consumer.next().unwrap().is_none());
    }

    #[test]
    fn test_consumer_polling_loop_catches_events() {
        let (db, _temp) = test_db();

        // Create consumer first (no events yet)
        let mut consumer = test_consumer(db.clone(), "test", "c1").unwrap();

        // Start a polling loop (similar to projector behavior)
        let mut found_event = false;
        for iteration in 0..50 {
            if iteration == 10 {
                // Publish event during polling loop
                publish_event(&db, "test:event1", "data1").unwrap();
            }

            if let Some(event) = consumer.next().unwrap() {
                assert_eq!(event.event_type, "test:event1");
                consumer.ack(event.id).unwrap();
                found_event = true;
                break;
            }

            // Small sleep to simulate polling interval
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

        assert!(
            found_event,
            "Consumer polling loop should catch event published during polling"
        );
    }
}
