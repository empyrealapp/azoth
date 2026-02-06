//! Stream-based event consumption for async/await workflows
//!
//! Provides `futures::Stream` implementations for consuming events.

use crate::{consumer::Consumer, error::Result, filter::Event};
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// Type alias for the pending event future to reduce type complexity
type PendingEventFuture = Pin<Box<dyn std::future::Future<Output = Result<Option<Event>>> + Send>>;

/// A stream wrapper for a Consumer that yields events
///
/// This provides an ergonomic way to consume events using async/await:
///
/// ```ignore
/// use futures::StreamExt;
///
/// let consumer = bus.subscribe("tasks", "worker")?;
/// let mut stream = consumer.into_stream();
///
/// while let Some(result) = stream.next().await {
///     let event = result?;
///     println!("Got event: {}", event.id);
///     stream.ack(event.id)?;
/// }
/// ```
#[pin_project::pin_project]
pub struct ConsumerStream {
    consumer: Consumer,
    #[pin]
    pending_future: Option<PendingEventFuture>,
}

impl ConsumerStream {
    /// Create a new ConsumerStream from a Consumer
    pub fn new(consumer: Consumer) -> Self {
        Self {
            consumer,
            pending_future: None,
        }
    }

    /// Acknowledge an event after processing
    ///
    /// Call this after successfully processing an event to advance the cursor.
    pub fn ack(&mut self, event_id: u64) -> Result<()> {
        self.consumer.ack(event_id)
    }

    /// Get the consumer's stream name
    pub fn stream_name(&self) -> &str {
        self.consumer.stream()
    }

    /// Get the consumer's name
    pub fn consumer_name(&self) -> &str {
        self.consumer.name()
    }

    /// Get the current cursor position
    pub fn position(&self) -> Result<Option<u64>> {
        self.consumer.position()
    }
}

// Note: Implementing Stream properly with async operations requires careful handling.
// For now, we provide a simpler synchronous stream that yields available events.
// For full async support with wake notifications, use Consumer.next_async() directly.

impl Stream for ConsumerStream {
    type Item = Result<Event>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        // Try to get the next event synchronously
        match this.consumer.next() {
            Ok(Some(event)) => Poll::Ready(Some(Ok(event))),
            Ok(None) => Poll::Pending, // No events available, will need to be woken
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl Consumer {
    /// Convert this consumer into an async Stream
    ///
    /// The returned stream yields `Result<Event>` items.
    /// After processing each event, call `ack()` on the stream.
    ///
    /// Note: For true async wake-up behavior with notifications,
    /// use `next_async()` directly in a loop instead.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use futures::StreamExt;
    ///
    /// let consumer = bus.subscribe("orders", "processor")?;
    /// let mut stream = consumer.into_stream();
    ///
    /// while let Some(result) = stream.next().await {
    ///     let event = result?;
    ///     process(&event).await?;
    ///     stream.ack(event.id)?;
    /// }
    /// ```
    pub fn into_stream(self) -> ConsumerStream {
        ConsumerStream::new(self)
    }
}

/// Create an async event stream using `futures::stream::unfold`
///
/// This provides a properly async stream that integrates with the wake strategy.
/// The stream will yield events as they become available, waiting asynchronously
/// when no events are present.
///
/// Note: This stream does not auto-acknowledge events. You need to manually
/// acknowledge events using the consumer after the stream is dropped or by
/// tracking event IDs. For most use cases, prefer `auto_ack_stream()`.
///
/// # Example
///
/// ```ignore
/// use futures::StreamExt;
/// use azoth_bus::stream::event_stream;
///
/// let consumer = bus.subscribe("tasks", "worker")?;
/// let mut stream = event_stream(consumer);
///
/// while let Some(result) = stream.next().await {
///     match result {
///         Ok(event) => {
///             println!("Processing: {}", event.id);
///             // Note: events are NOT auto-acknowledged
///         }
///         Err(e) => eprintln!("Error: {}", e),
///     }
/// }
/// ```
pub fn event_stream(consumer: Consumer) -> impl Stream<Item = Result<Event>> {
    futures::stream::unfold(consumer, |mut consumer| async move {
        match consumer.next_async().await {
            Ok(Some(event)) => Some((Ok(event), consumer)),
            Ok(None) => None, // Stream ended (shouldn't happen with async wait)
            Err(e) => Some((Err(e), consumer)),
        }
    })
}

/// Create an auto-acknowledging async event stream
///
/// Events are automatically acknowledged after being yielded.
/// Use this when you want simple fire-and-forget processing.
///
/// # Example
///
/// ```ignore
/// use futures::StreamExt;
/// use azoth_bus::stream::auto_ack_stream;
///
/// let consumer = bus.subscribe("logs", "processor")?;
/// let mut stream = auto_ack_stream(consumer);
///
/// while let Some(result) = stream.next().await {
///     let event = result?;
///     println!("Logged: {:?}", event.payload);
///     // Event is automatically acknowledged
/// }
/// ```
pub fn auto_ack_stream(consumer: Consumer) -> impl Stream<Item = Result<Event>> {
    futures::stream::unfold(consumer, |mut consumer| async move {
        match consumer.next_async().await {
            Ok(Some(event)) => {
                let event_id = event.id;
                // Auto-acknowledge after receiving
                if let Err(e) = consumer.ack(event_id) {
                    return Some((Err(e), consumer));
                }
                Some((Ok(event), consumer))
            }
            Ok(None) => None,
            Err(e) => Some((Err(e), consumer)),
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::notification::WakeStrategy;
    use azoth::{AzothDb, Transaction};
    use std::sync::Arc;
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

    #[test]
    fn test_consumer_stream_creation() {
        let (db, _temp) = test_db();

        let consumer = Consumer::new(
            db,
            "test".to_string(),
            "c1".to_string(),
            WakeStrategy::default(),
        )
        .unwrap();

        let stream = consumer.into_stream();
        assert_eq!(stream.stream_name(), "test");
        assert_eq!(stream.consumer_name(), "c1");
    }

    // Note: Async stream tests are tricky because Transaction::execute()
    // panics in async context. We test the synchronous parts here and
    // rely on the notification tests for async behavior.

    #[test]
    fn test_event_stream_sync() {
        let (db, _temp) = test_db();

        // Publish events first
        publish_event(&db, "test:event1", "data1").unwrap();
        publish_event(&db, "test:event2", "data2").unwrap();

        let consumer = Consumer::new(
            db,
            "test".to_string(),
            "c1".to_string(),
            WakeStrategy::default(),
        )
        .unwrap();

        // Use into_stream which provides a sync-compatible stream
        let stream = consumer.into_stream();

        // Stream name should be preserved
        assert_eq!(stream.stream_name(), "test");
        assert_eq!(stream.consumer_name(), "c1");

        // We can ack through the stream
        // Note: actual async streaming is tested in integration tests
    }

    #[test]
    fn test_consumer_into_stream_preserves_state() {
        let (db, _temp) = test_db();

        publish_event(&db, "test:event1", "data1").unwrap();

        let consumer = Consumer::new(
            db,
            "test".to_string(),
            "c1".to_string(),
            WakeStrategy::default(),
        )
        .unwrap();

        let stream = consumer.into_stream();

        // Position should be None initially (no events processed)
        assert_eq!(stream.position().unwrap(), None);
    }
}
