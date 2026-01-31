//! Continuous Event Processor
//!
//! Provides a builder API for setting up long-running event processors
//! that continuously consume events and route them to handlers.

use crate::{
    AzothDb, AzothError, DeadLetterQueue, EventHandler, EventHandlerRegistry, EventId, Result,
};
use azoth_core::traits::CanonicalStore;
use rusqlite::Connection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Error handling strategy for failed event processing
#[allow(clippy::type_complexity)]
pub enum ErrorStrategy {
    /// Fail fast - stop processing on first error
    FailFast,
    /// Log and skip - log the error and continue with next event
    LogAndSkip,
    /// Send to dead letter queue and continue
    DeadLetterQueue,
    /// Retry with backoff - retry failed events with exponential backoff
    RetryWithBackoff {
        max_retries: usize,
        initial_delay_ms: u64,
    },
    /// Custom handler - call a custom function to decide what to do
    Custom(Arc<dyn Fn(&AzothError, EventId, &[u8]) -> ErrorAction + Send + Sync>),
}

/// Action to take after an error
#[derive(Debug, Clone, Copy)]
pub enum ErrorAction {
    /// Stop processing
    Stop,
    /// Skip this event and continue
    Skip,
    /// Retry this event
    Retry,
    /// Send to dead letter queue and continue
    DeadLetter,
}

/// Builder for continuous event processors
///
/// # Example
///
/// ```ignore
/// let processor = EventProcessorBuilder::new(db)
///     .with_handler(DepositHandler)
///     .with_handler(WithdrawHandler)
///     .with_poll_interval(Duration::from_millis(100))
///     .with_error_strategy(ErrorStrategy::LogAndSkip)
///     .build()?;
///
/// processor.run().await?;
/// ```
pub struct EventProcessorBuilder {
    db: Arc<AzothDb>,
    registry: EventHandlerRegistry,
    poll_interval: Duration,
    batch_size: usize,
    error_strategy: ErrorStrategy,
    dlq: Option<Arc<DeadLetterQueue>>,
}

impl EventProcessorBuilder {
    /// Create a new event processor builder
    pub fn new(db: Arc<AzothDb>) -> Self {
        Self {
            db,
            registry: EventHandlerRegistry::new(),
            poll_interval: Duration::from_millis(100),
            batch_size: 100,
            error_strategy: ErrorStrategy::FailFast,
            dlq: None,
        }
    }

    /// Register an event handler
    pub fn with_handler(mut self, handler: Box<dyn EventHandler>) -> Self {
        self.registry.register(handler);
        self
    }

    /// Set the poll interval (how often to check for new events)
    pub fn with_poll_interval(mut self, interval: Duration) -> Self {
        self.poll_interval = interval;
        self
    }

    /// Set the batch size (how many events to process at once)
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }

    /// Set the error handling strategy
    pub fn with_error_strategy(mut self, strategy: ErrorStrategy) -> Self {
        self.error_strategy = strategy;
        self
    }

    /// Enable dead letter queue
    pub fn with_dead_letter_queue(mut self, dlq: Arc<DeadLetterQueue>) -> Self {
        self.dlq = Some(dlq);
        self
    }

    /// Build the event processor
    pub fn build(self, conn: Arc<Connection>) -> EventProcessor {
        EventProcessor {
            db: self.db,
            conn,
            registry: Arc::new(self.registry),
            poll_interval: self.poll_interval,
            batch_size: self.batch_size,
            error_strategy: self.error_strategy,
            dlq: self.dlq,
            shutdown: Arc::new(AtomicBool::new(false)),
            cursor: 0,
        }
    }
}

/// Continuous event processor
///
/// Runs in the background, continuously consuming events and routing
/// them to registered handlers.
pub struct EventProcessor {
    db: Arc<AzothDb>,
    conn: Arc<Connection>,
    registry: Arc<EventHandlerRegistry>,
    poll_interval: Duration,
    batch_size: usize,
    error_strategy: ErrorStrategy,
    dlq: Option<Arc<DeadLetterQueue>>,
    shutdown: Arc<AtomicBool>,
    cursor: u64,
}

impl EventProcessor {
    /// Create a new builder
    pub fn builder(db: Arc<AzothDb>) -> EventProcessorBuilder {
        EventProcessorBuilder::new(db)
    }

    /// Run the processor until shutdown is signaled
    ///
    /// This is an async function that runs continuously, polling for new
    /// events and processing them through registered handlers.
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!("Event processor started");

        while !self.shutdown.load(Ordering::SeqCst) {
            let processed = self.process_batch()?;

            if processed == 0 {
                // No events, sleep before polling again
                tokio::time::sleep(self.poll_interval).await;
            }
        }

        tracing::info!("Event processor shutdown");
        Ok(())
    }

    /// Run the processor synchronously (blocking)
    ///
    /// Useful for single-threaded applications or testing.
    pub fn run_blocking(&mut self) -> Result<()> {
        tracing::info!("Event processor started (blocking mode)");

        while !self.shutdown.load(Ordering::SeqCst) {
            let processed = self.process_batch()?;

            if processed == 0 {
                // No events, sleep before polling again
                std::thread::sleep(self.poll_interval);
            }
        }

        tracing::info!("Event processor shutdown");
        Ok(())
    }

    /// Process a single batch of events
    ///
    /// Returns the number of events processed.
    pub fn process_batch(&mut self) -> Result<usize> {
        let canonical = self.db.as_ref().canonical();
        let meta = canonical.as_ref().meta()?;
        let tip = if meta.next_event_id > 0 {
            meta.next_event_id - 1
        } else {
            0
        };

        if self.cursor >= tip {
            return Ok(0);
        }

        let to = std::cmp::min(tip + 1, self.cursor + self.batch_size as u64 + 1);
        let mut iter = canonical.as_ref().iter_events(self.cursor + 1, Some(to))?;
        let mut processed = 0;

        while let Some((id, bytes)) = iter.next()? {
            match self.registry.process(self.conn.as_ref(), id, &bytes) {
                Ok(_) => {
                    self.cursor = id;
                    processed += 1;
                }
                Err(e) => {
                    let action = self.handle_error(&e, id, &bytes);
                    match action {
                        ErrorAction::Stop => return Err(e),
                        ErrorAction::Skip => {
                            tracing::warn!("Skipping event {} due to error: {}", id, e);
                            self.cursor = id;
                        }
                        ErrorAction::Retry => {
                            tracing::info!("Will retry event {} on next batch", id);
                            break;
                        }
                        ErrorAction::DeadLetter => {
                            if let Some(dlq) = &self.dlq {
                                match dlq.add(id, &bytes, &e) {
                                    Ok(dlq_id) => {
                                        tracing::warn!(
                                            "Event {} sent to DLQ (id: {}): {}",
                                            id,
                                            dlq_id,
                                            e
                                        );
                                        self.cursor = id; // Move past the failed event
                                    }
                                    Err(dlq_err) => {
                                        tracing::error!(
                                            "Failed to add event {} to DLQ: {}",
                                            id,
                                            dlq_err
                                        );
                                        return Err(e);
                                    }
                                }
                            } else {
                                tracing::error!(
                                    "DeadLetter action requested but no DLQ configured"
                                );
                                return Err(e);
                            }
                        }
                    }
                }
            }
        }

        if processed > 0 {
            tracing::debug!("Processed {} events (cursor: {})", processed, self.cursor);
        }

        Ok(processed)
    }

    /// Handle an error according to the configured strategy
    fn handle_error(
        &self,
        error: &AzothError,
        event_id: EventId,
        event_bytes: &[u8],
    ) -> ErrorAction {
        match &self.error_strategy {
            ErrorStrategy::FailFast => ErrorAction::Stop,
            ErrorStrategy::LogAndSkip => {
                tracing::error!("Error processing event {}: {}", event_id, error);
                ErrorAction::Skip
            }
            ErrorStrategy::DeadLetterQueue => {
                tracing::error!(
                    "Error processing event {}: {}. Sending to DLQ",
                    event_id,
                    error
                );
                ErrorAction::DeadLetter
            }
            ErrorStrategy::RetryWithBackoff {
                max_retries,
                initial_delay_ms,
            } => {
                tracing::warn!(
                    "Error processing event {}: {}. Will retry (max_retries: {}, initial_delay: {}ms)",
                    event_id, error, max_retries, initial_delay_ms
                );
                ErrorAction::Retry
            }
            ErrorStrategy::Custom(handler) => handler(error, event_id, event_bytes),
        }
    }

    /// Signal graceful shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Get a handle for shutting down the processor
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle {
            shutdown: self.shutdown.clone(),
        }
    }

    /// Get the current cursor position
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Get the event lag (how many events behind the tip)
    pub fn lag(&self) -> Result<u64> {
        let canonical = self.db.as_ref().canonical();
        let meta = canonical.as_ref().meta()?;
        let tip = if meta.next_event_id > 0 {
            meta.next_event_id - 1
        } else {
            0
        };
        Ok(tip.saturating_sub(self.cursor))
    }
}

/// Handle for shutting down an event processor
#[derive(Clone)]
pub struct ShutdownHandle {
    shutdown: Arc<AtomicBool>,
}

impl ShutdownHandle {
    /// Signal shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }
}
