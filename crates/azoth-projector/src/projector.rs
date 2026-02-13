use azoth_core::{
    error::Result,
    traits::{CanonicalStore, ProjectionStore, ProjectionTxn},
    types::EventId,
    ProjectorConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// Projector: consumes events from canonical store and applies to projection
pub struct Projector<C, P>
where
    C: CanonicalStore,
    P: ProjectionStore,
{
    canonical: Arc<C>,
    projection: Arc<P>,
    config: ProjectorConfig,
    shutdown: Arc<AtomicBool>,
    /// When set, the projector awaits this notification instead of polling.
    /// The `FileEventLog` fires `notify_waiters()` after every successful append,
    /// giving near-zero-latency projection with zero CPU waste when idle.
    event_notify: Option<Arc<Notify>>,
}

impl<C, P> Projector<C, P>
where
    C: CanonicalStore,
    P: ProjectionStore,
{
    pub fn new(canonical: Arc<C>, projection: Arc<P>, config: ProjectorConfig) -> Self {
        Self {
            canonical,
            projection,
            config,
            shutdown: Arc::new(AtomicBool::new(false)),
            event_notify: None,
        }
    }

    /// Attach an event notification handle for push-based projection.
    ///
    /// When set, `run_continuous()` awaits this notification instead of
    /// sleeping for `poll_interval_ms` when caught up, giving near-zero
    /// latency event processing with zero idle CPU usage.
    pub fn with_event_notify(mut self, notify: Arc<Notify>) -> Self {
        self.event_notify = Some(notify);
        self
    }

    /// Run one iteration of the projector loop
    pub fn run_once(&self) -> Result<ProjectorStats> {
        let start = Instant::now();

        // Get current cursor
        let cursor = self.projection.get_cursor()?;

        // Determine target event ID
        let meta = self.canonical.meta()?;
        let target = if let Some(sealed) = meta.sealed_event_id {
            sealed
        } else if meta.next_event_id > 0 {
            meta.next_event_id - 1
        } else {
            return Ok(ProjectorStats::empty());
        };

        // Check if caught up (but not if cursor is u64::MAX which means no events processed)
        if cursor != u64::MAX && cursor >= target {
            return Ok(ProjectorStats::empty());
        }

        // Determine batch size
        // Note: cursor of u64::MAX indicates -1 was cast from i64 (no events processed yet)
        let from = if cursor == u64::MAX { 0 } else { cursor + 1 };
        let to = std::cmp::min(target + 1, from + self.config.batch_events_max as u64);

        // Fetch events
        let mut events = Vec::new();
        let mut total_bytes = 0;
        let mut iter = self.canonical.iter_events(from, Some(to))?;

        while let Some((id, bytes)) = iter.next()? {
            total_bytes += bytes.len();
            events.push((id, bytes));

            // Check byte limit
            if total_bytes >= self.config.batch_bytes_max {
                break;
            }

            // Check latency limit
            if start.elapsed().as_millis() > self.config.max_apply_latency_ms as u128 {
                break;
            }
        }

        if events.is_empty() {
            return Ok(ProjectorStats::empty());
        }

        // Apply events in a transaction
        let mut txn = self.projection.begin_txn()?;
        txn.apply_batch(&events)?;

        let last_id = events.last().unwrap().0;
        Box::new(txn).commit(last_id)?;

        Ok(ProjectorStats {
            events_applied: events.len(),
            bytes_processed: total_bytes,
            duration: start.elapsed(),
            new_cursor: last_id,
        })
    }

    /// Run the projector continuously until shutdown.
    ///
    /// When an `event_notify` handle is set (via [`with_event_notify`]),
    /// the projector awaits the notification instead of polling, giving
    /// near-zero-latency projection with zero CPU waste when idle.
    /// Falls back to `poll_interval_ms` sleep if no notifier is present.
    pub async fn run_continuous(&self) -> Result<()> {
        while !self.shutdown.load(Ordering::SeqCst) {
            match self.run_once() {
                Ok(stats) => {
                    if stats.events_applied == 0 {
                        // Caught up -- wait for new events
                        if let Some(notify) = &self.event_notify {
                            // Push-based: await notification from event log
                            // Use tokio::select! so we also wake on shutdown
                            tokio::select! {
                                _ = notify.notified() => {}
                                _ = tokio::time::sleep(Duration::from_millis(self.config.poll_interval_ms)) => {}
                            }
                        } else {
                            // Legacy polling fallback
                            tokio::time::sleep(Duration::from_millis(self.config.poll_interval_ms))
                                .await;
                        }
                    } else {
                        tracing::debug!(
                            "Applied {} events, {} bytes in {:?}",
                            stats.events_applied,
                            stats.bytes_processed,
                            stats.duration
                        );
                    }
                }
                Err(e) => {
                    tracing::error!("Projector error: {}", e);
                    // Back off on error
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        Ok(())
    }

    /// Signal graceful shutdown
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Check lag (events behind)
    pub fn get_lag(&self) -> Result<u64> {
        let cursor = self.projection.get_cursor()?;
        let meta = self.canonical.meta()?;

        let tip = if let Some(sealed) = meta.sealed_event_id {
            sealed
        } else if meta.next_event_id > 0 {
            meta.next_event_id - 1
        } else {
            return Ok(0);
        };

        // Handle cursor = u64::MAX (represents -1, no events processed)
        if cursor == u64::MAX {
            return Ok(tip + 1);
        }

        Ok(tip.saturating_sub(cursor))
    }
}

#[derive(Debug, Clone)]
pub struct ProjectorStats {
    pub events_applied: usize,
    pub bytes_processed: usize,
    pub duration: Duration,
    pub new_cursor: EventId,
}

impl ProjectorStats {
    fn empty() -> Self {
        Self {
            events_applied: 0,
            bytes_processed: 0,
            duration: Duration::from_secs(0),
            new_cursor: 0,
        }
    }
}
