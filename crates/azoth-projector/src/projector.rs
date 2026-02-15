use azoth_core::{
    error::Result,
    traits::{CanonicalStore, ProjectionStore, ProjectionTxn},
    types::EventId,
    ProjectorConfig,
};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
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
    /// Running count of event ID gaps detected across all `run_once()` calls.
    /// Used for operational monitoring / health checks.
    total_gaps_detected: Arc<AtomicU64>,
    /// Running count of individual missing event IDs across all gaps.
    total_events_skipped: Arc<AtomicU64>,
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
            total_gaps_detected: Arc::new(AtomicU64::new(0)),
            total_events_skipped: Arc::new(AtomicU64::new(0)),
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

    /// Run one iteration of the projector loop.
    ///
    /// Fetches a batch of events from the canonical store and applies them
    /// to the projection. Detects and tolerates event ID gaps: if the event
    /// log is missing events (e.g. due to DLQ fallback), the projector logs
    /// a warning and advances past the gap instead of blocking.
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

        // Detect event ID gaps (events lost to DLQ)
        let (gaps_detected, events_skipped) = detect_gaps(from, &events);

        if gaps_detected > 0 {
            self.total_gaps_detected
                .fetch_add(gaps_detected, Ordering::Relaxed);
            self.total_events_skipped
                .fetch_add(events_skipped, Ordering::Relaxed);
        }

        // Apply events in a transaction (only events we actually have)
        let mut txn = self.projection.begin_txn()?;
        txn.apply_batch(&events)?;

        let last_id = events.last().unwrap().0;
        Box::new(txn).commit(last_id)?;

        Ok(ProjectorStats {
            events_applied: events.len(),
            bytes_processed: total_bytes,
            duration: start.elapsed(),
            new_cursor: last_id,
            gaps_detected,
            events_skipped,
        })
    }

    /// Run the projector continuously until shutdown.
    ///
    /// When an `event_notify` handle is set (via [`Self::with_event_notify`]),
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
                    } else if stats.gaps_detected > 0 {
                        tracing::warn!(
                            events = stats.events_applied,
                            bytes = stats.bytes_processed,
                            gaps = stats.gaps_detected,
                            skipped = stats.events_skipped,
                            elapsed = ?stats.duration,
                            "Applied events with gaps (events may be in DLQ)"
                        );
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

    /// Returns the total number of event ID gaps detected across all iterations.
    pub fn total_gaps_detected(&self) -> u64 {
        self.total_gaps_detected.load(Ordering::Relaxed)
    }

    /// Returns the total number of individual event IDs that were skipped due to gaps.
    pub fn total_events_skipped(&self) -> u64 {
        self.total_events_skipped.load(Ordering::Relaxed)
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
    /// Number of event ID gaps detected in this batch.
    pub gaps_detected: u64,
    /// Number of individual event IDs that were skipped due to gaps.
    pub events_skipped: u64,
}

impl ProjectorStats {
    fn empty() -> Self {
        Self {
            events_applied: 0,
            bytes_processed: 0,
            duration: Duration::from_secs(0),
            new_cursor: 0,
            gaps_detected: 0,
            events_skipped: 0,
        }
    }
}

/// Detect event ID gaps in a batch of events.
///
/// Checks two types of gaps:
/// 1. Between the expected first event ID and the actual first event
/// 2. Between consecutive events within the batch
///
/// Returns `(gaps_detected, events_skipped)`.
fn detect_gaps(expected_first: EventId, events: &[(EventId, Vec<u8>)]) -> (u64, u64) {
    if events.is_empty() {
        return (0, 0);
    }

    let mut gaps_detected: u64 = 0;
    let mut events_skipped: u64 = 0;

    // Check gap between cursor and first event
    let actual_first = events[0].0;
    if actual_first > expected_first {
        let missing = actual_first - expected_first;
        gaps_detected += 1;
        events_skipped += missing;
        tracing::warn!(
            expected_id = expected_first,
            actual_id = actual_first,
            missing_count = missing,
            "Projector: event gap detected between cursor and first event \
             (events {}-{} missing, likely in DLQ). Advancing past gap.",
            expected_first,
            actual_first - 1,
        );
    }

    // Check gaps within the batch
    for window in events.windows(2) {
        let prev_id = window[0].0;
        let curr_id = window[1].0;
        let expected = prev_id + 1;
        if curr_id > expected {
            let missing = curr_id - expected;
            gaps_detected += 1;
            events_skipped += missing;
            tracing::warn!(
                expected_id = expected,
                actual_id = curr_id,
                missing_count = missing,
                "Projector: event gap detected within batch \
                 (events {}-{} missing, likely in DLQ). Skipping gap.",
                expected,
                curr_id - 1,
            );
        }
    }

    (gaps_detected, events_skipped)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(id: EventId) -> (EventId, Vec<u8>) {
        (id, vec![id as u8])
    }

    #[test]
    fn test_no_gaps_sequential() {
        let events = vec![event(5), event(6), event(7)];
        let (gaps, skipped) = detect_gaps(5, &events);
        assert_eq!(gaps, 0);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_gap_before_first_event() {
        // Expected 5, but first available event is 8 (5,6,7 missing)
        let events = vec![event(8), event(9), event(10)];
        let (gaps, skipped) = detect_gaps(5, &events);
        assert_eq!(gaps, 1);
        assert_eq!(skipped, 3); // events 5, 6, 7
    }

    #[test]
    fn test_gap_within_batch() {
        // 5,6 present, 7,8 missing, 9 present
        let events = vec![event(5), event(6), event(9)];
        let (gaps, skipped) = detect_gaps(5, &events);
        assert_eq!(gaps, 1);
        assert_eq!(skipped, 2); // events 7, 8
    }

    #[test]
    fn test_multiple_gaps() {
        // Gap before first (0,1 missing), gap mid-batch (4 missing), gap again (7 missing)
        let events = vec![event(2), event(3), event(5), event(8)];
        let (gaps, skipped) = detect_gaps(0, &events);
        assert_eq!(gaps, 3); // before first, between 3-5, between 5-8
        assert_eq!(skipped, 2 + 1 + 2); // 0-1 + 4 + 6-7
    }

    #[test]
    fn test_single_event_no_gap() {
        let events = vec![event(42)];
        let (gaps, skipped) = detect_gaps(42, &events);
        assert_eq!(gaps, 0);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_single_event_with_gap() {
        let events = vec![event(45)];
        let (gaps, skipped) = detect_gaps(42, &events);
        assert_eq!(gaps, 1);
        assert_eq!(skipped, 3); // events 42, 43, 44
    }

    #[test]
    fn test_empty_events() {
        let events: Vec<(EventId, Vec<u8>)> = vec![];
        let (gaps, skipped) = detect_gaps(0, &events);
        assert_eq!(gaps, 0);
        assert_eq!(skipped, 0);
    }

    #[test]
    fn test_first_event_matches_exactly() {
        let events = vec![event(0), event(1), event(2)];
        let (gaps, skipped) = detect_gaps(0, &events);
        assert_eq!(gaps, 0);
        assert_eq!(skipped, 0);
    }
}
