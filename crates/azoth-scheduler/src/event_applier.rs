//! Event applier for scheduler events.
//!
//! This component processes SchedulerEvents from the canonical log
//! and applies them to the SQLite projection. It runs as a separate
//! loop from the scheduler itself to avoid blocking.

use crate::error::Result;
use crate::events::SchedulerEvent;
use crate::projection::ScheduleProjection;
use azoth_core::traits::CanonicalStore;
use azoth_core::types::EventId;
use rusqlite::Connection;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

/// Event applier that processes SchedulerEvents and updates the projection.
///
/// This runs in a separate loop from the scheduler to avoid blocking
/// SQLite operations.
pub struct SchedulerEventApplier<C: CanonicalStore> {
    canonical: Arc<C>,
    projection: ScheduleProjection,
    shutdown: Arc<AtomicBool>,
    cursor: EventId,
}

impl<C: CanonicalStore> SchedulerEventApplier<C> {
    /// Create a new event applier.
    pub fn new(canonical: Arc<C>, conn: Arc<Connection>) -> Result<Self> {
        let projection = ScheduleProjection::new(conn);
        projection.init_schema()?;

        Ok(Self {
            canonical,
            projection,
            shutdown: Arc::new(AtomicBool::new(false)),
            cursor: u64::MAX, // Start before first event
        })
    }

    /// Run the event applier loop.
    ///
    /// This continuously polls for new scheduler events and applies them
    /// to the projection.
    pub async fn run(&mut self) -> Result<()> {
        info!("Scheduler event applier starting");

        while !self.shutdown.load(Ordering::SeqCst) {
            match self.run_once() {
                Ok(applied) => {
                    if applied == 0 {
                        // Caught up, sleep briefly
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    } else {
                        debug!("Applied {} scheduler events", applied);
                    }
                }
                Err(e) => {
                    error!("Event applier error: {}", e);
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        info!("Scheduler event applier stopped");
        Ok(())
    }

    /// Process one batch of events.
    pub fn run_once(&mut self) -> Result<usize> {
        // Get current tip
        let meta = self.canonical.meta()?;

        let tip = if let Some(sealed) = meta.sealed_event_id {
            sealed
        } else if meta.next_event_id > 0 {
            meta.next_event_id - 1
        } else {
            return Ok(0);
        };

        // Check if caught up (cursor of u64::MAX means we haven't processed any events yet)
        if self.cursor != u64::MAX && self.cursor >= tip {
            return Ok(0);
        }

        // Fetch next batch
        let from = if self.cursor == u64::MAX {
            0
        } else {
            self.cursor + 1
        };
        let to = std::cmp::min(tip + 1, from + 100); // Batch of 100

        let mut iter = self.canonical.iter_events(from, Some(to))?;
        let mut count = 0;

        while let Some((id, bytes)) = iter.next()? {
            // Try to deserialize as SchedulerEvent
            if let Ok(event) = serde_json::from_slice::<SchedulerEvent>(&bytes) {
                self.projection.apply_event(&event)?;
                count += 1;
            }

            self.cursor = id;
        }

        Ok(count)
    }

    /// Signal graceful shutdown.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::SeqCst);
    }

    /// Get the current cursor position.
    pub fn cursor(&self) -> EventId {
        self.cursor
    }
}
