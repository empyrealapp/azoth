//! Retention policies for automatic event cleanup

use crate::error::Result;
use azoth::AzothDb;
use azoth_core::traits::canonical::{CanonicalReadTxn, CanonicalStore};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Retention policy for events in a stream
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RetentionPolicy {
    /// Keep all events (no cleanup)
    KeepAll,

    /// Keep events for N days
    KeepDays(u64),

    /// Keep last N events
    KeepCount(u64),
}

/// Statistics from a compaction operation
#[derive(Debug, Clone, Default)]
pub struct CompactionStats {
    /// Number of events deleted
    pub deleted: u64,

    /// Position of slowest consumer (events before this were not deleted)
    pub min_consumer_position: Option<u64>,

    /// Cutoff position based on policy
    pub policy_cutoff: u64,

    /// Actual cutoff used (min of policy cutoff and slowest consumer)
    pub actual_cutoff: u64,
}

impl CompactionStats {
    pub fn empty() -> Self {
        Self::default()
    }
}

/// Manager for retention policies and compaction
pub struct RetentionManager {
    db: Arc<AzothDb>,
}

impl RetentionManager {
    /// Create a new retention manager
    pub fn new(db: Arc<AzothDb>) -> Self {
        Self { db }
    }

    /// Set retention policy for a stream
    pub fn set_retention(&self, stream: &str, policy: RetentionPolicy) -> Result<()> {
        let key = format!("bus:stream:{}:retention", stream).into_bytes();
        let policy_bytes = serde_json::to_vec(&policy)?;

        azoth::Transaction::new(&self.db)
            .write_keys(vec![key.clone()])
            .execute(|ctx| {
                ctx.set(&key, &azoth::TypedValue::Bytes(policy_bytes))?;
                Ok(())
            })?;

        Ok(())
    }

    /// Get retention policy for a stream
    pub fn get_retention(&self, stream: &str) -> Result<RetentionPolicy> {
        let key = format!("bus:stream:{}:retention", stream).into_bytes();
        let txn = self.db.canonical().read_txn()?;

        match txn.get_state(&key)? {
            Some(bytes) => {
                let value = azoth::TypedValue::from_bytes(&bytes)?;
                let policy_bytes = match value {
                    azoth::TypedValue::Bytes(b) => b,
                    _ => {
                        return Err(crate::error::BusError::InvalidState(
                            "Retention policy must be bytes".into(),
                        ))
                    }
                };
                Ok(serde_json::from_slice(&policy_bytes)?)
            }
            None => Ok(RetentionPolicy::KeepAll), // Default: keep everything
        }
    }

    /// Find the minimum (slowest) consumer cursor position for a stream
    ///
    /// Returns None if there are no consumers, otherwise returns the position
    /// of the consumer that is furthest behind.
    fn find_min_consumer_cursor(&self, stream: &str) -> Result<Option<u64>> {
        // Use EventBus to list consumers instead of scanning LMDB directly
        // This avoids LMDB cursor iteration issues
        let bus = crate::EventBus::new(self.db.clone());
        let consumers = bus.list_consumers(stream)?;

        if consumers.is_empty() {
            return Ok(None);
        }

        // Find the slowest consumer (minimum cursor position)
        // Position is the count of events processed, so we need to convert back to cursor
        // cursor = position - 1 (last acked event ID)
        let min_position = consumers.iter().map(|c| c.position).min().unwrap();

        // If position is 0, no events have been acked, so return None
        if min_position == 0 {
            return Ok(None);
        }

        // cursor = last acked event ID = position - 1
        Ok(Some(min_position - 1))
    }

    /// Compact a stream by deleting old events according to retention policy
    ///
    /// This will:
    /// 1. Calculate cutoff based on retention policy
    /// 2. Find slowest consumer
    /// 3. Use the minimum of policy cutoff and slowest consumer
    /// 4. Delete events before that cutoff
    ///
    /// Events still needed by active consumers are never deleted.
    pub fn compact(&self, stream: &str) -> Result<CompactionStats> {
        let policy = self.get_retention(stream)?;

        // Calculate cutoff based on policy
        let policy_cutoff = match policy {
            RetentionPolicy::KeepAll => {
                return Ok(CompactionStats::empty());
            }
            RetentionPolicy::KeepDays(_days) => {
                // TODO: Implement time-based retention
                // Would need to store event timestamps or scan events to find cutoff
                // For now, we skip time-based retention
                return Ok(CompactionStats::empty());
            }
            RetentionPolicy::KeepCount(max) => {
                let meta = self.db.canonical().meta()?;
                let head = meta.next_event_id;
                head.saturating_sub(max)
            }
        };

        // Find slowest consumer
        let min_consumer_position = self.find_min_consumer_cursor(stream)?;

        // Calculate safe cutoff (never delete past slowest consumer)
        let actual_cutoff = match min_consumer_position {
            Some(min_pos) => policy_cutoff.min(min_pos),
            None => policy_cutoff, // No consumers, use policy cutoff
        };

        // Delete events before cutoff (if any)
        let deleted = if actual_cutoff > 0 {
            // Note: delete_range is exclusive on the end, so we delete [0, actual_cutoff)
            // This preserves event at actual_cutoff
            // TODO: Need to check if event_log has delete_range method
            // For now, we'll return 0 as we can't actually delete
            0
        } else {
            0
        };

        Ok(CompactionStats {
            deleted,
            min_consumer_position,
            policy_cutoff,
            actual_cutoff,
        })
    }

    /// Run compaction on all streams with retention policies
    pub fn compact_all(&self) -> Result<Vec<(String, CompactionStats)>> {
        let prefix = b"bus:stream:";

        let mut results = Vec::new();
        let mut iter = self.db.canonical().scan_prefix(prefix)?;

        while let Some((key, _value)) = iter.next()? {
            let key_str = String::from_utf8_lossy(&key);

            // Parse key: bus:stream:{name}:retention
            if key_str.ends_with(":retention") {
                let parts: Vec<&str> = key_str.split(':').collect();
                if parts.len() >= 3 {
                    let stream = parts[2];
                    let stats = self.compact(stream)?;
                    results.push((stream.to_string(), stats));
                }
            }
        }

        Ok(results)
    }

    /// Run continuous compaction in the background
    ///
    /// This will compact all streams periodically according to the interval.
    pub async fn run_continuous(self: Arc<Self>, interval: Duration) {
        let mut ticker = tokio::time::interval(interval);

        loop {
            ticker.tick().await;

            match self.compact_all() {
                Ok(results) => {
                    for (stream, stats) in results {
                        if stats.deleted > 0 {
                            tracing::info!(
                                stream = %stream,
                                deleted = stats.deleted,
                                actual_cutoff = stats.actual_cutoff,
                                "Compacted stream"
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = ?e, "Compaction failed");
                }
            }
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

    fn publish_events(db: &AzothDb, stream: &str, count: usize) -> Result<()> {
        Transaction::new(db).execute(|ctx| {
            for i in 0..count {
                let event_type = format!("{}:event{}", stream, i);
                ctx.log(&event_type, &format!("data{}", i))?;
            }
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn test_retention_policy_storage() {
        let (db, _temp) = test_db();
        let mgr = RetentionManager::new(db);

        // Set policy
        mgr.set_retention("test", RetentionPolicy::KeepDays(7))
            .unwrap();

        // Get policy
        let policy = mgr.get_retention("test").unwrap();
        assert_eq!(policy, RetentionPolicy::KeepDays(7));
    }

    #[test]
    fn test_retention_default_keep_all() {
        let (db, _temp) = test_db();
        let mgr = RetentionManager::new(db);

        // No policy set, should default to KeepAll
        let policy = mgr.get_retention("nonexistent").unwrap();
        assert_eq!(policy, RetentionPolicy::KeepAll);
    }

    #[test]
    fn test_find_min_consumer_cursor_no_consumers() {
        let (db, _temp) = test_db();
        let mgr = RetentionManager::new(db.clone());

        // No consumers - should return None
        let min = mgr.find_min_consumer_cursor("test").unwrap();
        assert_eq!(min, None);
    }

    #[test]
    fn test_find_min_consumer_cursor_with_consumers() {
        let (db, _temp) = test_db();
        let mgr = RetentionManager::new(db.clone());

        // Create consumers with different positions
        publish_events(&db, "test", 10).unwrap();

        // Use the bus to create consumers
        {
            let bus = crate::EventBus::new(db.clone());
            let mut c1 = bus.subscribe("test", "c1").unwrap();
            let mut c2 = bus.subscribe("test", "c2").unwrap();

            // c1 acks 5 events, c2 acks 3 events
            for _ in 0..5 {
                if let Some(event) = c1.next().unwrap() {
                    c1.ack(event.id).unwrap();
                }
            }

            for _ in 0..3 {
                if let Some(event) = c2.next().unwrap() {
                    c2.ack(event.id).unwrap();
                }
            }
        } // Drop consumers here before scanning

        // Give a moment for all writes to complete
        std::thread::sleep(std::time::Duration::from_millis(10));

        // Slowest consumer is c2 at position 2 (acked events 0, 1, 2)
        let min = mgr.find_min_consumer_cursor("test").unwrap();
        assert_eq!(min, Some(2));
    }

    #[test]
    fn test_compact_keep_count() {
        let (db, _temp) = test_db();
        let mgr = RetentionManager::new(db.clone());

        publish_events(&db, "test", 100).unwrap();

        // Set retention to keep last 50 events
        mgr.set_retention("test", RetentionPolicy::KeepCount(50))
            .unwrap();

        let stats = mgr.compact("test").unwrap();

        // Should want to delete events 0-49 (policy cutoff = 100 - 50 = 50)
        assert_eq!(stats.policy_cutoff, 50);
        assert_eq!(stats.actual_cutoff, 50); // No consumers, so use policy cutoff
    }

    #[test]
    fn test_compact_respects_slow_consumers() {
        let (db, _temp) = test_db();
        let mgr = RetentionManager::new(db.clone());

        publish_events(&db, "test", 100).unwrap();

        // Create a slow consumer at position 60
        let bus = crate::EventBus::new(db.clone());
        let mut consumer = bus.subscribe("test", "slow").unwrap();

        for _ in 0..61 {
            if let Some(event) = consumer.next().unwrap() {
                consumer.ack(event.id).unwrap();
            }
        }

        // Set retention to keep last 30 events (would delete up to event 70)
        mgr.set_retention("test", RetentionPolicy::KeepCount(30))
            .unwrap();

        let stats = mgr.compact("test").unwrap();

        // Policy wants to delete up to 70, but slowest consumer is at 60
        assert_eq!(stats.policy_cutoff, 70);
        assert_eq!(stats.min_consumer_position, Some(60));
        assert_eq!(stats.actual_cutoff, 60); // Should stop at consumer position
    }

    #[test]
    fn test_compact_all() {
        let (db, _temp) = test_db();
        let mgr = RetentionManager::new(db.clone());

        // Note: All events go into the same event log
        // stream1 gets events 0-49, stream2 gets events 50-79
        publish_events(&db, "stream1", 50).unwrap();
        publish_events(&db, "stream2", 30).unwrap();

        // Total events: 80 (next_event_id = 80)
        mgr.set_retention("stream1", RetentionPolicy::KeepCount(20))
            .unwrap();
        mgr.set_retention("stream2", RetentionPolicy::KeepCount(10))
            .unwrap();

        let results = mgr.compact_all().unwrap();

        assert_eq!(results.len(), 2);

        // Find results by stream name
        let stream1_stats = results
            .iter()
            .find(|(name, _)| name == "stream1")
            .map(|(_, stats)| stats)
            .unwrap();
        let stream2_stats = results
            .iter()
            .find(|(name, _)| name == "stream2")
            .map(|(_, stats)| stats)
            .unwrap();

        // KeepCount(20) with 80 total events = cutoff at 60
        assert_eq!(stream1_stats.policy_cutoff, 60);
        // KeepCount(10) with 80 total events = cutoff at 70
        assert_eq!(stream2_stats.policy_cutoff, 70);
    }
}
