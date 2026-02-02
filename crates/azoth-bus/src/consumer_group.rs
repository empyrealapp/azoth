//! Consumer groups for load-balanced event consumption

use crate::error::Result;
use crate::filter::{Event, EventFilter, EventFilterTrait};
use azoth::prelude::anyhow;
use azoth::{AzothDb, Transaction, TypedValue};
use azoth_core::traits::canonical::CanonicalStore;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// A consumer group manages load-balanced consumption across multiple members
pub struct ConsumerGroup {
    db: Arc<AzothDb>,
    stream: String,
    group_name: String,
    filter: EventFilter,
    lease_duration: Duration,
}

/// Metadata about a claimed event
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Claim {
    member_id: String,
    claimed_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

/// An event that has been claimed by a group member
pub struct ClaimedEvent {
    pub event: Event,
    pub claim_id: String,
    pub lease_expires_at: DateTime<Utc>,
}

/// A member of a consumer group
pub struct GroupMember {
    db: Arc<AzothDb>,
    stream: String,
    group_name: String,
    member_id: String,
    filter: EventFilter,
    lease_duration: Duration,
}

impl ConsumerGroup {
    /// Create a new consumer group for a stream
    pub fn new(
        db: Arc<AzothDb>,
        stream: String,
        group_name: String,
        lease_duration: Duration,
    ) -> Self {
        // Auto-filter to stream prefix
        let filter = EventFilter::prefix(&format!("{}:", stream));

        Self {
            db,
            stream,
            group_name,
            filter,
            lease_duration,
        }
    }

    /// Add additional filtering
    pub fn with_filter(mut self, filter: EventFilter) -> Self {
        self.filter = self.filter.and(filter);
        self
    }

    /// Join the group as a member
    pub fn join(&self, member_id: &str) -> Result<GroupMember> {
        let member = GroupMember {
            db: self.db.clone(),
            stream: self.stream.clone(),
            group_name: self.group_name.clone(),
            member_id: member_id.to_string(),
            filter: self.filter.clone(),
            lease_duration: self.lease_duration,
        };

        // Register member
        let meta_key = format!(
            "bus:group:{}:{}:member:{}",
            self.stream, self.group_name, member_id
        )
        .into_bytes();

        let meta = serde_json::json!({
            "joined_at": Utc::now(),
            "member_id": member_id,
        });

        let meta_bytes = serde_json::to_vec(&meta)?;

        Transaction::new(&self.db)
            .write_keys(vec![meta_key.clone()])
            .execute(|ctx| {
                ctx.set(&meta_key, &TypedValue::Bytes(meta_bytes))?;
                Ok(())
            })?;

        Ok(member)
    }

    /// List all active members
    pub fn list_members(&self) -> Result<Vec<String>> {
        let prefix = format!("bus:group:{}:{}:member:", self.stream, self.group_name).into_bytes();

        let mut members = Vec::new();
        let mut iter = self.db.canonical().scan_prefix(&prefix)?;

        while let Some((key, _value)) = iter.next()? {
            let key_str = String::from_utf8_lossy(&key);
            let parts: Vec<&str> = key_str.split(':').collect();
            if parts.len() >= 6 {
                members.push(parts[5].to_string());
            }
        }

        Ok(members)
    }
}

impl GroupMember {
    fn claim_key_for(&self, event_id: u64) -> String {
        format!(
            "bus:group:{}:{}:claim:{}",
            self.stream, self.group_name, event_id
        )
    }

    fn group_cursor_key(&self) -> String {
        format!("bus:group:{}:{}:cursor", self.stream, self.group_name)
    }

    fn reclaim_list_key(&self) -> String {
        format!("bus:group:{}:{}:reclaim", self.stream, self.group_name)
    }

    /// Find next event matching filter, starting from event_id
    ///
    /// WORKAROUND: There's an LMDB cursor issue where calling iter_events multiple times
    /// doesn't work correctly. So we call it once with a larger limit.
    fn find_next_matching_event(
        &self,
        start_id: u64,
        max_search: u64,
    ) -> Result<Option<(u64, Event)>> {
        let mut iter = self
            .db
            .canonical()
            .iter_events(start_id, Some(max_search))?;

        while let Some((id, bytes)) = iter.next()? {
            let event = Event::decode(id, &bytes)?;
            if self.filter.matches(&event) {
                return Ok(Some((id, event)));
            }
        }

        Ok(None)
    }

    fn read_event(&self, event_id: u64) -> Result<Option<Event>> {
        // WORKAROUND: Use find_next_matching_event to avoid LMDB cursor issue
        // Search up to 10 events ahead in case there are non-matching events
        if let Some((id, event)) = self.find_next_matching_event(event_id, 10)? {
            if id == event_id {
                return Ok(Some(event));
            }
        }
        Ok(None)
    }

    fn create_claim(&self) -> Claim {
        let now = Utc::now();
        Claim {
            member_id: self.member_id.clone(),
            claimed_at: now,
            expires_at: now + chrono::Duration::from_std(self.lease_duration).unwrap(),
        }
    }

    /// Attempt to claim the next event for processing
    ///
    /// This checks the reclaim list first (for nacked/expired events),
    /// then advances the group cursor to claim fresh events.
    ///
    /// Returns None if the group is caught up.
    pub fn claim_next(&mut self) -> Result<Option<ClaimedEvent>> {
        let reclaim_key = self.reclaim_list_key().into_bytes();
        let cursor_key = self.group_cursor_key().into_bytes();

        // Read current state outside transaction
        let txn = self.db.canonical().read_only_txn()?;

        // Check reclaim list
        let reclaim_list: Vec<u64> = match txn.get_state(&reclaim_key)? {
            Some(bytes) => {
                let value = TypedValue::from_bytes(&bytes)?;
                match value {
                    TypedValue::Bytes(json_bytes) => serde_json::from_slice(&json_bytes)?,
                    _ => Vec::new(),
                }
            }
            None => Vec::new(),
        };

        // Get cursor
        let cursor = match txn.get_state(&cursor_key)? {
            Some(bytes) => {
                let value = TypedValue::from_bytes(&bytes)?;
                value.as_i64()? as u64
            }
            None => 0,
        };

        drop(txn); // Release read transaction

        // Priority: Fresh events first (make forward progress), then reclaim list
        let meta = self.db.canonical().meta()?;

        // Try to claim next fresh event
        if cursor < meta.next_event_id {
            // Search for next matching event (up to 100 events ahead)
            let max_search = 100;
            if let Some((event_id, event)) = self.find_next_matching_event(cursor, max_search)? {
                // Try to claim it
                let claim_key = self.claim_key_for(event_id).into_bytes();
                let mut claimed = false;

                let result = Transaction::new(&self.db)
                    .write_keys(vec![cursor_key.clone(), claim_key.clone()])
                    .execute(|ctx| {
                        // Check if already claimed
                        if ctx.exists(&claim_key)? {
                            return Ok(()); // Someone else claimed it
                        }

                        // Write claim
                        let claim = self.create_claim();
                        let claim_bytes = serde_json::to_vec(&claim)
                            .map_err(|e| anyhow::anyhow!("Failed to serialize: {}", e))?;
                        ctx.set(&claim_key, &TypedValue::Bytes(claim_bytes))?;

                        // Advance cursor to event_id + 1
                        ctx.set(&cursor_key, &TypedValue::I64((event_id + 1) as i64))?;

                        claimed = true;
                        Ok(())
                    });

                if result.is_ok() && claimed {
                    let claim = self.create_claim();
                    return Ok(Some(ClaimedEvent {
                        event,
                        claim_id: String::from_utf8_lossy(&claim_key).to_string(),
                        lease_expires_at: claim.expires_at,
                    }));
                }
            }
        }

        // No fresh events, try reclaim list
        if !reclaim_list.is_empty() {
            if let Some(&event_id) = reclaim_list.last() {
                if let Some(event) = self.read_event(event_id)? {
                    // Claim it in a transaction
                    let claim_key = self.claim_key_for(event_id).into_bytes();
                    let mut claimed = false;

                    Transaction::new(&self.db)
                        .write_keys(vec![reclaim_key.clone(), claim_key.clone()])
                        .execute(|ctx| {
                            // Check reclaim list again
                            let mut events: Vec<u64> =
                                if let Some(value) = ctx.get_opt(&reclaim_key)? {
                                    match value {
                                        TypedValue::Bytes(json_bytes) => {
                                            serde_json::from_slice(&json_bytes).map_err(|e| {
                                                anyhow::anyhow!("Failed to parse: {}", e)
                                            })?
                                        }
                                        _ => Vec::new(),
                                    }
                                } else {
                                    Vec::new()
                                };

                            // Pop the event if still there
                            if events.last() == Some(&event_id) {
                                events.pop();

                                // Write claim
                                let claim = self.create_claim();
                                let claim_bytes = serde_json::to_vec(&claim)
                                    .map_err(|e| anyhow::anyhow!("Failed to serialize: {}", e))?;
                                ctx.set(&claim_key, &TypedValue::Bytes(claim_bytes))?;

                                // Update reclaim list
                                let events_bytes = serde_json::to_vec(&events)
                                    .map_err(|e| anyhow::anyhow!("Failed to serialize: {}", e))?;
                                ctx.set(&reclaim_key, &TypedValue::Bytes(events_bytes))?;

                                claimed = true;
                            }

                            Ok(())
                        })?;

                    if claimed {
                        let claim = self.create_claim();
                        return Ok(Some(ClaimedEvent {
                            event,
                            claim_id: String::from_utf8_lossy(&claim_key).to_string(),
                            lease_expires_at: claim.expires_at,
                        }));
                    }
                }
            }
        }

        Ok(None) // Caught up and no reclaimed events
    }

    /// Release a claimed event
    ///
    /// If success=true, the event is marked as processed.
    /// If success=false (nack), the event is added to the reclaim list.
    pub fn release(&mut self, event_id: u64, success: bool) -> Result<()> {
        let reclaim_key = self.reclaim_list_key().into_bytes();
        let claim_key = self.claim_key_for(event_id).into_bytes();

        Transaction::new(&self.db)
            .write_keys(vec![reclaim_key.clone(), claim_key.clone()])
            .execute(|ctx| {
                // Delete claim
                ctx.delete(&claim_key)?;

                if !success {
                    // Nack - add to reclaim list
                    let mut events: Vec<u64> = if let Some(value) = ctx.get_opt(&reclaim_key)? {
                        if let TypedValue::Bytes(json_bytes) = value {
                            serde_json::from_slice(&json_bytes).map_err(|e| {
                                anyhow::anyhow!("Failed to parse reclaim list: {}", e)
                            })?
                        } else {
                            Vec::new()
                        }
                    } else {
                        Vec::new()
                    };

                    events.push(event_id);
                    let events_bytes = serde_json::to_vec(&events)
                        .map_err(|e| anyhow::anyhow!("Failed to serialize reclaim list: {}", e))?;
                    ctx.set(&reclaim_key, &TypedValue::Bytes(events_bytes))?;
                }

                Ok(())
            })?;

        Ok(())
    }

    /// Cleanup expired claims and add them to reclaim list
    ///
    /// This should be called periodically to reclaim events from members
    /// that failed to process them within the lease duration.
    pub fn cleanup_expired_claims(&self) -> Result<u64> {
        let claim_prefix =
            format!("bus:group:{}:{}:claim:", self.stream, self.group_name).into_bytes();
        let reclaim_key = self.reclaim_list_key().into_bytes();

        let mut expired = Vec::new();
        let now = Utc::now();

        // Find expired claims
        let mut iter = self.db.canonical().scan_prefix(&claim_prefix)?;
        while let Some((key, value)) = iter.next()? {
            let typed = TypedValue::from_bytes(&value)?;
            if let TypedValue::Bytes(json_bytes) = typed {
                if let Ok(claim) = serde_json::from_slice::<Claim>(&json_bytes) {
                    if claim.expires_at < now {
                        // Extract event_id from key: bus:group:stream:group:claim:ID
                        let key_str = String::from_utf8_lossy(&key);
                        let parts: Vec<&str> = key_str.split(':').collect();
                        if parts.len() >= 6 {
                            if let Ok(event_id) = parts[5].parse::<u64>() {
                                expired.push((key.clone(), event_id));
                            }
                        }
                    }
                }
            }
        }

        if expired.is_empty() {
            return Ok(0);
        }

        // Delete expired claims and add to reclaim list
        let mut write_keys: Vec<Vec<u8>> = expired.iter().map(|(k, _)| k.clone()).collect();
        write_keys.push(reclaim_key.clone());

        let count = expired.len() as u64;

        Transaction::new(&self.db)
            .write_keys(write_keys)
            .execute(|ctx| {
                // Load current reclaim list
                let mut reclaim_list: Vec<u64> = if let Some(value) = ctx.get_opt(&reclaim_key)? {
                    if let TypedValue::Bytes(json_bytes) = value {
                        serde_json::from_slice(&json_bytes)
                            .map_err(|e| anyhow::anyhow!("Failed to parse reclaim list: {}", e))?
                    } else {
                        Vec::new()
                    }
                } else {
                    Vec::new()
                };

                // Delete claims and add to reclaim list
                for (claim_key, event_id) in expired {
                    ctx.delete(&claim_key)?;
                    reclaim_list.push(event_id);
                }

                // Save updated reclaim list
                let reclaim_bytes = serde_json::to_vec(&reclaim_list)
                    .map_err(|e| anyhow::anyhow!("Failed to serialize reclaim list: {}", e))?;
                ctx.set(&reclaim_key, &TypedValue::Bytes(reclaim_bytes))?;

                Ok(())
            })?;

        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use azoth::Transaction as AzothTransaction;
    use tempfile::TempDir;

    fn test_db() -> (Arc<AzothDb>, TempDir) {
        let temp = TempDir::new().unwrap();
        let db = AzothDb::open(temp.path()).unwrap();
        (Arc::new(db), temp)
    }

    fn publish_events(db: &AzothDb, stream: &str, count: usize) -> Result<()> {
        AzothTransaction::new(db).execute(|ctx| {
            for i in 0..count {
                let event_type = format!("{}:event{}", stream, i);
                ctx.log(&event_type, &format!("data{}", i))?;
            }
            Ok(())
        })?;
        Ok(())
    }

    #[test]
    fn test_iter_events_basic() {
        let (db, _temp) = test_db();
        publish_events(&db, "test", 10).unwrap();

        // Check meta
        let meta = db.canonical().meta().unwrap();
        eprintln!("next_event_id: {}", meta.next_event_id);
        assert_eq!(meta.next_event_id, 10);

        // Iterate all events at once
        let mut iter = db.canonical().iter_events(0, Some(100)).unwrap();
        let mut count = 0;
        while let Some((id, bytes)) = iter.next().unwrap() {
            eprintln!("Event {}: {} bytes", id, bytes.len());
            count += 1;
        }
        eprintln!("Total events: {}", count);
        assert_eq!(count, 10);

        // Try individual reads
        for i in 0..10 {
            eprintln!("\nTrying to read event {}", i);
            let mut iter = db.canonical().iter_events(i, Some(1)).unwrap();
            let result = iter.next().unwrap();
            eprintln!("Result for event {}: {:?}", i, result.is_some());
            if let Some((id, _)) = result {
                eprintln!("Got event ID: {}", id);
            }
        }
    }

    #[test]
    fn test_consumer_group_creation() {
        let (db, _temp) = test_db();
        let group = ConsumerGroup::new(
            db.clone(),
            "test".to_string(),
            "workers".to_string(),
            Duration::from_secs(30),
        );

        let member = group.join("worker-1").unwrap();
        assert_eq!(member.member_id, "worker-1");
    }

    #[test]
    fn test_list_members() {
        let (db, _temp) = test_db();
        let group = ConsumerGroup::new(
            db.clone(),
            "test".to_string(),
            "workers".to_string(),
            Duration::from_secs(30),
        );

        group.join("worker-1").unwrap();
        group.join("worker-2").unwrap();

        let members = group.list_members().unwrap();
        assert_eq!(members.len(), 2);
        assert!(members.contains(&"worker-1".to_string()));
        assert!(members.contains(&"worker-2".to_string()));
    }

    #[test]
    fn test_claim_and_release() {
        let (db, _temp) = test_db();
        publish_events(&db, "test", 10).unwrap();

        let group = ConsumerGroup::new(
            db.clone(),
            "test".to_string(),
            "workers".to_string(),
            Duration::from_secs(30),
        );

        let mut member = group.join("worker-1").unwrap();

        // Claim an event
        let claimed = member.claim_next().unwrap();
        assert!(claimed.is_some());
        let event = claimed.unwrap();
        assert_eq!(event.event.id, 0);

        // Release successfully
        member.release(event.event.id, true).unwrap();

        // Claim next event
        let claimed = member.claim_next().unwrap();
        assert!(claimed.is_some());
        assert_eq!(claimed.unwrap().event.id, 1);
    }

    #[test]
    fn test_nack_and_reclaim() {
        let (db, _temp) = test_db();
        publish_events(&db, "test", 5).unwrap();

        let group = ConsumerGroup::new(
            db.clone(),
            "test".to_string(),
            "workers".to_string(),
            Duration::from_secs(30),
        );

        let mut member = group.join("worker-1").unwrap();

        // Claim and nack event 0
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 0);
        member.release(claimed.event.id, false).unwrap(); // Nack

        // Should make forward progress and claim event 1 (not reclaim 0 yet)
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 1);
        member.release(claimed.event.id, false).unwrap(); // Nack

        // Continue forward progress - claim event 2
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 2);
        member.release(claimed.event.id, true).unwrap(); // Success

        // Continue forward progress - claim event 3
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 3);
        member.release(claimed.event.id, true).unwrap(); // Success

        // Continue forward progress - claim event 4
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 4);
        member.release(claimed.event.id, true).unwrap(); // Success

        // Now we're caught up (cursor=5, next_event_id=5)
        // Should get event 1 from reclaim list (LIFO - last nacked)
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 1);
        member.release(claimed.event.id, true).unwrap(); // Success

        // Should get event 0 from reclaim list (first nacked)
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 0);
        member.release(claimed.event.id, true).unwrap(); // Success

        // Now fully caught up with no reclaimed events
        assert!(member.claim_next().unwrap().is_none());
    }

    #[test]
    fn test_sequential_claims() {
        let (db, _temp) = test_db();
        publish_events(&db, "test", 20).unwrap();

        let group = ConsumerGroup::new(
            db.clone(),
            "test".to_string(),
            "workers".to_string(),
            Duration::from_secs(30),
        );

        let mut worker = group.join("worker-1").unwrap();

        // Claim and release all 20 events
        for expected_id in 0..20 {
            let claimed = worker.claim_next().unwrap();
            assert!(claimed.is_some(), "Failed to claim event {}", expected_id);
            let claimed = claimed.unwrap();
            assert_eq!(claimed.event.id, expected_id);
            worker.release(claimed.event.id, true).unwrap();
        }

        // Should be caught up
        assert!(worker.claim_next().unwrap().is_none());
    }

    #[test]
    fn test_round_robin_claims() {
        let (db, _temp) = test_db();
        publish_events(&db, "test", 20).unwrap();

        let group = ConsumerGroup::new(
            db.clone(),
            "test".to_string(),
            "workers".to_string(),
            Duration::from_secs(30),
        );

        let mut w1 = group.join("worker-1").unwrap();
        let mut w2 = group.join("worker-2").unwrap();
        let mut w3 = group.join("worker-3").unwrap();

        // Simulate round-robin claiming like the example
        let mut total_claimed = 0;
        loop {
            let mut claimed_any = false;

            if let Some(claimed) = w1.claim_next().unwrap() {
                w1.release(claimed.event.id, true).unwrap();
                total_claimed += 1;
                claimed_any = true;
            }

            if let Some(claimed) = w2.claim_next().unwrap() {
                w2.release(claimed.event.id, true).unwrap();
                total_claimed += 1;
                claimed_any = true;
            }

            if let Some(claimed) = w3.claim_next().unwrap() {
                w3.release(claimed.event.id, true).unwrap();
                total_claimed += 1;
                claimed_any = true;
            }

            if !claimed_any {
                break;
            }
        }

        assert_eq!(total_claimed, 20, "Should have claimed all 20 events");
    }

    #[test]
    fn test_multiple_members_no_duplicate_claims() {
        let (db, _temp) = test_db();
        publish_events(&db, "test", 10).unwrap();

        let group = ConsumerGroup::new(
            db.clone(),
            "test".to_string(),
            "workers".to_string(),
            Duration::from_secs(30),
        );

        let mut m1 = group.join("worker-1").unwrap();
        let mut m2 = group.join("worker-2").unwrap();

        let mut claimed_ids = Vec::new();

        // Each member claims 5 events
        for _ in 0..5 {
            if let Some(claimed) = m1.claim_next().unwrap() {
                claimed_ids.push(claimed.event.id);
                m1.release(claimed.event.id, true).unwrap();
            }

            if let Some(claimed) = m2.claim_next().unwrap() {
                claimed_ids.push(claimed.event.id);
                m2.release(claimed.event.id, true).unwrap();
            }
        }

        // Should have claimed all 10 events
        assert_eq!(claimed_ids.len(), 10);

        // No duplicates
        claimed_ids.sort();
        claimed_ids.dedup();
        assert_eq!(claimed_ids.len(), 10);
    }

    #[test]
    fn test_expired_claim_cleanup() {
        let (db, _temp) = test_db();
        publish_events(&db, "test", 5).unwrap();

        let group = ConsumerGroup::new(
            db.clone(),
            "test".to_string(),
            "workers".to_string(),
            Duration::from_millis(1), // Very short lease
        );

        let mut member = group.join("worker-1").unwrap();

        // Claim an event
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 0);

        // Wait for lease to expire
        std::thread::sleep(Duration::from_millis(10));

        // Cleanup expired claims
        let cleaned = member.cleanup_expired_claims().unwrap();
        assert_eq!(cleaned, 1);

        // Should make forward progress and claim event 1 (cursor=1)
        let claimed = member.claim_next().unwrap().unwrap();
        assert_eq!(claimed.event.id, 1);
        member.release(claimed.event.id, true).unwrap();

        // Claim remaining fresh events (2, 3, 4)
        for expected_id in 2..5 {
            let claimed = member.claim_next().unwrap().unwrap();
            assert_eq!(claimed.event.id, expected_id);
            member.release(claimed.event.id, true).unwrap();
        }

        // Now caught up (cursor=5), should get event 0 from reclaim list
        let reclaimed = member.claim_next().unwrap().unwrap();
        assert_eq!(reclaimed.event.id, 0);
        member.release(reclaimed.event.id, true).unwrap();

        // Fully caught up
        assert!(member.claim_next().unwrap().is_none());
    }
}
