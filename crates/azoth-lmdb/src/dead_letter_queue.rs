//! Dead Letter Queue (DLQ) for failed event log writes.
//!
//! When the two-phase commit succeeds for LMDB state (phase 1) but fails
//! to write events to the file log (phase 2), events are persisted here
//! so they can be recovered and replayed later.
//!
//! Format: Each entry is a JSON line containing the event metadata and
//! base64-encoded event data. This is intentionally simple and human-readable
//! for operational debugging.

use azoth_core::{
    error::{AzothError, Result},
    types::EventId,
};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// A dead letter queue entry representing a failed event log write.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct DlqEntry {
    /// The pre-allocated event ID from LMDB metadata.
    pub event_id: EventId,
    /// Base64-encoded event data.
    pub event_data_b64: String,
    /// ISO 8601 timestamp when the entry was written to the DLQ.
    pub timestamp: String,
    /// The original error message from the failed event log write.
    pub error: String,
}

/// Dead Letter Queue for persisting events that failed to write to the event log.
///
/// Thread-safe: uses a parking_lot mutex around the file handle to allow
/// concurrent callers. In practice, only one write txn commits at a time
/// (LMDB single-writer), but this is defensive.
pub struct DeadLetterQueue {
    path: PathBuf,
    writer: parking_lot::Mutex<Option<File>>,
}

impl DeadLetterQueue {
    /// Open or create the DLQ file at `dir/dead_letter_queue.jsonl`.
    ///
    /// Creates the parent directory if it does not exist.
    pub fn open(dir: &Path) -> Result<Arc<Self>> {
        fs::create_dir_all(dir)?;
        let path = dir.join("dead_letter_queue.jsonl");

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                AzothError::Io(std::io::Error::new(
                    e.kind(),
                    format!("Failed to open DLQ at {}: {}", path.display(), e),
                ))
            })?;

        tracing::info!("Dead letter queue opened at {}", path.display());

        Ok(Arc::new(Self {
            path,
            writer: parking_lot::Mutex::new(Some(file)),
        }))
    }

    /// Write a batch of failed events to the DLQ.
    ///
    /// Each event is written as a separate JSON line and flushed with fsync
    /// to ensure durability. If the DLQ itself fails to write, we log a
    /// critical error — this is the last resort.
    pub fn write_batch(
        &self,
        first_event_id: EventId,
        events: &[Vec<u8>],
        original_error: &str,
    ) -> Result<()> {
        use base64::Engine;

        let mut guard = self.writer.lock();
        let file = guard.as_mut().ok_or_else(|| {
            AzothError::InvalidState("DLQ file handle is closed".into())
        })?;

        let timestamp = chrono::Utc::now().to_rfc3339();

        for (i, event_data) in events.iter().enumerate() {
            let entry = DlqEntry {
                event_id: first_event_id + i as u64,
                event_data_b64: base64::engine::general_purpose::STANDARD.encode(event_data),
                timestamp: timestamp.clone(),
                error: original_error.to_string(),
            };

            let json = serde_json::to_string(&entry).map_err(|e| {
                AzothError::Serialization(format!("Failed to serialize DLQ entry: {}", e))
            })?;

            writeln!(file, "{}", json).map_err(|e| {
                AzothError::Io(std::io::Error::new(
                    e.kind(),
                    format!("Failed to write DLQ entry for event {}: {}", entry.event_id, e),
                ))
            })?;
        }

        // Ensure DLQ entries are durably persisted (this is our safety net)
        file.sync_all().map_err(|e| {
            AzothError::Io(std::io::Error::new(
                e.kind(),
                format!("Failed to fsync DLQ: {}", e),
            ))
        })?;

        tracing::error!(
            first_event_id = first_event_id,
            count = events.len(),
            dlq_path = %self.path.display(),
            "Events written to dead letter queue (event log write failed)"
        );

        Ok(())
    }

    /// Read all DLQ entries (for recovery/replay tooling).
    pub fn read_all(&self) -> Result<Vec<DlqEntry>> {
        let file = File::open(&self.path).map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                return AzothError::NotFound(format!("DLQ file not found: {}", self.path.display()));
            }
            AzothError::Io(e)
        })?;

        let reader = BufReader::new(file);
        let mut entries = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: DlqEntry = serde_json::from_str(&line).map_err(|e| {
                AzothError::Serialization(format!("Failed to deserialize DLQ entry: {}", e))
            })?;
            entries.push(entry);
        }

        Ok(entries)
    }

    /// Returns the number of entries currently in the DLQ.
    /// Used for health checks to detect canonical-vs-event-log drift.
    pub fn entry_count(&self) -> Result<usize> {
        match File::open(&self.path) {
            Ok(file) => {
                let reader = BufReader::new(file);
                Ok(reader.lines().count())
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(AzothError::Io(e)),
        }
    }

    /// Path to the DLQ file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;

    fn temp_dlq() -> (Arc<DeadLetterQueue>, tempfile::TempDir) {
        let dir = tempfile::TempDir::new().unwrap();
        let dlq = DeadLetterQueue::open(dir.path()).unwrap();
        (dlq, dir)
    }

    #[test]
    fn test_open_creates_file() {
        let (dlq, _dir) = temp_dlq();
        assert!(dlq.path().exists());
    }

    #[test]
    fn test_empty_dlq_has_zero_entries() {
        let (dlq, _dir) = temp_dlq();
        assert_eq!(dlq.entry_count().unwrap(), 0);
    }

    #[test]
    fn test_write_and_read_single_event() {
        let (dlq, _dir) = temp_dlq();
        let event_data = b"hello world".to_vec();

        dlq.write_batch(42, &[event_data.clone()], "test error")
            .unwrap();

        let entries = dlq.read_all().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].event_id, 42);
        assert_eq!(entries[0].error, "test error");

        // Verify base64 round-trip
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&entries[0].event_data_b64)
            .unwrap();
        assert_eq!(decoded, event_data);
    }

    #[test]
    fn test_write_batch_multiple_events() {
        let (dlq, _dir) = temp_dlq();
        let events: Vec<Vec<u8>> = vec![
            b"event_a".to_vec(),
            b"event_b".to_vec(),
            b"event_c".to_vec(),
        ];

        dlq.write_batch(100, &events, "disk full").unwrap();

        let entries = dlq.read_all().unwrap();
        assert_eq!(entries.len(), 3);

        // Event IDs should be sequential starting from first_event_id
        assert_eq!(entries[0].event_id, 100);
        assert_eq!(entries[1].event_id, 101);
        assert_eq!(entries[2].event_id, 102);

        // All entries share the same error message
        for entry in &entries {
            assert_eq!(entry.error, "disk full");
        }

        assert_eq!(dlq.entry_count().unwrap(), 3);
    }

    #[test]
    fn test_multiple_write_batches_append() {
        let (dlq, _dir) = temp_dlq();

        dlq.write_batch(10, &[b"first".to_vec()], "err1").unwrap();
        dlq.write_batch(20, &[b"second".to_vec(), b"third".to_vec()], "err2")
            .unwrap();

        let entries = dlq.read_all().unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].event_id, 10);
        assert_eq!(entries[1].event_id, 20);
        assert_eq!(entries[2].event_id, 21);
        assert_eq!(dlq.entry_count().unwrap(), 3);
    }

    #[test]
    fn test_dlq_survives_reopen() {
        let dir = tempfile::TempDir::new().unwrap();

        // Write some entries
        {
            let dlq = DeadLetterQueue::open(dir.path()).unwrap();
            dlq.write_batch(1, &[b"persistent".to_vec()], "crash")
                .unwrap();
        }

        // Reopen and verify entries survived
        {
            let dlq = DeadLetterQueue::open(dir.path()).unwrap();
            let entries = dlq.read_all().unwrap();
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].event_id, 1);

            let decoded = base64::engine::general_purpose::STANDARD
                .decode(&entries[0].event_data_b64)
                .unwrap();
            assert_eq!(decoded, b"persistent");
        }
    }

    #[test]
    fn test_entry_count_without_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let dlq = DeadLetterQueue::open(dir.path()).unwrap();
        // File exists but is empty
        assert_eq!(dlq.entry_count().unwrap(), 0);
    }

    #[test]
    fn test_binary_event_data_roundtrip() {
        let (dlq, _dir) = temp_dlq();

        // Test with binary data that would break if not base64-encoded
        let binary_data: Vec<u8> = (0..=255).collect();
        dlq.write_batch(0, &[binary_data.clone()], "binary test")
            .unwrap();

        let entries = dlq.read_all().unwrap();
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(&entries[0].event_data_b64)
            .unwrap();
        assert_eq!(decoded, binary_data);
    }
}
