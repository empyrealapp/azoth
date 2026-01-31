# File-Based Event Log

## Overview

Azoth uses a high-performance file-based event log instead of storing events in LMDB. This provides 5-10x faster writes, automatic rotation, and easy archival to IPFS.

## Architecture

### Hybrid Storage

```
LmdbCanonicalStore
├── state_db (LMDB)           # KV state (transactional)
├── meta_db (LMDB)            # Metadata + EventId allocation
└── event_log (FileEventLog)  # Events (append-only files)
```

### Why File-Based?

**Before (LMDB for events):**
- Full ACID overhead for append-only data
- LMDB database bloat
- Slower writes (10-20k events/sec)
- Difficult to archive old events

**After (File-based log):**
- Sequential file I/O (50-200k events/sec)
- No ACID overhead
- Easy rotation and archival
- Reduced LMDB size

## File Format

### Directory Structure

```
event-log/
├── meta.json              # Metadata
├── events-00000000.log    # Log file 0
├── events-00000001.log    # Log file 1 (after rotation)
└── events-00000002.log    # Log file 2 (after rotation)
```

### Metadata (meta.json)

```json
{
  "next_event_id": 12345,
  "current_file_num": 2,
  "oldest_event_id": 0,
  "total_events": 12345
}
```

### Event Entry Format

Each event is stored as:

```
[event_id: u64 (big-endian)]
[size: u32 (big-endian)]
[data: bytes]
```

**Example:**
```
Event 0: [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]  # EventId = 0
         [0x00, 0x00, 0x00, 0x07]                          # Size = 7
         [0x65, 0x76, 0x65, 0x6E, 0x74, 0x20, 0x30]        # Data = "event 0"
```

### Why Big-Endian?

- Preserves sort order when comparing byte strings
- Consistent with LMDB EventId encoding
- Makes manual inspection easier

## EventLog Trait

```rust
pub trait EventLog: Send + Sync {
    /// Append event with pre-allocated EventId
    fn append_with_id(&self, event_id: EventId, event_bytes: &[u8]) -> Result<()>;

    /// Append batch with pre-allocated EventIds
    fn append_batch_with_ids(&self, first_event_id: EventId, events: &[Vec<u8>]) -> Result<()>;

    /// Iterate over events in a range
    fn iter_range(&self, start: EventId, end: Option<EventId>) -> Result<Box<dyn EventLogIterator>>;

    /// Get single event by ID
    fn get(&self, event_id: EventId) -> Result<Option<Vec<u8>>>;

    /// Delete events in range (for archival)
    fn delete_range(&self, start: EventId, end: EventId) -> Result<usize>;

    /// Rotate the current log file
    fn rotate(&self) -> Result<std::path::PathBuf>;

    /// Get the oldest event ID still in storage
    fn oldest_event_id(&self) -> Result<EventId>;

    /// Get the newest event ID in storage
    fn newest_event_id(&self) -> Result<EventId>;

    /// Sync all writes to disk
    fn sync(&self) -> Result<()>;

    /// Get statistics about the event log
    fn stats(&self) -> Result<EventLogStats>;
}
```

## Configuration

```rust
pub struct FileEventLogConfig {
    /// Base directory for event log files
    pub base_dir: PathBuf,  // Default: "./data/event-log"

    /// Maximum size before rotation
    pub max_file_size: u64,  // Default: 512MB

    /// Write buffer size
    pub write_buffer_size: usize,  // Default: 64KB
}
```

## EventId Synchronization

### The Challenge

Two sources of EventIds:
1. **LMDB metadata** (transactional, source of truth)
2. **FileEventLog** (needs EventIds to write)

### The Solution

**LMDB is the single source of truth:**

```rust
// Phase 1: Allocate EventIds from LMDB metadata
let first_event_id = allocate_event_ids_from_lmdb(count)?;

// Phase 2: Commit LMDB transaction (state + metadata)
lmdb_txn.commit()?;

// Phase 3: Write events to FileEventLog with pre-allocated IDs
event_log.append_batch_with_ids(first_event_id, &events)?;

// FileEventLog updates its internal counter to match
```

### Why Pre-Allocated IDs?

**Problem:**
- If FileEventLog allocated its own IDs, they could diverge from LMDB
- Concurrent transactions would cause EventId conflicts
- Crash recovery would be ambiguous

**Solution:**
- LMDB allocates EventIds transactionally
- FileEventLog receives pre-allocated IDs
- FileEventLog syncs its internal counter after write
- Crash recovery: LMDB metadata is authoritative

## Rotation

### Automatic Rotation

Rotation happens when:
```rust
current_file_size >= max_file_size
```

**Default:** 512MB per file

### Rotation Process

1. Flush current writer
2. Increment file number
3. Open new log file
4. Update metadata
5. Return path to rotated file

```rust
// Before rotation
event-log/
├── events-00000000.log (512MB)  ← Active
└── meta.json

// After rotation
event-log/
├── events-00000000.log (512MB)  ← Sealed
├── events-00000001.log (0MB)    ← Active
└── meta.json (updated)
```

### Manual Rotation

```rust
let rotated_path = event_log.rotate()?;
println!("Rotated: {}", rotated_path.display());
```

## Statistics

```rust
pub struct EventLogStats {
    /// Total number of events
    pub event_count: u64,

    /// Oldest event ID still in storage
    pub oldest_event_id: EventId,

    /// Newest event ID in storage
    pub newest_event_id: EventId,

    /// Total bytes used by event storage
    pub total_bytes: u64,

    /// Number of active log files
    pub file_count: usize,
}
```

### Usage

```rust
let stats = event_log.stats()?;
println!("Events: {}", stats.event_count);
println!("Files: {}", stats.file_count);
println!("Disk usage: {} MB", stats.total_bytes / 1024 / 1024);
```

## Performance

### Write Performance

| Operation | Rate | Latency (p50) |
|-----------|------|---------------|
| Single append | 50-100k/sec | < 1ms |
| Batch append (100 events) | 100-200k/sec | < 5ms |
| Batch append (1000 events) | 150-200k/sec | < 10ms |

### Read Performance

| Operation | Rate | Latency |
|-----------|------|---------|
| Sequential iteration | 500k-1M events/sec | N/A |
| Random get | 10-50k/sec | 100-500μs |

### Rotation Performance

| File Size | Rotation Time |
|-----------|---------------|
| 512MB | ~10ms |
| 1GB | ~20ms |
| 2GB | ~40ms |

## Integration with LMDB

### Transaction Flow

```rust
// In LmdbWriteTxn::commit()

// Phase 1: Allocate EventIds (transactional)
let first_id = self.allocate_event_ids(events.len())?;

// Phase 2: Commit LMDB (state + metadata)
let txn = self.txn.take().unwrap();
txn.commit()?;

// Phase 3: Write events to FileEventLog
if !self.pending_events.is_empty() {
    self.event_log.append_batch_with_ids(first_id, &self.pending_events)?;
}
```

### Error Handling

**If LMDB commit fails:**
- EventIds not allocated
- No events written
- Transaction fully rolled back

**If FileEventLog write fails:**
- LMDB commit succeeded (state + metadata updated)
- EventIds allocated but events not written
- **Critical error** - should rarely happen
- System should crash/alert

**Why is this acceptable?**
- File writes are extremely reliable
- Failure indicates disk/system failure
- Better to fail fast than continue with inconsistent state

## Archival (Planned)

### IPFS Upload

```rust
pub async fn archive_old_logs(&self, max_age_days: u32) -> Result<Vec<ArchiveInfo>> {
    let cutoff = Utc::now() - Duration::days(max_age_days as i64);

    let old_files = self.find_files_before(cutoff)?;

    let mut archives = Vec::new();
    for file_path in old_files {
        // Optional: Encrypt
        let encrypted = encrypt_file(&file_path, &encryption_key)?;

        // Upload to IPFS
        let cid = ipfs_client.add(&encrypted).await?;

        // Track in SQLite
        projection.record_archive(&ArchiveRecord {
            log_file: file_path.clone(),
            first_event_id: parse_first_event_id(&file_path)?,
            last_event_id: parse_last_event_id(&file_path)?,
            ipfs_cid: cid.clone(),
            archived_at: Utc::now(),
            encrypted: encryption_key.is_some(),
        })?;

        // Delete local file
        std::fs::remove_file(&file_path)?;

        archives.push(ArchiveInfo { path: file_path, cid });
    }

    Ok(archives)
}
```

### Archive Tracking (SQLite)

```sql
CREATE TABLE event_archives (
    id INTEGER PRIMARY KEY,
    log_file_name TEXT NOT NULL,
    first_event_id INTEGER NOT NULL,
    last_event_id INTEGER NOT NULL,
    ipfs_cid TEXT NOT NULL,
    archived_at TEXT NOT NULL,
    encrypted BOOLEAN NOT NULL DEFAULT 0
);

CREATE INDEX idx_event_archives_range
ON event_archives(first_event_id, last_event_id);
```

### Rebuild from Archive

```rust
pub async fn rebuild_from_archive(&self, event_range: (EventId, EventId)) -> Result<()> {
    // Find archives containing the range
    let archives = projection.find_archives_for_range(event_range)?;

    for archive in archives {
        // Download from IPFS
        let encrypted_data = ipfs_client.cat(&archive.ipfs_cid).await?;

        // Decrypt if needed
        let log_data = if archive.encrypted {
            decrypt(&encrypted_data, &encryption_key)?
        } else {
            encrypted_data
        };

        // Parse and apply events
        let events = parse_log_file(&log_data)?;
        for (event_id, event_data) in events {
            if event_id >= event_range.0 && event_id < event_range.1 {
                let decoded = decoder.decode(&event_data)?;
                applier.apply(&decoded, projection)?;
            }
        }
    }

    Ok(())
}
```

## Monitoring

### Health Checks

```rust
// Check disk usage
let stats = event_log.stats()?;
if stats.total_bytes > MAX_DISK_USAGE {
    alert!("Event log disk usage too high: {} GB", stats.total_bytes / 1_000_000_000);
}

// Check file count
if stats.file_count > MAX_FILE_COUNT {
    alert!("Too many log files: {}. Run archival!", stats.file_count);
}

// Check newest event
let newest = event_log.newest_event_id()?;
let lmdb_next = canonical.meta()?.next_event_id;
if newest + 1 != lmdb_next {
    alert!("EventId mismatch! FileLog: {}, LMDB: {}", newest, lmdb_next);
}
```

### Metrics

```rust
// Write rate
let start = event_log.stats()?;
std::thread::sleep(Duration::from_secs(1));
let end = event_log.stats()?;
let events_per_sec = end.event_count - start.event_count;

// Disk growth rate
let bytes_per_sec = end.total_bytes - start.total_bytes;

// File count
let file_count = end.file_count;
```

## Troubleshooting

### EventId Mismatch

**Symptom:**
```
EventId mismatch: expected 12345, got 12344
```

**Cause:**
- Crash during Phase 3 (after LMDB commit, before file write)
- LMDB allocated EventIds but file write didn't happen

**Fix:**
```rust
// Sync FileEventLog counter with LMDB
let lmdb_next = canonical.meta()?.next_event_id;
event_log.sync_counter(lmdb_next)?;
```

### Corrupted Log File

**Symptom:**
```
Failed to read event header at offset 1234567
```

**Cause:**
- Disk corruption
- Interrupted write

**Fix:**
```rust
// Truncate file at last valid event
event_log.repair_file(file_num)?;

// Or rebuild from LMDB (if events still in LMDB)
event_log.rebuild_from_canonical(&canonical)?;
```

### Disk Full

**Symptom:**
```
No space left on device
```

**Fix:**
```rust
// Archive old files
archive_old_logs(30).await?;  // Archive files older than 30 days

// Or manually delete
let files = event_log.list_files()?;
for file in &files[0..files.len()-2] {  // Keep last 2 files
    std::fs::remove_file(file)?;
}
```

## Best Practices

### 1. Monitor Disk Usage

Set up alerts:
- Disk usage > 80%
- File count > 100
- EventId mismatch

### 2. Regular Archival

Archive old files periodically:
```rust
// Daily cron job
if should_archive() {
    archive_old_logs(30).await?;  // Keep 30 days locally
}
```

### 3. Backup Metadata

The `meta.json` file is critical:
```rust
// Include in regular backups
backup_files(&[
    "event-log/meta.json",
    "canonical/data.mdb",  // LMDB
]);
```

### 4. Validate After Rotation

```rust
// After rotation, verify new file
event_log.rotate()?;
let stats = event_log.stats()?;
assert!(stats.file_count > 0);
```

### 5. Use Sync for Critical Writes

```rust
// For critical events, sync immediately
event_log.append_with_id(event_id, event_data)?;
event_log.sync()?;  // Ensures data is on disk
```

## Future Enhancements

- [ ] Compression (gzip/zstd) for rotated files
- [ ] Automatic IPFS archival
- [ ] Event indexing for faster random access
- [ ] Multiple concurrent writers (with lock coordination)
- [ ] Checksums for corruption detection
- [ ] Encryption at rest
