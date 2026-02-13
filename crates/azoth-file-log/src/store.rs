use azoth_core::{
    error::{AzothError, Result},
    event_log::{EventLog, EventLogIterator, EventLogStats},
    types::EventId,
};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use parking_lot::Mutex;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

/// Configuration for file-based event log
#[derive(Debug, Clone)]
pub struct FileEventLogConfig {
    /// Base directory for event log files
    pub base_dir: PathBuf,

    /// Maximum size of a single log file before rotation (bytes)
    pub max_file_size: u64,

    /// Buffer size for writes
    pub write_buffer_size: usize,

    /// Maximum size for batch write buffer (bytes)
    pub batch_buffer_size: usize,

    /// Maximum size of a single event payload (bytes)
    pub max_event_size: usize,

    /// Maximum total size for a single append batch (bytes)
    pub max_batch_bytes: usize,

    /// Whether to flush the write buffer after each append (default: true).
    ///
    /// When `true`, every `append_with_id` / `append_events_batch` call flushes
    /// the `BufWriter`, ensuring data reaches the OS page cache immediately.
    /// Set to `false` for maximum throughput at the cost of losing in-flight
    /// buffered events on a process crash (the OS-level file won't be corrupted,
    /// but un-flushed events will be lost).
    pub flush_on_append: bool,
}

impl Default for FileEventLogConfig {
    fn default() -> Self {
        Self {
            base_dir: PathBuf::from("./data/event-log"),
            max_file_size: 512 * 1024 * 1024,  // 512MB
            write_buffer_size: 256 * 1024,     // 256KB (increased from 64KB)
            batch_buffer_size: 1024 * 1024,    // 1MB for batch writes
            max_event_size: 4 * 1024 * 1024,   // 4MB single-event limit
            max_batch_bytes: 64 * 1024 * 1024, // 64MB batch limit
            flush_on_append: true,
        }
    }
}

/// Metadata stored in meta.json
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct EventLogMeta {
    /// Next EventId to assign
    next_event_id: EventId,

    /// Current active log file number
    current_file_num: u64,

    /// Oldest event ID still in storage
    oldest_event_id: EventId,

    /// Total number of events across all files
    total_events: u64,
}

/// File-based event log implementation
pub struct FileEventLog {
    config: FileEventLogConfig,
    meta: Arc<Mutex<EventLogMeta>>,
    next_event_id: Arc<AtomicU64>,
    writer: Arc<Mutex<BufWriter<File>>>,
    current_file_num: Arc<AtomicU64>,
}

impl FileEventLog {
    /// Open or create a file-based event log
    pub fn open(config: FileEventLogConfig) -> Result<Self> {
        // Create base directory
        std::fs::create_dir_all(&config.base_dir)?;

        // Load or create metadata
        let meta_path = config.base_dir.join("meta.json");
        let meta = if meta_path.exists() {
            let data = std::fs::read_to_string(&meta_path)?;
            serde_json::from_str(&data)
                .map_err(|e| AzothError::Projection(format!("Failed to parse meta.json: {}", e)))?
        } else {
            EventLogMeta::default()
        };

        let next_event_id = Arc::new(AtomicU64::new(meta.next_event_id));
        let current_file_num = Arc::new(AtomicU64::new(meta.current_file_num));

        // Open current log file for appending
        let log_path = Self::log_file_path(&config.base_dir, meta.current_file_num);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let writer = Arc::new(Mutex::new(BufWriter::with_capacity(
            config.write_buffer_size,
            file,
        )));

        Ok(Self {
            config,
            meta: Arc::new(Mutex::new(meta)),
            next_event_id,
            writer,
            current_file_num,
        })
    }

    /// Get path to a log file by number
    fn log_file_path(base_dir: &Path, file_num: u64) -> PathBuf {
        base_dir.join(format!("events-{:08}.log", file_num))
    }

    /// Save metadata to disk
    fn save_meta(&self) -> Result<()> {
        let meta = self.meta.lock();
        let meta_path = self.config.base_dir.join("meta.json");
        // Use compact serialization (not pretty) for better performance
        let data = serde_json::to_string(&*meta)
            .map_err(|e| AzothError::Projection(format!("Failed to serialize meta: {}", e)))?;
        std::fs::write(&meta_path, data)?;
        Ok(())
    }

    /// Write a single event entry to the current log file
    ///
    /// Format: [event_id: u64][size: u32][data: bytes]
    fn write_event_entry(&self, event_id: EventId, event_bytes: &[u8]) -> Result<()> {
        if event_bytes.len() > self.config.max_event_size {
            return Err(AzothError::InvalidState(format!(
                "Event size {} exceeds max_event_size {}",
                event_bytes.len(),
                self.config.max_event_size
            )));
        }

        if event_bytes.len() > u32::MAX as usize {
            return Err(AzothError::InvalidState(format!(
                "Event size {} exceeds u32 encoding limit",
                event_bytes.len()
            )));
        }

        let mut writer = self.writer.lock();

        // Write event_id (8 bytes, big-endian)
        writer.write_all(&event_id.to_be_bytes())?;

        // Write size (4 bytes, big-endian)
        let size = event_bytes.len() as u32;
        writer.write_all(&size.to_be_bytes())?;

        // Write event data
        writer.write_all(event_bytes)?;

        Ok(())
    }

    /// Check if rotation is needed and perform it
    fn check_rotation(&self) -> Result<Option<PathBuf>> {
        let log_path = Self::log_file_path(
            &self.config.base_dir,
            self.current_file_num.load(Ordering::SeqCst),
        );

        let file_size = std::fs::metadata(&log_path)?.len();

        if file_size >= self.config.max_file_size {
            self.rotate_internal()
        } else {
            Ok(None)
        }
    }

    /// Perform log rotation
    fn rotate_internal(&self) -> Result<Option<PathBuf>> {
        // Flush current writer
        {
            let mut writer = self.writer.lock();
            writer.flush()?;
        }

        let old_file_num = self.current_file_num.load(Ordering::SeqCst);
        let old_path = Self::log_file_path(&self.config.base_dir, old_file_num);

        // Increment file number
        let new_file_num = old_file_num + 1;
        self.current_file_num.store(new_file_num, Ordering::SeqCst);

        // Update metadata
        {
            let mut meta = self.meta.lock();
            meta.current_file_num = new_file_num;
        }
        self.save_meta()?;

        // Open new log file
        let new_path = Self::log_file_path(&self.config.base_dir, new_file_num);
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&new_path)?;

        // Replace writer
        {
            let mut writer = self.writer.lock();
            *writer = BufWriter::with_capacity(self.config.write_buffer_size, file);
        }

        tracing::info!(
            "Rotated event log: {} -> {}",
            old_path.display(),
            new_path.display()
        );

        Ok(Some(old_path))
    }
}

impl EventLog for FileEventLog {
    fn append_with_id(&self, event_id: EventId, event_bytes: &[u8]) -> Result<()> {
        // Write event entry
        self.write_event_entry(event_id, event_bytes)?;

        // Conditionally flush based on config
        if self.config.flush_on_append {
            let mut writer = self.writer.lock();
            writer.flush()?;
        }

        // Update metadata
        {
            let mut meta = self.meta.lock();
            meta.next_event_id = event_id + 1;
            meta.total_events += 1;
        }

        // Update atomic counter to match
        self.next_event_id.store(event_id + 1, Ordering::SeqCst);

        // Check for rotation
        self.check_rotation()?;

        Ok(())
    }

    fn append_batch_with_ids(&self, first_event_id: EventId, events: &[Vec<u8>]) -> Result<()> {
        if events.is_empty() {
            return Err(AzothError::InvalidState("Cannot append empty batch".into()));
        }

        // Calculate total size needed: (8 + 4 + data_len) per event
        let total_size: usize = events
            .iter()
            .map(|e| 8 + 4 + e.len()) // event_id + size + data
            .sum();

        if total_size > self.config.max_batch_bytes {
            return Err(AzothError::InvalidState(format!(
                "Batch size {} exceeds max_batch_bytes {}",
                total_size, self.config.max_batch_bytes
            )));
        }

        for event in events {
            if event.len() > self.config.max_event_size {
                return Err(AzothError::InvalidState(format!(
                    "Event size {} exceeds max_event_size {}",
                    event.len(),
                    self.config.max_event_size
                )));
            }
            if event.len() > u32::MAX as usize {
                return Err(AzothError::InvalidState(format!(
                    "Event size {} exceeds u32 encoding limit",
                    event.len()
                )));
            }
        }

        // Check if batch exceeds reasonable size
        if total_size > self.config.batch_buffer_size {
            // For very large batches, fall back to individual writes
            // to avoid excessive memory allocation
            for (i, event_bytes) in events.iter().enumerate() {
                let event_id = first_event_id + i as u64;
                self.write_event_entry(event_id, event_bytes)?;
            }
        } else {
            // Pre-allocate buffer for entire batch
            let mut buffer = Vec::with_capacity(total_size);

            // Serialize all events into buffer
            for (i, event_bytes) in events.iter().enumerate() {
                let event_id = first_event_id + i as u64;

                // Write event_id (8 bytes, big-endian)
                buffer.extend_from_slice(&event_id.to_be_bytes());

                // Write size (4 bytes, big-endian)
                let size = event_bytes.len() as u32;
                buffer.extend_from_slice(&size.to_be_bytes());

                // Write event data
                buffer.extend_from_slice(event_bytes);
            }

            // Single lock acquisition + single write syscall
            let mut writer = self.writer.lock();
            writer.write_all(&buffer)?;
        }

        // Conditionally flush based on config
        if self.config.flush_on_append {
            let mut writer = self.writer.lock();
            writer.flush()?;
        }

        // Update metadata
        let last_id = first_event_id + events.len() as u64 - 1;
        {
            let mut meta = self.meta.lock();
            meta.next_event_id = last_id + 1;
            meta.total_events += events.len() as u64;
        }

        // Update atomic counter to match
        self.next_event_id.store(last_id + 1, Ordering::SeqCst);

        // Check for rotation
        self.check_rotation()?;

        Ok(())
    }

    fn next_event_id(&self) -> Result<EventId> {
        Ok(self.next_event_id.load(Ordering::SeqCst))
    }

    fn iter_range(
        &self,
        start: EventId,
        end: Option<EventId>,
    ) -> Result<Box<dyn EventLogIterator>> {
        // Flush writer to ensure all data is on disk
        {
            let mut writer = self.writer.lock();
            writer.flush()?;
        }

        let meta = self.meta.lock();
        let end_id = end.unwrap_or(meta.next_event_id);

        Ok(Box::new(FileEventLogIter::new(
            self.config.base_dir.clone(),
            start,
            end_id,
            meta.current_file_num,
            self.config.max_event_size,
        )?))
    }

    fn get(&self, event_id: EventId) -> Result<Option<Vec<u8>>> {
        // Use iter_range to find single event
        let mut iter = self.iter_range(event_id, Some(event_id + 1))?;

        match iter.next() {
            Some(Ok((id, data))) if id == event_id => Ok(Some(data)),
            Some(Ok(_)) => Ok(None),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }

    fn delete_range(&self, start: EventId, end: EventId) -> Result<usize> {
        // For now, deletion is not implemented
        // In production, this would:
        // 1. Identify log files that contain only events in [start, end]
        // 2. Delete those files
        // 3. Update oldest_event_id in metadata
        // 4. Compact remaining files if needed

        tracing::warn!("delete_range not yet implemented: {} to {}", start, end);
        Ok(0)
    }

    fn rotate(&self) -> Result<PathBuf> {
        self.rotate_internal()?
            .ok_or_else(|| AzothError::InvalidState("No rotation needed".into()))
    }

    fn oldest_event_id(&self) -> Result<EventId> {
        let meta = self.meta.lock();
        Ok(meta.oldest_event_id)
    }

    fn newest_event_id(&self) -> Result<EventId> {
        let next = self.next_event_id.load(Ordering::SeqCst);
        if next == 0 {
            Ok(0)
        } else {
            Ok(next - 1)
        }
    }

    fn sync(&self) -> Result<()> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        writer.get_ref().sync_all()?;
        self.save_meta()?;
        Ok(())
    }

    fn stats(&self) -> Result<EventLogStats> {
        let meta = self.meta.lock();

        // Calculate total bytes across all log files
        let mut total_bytes = 0u64;
        let mut file_count = 0usize;

        for file_num in 0..=meta.current_file_num {
            let path = Self::log_file_path(&self.config.base_dir, file_num);
            if path.exists() {
                total_bytes += std::fs::metadata(&path)?.len();
                file_count += 1;
            }
        }

        Ok(EventLogStats {
            event_count: meta.total_events,
            oldest_event_id: meta.oldest_event_id,
            newest_event_id: if meta.next_event_id == 0 {
                0
            } else {
                meta.next_event_id - 1
            },
            total_bytes,
            file_count,
        })
    }
}

/// Ensure metadata is saved on drop
impl Drop for FileEventLog {
    fn drop(&mut self) {
        // Attempt to flush writer and save metadata on drop
        // This ensures meta.json is written even if the user forgets to call sync()
        if let Err(e) = self.sync() {
            eprintln!("Warning: Failed to sync FileEventLog on drop: {}", e);
        }
    }
}

/// Iterator over events in file-based log
struct FileEventLogIter {
    base_dir: PathBuf,
    current_file_num: u64,
    max_file_num: u64,
    current_file: Option<File>,
    next_event_id: EventId,
    end_event_id: EventId,
    max_event_size: usize,
}

impl FileEventLogIter {
    fn new(
        base_dir: PathBuf,
        start: EventId,
        end: EventId,
        max_file_num: u64,
        max_event_size: usize,
    ) -> Result<Self> {
        let mut iter = Self {
            base_dir,
            current_file_num: 0,
            max_file_num,
            current_file: None,
            next_event_id: start,
            end_event_id: end,
            max_event_size,
        };

        // Open first file
        iter.open_next_file()?;

        Ok(iter)
    }

    fn open_next_file(&mut self) -> Result<bool> {
        while self.current_file_num <= self.max_file_num {
            let path = FileEventLog::log_file_path(&self.base_dir, self.current_file_num);

            if path.exists() {
                let file = File::open(&path)?;
                self.current_file = Some(file);
                return Ok(true);
            }

            self.current_file_num += 1;
        }

        Ok(false)
    }

    fn read_next_event(&mut self) -> Result<Option<(EventId, Vec<u8>)>> {
        loop {
            if self.next_event_id >= self.end_event_id {
                return Ok(None);
            }

            let file = match self.current_file.as_mut() {
                Some(f) => f,
                None => return Ok(None),
            };

            // Try to read event header: [event_id: u64][size: u32]
            let mut header = [0u8; 12];
            match file.read_exact(&mut header) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    // End of file, try next file
                    self.current_file_num += 1;
                    if !self.open_next_file()? {
                        return Ok(None);
                    }
                    continue;
                }
                Err(e) => return Err(e.into()),
            }

            let event_id = u64::from_be_bytes(header[0..8].try_into().unwrap());
            let size = u32::from_be_bytes(header[8..12].try_into().unwrap());

            if size as usize > self.max_event_size {
                return Err(AzothError::InvalidState(format!(
                    "Event {} size {} exceeds max_event_size {}",
                    event_id, size, self.max_event_size
                )));
            }

            // Read event data
            let mut data = vec![0u8; size as usize];
            file.read_exact(&mut data)?;

            // Skip events before start
            if event_id < self.next_event_id {
                continue;
            }

            self.next_event_id = event_id + 1;
            return Ok(Some((event_id, data)));
        }
    }
}

impl Iterator for FileEventLogIter {
    type Item = Result<(EventId, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_next_event() {
            Ok(Some(event)) => Some(Ok(event)),
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }
}

// EventLogIterator is automatically implemented via blanket impl in azoth-core

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (FileEventLog, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let config = FileEventLogConfig {
            base_dir: temp_dir.path().to_path_buf(),
            max_file_size: 1024, // Small for testing rotation
            write_buffer_size: 128,
            batch_buffer_size: 4096,
            max_event_size: 1024 * 1024,
            max_batch_bytes: 16 * 1024 * 1024,
            flush_on_append: true,
        };
        let log = FileEventLog::open(config).unwrap();
        (log, temp_dir)
    }

    #[test]
    fn test_append_and_read() {
        let (log, _temp) = setup();

        // Append events with pre-allocated IDs
        log.append_with_id(0, b"event 0").unwrap();
        log.append_with_id(1, b"event 1").unwrap();
        log.append_with_id(2, b"event 2").unwrap();

        // Read events
        let mut iter = log.iter_range(0, None).unwrap();
        assert_eq!(iter.next().unwrap().unwrap(), (0, b"event 0".to_vec()));
        assert_eq!(iter.next().unwrap().unwrap(), (1, b"event 1".to_vec()));
        assert_eq!(iter.next().unwrap().unwrap(), (2, b"event 2".to_vec()));
        assert!(iter.next().is_none());
    }

    #[test]
    fn test_batch_append() {
        let (log, _temp) = setup();

        let events = vec![
            b"event 0".to_vec(),
            b"event 1".to_vec(),
            b"event 2".to_vec(),
        ];

        log.append_batch_with_ids(0, &events).unwrap();

        let mut iter = log.iter_range(0, None).unwrap();
        assert_eq!(iter.next().unwrap().unwrap(), (0, b"event 0".to_vec()));
        assert_eq!(iter.next().unwrap().unwrap(), (1, b"event 1".to_vec()));
        assert_eq!(iter.next().unwrap().unwrap(), (2, b"event 2".to_vec()));
    }

    #[test]
    fn test_get_single_event() {
        let (log, _temp) = setup();

        log.append_with_id(0, b"event 0").unwrap();
        log.append_with_id(1, b"event 1").unwrap();
        log.append_with_id(2, b"event 2").unwrap();

        assert_eq!(log.get(1).unwrap(), Some(b"event 1".to_vec()));
        assert_eq!(log.get(99).unwrap(), None);
    }

    #[test]
    fn test_stats() {
        let (log, _temp) = setup();

        log.append_with_id(0, b"event 0").unwrap();
        log.append_with_id(1, b"event 1").unwrap();

        // Sync to ensure data is written to disk
        log.sync().unwrap();

        let stats = log.stats().unwrap();
        assert_eq!(stats.event_count, 2);
        assert_eq!(stats.oldest_event_id, 0);
        assert_eq!(stats.newest_event_id, 1);
        assert!(stats.total_bytes > 0);
        assert_eq!(stats.file_count, 1);
    }
}
