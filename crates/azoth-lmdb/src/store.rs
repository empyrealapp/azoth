use azoth_core::{
    error::{AzothError, Result},
    event_log::{EventLog, EventLogIterator},
    lock_manager::LockManager,
    traits::{CanonicalStore, EventIter},
    types::{BackupInfo, CanonicalMeta, EventId},
    CanonicalConfig,
};
use azoth_file_log::{FileEventLog, FileEventLogConfig};
use lmdb::{Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

use crate::keys::meta_keys;
use crate::txn::LmdbWriteTxn;

/// Adapter to convert EventLogIterator to EventIter
struct EventIterAdapter(Box<dyn EventLogIterator>);

impl EventIter for EventIterAdapter {
    fn next(&mut self) -> Result<Option<(EventId, Vec<u8>)>> {
        // Convert from Iterator's Option<Result<T>> to EventIter's Result<Option<T>>
        match self.0.next() {
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

/// LMDB-backed canonical store
///
/// State is stored in LMDB for transactional integrity.
/// Events are stored in a file-based event log for better performance.
pub struct LmdbCanonicalStore {
    pub(crate) env: Arc<Environment>,
    pub(crate) state_db: Database,
    pub(crate) events_db: Database, // Deprecated: kept for backward compatibility
    pub(crate) meta_db: Database,
    pub(crate) config_path: std::path::PathBuf,
    pub(crate) event_log: Arc<FileEventLog>, // NEW: File-based event storage
    lock_manager: Arc<LockManager>,
    write_lock: Arc<Mutex<()>>, // Single writer
    paused: Arc<AtomicBool>,
}

impl LmdbCanonicalStore {
    /// Get the next event ID from meta
    fn get_next_event_id<T: Transaction>(&self, txn: &T) -> Result<EventId> {
        match txn.get(self.meta_db, &meta_keys::NEXT_EVENT_ID) {
            Ok(bytes) => {
                let id_str = std::str::from_utf8(bytes)
                    .map_err(|e| AzothError::Serialization(e.to_string()))?;
                id_str
                    .parse::<EventId>()
                    .map_err(|e| AzothError::Serialization(e.to_string()))
            }
            Err(lmdb::Error::NotFound) => Ok(0),
            Err(e) => Err(AzothError::Transaction(e.to_string())),
        }
    }

    /// Get the sealed event ID from meta (if any)
    fn get_sealed_event_id<T: Transaction>(&self, txn: &T) -> Result<Option<EventId>> {
        match txn.get(self.meta_db, &meta_keys::SEALED_EVENT_ID) {
            Ok(bytes) => {
                let id_str = std::str::from_utf8(bytes)
                    .map_err(|e| AzothError::Serialization(e.to_string()))?;
                let id = id_str
                    .parse::<EventId>()
                    .map_err(|e| AzothError::Serialization(e.to_string()))?;
                Ok(Some(id))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(AzothError::Transaction(e.to_string())),
        }
    }

    /// Set meta value
    fn set_meta(&self, txn: &mut lmdb::RwTransaction, key: &str, value: &str) -> Result<()> {
        txn.put(self.meta_db, &key, &value, WriteFlags::empty())
            .map_err(|e| AzothError::Transaction(e.to_string()))
    }

    /// Get meta value
    fn get_meta<T: Transaction>(&self, txn: &T, key: &str) -> Result<Option<String>> {
        match txn.get(self.meta_db, &key) {
            Ok(bytes) => {
                let value = std::str::from_utf8(bytes)
                    .map_err(|e| AzothError::Serialization(e.to_string()))?
                    .to_string();
                Ok(Some(value))
            }
            Err(lmdb::Error::NotFound) => Ok(None),
            Err(e) => Err(AzothError::Transaction(e.to_string())),
        }
    }
}

impl CanonicalStore for LmdbCanonicalStore {
    type Txn<'a> = LmdbWriteTxn<'a>;

    fn open(cfg: CanonicalConfig) -> Result<Self> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&cfg.path)?;

        // Configure LMDB environment
        let mut env_builder = Environment::new();
        env_builder.set_max_dbs(3); // state, events, meta
        env_builder.set_map_size(cfg.map_size);
        env_builder.set_max_readers(cfg.max_readers);

        // Set sync flags based on config
        let mut flags = EnvironmentFlags::empty();
        match cfg.sync_mode {
            azoth_core::config::SyncMode::Full => {}
            azoth_core::config::SyncMode::NoMetaSync => {
                flags.insert(EnvironmentFlags::NO_META_SYNC);
            }
            azoth_core::config::SyncMode::NoSync => {
                flags.insert(EnvironmentFlags::NO_SYNC);
            }
        }
        env_builder.set_flags(flags);

        let env = env_builder
            .open(&cfg.path)
            .map_err(|e| AzothError::Io(std::io::Error::other(e)))?;

        // Open databases
        let state_db = env
            .create_db(Some("state"), DatabaseFlags::empty())
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let events_db = env
            .create_db(Some("events"), DatabaseFlags::empty())
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let meta_db = env
            .create_db(Some("meta"), DatabaseFlags::empty())
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let env = Arc::new(env);
        let lock_manager = Arc::new(LockManager::new(cfg.stripe_count));

        // Initialize file-based event log
        let event_log_config = FileEventLogConfig {
            base_dir: cfg.path.join("event-log"),
            ..Default::default()
        };
        let event_log = Arc::new(FileEventLog::open(event_log_config)?);

        // Initialize metadata if needed
        {
            let mut txn = env
                .begin_rw_txn()
                .map_err(|e| AzothError::Transaction(e.to_string()))?;

            // Initialize next_event_id if not present
            if txn.get(meta_db, &meta_keys::NEXT_EVENT_ID).is_err() {
                txn.put(
                    meta_db,
                    &meta_keys::NEXT_EVENT_ID,
                    &"0",
                    WriteFlags::empty(),
                )
                .map_err(|e| AzothError::Transaction(e.to_string()))?;
            }

            // Initialize schema_version if not present
            if txn.get(meta_db, &meta_keys::SCHEMA_VERSION).is_err() {
                txn.put(
                    meta_db,
                    &meta_keys::SCHEMA_VERSION,
                    &"1",
                    WriteFlags::empty(),
                )
                .map_err(|e| AzothError::Transaction(e.to_string()))?;
            }

            // Initialize timestamps if not present
            let now = chrono::Utc::now().to_rfc3339();
            if txn.get(meta_db, &meta_keys::CREATED_AT).is_err() {
                txn.put(meta_db, &meta_keys::CREATED_AT, &now, WriteFlags::empty())
                    .map_err(|e| AzothError::Transaction(e.to_string()))?;
            }
            txn.put(meta_db, &meta_keys::UPDATED_AT, &now, WriteFlags::empty())
                .map_err(|e| AzothError::Transaction(e.to_string()))?;

            txn.commit()
                .map_err(|e| AzothError::Transaction(e.to_string()))?;
        }

        Ok(Self {
            env,
            state_db,
            events_db,
            meta_db,
            config_path: cfg.path.clone(),
            event_log,
            lock_manager,
            write_lock: Arc::new(Mutex::new(())),
            paused: Arc::new(AtomicBool::new(false)),
        })
    }

    fn close(&self) -> Result<()> {
        // LMDB closes automatically on drop
        Ok(())
    }

    fn read_txn(&self) -> Result<Self::Txn<'_>> {
        // Read-only transactions not supported yet
        // Would require separate transaction type
        Err(AzothError::InvalidState(
            "Read-only transactions not yet implemented".into(),
        ))
    }

    fn write_txn(&self) -> Result<Self::Txn<'_>> {
        // Acquire write lock (single writer)
        // Note: This is held for the entire transaction duration
        let _guard = self.write_lock.lock().unwrap();

        // Check if paused
        if self.paused.load(Ordering::SeqCst) {
            return Err(AzothError::Paused);
        }

        // Check if sealed
        let ro_txn = self
            .env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;
        if let Some(sealed_id) = self.get_sealed_event_id(&ro_txn)? {
            return Err(AzothError::Sealed(sealed_id));
        }
        drop(ro_txn); // Release read transaction

        let txn = self
            .env
            .begin_rw_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        // Lock is released when guard drops at the end of this scope
        // This means the transaction can only be created, not held across await points
        drop(_guard);

        Ok(LmdbWriteTxn::new(
            txn,
            self.state_db,
            self.events_db,
            self.meta_db,
            self.event_log.clone(),
        ))
    }

    fn iter_events(&self, from: EventId, to: Option<EventId>) -> Result<Box<dyn EventIter>> {
        // Use file-based event log instead of LMDB
        // Convert EventLogIterator to EventIter via adapter
        let iter = self.event_log.iter_range(from, to)?;
        Ok(Box::new(EventIterAdapter(iter)))
    }

    fn seal(&self) -> Result<EventId> {
        let _guard = self.write_lock.lock().unwrap();

        let mut txn = self
            .env
            .begin_rw_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let next_event_id = self.get_next_event_id(&txn)?;
        let sealed_event_id = if next_event_id > 0 {
            next_event_id - 1
        } else {
            0
        };

        self.set_meta(
            &mut txn,
            meta_keys::SEALED_EVENT_ID,
            &sealed_event_id.to_string(),
        )?;
        self.set_meta(
            &mut txn,
            meta_keys::UPDATED_AT,
            &chrono::Utc::now().to_rfc3339(),
        )?;

        txn.commit()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        Ok(sealed_event_id)
    }

    fn lock_manager(&self) -> &LockManager {
        &self.lock_manager
    }

    fn pause_ingestion(&self) -> Result<()> {
        self.paused.store(true, Ordering::SeqCst);
        // Wait for in-flight transactions (they hold write_lock)
        let _guard = self.write_lock.lock().unwrap();
        Ok(())
    }

    fn resume_ingestion(&self) -> Result<()> {
        self.paused.store(false, Ordering::SeqCst);
        Ok(())
    }

    fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    fn backup_to(&self, dir: &Path) -> Result<BackupInfo> {
        crate::backup::backup_to(self, dir)
    }

    fn restore_from(dir: &Path, cfg: CanonicalConfig) -> Result<Self> {
        crate::backup::restore_from(dir, cfg)
    }

    fn meta(&self) -> Result<CanonicalMeta> {
        let txn = self
            .env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let next_event_id = self.get_next_event_id(&txn)?;
        let sealed_event_id = self.get_sealed_event_id(&txn)?;
        let schema_version = self
            .get_meta(&txn, meta_keys::SCHEMA_VERSION)?
            .and_then(|s| s.parse().ok())
            .unwrap_or(1);
        let created_at = self
            .get_meta(&txn, meta_keys::CREATED_AT)?
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());
        let updated_at = self
            .get_meta(&txn, meta_keys::UPDATED_AT)?
            .unwrap_or_else(|| chrono::Utc::now().to_rfc3339());

        Ok(CanonicalMeta {
            next_event_id,
            sealed_event_id,
            schema_version,
            created_at,
            updated_at,
        })
    }
}

impl LmdbCanonicalStore {
    /// Get reference to the file-based event log
    ///
    /// This allows direct access to event storage for advanced use cases.
    pub fn event_log(&self) -> &Arc<FileEventLog> {
        &self.event_log
    }
}
