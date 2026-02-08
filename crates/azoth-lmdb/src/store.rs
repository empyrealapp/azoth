use azoth_core::{
    error::{AzothError, Result},
    event_log::{EventLog, EventLogIterator},
    lock_manager::LockManager,
    traits::{self, CanonicalStore, EventIter, StateIter},
    types::{BackupInfo, CanonicalMeta, EventId},
    CanonicalConfig,
};
use azoth_file_log::{FileEventLog, FileEventLogConfig};
use lmdb::{Database, DatabaseFlags, Environment, EnvironmentFlags, Transaction, WriteFlags};
use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;

use crate::keys::meta_keys;
use crate::preflight_cache::PreflightCache;
use crate::read_pool::LmdbReadPool;
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
    pub(crate) meta_db: Database,
    pub(crate) config_path: std::path::PathBuf,
    pub(crate) event_log: Arc<FileEventLog>,
    lock_manager: Arc<LockManager>,
    write_lock: Arc<Mutex<()>>,
    paused: Arc<AtomicBool>,
    chunk_size: usize,
    txn_counter: Arc<AtomicUsize>,
    preflight_cache: Arc<PreflightCache>,
    read_pool: Option<Arc<LmdbReadPool>>,
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
    type ReadTxn<'a> = crate::txn::LmdbReadTxn<'a>;

    fn open(cfg: CanonicalConfig) -> Result<Self> {
        // Create directory if it doesn't exist
        std::fs::create_dir_all(&cfg.path)?;

        // Configure LMDB environment
        let mut env_builder = Environment::new();
        env_builder.set_max_dbs(2); // state, meta
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

        let meta_db = env
            .create_db(Some("meta"), DatabaseFlags::empty())
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let env = Arc::new(env);
        let lock_manager = Arc::new(LockManager::new(
            cfg.stripe_count,
            Duration::from_millis(cfg.lock_timeout_ms),
        ));

        // Initialize preflight cache
        let preflight_cache = Arc::new(PreflightCache::new(
            cfg.preflight_cache_size,
            cfg.preflight_cache_ttl_secs,
            cfg.preflight_cache_enabled,
        ));

        // Initialize file-based event log
        let event_log_config = FileEventLogConfig {
            base_dir: cfg.path.join("event-log"),
            max_event_size: cfg.event_max_size_bytes,
            max_batch_bytes: cfg.event_batch_max_bytes,
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
            // schema_version starts at 0 so that migrations starting from version 1 will be applied
            if txn.get(meta_db, &meta_keys::SCHEMA_VERSION).is_err() {
                txn.put(
                    meta_db,
                    &meta_keys::SCHEMA_VERSION,
                    &"0",
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

        // Initialize read pool if enabled
        let read_pool = if cfg.read_pool.enabled {
            Some(Arc::new(LmdbReadPool::new(
                env.clone(),
                state_db,
                cfg.read_pool.clone(),
            )))
        } else {
            None
        };

        Ok(Self {
            env,
            state_db,
            meta_db,
            config_path: cfg.path.clone(),
            event_log,
            lock_manager,
            write_lock: Arc::new(Mutex::new(())),
            paused: Arc::new(AtomicBool::new(false)),
            chunk_size: cfg.state_iter_chunk_size,
            txn_counter: Arc::new(AtomicUsize::new(0)),
            preflight_cache,
            read_pool,
        })
    }

    fn close(&self) -> Result<()> {
        // LMDB closes automatically on drop
        Ok(())
    }

    fn read_txn(&self) -> Result<Self::ReadTxn<'_>> {
        let txn = self
            .env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;
        Ok(crate::txn::LmdbReadTxn::new(txn, self.state_db))
    }

    fn write_txn(&self) -> Result<Self::Txn<'_>> {
        // Increment transaction counter (transaction starting)
        self.txn_counter.fetch_add(1, Ordering::SeqCst);

        // Check if paused (no lock needed)
        if self.paused.load(Ordering::SeqCst) {
            self.txn_counter.fetch_sub(1, Ordering::SeqCst);
            return Err(AzothError::Paused);
        }

        // Check if sealed
        let ro_txn = self.env.begin_ro_txn().map_err(|e| {
            self.txn_counter.fetch_sub(1, Ordering::SeqCst);
            AzothError::Transaction(e.to_string())
        })?;
        if let Some(sealed_id) = self.get_sealed_event_id(&ro_txn)? {
            self.txn_counter.fetch_sub(1, Ordering::SeqCst);
            return Err(AzothError::Sealed(sealed_id));
        }
        drop(ro_txn); // Release read transaction

        // Let LMDB handle write serialization (no lock needed)
        let txn = self.env.begin_rw_txn().map_err(|e| {
            self.txn_counter.fetch_sub(1, Ordering::SeqCst);
            AzothError::Transaction(e.to_string())
        })?;

        Ok(LmdbWriteTxn::new(
            txn,
            self.state_db,
            self.meta_db,
            self.event_log.clone(),
            Arc::downgrade(&self.txn_counter),
            self.preflight_cache.clone(),
        ))
    }

    fn iter_events(&self, from: EventId, to: Option<EventId>) -> Result<Box<dyn EventIter>> {
        // Use file-based event log instead of LMDB
        // Convert EventLogIterator to EventIter via adapter
        let iter = self.event_log.iter_range(from, to)?;
        Ok(Box::new(EventIterAdapter(iter)))
    }

    fn range(&self, start: &[u8], end: Option<&[u8]>) -> Result<Box<dyn traits::StateIter>> {
        let iter = crate::state_iter::LmdbStateIter::new(
            self.env.clone(),
            self.state_db,
            start,
            end,
            self.chunk_size,
        )?;
        Ok(Box::new(iter))
    }

    fn scan_prefix(&self, prefix: &[u8]) -> Result<Box<dyn traits::StateIter>> {
        let iter = crate::state_iter::LmdbStateIter::with_prefix(
            self.env.clone(),
            self.state_db,
            prefix,
            self.chunk_size,
        )?;
        Ok(Box::new(iter))
    }

    fn seal(&self) -> Result<EventId> {
        let _guard = self.write_lock.lock().unwrap();

        let mut txn = self
            .env
            .begin_rw_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        let next_event_id = self.get_next_event_id(&txn)?;
        // Empty event log: don't seal at all. Writing SEALED_EVENT_ID=0 makes the
        // projector believe event 0 exists and can cause "lag" to stay >0 forever.
        let sealed_event_id = if next_event_id > 0 { next_event_id - 1 } else { 0 };
        if next_event_id > 0 {
            self.set_meta(
                &mut txn,
                meta_keys::SEALED_EVENT_ID,
                &sealed_event_id.to_string(),
            )?;
        } else {
            // Clear any previous seal, if present.
            let _ = txn.del(self.meta_db, &meta_keys::SEALED_EVENT_ID, None);
        }
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

        // Wait for in-flight transactions to complete
        let start = std::time::Instant::now();
        while self.txn_counter.load(Ordering::SeqCst) > 0 {
            if start.elapsed() > std::time::Duration::from_secs(30) {
                return Err(AzothError::Timeout(
                    "Waiting for in-flight transactions to complete".into(),
                ));
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }

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
    /// Clear the sealed marker, re-enabling writes.
    ///
    /// Sealing is used as a temporary barrier to create deterministic snapshots. Backups should
    /// clear the seal before resuming ingestion; otherwise the DB becomes permanently read-only.
    pub fn clear_seal(&self) -> Result<()> {
        let _guard = self.write_lock.lock().unwrap();

        let mut txn = self
            .env
            .begin_rw_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;

        // Ignore errors (e.g. NotFound).
        let _ = txn.del(self.meta_db, &meta_keys::SEALED_EVENT_ID, None);
        let _ = self.set_meta(
            &mut txn,
            meta_keys::UPDATED_AT,
            &chrono::Utc::now().to_rfc3339(),
        );

        txn.commit()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;
        Ok(())
    }

    /// Begin a read-only transaction
    ///
    /// Read-only transactions allow concurrent reads without blocking writes or other reads.
    /// This is more efficient than write_txn() for preflight validation and queries.
    pub fn read_only_txn(&self) -> Result<crate::txn::LmdbReadTxn<'_>> {
        let txn = self
            .env
            .begin_ro_txn()
            .map_err(|e| AzothError::Transaction(e.to_string()))?;
        Ok(crate::txn::LmdbReadTxn::new(txn, self.state_db))
    }

    /// Iterate events asynchronously
    ///
    /// This method wraps the synchronous event iterator in a blocking task,
    /// allowing it to be used in async contexts without blocking the runtime.
    pub async fn iter_events_async(
        &self,
        from: EventId,
        to: Option<EventId>,
    ) -> Result<Vec<(EventId, Vec<u8>)>> {
        let event_log = self.event_log.clone();

        tokio::task::spawn_blocking(move || {
            let mut iter = event_log.iter_range(from, to)?;
            let mut results = Vec::new();

            loop {
                match iter.next() {
                    Some(Ok(item)) => results.push(item),
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }

            Ok(results)
        })
        .await
        .map_err(|e| AzothError::Internal(format!("Task join error: {}", e)))?
    }

    /// Get single state value asynchronously
    ///
    /// This method wraps the synchronous read in a blocking task,
    /// allowing it to be used in async contexts without blocking the runtime.
    pub async fn get_state_async(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let env = self.env.clone();
        let state_db = self.state_db;
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || {
            let txn = env
                .begin_ro_txn()
                .map_err(|e| AzothError::Transaction(e.to_string()))?;

            match txn.get(state_db, &key) {
                Ok(bytes) => Ok(Some(bytes.to_vec())),
                Err(lmdb::Error::NotFound) => Ok(None),
                Err(e) => Err(AzothError::Transaction(e.to_string())),
            }
        })
        .await
        .map_err(|e| AzothError::Internal(format!("Task join error: {}", e)))?
    }

    /// Scan prefix asynchronously
    ///
    /// This method wraps the synchronous prefix scan in a blocking task,
    /// allowing it to be used in async contexts without blocking the runtime.
    pub async fn scan_prefix_async(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let env = self.env.clone();
        let state_db = self.state_db;
        let prefix = prefix.to_vec();
        let chunk_size = self.chunk_size;

        tokio::task::spawn_blocking(move || {
            let mut iter =
                crate::state_iter::LmdbStateIter::with_prefix(env, state_db, &prefix, chunk_size)?;

            let mut results = Vec::new();
            while let Some(item) = iter.next()? {
                results.push(item);
            }

            Ok(results)
        })
        .await
        .map_err(|e| AzothError::Internal(format!("Task join error: {}", e)))?
    }

    /// Get reference to the file-based event log
    ///
    /// This allows direct access to event storage for advanced use cases.
    pub fn event_log(&self) -> &Arc<FileEventLog> {
        &self.event_log
    }

    /// Get reference to the preflight cache
    ///
    /// This allows access to cache statistics and manual cache operations.
    pub fn preflight_cache(&self) -> &Arc<PreflightCache> {
        &self.preflight_cache
    }

    /// Get reference to the read pool (if enabled)
    ///
    /// Returns None if read pooling was not enabled in config.
    pub fn read_pool(&self) -> Option<&Arc<LmdbReadPool>> {
        self.read_pool.as_ref()
    }

    /// Check if read pooling is enabled
    pub fn has_read_pool(&self) -> bool {
        self.read_pool.is_some()
    }
}
