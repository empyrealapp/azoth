//! Enhanced transaction API with preflight, state updates, and event logging
//!
//! Provides a unified interface for the three-phase transaction pattern:
//! 1. **Preflight**: Validate constraints (key exists, value >= X, etc.)
//! 2. **Update**: Modify state (KV operations)
//! 3. **Log**: Append events
//!
//! # Deadlock Prevention
//!
//! This module provides automatic deadlock prevention through:
//! - **Automatic key sorting**: Locks are always acquired in stripe order
//! - **Timeout-based acquisition**: Locks timeout instead of blocking forever
//! - **Strict key validation**: Only declared keys can be accessed
//!
//! # Async Safety
//!
//! LMDB operations are inherently synchronous and blocking. When used from async
//! code, this can cause deadlocks if the blocking operation holds up the async
//! runtime's worker threads.
//!
//! This module provides two transaction types:
//!
//! - **`Transaction`**: For synchronous contexts (CLI tools, tests, sync code)
//! - **`AsyncTransaction`**: For async contexts (servers, async handlers)
//!
//! ## Choosing the Right API
//!
//! | Context | Use | Notes |
//! |---------|-----|-------|
//! | `fn main()` | `Transaction` | No async runtime |
//! | `#[test]` | `Transaction` | Sync tests |
//! | `#[tokio::test]` | `AsyncTransaction` | Async tests |
//! | `async fn handler()` | `AsyncTransaction` | Server handlers |
//! | `spawn_blocking` | `Transaction` | Already on blocking thread |
//!
//! ## Runtime Safety Checks
//!
//! `Transaction::execute()` includes a runtime check that detects if called from
//! within a Tokio async context. If detected, it will:
//! - Log a warning in debug builds
//! - Panic in debug builds (to catch issues early)
//! - Log an error in release builds (to avoid crashing production)
//!
//! # Example (Synchronous)
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::{Transaction, TypedValue};
//!
//! # fn main() -> Result<()> {
//! let db = AzothDb::open("./data")?;
//!
//! // Synchronous transaction - only use from sync code!
//! Transaction::new(&db)
//!     // Declare all keys that will be accessed
//!     .keys(vec![b"balance".to_vec()])
//!     // Phase 1: Preflight validation
//!     .validate(|ctx| {
//!         let balance = ctx.get(b"balance")?.as_i64()?;
//!         if balance < 50 {
//!             return Err(AzothError::PreflightFailed("Insufficient balance".into()));
//!         }
//!         Ok(())
//!     })
//!     .execute(|ctx| {
//!         let balance = ctx.get(b"balance")?.as_i64()?;
//!         ctx.set(b"balance", &TypedValue::I64(balance - 50))?;
//!         ctx.log_bytes(b"withdraw:50")?;
//!         Ok(())
//!     })?;
//! # Ok(())
//! # }
//! ```
//!
//! # Example (Async)
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::{AsyncTransaction, TypedValue};
//! use std::sync::Arc;
//!
//! # async fn example() -> Result<()> {
//! let db = Arc::new(AzothDb::open("./data")?);
//!
//! // Async-safe transaction - automatically uses spawn_blocking
//! AsyncTransaction::new(db)
//!     .keys(vec![b"balance".to_vec()])
//!     .validate(|ctx| {
//!         let balance = ctx.get(b"balance")?.as_i64()?;
//!         if balance < 50 {
//!             return Err(AzothError::PreflightFailed("Insufficient balance".into()));
//!         }
//!         Ok(())
//!     })
//!     .execute(|ctx| {
//!         let balance = ctx.get(b"balance")?.as_i64()?;
//!         ctx.set(b"balance", &TypedValue::I64(balance - 50))?;
//!         ctx.log_bytes(b"withdraw:50")?;
//!         Ok(())
//!     })
//!     .await?;
//! # Ok(())
//! # }
//! ```

use crate::{
    AzothDb, AzothError, CanonicalReadTxn, CanonicalStore, CanonicalTxn, CommitInfo, EventId,
    Result, TypedValue,
};
use azoth_lmdb::preflight_cache::{CachedValue, PreflightCache};
use std::collections::HashSet;
use std::sync::Arc;

/// Maximum number of keys that can be declared per transaction.
///
/// Declaring too many keys causes excessive stripe lock acquisition, high memory
/// usage for the value cache, and potential cache pollution. This limit acts as
/// a safety guard against accidental misuse or denial-of-service scenarios.
pub const MAX_DECLARED_KEYS: usize = 10_000;

/// Check that the number of declared keys does not exceed [`MAX_DECLARED_KEYS`].
fn check_key_limit(count: usize) -> Result<()> {
    if count > MAX_DECLARED_KEYS {
        return Err(AzothError::Config(format!(
            "Transaction declares {} keys, which exceeds the maximum of {}. \
             Consider batching operations or increasing MAX_DECLARED_KEYS.",
            count, MAX_DECLARED_KEYS
        )));
    }
    Ok(())
}

// ============================================================================
// Async Transaction API
// ============================================================================

/// Async-safe transaction builder for use in async contexts (servers, handlers)
///
/// This is the **recommended** transaction API for async code. It automatically
/// wraps LMDB operations in `spawn_blocking` to prevent blocking the async runtime.
///
/// Unlike `Transaction<'a>`, this struct owns an `Arc<AzothDb>` and can be moved
/// across await points. All closures must be `Send + 'static`.
///
/// # Example
///
/// ```no_run
/// use azoth::prelude::*;
/// use azoth::{AsyncTransaction, TypedValue};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<()> {
/// let db = Arc::new(AzothDb::open("./data")?);
///
/// AsyncTransaction::new(db)
///     .keys(vec![b"balance".to_vec()])
///     .validate(|ctx| {
///         let balance = ctx.get(b"balance")?.as_i64()?;
///         if balance < 50 {
///             return Err(AzothError::PreflightFailed("Insufficient balance".into()));
///         }
///         Ok(())
///     })
///     .execute(|ctx| {
///         let balance = ctx.get(b"balance")?.as_i64()?;
///         ctx.set(b"balance", &TypedValue::I64(balance - 50))?;
///         ctx.log_bytes(b"withdraw:50")?;
///         Ok(())
///     })
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct AsyncTransaction {
    db: Arc<AzothDb>,
    declared_keys: HashSet<Vec<u8>>,
    validators: Vec<PreflightValidator>,
}

/// Type alias for preflight validator functions to reduce type complexity
type PreflightValidator = Box<dyn FnOnce(&PreflightContext) -> Result<()> + Send + 'static>;

impl AsyncTransaction {
    /// Create a new async-safe transaction builder
    pub fn new(db: Arc<AzothDb>) -> Self {
        Self {
            db,
            declared_keys: HashSet::new(),
            validators: Vec::new(),
        }
    }

    /// Declare keys that will be accessed during this transaction
    ///
    /// All keys that will be read or written must be declared upfront.
    /// Keys can be declared in any order - they will be sorted internally.
    pub fn keys(mut self, keys: Vec<Vec<u8>>) -> Self {
        self.declared_keys.extend(keys);
        self
    }

    /// Add a validation function that runs with locks held
    ///
    /// Validators run in preflight phase AFTER locks are acquired.
    /// The closure must be `Send + 'static` to be moved into spawn_blocking.
    pub fn validate<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&PreflightContext) -> Result<()> + Send + 'static,
    {
        self.validators.push(Box::new(f));
        self
    }

    /// Execute the transaction asynchronously (recommended for async code)
    ///
    /// This wraps the synchronous LMDB operations in `spawn_blocking` to prevent
    /// blocking the Tokio runtime. The closure must be `Send + 'static`.
    ///
    /// This is the primary execution method for `AsyncTransaction`.
    pub async fn execute<F>(self, f: F) -> Result<CommitInfo>
    where
        F: FnOnce(&mut TransactionContext<'_>) -> Result<()> + Send + 'static,
    {
        let db = self.db;
        let declared_keys = self.declared_keys;
        let validators = self.validators;

        // Check key count limit before spawning the blocking task
        check_key_limit(declared_keys.len())?;

        tokio::task::spawn_blocking(move || {
            // Phase 1: Acquire locks on declared keys (sorted, deadlock-free)
            let lock_manager = db.canonical().lock_manager();
            let keys_vec: Vec<&[u8]> = declared_keys.iter().map(|k| k.as_slice()).collect();
            let _locks = lock_manager.acquire_keys(&keys_vec)?;

            // Phase 2: Preflight validation (with locks held!)
            let cache = db.canonical().preflight_cache();
            let ctx = PreflightContext::new(&db, cache, &declared_keys);
            for validator in validators {
                validator(&ctx)?;
            }

            // Phase 3 & 4: Begin transaction, run updates, append events, commit
            let txn = db.canonical().write_txn()?;
            let mut update_ctx = TransactionContext {
                txn,
                declared_keys: &declared_keys,
                value_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
            };

            f(&mut update_ctx)?;

            // Commit and invalidate cache for modified keys
            let commit_info = update_ctx.txn.commit()?;
            let keys_to_invalidate: Vec<Vec<u8>> = declared_keys.iter().cloned().collect();
            cache.invalidate_keys(&keys_to_invalidate);

            // Phase 5: Locks released here via RAII

            Ok(commit_info)
        })
        .await
        .map_err(|e| AzothError::Internal(format!("Transaction task failed: {}", e)))?
    }
}

/// Preflight context for validation
///
/// Provides read-only access to state during preflight phase.
/// Only keys declared via `Transaction::keys()` can be accessed.
pub struct PreflightContext<'a> {
    db: &'a AzothDb,
    cache: &'a Arc<PreflightCache>,
    declared_keys: &'a HashSet<Vec<u8>>,
}

impl<'a> PreflightContext<'a> {
    fn new(
        db: &'a AzothDb,
        cache: &'a Arc<PreflightCache>,
        declared_keys: &'a HashSet<Vec<u8>>,
    ) -> Self {
        Self {
            db,
            cache,
            declared_keys,
        }
    }

    /// Check if a key was declared for this transaction
    fn check_key_declared(&self, key: &[u8]) -> Result<()> {
        if !self.declared_keys.contains(key) {
            return Err(AzothError::UndeclaredKeyAccess {
                key: String::from_utf8_lossy(key).to_string(),
            });
        }
        Ok(())
    }

    /// Get a typed value from state
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn get(&self, key: &[u8]) -> Result<TypedValue> {
        self.check_key_declared(key)?;

        // Check cache first
        if let Some(cached) = self.cache.get(key) {
            match cached {
                CachedValue::Some(bytes) => return TypedValue::from_bytes(&bytes),
                CachedValue::None => {
                    return Err(AzothError::InvalidState("Key does not exist".into()))
                }
            }
        }

        // Cache miss - read from LMDB
        let txn = self.db.canonical().read_txn()?;
        match txn.get_state(key)? {
            Some(bytes) => {
                // Cache the raw bytes
                self.cache
                    .insert(key.to_vec(), CachedValue::Some(bytes.clone()));
                TypedValue::from_bytes(&bytes)
            }
            None => {
                // Cache the non-existent key
                self.cache.insert(key.to_vec(), CachedValue::None);
                Err(AzothError::InvalidState("Key does not exist".into()))
            }
        }
    }

    /// Get a typed value, returning None if key doesn't exist
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn get_opt(&self, key: &[u8]) -> Result<Option<TypedValue>> {
        self.check_key_declared(key)?;

        // Check cache first
        if let Some(cached) = self.cache.get(key) {
            match cached {
                CachedValue::Some(bytes) => return Ok(Some(TypedValue::from_bytes(&bytes)?)),
                CachedValue::None => return Ok(None),
            }
        }

        // Cache miss - read from LMDB
        let txn = self.db.canonical().read_txn()?;
        match txn.get_state(key)? {
            Some(bytes) => {
                // Cache the raw bytes
                self.cache
                    .insert(key.to_vec(), CachedValue::Some(bytes.clone()));
                Ok(Some(TypedValue::from_bytes(&bytes)?))
            }
            None => {
                // Cache the non-existent key
                self.cache.insert(key.to_vec(), CachedValue::None);
                Ok(None)
            }
        }
    }

    /// Check if key exists
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        self.check_key_declared(key)?;

        // Check cache first
        if let Some(cached) = self.cache.get(key) {
            match cached {
                CachedValue::Some(_) => return Ok(true),
                CachedValue::None => return Ok(false),
            }
        }

        // Cache miss - read from LMDB
        let txn = self.db.canonical().read_txn()?;
        match txn.get_state(key)? {
            Some(bytes) => {
                // Cache the value
                self.cache.insert(key.to_vec(), CachedValue::Some(bytes));
                Ok(true)
            }
            None => {
                // Cache the non-existent key
                self.cache.insert(key.to_vec(), CachedValue::None);
                Ok(false)
            }
        }
    }
}

/// Transaction context for state updates and event logging
///
/// Only keys declared via `Transaction::keys()` can be accessed.
pub struct TransactionContext<'a> {
    txn: <crate::LmdbCanonicalStore as CanonicalStore>::Txn<'a>,
    declared_keys: &'a HashSet<Vec<u8>>,
    value_cache: std::cell::RefCell<std::collections::HashMap<Vec<u8>, TypedValue>>,
}

impl<'a> TransactionContext<'a> {
    /// Check if a key was declared for this transaction
    fn check_key_declared(&self, key: &[u8]) -> Result<()> {
        if !self.declared_keys.contains(key) {
            return Err(AzothError::UndeclaredKeyAccess {
                key: String::from_utf8_lossy(key).to_string(),
            });
        }
        Ok(())
    }

    /// Get a typed value from state
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn get(&self, key: &[u8]) -> Result<TypedValue> {
        self.check_key_declared(key)?;

        // Check cache first
        {
            let cache = self.value_cache.borrow();
            if let Some(cached) = cache.get(key) {
                return Ok(cached.clone());
            }
        }

        // Read from store
        match self.txn.get_state(key)? {
            Some(bytes) => {
                let value = TypedValue::from_bytes(&bytes)?;
                // Cache the value
                self.value_cache
                    .borrow_mut()
                    .insert(key.to_vec(), value.clone());
                Ok(value)
            }
            None => Err(AzothError::InvalidState("Key does not exist".into())),
        }
    }

    /// Get a typed value, returning None if key doesn't exist
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn get_opt(&self, key: &[u8]) -> Result<Option<TypedValue>> {
        self.check_key_declared(key)?;

        // Check cache first
        {
            let cache = self.value_cache.borrow();
            if let Some(cached) = cache.get(key) {
                return Ok(Some(cached.clone()));
            }
        }

        // Read from store
        match self.txn.get_state(key)? {
            Some(bytes) => {
                let value = TypedValue::from_bytes(&bytes)?;
                // Cache the value
                self.value_cache
                    .borrow_mut()
                    .insert(key.to_vec(), value.clone());
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    /// Set a key to a typed value
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn set(&mut self, key: &[u8], value: &TypedValue) -> Result<()> {
        self.check_key_declared(key)?;

        let bytes = value.to_bytes()?;
        self.txn.put_state(key, &bytes)?;
        // Update cache with new value
        self.value_cache
            .borrow_mut()
            .insert(key.to_vec(), value.clone());
        Ok(())
    }

    /// Delete a key from state
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.check_key_declared(key)?;

        self.txn.del_state(key)?;
        // Remove from cache
        self.value_cache.borrow_mut().remove(key);
        Ok(())
    }

    /// Check if a key exists
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        self.check_key_declared(key)?;

        Ok(self.txn.get_state(key)?.is_some())
    }

    /// Update a key with a function
    ///
    /// Reads the current value, applies the function, and writes the result.
    /// If the key doesn't exist, passes None to the function.
    ///
    /// # Errors
    ///
    /// Returns `UndeclaredKeyAccess` if the key was not declared via `keys()`.
    pub fn update<F>(&mut self, key: &[u8], f: F) -> Result<()>
    where
        F: FnOnce(Option<TypedValue>) -> Result<TypedValue>,
    {
        self.check_key_declared(key)?;

        let old = self.get_opt(key)?;
        let new = f(old)?;
        self.set(key, &new)
    }

    /// Log a structured event (formats as "type:payload" for handler routing)
    pub fn log<T: serde::Serialize>(&mut self, event_type: &str, payload: &T) -> Result<EventId> {
        let json =
            serde_json::to_string(payload).map_err(|e| AzothError::Serialization(e.to_string()))?;
        let event = format!("{}:{}", event_type, json);
        self.txn.append_event(event.as_bytes())
    }

    /// Log multiple structured events
    pub fn log_many<T: serde::Serialize>(
        &mut self,
        events: &[(&str, T)],
    ) -> Result<(EventId, EventId)> {
        let encoded: Vec<Vec<u8>> = events
            .iter()
            .map(|(event_type, payload)| {
                let json = serde_json::to_string(payload)
                    .map_err(|e| AzothError::Serialization(e.to_string()))?;
                Ok(format!("{}:{}", event_type, json).into_bytes())
            })
            .collect::<Result<Vec<Vec<u8>>>>()?;
        self.txn.append_events(&encoded)
    }

    /// Log raw bytes as an event (for simple/legacy events)
    pub fn log_bytes(&mut self, event: &[u8]) -> Result<EventId> {
        self.txn.append_event(event)
    }

    /// Iterate over all state keys and values
    ///
    /// Returns a vector of (key, value) pairs.
    /// Note: This performs a full scan and should be used sparingly.
    pub fn iter_state(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.txn.iter_state()
    }
}

/// Transaction builder
///
/// Provides a fluent API for building transactions with lock-based preflight,
/// state updates, and event logging.
///
/// # Deadlock Prevention
///
/// This implementation prevents deadlocks through:
/// 1. **Automatic key sorting**: Locks are acquired in stripe index order
/// 2. **Timeout-based acquisition**: Uses configurable timeout instead of blocking forever
/// 3. **Strict key validation**: Only declared keys can be accessed
///
/// # Example
///
/// ```no_run
/// use azoth::prelude::*;
/// use azoth::{Transaction, TypedValue};
///
/// # fn main() -> Result<()> {
/// let db = AzothDb::open("./data")?;
///
/// Transaction::new(&db)
///     .keys(vec![b"account_a".to_vec(), b"account_b".to_vec()])
///     .validate(|ctx| {
///         let balance = ctx.get(b"account_a")?.as_i64()?;
///         if balance < 100 {
///             return Err(AzothError::PreflightFailed("Insufficient funds".into()));
///         }
///         Ok(())
///     })
///     .execute(|ctx| {
///         let from = ctx.get(b"account_a")?.as_i64()?;
///         let to = ctx.get(b"account_b")?.as_i64()?;
///         ctx.set(b"account_a", &TypedValue::I64(from - 100))?;
///         ctx.set(b"account_b", &TypedValue::I64(to + 100))?;
///         Ok(())
///     })?;
/// # Ok(())
/// # }
/// ```
#[allow(clippy::type_complexity)]
pub struct Transaction<'a> {
    db: &'a AzothDb,
    declared_keys: HashSet<Vec<u8>>,
    validators: Vec<Box<dyn FnOnce(&PreflightContext) -> Result<()> + 'a>>,
}

impl<'a> Transaction<'a> {
    /// Create a new transaction builder
    pub fn new(db: &'a AzothDb) -> Self {
        Self {
            db,
            declared_keys: HashSet::new(),
            validators: Vec::new(),
        }
    }

    /// Declare keys that will be accessed during this transaction
    ///
    /// All keys that will be read or written must be declared upfront.
    /// This enables:
    /// - Deadlock-free lock acquisition (keys are locked in sorted order)
    /// - Strict validation (accessing undeclared keys is an error)
    ///
    /// Keys can be declared in any order - they will be sorted internally.
    pub fn keys(mut self, keys: Vec<Vec<u8>>) -> Self {
        self.declared_keys.extend(keys);
        self
    }

    /// Add a validation function that runs with locks held
    ///
    /// Validators run in preflight phase AFTER locks are acquired.
    /// This ensures that the state you're validating against cannot
    /// change until your transaction commits.
    pub fn validate<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&PreflightContext) -> Result<()> + 'a,
    {
        self.validators.push(Box::new(f));
        self
    }

    /// Convenience: Add a preflight validation function (alias for validate)
    ///
    /// Preflight functions run before the transaction begins and can validate
    /// constraints on the current state.
    pub fn preflight<F>(self, f: F) -> Self
    where
        F: FnOnce(&PreflightContext) -> Result<()> + 'a,
    {
        self.validate(f)
    }

    /// Add a constraint with arbitrary validation logic on a key
    ///
    /// Automatically adds the key to declared keys.
    pub fn require<F>(mut self, key: Vec<u8>, validator: F) -> Self
    where
        F: FnOnce(Option<TypedValue>) -> Result<()> + 'a,
    {
        self.declared_keys.insert(key.clone());
        self.validators.push(Box::new(move |ctx| {
            let value = ctx.get_opt(&key)?;
            validator(value)
        }));
        self
    }

    /// Add a constraint that a key must exist
    ///
    /// Automatically adds the key to declared keys.
    pub fn require_exists(mut self, key: Vec<u8>) -> Self {
        self.declared_keys.insert(key.clone());
        self.preflight(move |ctx| {
            if !ctx.exists(&key)? {
                return Err(AzothError::PreflightFailed(format!(
                    "Key {:?} must exist",
                    String::from_utf8_lossy(&key)
                )));
            }
            Ok(())
        })
    }

    /// Add a constraint that a typed value must be >= minimum (I64)
    ///
    /// Automatically adds the key to declared keys.
    pub fn require_min(mut self, key: Vec<u8>, min: i64) -> Self {
        self.declared_keys.insert(key.clone());
        self.preflight(move |ctx| {
            let value = ctx.get(&key)?.as_i64()?;
            if value < min {
                return Err(AzothError::PreflightFailed(format!(
                    "Value {} < minimum {}",
                    value, min
                )));
            }
            Ok(())
        })
    }

    /// Add a constraint that a typed value must be <= maximum (I64)
    ///
    /// Automatically adds the key to declared keys.
    pub fn require_max(mut self, key: Vec<u8>, max: i64) -> Self {
        self.declared_keys.insert(key.clone());
        self.preflight(move |ctx| {
            let value = ctx.get(&key)?.as_i64()?;
            if value > max {
                return Err(AzothError::PreflightFailed(format!(
                    "Value {} > maximum {}",
                    value, max
                )));
            }
            Ok(())
        })
    }

    /// Execute the transaction with the given update function (SYNCHRONOUS)
    ///
    /// # Warning: Async Safety
    ///
    /// This method is **synchronous** and will block the current thread while
    /// LMDB performs I/O operations. If called from within a Tokio async context,
    /// this can cause deadlocks by blocking runtime worker threads.
    ///
    /// **For async code, use `AsyncTransaction` instead.**
    ///
    /// This method includes a runtime check that will:
    /// - Panic in debug builds if called from async context (to catch bugs early)
    /// - Log an error in release builds (to avoid crashing production)
    ///
    /// # Phases
    ///
    /// This runs all phases with proper lock-based isolation:
    /// 1. Acquire stripe locks on all declared keys (sorted order, deadlock-free)
    /// 2. Preflight validation (with locks held - state cannot change)
    /// 3. Begin LMDB transaction
    /// 4. Apply state updates (KV operations)
    /// 5. Append events (log operations)
    /// 6. Atomic commit (state + events together)
    /// 7. Release locks (RAII)
    ///
    /// # Architecture
    ///
    /// - **Preflight**: Parallel (many concurrent, stripe locking prevents conflicts)
    /// - **Commit**: Serialized (FIFO queue, single-writer for determinism)
    /// - **Locks**: Held from preflight start through commit, then released
    ///
    /// # Errors
    ///
    /// - `LockTimeout`: Lock acquisition timed out (possible contention)
    /// - `UndeclaredKeyAccess`: Tried to access a key not declared via `keys()`
    /// - `PreflightFailed`: Validation failed
    /// - Other errors from state access or event logging
    pub fn execute<F>(self, f: F) -> Result<CommitInfo>
    where
        F: for<'b> FnOnce(&mut TransactionContext<'b>) -> Result<()>,
    {
        // Runtime safety check: detect if we're in an async context
        if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            // We're inside a Tokio runtime - this is dangerous!
            let msg = "Transaction::execute() called from async context! \
                       This can cause deadlocks. Use AsyncTransaction instead.";

            #[cfg(debug_assertions)]
            {
                // In debug builds, panic to catch the issue early
                panic!("{}", msg);
            }

            #[cfg(not(debug_assertions))]
            {
                // In release builds, log an error but continue
                // (to avoid crashing production, but make the issue visible)
                tracing::error!("{}", msg);
            }
        }

        // Guard against excessive key declarations
        check_key_limit(self.declared_keys.len())?;

        // Phase 1: Acquire locks on declared keys (sorted, deadlock-free)
        let lock_manager = self.db.canonical().lock_manager();
        let keys_vec: Vec<&[u8]> = self.declared_keys.iter().map(|k| k.as_slice()).collect();
        let _locks = lock_manager.acquire_keys(&keys_vec)?;

        // Phase 2: Preflight validation (with locks held!)
        let cache = self.db.canonical().preflight_cache();
        let ctx = PreflightContext::new(self.db, cache, &self.declared_keys);
        for validator in self.validators {
            validator(&ctx)?;
        }

        // Phase 3 & 4: Begin transaction, run updates, append events, commit
        let txn = self.db.canonical().write_txn()?;
        let mut update_ctx = TransactionContext {
            txn,
            declared_keys: &self.declared_keys,
            value_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
        };

        f(&mut update_ctx)?;

        // Commit and invalidate cache for modified keys
        let commit_info = update_ctx.txn.commit()?;
        let keys_to_invalidate: Vec<Vec<u8>> = self.declared_keys.iter().cloned().collect();
        cache.invalidate_keys(&keys_to_invalidate);

        // Phase 5: Locks released here via RAII when _locks goes out of scope

        Ok(commit_info)
    }

    /// Execute the transaction without async safety checks (ADVANCED USE ONLY)
    ///
    /// This method bypasses the runtime check that detects async context misuse.
    /// Only use this if you **know** you're already on a blocking thread, such as:
    /// - Inside a `spawn_blocking` task
    /// - In a dedicated blocking thread pool
    /// - In synchronous CLI tools or tests
    ///
    /// # Safety
    ///
    /// Calling this from an async context can cause deadlocks. The caller is
    /// responsible for ensuring this is only called from a blocking context.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use azoth::prelude::*;
    /// use azoth::{Transaction, TypedValue};
    /// use std::sync::Arc;
    ///
    /// # async fn example() -> anyhow::Result<()> {
    /// let db = Arc::new(AzothDb::open("./data")?);
    /// let db_clone = db.clone();
    ///
    /// // Safe: we're inside spawn_blocking
    /// tokio::task::spawn_blocking(move || {
    ///     Transaction::new(&db_clone)
    ///         .keys(vec![b"key".to_vec()])
    ///         .execute_blocking(|ctx| {
    ///             ctx.set(b"key", &TypedValue::I64(42))?;
    ///             Ok(())
    ///         })
    /// }).await??;
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute_blocking<F>(self, f: F) -> Result<CommitInfo>
    where
        F: for<'b> FnOnce(&mut TransactionContext<'b>) -> Result<()>,
    {
        // No async safety check - caller takes responsibility

        // Guard against excessive key declarations
        check_key_limit(self.declared_keys.len())?;

        // Phase 1: Acquire locks on declared keys (sorted, deadlock-free)
        let lock_manager = self.db.canonical().lock_manager();
        let keys_vec: Vec<&[u8]> = self.declared_keys.iter().map(|k| k.as_slice()).collect();
        let _locks = lock_manager.acquire_keys(&keys_vec)?;

        // Phase 2: Preflight validation (with locks held!)
        let cache = self.db.canonical().preflight_cache();
        let ctx = PreflightContext::new(self.db, cache, &self.declared_keys);
        for validator in self.validators {
            validator(&ctx)?;
        }

        // Phase 3 & 4: Begin transaction, run updates, append events, commit
        let txn = self.db.canonical().write_txn()?;
        let mut update_ctx = TransactionContext {
            txn,
            declared_keys: &self.declared_keys,
            value_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
        };

        f(&mut update_ctx)?;

        // Commit and invalidate cache for modified keys
        let commit_info = update_ctx.txn.commit()?;
        let keys_to_invalidate: Vec<Vec<u8>> = self.declared_keys.iter().cloned().collect();
        cache.invalidate_keys(&keys_to_invalidate);

        Ok(commit_info)
    }
}

/// Execute a transaction asynchronously using spawn_blocking
///
/// This is a convenience function for executing LMDB transactions from async code
/// without blocking the Tokio runtime. It wraps the synchronous transaction
/// execution in a blocking task.
///
/// # Arguments
///
/// * `db` - An `Arc<AzothDb>` (cloned into the blocking task)
/// * `keys` - Keys that will be accessed (for lock acquisition)
/// * `f` - The transaction function to execute
///
/// # Example
///
/// ```no_run
/// use azoth::prelude::*;
/// use azoth::{execute_transaction_async, TypedValue};
/// use std::sync::Arc;
///
/// # async fn example() -> Result<()> {
/// let db = Arc::new(AzothDb::open("./data")?);
///
/// execute_transaction_async(
///     db.clone(),
///     vec![b"balance".to_vec()],
///     |ctx| {
///         ctx.set(b"balance", &TypedValue::I64(100))?;
///         ctx.log_bytes(b"init:100")?;
///         Ok(())
///     }
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn execute_transaction_async<F>(
    db: Arc<AzothDb>,
    keys: Vec<Vec<u8>>,
    f: F,
) -> Result<CommitInfo>
where
    F: for<'b> FnOnce(&mut TransactionContext<'b>) -> Result<()> + Send + 'static,
{
    tokio::task::spawn_blocking(move || Transaction::new(&db).keys(keys).execute(f))
        .await
        .map_err(|e| AzothError::Internal(format!("Transaction task failed: {}", e)))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_test_db() -> (AzothDb, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let db = AzothDb::open(temp_dir.path()).unwrap();
        (db, temp_dir)
    }

    #[test]
    fn test_basic_transaction() {
        let (db, _temp) = create_test_db();

        // Initialize balance
        let mut txn = db.canonical().write_txn().unwrap();
        let balance_bytes = TypedValue::I64(100).to_bytes().unwrap();
        txn.put_state(b"balance", &balance_bytes).unwrap();
        txn.commit().unwrap();

        // Transaction with preflight and update
        let result = Transaction::new(&db)
            .keys(vec![b"balance".to_vec()])
            .preflight(|ctx| {
                let balance = ctx.get(b"balance")?.as_i64()?;
                assert_eq!(balance, 100);
                Ok(())
            })
            .execute(|ctx| {
                let balance = ctx.get(b"balance")?.as_i64()?;
                ctx.set(b"balance", &TypedValue::I64(balance - 50))?;
                ctx.log_bytes(b"withdraw:50")?;
                Ok(())
            });

        if let Err(e) = &result {
            eprintln!("Transaction failed: {:?}", e);
        }
        assert!(result.is_ok());

        // Verify final balance
        let txn = db.canonical().read_txn().unwrap();
        let balance_bytes = txn.get_state(b"balance").unwrap().unwrap();
        let balance = TypedValue::from_bytes(&balance_bytes)
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(balance, 50);
    }

    #[test]
    fn test_preflight_failure() {
        let (db, _temp) = create_test_db();

        // Initialize balance
        let mut txn = db.canonical().write_txn().unwrap();
        let balance_bytes = TypedValue::I64(10).to_bytes().unwrap();
        txn.put_state(b"balance", &balance_bytes).unwrap();
        txn.commit().unwrap();

        // Transaction should fail preflight
        let result = Transaction::new(&db)
            .require_min(b"balance".to_vec(), 50)
            .execute(|ctx| {
                ctx.set(b"balance", &TypedValue::I64(0))?;
                Ok(())
            });

        assert!(result.is_err());

        // Balance should be unchanged
        let txn = db.canonical().read_txn().unwrap();
        let balance_bytes = txn.get_state(b"balance").unwrap().unwrap();
        let balance = TypedValue::from_bytes(&balance_bytes)
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(balance, 10);
    }

    #[test]
    fn test_multiple_constraints() {
        let (db, _temp) = create_test_db();

        // Initialize
        let mut txn = db.canonical().write_txn().unwrap();
        let balance_bytes = TypedValue::I64(75).to_bytes().unwrap();
        txn.put_state(b"balance", &balance_bytes).unwrap();
        txn.commit().unwrap();

        // Transaction with multiple constraints
        let result = Transaction::new(&db)
            .require_exists(b"balance".to_vec())
            .require_min(b"balance".to_vec(), 50)
            .require_max(b"balance".to_vec(), 100)
            .execute(|ctx| {
                ctx.set(b"balance", &TypedValue::I64(60))?;
                Ok(())
            });

        assert!(result.is_ok());
    }

    #[test]
    fn test_multiple_events() {
        let (db, _temp) = create_test_db();

        // Note: No keys needed for event-only transactions
        let result = Transaction::new(&db).execute(|ctx| {
            ctx.log_bytes(b"event1")?;
            ctx.log_bytes(b"event2")?;
            ctx.log_bytes(b"event3")?;
            Ok(())
        });

        let commit_info = result.unwrap();
        assert_eq!(commit_info.events_written, 3);
    }

    #[test]
    fn test_undeclared_key_access_in_preflight() {
        let (db, _temp) = create_test_db();

        // Initialize
        let mut txn = db.canonical().write_txn().unwrap();
        txn.put_state(b"key1", &TypedValue::I64(1).to_bytes().unwrap())
            .unwrap();
        txn.put_state(b"key2", &TypedValue::I64(2).to_bytes().unwrap())
            .unwrap();
        txn.commit().unwrap();

        // Only declare key1, try to access key2
        let result = Transaction::new(&db)
            .keys(vec![b"key1".to_vec()])
            .validate(|ctx| {
                let _ = ctx.get(b"key1")?; // OK
                let _ = ctx.get(b"key2")?; // Should fail
                Ok(())
            })
            .execute(|_ctx| Ok(()));

        assert!(matches!(
            result,
            Err(AzothError::UndeclaredKeyAccess { .. })
        ));
    }

    #[test]
    fn test_undeclared_key_access_in_execute() {
        let (db, _temp) = create_test_db();

        // Initialize
        let mut txn = db.canonical().write_txn().unwrap();
        txn.put_state(b"key1", &TypedValue::I64(1).to_bytes().unwrap())
            .unwrap();
        txn.put_state(b"key2", &TypedValue::I64(2).to_bytes().unwrap())
            .unwrap();
        txn.commit().unwrap();

        // Only declare key1, try to access key2
        let result = Transaction::new(&db)
            .keys(vec![b"key1".to_vec()])
            .execute(|ctx| {
                let _ = ctx.get(b"key1")?; // OK
                ctx.set(b"key2", &TypedValue::I64(99))?; // Should fail
                Ok(())
            });

        assert!(matches!(
            result,
            Err(AzothError::UndeclaredKeyAccess { .. })
        ));
    }

    #[test]
    fn test_require_auto_declares_key() {
        let (db, _temp) = create_test_db();

        // Initialize
        let mut txn = db.canonical().write_txn().unwrap();
        txn.put_state(b"balance", &TypedValue::I64(100).to_bytes().unwrap())
            .unwrap();
        txn.commit().unwrap();

        // require_min should auto-declare the key
        let result = Transaction::new(&db)
            .require_min(b"balance".to_vec(), 50)
            .execute(|ctx| {
                // Should be able to access balance since require_min declared it
                ctx.set(b"balance", &TypedValue::I64(75))?;
                Ok(())
            });

        assert!(result.is_ok());
    }

    #[test]
    fn test_multi_key_transaction() {
        let (db, _temp) = create_test_db();

        // Initialize two accounts
        let mut txn = db.canonical().write_txn().unwrap();
        txn.put_state(b"account_a", &TypedValue::I64(1000).to_bytes().unwrap())
            .unwrap();
        txn.put_state(b"account_b", &TypedValue::I64(500).to_bytes().unwrap())
            .unwrap();
        txn.commit().unwrap();

        // Transfer between accounts - keys can be in any order
        let result = Transaction::new(&db)
            .keys(vec![b"account_b".to_vec(), b"account_a".to_vec()]) // Unsorted order is fine
            .validate(|ctx| {
                let balance = ctx.get(b"account_a")?.as_i64()?;
                if balance < 100 {
                    return Err(AzothError::PreflightFailed("Insufficient funds".into()));
                }
                Ok(())
            })
            .execute(|ctx| {
                let a = ctx.get(b"account_a")?.as_i64()?;
                let b = ctx.get(b"account_b")?.as_i64()?;
                ctx.set(b"account_a", &TypedValue::I64(a - 100))?;
                ctx.set(b"account_b", &TypedValue::I64(b + 100))?;
                ctx.log(
                    "transfer",
                    &serde_json::json!({"from": "a", "to": "b", "amount": 100}),
                )?;
                Ok(())
            });

        assert!(result.is_ok());

        // Verify balances
        let txn = db.canonical().read_txn().unwrap();
        let a = TypedValue::from_bytes(&txn.get_state(b"account_a").unwrap().unwrap())
            .unwrap()
            .as_i64()
            .unwrap();
        let b = TypedValue::from_bytes(&txn.get_state(b"account_b").unwrap().unwrap())
            .unwrap()
            .as_i64()
            .unwrap();
        assert_eq!(a, 900);
        assert_eq!(b, 600);
    }
}
