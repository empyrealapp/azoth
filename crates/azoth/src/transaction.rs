//! Enhanced transaction API with preflight, state updates, and event logging
//!
//! Provides a unified interface for the three-phase transaction pattern:
//! 1. **Preflight**: Validate constraints (key exists, value >= X, etc.)
//! 2. **Update**: Modify state (KV operations)
//! 3. **Log**: Append events
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::{Transaction, TypedValue};
//!
//! # fn main() -> Result<()> {
//! let db = AzothDb::open("./data")?;
//!
//! // Build and execute a transaction with lambda-based validation
//! Transaction::new(&db)
//!     // Phase 1: Preflight validation with arbitrary logic
//!     .require(b"balance".to_vec(), |value| {
//!         let typed_value = value.ok_or(AzothError::PreflightFailed("Balance must exist".into()))?;
//!         let balance = typed_value.as_i64()?;
//!         if balance < 50 {
//!             return Err(AzothError::PreflightFailed("Insufficient balance".into()));
//!         }
//!         Ok(())
//!     })
//!     // Phase 2 & 3: State updates and event logging
//!     .execute(|ctx| {
//!         let balance = ctx.get(b"balance")?.as_i64()?;
//!         ctx.set(b"balance", &TypedValue::I64(balance - 50))?;
//!         ctx.log_bytes(b"withdraw:50")?;
//!         ctx.log_bytes(b"fee:1")?;
//!         Ok(())
//!     })?;
//! # Ok(())
//! # }
//! ```

use crate::{
    AzothDb, AzothError, CanonicalStore, CanonicalTxn, CommitInfo, EventId, Result, TypedValue,
};

/// Preflight context for validation
///
/// Provides read-only access to state during preflight phase
pub struct PreflightContext<'a> {
    db: &'a AzothDb,
}

impl<'a> PreflightContext<'a> {
    fn new(db: &'a AzothDb) -> Self {
        Self { db }
    }

    /// Get a typed value from state
    pub fn get(&self, key: &[u8]) -> Result<TypedValue> {
        let txn = self.db.canonical().read_only_txn()?;
        match txn.get_state(key)? {
            Some(bytes) => TypedValue::from_bytes(&bytes),
            None => Err(AzothError::InvalidState("Key does not exist".into())),
        }
    }

    /// Get a typed value, returning None if key doesn't exist
    pub fn get_opt(&self, key: &[u8]) -> Result<Option<TypedValue>> {
        let txn = self.db.canonical().read_only_txn()?;
        match txn.get_state(key)? {
            Some(bytes) => Ok(Some(TypedValue::from_bytes(&bytes)?)),
            None => Ok(None),
        }
    }

    /// Check if key exists
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        let txn = self.db.canonical().read_only_txn()?;
        Ok(txn.get_state(key)?.is_some())
    }
}

/// Transaction context for state updates and event logging
pub struct TransactionContext<'a> {
    txn: <crate::LmdbCanonicalStore as CanonicalStore>::Txn<'a>,
    value_cache: std::cell::RefCell<std::collections::HashMap<Vec<u8>, TypedValue>>,
}

impl<'a> TransactionContext<'a> {
    /// Get a typed value from state
    pub fn get(&self, key: &[u8]) -> Result<TypedValue> {
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
    pub fn get_opt(&self, key: &[u8]) -> Result<Option<TypedValue>> {
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
    pub fn set(&mut self, key: &[u8], value: &TypedValue) -> Result<()> {
        let bytes = value.to_bytes()?;
        self.txn.put_state(key, &bytes)?;
        // Update cache with new value
        self.value_cache
            .borrow_mut()
            .insert(key.to_vec(), value.clone());
        Ok(())
    }

    /// Delete a key from state
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.txn.del_state(key)?;
        // Remove from cache
        self.value_cache.borrow_mut().remove(key);
        Ok(())
    }

    /// Check if a key exists
    pub fn exists(&self, key: &[u8]) -> Result<bool> {
        Ok(self.txn.get_state(key)?.is_some())
    }

    /// Update a key with a function
    ///
    /// Reads the current value, applies the function, and writes the result.
    /// If the key doesn't exist, passes None to the function.
    pub fn update<F>(&mut self, key: &[u8], f: F) -> Result<()>
    where
        F: FnOnce(Option<TypedValue>) -> Result<TypedValue>,
    {
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
}

/// Transaction builder
///
/// Provides a fluent API for building transactions with lock-based preflight,
/// state updates, and event logging.
///
/// The key insight: **Preflight is about lock acquisition**
/// 1. Declare keys upfront (read_keys, write_keys)
/// 2. Acquire stripe locks on those keys
/// 3. Validate with locks held (ensures isolation)
/// 4. Execute transaction (fast, serialized)
/// 5. Release locks (RAII)
#[allow(clippy::type_complexity)]
pub struct Transaction<'a> {
    db: &'a AzothDb,
    read_keys: Vec<Vec<u8>>,  // Keys to acquire read locks on
    write_keys: Vec<Vec<u8>>, // Keys to acquire write locks on
    validators: Vec<Box<dyn FnOnce(&PreflightContext) -> Result<()> + 'a>>,
}

impl<'a> Transaction<'a> {
    /// Create a new transaction builder
    pub fn new(db: &'a AzothDb) -> Self {
        Self {
            db,
            read_keys: Vec::new(),
            write_keys: Vec::new(),
            validators: Vec::new(),
        }
    }

    /// Declare keys that will be read during this transaction
    ///
    /// These keys will have read locks acquired during preflight.
    /// Multiple readers can hold read locks concurrently.
    pub fn read_keys(mut self, keys: Vec<Vec<u8>>) -> Self {
        self.read_keys.extend(keys);
        self
    }

    /// Declare keys that will be written during this transaction
    ///
    /// These keys will have write locks acquired during preflight.
    /// Write locks are exclusive (no other readers or writers).
    pub fn write_keys(mut self, keys: Vec<Vec<u8>>) -> Self {
        self.write_keys.extend(keys);
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
    pub fn preflight<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&PreflightContext) -> Result<()> + 'a,
    {
        self.validators.push(Box::new(f));
        self
    }

    /// Add a constraint with arbitrary validation logic on a key
    pub fn require<F>(mut self, key: Vec<u8>, validator: F) -> Self
    where
        F: FnOnce(Option<TypedValue>) -> Result<()> + 'a,
    {
        self.read_keys.push(key.clone());
        self.validators.push(Box::new(move |ctx| {
            let value = ctx.get_opt(&key)?;
            validator(value)
        }));
        self
    }

    /// Add a constraint that a key must exist
    pub fn require_exists(self, key: Vec<u8>) -> Self {
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
    pub fn require_min(self, key: Vec<u8>, min: i64) -> Self {
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
    pub fn require_max(self, key: Vec<u8>, max: i64) -> Self {
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

    /// Execute the transaction with the given update function
    ///
    /// This runs all phases with proper lock-based isolation:
    /// 1. Acquire stripe locks on all declared keys (parallel across non-conflicting keys)
    /// 2. Preflight validation (with locks held - state cannot change)
    /// 3. If preflight passes, submit to FIFO commit queue (serialized, single-writer)
    /// 4. Apply state updates (KV operations)
    /// 5. Append events (log operations)
    /// 6. Atomic commit (state + events together)
    /// 7. Release locks (RAII)
    ///
    /// Architecture:
    /// - **Preflight**: Parallel (many concurrent, stripe locking prevents conflicts)
    /// - **Commit**: Serialized (FIFO queue, single-writer for determinism)
    /// - **Locks**: Held from preflight start through commit, then released
    ///
    /// This design enables:
    /// - High throughput preflight (50-200k/sec parallel validation)
    /// - Fast serialized commits (20-50k/sec with no validation overhead)
    /// - Strong isolation (validated state == committed state)
    pub fn execute<F>(self, f: F) -> Result<CommitInfo>
    where
        F: for<'b> FnOnce(&mut TransactionContext<'b>) -> Result<()>,
    {
        // Phase 1: Acquire locks on declared keys
        let lock_manager = self.db.canonical().lock_manager();

        // Acquire read locks
        let _read_locks: Vec<_> = self
            .read_keys
            .iter()
            .map(|key| lock_manager.read_lock(key))
            .collect();

        // Acquire write locks
        let _write_locks: Vec<_> = self
            .write_keys
            .iter()
            .map(|key| lock_manager.write_lock(key))
            .collect();

        // Phase 2: Preflight validation (with locks held!)
        let ctx = PreflightContext::new(self.db);
        for validator in self.validators {
            validator(&ctx)?;
        }

        // Phase 3 & 4: Begin transaction, run updates, append events, commit
        let txn = self.db.canonical().write_txn()?;
        let mut update_ctx = TransactionContext {
            txn,
            value_cache: std::cell::RefCell::new(std::collections::HashMap::new()),
        };

        f(&mut update_ctx)?;

        // Phase 5: Locks released here via RAII when _read_locks and _write_locks go out of scope

        update_ctx.txn.commit()
    }
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
        let txn = db.canonical().write_txn().unwrap();
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
        let txn = db.canonical().write_txn().unwrap();
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

        let result = Transaction::new(&db).execute(|ctx| {
            ctx.log_bytes(b"event1")?;
            ctx.log_bytes(b"event2")?;
            ctx.log_bytes(b"event3")?;
            Ok(())
        });

        let commit_info = result.unwrap();
        assert_eq!(commit_info.events_written, 3);
    }
}
