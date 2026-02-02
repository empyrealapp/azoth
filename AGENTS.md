# AGENTS.md - AI Agent Integration Guide for Azoth

This guide helps AI agents integrate Azoth into applications efficiently. Azoth is a high-performance embedded database for state management and event sourcing with ACID guarantees.

## Core Concepts

### What Azoth Is
- Embedded database combining transactional state storage (LMDB) and append-only event log
- Provides event sourcing with SQL projections (SQLite) derived from events
- Atomic commits of state + events together
- TEE-safe with deterministic behavior

### When to Use Azoth
Use Azoth when the application needs:
- **Event sourcing** with full audit trails
- **ACID guarantees** for state changes
- **State + event atomicity** (both committed together or not at all)
- **SQL queries** over event-derived data
- **High-performance writes** (10K+ tx/sec) and reads (1M+ reads/sec)
- **Deterministic backups** for reproducible state

Good fits: blockchain systems, game servers, financial apps, audit-required systems
Poor fits: simple key-value needs (use raw LMDB), document-heavy workloads (use a document DB)

## Quick Integration Steps

### 1. Add Dependencies

Add to `Cargo.toml`:
```toml
[dependencies]
azoth = "0.1.1"
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
```

Optional crates:
- `azoth-scheduler = "0.1.1"` - For cron-based task scheduling
- `tokio = { version = "1.0", features = ["rt", "sync", "macros"] }` - For async event processing

### 2. Initialize Database

```rust
use azoth::prelude::*;

// Open database (creates if doesn't exist)
let db = AzothDb::open("./data")?;
// Creates:
// - ./data/canonical/ (LMDB state + event log)
// - ./data/projection.db (SQLite)
```

### 3. Write Transactions

Use the `Transaction` API for all state changes:

```rust
use azoth::{Transaction, TypedValue};

Transaction::new(&db).execute(|ctx| {
    // Read state
    let balance = ctx.get(b"balance")?.as_i64()?;

    // Write state
    ctx.set(b"balance", &TypedValue::I64(balance + 100))?;

    // Log events (atomically committed with state)
    ctx.log("deposit", &serde_json::json!({
        "amount": 100,
        "new_balance": balance + 100
    }))?;

    Ok(())
})?;
```

### 4. Process Events

Set up event handlers to project events into SQL:

```rust
use azoth::{EventHandler, EventProcessor};
use rusqlite::Connection;
use std::sync::Arc;
use std::time::Duration;

// Define handler
struct DepositHandler;

impl EventHandler for DepositHandler {
    fn event_type(&self) -> &str { "deposit" }

    fn handle(&self, conn: &Connection, event_id: EventId, payload: &[u8]) -> azoth::Result<()> {
        let data: serde_json::Value = serde_json::from_slice(payload)?;
        let amount = data["amount"].as_i64().unwrap_or(0);

        conn.execute(
            "INSERT INTO deposits (event_id, amount) VALUES (?1, ?2)",
            [event_id as i64, amount]
        )?;
        Ok(())
    }

    fn validate(&self, _payload: &[u8]) -> azoth::Result<()> { Ok(()) }
}

// Build processor
let processor = EventProcessor::builder(Arc::new(db))
    .with_handler(Box::new(DepositHandler))
    .with_poll_interval(Duration::from_millis(100))
    .with_batch_size(100)
    .build(conn);

// Process events
processor.process_batch()?;
```

## Common Patterns

### Pattern 1: Basic CRUD with Events

```rust
// Create
Transaction::new(&db).execute(|ctx| {
    ctx.set(b"user:123", &TypedValue::String("Alice".into()))?;
    ctx.log("user_created", &serde_json::json!({"id": 123, "name": "Alice"}))?;
    Ok(())
})?;

// Read
Transaction::new(&db).execute(|ctx| {
    let name = ctx.get(b"user:123")?.as_string()?;
    println!("User: {}", name);
    Ok(())
})?;

// Update
Transaction::new(&db).execute(|ctx| {
    ctx.set(b"user:123", &TypedValue::String("Alice Smith".into()))?;
    ctx.log("user_updated", &serde_json::json!({"id": 123, "name": "Alice Smith"}))?;
    Ok(())
})?;

// Delete
Transaction::new(&db).execute(|ctx| {
    ctx.delete(b"user:123")?;
    ctx.log("user_deleted", &serde_json::json!({"id": 123}))?;
    Ok(())
})?;
```

### Pattern 2: Preflight Validation

Validate constraints before executing transaction:

```rust
Transaction::new(&db)
    .require(b"balance".to_vec(), |value| {
        let balance = value
            .ok_or(AzothError::PreflightFailed("Balance must exist".into()))?
            .as_i64()?;
        if balance < 100 {
            return Err(AzothError::PreflightFailed("Insufficient funds".into()));
        }
        Ok(())
    })
    .execute(|ctx| {
        let balance = ctx.get(b"balance")?.as_i64()?;
        ctx.set(b"balance", &TypedValue::I64(balance - 100))?;
        ctx.log("withdrawal", &serde_json::json!({"amount": 100}))?;
        Ok(())
    })?;
```

### Pattern 3: Batch Event Processing

```rust
// Write many events
for i in 0..1000 {
    Transaction::new(&db).execute(|ctx| {
        ctx.log("click", &serde_json::json!({"user_id": i}))?;
        Ok(())
    })?;
}

// Process in batches (979K events/sec throughput)
let processor = EventProcessor::builder(Arc::new(db))
    .with_handler(Box::new(ClickHandler))
    .with_batch_size(1000) // Adjust based on memory
    .build(conn);

while processor.lag()? > 0 {
    processor.process_batch()?;
}
```

### Pattern 4: Error Handling with Dead Letter Queue

```rust
use azoth::{DeadLetterQueue, ErrorStrategy};

let dlq = Arc::new(DeadLetterQueue::new(conn.clone())?);

let processor = EventProcessor::builder(db)
    .with_handler(Box::new(MyHandler))
    .with_error_strategy(ErrorStrategy::DeadLetterQueue)
    .with_dead_letter_queue(dlq.clone())
    .build(conn);

// Run processor
processor.run().await?;

// Later, inspect failures
let failed = dlq.list(100)?;
for event in failed {
    println!("Failed: {} - {}", event.event_id, event.error_message);
    // Retry or remove
    dlq.mark_retry(event.id)?;
}
```

### Pattern 5: Scheduled Tasks

```rust
use azoth_scheduler::{Task, Scheduler};

// Define task
struct CleanupTask;

impl Task for CleanupTask {
    fn execute(&self, db: &AzothDb) -> azoth::Result<()> {
        Transaction::new(db).execute(|ctx| {
            // Perform cleanup
            ctx.log("cleanup_executed", &serde_json::json!({}))?;
            Ok(())
        })
    }
}

// Schedule task
let scheduler = Scheduler::new(db)?;
scheduler.schedule_cron("0 0 * * *", Arc::new(CleanupTask))?; // Daily at midnight
scheduler.start().await?;
```

### Pattern 6: Database Migrations

Migrations manage schema evolution for SQL projections. Use them to version your projection tables.

#### Code-Based Migrations

Define migrations in Rust code using the macro:

```rust
use azoth::{migration, Migration, MigrationManager};

// Define migration using macro
migration!(
    CreateUsersTable,
    version: 2,  // Version 1 is the base schema
    name: "create_users_table",
    up: |conn| {
        conn.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                email TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        ).map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    },
    down: |conn| {
        conn.execute("DROP TABLE IF EXISTS users", [])
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }
);

// Create manager and register migrations
let mut manager = MigrationManager::new();
manager.add(Box::new(CreateUsersTable));

// Run all pending migrations
manager.run(db.projection())?;
```

#### File-Based Migrations

For larger teams or SQL-first workflows:

```rust
// Generate a new migration file
let manager = MigrationManager::new();
let migration_file = manager.generate("./migrations", "add_user_preferences")?;
// Creates: migrations/0002_add_user_preferences.sql
//     and: migrations/0002_add_user_preferences.down.sql

// Load all migrations from directory
let mut manager = MigrationManager::new();
manager.load_from_directory("./migrations")?;
manager.run(db.projection())?;
```

Migration file format (`0002_add_user_preferences.sql`):
```sql
-- Migration: add_user_preferences
-- Version: 2

CREATE TABLE user_preferences (
    user_id INTEGER PRIMARY KEY,
    theme TEXT NOT NULL DEFAULT 'light',
    notifications BOOLEAN NOT NULL DEFAULT 1,
    FOREIGN KEY (user_id) REFERENCES users(id)
);
```

#### Migration Management

```rust
// Check current schema version
let version = db.projection().schema_version()?;
println!("Current version: {}", version);

// List all registered migrations
for m in manager.list() {
    println!("v{}: {}", m.version, m.name);
}

// Check pending migrations
let pending = manager.pending(db.projection())?;
println!("Pending: {} migrations", pending.len());

// View migration history
let history = manager.history(db.projection())?;
for entry in history {
    println!("v{}: {} (applied: {})",
             entry.version, entry.name, entry.applied_at);
}

// Rollback last migration (dangerous - use with caution)
manager.rollback_last(db.projection())?;
```

#### Manual Migration Implementation

For complex migrations, implement the `Migration` trait directly:

```rust
struct ComplexMigration;

impl Migration for ComplexMigration {
    fn version(&self) -> u32 { 3 }
    fn name(&self) -> &str { "complex_migration" }

    fn up(&self, conn: &Connection) -> azoth::Result<()> {
        // Multi-step migration
        conn.execute("ALTER TABLE users ADD COLUMN status TEXT", [])?;
        conn.execute("UPDATE users SET status = 'active'", [])?;
        conn.execute("CREATE INDEX idx_status ON users(status)", [])?;
        Ok(())
    }

    fn down(&self, conn: &Connection) -> azoth::Result<()> {
        conn.execute("DROP INDEX IF EXISTS idx_status", [])?;
        // Note: SQLite doesn't support DROP COLUMN easily
        Ok(())
    }

    fn verify(&self, conn: &Connection) -> azoth::Result<()> {
        // Optional: verify migration succeeded
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM users WHERE status IS NULL",
            [],
            |row| row.get(0)
        )?;
        if count > 0 {
            return Err(AzothError::InvalidState(
                "Migration left NULL status values".into()
            ));
        }
        Ok(())
    }
}
```

#### Best Practices for Migrations

1. **Version Numbers**: Start at version 2 (version 1 is base schema), increment sequentially
2. **Never Modify Existing Migrations**: Once applied in production, treat as immutable
3. **Test Rollbacks**: Ensure down() migrations work before deploying
4. **Idempotent Operations**: Use `IF NOT EXISTS` and `IF EXISTS` for safety
5. **Data Migrations**: Separate schema changes from data transformations
6. **Foreign Keys**: Create dependent tables in order, drop in reverse order

```rust
// Good: Idempotent migrations
conn.execute(
    "CREATE TABLE IF NOT EXISTS users (...)",
    []
)?;

// Good: Safe rollback
conn.execute(
    "DROP TABLE IF EXISTS users",
    []
)?;
```

#### Migration Workflow

1. **Development**: Generate migration, edit SQL, test locally
2. **Testing**: Run migrations on test database, verify with `verify()`
3. **Production**: Run `manager.run()` on startup or via CLI
4. **Monitoring**: Log migration execution, track schema version

```rust
// Typical application startup
let db = AzothDb::open("./data")?;

let mut manager = MigrationManager::new();
manager.load_from_directory("./migrations")?;

let pending = manager.pending(db.projection())?;
if !pending.is_empty() {
    println!("Applying {} migrations...", pending.len());
    manager.run(db.projection())?;
    println!("Migrations complete!");
}
```

## TypedValue Reference

Azoth uses strongly-typed values:

```rust
// Integers
TypedValue::I64(42)
TypedValue::U256(U256::from(1000u64))

// Strings
TypedValue::String("hello".to_string())

// Collections
TypedValue::Set(Set::from(["admin", "user"]))
TypedValue::Array(Array::from(vec!["item1".to_string()]))

// Raw bytes
TypedValue::Bytes(vec![1, 2, 3])

// Reading values
let value = ctx.get(b"key")?;
match value {
    TypedValue::I64(n) => println!("Number: {}", n),
    TypedValue::String(s) => println!("String: {}", s),
    _ => {}
}

// Or use type-specific getters
let num = ctx.get(b"key")?.as_i64()?;
let text = ctx.get(b"key")?.as_string()?;
```

## Decision Guide

### Choose Transaction API when:
- You need preflight validation
- You want explicit error handling
- You need to read before writing

### Choose Direct CanonicalStore when:
- Maximum performance needed
- Simple writes without logic
- Building custom abstractions

### Setup Event Handlers when:
- You need SQL queries over events
- Building dashboards/analytics
- Deriving computed state

### Use Dead Letter Queue when:
- Event processing can fail
- You need retry logic
- Production reliability matters

### Use Scheduler when:
- Periodic tasks needed (cleanup, reports, etc.)
- Cron-based execution required
- Background job processing

### Use Migrations when:
- Creating projection tables for the first time
- Modifying projection schema (add/remove columns, indexes)
- Application evolves and needs new SQL views
- Deploying schema changes across environments
- Team needs to track schema version history

### Choose Code-Based Migrations when:
- Small team or single developer
- Complex migrations with validation logic
- Type safety and IDE support desired
- Migrations are part of application logic

### Choose File-Based Migrations when:
- Larger teams need SQL review process
- Database administrators manage schemas
- SQL-first workflow preferred
- Migration history tracked in version control

## Best Practices

### 1. Event Design
- Events are immutable - design them carefully
- Include all data needed for projections
- Use semantic event names (`order_created`, not `create`)
- Keep events small and focused

### 2. State Keys
- Use consistent key naming: `entity:id:field`
- Example: `user:123:balance`, `order:456:status`
- Avoid long keys (impacts memory)

### 3. Projector Performance
- Run projector regularly (not just once)
- Use batch processing for high throughput
- Monitor lag: `db.projector().get_lag()?`
- Consider multiple projections for different views

### 4. Error Handling
```rust
// Always handle errors explicitly
Transaction::new(&db).execute(|ctx| {
    let balance = match ctx.get_opt(b"balance")? {
        Some(v) => v.as_i64()?,
        None => 0, // Default for new accounts
    };
    // ... rest of logic
    Ok(())
})?;
```

### 5. Testing
```rust
#[test]
fn test_transaction() -> azoth::Result<()> {
    let temp_dir = tempfile::tempdir()?;
    let db = AzothDb::open(temp_dir.path())?;

    Transaction::new(&db).execute(|ctx| {
        ctx.set(b"test", &TypedValue::I64(42))?;
        Ok(())
    })?;

    Transaction::new(&db).execute(|ctx| {
        let val = ctx.get(b"test")?.as_i64()?;
        assert_eq!(val, 42);
        Ok(())
    })?;

    Ok(())
}
```

## Troubleshooting

### "PreflightFailed" errors
- Check that required keys exist before validation
- Ensure validation logic matches execute logic
- Use `.require_exists()` for existence checks

### Events not appearing in projections
- Run `db.projector().run_once()` after writing events
- Check lag: `db.projector().get_lag()?`
- Verify event handlers are registered
- Check event type names match exactly

### Performance issues
- Use batch operations for bulk writes
- Increase batch size for projector
- Check disk I/O (Azoth is fast, disk might not be)
- Use `.with_batch_size(1000)` or higher for event processing

### "LockConflict" errors
- Retry with exponential backoff
- Reduce contention by striping keys
- Consider optimistic locking patterns

## Examples Location

See `crates/azoth/examples/` for complete examples:
- `basic_usage.rs` - CRUD operations
- `lambda_require.rs` - Preflight validation
- `event_processor_builder.rs` - Event handling setup
- `continuous_processor.rs` - Long-running processors
- `custom_errors.rs` - Error handling patterns
- `backup_restore.rs` - Deterministic backups
- `batch_processing.rs` - High-throughput patterns
- `migration_example.rs` - Database schema migrations

## API Quick Reference

### Database
```rust
let db = AzothDb::open(path)?;
db.canonical()     // Access canonical store
db.projection()    // Access projection store
db.projector()     // Get projector instance
db.backup_to(path)?  // Create backup
```

### Transactions
```rust
Transaction::new(&db)
    .require(key, |val| { /* validate */ })
    .require_exists(key)
    .require_min(key, min_value)
    .execute(|ctx| { /* logic */ })?;
```

### Context Operations
```rust
ctx.get(key)?              // Get (must exist)
ctx.get_opt(key)?          // Get optional
ctx.exists(key)?           // Check existence
ctx.set(key, value)?       // Set value
ctx.delete(key)?           // Delete key
ctx.update(key, |old| { /* transform */ })?  // Functional update
ctx.log(type, payload)?    // Log event
ctx.log_many(&[...])?      // Log multiple events
```

### Event Processing
```rust
EventProcessor::builder(db)
    .with_handler(handler)
    .with_poll_interval(duration)
    .with_batch_size(size)
    .with_error_strategy(strategy)
    .build(conn)
```

### Migrations
```rust
// Create and register migrations
let mut manager = MigrationManager::new();
manager.add(Box::new(MyMigration));
manager.load_from_directory("./migrations")?;

// Check status
db.projection().schema_version()?      // Current version
manager.list()                          // All migrations
manager.pending(db.projection())?       // Pending migrations
manager.history(db.projection())?       // Applied migrations

// Run migrations
manager.run(db.projection())?           // Apply pending
manager.rollback_last(db.projection())?  // Rollback (dangerous)

// Generate new migration file
manager.generate("./migrations", "migration_name")?
```

## Performance Expectations

Single-threaded benchmarks (release mode):
- State writes: 11,243 tx/sec
- Event appends: 13,092 events/sec
- Batch appends: 979,323 events/sec (75x speedup)
- State reads: 977,975 reads/sec
- Atomic (state + event): 10,266 tx/sec

Use batching for maximum throughput.

## Getting Help

- Crates.io: https://crates.io/crates/azoth
- Repository: https://github.com/empyrealapp/azoth
- Examples: `crates/azoth/examples/`
- Tests: `cargo test --workspace`

---

**Key Principle**: State and events are always committed atomically. This ensures consistency and enables powerful event sourcing patterns with ACID guarantees.
