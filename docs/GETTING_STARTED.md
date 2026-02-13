# Getting Started with Azoth

This guide will walk you through using Azoth in your application.

## Installation

Add Azoth to your `Cargo.toml`:

```toml
[dependencies]
azoth = { path = "../azoth/azoth" }
```

## Basic Usage

### 1. Open a Database

```rust
use azoth::prelude::*;

let db = AzothDb::open("./data")?;
```

This creates:
- `./data/canonical/` - LMDB canonical store
- `./data/canonical/event-log/` - File-based event log
- `./data/projection.db` - SQLite projection store

### 2. Simple Transaction

```rust
use azoth::prelude::*;

// Begin transaction
TransactionBuilder::new(&db)
    .execute(|ctx| {
        // State updates
        ctx.put_state(b"key", b"value")?;

        // Events
        ctx.append_event(b"event_data")?;
        Ok(())
    })?;
```

### 3. Run Projector

```rust
// Process events and update SQL projections
db.projector().run_once()?;
```

## Lock-Based Preflight

### Basic Example

```rust
TransactionBuilder::new(&db)
    // Declare keys upfront
    .keys(vec![b"balance".to_vec()])

    // Validation with locks held
    .validate(|ctx| {
        let balance = ctx.get_state(b"balance")?
            .ok_or_else(|| AzothError::InvalidState("No balance".into()))?;

        let balance_val: u64 = bincode::deserialize(&balance)?;
        if balance_val < 100 {
            return Err(AzothError::InvalidState("Insufficient balance".into()));
        }
        Ok(())
    })

    // Execute with updates
    .execute(|ctx| {
        let balance = ctx.get_state(b"balance")?
            .ok_or_else(|| AzothError::InvalidState("No balance".into()))?;

        let mut balance_val: u64 = bincode::deserialize(&balance)?;
        balance_val -= 100;

        ctx.put_state(b"balance", &bincode::serialize(&balance_val)?)?;
        ctx.append_event(b"withdrew 100")?;
        Ok(())
    })?;
```

## Typed Values

Azoth provides U256/I256 types for large numbers:

```rust
use azoth::prelude::*;

TransactionBuilder::new(&db)
    .execute(|ctx| {
        // Store U256
        let balance = U256::from(1000u64);
        ctx.put_typed(b"balance", &TypedValue::U256(balance))?;

        // Read U256
        let balance = ctx.get_typed(b"balance")?.as_u256()?;

        // Update
        let new_balance = balance - U256::from(100u64);
        ctx.put_typed(b"balance", &TypedValue::U256(new_balance))?;

        Ok(())
    })?;
```

### Available Types

```rust
pub enum TypedValue {
    U64(u64),
    I64(i64),
    U256(U256),
    I256(I256),
    Bytes(Vec<u8>),
    String(String),
    Array(Vec<TypedValue>),
    Set(HashSet<TypedValue>),
}
```

## Event Handling

### Define Event Decoder

```rust
use azoth::prelude::*;

struct MyEventDecoder;

impl EventDecoder for MyEventDecoder {
    fn decode(&self, event_bytes: &[u8]) -> Result<DecodedEvent> {
        let event: serde_json::Value = serde_json::from_slice(event_bytes)?;

        Ok(DecodedEvent {
            event_type: event["type"].as_str().unwrap().to_string(),
            payload: event_bytes.to_vec(),
        })
    }
}
```

### Define Event Applier

```rust
struct MyEventApplier;

impl EventApplier for MyEventApplier {
    fn apply(&self, event: &DecodedEvent, txn: &dyn ProjectionTxn) -> Result<()> {
        match event.event_type.as_str() {
            "deposit" => {
                let payload: serde_json::Value = serde_json::from_slice(&event.payload)?;
                let account_id = payload["account_id"].as_i64().unwrap();
                let amount = payload["amount"].as_i64().unwrap();

                // Update SQL
                txn.execute(
                    "INSERT INTO accounts (id, balance) VALUES (?1, ?2)
                     ON CONFLICT(id) DO UPDATE SET balance = balance + ?2",
                    &[&account_id, &amount],
                )?;
                Ok(())
            }
            _ => Ok(()),
        }
    }
}
```

### Use Custom Handlers

```rust
let decoder = Box::new(MyEventDecoder);
let applier = Box::new(MyEventApplier);

let projector_config = ProjectorConfig {
    decoder,
    applier,
    ..Default::default()
};

let projector = Projector::new(
    canonical.clone(),
    projection.clone(),
    projector_config,
);

projector.run_once()?;
```

## Migrations

### Define Migration

```rust
use azoth::prelude::*;
use rusqlite::Connection;

struct CreateAccountsTable;

impl Migration for CreateAccountsTable {
    fn version(&self) -> u32 {
        2  // Version 1 is base schema
    }

    fn name(&self) -> &str {
        "create_accounts_table"
    }

    fn up(&self, conn: &Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        ).map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }

    fn down(&self, conn: &Connection) -> Result<()> {
        conn.execute("DROP TABLE accounts", [])
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }
}
```

### Run Migrations

```rust
use azoth::prelude::*;

let mut manager = MigrationManager::new();
manager.add(Box::new(CreateAccountsTable));

// Run all pending migrations
manager.run(db.projection())?;

// Check pending migrations
let pending = manager.pending(db.projection())?;
println!("Pending migrations: {:?}", pending);

// Rollback last migration (dangerous!)
// manager.rollback_last(db.projection())?;
```

## Backup & Restore

### Create Backup

```rust
use azoth::backup::*;

let backup = BackupBuilder::new(&db)
    .path("./backups/backup-001")
    .seal_before_backup(true)  // Seal to ensure consistency
    .pause_ingestion(true)     // Pause writes during backup
    .create()?;

println!("Backup created at: {}", backup.path.display());
println!("Sealed at EventId: {:?}", backup.sealed_event_id);
```

### Restore from Backup

```rust
let db = AzothDb::restore("./backups/backup-001", "./data")?;

// Verify restored state
let cursor = db.projection().get_cursor()?;
println!("Restored to EventId: {}", cursor);
```

## Querying Projections

### Direct SQL Access

```rust
let conn = db.projection().conn().lock();

let balance: i64 = conn.query_row(
    "SELECT balance FROM accounts WHERE id = ?1",
    &[&account_id],
    |row| row.get(0),
)?;

println!("Account balance: {}", balance);
```

### Custom Queries

```rust
let conn = db.projection().conn().lock();

let mut stmt = conn.prepare(
    "SELECT id, balance FROM accounts WHERE balance > ?1"
)?;

let accounts = stmt.query_map(&[&1000], |row| {
    Ok((
        row.get::<_, i64>(0)?,
        row.get::<_, i64>(1)?,
    ))
})?;

for account in accounts {
    let (id, balance) = account?;
    println!("Account {}: {}", id, balance);
}
```

## Advanced: Concurrent Transactions

```rust
use std::sync::Arc;
use std::thread;

let db = Arc::new(AzothDb::open("./data")?);

// Spawn 100 concurrent transactions
let handles: Vec<_> = (0..100)
    .map(|i| {
        let db = db.clone();
        thread::spawn(move || {
            let key = format!("account_{}", i);

            TransactionBuilder::new(&db)
                .keys(vec![key.as_bytes().to_vec()])
                .execute(|ctx| {
                    ctx.put_typed(
                        key.as_bytes(),
                        &TypedValue::U256(U256::from(1000u64)),
                    )?;
                    ctx.append_event_json("deposit", &serde_json::json!({
                        "account": i,
                        "amount": 1000
                    }))?;
                    Ok(())
                })
        })
    })
    .collect();

// Wait for all to complete
for handle in handles {
    handle.join().unwrap()?;
}

println!("100 transactions completed successfully!");
```

## Performance Tips

### 1. Batch Event Processing

Configure projector for optimal batching:

```rust
let config = ProjectorConfig {
    batch_events_max: 1000,      // Process up to 1000 events per batch
    batch_bytes_max: 4 * 1024 * 1024,  // Or 4MB, whichever comes first
    max_apply_latency_ms: 100,   // Or 100ms timeout
    ..Default::default()
};
```

### 2. Stripe Lock Tuning

Adjust stripe count for your workload:

```rust
let canonical_config = CanonicalConfig {
    stripe_count: 1024,  // More stripes = more parallelism
    ..Default::default()
};
```

### 3. Pre-Declare Keys

Always declare keys upfront for better lock efficiency:

```rust
TransactionBuilder::new(&db)
    .keys(vec![b"key1".to_vec(), b"key2".to_vec(), b"key3".to_vec()])
    // ...
```

### 4. Use Typed Values

TypedValues are more efficient than raw serialization:

```rust
// Good
ctx.put_typed(b"balance", &TypedValue::U256(balance))?;

// Less efficient
let bytes = bincode::serialize(&balance)?;
ctx.put_state(b"balance", &bytes)?;
```

## Error Handling

```rust
use azoth::prelude::*;

match TransactionBuilder::new(&db).execute(|ctx| {
    // Transaction logic
    Ok(())
}) {
    Ok(commit_info) => {
        println!("Committed {} events", commit_info.events_written);
    }
    Err(AzothError::InvalidState(msg)) => {
        println!("Validation failed: {}", msg);
    }
    Err(AzothError::Sealed(event_id)) => {
        println!("Store is sealed at EventId: {}", event_id);
    }
    Err(AzothError::Paused) => {
        println!("Ingestion is paused");
    }
    Err(e) => {
        println!("Error: {:?}", e);
    }
}
```

## Testing

### Unit Tests

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_transaction() {
        let temp_dir = TempDir::new().unwrap();
        let db = AzothDb::open(temp_dir.path()).unwrap();

        TransactionBuilder::new(&db)
            .execute(|ctx| {
                ctx.put_state(b"key", b"value")?;
                Ok(())
            })
            .unwrap();

        // Verify
        let mut txn = db.canonical().write_txn().unwrap();
        let value = txn.get_state(b"key").unwrap().unwrap();
        assert_eq!(value, b"value");
    }
}
```

### Integration Tests

```rust
#[test]
fn test_end_to_end() {
    let temp_dir = TempDir::new().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();

    // Define migration
    // ... (see Migrations section)

    // Run migration
    let mut manager = MigrationManager::new();
    manager.add(Box::new(CreateAccountsTable));
    manager.run(db.projection()).unwrap();

    // Execute transaction
    TransactionBuilder::new(&db)
        .execute(|ctx| {
            ctx.put_typed(b"balance", &TypedValue::U256(U256::from(1000u64)))?;
            ctx.append_event_json("deposit", &serde_json::json!({
                "account_id": 1,
                "amount": 1000
            }))?;
            Ok(())
        })
        .unwrap();

    // Run projector
    db.projector().run_once().unwrap();

    // Verify SQL
    let conn = db.projection().conn().lock();
    let balance: i64 = conn.query_row(
        "SELECT balance FROM accounts WHERE id = 1",
        [],
        |row| row.get(0),
    ).unwrap();

    assert_eq!(balance, 1000);
}
```

## Next Steps

- Read [Architecture](ARCHITECTURE.md) for design details
- See [examples/](../examples/) for more code samples
- Check [File Event Log](FILE_EVENT_LOG.md) for event storage internals
