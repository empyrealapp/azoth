# Azoth Storage Subsystem

[![Tests](https://img.shields.io/badge/tests-52%20passing-brightgreen)]()
[![Crates](https://img.shields.io/badge/crates-6-blue)]()
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue)]()

**Azoth** is a high-performance embedded database for state management and event sourcing. It combines fast transactional writes, append-only event logs, queryable SQL projections, and deterministic backups—making it ideal for applications requiring ACID guarantees with event sourcing, including blockchain systems, game servers, financial applications, and any system needing reliable state management with audit trails.

## Features

- 🔐 **TEE-Safe**: Designed for trusted execution environments with deterministic behavior
- ⚡ **High Performance**: 10K+ tx/sec writes, 1M+ reads/sec
- 🔒 **Stripe Locking**: Parallel preflight validation with per-key locking
- 📦 **Event Sourcing**: Append-only event log with ACID guarantees
- 🔍 **SQL Projections**: Queryable views derived from events (SQLite)
- 💾 **Deterministic Backup**: Pausable ingestion with sealed snapshots
- 🎯 **Atomic Transactions**: State + events committed atomically
- 🚀 **Batch Processing**: 979K+ events/sec with batching
- 🔄 **Dead Letter Queue**: Failed event recovery and replay

## Quick Start

```rust
use azoth::prelude::*;
use azoth::{Transaction, TypedValue};

// Open database
let db = AzothDb::open("./data")?;

// Execute a transaction with preflight validation
Transaction::new(&db)
    .require(b"balance".to_vec(), |value| {
        // Preflight validation
        let typed_value = value.ok_or(
            AzothError::PreflightFailed("Balance must exist".into())
        )?;
        let balance = typed_value.as_i64()?;
        if balance < 100 {
            return Err(AzothError::PreflightFailed("Insufficient balance".into()));
        }
        Ok(())
    })
    .execute(|ctx| {
        // State updates
        let balance = ctx.get(b"balance")?.as_i64()?;
        ctx.set(b"balance", &TypedValue::I64(balance - 100))?;

        // Log events
        ctx.log("withdraw", &serde_json::json!({
            "amount": 100
        }))?;
        Ok(())
    })?;

// Run projector to update SQL views
db.projector().run_once()?;
```

## Architecture

```
┌──────────────────┐
│   Transaction    │  Lock-based preflight validation
└─────────┬────────┘
          │
          ▼
┌─────────────────┐
│ Canonical Store │  State: LMDB (transactional KV)
│   (LMDB)        │  Events: Append-only log
└────────┬────────┘
         │ events
         ▼
┌─────────────────┐
│   Projector     │  Pull mode, batching, error handling
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  SQL Projection │  SQLite with cursor tracking
│   (SQLite)      │  + Dead Letter Queue
└─────────────────┘
```

## Key Concepts

### Simplified API

Azoth provides a clean, semantic API:

```rust
// Reading state
let value = ctx.get(b"key")?;          // Get typed value
let opt = ctx.get_opt(b"key")?;        // Optional get
let exists = ctx.exists(b"key")?;      // Check existence

// Writing state
ctx.set(b"key", &TypedValue::I64(42))?;     // Set value
ctx.delete(b"key")?;                         // Delete key
ctx.update(b"key", |old| {                   // Functional update
    Ok(TypedValue::I64(old.unwrap().as_i64()? + 1))
})?;

// Logging events
ctx.log("event_type", &payload)?;            // Structured event
ctx.log_many(&[("e1", data1), ("e2", data2)])?;  // Multiple events
ctx.log_bytes(b"raw_event")?;                // Raw bytes
```

### Three-Phase Transactions

1. **Preflight**: Validate constraints with stripe locks (parallel for non-conflicting keys)
2. **State Update**: Commit to LMDB (single-writer, fast)
3. **Event Append**: Write to log (sequential, very fast)

```rust
Transaction::new(&db)
    // Phase 1: Preflight with stripe locks
    .require_exists(b"key".to_vec())
    .require_min(b"balance".to_vec(), 100)
    // Phase 2 & 3: State updates + event logging
    .execute(|ctx| {
        ctx.set(b"key", &TypedValue::I64(42))?;
        ctx.log("updated", &payload)?;
        Ok(())
    })?;
```

### Event Processing with Error Handling

```rust
use azoth::{EventProcessor, ErrorStrategy};

let processor = EventProcessor::builder(db)
    .with_handler(Box::new(MyHandler))
    .with_error_strategy(ErrorStrategy::DeadLetterQueue)
    .with_dead_letter_queue(dlq)
    .build(conn);

processor.run().await?;
```

Error strategies:
- **FailFast**: Stop on first error
- **LogAndSkip**: Log error and continue
- **DeadLetterQueue**: Store failed events for replay
- **RetryWithBackoff**: Exponential backoff retry
- **Custom**: User-defined error handling

## Performance

### Benchmark Results (Single-threaded, Release Build)

| Operation | Throughput | Latency |
|-----------|------------|---------|
| **State Writes** | 11,243 tx/sec | 89μs |
| **Event Appends** | 13,092 events/sec | 76μs |
| **Batch Appends** | **979,323 events/sec** | **1μs** |
| **Atomic (State + Event)** | 10,266 tx/sec | 97μs |
| **Transaction API** | 8,849 tx/sec | 113μs |
| **State Reads** | **977,975 reads/sec** | **1μs** |
| **Event Iteration** | **1,141,680 events/sec** | **0.9μs** |

**Key Insights:**
- Batch processing provides **75x** speedup over individual appends
- Read performance is exceptional at nearly 1M reads/sec
- Single transactions maintain ~10K tx/sec with full ACID guarantees
- Event iteration is blazing fast for projector catch-up

Run benchmarks:
```bash
cargo bench --bench basic_benchmark
```

## Crates

| Crate | Purpose | LOC |
|-------|---------|-----|
| `azoth` | Unified API and utilities | 2000+ |
| `azoth-core` | Traits, types, errors | 400 |
| `azoth-lmdb` | LMDB canonical store | 800 |
| `azoth-sqlite` | SQLite projection store | 600 |
| `azoth-file-log` | File-based event log (future) | 600 |
| `azoth-projector` | Event processor | 500 |

## Examples

See the `crates/azoth/examples/` directory:

- `basic_usage.rs` - Simple CRUD operations with TypedValue
- `lambda_require.rs` - Lambda-based preflight validation
- `event_handlers.rs` - Custom event handler implementation
- `event_processor_builder.rs` - Event processing with error handling
- `custom_errors.rs` - Custom business logic errors
- `continuous_processor.rs` - Long-running event processor

## Testing

```bash
# Run all tests (52 passing ✓)
cargo test --workspace

# Run integration tests
cargo test -p azoth --test integration_test

# Run stress tests
cargo test -p azoth --test stress_test

# Run doctests
cargo test -p azoth --doc
```

## Dead Letter Queue

Failed events are automatically stored in the DLQ for later analysis:

```rust
// Setup DLQ
let dlq = Arc::new(DeadLetterQueue::new(conn.clone())?);

let processor = EventProcessor::builder(db)
    .with_error_strategy(ErrorStrategy::DeadLetterQueue)
    .with_dead_letter_queue(dlq.clone())
    .build(conn);

// Later, inspect and retry failed events
let failed = dlq.list(100)?;
for event in failed {
    println!("Failed: {}", event.error_message);
    dlq.mark_retry(event.id)?;  // Or dlq.remove(event.id)?
}
```

## Typed Values

Azoth supports rich typed values:

```rust
// Integer types
TypedValue::I64(42)
TypedValue::U256(U256::from(1000u64))

// Collections
TypedValue::Set(Set::from(["admin", "user"]))
TypedValue::Array(Array::from(vec!["event1".to_string()]))

// Raw bytes
TypedValue::Bytes(vec![1, 2, 3])

// Strings
TypedValue::String("hello".to_string())
```

## Backup & Restore

Deterministic backup with sealed snapshots:

```bash
# Backup (automatically pauses ingestion, seals, and resumes)
db.backup_to("./backup")?;

# Restore
let db = AzothDb::restore_from("./backup", "./new-data")?;
```

Backup includes:
- Sealed canonical state (LMDB)
- SQL projection with cursor
- Manifest with sealed event ID

## Status

- All core features implemented
- Comprehensive test coverage (52 tests passing)
- Clean, semantic API
- Benchmarked performance
- Dead letter queue for reliability
- Error handling strategies
- Deterministic backup/restore

## Future Work

- Multiple concurrent projections
- Automatic DLQ replay
- Metrics and observability
- Circuit breaker pattern
- Incremental snapshots

## License

MIT OR Apache-2.0
