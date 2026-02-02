# azoth-bus

Multi-consumer pub/sub event bus built on Azoth's embedded database primitives.

## Overview

Azoth Bus provides durable, multi-consumer event streaming with independent cursors, load-balanced consumer groups, and automatic retention policies. It's built as a pure compositional layer on top of Azoth's LMDB-backed event log, requiring no modifications to Azoth core.

## Quick Start

```rust
use azoth::{AzothDb, Transaction};
use azoth_bus::EventBus;
use std::sync::Arc;

// Open database and create bus
let db = Arc::new(AzothDb::open("./data")?);
let bus = EventBus::new(db.clone());

// Publish events
Transaction::new(&db).execute(|ctx| {
    ctx.log("orders:created", &serde_json::json!({
        "order_id": "12345",
        "amount": 99.99
    }))?;
    Ok(())
})?;

// Subscribe and consume
let mut consumer = bus.subscribe("orders", "processor")?;
while let Some(event) = consumer.next()? {
    println!("Processing: {}", event.event_type);
    consumer.ack(event.id)?;
}
```

## Features

### Multi-Consumer Event Processing
- **Independent cursors**: Each consumer tracks its own position
- **Stream filtering**: Subscribe to `"orders"` to automatically see only `"orders:*"` events
- **Composable filters**: Combine prefix, exact, and/or filters for fine-grained control
- **Lag monitoring**: Track how far behind each consumer is from the head

### Consumer Groups (Load Balancing)
- **Parallel processing**: Multiple workers claim and process events concurrently
- **No duplicates**: Atomic claim mechanism ensures each event is processed exactly once
- **Fault tolerance**: Lease-based expiration automatically reclaims stale claims
- **Nack and retry**: Failed events can be nacked and retried with forward progress semantics

### Async Support
- **Non-blocking consumption**: Use `next_async()` for async/await workloads
- **Pluggable wake strategies**: Poll-based (default) or notification-based (Tokio Notify)

### Retention Policies
- **Automatic cleanup**: Configure per-stream retention (KeepAll, KeepCount, KeepDays)
- **Safe compaction**: Never deletes events still needed by slow consumers
- **Background compaction**: Optional continuous compaction task

## Usage

### Basic Consumer

```rust
// Subscribe to stream (auto-filters to "orders:*" events)
let mut consumer = bus.subscribe("orders", "my-consumer")?;

// Read and acknowledge events
while let Some(event) = consumer.next()? {
    process(&event)?;
    consumer.ack(event.id)?; // Advances cursor
}

// Check consumer status
let info = bus.consumer_info("orders", "my-consumer")?;
println!("Position: {}, Lag: {}", info.position, info.lag);
```

### Consumer Groups

```rust
use std::time::Duration;

// Create consumer group
let group = bus.consumer_group("orders", "workers");

// Join as a member
let mut worker = group.join("worker-1")?;

// Claim and process events
loop {
    if let Some(claimed) = worker.claim_next()? {
        match process_order(&claimed.event).await {
            Ok(_) => worker.release(claimed.event.id, true)?,  // Ack
            Err(_) => worker.release(claimed.event.id, false)?, // Nack for retry
        }
    } else {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
```

### Async Consumption

```rust
// Use async iterator
while let Some(event) = consumer.next_async().await? {
    process(&event).await?;
    consumer.ack(event.id)?;
}
```

### Retention Policies

```rust
use azoth_bus::RetentionManager;

let mgr = RetentionManager::new(db);

// Set retention per stream
mgr.set_retention("logs", RetentionPolicy::KeepCount(1000))?;
mgr.set_retention("events", RetentionPolicy::KeepDays(7))?;

// Run compaction
let stats = mgr.compact("logs")?;
println!("Deleted {} events", stats.deleted);

// Or run continuous background compaction
tokio::spawn(async move {
    mgr.run_continuous(Duration::from_hours(1)).await;
});
```

### Event Filters

```rust
// Additional filtering within a stream
let consumer = bus.subscribe("orders", "processor")?
    .with_filter(EventFilter::prefix("orders:created")
        .or(EventFilter::prefix("orders:updated")));
```

## Architecture

Azoth Bus stores all metadata in LMDB using structured key prefixes:

```
bus:consumer:{stream}:{name}:cursor      → Last acked event ID
bus:consumer:{stream}:{name}:meta        → Consumer metadata (JSON)
bus:group:{stream}:{group}:cursor        → Next event to claim
bus:group:{stream}:{group}:claim:{id}    → Claim info with lease
bus:group:{stream}:{group}:member:{id}   → Member metadata
bus:group:{stream}:{group}:reclaim       → Nacked events for retry
bus:stream:{name}:config                 → Retention policy (JSON)
```

Event iteration uses Azoth's existing event log APIs. Cursor updates use Azoth's transaction system for atomicity.

## Examples

```bash
# Simple consumer
cargo run -p azoth-bus --example simple_consumer

# Multi-consumer with independent cursors
cargo run -p azoth-bus --example multi_consumer

# Async consumption
cargo run -p azoth-bus --example async_consumer

# Consumer groups (load balancing)
cargo run -p azoth-bus --example consumer_group

# Retention policies
cargo run -p azoth-bus --example retention
```

## Testing

```bash
cargo test -p azoth-bus
```

**Test Coverage (33 tests):**
- Core consumer functionality (creation, ack, seek, filtering)
- Independent cursors and lag monitoring
- Event filtering (prefix, exact, and, or)
- Async notifications (poll and notify strategies)
- Retention policies (KeepAll, KeepCount)
- Consumer groups (claims, releases, expiration)
- Nack/reclaim with forward progress semantics
- LMDB cursor edge cases (empty ranges, sequential calls)

## Implementation Notes

### Consumer Group Forward Progress

Consumer groups prioritize making forward progress over retrying failed events:
1. Fresh events are claimed first (advancing the cursor)
2. Nacked events are pushed to a reclaim list
3. Once caught up, reclaim list is retried (LIFO)

This ensures transient failures don't block new event processing.

### LMDB Cursor Workarounds

The implementation works around two LMDB cursor issues:

1. **Sequential iterator creation**: Sequential calls to `iter_events()` with different positions can fail. The implementation uses batch iteration with larger limits instead.

2. **Empty range scans**: LMDB panics when scanning empty key ranges. The `list_consumers()` function uses `std::panic::catch_unwind` to handle this gracefully.

## Known Limitations

- **KeepCount retention**: Works globally on the entire event log, not per-stream (all streams share the same log)
- **KeepDays retention**: Not yet implemented (requires event timestamps)
- **Actual deletion**: Compaction calculates what to delete but doesn't execute deletion yet (requires `event_log.delete_range()` implementation)

## Future Features

- **Named streams**: Multiple logical event logs for true isolation
- **Value encryption**: At-rest encryption for sensitive event data

## Performance

- Cursor updates: Atomic via LMDB write locks
- Event iteration: Efficient sequential LMDB reads
- Concurrent consumers: Supported via LMDB MVCC
- Target throughput: 50k+ events/sec per consumer

## License

MIT OR Apache-2.0
