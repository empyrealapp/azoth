# azoth-bus

Multi-consumer pub/sub event bus built on Azoth's embedded database primitives.

## Overview

Azoth Bus provides durable, multi-consumer event streaming with independent cursors, load-balanced consumer groups, and automatic retention policies. It's built as a pure compositional layer on top of Azoth's LMDB-backed event log, requiring no modifications to Azoth core.

**New in 0.2:** Real-time notifications - consumers wake instantly when events are published, eliminating polling overhead.

## Quick Start

```rust
use azoth::AzothDb;
use azoth_bus::EventBus;
use std::sync::Arc;

// Open database and create bus with notification support
let db = Arc::new(AzothDb::open("./data")?);
let bus = EventBus::with_notifications(db.clone());

// Publish events using the bus API (auto-notifies consumers)
bus.publish("orders", "created", &serde_json::json!({
    "order_id": "12345",
    "amount": 99.99
}))?;

// Subscribe and consume
let mut consumer = bus.subscribe("orders", "processor")?;
while let Some(event) = consumer.next()? {
    println!("Processing: {}", event.event_type);
    consumer.ack(event.id)?;
}
```

## Features

### Real-Time Event Streaming (NEW)
- **Instant wake-up**: Consumers wake immediately when events are published - no polling required
- **Notification-based**: Uses `tokio::sync::Notify` for sub-millisecond latency
- **Atomic publish + notify**: Events are committed and consumers notified in one operation
- **Async-safe**: `publish_async()` and `ack_async()` for use in async contexts

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
- **Stream trait**: `futures::Stream` implementation for ergonomic async iteration

### Stream Processing & Actor Patterns
- **Transform pipelines**: Chain agents that consume, transform, and publish to other streams
- **Actor mailboxes**: Messages accumulate durably until agents process them
- **Offline tolerance**: Agents can go offline and catch up on missed messages later
- **Independent cursors**: Each agent tracks its own position in each stream

### Retention Policies
- **Automatic cleanup**: Configure per-stream retention (KeepAll, KeepCount, KeepDays)
- **Safe compaction**: Never deletes events still needed by slow consumers
- **Background compaction**: Optional continuous compaction task

## Usage

### Publishing Events (Recommended)

Use `bus.publish()` for automatic event formatting and consumer notification:

```rust
// Synchronous publish (for non-async contexts)
let event_id = bus.publish("orders", "created", &json!({"id": 123}))?;

// Async publish (for async contexts - uses spawn_blocking internally)
let event_id = bus.publish_async("orders", "created", json!({"id": 123})).await?;

// Batch publish (atomic)
let (first, last) = bus.publish_batch("orders", &[
    ("created", json!({"id": 1})),
    ("updated", json!({"id": 1, "status": "paid"})),
])?;
```

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

### Real-Time Async Processing

For instant wake-up when events arrive:

```rust
// Create bus with notification support
let bus = EventBus::with_notifications(db);

// Create consumer (do this before entering async context)
let mut consumer = bus.subscribe("tasks", "worker")?;

// In async context: consumer wakes instantly on publish
loop {
    match consumer.next_async().await? {
        Some(event) => {
            process(&event).await?;
            consumer.ack_async(event.id).await?; // Async-safe ack
        }
        None => break,
    }
}
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

### Stream API

Use `futures::Stream` for ergonomic async iteration:

```rust
use futures::StreamExt;
use azoth_bus::{event_stream, auto_ack_stream};

// Manual acknowledgment stream
let stream = event_stream(consumer);
futures::pin_mut!(stream);

while let Some(result) = stream.next().await {
    let event = result?;
    // Process event...
}

// Auto-acknowledging stream (fire-and-forget)
let stream = auto_ack_stream(consumer);
futures::pin_mut!(stream);

while let Some(result) = stream.next().await {
    let event = result?;
    println!("Processed: {}", event.id);
    // Event is auto-acknowledged
}
```

### Stream Processing Pipeline

Build data transformation pipelines where agents consume, transform, and forward:

```rust
// Agent 1: Consume from "numbers", transform, publish to "doubled"
async fn doubler_agent(bus: EventBus, mut consumer: Consumer) {
    while let Some(event) = consumer.next_async().await? {
        let value: i64 = parse_value(&event.payload);
        
        // Transform and forward to next stream
        bus.publish_async("doubled", "value", json!({
            "original": value,
            "result": value * 2
        })).await?;
        
        consumer.ack_async(event.id).await?;
    }
}

// Agent 2: Consume from "doubled", print results
async fn printer_agent(mut consumer: Consumer) {
    while let Some(event) = consumer.next_async().await? {
        println!("Result: {:?}", event.payload);
        consumer.ack_async(event.id).await?;
    }
}
```

### Actor Mailbox Pattern

Use streams as durable mailboxes - messages accumulate until actors process them:

```rust
// Send messages while actor is offline
bus.publish("actor-a", "task", &json!({"id": 1}))?;
bus.publish("actor-a", "task", &json!({"id": 2}))?;

// Later: actor comes online and processes accumulated messages
let mut actor = bus.subscribe("actor-a", "worker")?;

// Check how many messages are waiting
let info = bus.consumer_info("actor-a", "worker")?;
println!("Mailbox has {} messages", info.lag);

// Process all waiting messages
while let Some(event) = actor.next()? {
    process_task(&event)?;
    actor.ack(event.id)?;
}
```

Key properties:
- **Durable**: Messages survive process restarts
- **Independent cursors**: Each actor tracks its own position
- **Offline tolerance**: Actors can go offline and catch up later
- **Buffered or real-time**: Use `Poll` for buffered, `Notify` for instant wake-up

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

## Wake Strategies

Azoth Bus supports two wake strategies for async consumers:

| Strategy | Use Case | Latency | CPU Usage |
|----------|----------|---------|-----------|
| `Poll` (default) | Simple setups, testing | ~10ms (configurable) | Higher |
| `Notify` | Real-time, production | Sub-millisecond | Minimal |

```rust
use azoth_bus::{EventBus, WakeStrategy};
use std::time::Duration;

// Polling (checks every 10ms by default)
let bus = EventBus::new(db.clone());

// Custom poll interval
let bus = EventBus::with_wake_strategy(
    db.clone(),
    WakeStrategy::poll(Duration::from_millis(5))
);

// Notification-based (instant wake-up)
let bus = EventBus::with_notifications(db.clone());
```

## Architecture

Azoth Bus stores all metadata in LMDB using structured key prefixes:

```
bus:consumer:{stream}:{name}:cursor      -> Last acked event ID
bus:consumer:{stream}:{name}:meta        -> Consumer metadata (JSON)
bus:group:{stream}:{group}:cursor        -> Next event to claim
bus:group:{stream}:{group}:claim:{id}    -> Claim info with lease
bus:group:{stream}:{group}:member:{id}   -> Member metadata
bus:group:{stream}:{group}:reclaim       -> Nacked events for retry
bus:stream:{name}:config                 -> Retention policy (JSON)
```

Event iteration uses Azoth's existing event log APIs. Cursor updates use Azoth's transaction system for atomicity.

## Examples

```bash
# Simple consumer
cargo run -p azoth-bus --example simple_consumer

# Multi-consumer with independent cursors
cargo run -p azoth-bus --example multi_consumer

# Async consumption with polling
cargo run -p azoth-bus --example async_consumer

# Real-time notifications (instant wake-up)
cargo run -p azoth-bus --example async_notifications

# Stream processing pipeline (Generator -> Doubler -> Printer)
cargo run -p azoth-bus --example stream_processing

# Actor mailbox pattern (buffered message consumption)
cargo run -p azoth-bus --example actor_mailbox

# Consumer groups (load balancing)
cargo run -p azoth-bus --example consumer_group

# Retention policies
cargo run -p azoth-bus --example retention
```

## Testing

```bash
cargo test -p azoth-bus
```

**Test Coverage (43 tests):**
- Core consumer functionality (creation, ack, seek, filtering)
- Independent cursors and lag monitoring
- Event filtering (prefix, exact, and, or)
- Async notifications (poll and notify strategies)
- Retention policies (KeepAll, KeepCount)
- Consumer groups (claims, releases, expiration)
- Nack/reclaim with forward progress semantics
- LMDB cursor edge cases (empty ranges, sequential calls)
- **Publish API** (publish, publish_batch, publish_raw)
- **Stream API** (event_stream, auto_ack_stream)

## Implementation Notes

### Consumer Group Forward Progress

Consumer groups prioritize making forward progress over retrying failed events:
1. Fresh events are claimed first (advancing the cursor)
2. Nacked events are pushed to a reclaim list
3. Once caught up, reclaim list is retried (LIFO)

This ensures transient failures don't block new event processing.

### Async Transaction Safety

Azoth's `Transaction` API panics when called from async contexts to prevent deadlocks. Use:
- `bus.publish()` / `consumer.ack()` - for synchronous code
- `bus.publish_async()` / `consumer.ack_async()` - for async code

Consumer creation (`bus.subscribe()`) uses synchronous transactions, so create consumers before entering the async runtime or use `spawn_blocking`.

### LMDB Cursor Workarounds

The implementation works around two LMDB cursor issues:

1. **Sequential iterator creation**: Sequential calls to `iter_events()` with different positions can fail. The implementation uses batch iteration with larger limits instead.

2. **Empty range scans**: LMDB panics when scanning empty key ranges. The `list_consumers()` function uses `std::panic::catch_unwind` to handle this gracefully.

## Known Limitations

- **KeepCount retention**: Works globally on the entire event log, not per-stream (all streams share the same log)
- **KeepDays retention**: Not yet implemented (requires event timestamps)
- **Actual deletion**: Compaction calculates what to delete but doesn't execute deletion yet (requires `event_log.delete_range()` implementation)

## Performance

- Cursor updates: Atomic via LMDB write locks
- Event iteration: Efficient sequential LMDB reads
- Concurrent consumers: Supported via LMDB MVCC
- Notification latency: Sub-millisecond with `WakeStrategy::Notify`
- Target throughput: 50k+ events/sec per consumer

## License

MIT OR Apache-2.0
