# Architecture

## Overview

Azoth is a hybrid storage system combining:
- **LMDB** for transactional KV state
- **File-based log** for high-throughput event storage
- **SQLite** for queryable projections
- **Lock-based preflight** for parallel transaction validation

## Core Components

### 1. Canonical Store (LMDB)

The source of truth for application state.

**Structure:**
```
LmdbCanonicalStore
├── state_db (LMDB)       # KV state (transactional)
├── meta_db (LMDB)        # Metadata (EventId allocation, seal state)
└── event_log (Files)     # Events (append-only files)
```

**Responsibilities:**
- Store KV state with ACID guarantees
- Allocate EventIds monotonically
- Track seal state for deterministic backups
- Coordinate with file-based event log

### 2. File-Based Event Log

High-performance append-only event storage.

**Structure:**
```
event-log/
├── meta.json              # Metadata (next_event_id, file numbers)
├── events-00000000.log    # Log file 0
├── events-00000001.log    # Log file 1 (after rotation)
└── events-00000002.log    # Log file 2 (after rotation)
```

**Event Format:**
```
[event_id: u64][size: u32][data: bytes]
```

**Features:**
- Sequential writes (no ACID overhead)
- Automatic rotation at 512MB (configurable)
- Memory-mapped reads for fast iteration
- Buffered writes (64KB buffer)

**Performance:**
- 50-200k events/sec (5-10x faster than LMDB)
- < 1ms append latency
- ~10ms rotation time for 512MB files

### 3. Projection Store (SQLite)

Queryable SQL views derived from events.

**Structure:**
```sql
CREATE TABLE projection_meta (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    last_applied_event_id INTEGER NOT NULL DEFAULT 0,
    schema_version INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- User-defined tables created by migrations
CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    balance INTEGER NOT NULL,
    ...
);
```

**Features:**
- WAL mode for performance
- Cursor tracking (last_applied_event_id)
- Schema migrations via MigrationManager
- Batch application with atomic commits

### 4. Projector

Event processor that updates SQL projections.

**Algorithm:**
```rust
loop {
    // Get current cursor
    let cursor = projection.get_cursor()?;

    // Determine target (sealed_event_id or latest)
    let target = if sealed {
        canonical.meta()?.sealed_event_id
    } else {
        canonical.meta()?.next_event_id - 1
    };

    // Fetch batch
    let events = canonical.iter_events(cursor + 1, target)?;
    let batch = collect_batch(events, config)?;

    // Apply batch in SQL transaction
    let txn = projection.begin_txn()?;
    for event in batch {
        decoder.decode(event)?;
        applier.apply(decoded, &txn)?;
    }
    txn.set_cursor(last_event_id)?;
    txn.commit()?;
}
```

**Batching Configuration:**
- `batch_events_max`: 1000 events
- `batch_bytes_max`: 4MB
- `max_apply_latency_ms`: 100ms

## Transaction Flow

### Three-Phase Architecture

```
┌─────────────────────────────────────────────────┐
│ Phase 1: Lock-Based Preflight (Async, Parallel)│
├─────────────────────────────────────────────────┤
│ 1. Declare read/write keys upfront             │
│ 2. Acquire stripe locks (hash(key) % stripes)  │
│ 3. Run validation lambdas                      │
│ 4. Non-conflicting keys → parallel execution   │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│ Phase 2: State Update (Sync, Single-Writer)    │
├─────────────────────────────────────────────────┤
│ 1. Begin LMDB write transaction                │
│ 2. Apply state updates (puts/deletes)          │
│ 3. Allocate EventIds from metadata             │
│ 4. Commit LMDB transaction                     │
│ 5. Very fast (microseconds)                    │
└─────────────────────────────────────────────────┘
                      ↓
┌─────────────────────────────────────────────────┐
│ Phase 3: Event Append (Async, Sequential)      │
├─────────────────────────────────────────────────┤
│ 1. Write events to file log                    │
│ 2. Use pre-allocated EventIds from LMDB        │
│ 3. Sequential file I/O (very fast)             │
│ 4. Flush buffer and sync                       │
│ 5. Release stripe locks                        │
└─────────────────────────────────────────────────┘
```

### Example Transaction

```rust
TransactionBuilder::new(&db)
    // Phase 1: Declare keys and acquire locks
    .read_keys(vec![b"balance".to_vec()])
    .write_keys(vec![b"balance".to_vec()])

    // Phase 1: Validate with locks held
    .validate(|ctx| {
        let balance = ctx.get_typed(b"balance")?.as_u256()?;
        if balance < U256::from(100u64) {
            return Err(AzothError::InvalidState("Insufficient".into()));
        }
        Ok(())
    })

    // Phase 2 & 3: Execute and commit
    .execute(|ctx| {
        // Phase 2: State updates
        let balance = ctx.get_typed(b"balance")?.as_u256()?;
        ctx.put_typed(b"balance", &TypedValue::U256(balance - 100))?;

        // Phase 3: Event appends
        ctx.append_event_json("withdraw", &json!({ "amount": 100 }))?;
        Ok(())
    })?;
// Locks released after commit
```

## Lock-Based Preflight

### Stripe Locking

**Configuration:**
- Default: 256 stripes (configurable up to 1024)
- Hash function: xxhash (fast, good distribution)
- Lock type: RwLock (many readers, one writer per stripe)

**Key Distribution:**
```rust
fn stripe_index(key: &[u8]) -> usize {
    xxhash64(key) % num_stripes
}
```

**Concurrency:**
```
Non-conflicting keys → Different stripes → Parallel preflight
Conflicting keys → Same stripe → Serialized preflight
```

**Example:**
```
Transaction A: read["account1"], write["account1"]
Transaction B: read["account2"], write["account2"]
Transaction C: read["account1"], write["account3"]

A and B: Different stripes → Parallel preflight ✓
A and C: Same stripe (account1) → Serialized ✗
```

### FIFO Commit Queue

After preflight succeeds, transactions enter a FIFO commit queue:

```
Preflight (parallel) → FIFO Queue → Commit (serialized, fast)
```

**Why FIFO?**
- Deterministic execution order (critical for TEE)
- Prevents priority inversion
- Simple reasoning about state transitions

**Performance:**
- Preflight: 50-200k ops/sec (parallel)
- Commit: 20-50k/sec (serialized but very fast)
- Total throughput limited by commit rate

## EventId Allocation

### The Challenge

Two sources of truth:
1. LMDB metadata (transactional, source of truth)
2. FileEventLog (needs EventIds to write)

### The Solution

**LMDB metadata is the single source of truth:**

```rust
// In LmdbWriteTxn::allocate_event_ids()
let next_id = read_from_lmdb_meta("next_event_id")?;
let new_next_id = next_id + count;
update_lmdb_meta("next_event_id", new_next_id)?;
// EventIds: [next_id..new_next_id)

// In commit():
// 1. Commit LMDB (state + metadata)
txn.commit()?;

// 2. Write events to FileEventLog with pre-allocated IDs
event_log.append_batch_with_ids(first_event_id, &events)?;

// 3. FileEventLog updates its internal counter to match
```

**Key Properties:**
- EventIds allocated atomically with state updates
- No gaps in EventId sequence (monotonic)
- FileEventLog syncs its counter after write
- Crash recovery: LMDB metadata is authoritative

## Backup & Restore

### Pausable Backup Workflow

```rust
// 1. Pause ingestion (stop new writes)
canonical.pause_ingestion()?;

// 2. Seal at current EventId
let sealed_id = canonical.seal()?;

// 3. Projector catches up to seal
while projection.get_cursor()? < sealed_id {
    projector.run_once()?;
}

// 4. Backup both stores
canonical.backup_to("./backup")?;
projection.backup_to("./backup")?;

// 5. Write manifest
let manifest = BackupManifest {
    sealed_event_id: sealed_id,
    canonical_backend: "lmdb",
    projection_cursor: sealed_id,
    schema_version: 1,
    timestamp: Utc::now(),
};
write_manifest("./backup/manifest.json", &manifest)?;

// 6. Resume ingestion
canonical.resume_ingestion()?;
```

### Restore Workflow

```rust
// 1. Read manifest
let manifest = read_manifest("./backup/manifest.json")?;

// 2. Restore canonical store
let canonical = LmdbCanonicalStore::restore_from("./backup", config)?;

// 3. Restore projection store
let projection = SqliteProjectionStore::restore_from("./backup", config)?;

// 4. Verify cursor matches sealed_event_id
assert_eq!(projection.get_cursor()?, manifest.sealed_event_id);

// 5. Resume normal operation
// Projector will catch up to any new events
```

## Migration System

### Schema Versioning

```rust
pub trait Migration: Send + Sync {
    fn version(&self) -> u32;
    fn name(&self) -> &str;
    fn up(&self, conn: &Connection) -> Result<()>;
    fn down(&self, conn: &Connection) -> Result<()>;
}
```

### Example Migration

```rust
struct CreateAccountsTable;

impl Migration for CreateAccountsTable {
    fn version(&self) -> u32 { 2 }
    fn name(&self) -> &str { "create_accounts_table" }

    fn up(&self, conn: &Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        )?;
        Ok(())
    }

    fn down(&self, conn: &Connection) -> Result<()> {
        conn.execute("DROP TABLE accounts", [])?;
        Ok(())
    }
}

// Usage
let mut manager = MigrationManager::new();
manager.add(Box::new(CreateAccountsTable));
manager.run(db.projection())?;
```

## Performance Characteristics

### Throughput

| Operation | Rate | Notes |
|-----------|------|-------|
| Preflight (non-conflicting) | 50-200k/sec | Parallel validation |
| Preflight (conflicting) | 10-50k/sec | Serialized per stripe |
| Commits | 20-50k/sec | Serialized, fast |
| Event writes | 50-200k/sec | File-based log |
| Projection updates | 1-5k/sec | Depends on SQL complexity |

### Latency (p50)

| Operation | Latency | Notes |
|-----------|---------|-------|
| Preflight | 100-500μs | With locks |
| State commit | 50-200μs | LMDB write |
| Event append | 100-500μs | Buffered |
| Full transaction | 1-5ms | End-to-end |

### Resource Usage

| Resource | Usage | Notes |
|----------|-------|-------|
| LMDB map size | 1-10GB | State only (no events) |
| Event log disk | Unbounded | Rotation + archival |
| SQLite size | 100MB-10GB | Depends on projections |
| Memory | 100-500MB | Buffers + caches |

## Design Trade-offs

### Why File-Based Events?

**Advantages:**
- 5-10x faster writes than LMDB
- No ACID overhead for append-only data
- Easy rotation and archival
- Reduced LMDB database size

**Disadvantages:**
- Two-phase commit (LMDB then file)
- EventId synchronization complexity
- Less transactional guarantees

**Mitigation:**
- LMDB metadata is source of truth
- FileEventLog uses pre-allocated EventIds
- Crash recovery handled by LMDB

### Why Single-Writer?

**Advantages:**
- Deterministic execution order (TEE requirement)
- No conflict resolution needed
- Simpler implementation
- Faster commits (no coordination)

**Disadvantages:**
- Commit throughput limited to 20-50k/sec
- Cannot scale horizontally

**Mitigation:**
- Preflight parallelism (50-200k/sec)
- Fast commits keep queue depth low
- Good enough for single-node use case

### Why Pull-Based Projector?

**Advantages:**
- Backpressure handling (projector sets pace)
- Easy to rebuild (just re-process events)
- No coordination with canonical store

**Disadvantages:**
- Projection lag (typically <100ms)
- Extra complexity vs push

**Mitigation:**
- Batching reduces overhead
- Applications query canonical if needed
- Lag monitoring built-in

## Security Considerations

### TEE Safety

- Single-writer eliminates need for coordination
- Deterministic execution order via FIFO queue
- Pausable backup prevents partial state
- Sealed snapshots for provable history

### Event Integrity

- EventIds are monotonic and gapless
- Events cannot be modified after write
- Rotation preserves all events
- IPFS archival provides immutability (planned)

### State Integrity

- ACID guarantees from LMDB
- Atomic state + event commits
- Cursor tracking ensures consistency
- Schema migrations versioned
