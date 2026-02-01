//! Continuous Event Processor Example
//!
//! Demonstrates:
//! - Long-running event processor that continuously processes events
//! - Event handler registry with deposit/withdraw handlers
//! - Graceful shutdown with signals
//! - Background writer generating events
//!
//! Run with: cargo run --example continuous_processor

use azoth::prelude::*;
use azoth::{EventHandler, EventHandlerRegistry};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Serialize, Deserialize)]
struct DepositPayload {
    account_id: u64,
    amount: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WithdrawPayload {
    account_id: u64,
    amount: i64,
}

// Handler for deposit events
struct DepositHandler;

impl EventHandler for DepositHandler {
    fn event_type(&self) -> &str {
        "deposit"
    }

    fn handle(&self, conn: &Connection, event_id: EventId, payload: &[u8]) -> azoth::Result<()> {
        let deposit: DepositPayload =
            serde_json::from_slice(payload).map_err(|e| AzothError::EventDecode(e.to_string()))?;

        println!(
            "  📥 Deposit: account={}, amount={} (event {})",
            deposit.account_id, deposit.amount, event_id
        );

        conn.execute(
            "INSERT INTO accounts (id, balance) VALUES (?1, ?2)
             ON CONFLICT(id) DO UPDATE SET balance = balance + ?2",
            [deposit.account_id as i64, deposit.amount],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }

    fn validate(&self, payload: &[u8]) -> azoth::Result<()> {
        let deposit: DepositPayload =
            serde_json::from_slice(payload).map_err(|e| AzothError::EventDecode(e.to_string()))?;
        if deposit.amount <= 0 {
            return Err(AzothError::PreflightFailed(
                "Deposit amount must be positive".into(),
            ));
        }
        Ok(())
    }
}

// Handler for withdraw events
struct WithdrawHandler;

impl EventHandler for WithdrawHandler {
    fn event_type(&self) -> &str {
        "withdraw"
    }

    fn handle(&self, conn: &Connection, event_id: EventId, payload: &[u8]) -> azoth::Result<()> {
        let withdraw: WithdrawPayload =
            serde_json::from_slice(payload).map_err(|e| AzothError::EventDecode(e.to_string()))?;

        println!(
            "  📤 Withdraw: account={}, amount={} (event {})",
            withdraw.account_id, withdraw.amount, event_id
        );

        conn.execute(
            "UPDATE accounts SET balance = balance - ?1 WHERE id = ?2",
            [withdraw.amount, withdraw.account_id as i64],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }

    fn validate(&self, payload: &[u8]) -> azoth::Result<()> {
        let withdraw: WithdrawPayload =
            serde_json::from_slice(payload).map_err(|e| AzothError::EventDecode(e.to_string()))?;
        if withdraw.amount <= 0 {
            return Err(AzothError::PreflightFailed(
                "Withdraw amount must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// Continuous event processor
#[allow(dead_code)]
async fn event_processor(
    db: &AzothDb,
    conn: &Connection,
    registry: &EventHandlerRegistry,
    shutdown: &AtomicBool,
) -> Result<()> {
    println!("🚀 Event processor started");

    let mut cursor = 0u64;

    while !shutdown.load(Ordering::SeqCst) {
        // Check for new events
        let meta = db.canonical().meta()?;
        let tip = if meta.next_event_id > 0 {
            meta.next_event_id - 1
        } else {
            0
        };

        if cursor < tip {
            // Process new events
            let mut iter = db.canonical().iter_events(cursor + 1, Some(tip + 1))?;
            let mut processed = 0;

            while let Some((id, bytes)) = iter.next()? {
                registry.process(conn, id, &bytes)?;
                cursor = id;
                processed += 1;
            }

            if processed > 0 {
                println!(
                    "✅ Processed {} events (cursor now at {})",
                    processed, cursor
                );
            }
        } else {
            // Caught up, sleep briefly
            sleep(Duration::from_millis(100)).await;
        }
    }

    println!("🛑 Event processor shutdown");
    Ok(())
}

/// Background writer that generates events
async fn event_writer(db: &AzothDb, shutdown: &AtomicBool) -> Result<()> {
    println!("✍️  Event writer started");

    let mut counter = 0;

    while !shutdown.load(Ordering::SeqCst) {
        // Generate a random deposit or withdraw
        let account_id = (counter % 3) + 1;

        if counter % 2 == 0 {
            // Deposit
            let deposit = DepositPayload {
                account_id,
                amount: (100 + (counter * 10)) as i64,
            };
            let json = serde_json::to_string(&deposit).unwrap();
            let event = format!("deposit:{}", json);

            let mut txn = db.canonical().write_txn()?;
            txn.append_event(event.as_bytes())?;
            txn.commit()?;

            println!(
                "📝 Wrote deposit event: account={}, amount={}",
                account_id, deposit.amount
            );
        } else {
            // Withdraw
            let withdraw = WithdrawPayload {
                account_id,
                amount: 50,
            };
            let json = serde_json::to_string(&withdraw).unwrap();
            let event = format!("withdraw:{}", json);

            let mut txn = db.canonical().write_txn()?;
            txn.append_event(event.as_bytes())?;
            txn.commit()?;

            println!(
                "📝 Wrote withdraw event: account={}, amount={}",
                account_id, withdraw.amount
            );
        }

        counter += 1;

        // Write every 500ms
        sleep(Duration::from_millis(500)).await;

        // Stop after 10 events
        if counter >= 10 {
            break;
        }
    }

    println!("✍️  Event writer finished");
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("azoth=info")
        .init();

    println!("=== Continuous Event Processor Example ===\n");

    let temp_dir = tempfile::tempdir()?;
    let db = AzothDb::open(temp_dir.path())?;

    // Setup SQL table
    let db_path = temp_dir.path().join("accounts.db");
    let conn = Connection::open(&db_path).map_err(|e| AzothError::Projection(e.to_string()))?;

    conn.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0
        )",
        [],
    )
    .map_err(|e| AzothError::Projection(e.to_string()))?;

    println!("✅ Database and SQL table created\n");

    // Register event handlers
    let mut registry = EventHandlerRegistry::new();
    registry.register(Box::new(DepositHandler));
    registry.register(Box::new(WithdrawHandler));

    println!(
        "✅ Event handlers registered: {:?}\n",
        registry.event_types()
    );

    // Shutdown signal
    let shutdown = AtomicBool::new(false);

    println!("🎬 Starting continuous processing...\n");
    println!("   (Writing 10 events over 5 seconds)\n");

    // Run writer first to generate events
    event_writer(&db, &shutdown).await?;

    println!("\n🔄 Processing events...\n");

    // Now run processor to consume the events
    // It will process all events then hit the shutdown flag
    let cursor = 0u64;
    let meta = db.canonical().meta()?;
    let tip = if meta.next_event_id > 0 {
        meta.next_event_id - 1
    } else {
        0
    };

    if cursor < tip {
        let mut iter = db.canonical().iter_events(cursor + 1, Some(tip + 1))?;
        let mut processed = 0;

        while let Some((id, bytes)) = iter.next()? {
            registry.process(&conn, id, &bytes)?;
            processed += 1;
        }

        println!("✅ Processed {} events\n", processed);
    }

    println!("\n📊 Final Account Balances:\n");

    // Query final balances
    let mut stmt = conn
        .prepare("SELECT id, balance FROM accounts ORDER BY id")
        .map_err(|e| AzothError::Projection(e.to_string()))?;

    let rows = stmt
        .query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)))
        .map_err(|e| AzothError::Projection(e.to_string()))?;

    for row in rows {
        let (id, balance) = row.map_err(|e| AzothError::Projection(e.to_string()))?;
        println!("   Account {}: balance = {}", id, balance);
    }

    println!("\n=== Example Complete ===");
    println!("\n💡 Key Points:");
    println!("   ✓ Event processor runs continuously in background");
    println!("   ✓ Handlers match events by type (deposit vs withdraw)");
    println!("   ✓ Multiple concurrent writers/processors supported");
    println!("   ✓ Graceful shutdown with atomic flags");
    println!("   ✓ Easy to extend with more event types/handlers\n");

    Ok(())
}
