//! Event Processor Builder Example
//!
//! Demonstrates the new builder API for continuous event processing.
//! This is the recommended way to set up long-running event processors.
//!
//! Run with: cargo run --example event_processor_builder

use azoth::prelude::*;
use azoth::{EventHandler, EventProcessor};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

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

// Deposit handler
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

    fn validate(&self, _payload: &[u8]) -> azoth::Result<()> {
        Ok(())
    }
}

// Withdraw handler
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

    fn validate(&self, _payload: &[u8]) -> azoth::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("azoth=info")
        .init();

    println!("=== Event Processor Builder Example ===\n");

    let temp_dir = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp_dir.path())?);

    // Setup SQL table
    let db_path = temp_dir.path().join("accounts.db");
    let conn =
        Arc::new(Connection::open(&db_path).map_err(|e| AzothError::Projection(e.to_string()))?);

    conn.execute(
        "CREATE TABLE accounts (
            id INTEGER PRIMARY KEY,
            balance INTEGER NOT NULL DEFAULT 0
        )",
        [],
    )
    .map_err(|e| AzothError::Projection(e.to_string()))?;

    println!("✅ Database and SQL table created\n");

    // ========================================
    // Build the event processor
    // ========================================
    println!("🔨 Building event processor...");

    let mut processor = EventProcessor::builder(db.clone())
        .with_handler(Box::new(DepositHandler))
        .with_handler(Box::new(WithdrawHandler))
        .with_poll_interval(Duration::from_millis(50))
        .with_batch_size(10)
        .build(conn.clone());

    println!("   ✓ Registered 2 handlers");
    println!("   ✓ Poll interval: 50ms");
    println!("   ✓ Batch size: 10 events\n");

    // ========================================
    // Write some events first
    // ========================================
    println!("✍️  Writing events...\n");

    for i in 0..5 {
        let account_id = (i % 2) + 1;

        if i % 2 == 0 {
            // Deposit
            Transaction::new(&*db).execute(|ctx| {
                ctx.log(
                    "deposit",
                    &DepositPayload {
                        account_id,
                        amount: (100 * (i + 1)) as i64,
                    },
                )?;
                Ok(())
            })?;
            println!("   📝 Wrote deposit event for account {}", account_id);
        } else {
            // Withdraw
            Transaction::new(&*db).execute(|ctx| {
                ctx.log(
                    "withdraw",
                    &WithdrawPayload {
                        account_id,
                        amount: 50,
                    },
                )?;
                Ok(())
            })?;
            println!("   📝 Wrote withdraw event for account {}", account_id);
        }
    }

    println!("\n✍️  Finished writing events\n");

    // ========================================
    // Process events
    // ========================================
    println!("🚀 Processing events...\n");

    // Process all events (will return when caught up)
    while processor.lag()? > 0 {
        processor.process_batch()?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    println!("✓ All events processed\n");

    // ========================================
    // Check final balances
    // ========================================
    println!("📊 Final Account Balances:\n");

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
    println!("\n💡 Key Features:");
    println!("   ✓ Builder pattern for clean, readable setup");
    println!("   ✓ Automatic handler registration");
    println!("   ✓ Configurable poll interval and batch size");
    println!("   ✓ Graceful shutdown with shutdown handles");
    println!("   ✓ Async/await support for modern Rust");
    println!("   ✓ Easy to extend with new handlers\n");

    Ok(())
}
