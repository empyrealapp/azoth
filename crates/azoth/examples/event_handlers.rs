//! Event Handlers Example
//!
//! Demonstrates:
//! - Defining event handlers that transform events to SQL
//! - Registering handlers
//! - Processing events through the registry
//!
//! Run with: cargo run --example event_handlers

use azoth::prelude::*;
use azoth::{EventHandler, EventHandlerRegistry};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct DepositPayload {
    account_id: u64,
    amount: i64,
}

#[derive(Debug, Serialize, Deserialize)]
struct WithdrawPayload {
    account_id: u64,
    amount: i64,
    fee: i64,
}

// Handler for deposit events
struct DepositHandler;

impl EventHandler for DepositHandler {
    fn event_type(&self) -> &str {
        "deposit"
    }

    fn handle(&self, conn: &Connection, event_id: EventId, payload: &[u8]) -> azoth::Result<()> {
        // Decode payload JSON
        let deposit: DepositPayload =
            serde_json::from_slice(payload).map_err(|e| AzothError::EventDecode(e.to_string()))?;

        println!("Processing deposit event {}: {:?}", event_id, deposit);

        // Update SQL table
        conn.execute(
            "INSERT INTO accounts (id, balance) VALUES (?1, ?2)
             ON CONFLICT(id) DO UPDATE SET balance = balance + ?2",
            [deposit.account_id as i64, deposit.amount],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }

    fn validate(&self, payload: &[u8]) -> azoth::Result<()> {
        // Preflight validation
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
            "Processing withdraw event {}: account={}, amount={}, fee={}",
            event_id, withdraw.account_id, withdraw.amount, withdraw.fee
        );

        let total = withdraw.amount + withdraw.fee;

        conn.execute(
            "UPDATE accounts SET balance = balance - ?1 WHERE id = ?2",
            [total, withdraw.account_id as i64],
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
        if withdraw.fee < 0 {
            return Err(AzothError::PreflightFailed("Fee cannot be negative".into()));
        }
        Ok(())
    }
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("azoth=info")
        .init();

    println!("=== Event Handlers Example ===\n");

    let temp_dir = tempfile::tempdir()?;
    let db = AzothDb::open(temp_dir.path())?;

    // ========================================
    // 1. Setup: Create SQL table
    // ========================================
    println!("1. Setting up SQL table...");

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

    println!("✓ SQL table created\n");

    // ========================================
    // 2. Register Event Handlers
    // ========================================
    println!("2. Registering event handlers...");

    let mut registry = EventHandlerRegistry::new();
    registry.register(Box::new(DepositHandler));
    registry.register(Box::new(WithdrawHandler));

    println!("✓ Handlers registered: {:?}\n", registry.event_types());

    // ========================================
    // 3. Write Events to Canonical Store
    // ========================================
    println!("3. Writing events...");

    let mut txn = db.canonical().write_txn()?;

    // Deposit event
    let deposit = DepositPayload {
        account_id: 1,
        amount: 1000,
    };
    let deposit_json = serde_json::to_string(&deposit).unwrap();
    let deposit_event = format!("deposit:{}", deposit_json);
    txn.append_event(deposit_event.as_bytes())?;

    // Another deposit
    let deposit2 = DepositPayload {
        account_id: 1,
        amount: 500,
    };
    let deposit2_json = serde_json::to_string(&deposit2).unwrap();
    let deposit2_event = format!("deposit:{}", deposit2_json);
    txn.append_event(deposit2_event.as_bytes())?;

    // Withdraw event
    let withdraw = WithdrawPayload {
        account_id: 1,
        amount: 200,
        fee: 10,
    };
    let withdraw_json = serde_json::to_string(&withdraw).unwrap();
    let withdraw_event = format!("withdraw:{}", withdraw_json);
    txn.append_event(withdraw_event.as_bytes())?;

    txn.commit()?;

    println!("✓ Events written to canonical store\n");

    // ========================================
    // 4. Process Events through Handlers
    // ========================================
    println!("4. Processing events...");

    let mut iter = db.canonical().iter_events(0, None)?;
    let mut event_id = 0;

    while let Some((id, bytes)) = iter.next()? {
        registry.process(&conn, id, &bytes)?;
        event_id = id;
    }

    println!("✓ Processed {} events\n", event_id + 1);

    // ========================================
    // 5. Query Final State
    // ========================================
    println!("5. Querying final balance...");

    let balance: i64 = conn
        .query_row("SELECT balance FROM accounts WHERE id = 1", [], |row| {
            row.get(0)
        })
        .map_err(|e| AzothError::Projection(e.to_string()))?;

    println!("✓ Account 1 balance: {}", balance);
    println!("   (1000 + 500 - 200 - 10 = 1290)\n");

    // ========================================
    // 6. Validation Example
    // ========================================
    println!("6. Testing validation...");

    let invalid_deposit = DepositPayload {
        account_id: 1,
        amount: -100, // Invalid: negative
    };
    let invalid_json = serde_json::to_string(&invalid_deposit).unwrap();
    let invalid_event = format!("deposit:{}", invalid_json);

    match registry.validate(invalid_event.as_bytes()) {
        Ok(_) => println!("✗ Validation should have failed"),
        Err(e) => println!("✓ Validation correctly rejected: {}", e),
    }

    println!("\n=== Example Complete ===");
    println!("\nKey Points:");
    println!("- Event handlers transform events into SQL operations");
    println!("- Handlers can validate events during preflight");
    println!("- Structured events (JSON) make data handling type-safe");
    println!("- Handlers can be added/removed dynamically");

    Ok(())
}
