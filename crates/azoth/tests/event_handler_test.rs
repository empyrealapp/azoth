//! Tests for event handler system

use azoth::prelude::*;
use azoth::{EventHandler, EventHandlerRegistry, EventProcessor};
use rusqlite::Connection;
use std::sync::Arc;
use tempfile::TempDir;

fn create_test_db() -> (AzothDb, TempDir) {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();
    (db, temp_dir)
}

// Test event handler for deposits
struct DepositHandler;

impl EventHandler for DepositHandler {
    fn event_type(&self) -> &str {
        "deposit"
    }

    fn handle(&self, conn: &Connection, _event_id: EventId, payload: &[u8]) -> Result<()> {
        let amount: i64 = String::from_utf8_lossy(payload)
            .parse()
            .map_err(|e: std::num::ParseIntError| AzothError::EventDecode(e.to_string()))?;

        // Create accounts table if it doesn't exist (simplified for test)
        conn.execute(
            "CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0
            )",
            [],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Insert or update account
        conn.execute(
            "INSERT INTO accounts (id, balance) VALUES (1, ?1)
             ON CONFLICT(id) DO UPDATE SET balance = balance + ?1",
            [amount],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }

    fn validate(&self, payload: &[u8]) -> Result<()> {
        // Validate payload is a valid integer
        String::from_utf8_lossy(payload)
            .parse::<i64>()
            .map_err(|e| AzothError::PreflightFailed(e.to_string()))?;
        Ok(())
    }
}

// Test event handler for withdrawals
struct WithdrawHandler;

impl EventHandler for WithdrawHandler {
    fn event_type(&self) -> &str {
        "withdraw"
    }

    fn handle(&self, conn: &Connection, _event_id: EventId, payload: &[u8]) -> Result<()> {
        let amount: i64 = String::from_utf8_lossy(payload)
            .parse()
            .map_err(|e: std::num::ParseIntError| AzothError::EventDecode(e.to_string()))?;

        conn.execute(
            "UPDATE accounts SET balance = balance - ?1 WHERE id = 1",
            [amount],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(())
    }
}

#[test]
fn test_event_handler_registry() {
    let mut registry = EventHandlerRegistry::new();

    // Register handlers
    registry.register(Box::new(DepositHandler));
    registry.register(Box::new(WithdrawHandler));

    // Should be able to get handlers
    assert!(registry.get("deposit").is_some());
    assert!(registry.get("withdraw").is_some());
    assert!(registry.get("nonexistent").is_none());

    // List event types
    let types = registry.event_types();
    assert_eq!(types.len(), 2);
    assert!(types.contains(&"deposit"));
    assert!(types.contains(&"withdraw"));
}

#[test]
#[should_panic(expected = "already registered")]
fn test_duplicate_handler_registration() {
    let mut registry = EventHandlerRegistry::new();
    registry.register(Box::new(DepositHandler));
    registry.register(Box::new(DepositHandler)); // Should panic
}

#[test]
fn test_try_register() {
    let mut registry = EventHandlerRegistry::new();

    // First registration should succeed
    assert!(registry.try_register(Box::new(DepositHandler)).is_ok());

    // Second registration should fail
    assert!(registry.try_register(Box::new(DepositHandler)).is_err());
}

#[test]
fn test_event_validation() {
    let mut registry = EventHandlerRegistry::new();
    registry.register(Box::new(DepositHandler));

    // Valid event
    assert!(registry.validate(b"deposit:100").is_ok());

    // Invalid format (no colon)
    assert!(registry.validate(b"deposit").is_err());

    // Invalid payload
    assert!(registry.validate(b"deposit:not_a_number").is_err());

    // Unknown event type
    assert!(registry.validate(b"unknown:100").is_err());
}

#[test]
fn test_event_processing() {
    let (_db, temp) = create_test_db();

    // Create a test SQLite connection
    let conn = Connection::open(temp.path().join("test.db")).unwrap();

    // Create registry and register handler
    let mut registry = EventHandlerRegistry::new();
    registry.register(Box::new(DepositHandler));

    // Process deposit event
    registry.process(&conn, 0, b"deposit:100").unwrap();

    // Verify balance
    let balance: i64 = conn
        .query_row("SELECT balance FROM accounts WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(balance, 100);

    // Process another deposit
    registry.process(&conn, 1, b"deposit:50").unwrap();

    let balance: i64 = conn
        .query_row("SELECT balance FROM accounts WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(balance, 150);
}

#[test]
fn test_multiple_handlers() {
    let (_db, temp) = create_test_db();
    let conn = Connection::open(temp.path().join("test.db")).unwrap();

    let mut registry = EventHandlerRegistry::new();
    registry.register(Box::new(DepositHandler));
    registry.register(Box::new(WithdrawHandler));

    // Process events
    registry.process(&conn, 0, b"deposit:100").unwrap();
    registry.process(&conn, 1, b"deposit:50").unwrap();
    registry.process(&conn, 2, b"withdraw:30").unwrap();

    // Verify final balance
    let balance: i64 = conn
        .query_row("SELECT balance FROM accounts WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(balance, 120); // 100 + 50 - 30
}

#[test]
fn test_build_with_projection_opens_connection() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // build_with_projection should succeed and return a functional processor
    let mut processor = EventProcessor::builder(db.clone())
        .with_handler(Box::new(DepositHandler))
        .build_with_projection()
        .expect("build_with_projection should succeed");

    // Cursor should start as None (no events processed yet)
    assert_eq!(processor.cursor(), None);

    // With no events, process_batch returns 0
    assert_eq!(processor.process_batch().unwrap(), 0);
}

#[test]
fn test_build_with_projection_processes_events() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // Write some events to the canonical log
    {
        let mut txn = db.canonical().write_txn().unwrap();
        txn.append_event(b"deposit:100").unwrap();
        txn.append_event(b"deposit:50").unwrap();
        txn.commit().unwrap();
    }

    // Build processor using the projection shortcut
    let mut processor = EventProcessor::builder(db.clone())
        .with_handler(Box::new(DepositHandler))
        .build_with_projection()
        .expect("build_with_projection should succeed");

    // Process all pending events
    let processed = processor.process_batch().unwrap();
    assert_eq!(processed, 2);

    // Cursor should have advanced past both events (last processed = event 1)
    assert_eq!(processor.cursor(), Some(1));

    // No lag remaining
    assert_eq!(processor.lag().unwrap(), 0);
}

#[test]
fn test_build_with_projection_with_multiple_handlers() {
    let (db, _temp) = create_test_db();
    let db = Arc::new(db);

    // Write deposit and withdraw events
    {
        let mut txn = db.canonical().write_txn().unwrap();
        txn.append_event(b"deposit:200").unwrap();
        txn.append_event(b"deposit:50").unwrap();
        txn.append_event(b"withdraw:75").unwrap();
        txn.commit().unwrap();
    }

    // Build with multiple handlers
    let mut processor = EventProcessor::builder(db.clone())
        .with_handler(Box::new(DepositHandler))
        .with_handler(Box::new(WithdrawHandler))
        .build_with_projection()
        .expect("build_with_projection should succeed");

    let processed = processor.process_batch().unwrap();
    assert_eq!(processed, 3);

    // Query the projection database to verify handlers wrote correctly.
    // build_with_projection opens its own connection to the projection DB,
    // so we open a separate one to read from it.
    let proj_path = db.projection().db_path().to_path_buf();
    let verify_conn = Connection::open(&proj_path).unwrap();
    let balance: i64 = verify_conn
        .query_row("SELECT balance FROM accounts WHERE id = 1", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(balance, 175); // 200 + 50 - 75
}

#[test]
fn test_build_with_projection_blocking_run() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = Arc::new(AzothDb::open(temp_dir.path()).unwrap());

    // Write events
    {
        let mut txn = db.canonical().write_txn().unwrap();
        txn.append_event(b"deposit:500").unwrap();
        txn.commit().unwrap();
    }

    // Build inside the dedicated thread because EventProcessor holds
    // Arc<Connection> which is !Send (rusqlite::Connection is !Sync).
    let db2 = db.clone();
    let (shutdown_tx, shutdown_rx) = std::sync::mpsc::channel::<()>();
    let handle = std::thread::spawn(move || {
        let mut processor = EventProcessor::builder(db2)
            .with_handler(Box::new(DepositHandler))
            .build_with_projection()
            .expect("build_with_projection should succeed");

        let shutdown = processor.shutdown_handle();

        // Signal the main thread that shutdown handle is ready
        // by simply running; main thread uses the channel to stop us.
        std::thread::spawn(move || {
            let _ = shutdown_rx.recv();
            shutdown.shutdown();
        });

        processor.run_blocking()
    });

    // Wait until the processor catches up
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        let proj_path = db.projection().db_path().to_path_buf();
        let check_conn = Connection::open(&proj_path).unwrap();
        let result: std::result::Result<i64, _> =
            check_conn.query_row("SELECT balance FROM accounts WHERE id = 1", [], |row| {
                row.get(0)
            });
        if let Ok(balance) = result {
            if balance == 500 {
                break;
            }
        }
        if std::time::Instant::now() > deadline {
            panic!("EventProcessor did not process events within 2s");
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }

    let _ = shutdown_tx.send(());
    handle
        .join()
        .expect("processor thread should not panic")
        .unwrap();
}
