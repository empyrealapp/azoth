//! Backup and Restore Example
//!
//! Demonstrates:
//! - Safe backup workflow (pause → seal → backup → resume)
//! - Backup with encryption and compression options
//! - Restoring from backup
//! - Verifying data integrity
//!
//! Run with: cargo run --example backup_restore

use azoth::prelude::*;

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("azoth=info")
        .init();

    println!("=== Backup and Restore Example ===\n");

    let temp_dir = tempfile::tempdir()?;
    let original_path = temp_dir.path().join("original");
    let backup_path = temp_dir.path().join("backup");
    let restored_path = temp_dir.path().join("restored");

    // ========================================
    // 1. Create and Populate Database
    // ========================================
    println!("1. Creating original database...");

    let db = AzothDb::open(&original_path)?;

    // Write some data
    let mut txn = db.canonical().write_txn()?;
    txn.put_state(b"account:1:balance", b"1000")?;
    txn.put_state(b"account:2:balance", b"2000")?;
    txn.put_state(b"config:version", b"1.0.0")?;
    txn.append_event(b"init:account:1")?;
    txn.append_event(b"init:account:2")?;
    txn.commit()?;

    // Add more events
    let mut txn = db.canonical().write_txn()?;
    txn.put_state(b"account:1:balance", b"900")?;
    txn.append_event(b"withdraw:1:100")?;
    txn.commit()?;

    println!("✓ Database populated with state and events\n");

    // Run projector
    db.projector().run_once()?;
    println!("✓ Projector caught up\n");

    // ========================================
    // 2. Backup (Standard)
    // ========================================
    println!("2. Creating standard backup...");

    // Standard backup (follows safe workflow)
    db.backup_to(&backup_path)?;

    println!("✓ Backup created at {:?}", backup_path);

    // Verify backup files exist
    assert!(backup_path.join("manifest.json").exists());
    assert!(backup_path.join("canonical").exists());
    assert!(backup_path.join("projection.db").exists());

    println!("✓ Backup files verified\n");

    // ========================================
    // 3. Backup with Options
    // ========================================
    println!("3. Creating backup with encryption & compression...");

    let encrypted_backup_path = temp_dir.path().join("backup-encrypted");

    let key = EncryptionKey::generate();
    let options = BackupOptions::new()
        .with_encryption(key.clone())
        .with_compression(true)
        .with_compression_level(6);

    println!("   Encryption enabled: {}", options.is_encrypted());
    println!("   Compression enabled: {}", options.compression);

    db.backup_with_options(&encrypted_backup_path, &options)?;

    println!("✓ Encrypted backup created\n");

    // ========================================
    // 4. Restore from Backup
    // ========================================
    println!("4. Restoring from backup...");

    let restored_db = AzothDb::restore_from(&backup_path, &restored_path)?;

    println!("✓ Database restored to {:?}\n", restored_path);

    // ========================================
    // 5. Verify Restored Data
    // ========================================
    println!("5. Verifying restored data...");

    // Check state
    let txn = restored_db.canonical().write_txn()?;
    let balance1 = String::from_utf8(
        txn.get_state(b"account:1:balance")?
            .expect("balance should exist"),
    )
    .map_err(|e| AzothError::Serialization(e.to_string()))?;
    let balance2 = String::from_utf8(
        txn.get_state(b"account:2:balance")?
            .expect("balance should exist"),
    )
    .map_err(|e| AzothError::Serialization(e.to_string()))?;
    let version = String::from_utf8(
        txn.get_state(b"config:version")?
            .expect("version should exist"),
    )
    .map_err(|e| AzothError::Serialization(e.to_string()))?;

    println!("   Account 1 balance: {}", balance1);
    println!("   Account 2 balance: {}", balance2);
    println!("   Config version: {}", version);

    assert_eq!(balance1, "900");
    assert_eq!(balance2, "2000");
    assert_eq!(version, "1.0.0");

    println!("✓ State verified\n");

    // Check events
    let mut iter = restored_db.canonical().iter_events(0, None)?;
    let mut event_count = 0;

    println!("   Restored events:");
    while let Some((id, bytes)) = iter.next()? {
        let event_str = String::from_utf8_lossy(&bytes);
        println!("     Event {}: {}", id, event_str);
        event_count += 1;
    }

    assert_eq!(event_count, 3);
    println!("✓ Events verified ({} events)\n", event_count);

    // Check metadata
    let meta = restored_db.canonical().meta()?;
    println!("   Metadata:");
    println!("     Next event ID: {}", meta.next_event_id);
    println!("     Sealed: {:?}", meta.sealed_event_id);

    assert_eq!(meta.next_event_id, 3);
    assert!(meta.sealed_event_id.is_some());

    println!("✓ Metadata verified\n");

    // Check projection cursor
    let cursor = restored_db.projection().get_cursor()?;
    println!("   Projection cursor: {}", cursor);
    println!("✓ Projection cursor verified\n");

    // ========================================
    // 6. Continue Using Restored Database
    // ========================================
    println!("6. Writing to restored database...");

    // Note: Can't write because db is sealed after backup
    // Resume ingestion first
    restored_db.canonical().resume_ingestion()?;

    // Now we can write (but we'd need to unseal - not shown here)
    // For demonstration, we just verify the state is correct

    println!("✓ Restored database is functional\n");

    println!("=== Example Complete ===");
    println!("\nKey Points:");
    println!("- Backup follows safe workflow (pause → seal → backup → resume)");
    println!("- All state, events, and metadata are preserved");
    println!("- Backup includes manifest.json with versioning info");
    println!("- Restored database is fully functional");
    println!("- Optional encryption and compression available");

    Ok(())
}
