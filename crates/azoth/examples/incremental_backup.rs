//! Example: Incremental Backup System
//!
//! Demonstrates how to use incremental backups for efficient storage
//! and point-in-time recovery.

use azoth::prelude::*;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    // Setup logging
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // Clean up any existing data
    let _ = std::fs::remove_dir_all("./tmp/incr_backup");

    tracing::info!("=== Step 1: Create database and write initial events ===\n");

    // Open database
    let db = Arc::new(AzothDb::open("./tmp/incr_backup/db")?);

    // Write initial batch of events
    for i in 0..100 {
        let mut txn = db.canonical().write_txn()?;
        let event_data = format!("Initial event {}", i);
        txn.put_state(format!("key_{}", i).as_bytes(), b"value")?;
        txn.append_event(event_data.as_bytes())?;
        txn.commit()?;
    }

    tracing::info!("Wrote 100 initial events\n");

    tracing::info!("=== Step 2: Configure incremental backup ===\n");

    // Configure incremental backup
    let config = IncrementalBackupConfig {
        full_backup_interval: Duration::from_secs(3600 * 24), // Daily full backups
        incremental_interval: Duration::from_secs(3600),      // Hourly incrementals
        compression: true,
        encryption_key: None, // Could add encryption here
        retention: BackupRetention {
            full_backups: 7,      // Keep last 7 full backups
            incremental_days: 30, // Keep incrementals for 30 days
        },
        backup_dir: "./tmp/incr_backup/backups".into(),
    };

    tracing::info!("Configuration:");
    tracing::info!(
        "  Full backup interval: {} hours",
        config.full_backup_interval.as_secs() / 3600
    );
    tracing::info!(
        "  Incremental interval: {} hours",
        config.incremental_interval.as_secs() / 3600
    );
    tracing::info!("  Compression: {}", config.compression);
    tracing::info!(
        "  Retention: {} full backups, {} days of incrementals\n",
        config.retention.full_backups,
        config.retention.incremental_days
    );

    let backup_mgr = IncrementalBackup::new(db.clone(), config);

    tracing::info!("=== Step 3: Create first backup (will be full) ===\n");

    let backup1 = backup_mgr.create_backup().await?;
    tracing::info!("Backup created:");
    tracing::info!("  ID: {}", backup1.id);
    tracing::info!("  Type: {:?}", backup1.backup_type);
    tracing::info!("  Events: {}-{}", backup1.from_event, backup1.to_event);
    tracing::info!("  Size: {} bytes", backup1.size_bytes);
    tracing::info!("  Compressed: {}", backup1.compressed);
    tracing::info!("  Checksum: {}\n", backup1.checksum);

    assert_eq!(backup1.backup_type, BackupType::Full);
    assert_eq!(backup1.from_event, 0);

    tracing::info!("=== Step 4: Write more events ===\n");

    // Write more events (simulate continued operation)
    for i in 100..200 {
        let mut txn = db.canonical().write_txn()?;
        let event_data = format!("New event {}", i);
        txn.put_state(format!("key_{}", i).as_bytes(), b"value")?;
        txn.append_event(event_data.as_bytes())?;
        txn.commit()?;
    }

    tracing::info!("Wrote 100 more events (total: 200)\n");

    tracing::info!("=== Step 5: Create incremental backup ===\n");

    // Create another backup (will be incremental)
    let backup2 = backup_mgr.create_backup().await?;
    tracing::info!("Backup created:");
    tracing::info!("  ID: {}", backup2.id);
    tracing::info!("  Type: {:?}", backup2.backup_type);
    tracing::info!("  Base backup: {:?}", backup2.base_backup_id);
    tracing::info!("  Events: {}-{}", backup2.from_event, backup2.to_event);
    tracing::info!("  Size: {} bytes", backup2.size_bytes);
    tracing::info!("  Checksum: {}\n", backup2.checksum);

    assert_eq!(backup2.backup_type, BackupType::Incremental);
    assert!(backup2.base_backup_id.is_some());

    // Size comparison
    let full_size = backup1.size_bytes;
    let inc_size = backup2.size_bytes;
    let savings_pct = ((full_size - inc_size) as f64 / full_size as f64) * 100.0;

    tracing::info!("Storage comparison:");
    tracing::info!("  Full backup: {} bytes", full_size);
    tracing::info!("  Incremental: {} bytes", inc_size);
    tracing::info!("  Savings: {:.1}%\n", savings_pct);

    tracing::info!("=== Step 6: Write even more events ===\n");

    for i in 200..300 {
        let mut txn = db.canonical().write_txn()?;
        let event_data = format!("Event batch 3: {}", i);
        txn.put_state(format!("key_{}", i).as_bytes(), b"value")?;
        txn.append_event(event_data.as_bytes())?;
        txn.commit()?;
    }

    tracing::info!("Wrote 100 more events (total: 300)\n");

    tracing::info!("=== Step 7: Create another incremental backup ===\n");

    let backup3 = backup_mgr.create_backup().await?;
    tracing::info!("Backup created:");
    tracing::info!("  ID: {}", backup3.id);
    tracing::info!("  Type: {:?}", backup3.backup_type);
    tracing::info!("  Base backup: {:?}", backup3.base_backup_id);
    tracing::info!("  Events: {}-{}", backup3.from_event, backup3.to_event);
    tracing::info!("  Size: {} bytes\n", backup3.size_bytes);

    tracing::info!("=== Step 8: Demonstrate point-in-time recovery ===\n");

    tracing::info!("Backup chain:");
    tracing::info!("  1. Full backup (events 0-{})", backup1.to_event);
    tracing::info!(
        "  2. Incremental (events {}-{})",
        backup2.from_event,
        backup2.to_event
    );
    tracing::info!(
        "  3. Incremental (events {}-{})",
        backup3.from_event,
        backup3.to_event
    );
    tracing::info!("");
    tracing::info!("To restore to any event ID, the system would:");
    tracing::info!("  1. Find the base full backup");
    tracing::info!("  2. Apply incremental backups in order");
    tracing::info!("  3. Stop at the target event\n");

    // Demonstrate conceptual restore
    let target_event = 150;
    tracing::info!("To restore to event {}:", target_event);
    tracing::info!("  1. Restore full backup (events 0-{})", backup1.to_event);
    tracing::info!(
        "  2. Apply incremental backup 2 partially (events {}-{})",
        backup2.from_event,
        target_event
    );
    tracing::info!("");

    tracing::info!("=== Summary ===\n");

    tracing::info!("Benefits of incremental backups:");
    tracing::info!("  ✓ Reduced storage: Only store changed events");
    tracing::info!("  ✓ Faster backups: Less data to copy");
    tracing::info!("  ✓ Point-in-time recovery: Restore to any event ID");
    tracing::info!("  ✓ Retention policies: Keep multiple restore points");
    tracing::info!("  ✓ Automatic cleanup: Old backups removed automatically\n");

    tracing::info!("Storage breakdown:");
    tracing::info!(
        "  Full backup:        {} bytes (100 events)",
        backup1.size_bytes
    );
    tracing::info!(
        "  Incremental 1:      {} bytes (100 events)",
        backup2.size_bytes
    );
    tracing::info!(
        "  Incremental 2:      {} bytes (100 events)",
        backup3.size_bytes
    );
    let total_incremental = backup1.size_bytes + backup2.size_bytes + backup3.size_bytes;
    let full_only_estimate = backup1.size_bytes * 3; // Rough estimate
    let savings =
        ((full_only_estimate - total_incremental) as f64 / full_only_estimate as f64) * 100.0;
    tracing::info!("  Total with incremental: {} bytes", total_incremental);
    tracing::info!("  Estimated full-only:    {} bytes", full_only_estimate);
    tracing::info!("  Estimated savings:      {:.1}%", savings);

    // Cleanup
    let _ = std::fs::remove_dir_all("./tmp/incr_backup");

    Ok(())
}
