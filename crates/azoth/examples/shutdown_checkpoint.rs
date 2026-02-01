//! Shutdown Checkpoint Example
//!
//! Demonstrates how to create a checkpoint when the application shuts down.
//! This is critical for ensuring data integrity and having a recovery point.
//!
//! # Usage
//!
//! ```bash
//! cargo run --example shutdown_checkpoint
//! # Do some work...
//! # Press Ctrl+C to trigger graceful shutdown with checkpoint
//! ```

use azoth::checkpoint::{CheckpointConfig, CheckpointManager, LocalStorage};
use azoth::{AzothDb, CanonicalStore, CanonicalTxn, EncryptionKey};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    tracing::info!("Starting application...");

    // Open database
    let db = Arc::new(AzothDb::open("./data")?);

    // Do some work...
    {
        let mut txn = db.canonical().write_txn()?;
        txn.put_state(b"example_key", b"example_value")?;
        txn.append_event(b"example_event")?;
        txn.commit()?;

        db.projector().run_once()?;
    }

    tracing::info!("Application running. Press Ctrl+C to shut down gracefully.");

    // Wait for shutdown signal
    signal::ctrl_c().await?;
    tracing::info!("Received shutdown signal");

    // Configure checkpoint
    let encryption_key = EncryptionKey::generate();
    tracing::info!(
        "Encryption key for checkpoint: {}",
        encryption_key.to_identity_string()
    );

    let config = CheckpointConfig::new()
        .with_encryption(encryption_key)
        .with_compression(true)
        .with_compression_level(3);

    // Create checkpoint manager with local storage
    let storage = LocalStorage::new(PathBuf::from("./checkpoints"));
    let manager = CheckpointManager::new(db.clone(), storage, config);

    // Perform graceful shutdown with checkpoint
    tracing::info!("Performing graceful shutdown...");

    // 1. Prepare database for shutdown (seal, catch up projector)
    db.prepare_shutdown()?;

    // 2. Create final checkpoint
    let metadata = manager.shutdown_checkpoint().await?;
    tracing::info!("Shutdown checkpoint created: id={}", metadata.id);

    // Close database
    Arc::try_unwrap(db)
        .map_err(|_| anyhow::anyhow!("Database still in use"))?
        .close()?;

    tracing::info!("✅ Shutdown complete with checkpoint saved");
    Ok(())
}
