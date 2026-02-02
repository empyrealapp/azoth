//! Checkpoint Service Example
//!
//! Demonstrates how to create a periodic checkpoint service that:
//! 1. Backs up the database every minute
//! 2. Compresses and encrypts the backup
//! 3. Uploads to storage (IPFS, local filesystem, or custom backend)
//!
//! # Usage
//!
//! ## With IPFS/Pinata:
//! ```bash
//! export PINATA_API_KEY="your-api-key"
//! export PINATA_SECRET_KEY="your-secret-key"
//! cargo run --example checkpoint_service -- --storage ipfs
//! ```
//!
//! ## With local filesystem:
//! ```bash
//! cargo run --example checkpoint_service -- --storage local
//! ```

use azoth::checkpoint::{CheckpointConfig, CheckpointManager, CheckpointStorage};
use azoth::{AzothDb, EncryptionKey, IpfsStorage, LocalStorage};
use azoth_scheduler::task_handler::{TaskContext, TaskEvent, TaskHandler};
use azoth_scheduler::{ScheduleTaskRequest, Scheduler};
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;

/// Task handler that uses CheckpointManager with pluggable storage
struct CheckpointTaskHandler<S: CheckpointStorage> {
    manager: Arc<CheckpointManager<S>>,
}

impl<S: CheckpointStorage> CheckpointTaskHandler<S> {
    fn new(manager: Arc<CheckpointManager<S>>) -> Self {
        Self { manager }
    }
}

impl<S: CheckpointStorage + 'static> TaskHandler for CheckpointTaskHandler<S> {
    fn task_type(&self) -> &str {
        "checkpoint"
    }

    fn execute(&self, ctx: &TaskContext, _payload: &[u8]) -> azoth_scheduler::Result<TaskEvent> {
        tracing::info!(
            "Executing checkpoint task: task_id={}, execution_id={}",
            ctx.task_id,
            ctx.execution_id
        );

        // Create async runtime for the checkpoint operation
        let runtime = tokio::runtime::Runtime::new()
            .map_err(|e| azoth_scheduler::SchedulerError::HandlerError(e.to_string()))?;

        let metadata = runtime
            .block_on(self.manager.create_checkpoint())
            .map_err(|e| azoth_scheduler::SchedulerError::HandlerError(e.to_string()))?;

        // Serialize metadata as event payload
        let payload = serde_json::to_vec(&metadata)?;

        Ok(TaskEvent {
            event_type: "checkpoint_completed".to_string(),
            payload,
        })
    }
}

#[derive(Debug, Clone, Copy)]
enum StorageBackend {
    Local,
    Ipfs,
}

impl StorageBackend {
    fn from_args() -> Self {
        let args: Vec<String> = std::env::args().collect();
        if args.iter().any(|a| a == "--storage") {
            if let Some(pos) = args.iter().position(|a| a == "--storage") {
                if args.len() > pos + 1 {
                    match args[pos + 1].as_str() {
                        "ipfs" => return Self::Ipfs,
                        "local" => return Self::Local,
                        _ => {}
                    }
                }
            }
        }
        Self::Local // Default to local
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    // Parse command line arguments
    let storage_backend = StorageBackend::from_args();

    tracing::info!(
        "Checkpoint service starting with {:?} storage backend",
        storage_backend
    );

    // Open database
    let db = Arc::new(AzothDb::open("./data")?);

    // Generate encryption key (in production, load from secure storage)
    let encryption_key = EncryptionKey::generate();
    tracing::info!(
        "Generated encryption key: {}",
        encryption_key.to_identity_string()
    );
    tracing::info!("⚠️  Save this key securely to restore checkpoints!");

    // Configure checkpoint options
    let config = CheckpointConfig::new()
        .with_encryption(encryption_key.clone())
        .with_compression(true)
        .with_compression_level(3); // Fast compression for frequent checkpoints

    // Clone config for shutdown checkpoint
    let shutdown_config = config.clone();

    // Create projection database for scheduler
    let projection_conn = Arc::new(Connection::open("./scheduler.db")?);

    // Create scheduler and register handler based on storage backend
    let mut scheduler = match storage_backend {
        StorageBackend::Local => {
            tracing::info!("Using local filesystem storage at ./checkpoints");
            let storage = LocalStorage::new(PathBuf::from("./checkpoints"));
            let manager = Arc::new(CheckpointManager::new(db.clone(), storage, config));
            let handler = CheckpointTaskHandler::new(manager);

            Scheduler::builder(db.clone())
                .with_task_handler(handler)
                .with_poll_interval(Duration::from_secs(5))
                .build(projection_conn)?
        }
        StorageBackend::Ipfs => {
            tracing::info!("Using IPFS storage (Pinata)");
            let storage = IpfsStorage::from_env()?;
            let manager = Arc::new(CheckpointManager::new(db.clone(), storage, config));
            let handler = CheckpointTaskHandler::new(manager);

            Scheduler::builder(db.clone())
                .with_task_handler(handler)
                .with_poll_interval(Duration::from_secs(5))
                .build(projection_conn)?
        }
    };

    // Schedule checkpoint every minute (60 seconds)
    scheduler.schedule_task(
        ScheduleTaskRequest::builder("periodic-checkpoint")
            .task_type("checkpoint")
            .interval(60)
            .build()?,
    )?;

    tracing::info!("✅ Checkpoint service started. Checkpoints will run every 60 seconds.");
    tracing::info!("Press Ctrl+C to stop.");

    // Run scheduler with graceful shutdown on Ctrl+C
    tokio::select! {
        result = scheduler.run() => {
            if let Err(e) = result {
                tracing::error!("Scheduler error: {}", e);
            }
        }
        _ = signal::ctrl_c() => {
            tracing::info!("Received shutdown signal (Ctrl+C)");
        }
    }

    // Prepare database for shutdown
    tracing::info!("Preparing database for shutdown...");
    db.prepare_shutdown()?;

    // Create final checkpoint based on storage backend
    match storage_backend {
        StorageBackend::Local => {
            let storage = LocalStorage::new(PathBuf::from("./checkpoints"));
            let manager = CheckpointManager::new(db.clone(), storage, shutdown_config);

            tracing::info!("Creating final shutdown checkpoint...");
            let metadata = manager.shutdown_checkpoint().await?;
            tracing::info!("✅ Shutdown checkpoint completed: id={}", metadata.id);
        }
        StorageBackend::Ipfs => {
            let storage = IpfsStorage::from_env()?;
            let manager = CheckpointManager::new(db.clone(), storage, shutdown_config);

            tracing::info!("Creating final shutdown checkpoint...");
            let metadata = manager.shutdown_checkpoint().await?;
            tracing::info!("✅ Shutdown checkpoint completed: id={}", metadata.id);
        }
    }

    // Close database
    tracing::info!("Closing database...");
    Arc::try_unwrap(db)
        .map_err(|_| anyhow::anyhow!("Database still in use"))?
        .close()?;

    tracing::info!("✅ Shutdown complete");
    Ok(())
}
