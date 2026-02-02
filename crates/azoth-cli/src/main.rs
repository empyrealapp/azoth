//! Azoth CLI - Command-line interface for Azoth database operations

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod commands;

#[derive(Parser)]
#[command(name = "azoth")]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    /// Path to the database directory
    #[arg(short, long, default_value = "./data")]
    db_path: PathBuf,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Migration management commands
    #[command(subcommand)]
    Migrate(MigrateCommands),

    /// Database information and status
    Status,

    /// Run the event projector
    Project {
        /// Run projector continuously
        #[arg(short, long)]
        continuous: bool,
    },
}

#[derive(Subcommand)]
enum MigrateCommands {
    /// List all registered migrations
    List {
        /// Path to migrations directory
        #[arg(short, long, default_value = "./migrations")]
        migrations_dir: PathBuf,
    },

    /// Show migration history
    History,

    /// Show pending migrations
    Pending {
        /// Path to migrations directory
        #[arg(short, long, default_value = "./migrations")]
        migrations_dir: PathBuf,
    },

    /// Run pending migrations
    Run {
        /// Path to migrations directory
        #[arg(short, long, default_value = "./migrations")]
        migrations_dir: PathBuf,

        /// Dry run - show what would be migrated without applying
        #[arg(long)]
        dry_run: bool,
    },

    /// Generate a new migration file
    Generate {
        /// Name of the migration (e.g., "create_users_table")
        name: String,

        /// Path to migrations directory
        #[arg(short, long, default_value = "./migrations")]
        migrations_dir: PathBuf,
    },

    /// Rollback the last migration
    Rollback {
        /// Skip confirmation prompt
        #[arg(short, long)]
        force: bool,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Initialize logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(log_level)),
        )
        .init();

    // Execute command
    match cli.command {
        Commands::Migrate(migrate_cmd) => {
            commands::migrate::execute(cli.db_path, migrate_cmd)?;
        }
        Commands::Status => {
            commands::status::execute(cli.db_path)?;
        }
        Commands::Project { continuous } => {
            commands::project::execute(cli.db_path, continuous)?;
        }
    }

    Ok(())
}
