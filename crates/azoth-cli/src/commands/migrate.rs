//! Migration command implementations

use anyhow::{Context, Result};
use azoth::prelude::*;
use std::io::{self, Write};
use std::path::PathBuf;

pub fn execute(db_path: PathBuf, command: crate::MigrateCommands) -> Result<()> {
    match command {
        crate::MigrateCommands::List { migrations_dir } => {
            list_migrations(migrations_dir)?;
        }
        crate::MigrateCommands::History => {
            show_history(db_path)?;
        }
        crate::MigrateCommands::Pending { migrations_dir } => {
            show_pending(db_path, migrations_dir)?;
        }
        crate::MigrateCommands::Run {
            migrations_dir,
            dry_run,
        } => {
            run_migrations(db_path, migrations_dir, dry_run)?;
        }
        crate::MigrateCommands::Generate {
            name,
            migrations_dir,
        } => {
            generate_migration(name, migrations_dir)?;
        }
        crate::MigrateCommands::Rollback { force } => {
            rollback_migration(db_path, force)?;
        }
    }

    Ok(())
}

fn list_migrations(migrations_dir: PathBuf) -> Result<()> {
    tracing::info!("Loading migrations from: {}", migrations_dir.display());

    let mut manager = MigrationManager::new();

    if migrations_dir.exists() {
        manager
            .load_from_directory(&migrations_dir)
            .context("Failed to load migrations")?;
    } else {
        tracing::warn!(
            "Migrations directory does not exist: {}",
            migrations_dir.display()
        );
        return Ok(());
    }

    let migrations = manager.list();

    if migrations.is_empty() {
        println!("No migrations found");
        return Ok(());
    }

    println!("\nRegistered Migrations:");
    println!("{:<10} Name", "Version");
    println!("{}", "=".repeat(60));

    for migration in migrations {
        println!("{:<10} {}", migration.version, migration.name);
    }

    println!("\nTotal: {} migration(s)", manager.list().len());

    Ok(())
}

fn show_history(db_path: PathBuf) -> Result<()> {
    let db = AzothDb::open(&db_path).context("Failed to open database")?;
    let manager = MigrationManager::new();

    let history = manager
        .history(db.projection())
        .context("Failed to get migration history")?;

    if history.is_empty() {
        println!("No migrations have been applied yet");
        return Ok(());
    }

    println!("\nMigration History:");
    println!("{:<10} {:<30} Applied At", "Version", "Name");
    println!("{}", "=".repeat(80));

    for entry in history {
        println!(
            "{:<10} {:<30} {}",
            entry.version, entry.name, entry.applied_at
        );
    }

    let current_version = db
        .projection()
        .schema_version()
        .context("Failed to get schema version")?;
    println!("\nCurrent schema version: {}", current_version);

    Ok(())
}

fn show_pending(db_path: PathBuf, migrations_dir: PathBuf) -> Result<()> {
    let db = AzothDb::open(&db_path).context("Failed to open database")?;
    let mut manager = MigrationManager::new();

    if migrations_dir.exists() {
        manager
            .load_from_directory(&migrations_dir)
            .context("Failed to load migrations")?;
    } else {
        tracing::warn!(
            "Migrations directory does not exist: {}",
            migrations_dir.display()
        );
        return Ok(());
    }

    let pending = manager
        .pending(db.projection())
        .context("Failed to get pending migrations")?;

    if pending.is_empty() {
        println!("No pending migrations - database is up to date");
        return Ok(());
    }

    println!("\nPending Migrations:");
    println!("{:<10} Name", "Version");
    println!("{}", "=".repeat(60));

    for migration in pending {
        println!("{:<10} {}", migration.version, migration.name);
    }

    println!(
        "\nTotal: {} pending migration(s)",
        manager
            .pending(db.projection())
            .context("Failed to get pending migrations")?
            .len()
    );

    Ok(())
}

fn run_migrations(db_path: PathBuf, migrations_dir: PathBuf, dry_run: bool) -> Result<()> {
    let db = AzothDb::open(&db_path).context("Failed to open database")?;
    let mut manager = MigrationManager::new();

    if !migrations_dir.exists() {
        tracing::warn!(
            "Migrations directory does not exist: {}",
            migrations_dir.display()
        );
        println!("No migrations to run");
        return Ok(());
    }

    manager
        .load_from_directory(&migrations_dir)
        .context("Failed to load migrations")?;

    let pending = manager
        .pending(db.projection())
        .context("Failed to get pending migrations")?;

    if pending.is_empty() {
        println!("No pending migrations - database is up to date");
        return Ok(());
    }

    println!("\nPending Migrations:");
    for migration in &pending {
        println!("  v{}: {}", migration.version, migration.name);
    }

    if dry_run {
        println!("\n[DRY RUN] Would apply {} migration(s)", pending.len());
        return Ok(());
    }

    println!("\nApplying {} migration(s)...", pending.len());

    manager
        .run(db.projection())
        .context("Failed to run migrations")?;

    println!("✓ All migrations applied successfully");

    let current_version = db
        .projection()
        .schema_version()
        .context("Failed to get schema version")?;
    println!("Current schema version: {}", current_version);

    Ok(())
}

fn generate_migration(name: String, migrations_dir: PathBuf) -> Result<()> {
    let manager = MigrationManager::new();

    let file_path = manager
        .generate(&migrations_dir, &name)
        .context("Failed to generate migration")?;

    println!("✓ Generated migration files:");
    println!("  {}", file_path.display());
    println!(
        "  {}",
        file_path
            .with_file_name(format!(
                "{}.down.sql",
                file_path.file_stem().unwrap().to_str().unwrap()
            ))
            .display()
    );

    Ok(())
}

fn rollback_migration(db_path: PathBuf, force: bool) -> Result<()> {
    let db = AzothDb::open(&db_path).context("Failed to open database")?;
    let manager = MigrationManager::new();

    let current_version = db
        .projection()
        .schema_version()
        .context("Failed to get schema version")?;

    if current_version <= 1 {
        println!("Cannot rollback base schema (version 1)");
        return Ok(());
    }

    if !force {
        print!(
            "⚠️  WARNING: Rolling back migration v{} may result in data loss.\nContinue? [y/N] ",
            current_version
        );
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !matches!(input.trim().to_lowercase().as_str(), "y" | "yes") {
            println!("Rollback cancelled");
            return Ok(());
        }
    }

    println!("Rolling back migration v{}...", current_version);

    manager
        .rollback_last(db.projection())
        .context("Failed to rollback migration")?;

    let new_version = db
        .projection()
        .schema_version()
        .context("Failed to get schema version")?;

    println!(
        "✓ Rolled back from v{} to v{}",
        current_version, new_version
    );

    Ok(())
}
