//! Database migration system
//!
//! Provides a flexible migration framework for evolving projection schemas.
//!
//! # Example
//!
//! ```no_run
//! use azoth::prelude::*;
//! use azoth::{Migration, MigrationManager};
//! use rusqlite::Connection;
//!
//! // Define migrations
//! struct CreateAccountsTable;
//!
//! impl Migration for CreateAccountsTable {
//!     fn version(&self) -> u32 {
//!         2
//!     }
//!
//!     fn name(&self) -> &str {
//!         "create_accounts_table"
//!     }
//!
//!     fn up(&self, conn: &Connection) -> Result<()> {
//!         conn.execute(
//!             "CREATE TABLE accounts (
//!                 id INTEGER PRIMARY KEY,
//!                 balance INTEGER NOT NULL DEFAULT 0,
//!                 created_at TEXT NOT NULL DEFAULT (datetime('now'))
//!             )",
//!             [],
//!         ).map_err(|e| AzothError::Projection(e.to_string()))?;
//!         Ok(())
//!     }
//!
//!     fn down(&self, conn: &Connection) -> Result<()> {
//!         conn.execute("DROP TABLE accounts", [])
//!             .map_err(|e| AzothError::Projection(e.to_string()))?;
//!         Ok(())
//!     }
//! }
//!
//! # fn main() -> Result<()> {
//! // Create manager and register migrations
//! let mut manager = MigrationManager::new();
//! manager.add(Box::new(CreateAccountsTable));
//!
//! // Run migrations
//! let db = AzothDb::open("./data")?;
//! manager.run(db.projection())?;
//! # Ok(())
//! # }
//! ```

use crate::{AzothError, ProjectionStore, Result};
use rusqlite::Connection;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Migration trait
///
/// Implement this to define a database migration.
pub trait Migration: Send + Sync {
    /// The version number this migration targets
    ///
    /// Versions should be sequential starting from 2 (version 1 is the base schema).
    fn version(&self) -> u32;

    /// Human-readable name for this migration
    fn name(&self) -> &str;

    /// Apply the migration (upgrade)
    fn up(&self, conn: &Connection) -> Result<()>;

    /// Rollback the migration (downgrade)
    ///
    /// Optional - default implementation returns an error.
    fn down(&self, _conn: &Connection) -> Result<()> {
        Err(AzothError::InvalidState(format!(
            "Migration '{}' does not support rollback",
            self.name()
        )))
    }

    /// Optional: verify the migration was applied correctly
    fn verify(&self, _conn: &Connection) -> Result<()> {
        Ok(())
    }
}

/// Migration manager
///
/// Manages a collection of migrations and runs them in order.
pub struct MigrationManager {
    migrations: Vec<Box<dyn Migration>>,
}

impl MigrationManager {
    /// Create a new migration manager
    pub fn new() -> Self {
        Self {
            migrations: Vec::new(),
        }
    }

    /// Add a migration
    ///
    /// Migrations will be sorted by version when run.
    pub fn add(&mut self, migration: Box<dyn Migration>) {
        self.migrations.push(migration);
    }

    /// Add multiple migrations
    pub fn add_all(&mut self, migrations: Vec<Box<dyn Migration>>) {
        for m in migrations {
            self.add(m);
        }
    }

    /// Load migrations from a directory
    ///
    /// Scans the directory for migration files and loads them.
    /// Migration files should be named: `{version}_{name}.sql`
    /// For example: `0002_create_users_table.sql`
    ///
    /// Optional down migrations can be in a file named: `{version}_{name}.down.sql`
    pub fn load_from_directory<P: AsRef<Path>>(&mut self, path: P) -> Result<()> {
        let path = path.as_ref();
        if !path.exists() {
            return Err(AzothError::InvalidState(format!(
                "Migration directory does not exist: {}",
                path.display()
            )));
        }

        let mut entries: Vec<_> = fs::read_dir(path)
            .map_err(|e| AzothError::Projection(format!("Failed to read directory: {}", e)))?
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "sql")
                    .unwrap_or(false)
            })
            .collect();

        entries.sort_by_key(|e| e.path());

        for entry in entries {
            let path = entry.path();
            let file_name = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| AzothError::InvalidState("Invalid migration filename".into()))?;

            // Skip .down.sql files
            if file_name.ends_with(".down") {
                continue;
            }

            let migration = FileMigration::from_file(&path)?;
            self.add(Box::new(migration));
        }

        Ok(())
    }

    /// Run all pending migrations on a SQLite projection store
    ///
    /// Applies migrations in version order, starting from the current schema version.
    /// Also initializes the migration history table if it doesn't exist.
    pub fn run(&self, projection: &Arc<crate::SqliteProjectionStore>) -> Result<()> {
        // Initialize migration history table
        self.init_migration_history(projection)?;

        // Get current version
        let current_version = projection.schema_version()?;

        // Sort migrations by version
        let mut sorted: Vec<_> = self.migrations.iter().collect();
        sorted.sort_by_key(|m| m.version());

        // Find pending migrations
        let pending: Vec<_> = sorted
            .into_iter()
            .filter(|m| m.version() > current_version)
            .collect();

        if pending.is_empty() {
            tracing::info!("No pending migrations");
            return Ok(());
        }

        tracing::info!("Running {} pending migrations", pending.len());

        // Run each migration
        for migration in pending {
            self.run_single(projection, &**migration)?;
        }

        Ok(())
    }

    /// Initialize the migration history table
    fn init_migration_history(&self, projection: &Arc<crate::SqliteProjectionStore>) -> Result<()> {
        let conn = projection.conn().lock().unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS migration_history (
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                applied_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }

    /// Run a single migration
    fn run_single(
        &self,
        projection: &Arc<crate::SqliteProjectionStore>,
        migration: &dyn Migration,
    ) -> Result<()> {
        tracing::info!(
            "Applying migration v{}: {}",
            migration.version(),
            migration.name()
        );

        // Execute the migration SQL
        {
            let conn = projection.conn().lock().unwrap();
            migration.up(&conn)?;

            // Record in migration history
            conn.execute(
                "INSERT OR REPLACE INTO migration_history (version, name, applied_at)
                 VALUES (?1, ?2, datetime('now'))",
                rusqlite::params![migration.version(), migration.name()],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        }

        // Update schema version
        projection.migrate(migration.version())?;

        tracing::info!("Migration v{} complete", migration.version());
        Ok(())
    }

    /// Rollback the last migration
    ///
    /// Warning: This is dangerous and may result in data loss.
    pub fn rollback_last(&self, projection: &Arc<crate::SqliteProjectionStore>) -> Result<()> {
        let current_version = projection.schema_version()?;

        if current_version <= 1 {
            return Err(AzothError::InvalidState(
                "Cannot rollback base schema".into(),
            ));
        }

        // Find migration for current version
        let migration = self
            .migrations
            .iter()
            .find(|m| m.version() == current_version)
            .ok_or_else(|| {
                AzothError::InvalidState(format!(
                    "No migration found for version {}",
                    current_version
                ))
            })?;

        tracing::warn!(
            "Rolling back migration v{}: {}",
            migration.version(),
            migration.name()
        );

        // Execute the down() migration
        {
            let conn = projection.conn().lock().unwrap();
            migration.down(&conn)?;
        }

        // Update schema version
        projection.migrate(current_version - 1)?;

        Ok(())
    }

    /// List all migrations
    pub fn list(&self) -> Vec<MigrationInfo> {
        let mut sorted: Vec<_> = self.migrations.iter().collect();
        sorted.sort_by_key(|m| m.version());

        sorted
            .into_iter()
            .map(|m| MigrationInfo {
                version: m.version(),
                name: m.name().to_string(),
            })
            .collect()
    }

    /// Get pending migrations
    pub fn pending(
        &self,
        projection: &Arc<crate::SqliteProjectionStore>,
    ) -> Result<Vec<MigrationInfo>> {
        let current_version = projection.schema_version()?;

        let mut sorted: Vec<_> = self.migrations.iter().collect();
        sorted.sort_by_key(|m| m.version());

        Ok(sorted
            .into_iter()
            .filter(|m| m.version() > current_version)
            .map(|m| MigrationInfo {
                version: m.version(),
                name: m.name().to_string(),
            })
            .collect())
    }

    /// Get migration history from the database
    pub fn history(
        &self,
        projection: &Arc<crate::SqliteProjectionStore>,
    ) -> Result<Vec<MigrationHistoryEntry>> {
        self.init_migration_history(projection)?;

        let conn = projection.conn().lock().unwrap();
        let mut stmt = conn
            .prepare("SELECT version, name, applied_at FROM migration_history ORDER BY version")
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        let entries = stmt
            .query_map([], |row| {
                Ok(MigrationHistoryEntry {
                    version: row.get(0)?,
                    name: row.get(1)?,
                    applied_at: row.get(2)?,
                })
            })
            .map_err(|e| AzothError::Projection(e.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        Ok(entries)
    }

    /// Generate a new migration file in the specified directory
    ///
    /// Creates a new migration file with the next available version number.
    /// Returns the path to the created file.
    pub fn generate<P: AsRef<Path>>(&self, migrations_dir: P, name: &str) -> Result<PathBuf> {
        let migrations_dir = migrations_dir.as_ref();

        // Create directory if it doesn't exist
        fs::create_dir_all(migrations_dir)
            .map_err(|e| AzothError::Projection(format!("Failed to create directory: {}", e)))?;

        // Find next version number
        let next_version = self
            .migrations
            .iter()
            .map(|m| m.version())
            .max()
            .unwrap_or(1)
            + 1;

        // Create filename
        let filename = format!("{:04}_{}.sql", next_version, name);
        let filepath = migrations_dir.join(&filename);

        // Create file with template
        let template = format!(
            "-- Migration: {}\n-- Version: {}\n-- Created: {}\n\n-- Add your migration SQL here\n",
            name,
            next_version,
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        );

        fs::write(&filepath, template)
            .map_err(|e| AzothError::Projection(format!("Failed to write file: {}", e)))?;

        // Also create a .down.sql file
        let down_filename = format!("{:04}_{}.down.sql", next_version, name);
        let down_filepath = migrations_dir.join(&down_filename);
        let down_template = format!(
            "-- Rollback: {}\n-- Version: {}\n\n-- Add your rollback SQL here\n",
            name, next_version
        );

        fs::write(&down_filepath, down_template)
            .map_err(|e| AzothError::Projection(format!("Failed to write file: {}", e)))?;

        tracing::info!(
            "Created migration files:\n  - {}\n  - {}",
            filepath.display(),
            down_filepath.display()
        );

        Ok(filepath)
    }
}

impl Default for MigrationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Migration information
#[derive(Debug, Clone)]
pub struct MigrationInfo {
    pub version: u32,
    pub name: String,
}

/// Migration history entry
#[derive(Debug, Clone)]
pub struct MigrationHistoryEntry {
    pub version: u32,
    pub name: String,
    pub applied_at: String,
}

/// File-based migration
///
/// Loads migration SQL from files on disk.
pub struct FileMigration {
    version: u32,
    name: String,
    up_sql: String,
    down_sql: Option<String>,
}

impl FileMigration {
    /// Load a migration from a SQL file
    ///
    /// The filename should be in the format: `{version}_{name}.sql`
    /// Optional down migration can be in: `{version}_{name}.down.sql`
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let file_name = path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| AzothError::InvalidState("Invalid migration filename".into()))?;

        // Parse version and name from filename
        let parts: Vec<&str> = file_name.splitn(2, '_').collect();
        if parts.len() != 2 {
            return Err(AzothError::InvalidState(format!(
                "Invalid migration filename format: {}. Expected: {{version}}_{{name}}.sql",
                file_name
            )));
        }

        let version: u32 = parts[0].parse().map_err(|_| {
            AzothError::InvalidState(format!("Invalid version number in filename: {}", parts[0]))
        })?;

        let name = parts[1].to_string();

        // Read up migration SQL
        let up_sql = fs::read_to_string(path)
            .map_err(|e| AzothError::Projection(format!("Failed to read migration file: {}", e)))?;

        // Try to read down migration SQL
        let down_path = path.with_file_name(format!("{}.down.sql", file_name));
        let down_sql = if down_path.exists() {
            Some(fs::read_to_string(&down_path).map_err(|e| {
                AzothError::Projection(format!("Failed to read down migration file: {}", e))
            })?)
        } else {
            None
        };

        Ok(Self {
            version,
            name,
            up_sql,
            down_sql,
        })
    }
}

impl Migration for FileMigration {
    fn version(&self) -> u32 {
        self.version
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn up(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch(&self.up_sql)
            .map_err(|e| AzothError::Projection(format!("Migration failed: {}", e)))?;
        Ok(())
    }

    fn down(&self, conn: &Connection) -> Result<()> {
        if let Some(ref down_sql) = self.down_sql {
            conn.execute_batch(down_sql)
                .map_err(|e| AzothError::Projection(format!("Rollback failed: {}", e)))?;
            Ok(())
        } else {
            Err(AzothError::InvalidState(format!(
                "Migration '{}' does not have a down migration file",
                self.name
            )))
        }
    }
}

/// Helper macro to define a migration
///
/// # Example
///
/// ```ignore
/// migration!(
///     CreateUsersTable,
///     version: 2,
///     name: "create_users_table",
///     up: |conn| {
///         conn.execute(
///             "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)",
///             [],
///         )?;
///         Ok(())
///     },
///     down: |conn| {
///         conn.execute("DROP TABLE users", [])?;
///         Ok(())
///     }
/// );
/// ```
#[macro_export]
macro_rules! migration {
    (
        $name:ident,
        version: $version:expr,
        name: $migration_name:expr,
        up: |$up_conn:ident| $up_body:block
        $(, down: |$down_conn:ident| $down_body:block)?
    ) => {
        struct $name;

        impl $crate::Migration for $name {
            fn version(&self) -> u32 {
                $version
            }

            fn name(&self) -> &str {
                $migration_name
            }

            fn up(&self, $up_conn: &rusqlite::Connection) -> $crate::Result<()> {
                $up_body
            }

            $(
                fn down(&self, $down_conn: &rusqlite::Connection) -> $crate::Result<()> {
                    $down_body
                }
            )?
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestMigration;

    impl Migration for TestMigration {
        fn version(&self) -> u32 {
            2
        }

        fn name(&self) -> &str {
            "test_migration"
        }

        fn up(&self, _conn: &Connection) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn test_migration_manager() {
        let mut manager = MigrationManager::new();
        manager.add(Box::new(TestMigration));

        let migrations = manager.list();
        assert_eq!(migrations.len(), 1);
        assert_eq!(migrations[0].version, 2);
        assert_eq!(migrations[0].name, "test_migration");
    }
}
