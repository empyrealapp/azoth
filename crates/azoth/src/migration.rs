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

    /// Run all pending migrations on a SQLite projection store
    ///
    /// Applies migrations in version order, starting from the current schema version.
    pub fn run(&self, projection: &Arc<crate::SqliteProjectionStore>) -> Result<()> {
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
