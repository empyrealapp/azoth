//! Tests for migration system

use azoth::prelude::*;
use azoth::{Migration, MigrationManager};
use rusqlite::Connection;

struct CreateUsersTable;

impl Migration for CreateUsersTable {
    fn version(&self) -> u32 {
        1 // First migration starts at version 1
    }

    fn name(&self) -> &str {
        "create_users_table"
    }

    fn up(&self, conn: &Connection) -> Result<()> {
        conn.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }

    fn down(&self, conn: &Connection) -> Result<()> {
        conn.execute("DROP TABLE users", [])
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }
}

struct AddEmailToUsers;

impl Migration for AddEmailToUsers {
    fn version(&self) -> u32 {
        2 // Second migration is version 2
    }

    fn name(&self) -> &str {
        "add_email_to_users"
    }

    fn up(&self, conn: &Connection) -> Result<()> {
        conn.execute("ALTER TABLE users ADD COLUMN email TEXT", [])
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        Ok(())
    }
}

#[test]
fn test_migration_manager() {
    let mut manager = MigrationManager::new();

    // Add migrations
    manager.add(Box::new(CreateUsersTable));
    manager.add(Box::new(AddEmailToUsers));

    // Should list migrations
    let migrations = manager.list();
    assert_eq!(migrations.len(), 2);
    assert_eq!(migrations[0].version, 1);
    assert_eq!(migrations[0].name, "create_users_table");
    assert_eq!(migrations[1].version, 2);
    assert_eq!(migrations[1].name, "add_email_to_users");
}

#[test]
fn test_migration_ordering() {
    let mut manager = MigrationManager::new();

    // Add out of order
    manager.add(Box::new(AddEmailToUsers));
    manager.add(Box::new(CreateUsersTable));

    // Should be sorted by version
    let migrations = manager.list();
    assert_eq!(migrations[0].version, 1);
    assert_eq!(migrations[1].version, 2);
}

#[test]
fn test_pending_migrations() {
    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();

    let mut manager = MigrationManager::new();
    manager.add(Box::new(CreateUsersTable));
    manager.add(Box::new(AddEmailToUsers));

    // All migrations should be pending (schema starts at 0)
    let pending = manager.pending(db.projection()).unwrap();
    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0].version, 1);
    assert_eq!(pending[1].version, 2);

    // After migrating to v1, only v2 should be pending
    db.projection().migrate(1).unwrap();
    let pending = manager.pending(db.projection()).unwrap();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].version, 2);

    // After migrating to v2, none should be pending
    db.projection().migrate(2).unwrap();
    let pending = manager.pending(db.projection()).unwrap();
    assert_eq!(pending.len(), 0);
}

#[test]
fn test_migration_add_all() {
    let mut manager = MigrationManager::new();

    let migrations: Vec<Box<dyn Migration>> =
        vec![Box::new(CreateUsersTable), Box::new(AddEmailToUsers)];

    manager.add_all(migrations);

    assert_eq!(manager.list().len(), 2);
}
