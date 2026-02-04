# Database Migrations

Database migrations for Azoth projection stores.

## Quick Start with CLI

The easiest way to manage migrations is using the Azoth CLI:

```bash
# Generate a new migration
azoth migrate generate create_users_table -m ./migrations

# List migrations
azoth migrate list -m ./migrations

# Check pending migrations
azoth migrate pending -m ./migrations

# Run migrations (dry-run first)
azoth migrate run -m ./migrations --dry-run
azoth migrate run -m ./migrations

# View migration history
azoth migrate history

# Check database status
azoth status
```

See [CLI Documentation](../crates/azoth-cli/README.md) for complete CLI reference.

## Migration System Overview

Azoth provides a powerful migration system that supports both file-based and code-based migrations:

- **File-based migrations**: Store SQL in separate `.sql` files for easier review and version control
- **Code-based migrations**: Implement the `Migration` trait for complex migrations that require Rust logic
- **Migration history**: Track all applied migrations with timestamps
- **Auto-discovery**: Automatically load migrations from a directory
- **Rollback support**: Optional `.down.sql` files for rollback capability
- **CLI tool**: Command-line interface for easy migration management

## File Structure

Migration files should follow this naming convention:

```
{version}_{name}.sql          # Up migration
{version}_{name}.down.sql     # Down migration (optional)
```

For example:
```
0001_create_users_table.sql
0001_create_users_table.down.sql
0002_add_user_roles.sql
0002_add_user_roles.down.sql
```

### Version Numbers

- **Version 1+**: Your migrations, numbered sequentially starting from 1
- Use 4-digit padding (e.g., `0001`, `0002`) for proper sorting

## Using File-Based Migrations

### 1. Generate a New Migration

**Using CLI (recommended):**

```bash
azoth migrate generate create_users_table -m ./migrations
```

**Using Rust API:**

```rust
use azoth::prelude::*;

let mut manager = MigrationManager::new();
let path = manager.generate("./migrations", "create_users_table")?;
// Creates:
//   ./migrations/0001_create_users_table.sql
//   ./migrations/0001_create_users_table.down.sql
```

### 2. Edit the Generated Files

Add your SQL to the `.sql` file:

```sql
-- migrations/0001_create_users_table.sql
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_users_email ON users(email);
```

Optionally add rollback SQL to the `.down.sql` file:

```sql
-- migrations/0001_create_users_table.down.sql
DROP INDEX IF EXISTS idx_users_email;
DROP TABLE IF EXISTS users;
```

### 3. Load and Run Migrations

**Using CLI (recommended):**

```bash
# Check what will be applied
azoth migrate pending -m ./migrations

# Run migrations
azoth migrate run -m ./migrations
```

**Using Rust API:**

```rust
use azoth::prelude::*;

let db = AzothDb::open("./data")?;
let mut manager = MigrationManager::new();

// Auto-discover and load all migrations from directory
manager.load_from_directory("./migrations")?;

// Run all pending migrations
manager.run(db.projection())?;
```

## Using Code-Based Migrations

For complex migrations that require Rust logic:

### Using the `migration!` Macro

```rust
use azoth::prelude::*;

migration!(
    CreateUsersTable,
    version: 1,
    name: "create_users_table",
    up: |conn| {
        conn.execute(
            "CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
            [],
        )?;
        Ok(())
    },
    down: |conn| {
        conn.execute("DROP TABLE users", [])?;
        Ok(())
    }
);

// Use it
let mut manager = MigrationManager::new();
manager.add(Box::new(CreateUsersTable));
```

### Implementing the Migration Trait

For more control:

```rust
use azoth::prelude::*;
use rusqlite::Connection;

struct CreateUsersTable;

impl Migration for CreateUsersTable {
    fn version(&self) -> u32 {
        1  // First migration starts at version 1
    }

    fn name(&self) -> &str {
        "create_users_table"
    }

    fn up(&self, conn: &Connection) -> Result<()> {
        // Your migration logic here
        conn.execute(
            "CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)",
            [],
        )?;
        Ok(())
    }

    fn down(&self, conn: &Connection) -> Result<()> {
        conn.execute("DROP TABLE users", [])?;
        Ok(())
    }
}
```

## Migration Management Commands

### Using CLI

```bash
# List all migrations
azoth migrate list -m ./migrations

# Show pending migrations
azoth migrate pending -m ./migrations

# View migration history
azoth migrate history

# Run migrations
azoth migrate run -m ./migrations

# Run with dry-run (show what would happen)
azoth migrate run -m ./migrations --dry-run

# Rollback last migration
azoth migrate rollback

# Force rollback without confirmation
azoth migrate rollback --force
```

### Using Rust API

```rust
// List all registered migrations
let migrations = manager.list();
for m in migrations {
    println!("v{}: {}", m.version, m.name);
}

// Get pending migrations
let pending = manager.pending(db.projection())?;
println!("Pending migrations: {}", pending.len());

// View migration history
let history = manager.history(db.projection())?;
for entry in history {
    println!("v{}: {} (applied: {})", entry.version, entry.name, entry.applied_at);
}

// Run all pending migrations
manager.run(db.projection())?;

// Rollback the last applied migration
// WARNING: This may result in data loss
manager.rollback_last(db.projection())?;
```

## Best Practices

1. **Version Control**: Always commit migration files with your code
2. **Test Down Migrations**: If you provide rollback migrations, test them
3. **Idempotent Operations**: Use `CREATE TABLE IF NOT EXISTS` and similar for safety
4. **Incremental Changes**: Keep migrations small and focused
5. **Data Migrations**: For data transformations, consider doing them in separate migrations
6. **Never Edit Applied Migrations**: Once a migration is applied in production, create a new migration for changes

## Migration History Table

The migration system automatically creates a `migration_history` table:

```sql
CREATE TABLE migration_history (
    version INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

This tracks all applied migrations with timestamps, providing a complete audit trail.

## Schema Version Tracking

The schema version is also tracked in the `projection_meta` table:

```sql
CREATE TABLE projection_meta (
    id INTEGER PRIMARY KEY CHECK (id = 0),
    last_applied_event_id INTEGER NOT NULL DEFAULT -1,
    schema_version INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

This ensures consistency between the migration system and the projection store.

## Example: Complete Migration Workflow

```rust
use azoth::prelude::*;

fn main() -> Result<()> {
    // Open database
    let db = AzothDb::open("./data")?;

    // Create migration manager
    let mut manager = MigrationManager::new();

    // Option 1: Load from files
    manager.load_from_directory("./migrations")?;

    // Option 2: Add code-based migrations
    migration!(
        AddUserRoles,
        version: 2,
        name: "add_user_roles",
        up: |conn| {
            conn.execute(
                "ALTER TABLE users ADD COLUMN role TEXT DEFAULT 'user'",
                [],
            )?;
            Ok(())
        }
    );
    manager.add(Box::new(AddUserRoles));

    // Check what's pending
    let pending = manager.pending(db.projection())?;
    println!("Found {} pending migrations", pending.len());

    // Run migrations
    manager.run(db.projection())?;

    // View history
    let history = manager.history(db.projection())?;
    println!("\nMigration History:");
    for entry in history {
        println!("  v{}: {} ({})", entry.version, entry.name, entry.applied_at);
    }

    Ok(())
}
```

## Troubleshooting

### "No migration found for version X"
- Make sure the migration file exists and is properly named
- Check that you've called `load_from_directory()` or manually added the migration

### "Invalid migration filename format"
- Filenames must be: `{version}_{name}.sql`
- Version must be a valid number
- Use underscores to separate version from name

### "Migration failed"
- Check the SQL syntax in your migration file
- Verify that referenced tables/columns exist
- Review the error message for specific details

### "Cannot rollback below version 0"
- Schema version 0 is the initial state before any migrations
- Only versions 1+ can be rolled back

## Advanced Features

### Custom Migration Verification

Implement the `verify()` method to add post-migration checks:

```rust
impl Migration for CreateUsersTable {
    // ... other methods ...

    fn verify(&self, conn: &Connection) -> Result<()> {
        // Verify the table was created correctly
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='users'",
            [],
            |row| row.get(0),
        )?;

        if count != 1 {
            return Err(AzothError::InvalidState("Users table not created".into()));
        }

        Ok(())
    }
}
```

### Combining File and Code Migrations

You can mix file-based and code-based migrations in the same manager:

```rust
let mut manager = MigrationManager::new();

// Load file-based migrations
manager.load_from_directory("./migrations")?;

// Add code-based migrations
manager.add(Box::new(MyCustomMigration));

// They'll all run in version order
manager.run(db.projection())?;
```
