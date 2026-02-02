# Azoth CLI

Command-line interface for Azoth database operations.

## Installation

```bash
cargo install --path crates/azoth-cli
```

Or build from source:

```bash
cargo build --release --package azoth-cli
```

The binary will be available at `target/release/azoth`.

## Usage

```bash
azoth [OPTIONS] <COMMAND>
```

### Options

- `-d, --db-path <DB_PATH>` - Path to the database directory (default: `./data`)
- `-v, --verbose` - Enable verbose logging
- `-h, --help` - Print help
- `-V, --version` - Print version

## Commands

### Migration Management

#### List Migrations

List all registered migrations from a directory:

```bash
azoth migrate list -m ./migrations
```

#### Show Migration History

Display all applied migrations with timestamps:

```bash
azoth migrate history
```

#### Show Pending Migrations

List migrations that haven't been applied yet:

```bash
azoth migrate pending -m ./migrations
```

#### Run Migrations

Apply all pending migrations:

```bash
azoth migrate run -m ./migrations
```

Dry run (show what would be applied without executing):

```bash
azoth migrate run -m ./migrations --dry-run
```

#### Generate Migration

Create a new migration file:

```bash
azoth migrate generate create_users_table -m ./migrations
```

This creates two files:
- `{version}_create_users_table.sql` - Up migration
- `{version}_create_users_table.down.sql` - Down migration (rollback)

#### Rollback Migration

Rollback the last applied migration:

```bash
azoth migrate rollback
```

Skip confirmation prompt:

```bash
azoth migrate rollback --force
```

### Database Status

View database information and projection status:

```bash
azoth status
```

Example output:
```
Database Status
============================================================
Path: ./data
Schema Version: 3

Canonical Store:
  Next Event ID: 1000

Projection Store:
  Last Applied Event ID: 999
  Schema Version: 3

✓ Projection is up to date
```

### Run Projector

Apply pending events to projections:

```bash
azoth project
```

Run continuously (processes events as they arrive):

```bash
azoth project --continuous
```

## Examples

### Complete Migration Workflow

```bash
# Create a migrations directory
mkdir -p ./migrations

# Generate a new migration
azoth migrate generate create_users_table -m ./migrations

# Edit the generated SQL files
# ... add your SQL to migrations/0002_create_users_table.sql ...

# Check what will be applied
azoth migrate pending -m ./migrations

# Apply migrations (dry run first)
azoth migrate run -m ./migrations --dry-run

# Apply for real
azoth migrate run -m ./migrations

# View migration history
azoth migrate history

# Check database status
azoth status
```

### Database Operations

```bash
# Check database status
azoth -d ./production-db status

# Run projector once
azoth -d ./production-db project

# Run migrations with custom db path
azoth -d ./production-db migrate run -m ./migrations

# Enable verbose logging
azoth -v migrate run -m ./migrations
```

## Migration File Format

Migration files should be named: `{version}_{name}.sql`

Example: `0002_create_users_table.sql`

```sql
-- Migration: create_users_table
-- Version: 2
-- Created: 2026-02-01 12:00:00 UTC

CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    email TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_users_email ON users(email);
```

Rollback file: `0002_create_users_table.down.sql`

```sql
-- Rollback: create_users_table
-- Version: 2

DROP INDEX IF EXISTS idx_users_email;
DROP TABLE IF EXISTS users;
```

## Exit Codes

- `0` - Success
- `1` - Error (check error message for details)

## Environment Variables

- `RUST_LOG` - Controls logging level (e.g., `RUST_LOG=debug azoth status`)

## Tips

1. **Always test migrations in a development environment first**
2. **Use dry-run mode** to preview changes: `azoth migrate run --dry-run`
3. **Keep migrations in version control** alongside your code
4. **Use descriptive migration names** that explain what they do
5. **Test rollback migrations** if you provide them
6. **Run projector** after applying migrations: `azoth project`
7. **Check status regularly** to monitor projection lag: `azoth status`

## Troubleshooting

### "Failed to open database"
- Ensure the database path exists or can be created
- Check file permissions
- Verify the path is correct

### "Migrations directory does not exist"
- Create the directory: `mkdir -p ./migrations`
- Or specify the correct path with `-m`

### "No pending migrations"
- All migrations have been applied
- Check `azoth migrate history` to see what's been applied
- Generate new migrations if needed

### "Projection lag: X events behind"
- Run `azoth project` to catch up
- Use `azoth project --continuous` for real-time processing

## Development

Build the CLI:
```bash
cargo build --package azoth-cli
```

Run tests:
```bash
cargo test --package azoth-cli
```

Run with cargo:
```bash
cargo run --package azoth-cli -- status
```
