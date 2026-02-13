//! Example demonstrating the migration system
//!
//! Run with: cargo run --example migration_example

use azoth::migration;
use azoth::prelude::*;
use tempfile::tempdir;

fn main() -> Result<()> {
    // Setup tracing
    tracing_subscriber::fmt::init();

    // Create a temporary database
    let temp_dir = tempdir().unwrap();
    let db_path = temp_dir.path().join("example.db");

    println!("Creating database at: {}", db_path.display());
    let db = AzothDb::open(&db_path)?;

    // Create migration manager
    let mut manager = MigrationManager::new();

    // Example 1: Add migrations using the macro
    println!("\n=== Example 1: Code-based Migrations ===");

    migration!(
        CreateUsersTable,
        version: 2,
        name: "create_users_table",
        up: |conn| {
            conn.execute(
                "CREATE TABLE users (
                    id INTEGER PRIMARY KEY,
                    username TEXT NOT NULL UNIQUE,
                    email TEXT NOT NULL UNIQUE,
                    created_at TEXT NOT NULL DEFAULT (datetime('now'))
                )",
                [],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;
            conn.execute(
                "CREATE INDEX idx_users_email ON users(email)",
                [],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;
            Ok(())
        },
        down: |conn| {
            conn.execute("DROP INDEX IF EXISTS idx_users_email", [])
                .map_err(|e| AzothError::Projection(e.to_string()))?;
            conn.execute("DROP TABLE IF EXISTS users", [])
                .map_err(|e| AzothError::Projection(e.to_string()))?;
            Ok(())
        }
    );

    migration!(
        CreatePostsTable,
        version: 3,
        name: "create_posts_table",
        up: |conn| {
            conn.execute(
                "CREATE TABLE posts (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT (datetime('now')),
                    FOREIGN KEY (user_id) REFERENCES users(id)
                )",
                [],
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?;
            Ok(())
        },
        down: |conn| {
            conn.execute("DROP TABLE IF EXISTS posts", [])
                .map_err(|e| AzothError::Projection(e.to_string()))?;
            Ok(())
        }
    );

    manager.add(Box::new(CreateUsersTable));
    manager.add(Box::new(CreatePostsTable));

    // List migrations
    println!("\nRegistered migrations:");
    for m in manager.list() {
        println!("  v{}: {}", m.version, m.name);
    }

    // Check pending
    let pending = manager.pending(db.projection())?;
    println!("\nPending migrations: {}", pending.len());
    for m in &pending {
        println!("  v{}: {}", m.version, m.name);
    }

    // Run migrations
    println!("\nRunning migrations...");
    manager.run(db.projection())?;

    // Check history
    let history = manager.history(db.projection())?;
    println!("\nMigration history:");
    for entry in &history {
        println!(
            "  v{}: {} (applied: {})",
            entry.version, entry.name, entry.applied_at
        );
    }

    // Example 2: File-based migrations
    println!("\n=== Example 2: File-based Migrations ===");

    // Create a temporary migrations directory
    let migrations_dir = temp_dir.path().join("migrations");
    std::fs::create_dir_all(&migrations_dir).unwrap();

    // Generate a new migration
    let migration_file = manager.generate(&migrations_dir, "add_user_preferences")?;
    println!("Generated migration: {}", migration_file.display());

    // Read and display the generated file
    let content = std::fs::read_to_string(&migration_file).unwrap();
    println!("\nGenerated migration content:");
    println!("{}", content);

    // Load migrations from directory
    let mut file_manager = MigrationManager::new();
    file_manager.load_from_directory(&migrations_dir)?;

    println!(
        "\nLoaded {} migrations from directory",
        file_manager.list().len()
    );
    for m in file_manager.list() {
        println!("  v{}: {}", m.version, m.name);
    }

    // Example 3: Demonstrate verification
    println!("\n=== Example 3: Migration Verification ===");

    // Check that tables were created
    {
        let conn = db.projection().conn().lock();

        // Query for users table
        let tables: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('users', 'posts')",
            )
            .map_err(|e| AzothError::Projection(e.to_string()))?
            .query_map([], |row| row.get(0))
            .map_err(|e| AzothError::Projection(e.to_string()))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        println!("Tables created: {:?}", tables);

        // Insert some test data
        conn.execute(
            "INSERT INTO users (username, email) VALUES (?1, ?2)",
            rusqlite::params!["alice", "alice@example.com"],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        conn.execute(
            "INSERT INTO posts (user_id, title, content) VALUES (?1, ?2, ?3)",
            rusqlite::params![1, "First Post", "Hello, world!"],
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

        // Query the data
        let user_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM users", [], |row| row.get(0))
            .map_err(|e| AzothError::Projection(e.to_string()))?;
        let post_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM posts", [], |row| row.get(0))
            .map_err(|e| AzothError::Projection(e.to_string()))?;

        println!("Users: {}, Posts: {}", user_count, post_count);
    }

    // Example 4: Schema version tracking
    println!("\n=== Example 4: Schema Version ===");
    let schema_version = db.projection().schema_version()?;
    println!("Current schema version: {}", schema_version);

    println!("\n=== Migration Example Complete ===");

    Ok(())
}
