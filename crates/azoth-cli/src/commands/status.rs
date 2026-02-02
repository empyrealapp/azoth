//! Status command implementation

use anyhow::{Context, Result};
use azoth::prelude::*;
use std::path::PathBuf;

pub fn execute(db_path: PathBuf) -> Result<()> {
    tracing::info!("Checking database status: {}", db_path.display());

    let db = AzothDb::open(&db_path).context("Failed to open database")?;

    println!("\nDatabase Status");
    println!("{}", "=".repeat(60));
    println!("Path: {}", db_path.display());

    // Schema version
    let schema_version = db
        .projection()
        .schema_version()
        .context("Failed to get schema version")?;
    println!("Schema Version: {}", schema_version);

    // Canonical store metadata
    let canonical_meta = db
        .canonical()
        .meta()
        .context("Failed to get canonical metadata")?;
    println!("\nCanonical Store:");
    println!("  Next Event ID: {}", canonical_meta.next_event_id);
    if let Some(sealed) = canonical_meta.sealed_event_id {
        println!("  Sealed Event ID: {}", sealed);
    }

    // Projection cursor
    let cursor = db
        .projection()
        .get_cursor()
        .context("Failed to get projection cursor")?;
    println!("\nProjection Store:");

    let last_applied = if cursor == u64::MAX {
        -1i64
    } else {
        cursor as i64
    };
    println!("  Last Applied Event ID: {}", last_applied);
    println!("  Schema Version: {}", schema_version);

    // Calculate lag
    let events_available = if canonical_meta.next_event_id > 0 {
        canonical_meta.next_event_id - 1
    } else {
        0
    };

    if cursor == u64::MAX && events_available > 0 {
        println!(
            "\n⚠️  Projection lag: {} event(s) behind",
            events_available + 1
        );
        println!("Run 'azoth project' to catch up");
    } else if cursor < events_available {
        let lag = events_available - cursor;
        println!("\n⚠️  Projection lag: {} event(s) behind", lag);
        println!("Run 'azoth project' to catch up");
    } else {
        println!("\n✓ Projection is up to date");
    }

    Ok(())
}
