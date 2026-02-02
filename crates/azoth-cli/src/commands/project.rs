//! Project command implementation

use anyhow::{Context, Result};
use azoth::prelude::*;
use std::path::PathBuf;

pub fn execute(db_path: PathBuf, continuous: bool) -> Result<()> {
    let db = AzothDb::open(&db_path).context("Failed to open database")?;

    if continuous {
        println!("Running projector continuously... (Press Ctrl+C to stop)");

        // This would need a proper async runtime and signal handling
        // For now, just show the concept
        loop {
            let stats = db.projector().run_once().context("Projector failed")?;

            if stats.events_applied > 0 {
                println!(
                    "Processed {} events ({} bytes) in {:?}",
                    stats.events_applied, stats.bytes_processed, stats.duration
                );
            }

            // Small delay to avoid tight loop
            std::thread::sleep(std::time::Duration::from_millis(100));
        }
    } else {
        println!("Running projector once...");

        let stats = db.projector().run_once().context("Projector failed")?;

        println!(
            "✓ Processed {} events ({} bytes) in {:?}",
            stats.events_applied, stats.bytes_processed, stats.duration
        );

        if stats.events_applied == 0 {
            println!("No events to process - projection is up to date");
        }
    }

    Ok(())
}
