use azoth_core::error::{AzothError, Result};
use rusqlite::Connection;

/// Update schema version
///
/// This is called by the MigrationManager after a migration has been applied.
/// It simply updates the schema_version in the projection_meta table.
///
/// Note: This function does NOT apply any migrations itself. All migration
/// logic should be defined using the Migration trait and applied via MigrationManager.
pub fn migrate(conn: &Connection, target_version: u32) -> Result<()> {
    let current_version: u32 = conn
        .query_row(
            "SELECT schema_version FROM projection_meta WHERE id = 0",
            [],
            |row| {
                let v: i64 = row.get(0)?;
                Ok(v as u32)
            },
        )
        .map_err(|e| AzothError::Projection(e.to_string()))?;

    if target_version < current_version {
        return Err(AzothError::InvalidState(format!(
            "Cannot downgrade schema version from {} to {}. Use MigrationManager::rollback_last() instead.",
            current_version, target_version
        )));
    }

    if current_version == target_version {
        // Already at target version
        return Ok(());
    }

    // Update to target version
    update_schema_version(conn, target_version)?;
    Ok(())
}

/// Update the schema version in metadata
fn update_schema_version(conn: &Connection, version: u32) -> Result<()> {
    conn.execute(
        "UPDATE projection_meta SET schema_version = ?1, updated_at = datetime('now') WHERE id = 0",
        [version as i64],
    )
    .map_err(|e| AzothError::Projection(e.to_string()))?;

    Ok(())
}
