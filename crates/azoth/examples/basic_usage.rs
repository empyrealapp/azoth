//! Basic Azoth Usage Example
//!
//! This example demonstrates:
//! - Opening a database
//! - Writing state and events atomically
//! - Using typed values
//! - Running the projector
//! - Querying metadata
//! - Event iteration
//!
//! Run with: cargo run --example basic_usage

use azoth::prelude::*;
use azoth::TypedValue;

fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt()
        .with_env_filter("azoth=info")
        .init();

    println!("\n╔═══════════════════════════════════════╗");
    println!("║   Azoth Basic Usage Example          ║");
    println!("╚═══════════════════════════════════════╝\n");

    // Create temporary directory for this example
    let temp_dir = tempfile::tempdir()?;
    let db_path = temp_dir.path().join("azoth-example");

    // ========================================
    // 1. Open Database
    // ========================================
    println!("📂 Step 1: Opening Database");
    println!("   Path: {:?}", db_path);

    let db = AzothDb::open(&db_path)?;

    println!("   ✅ Database opened successfully");
    println!("   📁 Canonical: {}/canonical/", db_path.display());
    println!("   📁 Events: {}/canonical/event-log/", db_path.display());
    println!("   📁 Projection: {}/projection.db\n", db_path.display());

    // ========================================
    // 2. Initialize Account with Typed Values
    // ========================================
    println!("💰 Step 2: Creating Account");

    Transaction::new(&db).execute(|ctx| {
        // Use I64 for balances
        ctx.set(b"balance", &TypedValue::I64(10_000))?;
        ctx.set(b"account_id", &TypedValue::String("ACC-12345".into()))?;
        ctx.set(b"account_type", &TypedValue::String("premium".into()))?;

        // Log creation event
        ctx.log(
            "account_created",
            &serde_json::json!({
                "account_id": "ACC-12345",
                "initial_balance": 10000,
                "account_type": "premium",
                "timestamp": chrono::Utc::now().to_rfc3339()
            }),
        )?;

        Ok(())
    })?;

    println!("   ✅ Account created");
    println!("   💵 Initial balance: 10,000");
    println!("   🆔 Account ID: ACC-12345");
    println!("   ⭐ Account type: premium\n");

    // ========================================
    // 3. Perform Transactions
    // ========================================
    println!("💸 Step 3: Performing Transactions");

    // Transaction 1: Deposit
    println!("\n   Transaction 1: Deposit +5,000");
    Transaction::new(&db).execute(|ctx| {
        let balance = ctx.get(b"balance")?.as_i64()?;
        let new_balance = balance + 5_000;

        ctx.set(b"balance", &TypedValue::I64(new_balance))?;
        ctx.log(
            "deposit",
            &serde_json::json!({
                "amount": 5000,
                "new_balance": new_balance,
            }),
        )?;

        println!("      ✅ Deposited 5,000");
        println!("      💰 New balance: {}", new_balance);
        Ok(())
    })?;

    // Transaction 2: Withdrawal
    println!("\n   Transaction 2: Withdrawal -3,000");
    Transaction::new(&db).execute(|ctx| {
        let balance = ctx.get(b"balance")?.as_i64()?;
        let new_balance = balance - 3_000;

        ctx.set(b"balance", &TypedValue::I64(new_balance))?;
        ctx.log(
            "withdrawal",
            &serde_json::json!({
                "amount": 3000,
                "new_balance": new_balance,
            }),
        )?;

        println!("      ✅ Withdrew 3,000");
        println!("      💰 New balance: {}", new_balance);
        Ok(())
    })?;

    // Transaction 3: Fee deduction
    println!("\n   Transaction 3: Monthly fee -50");
    Transaction::new(&db).execute(|ctx| {
        let balance = ctx.get(b"balance")?.as_i64()?;
        let new_balance = balance - 50;

        ctx.set(b"balance", &TypedValue::I64(new_balance))?;
        ctx.log(
            "fee_charged",
            &serde_json::json!({
                "amount": 50,
                "fee_type": "monthly_maintenance",
                "new_balance": new_balance,
            }),
        )?;

        println!("      ✅ Fee charged: 50");
        println!("      💰 Final balance: {}\n", new_balance);
        Ok(())
    })?;

    // ========================================
    // 4. Read Final State
    // ========================================
    println!("📊 Step 4: Reading Account State");

    Transaction::new(&db).execute(|ctx| {
        let balance = ctx.get(b"balance")?.as_i64()?;

        let account_id = match ctx.get(b"account_id")? {
            TypedValue::String(s) => s,
            _ => "unknown".to_string(),
        };

        let account_type = match ctx.get(b"account_type")? {
            TypedValue::String(s) => s,
            _ => "unknown".to_string(),
        };

        println!("   📋 Account Details:");
        println!("      🆔 ID: {}", account_id);
        println!("      ⭐ Type: {}", account_type);
        println!("      💰 Balance: {}", balance);
        println!("      📈 Total transactions: 3 (1 deposit, 1 withdrawal, 1 fee)\n");

        Ok(())
    })?;

    // ========================================
    // 5. Check Canonical Metadata
    // ========================================
    println!("🔍 Step 5: Canonical Store Metadata");

    let meta = db.canonical().meta()?;
    println!("   📊 Store Statistics:");
    println!("      📝 Next event ID: {}", meta.next_event_id);
    println!(
        "      🔒 Sealed: {}",
        meta.sealed_event_id
            .map_or("No".to_string(), |id| format!("Yes (at {})", id))
    );
    println!("      📖 Schema version: {}", meta.schema_version);
    println!("      🕐 Created: {}", meta.created_at);
    println!("      🕐 Updated: {}\n", meta.updated_at);

    // ========================================
    // 6. Iterate Events (Audit Trail)
    // ========================================
    println!("📜 Step 6: Event Audit Trail");

    let mut iter = db.canonical().iter_events(0, None)?;
    let mut count = 0;

    println!("   📋 Event History:");
    while let Some((id, bytes)) = iter.next()? {
        count += 1;
        if let Ok(json) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            let event_type = json
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            println!("      #{} [{}]", id, event_type);
            if let Some(amount) = json.get("amount") {
                println!("         Amount: {}", amount);
            }
            if let Some(balance) = json.get("new_balance") {
                println!("         Balance: {}", balance);
            }
        } else {
            println!("      #{} [raw event: {} bytes]", id, bytes.len());
        }
    }

    println!("   ✅ Total events: {}\n", count);

    // ========================================
    // 7. Run Projector
    // ========================================
    println!("⚙️  Step 7: Running Projector");

    let stats = db.projector().run_once()?;
    println!("   ✅ Projector completed");
    println!("      📊 Events processed: {}", stats.events_applied);
    println!(
        "      💾 Bytes processed: {} KB",
        stats.bytes_processed / 1024
    );
    println!("      ⏱️  Duration: {:?}", stats.duration);
    println!("      📍 New cursor: {}\n", stats.new_cursor);

    // ========================================
    // 8. Check Projection State
    // ========================================
    println!("📈 Step 8: Projection Status");

    let cursor = db.projection().get_cursor()?;
    let lag = db.projector().get_lag()?;

    println!("   📊 Projection State:");
    println!("      📍 Cursor: {}", cursor);
    println!("      ⏳ Lag: {} events", lag);

    if lag == 0 {
        println!("      ✅ Fully caught up!");
    }
    println!();

    // ========================================
    // 9. Batch Operations Demo
    // ========================================
    println!("🔄 Step 9: Batch Operations");

    println!("   Creating 10 micro-transactions...");
    for i in 1..=10 {
        Transaction::new(&db).execute(|ctx| {
            let balance = ctx.get(b"balance")?.as_i64()?;
            let amount = i * 10;
            let new_balance = balance + amount;

            ctx.set(b"balance", &TypedValue::I64(new_balance))?;
            ctx.log(
                "micro_deposit",
                &serde_json::json!({
                    "amount": amount,
                    "sequence": i,
                }),
            )?;

            Ok(())
        })?;
    }
    println!("   ✅ Completed 10 micro-transactions (+550 total)\n");

    // Run projector to catch up
    let stats = db.projector().run_once()?;
    println!(
        "   ⚙️  Projector processed {} events in {:?}\n",
        stats.events_applied, stats.duration
    );

    // ========================================
    // 10. Final Summary
    // ========================================
    println!("📊 Step 10: Final Summary");

    Transaction::new(&db).execute(|ctx| {
        let final_balance = ctx.get(b"balance")?.as_i64()?;
        println!("\n   💰 Final Balance: {}", final_balance);
        println!("      Starting: 10,000");
        println!("      + Deposit: 5,000");
        println!("      - Withdrawal: 3,000");
        println!("      - Fee: 50");
        println!("      + Micro-deposits: 550");
        println!("      ─────────────────");
        println!("      = Final: {}", final_balance);

        Ok(())
    })?;

    let final_meta = db.canonical().meta()?;
    println!("\n   📈 Total Statistics:");
    println!("      📝 Total events: {}", final_meta.next_event_id);
    println!(
        "      💾 Store size: ~{} KB",
        std::fs::metadata(db_path.join("canonical"))
            .map(|m| m.len() / 1024)
            .unwrap_or(0)
    );

    println!("\n╔═══════════════════════════════════════╗");
    println!("║   Example Completed Successfully!    ║");
    println!("╚═══════════════════════════════════════╝\n");

    println!("🎓 Key Takeaways:");
    println!("   ✓ State and events are committed atomically");
    println!("   ✓ Events provide an immutable audit trail");
    println!("   ✓ Typed values ensure type safety");
    println!("   ✓ Projector processes events asynchronously");
    println!("   ✓ All operations are strongly typed and safe");
    println!("   ✓ File-based event log provides high performance\n");

    Ok(())
}
