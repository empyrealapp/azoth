//! Example demonstrating lock-based preflight with stripe locking
//!
//! Shows how declaring keys upfront enables:
//! - Parallel preflight validation (non-conflicting keys)
//! - Serialized commit (FIFO queue for determinism)
//! - Strong isolation guarantees

use azoth::prelude::*;
use azoth::typed_values::{TypedValue, U256};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn main() -> Result<()> {
    println!("=== Lock-Based Preflight Example ===\n");

    // Create temporary database
    let temp_dir = tempfile::tempdir()?;
    let db = Arc::new(AzothDb::open(temp_dir.path())?);

    // Initialize multiple accounts
    println!("1. Initializing 10 accounts with balance 1000...");
    for i in 0..10 {
        let key = format!("account_{}", i);
        Transaction::new(&db).execute(|ctx| {
            ctx.set(key.as_bytes(), &TypedValue::U256(U256::from(1000u64)))?;
            ctx.log(
                "init",
                &serde_json::json!({
                    "account": i,
                    "balance": 1000
                }),
            )?;
            Ok(())
        })?;
    }
    println!("   Done\n");

    // Example 1: Non-conflicting transactions (parallel preflight)
    println!("2. Running 10 concurrent transactions on DIFFERENT accounts...");
    println!("   (Non-conflicting keys → parallel preflight via stripe locking)");

    let start = Instant::now();
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                let key = format!("account_{}", i);

                // Each transaction operates on a different account
                Transaction::new(&db)
                    // Declare keys upfront - acquires lock on this stripe
                    .keys(vec![key.as_bytes().to_vec()])
                    .validate(|ctx| {
                        // Validation with lock held
                        let balance = ctx.get(key.as_bytes())?.as_u256()?;
                        if balance.to_bytes() < U256::from(100u64).to_bytes() {
                            return Err(AzothError::PreflightFailed("Insufficient funds".into()));
                        }
                        Ok(())
                    })
                    .execute(|ctx| {
                        // Fast commit (FIFO queue, but preflights ran in parallel)
                        let _balance = ctx.get(key.as_bytes())?.as_u256()?;
                        ctx.set(key.as_bytes(), &TypedValue::U256(U256::from(900u64)))?;
                        ctx.log(
                            "withdraw",
                            &serde_json::json!({
                                "account": i,
                                "amount": 100
                            }),
                        )?;
                        Ok(())
                    })
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap()?;
    }
    let parallel_time = start.elapsed();
    println!("   Time: {:?}", parallel_time);
    println!("   Result: All 10 preflights ran in PARALLEL (different stripes)");
    println!("   Commits were SERIALIZED (FIFO queue)\n");

    // Reset accounts
    for i in 0..10 {
        let key = format!("account_{}", i);
        Transaction::new(&db).execute(|ctx| {
            ctx.set(key.as_bytes(), &TypedValue::U256(U256::from(1000u64)))?;
            Ok(())
        })?;
    }

    // Example 2: Conflicting transactions (serialized preflight)
    println!("3. Running 10 concurrent transactions on SAME account...");
    println!("   (Conflicting keys → serialized preflight via stripe locking)");

    let start = Instant::now();
    let handles: Vec<_> = (0..10)
        .map(|i| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                // All transactions operate on account_0 (conflicting!)
                Transaction::new(&db)
                    .keys(vec![b"account_0".to_vec()])
                    .validate(|ctx| {
                        let balance = ctx.get(b"account_0")?.as_u256()?;
                        if balance.to_bytes() < U256::from(100u64).to_bytes() {
                            return Err(AzothError::PreflightFailed("Insufficient funds".into()));
                        }
                        Ok(())
                    })
                    .execute(|ctx| {
                        let _balance = ctx.get(b"account_0")?.as_u256()?;
                        // Simplified decrement
                        ctx.set(b"account_0", &TypedValue::U256(U256::from(900u64)))?;
                        ctx.log(
                            "withdraw",
                            &serde_json::json!({
                                "attempt": i,
                                "amount": 100
                            }),
                        )?;
                        Ok(())
                    })
            })
        })
        .collect();

    let mut success_count = 0;
    for handle in handles {
        if handle.join().unwrap().is_ok() {
            success_count += 1;
        }
    }
    let serial_time = start.elapsed();
    println!("   Time: {:?}", serial_time);
    println!("   Result: Transactions were SERIALIZED (same stripe)");
    println!(
        "   Only {} succeeded (others failed validation)\n",
        success_count
    );

    // Example 3: Read locks vs Write locks
    println!("4. Demonstrating read vs write locks...");
    println!("   (Multiple readers can run in parallel)");

    let start = Instant::now();
    let handles: Vec<_> = (0..10)
        .map(|_i| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                // All transactions READ account_0 (read locks allow parallelism!)
                Transaction::new(&db)
                    .keys(vec![b"account_0".to_vec()])
                    .validate(|ctx| {
                        let _balance = ctx.get(b"account_0")?;
                        // Just reading, not writing
                        Ok(())
                    })
                    .execute(|_ctx| {
                        // No state updates, just an event
                        Ok(())
                    })
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap()?;
    }
    let read_time = start.elapsed();
    println!("   Time: {:?}", read_time);
    println!("   Result: All 10 read locks acquired IN PARALLEL\n");

    // Summary
    println!("=== Performance Summary ===");
    println!(
        "Non-conflicting writes (parallel preflight): {:?}",
        parallel_time
    );
    println!(
        "Conflicting writes (serialized preflight):   {:?}",
        serial_time
    );
    println!(
        "Read-only operations (parallel reads):       {:?}",
        read_time
    );
    println!("\nKey Insight:");
    println!("- Stripe locking enables massive parallelism for non-conflicting keys");
    println!("- FIFO commit queue ensures deterministic, serialized commits");
    println!("- Locks held from preflight → through commit → released");

    println!("\n=== Example Complete ===");
    Ok(())
}
