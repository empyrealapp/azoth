//! Example demonstrating lambda-based require statements with typed values
//!
//! Shows how to use arbitrary validation logic in preflight phase

use azoth::prelude::*;
use azoth::TypedValue;

fn main() -> Result<()> {
    // Create temporary database
    let temp_dir = tempfile::tempdir()?;
    let db = AzothDb::open(temp_dir.path())?;

    println!("=== Lambda-based Require Example ===\n");

    // Initialize account balance
    println!("1. Initializing account balance...");
    let initial_balance = 1000i64;
    Transaction::new(&db).execute(|ctx| {
        ctx.set(b"balance", &TypedValue::I64(initial_balance))?;
        ctx.log(
            "deposit",
            &serde_json::json!({
                "amount": 1000,
                "type": "initial"
            }),
        )?;
        Ok(())
    })?;
    println!("   Balance initialized: {}", 1000);

    // Example 1: Lambda with simple existence check
    println!("\n2. Withdraw with lambda-based validation...");
    let withdraw_amount = 250i64;
    let result = Transaction::new(&db)
        .require(b"balance".to_vec(), |value| {
            // Custom validation logic
            let typed_value =
                value.ok_or(AzothError::PreflightFailed("Balance must exist".into()))?;
            let balance = typed_value.as_i64()?;

            println!("   [Preflight] Current balance: {}", balance);

            // Custom business logic
            if balance < withdraw_amount {
                return Err(AzothError::PreflightFailed("Insufficient balance".into()));
            }

            println!("   [Preflight] Validation passed");
            Ok(())
        })
        .execute(|ctx| {
            let current = ctx.get(b"balance")?.as_i64()?;
            let new_balance = current - withdraw_amount;
            ctx.set(b"balance", &TypedValue::I64(new_balance))?;
            ctx.log(
                "withdraw",
                &serde_json::json!({
                    "amount": withdraw_amount
                }),
            )?;
            println!("   [Execute] Withdraw successful");
            Ok(())
        })?;

    println!(
        "   Transaction committed with {} events",
        result.events_written
    );

    // Example 2: Lambda with complex validation
    println!("\n3. Transfer with multiple validations...");
    let result = Transaction::new(&db)
        .require(b"balance".to_vec(), |value| {
            let typed_value = value.ok_or(AzothError::PreflightFailed("No balance".into()))?;
            let balance = typed_value.as_i64()?;

            // Multiple conditions
            if balance == 0 {
                return Err(AzothError::PreflightFailed("Balance is zero".into()));
            }

            println!("   [Preflight] Balance check passed (balance: {})", balance);
            Ok(())
        })
        .require(b"transfer_limit".to_vec(), |value| {
            // Optional key - only validate if exists
            if let Some(typed_value) = value {
                let limit = typed_value.as_i64()?;
                println!("   [Preflight] Transfer limit: {}", limit);
            } else {
                println!("   [Preflight] No transfer limit set");
            }
            Ok(())
        })
        .execute(|ctx| {
            // Perform transfer
            ctx.log(
                "transfer",
                &serde_json::json!({
                    "from": "account1",
                    "to": "account2",
                    "amount": 100
                }),
            )?;
            println!("   [Execute] Transfer complete");
            Ok(())
        })?;

    println!(
        "   Transaction committed with {} events",
        result.events_written
    );

    // Example 3: Failed validation
    println!("\n4. Attempting withdraw with insufficient balance...");
    let result = Transaction::new(&db)
        .require(b"balance".to_vec(), |value| {
            let typed_value = value.ok_or(AzothError::PreflightFailed("No balance".into()))?;
            let balance = typed_value.as_i64()?;

            // This will fail
            let required = 10000i64;
            if balance < required {
                return Err(AzothError::PreflightFailed(format!(
                    "Insufficient balance for large withdrawal: {} < {}",
                    balance, required
                )));
            }
            Ok(())
        })
        .execute(|ctx| {
            ctx.log(
                "withdraw",
                &serde_json::json!({
                    "amount": 10000
                }),
            )?;
            Ok(())
        });

    match result {
        Ok(_) => println!("   Unexpected success!"),
        Err(e) => println!("   Expected failure: {}", e),
    }

    println!("\n=== Example Complete ===");
    Ok(())
}
