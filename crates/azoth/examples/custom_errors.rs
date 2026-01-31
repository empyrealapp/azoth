//! Custom Error Types Example
//!
//! Demonstrates:
//! - Defining custom error types for business logic
//! - Converting between custom errors and AzothError
//! - Using custom errors in transactions
//! - Rich error context with custom error variants
//!
//! Run with: cargo run --example custom_errors

use azoth::prelude::*;
use azoth::TypedValue;
use thiserror::Error;

/// Custom application error type with business logic variants
#[derive(Error, Debug)]
pub enum BankError {
    #[error("Insufficient funds: account has {balance}, need {amount}")]
    InsufficientFunds { balance: i64, amount: i64 },

    #[error("Account {0} not found")]
    AccountNotFound(u64),

    #[error("Daily withdrawal limit exceeded: limit={limit}, attempted={attempted}")]
    WithdrawalLimitExceeded { limit: i64, attempted: i64 },

    #[error("Account {0} is frozen")]
    AccountFrozen(u64),

    #[error("Invalid transaction: {0}")]
    InvalidTransaction(String),

    #[error(transparent)]
    Azoth(#[from] AzothError),
}

/// Convert BankError to AzothError for compatibility with Azoth internals
impl From<BankError> for AzothError {
    fn from(err: BankError) -> Self {
        match err {
            BankError::Azoth(e) => e,
            BankError::InsufficientFunds { balance, amount } => {
                AzothError::PreflightFailed(format!(
                    "Insufficient funds: account has {}, need {}",
                    balance, amount
                ))
            }
            BankError::AccountNotFound(id) => {
                AzothError::PreflightFailed(format!("Account {} not found", id))
            }
            BankError::WithdrawalLimitExceeded { limit, attempted } => {
                AzothError::PreflightFailed(format!(
                    "Daily withdrawal limit exceeded: limit={}, attempted={}",
                    limit, attempted
                ))
            }
            BankError::AccountFrozen(id) => {
                AzothError::PreflightFailed(format!("Account {} is frozen", id))
            }
            BankError::InvalidTransaction(msg) => AzothError::PreflightFailed(msg),
        }
    }
}

type BankResult<T> = std::result::Result<T, BankError>;

/// Business logic: Validate withdrawal
fn validate_withdrawal(
    ctx: &azoth::transaction::PreflightContext,
    account_id: u64,
    amount: i64,
    daily_limit: i64,
) -> BankResult<()> {
    let key = format!("account:{}", account_id);

    // Check account exists
    if !ctx.exists(key.as_bytes())? {
        return Err(BankError::AccountNotFound(account_id));
    }

    // Check frozen status
    let frozen_key = format!("account:{}:frozen", account_id);
    if ctx.exists(frozen_key.as_bytes())? {
        return Err(BankError::AccountFrozen(account_id));
    }

    // Check balance
    let balance = ctx.get(key.as_bytes())?.as_i64()?;
    if balance < amount {
        return Err(BankError::InsufficientFunds { balance, amount });
    }

    // Check daily limit
    if amount > daily_limit {
        return Err(BankError::WithdrawalLimitExceeded {
            limit: daily_limit,
            attempted: amount,
        });
    }

    Ok(())
}

fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("azoth=info")
        .init();

    println!("=== Custom Error Types Example ===\n");

    let temp_dir = tempfile::tempdir()?;
    let db = AzothDb::open(temp_dir.path())?;

    // ========================================
    // Setup: Create accounts
    // ========================================
    println!("1. Setting up accounts...");

    Transaction::new(&db).execute(|ctx| {
        ctx.set(b"account:1", &TypedValue::I64(1000))?;
        ctx.set(b"account:2", &TypedValue::I64(500))?;
        ctx.set(b"account:3:frozen", &TypedValue::I64(1))?; // Account 3 is frozen
        Ok(())
    })?;

    println!("   ✓ Account 1: balance = 1000");
    println!("   ✓ Account 2: balance = 500");
    println!("   ✓ Account 3: frozen\n");

    // ========================================
    // Test 1: Successful withdrawal
    // ========================================
    println!("2. Test: Successful withdrawal");

    let result = Transaction::new(&db)
        .preflight(|ctx| {
            validate_withdrawal(ctx, 1, 200, 500).map_err(|e| e.into()) // Convert BankError to AzothError
        })
        .execute(|ctx| {
            ctx.update(b"account:1", |old| {
                let balance = old.unwrap().as_i64()?;
                Ok(TypedValue::I64(balance - 200))
            })?;
            ctx.log_bytes(b"withdraw:1:200")?;
            Ok(())
        });

    match result {
        Ok(_) => println!("   ✓ Withdrawal succeeded\n"),
        Err(e) => println!("   ✗ Unexpected error: {}\n", e),
    }

    // ========================================
    // Test 2: Insufficient funds
    // ========================================
    println!("3. Test: Insufficient funds");

    let result = Transaction::new(&db)
        .preflight(|ctx| validate_withdrawal(ctx, 2, 1000, 5000).map_err(|e| e.into()))
        .execute(|ctx| {
            ctx.update(b"account:2", |old| {
                let balance = old.unwrap().as_i64()?;
                Ok(TypedValue::I64(balance - 1000))
            })?;
            Ok(())
        });

    match result {
        Ok(_) => println!("   ✗ Should have failed"),
        Err(e) => println!("   ✓ Correctly rejected: {}\n", e),
    }

    // ========================================
    // Test 3: Daily limit exceeded
    // ========================================
    println!("4. Test: Daily withdrawal limit");

    let result = Transaction::new(&db)
        .preflight(|ctx| validate_withdrawal(ctx, 1, 600, 500).map_err(|e| e.into()))
        .execute(|ctx| {
            ctx.update(b"account:1", |old| {
                let balance = old.unwrap().as_i64()?;
                Ok(TypedValue::I64(balance - 600))
            })?;
            Ok(())
        });

    match result {
        Ok(_) => println!("   ✗ Should have failed"),
        Err(e) => println!("   ✓ Correctly rejected: {}\n", e),
    }

    // ========================================
    // Test 4: Account not found
    // ========================================
    println!("5. Test: Account not found");

    let result = Transaction::new(&db)
        .preflight(|ctx| validate_withdrawal(ctx, 999, 100, 500).map_err(|e| e.into()))
        .execute(|ctx| {
            ctx.log_bytes(b"withdraw:999:100")?;
            Ok(())
        });

    match result {
        Ok(_) => println!("   ✗ Should have failed"),
        Err(e) => println!("   ✓ Correctly rejected: {}\n", e),
    }

    // ========================================
    // Test 5: Frozen account
    // ========================================
    println!("6. Test: Frozen account");

    let result = Transaction::new(&db)
        .preflight(|ctx| validate_withdrawal(ctx, 3, 100, 500).map_err(|e| e.into()))
        .execute(|ctx| {
            ctx.log_bytes(b"withdraw:3:100")?;
            Ok(())
        });

    match result {
        Ok(_) => println!("   ✗ Should have failed"),
        Err(e) => println!("   ✓ Correctly rejected: {}\n", e),
    }

    // ========================================
    // Final balances
    // ========================================
    println!("7. Final balances:");

    Transaction::new(&db).execute(|ctx| {
        let balance1 = ctx.get(b"account:1")?.as_i64()?;
        let balance2 = ctx.get(b"account:2")?.as_i64()?;
        println!("   Account 1: {} (1000 - 200 = 800)", balance1);
        println!("   Account 2: {} (unchanged)", balance2);
        Ok(())
    })?;

    println!("\n=== Example Complete ===");
    println!("\n💡 Key Points:");
    println!("   ✓ Custom error types provide rich business logic context");
    println!("   ✓ Convert custom errors to AzothError via From trait");
    println!("   ✓ Preflight validation can return domain-specific errors");
    println!("   ✓ Error messages include detailed information for debugging");
    println!("   ✓ Type-safe error handling with pattern matching\n");

    Ok(())
}
