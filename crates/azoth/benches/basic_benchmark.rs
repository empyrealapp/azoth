use azoth::prelude::*;
use azoth::{Transaction, TypedValue};
use std::time::Instant;

fn main() {
    println!("=== Azoth Performance Benchmark ===\n");

    let temp_dir = tempfile::tempdir().unwrap();
    let db = AzothDb::open(temp_dir.path()).unwrap();

    // Benchmark 1: Simple state writes
    println!("1. State Write Performance");
    let start = Instant::now();
    let count = 10_000;
    for i in 0..count {
        let mut txn = db.canonical().write_txn().unwrap();
        let key = format!("key_{}", i);
        txn.put_state(key.as_bytes(), b"value").unwrap();
        txn.commit().unwrap();
    }
    let duration = start.elapsed();
    let tps = count as f64 / duration.as_secs_f64();
    println!("   {} transactions in {:?}", count, duration);
    println!("   {:.2} tx/sec\n", tps);

    // Benchmark 2: Event appends
    println!("2. Event Append Performance");
    let start = Instant::now();
    let count = 10_000;
    for i in 0..count {
        let mut txn = db.canonical().write_txn().unwrap();
        let event = format!("event_{}", i);
        txn.append_event(event.as_bytes()).unwrap();
        txn.commit().unwrap();
    }
    let duration = start.elapsed();
    let tps = count as f64 / duration.as_secs_f64();
    println!("   {} events in {:?}", count, duration);
    println!("   {:.2} events/sec\n", tps);

    // Benchmark 3: Batch event appends
    println!("3. Batch Event Append Performance");
    let start = Instant::now();
    let batches = 1_000;
    let batch_size = 100;
    for i in 0..batches {
        let mut txn = db.canonical().write_txn().unwrap();
        let events: Vec<Vec<u8>> = (0..batch_size)
            .map(|j| format!("batch_{}_{}", i, j).into_bytes())
            .collect();
        txn.append_events(&events).unwrap();
        txn.commit().unwrap();
    }
    let total_events = batches * batch_size;
    let duration = start.elapsed();
    let tps = total_events as f64 / duration.as_secs_f64();
    println!(
        "   {} events ({} batches of {}) in {:?}",
        total_events, batches, batch_size, duration
    );
    println!("   {:.2} events/sec\n", tps);

    // Benchmark 4: State + Event atomically
    println!("4. Atomic State + Event Performance");
    let start = Instant::now();
    let count = 10_000;
    for i in 0..count {
        let mut txn = db.canonical().write_txn().unwrap();
        let key = format!("atomic_{}", i);
        txn.put_state(key.as_bytes(), b"value").unwrap();
        txn.append_event(key.as_bytes()).unwrap();
        txn.commit().unwrap();
    }
    let duration = start.elapsed();
    let tps = count as f64 / duration.as_secs_f64();
    println!("   {} transactions in {:?}", count, duration);
    println!("   {:.2} tx/sec\n", tps);

    // Benchmark 5: Transaction API with TypedValue
    println!("5. Transaction API with TypedValue");
    let start = Instant::now();
    let count = 10_000;
    for i in 0..count {
        Transaction::new(&db)
            .execute(|ctx| {
                ctx.set(b"counter", &TypedValue::I64(i))?;
                ctx.log_bytes(b"increment")?;
                Ok(())
            })
            .unwrap();
    }
    let duration = start.elapsed();
    let tps = count as f64 / duration.as_secs_f64();
    println!("   {} transactions in {:?}", count, duration);
    println!("   {:.2} tx/sec\n", tps);

    // Benchmark 6: State reads (using read-only transactions to avoid blocking writers)
    println!("6. State Read Performance");
    let start = Instant::now();
    let count = 10_000;
    for i in 0..count {
        let txn = db.canonical().read_txn().unwrap();
        let key = format!("key_{}", i % 100); // Read from first 100 keys
        let _ = txn.get_state(key.as_bytes()).unwrap();
    }
    let duration = start.elapsed();
    let rps = count as f64 / duration.as_secs_f64();
    println!("   {} reads in {:?}", count, duration);
    println!("   {:.2} reads/sec\n", rps);

    // Benchmark 7: Event iteration
    println!("7. Event Iteration Performance");
    let meta = db.canonical().meta().unwrap();
    let _event_count = meta.next_event_id;
    let start = Instant::now();
    let mut iter = db.canonical().iter_events(0, None).unwrap();
    let mut count = 0;
    while iter.next().unwrap().is_some() {
        count += 1;
    }
    let duration = start.elapsed();
    let eps = count as f64 / duration.as_secs_f64();
    println!("   Iterated {} events in {:?}", count, duration);
    println!("   {:.2} events/sec\n", eps);

    println!("=== Benchmark Complete ===");
}
