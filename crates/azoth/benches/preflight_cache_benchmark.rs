//! Benchmark to measure preflight cache performance impact

use azoth::{AzothDb, Transaction, TypedValue};
use azoth_core::{CanonicalConfig, ProjectionConfig};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

fn create_db_with_cache(path: &std::path::Path, cache_enabled: bool) -> AzothDb {
    let canonical_config = CanonicalConfig::new(path.join("canonical"))
        .with_preflight_cache(cache_enabled)
        .with_preflight_cache_size(10_000)
        .with_preflight_cache_ttl(60);
    let projection_config = ProjectionConfig::new(path.join("projection.db"));
    AzothDb::open_with_config(path.to_path_buf(), canonical_config, projection_config).unwrap()
}

fn benchmark_hot_key_reads(db: &AzothDb, iterations: usize) -> std::time::Duration {
    // Initialize a hot key
    Transaction::new(db)
        .execute(|ctx| {
            ctx.set(b"hot_key", &TypedValue::I64(1000))?;
            Ok(())
        })
        .unwrap();

    // Benchmark reading the same key repeatedly during preflight
    let start = Instant::now();
    for i in 0..iterations {
        Transaction::new(db)
            .keys(vec![b"hot_key".to_vec()])
            .preflight(|ctx| {
                let _value = ctx.get(b"hot_key")?;
                Ok(())
            })
            .execute(|ctx| {
                // Write a dummy key to make it a real transaction
                let key = format!("counter_{}", i);
                ctx.set(key.as_bytes(), &TypedValue::I64(i as i64))?;
                Ok(())
            })
            .unwrap();
    }
    start.elapsed()
}

fn benchmark_mixed_workload(db: &AzothDb, iterations: usize) -> std::time::Duration {
    // Initialize some keys
    Transaction::new(db)
        .execute(|ctx| {
            for i in 0..10 {
                let key = format!("mixed_key_{}", i);
                ctx.set(key.as_bytes(), &TypedValue::I64(i))?;
            }
            Ok(())
        })
        .unwrap();

    // Benchmark mixed reads (some hot, some cold)
    let start = Instant::now();
    for i in 0..iterations {
        let key_idx = i % 10; // Read keys in round-robin fashion
        let key = format!("mixed_key_{}", key_idx);

        Transaction::new(db)
            .keys(vec![key.as_bytes().to_vec()])
            .preflight(|ctx| {
                let _value = ctx.get(key.as_bytes())?;
                Ok(())
            })
            .execute(|ctx| {
                let counter_key = format!("counter_{}", i);
                ctx.set(counter_key.as_bytes(), &TypedValue::I64(i as i64))?;
                Ok(())
            })
            .unwrap();
    }
    start.elapsed()
}

fn benchmark_cold_keys(db: &AzothDb, iterations: usize) -> std::time::Duration {
    // Benchmark reading different keys each time (all cache misses)
    let start = Instant::now();
    for i in 0..iterations {
        let key = format!("unique_key_{}", i);

        Transaction::new(db)
            .keys(vec![key.as_bytes().to_vec()])
            .preflight(|ctx| {
                let _exists = ctx.exists(key.as_bytes())?;
                Ok(())
            })
            .execute(|ctx| {
                ctx.set(key.as_bytes(), &TypedValue::I64(i as i64))?;
                Ok(())
            })
            .unwrap();
    }
    start.elapsed()
}

fn benchmark_concurrent_hot_keys(
    db: Arc<AzothDb>,
    num_threads: usize,
    iterations_per_thread: usize,
) -> std::time::Duration {
    // Initialize hot keys
    Transaction::new(&db)
        .execute(|ctx| {
            for i in 0..num_threads {
                let key = format!("thread_key_{}", i);
                ctx.set(key.as_bytes(), &TypedValue::I64(i as i64))?;
            }
            Ok(())
        })
        .unwrap();

    // Benchmark concurrent reads
    let start = Instant::now();
    let mut handles = vec![];

    for thread_id in 0..num_threads {
        let db_clone = Arc::clone(&db);
        let handle = thread::spawn(move || {
            let key = format!("thread_key_{}", thread_id);
            for i in 0..iterations_per_thread {
                Transaction::new(&db_clone)
                    .keys(vec![key.as_bytes().to_vec()])
                    .preflight(|ctx| {
                        let _value = ctx.get(key.as_bytes())?;
                        Ok(())
                    })
                    .execute(|ctx| {
                        let counter_key = format!("counter_{}_{}", thread_id, i);
                        ctx.set(counter_key.as_bytes(), &TypedValue::I64(i as i64))?;
                        Ok(())
                    })
                    .unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    start.elapsed()
}

fn main() {
    println!("=== Preflight Cache Performance Benchmark ===\n");

    let iterations = 1_000;

    // Benchmark 1: Hot key reads (cache enabled)
    println!("1. Hot Key Reads (Cache Enabled)");
    let temp_dir_enabled = tempfile::tempdir().unwrap();
    let db_enabled = create_db_with_cache(temp_dir_enabled.path(), true);
    let duration_enabled = benchmark_hot_key_reads(&db_enabled, iterations);
    let tps_enabled = iterations as f64 / duration_enabled.as_secs_f64();
    println!("   {} reads in {:?}", iterations, duration_enabled);
    println!("   {:.2} reads/sec", tps_enabled);
    println!(
        "   Cache stats: {:?}\n",
        db_enabled.canonical().preflight_cache().stats()
    );

    // Benchmark 2: Hot key reads (cache disabled)
    println!("2. Hot Key Reads (Cache Disabled)");
    let temp_dir_disabled = tempfile::tempdir().unwrap();
    let db_disabled = create_db_with_cache(temp_dir_disabled.path(), false);
    let duration_disabled = benchmark_hot_key_reads(&db_disabled, iterations);
    let tps_disabled = iterations as f64 / duration_disabled.as_secs_f64();
    println!("   {} reads in {:?}", iterations, duration_disabled);
    println!("   {:.2} reads/sec", tps_disabled);
    println!(
        "   Cache stats: {:?}\n",
        db_disabled.canonical().preflight_cache().stats()
    );

    // Calculate improvement
    let improvement = ((tps_enabled - tps_disabled) / tps_disabled) * 100.0;
    println!(
        "   → Cache improvement: {:.1}% ({:.2}x faster)\n",
        improvement,
        tps_enabled / tps_disabled
    );

    // Benchmark 3: Mixed workload (cache enabled)
    println!("3. Mixed Workload (Cache Enabled)");
    let temp_dir_mixed_enabled = tempfile::tempdir().unwrap();
    let db_mixed_enabled = create_db_with_cache(temp_dir_mixed_enabled.path(), true);
    let duration_mixed_enabled = benchmark_mixed_workload(&db_mixed_enabled, iterations);
    let tps_mixed_enabled = iterations as f64 / duration_mixed_enabled.as_secs_f64();
    println!("   {} reads in {:?}", iterations, duration_mixed_enabled);
    println!("   {:.2} reads/sec", tps_mixed_enabled);
    println!(
        "   Cache stats: {:?}\n",
        db_mixed_enabled.canonical().preflight_cache().stats()
    );

    // Benchmark 4: Mixed workload (cache disabled)
    println!("4. Mixed Workload (Cache Disabled)");
    let temp_dir_mixed_disabled = tempfile::tempdir().unwrap();
    let db_mixed_disabled = create_db_with_cache(temp_dir_mixed_disabled.path(), false);
    let duration_mixed_disabled = benchmark_mixed_workload(&db_mixed_disabled, iterations);
    let tps_mixed_disabled = iterations as f64 / duration_mixed_disabled.as_secs_f64();
    println!("   {} reads in {:?}", iterations, duration_mixed_disabled);
    println!("   {:.2} reads/sec", tps_mixed_disabled);
    println!(
        "   Cache stats: {:?}\n",
        db_mixed_disabled.canonical().preflight_cache().stats()
    );

    let improvement_mixed = ((tps_mixed_enabled - tps_mixed_disabled) / tps_mixed_disabled) * 100.0;
    println!(
        "   → Cache improvement: {:.1}% ({:.2}x faster)\n",
        improvement_mixed,
        tps_mixed_enabled / tps_mixed_disabled
    );

    // Benchmark 5: Cold keys (cache enabled)
    println!("5. Cold Keys - All Misses (Cache Enabled)");
    let temp_dir_cold_enabled = tempfile::tempdir().unwrap();
    let db_cold_enabled = create_db_with_cache(temp_dir_cold_enabled.path(), true);
    let duration_cold_enabled = benchmark_cold_keys(&db_cold_enabled, iterations);
    let tps_cold_enabled = iterations as f64 / duration_cold_enabled.as_secs_f64();
    println!("   {} reads in {:?}", iterations, duration_cold_enabled);
    println!("   {:.2} reads/sec", tps_cold_enabled);
    println!(
        "   Cache stats: {:?}\n",
        db_cold_enabled.canonical().preflight_cache().stats()
    );

    // Benchmark 6: Cold keys (cache disabled)
    println!("6. Cold Keys - All Misses (Cache Disabled)");
    let temp_dir_cold_disabled = tempfile::tempdir().unwrap();
    let db_cold_disabled = create_db_with_cache(temp_dir_cold_disabled.path(), false);
    let duration_cold_disabled = benchmark_cold_keys(&db_cold_disabled, iterations);
    let tps_cold_disabled = iterations as f64 / duration_cold_disabled.as_secs_f64();
    println!("   {} reads in {:?}", iterations, duration_cold_disabled);
    println!("   {:.2} reads/sec", tps_cold_disabled);
    println!(
        "   Cache stats: {:?}\n",
        db_cold_disabled.canonical().preflight_cache().stats()
    );

    let overhead = ((tps_cold_disabled - tps_cold_enabled) / tps_cold_disabled) * 100.0;
    println!(
        "   → Cache overhead for misses: {:.1}% ({:.2}x)\n",
        overhead,
        tps_cold_disabled / tps_cold_enabled
    );

    // Benchmark 7: Concurrent hot key reads (cache enabled)
    println!("7. Concurrent Hot Key Reads (Cache Enabled)");
    let num_threads = 8;
    let iterations_per_thread = 100;
    let temp_dir_concurrent_enabled = tempfile::tempdir().unwrap();
    let db_concurrent_enabled = Arc::new(create_db_with_cache(
        temp_dir_concurrent_enabled.path(),
        true,
    ));
    let duration_concurrent_enabled = benchmark_concurrent_hot_keys(
        db_concurrent_enabled.clone(),
        num_threads,
        iterations_per_thread,
    );
    let total_ops = num_threads * iterations_per_thread;
    let tps_concurrent_enabled = total_ops as f64 / duration_concurrent_enabled.as_secs_f64();
    println!(
        "   {} reads ({} threads × {} iterations) in {:?}",
        total_ops, num_threads, iterations_per_thread, duration_concurrent_enabled
    );
    println!("   {:.2} reads/sec", tps_concurrent_enabled);
    println!(
        "   Cache stats: {:?}\n",
        db_concurrent_enabled.canonical().preflight_cache().stats()
    );

    // Benchmark 8: Concurrent hot key reads (cache disabled)
    println!("8. Concurrent Hot Key Reads (Cache Disabled)");
    let temp_dir_concurrent_disabled = tempfile::tempdir().unwrap();
    let db_concurrent_disabled = Arc::new(create_db_with_cache(
        temp_dir_concurrent_disabled.path(),
        false,
    ));
    let duration_concurrent_disabled = benchmark_concurrent_hot_keys(
        db_concurrent_disabled.clone(),
        num_threads,
        iterations_per_thread,
    );
    let tps_concurrent_disabled = total_ops as f64 / duration_concurrent_disabled.as_secs_f64();
    println!(
        "   {} reads ({} threads × {} iterations) in {:?}",
        total_ops, num_threads, iterations_per_thread, duration_concurrent_disabled
    );
    println!("   {:.2} reads/sec", tps_concurrent_disabled);
    println!(
        "   Cache stats: {:?}\n",
        db_concurrent_disabled.canonical().preflight_cache().stats()
    );

    let improvement_concurrent =
        ((tps_concurrent_enabled - tps_concurrent_disabled) / tps_concurrent_disabled) * 100.0;
    println!(
        "   → Cache improvement: {:.1}% ({:.2}x faster)\n",
        improvement_concurrent,
        tps_concurrent_enabled / tps_concurrent_disabled
    );

    println!("=== Summary ===");
    println!("Hot key improvement: {:.1}%", improvement);
    println!("Mixed workload improvement: {:.1}%", improvement_mixed);
    println!("Cold key overhead: {:.1}%", overhead);
    println!("Concurrent improvement: {:.1}%", improvement_concurrent);
}
