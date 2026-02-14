//! Optional metrics instrumentation for Azoth.
//!
//! When the `observe` feature is enabled, key operations emit counters,
//! histograms, and gauges via the [`metrics`] crate. A downstream
//! application must install a metrics recorder (e.g. `metrics-exporter-prometheus`)
//! to collect the data.
//!
//! When the feature is **not** enabled every function in this module is a
//! zero-cost no-op.

/// Record a transaction commit (counter + latency histogram).
///
/// - `azoth.transaction.commits_total` – incremented on every commit
/// - `azoth.transaction.commit_duration_seconds` – histogram of commit latency
#[inline]
pub fn record_commit(duration: std::time::Duration) {
    #[cfg(feature = "observe")]
    {
        metrics::counter!("azoth.transaction.commits_total").increment(1);
        metrics::histogram!("azoth.transaction.commit_duration_seconds")
            .record(duration.as_secs_f64());
    }
    #[cfg(not(feature = "observe"))]
    {
        let _ = duration;
    }
}

/// Record a preflight validation (counter + duration).
///
/// - `azoth.preflight.total` – counter
/// - `azoth.preflight.duration_seconds` – histogram
#[inline]
pub fn record_preflight(duration: std::time::Duration, success: bool) {
    #[cfg(feature = "observe")]
    {
        let outcome = if success { "ok" } else { "fail" };
        metrics::counter!("azoth.preflight.total", "outcome" => outcome).increment(1);
        metrics::histogram!("azoth.preflight.duration_seconds").record(duration.as_secs_f64());
    }
    #[cfg(not(feature = "observe"))]
    {
        let _ = (duration, success);
    }
}

/// Record a preflight cache hit or miss.
///
/// - `azoth.preflight_cache.lookups_total` – counter with `result` label (`hit` / `miss`)
#[inline]
pub fn record_cache_lookup(hit: bool) {
    #[cfg(feature = "observe")]
    {
        let result = if hit { "hit" } else { "miss" };
        metrics::counter!("azoth.preflight_cache.lookups_total", "result" => result).increment(1);
    }
    #[cfg(not(feature = "observe"))]
    {
        let _ = hit;
    }
}

/// Set the current preflight cache size gauge.
///
/// - `azoth.preflight_cache.size` – gauge
#[inline]
pub fn set_cache_size(size: usize) {
    #[cfg(feature = "observe")]
    {
        metrics::gauge!("azoth.preflight_cache.size").set(size as f64);
    }
    #[cfg(not(feature = "observe"))]
    {
        let _ = size;
    }
}

/// Record a projector run (counter + duration + events processed).
///
/// - `azoth.projector.runs_total` – counter
/// - `azoth.projector.run_duration_seconds` – histogram
/// - `azoth.projector.events_processed_total` – counter
#[inline]
pub fn record_projector_run(duration: std::time::Duration, events_processed: u64) {
    #[cfg(feature = "observe")]
    {
        metrics::counter!("azoth.projector.runs_total").increment(1);
        metrics::histogram!("azoth.projector.run_duration_seconds").record(duration.as_secs_f64());
        metrics::counter!("azoth.projector.events_processed_total").increment(events_processed);
    }
    #[cfg(not(feature = "observe"))]
    {
        let _ = (duration, events_processed);
    }
}

/// Record a lock acquisition wait time.
///
/// - `azoth.lock.wait_duration_seconds` – histogram
#[inline]
pub fn record_lock_wait(duration: std::time::Duration) {
    #[cfg(feature = "observe")]
    {
        metrics::histogram!("azoth.lock.wait_duration_seconds").record(duration.as_secs_f64());
    }
    #[cfg(not(feature = "observe"))]
    {
        let _ = duration;
    }
}

/// Record a backup operation.
///
/// - `azoth.backup.total` – counter with `outcome` label
/// - `azoth.backup.duration_seconds` – histogram
#[inline]
pub fn record_backup(duration: std::time::Duration, success: bool) {
    #[cfg(feature = "observe")]
    {
        let outcome = if success { "ok" } else { "fail" };
        metrics::counter!("azoth.backup.total", "outcome" => outcome).increment(1);
        metrics::histogram!("azoth.backup.duration_seconds").record(duration.as_secs_f64());
    }
    #[cfg(not(feature = "observe"))]
    {
        let _ = (duration, success);
    }
}
