//! Projector: Event processing loop
//!
//! Consumes events from the canonical store and applies them to projections.
//!
//! Key features:
//! - Batching (events, bytes, latency limits)
//! - Graceful shutdown
//! - Lag monitoring
//! - Seal-aware processing

pub mod projector;

pub use projector::{Projector, ProjectorStats};
