//! # Azoth Bus
//!
//! Multi-consumer pub/sub capabilities built on top of Azoth's primitives.
//!
//! The bus provides:
//! - Named consumers with independent cursors
//! - Event filtering (by type prefix or custom filters)
//! - Consumer lag monitoring
//! - Stream-based organization
//!
//! ## Example
//!
//! ```rust,no_run
//! use azoth::AzothDb;
//! use azoth_bus::EventBus;
//! use std::sync::Arc;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let db = AzothDb::open("./data")?;
//! let bus = EventBus::new(Arc::new(db));
//!
//! // Subscribe to a stream
//! let mut consumer = bus.subscribe("knowledge", "my-consumer")?;
//!
//! // Read events (automatically filtered to "knowledge:*")
//! while let Some(event) = consumer.next()? {
//!     println!("Processing event: {}", event.event_type);
//!     consumer.ack(event.id)?;
//! }
//!
//! // Check consumer status
//! let info = bus.consumer_info("knowledge", "my-consumer")?;
//! println!("Consumer lag: {}", info.lag);
//! # Ok(())
//! # }
//! ```

pub mod bus;
pub mod config;
pub mod consumer;
pub mod consumer_group;
pub mod error;
pub mod filter;
pub mod notification;
pub mod retention;

pub use bus::{ConsumerInfo, EventBus};
pub use consumer::Consumer;
pub use consumer_group::{ClaimedEvent, ConsumerGroup, GroupMember};
pub use error::{BusError, Result};
pub use filter::EventFilter;
pub use notification::WakeStrategy;
pub use retention::{CompactionStats, RetentionManager, RetentionPolicy};
