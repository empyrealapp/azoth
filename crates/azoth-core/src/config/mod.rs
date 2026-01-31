pub mod canonical;
pub mod projection;
pub mod projector;

pub use canonical::{CanonicalConfig, SyncMode};
pub use projection::{ProjectionConfig, SynchronousMode};
pub use projector::ProjectorConfig;
