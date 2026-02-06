pub mod canonical;
pub mod projection;
pub mod projector;

pub use canonical::{CanonicalConfig, ReadPoolConfig, SyncMode};
pub use projection::{ProjectionConfig, SynchronousMode};
pub use projector::ProjectorConfig;
