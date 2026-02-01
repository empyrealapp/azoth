//! Convenience re-exports for common types.

pub use crate::config::SchedulerConfig;
pub use crate::error::{Result, SchedulerError};
pub use crate::event_applier::SchedulerEventApplier;
pub use crate::events::SchedulerEvent;
pub use crate::projection::{DueTask, ScheduleProjection, ScheduledTask, TaskFilter};
pub use crate::schedule::Schedule;
pub use crate::scheduler::{
    ScheduleTaskRequest, ScheduleTaskRequestBuilder, Scheduler, SchedulerBuilder,
};
pub use crate::task_handler::{TaskContext, TaskEvent, TaskHandler, TaskHandlerRegistry};
