//! Shared task-queue protocol and JetStream runtime.

pub mod config;
pub mod events;
pub mod nats;
pub mod queue;
pub mod types;
pub mod worker;

pub use config::QueueConfig;
pub use types::{
    AttemptErrorSource, AttemptFailure, AttemptFailureRecord, Task, TaskEnvelope, TaskError,
    TaskErrorSource, TaskId, TaskInfo, TaskMeta, TaskOutput, TaskResult, TaskState, TaskStatus,
};
