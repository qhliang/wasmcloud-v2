//! Worker traits and shared task execution context.

//! Task execution is cooperative: workers call `send_heartbeat` to publish
//! progress and `is_cancelled` at checkpoints. The JetStream lease renewal is
//! handled separately by a runner and must not be coupled to heartbeat success.

use async_trait::async_trait;

use crate::events::{EventProducer, HeartbeatEvent};
use crate::types::{Task, TaskError, TaskId, TaskOutput};

#[async_trait]
pub trait HeartbeatSink: Send + Sync {
    /// Publishes a producer-visible progress payload on core NATS.
    ///
    /// A failed heartbeat does not extend or invalidate the JetStream lease.
    async fn send_heartbeat(&self, info: String) -> Result<(), TaskError>;
}

#[async_trait]
pub trait CancellationSource: Send + Sync {
    /// Returns whether the local runner has asked the current task to stop.
    ///
    /// Implementations should call this at checkpoints before starting new
    /// side effects; it does not preempt long-running synchronous work.
    async fn is_cancelled(&self) -> Result<bool, TaskError>;
}

#[derive(Clone)]
pub struct TaskContext {
    /// Stable task identifier from the JetStream subject.
    pub task_id: TaskId,
    /// Current delivery attempt, starting at 1.
    pub attempt: u32,
    /// Unix-millisecond deadline encoded in the task envelope.
    pub execution_deadline_ms: u64,
    /// Decoded business payload supplied by the producer.
    pub payload: Vec<u8>,
    heartbeat: async_nats::Client,
    subject: String,
    cancellation: tokio_util::sync::CancellationToken,
}

impl TaskContext {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        task_id: impl Into<String>,
        attempt: u32,
        execution_deadline_ms: u64,
        task: Task,
        heartbeat: async_nats::Client,
        subject: String,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Self {
        Self {
            task_id: task_id.into(),
            attempt,
            execution_deadline_ms,
            payload: task.payload,
            heartbeat,
            subject,
            cancellation,
        }
    }

    pub fn task(&self) -> Task {
        Task {
            payload: self.payload.clone(),
        }
    }
}

#[async_trait]
impl HeartbeatSink for TaskContext {
    async fn send_heartbeat(&self, info: String) -> Result<(), TaskError> {
        // Keep the control event small enough for core NATS and observers.
        if info.len() > crate::config::HEARTBEAT_MAX_INFO_BYTES {
            return Err(TaskError::guest("heartbeat exceeds maximum size"));
        }
        let event = HeartbeatEvent {
            task_id: self.task_id.clone(),
            attempt: self.attempt,
            timestamp_ms: crate::nats::now_ms(),
            info,
            producer: None,
        };
        let raw = serde_json::to_vec(&event)
            .map_err(|err| TaskError::system(format!("failed to encode heartbeat: {err}")))?;
        self.heartbeat
            .publish(self.subject.clone(), raw.into())
            .await
            .map_err(|err| TaskError::system(format!("failed to publish heartbeat: {err}")))
    }
}

#[async_trait]
impl CancellationSource for TaskContext {
    async fn is_cancelled(&self) -> Result<bool, TaskError> {
        // Cross-process cancellation through META KV is a future extension.
        Ok(self.cancellation.is_cancelled())
    }
}

#[async_trait]
pub trait Worker: Send + Sync {
    /// Executes one delivery and returns a terminal output or typed failure.
    ///
    /// `Ok(Some(output))` means success. A guest error is retried by the
    /// queue until `max_deliver`; a system error terminates the delivery.
    async fn handle_task(&self, task: TaskContext) -> Result<TaskOutput, TaskError>;
}

#[async_trait]
pub trait Observer: Send + Sync {
    async fn on_heartbeat(&self, event: HeartbeatEvent) -> Result<(), TaskError>;
}

pub type SharedWorker = std::sync::Arc<dyn Worker>;

pub fn producer_identity(
    namespace: impl Into<String>,
    workload: impl Into<String>,
    component: impl Into<String>,
) -> EventProducer {
    EventProducer {
        namespace: namespace.into(),
        workload: workload.into(),
        component: component.into(),
    }
}
