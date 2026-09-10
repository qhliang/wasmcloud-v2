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
    /// Set once the terminal `complete` event has been published. Shared
    /// across clones so a worker that spawns the context into background
    /// tasks still deduplicates against the runner's ack-path fallback.
    terminate_published: std::sync::Arc<std::sync::atomic::AtomicBool>,
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
            terminate_published: std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    pub fn task(&self) -> Task {
        Task {
            payload: self.payload.clone(),
        }
    }

    /// Whether the terminal `complete` control event has been published.
    ///
    /// Deliberately private: the exactly-once guarantee is enforced inside
    /// [`Self::publish_terminate`], so callers never need to poll this flag.
    /// Exposing it would only invite business code to build their own
    /// terminate state machines on top of the runner's.
    fn terminate_published(&self) -> bool {
        self.terminate_published
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Publishes the terminal `complete` control event on `{queue}.events`.
    ///
    /// Exactly-once per delivery: the first *successful* publish wins and
    /// later calls are no-ops. A failed publish clears the flag again so a
    /// caller (typically the runner's ack-path fallback) can retry, keeping
    /// the runner's "terminate before ack" guarantee reachable.
    ///
    /// `status` serializes with the kebab-case spelling the host plugin's
    /// `parse_control_event` expects (`succeeded`, `max-retries-exceeded`,
    /// ...). `output`, when present, is base64-encoded into the event.
    pub async fn publish_terminate(
        &self,
        status: crate::types::TaskStatus,
        output: Option<Vec<u8>>,
        error: Option<String>,
    ) -> Result<(), TaskError> {
        if self.terminate_published() {
            return Ok(());
        }
        crate::events::publish_control_event(
            &self.heartbeat,
            self.subject.clone(),
            crate::events::complete_event(
                &self.task_id,
                self.attempt,
                status,
                output.as_deref(),
                error.as_deref(),
            ),
        )
        .await?;
        self.terminate_published
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    /// Publishes an `attempt_failed` control event on `{queue}.events`.
    ///
    /// `source` is `"guest"` for recoverable business failures and
    /// `"system"` for infrastructure/deadline failures, mirroring the host
    /// plugin's `AttemptErrorSource` mapping.
    pub async fn publish_attempt_failed(&self, source: &str, error: &str) -> Result<(), TaskError> {
        crate::events::publish_control_event(
            &self.heartbeat,
            self.subject.clone(),
            crate::events::attempt_failed_event(&self.task_id, self.attempt, source, error),
        )
        .await
    }
}

#[async_trait]
impl HeartbeatSink for TaskContext {
    async fn send_heartbeat(&self, info: String) -> Result<(), TaskError> {
        // Keep the control event small enough for core NATS and observers.
        if info.len() > crate::config::HEARTBEAT_MAX_INFO_BYTES {
            return Err(TaskError::guest("heartbeat exceeds maximum size"));
        }
        // The payload follows the `{queue}.events` control-event contract so
        // the host plugin can forward it to the observer's `on-heartbeat`.
        crate::events::publish_control_event(
            &self.heartbeat,
            self.subject.clone(),
            crate::events::heartbeat_event(&self.task_id, &info),
        )
        .await
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
