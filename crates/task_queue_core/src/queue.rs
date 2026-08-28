//! Producer API used by host and native clients.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use async_nats::jetstream::Context as JetStreamContext;

use crate::config::{PAYLOAD_MAX_BYTES, QueueConfig};
use crate::nats::{QueueHandles, now_ms, task_subject};
use crate::types::{Task, TaskEnvelope, TaskInfo, TaskMeta, TaskResult, TaskState, TaskStatus};

#[derive(Clone)]
pub struct TaskProducer {
    handles: Arc<QueueHandles>,
}

impl TaskProducer {
    /// Builds a producer over queue resources already created for a workload.
    pub fn from_handles(handles: Arc<QueueHandles>) -> Self {
        Self { handles }
    }

    /// Creates queue resources if needed and returns a shared producer.
    pub async fn connect(client: JetStreamContext, config: QueueConfig) -> Result<Self> {
        let handles = QueueHandles::create(client, config).await?;
        Ok(Self {
            handles: Arc::new(handles),
        })
    }

    pub async fn submit(&self, task: Task) -> Result<String> {
        // Store metadata first so consumers can resolve state even if a
        // worker starts immediately after the task subject is published.
        if task.payload.len() > PAYLOAD_MAX_BYTES {
            anyhow::bail!("payload exceeds {PAYLOAD_MAX_BYTES} bytes");
        }

        let task_id = uuid::Uuid::now_v7().to_string();
        let created_at_ms = now_ms();
        let deadline_ms = created_at_ms
            + self
                .handles
                .config
                .dispatch_timeout
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX);
        let meta = TaskMeta {
            schema_version: TaskEnvelope::SCHEMA_VERSION,
            id: task_id.clone(),
            queue: self.handles.config.name.clone(),
            state: TaskState::Queued,
            attempt: 0,
            created_at_ms,
            dispatched_at_ms: None,
            completed_at_ms: None,
            deadline_ms,
            cancel_requested: false,
            attempts: Vec::new(),
        };
        self.handles
            .put_metadata(&meta)
            .await
            .context("failed to store task metadata")?;

        let envelope = TaskEnvelope::raw(task_id.clone(), task.payload, deadline_ms);
        let payload = serde_json::to_vec(&envelope).context("failed to encode task")?;
        self.handles
            .jetstream
            .publish(
                task_subject(&self.handles.config.name, &task_id),
                payload.into(),
            )
            .await
            .context("failed to publish task")?;
        Ok(task_id)
    }

    pub async fn query_status(&self, task_id: &str) -> Result<Option<TaskInfo>> {
        let Ok(meta) = self.handles.get_metadata(task_id).await else {
            return Ok(None);
        };
        Ok(Some(task_info(&meta)))
    }

    /// Requests cancellation using a KV revision CAS to avoid lost updates.
    pub async fn cancel_task(&self, task_id: &str) -> Result<()> {
        let Some((revision, mut meta)) = self.handles.get_metadata_with_revision(task_id).await?
        else {
            anyhow::bail!("task not found");
        };
        if meta.state.is_terminal() {
            anyhow::bail!("task already completed");
        }
        meta.cancel_requested = true;
        // A queued task can transition directly to terminal cancellation.
        if meta.state == TaskState::Queued {
            meta.state = TaskState::Cancelled;
            meta.completed_at_ms = Some(now_ms());
        }
        self.handles
            .update_metadata(&meta, revision)
            .await
            .context("failed to update task metadata")?;
        if meta.state == TaskState::Cancelled {
            let subject = task_subject(&self.handles.config.name, task_id);
            let _ = self
                .handles
                .task_stream
                .purge()
                .filter(subject)
                .await
                .context("failed to purge cancelled task");
        }
        Ok(())
    }
}

pub fn task_info(meta: &TaskMeta) -> TaskInfo {
    TaskInfo {
        id: meta.id.clone(),
        status: status_variant(&meta.state),
        attempt: meta.attempt.checked_sub(1),
        created_at_ms: meta.created_at_ms,
        dispatched_at_ms: meta.dispatched_at_ms,
        completed_at_ms: meta.completed_at_ms,
        cancel_requested: meta.cancel_requested,
    }
}

pub fn status_variant(state: &TaskState) -> TaskStatus {
    match state {
        TaskState::Succeeded => TaskStatus::Succeeded,
        TaskState::DispatchTimeoutPending | TaskState::DispatchTimeout => {
            TaskStatus::DispatchTimeout
        }
        TaskState::ExecutionTimeout => TaskStatus::ExecutionTimeout,
        TaskState::Cancelled => TaskStatus::Cancelled,
        TaskState::MaxRetriesExceeded => TaskStatus::MaxRetriesExceeded,
        TaskState::Queued | TaskState::Running | TaskState::Failed => TaskStatus::Failed,
    }
}

pub fn terminal_result(
    task_id: &str,
    state: TaskState,
    attempt: u32,
    output: Option<Vec<u8>>,
    error: Option<String>,
    _completed_at_ms: u64,
) -> TaskResult {
    TaskResult {
        id: task_id.to_string(),
        status: status_variant(&state),
        attempt,
        output,
        error,
    }
}

pub fn execution_deadline(config: &QueueConfig, started_at_ms: u64) -> u64 {
    started_at_ms.saturating_add(
        config
            .execution_timeout
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX),
    )
}

pub fn duration_since_ms(started_at_ms: u64, now: u64) -> u64 {
    now.saturating_sub(started_at_ms)
}

pub fn sleep_duration(ms: u64) -> Duration {
    Duration::from_millis(ms)
}
