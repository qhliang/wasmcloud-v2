//! # Task Queue Host Plugin
//!
//! A durable work queue backed by the host's shared data-plane NATS
//! connection. Producers import `custom:task-queue/producer` and export
//! `custom:task-queue/observer`; workers import `custom:task-queue/task-control`
//! and export `custom:task-queue/worker`. A single component may implement
//! both roles (import both interfaces and export both interfaces).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::Context as _;
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt as _;
use tokio::sync::{Mutex, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use wash_runtime::engine::ctx::{ActiveCtx, SharedCtx, extract_active_ctx};
use wash_runtime::engine::workload::{ResolvedWorkload, WorkloadItem};
use wash_runtime::plugin::{HostPlugin, WitInterfaces, WorkloadTracker};
use wash_runtime::wit::WitInterface;

use task_queue_core::config::{
    HEARTBEAT_MAX_INFO_BYTES, HEARTBEAT_MIN_INTERVAL_MS, PAYLOAD_MAX_BYTES,
};
use task_queue_core::events::{SCHEMA_VERSION as EVENT_SCHEMA_VERSION, TaskResultEvent};
use task_queue_core::nats::{AckAction, QueueHandles, now_ms};
use task_queue_core::queue::{TaskProducer, status_variant};
use task_queue_core::types::base64_encode;
use task_queue_core::types::{
    AttemptFailureRecord, Task as CoreTask, TaskMeta, TaskResult as CoreTaskResult, TaskState,
};

mod bindings {
    wasmtime::component::bindgen!({
        world: "task-queue",
        imports: { default: async | trappable | tracing },
        exports: { default: async | trappable | tracing },
        // `custom:task-queue/producer` 支持 named 多实例：一个组件可多次 import
        // 同一接口（`import agentq: custom:task-queue/producer@0.1.0;` 等），
        // 每个命名实例经清单中对应 hostInterfaces 条目的 `queue` 配置路由到
        // 各自的 JetStream 队列。运行时按 import 名解析出 [`QueueId`] 注入
        // Host 方法；无名 import 走组件主队列，行为不变。
        named_imports: {
            "custom:task-queue/producer": super::QueueId,
        },
    });
}

/// named producer import 的路由键：值即该实例绑定的队列名。
///
/// 在 `on_workload_item_bind` 阶段由命名 hostInterfaces 条目的 `queue`
/// 配置构建 `import name -> QueueId` 注册表，运行时经 `add_to_linker`
/// 的 lookup 闭包按 import 名解析。
#[derive(Clone, Debug)]
pub struct QueueId(pub String);

// Role-scoped binding worlds. The monolithic `task-queue` world forces every
// bound component to export BOTH `observer` and `worker`, which breaks the
// normal single-role components: a producer exports only `observer`, a worker
// exports only `worker`. These worlds let `dispatch_observer` / `call_worker`
// bind the matching export without requiring the other one.
mod observer_bindings {
    wasmtime::component::bindgen!({
        world: "task-queue-observer",
        exports: { default: async | trappable | tracing },
    });
}

mod worker_bindings {
    wasmtime::component::bindgen!({
        world: "task-queue-worker",
        exports: { default: async | trappable | tracing },
    });
}

use bindings::custom::task_queue::types::{
    AttemptErrorSource, AttemptFailure, Task as GuestTask, TaskInfo, TaskResult, TaskStatus,
};
use observer_bindings::custom::task_queue::types as obs;
use worker_bindings::custom::task_queue::types as wk;

pub const PLUGIN_ID: &str = "task-queue";

fn guest_status(status: task_queue_core::types::TaskStatus) -> TaskStatus {
    match status {
        task_queue_core::types::TaskStatus::Succeeded => TaskStatus::Succeeded,
        task_queue_core::types::TaskStatus::Failed => TaskStatus::Failed,
        task_queue_core::types::TaskStatus::DispatchTimeout => TaskStatus::DispatchTimeout,
        task_queue_core::types::TaskStatus::ExecutionTimeout => TaskStatus::ExecutionTimeout,
        task_queue_core::types::TaskStatus::Cancelled => TaskStatus::Cancelled,
        task_queue_core::types::TaskStatus::MaxRetriesExceeded => TaskStatus::MaxRetriesExceeded,
    }
}

fn guest_task_result(result: CoreTaskResult) -> TaskResult {
    TaskResult {
        id: result.id,
        status: guest_status(result.status),
        attempt: result.attempt,
        output: result.output,
        error: result.error,
    }
}

fn guest_task_info(meta: &TaskMeta) -> TaskInfo {
    TaskInfo {
        id: meta.id.clone(),
        status: guest_status(status_variant(&meta.state)),
        attempt: meta.attempt.checked_sub(1),
        created_at_ms: meta.created_at_ms,
        dispatched_at_ms: meta.dispatched_at_ms,
        completed_at_ms: meta.completed_at_ms,
        cancel_requested: meta.cancel_requested,
    }
}

/// Convert the full-world `AttemptFailure` carried by `CallbackEvent` into the
/// observer-only binding world's `AttemptFailure`.
fn to_observer_attempt_failure(event: AttemptFailure) -> obs::AttemptFailure {
    obs::AttemptFailure {
        id: event.id,
        attempt: event.attempt,
        source: match event.source {
            AttemptErrorSource::Guest => obs::AttemptErrorSource::Guest,
            AttemptErrorSource::System => obs::AttemptErrorSource::System,
        },
        error: event.error,
        started_at_ms: event.started_at_ms,
        failed_at_ms: event.failed_at_ms,
        duration_ms: event.duration_ms,
    }
}

/// Convert the full-world `TaskResult` carried by `CallbackEvent` into the
/// observer-only binding world's `TaskResult`.
fn to_observer_task_result(result: TaskResult) -> obs::TaskResult {
    obs::TaskResult {
        id: result.id,
        status: match result.status {
            TaskStatus::Succeeded => obs::TaskStatus::Succeeded,
            TaskStatus::Failed => obs::TaskStatus::Failed,
            TaskStatus::DispatchTimeout => obs::TaskStatus::DispatchTimeout,
            TaskStatus::ExecutionTimeout => obs::TaskStatus::ExecutionTimeout,
            TaskStatus::Cancelled => obs::TaskStatus::Cancelled,
            TaskStatus::MaxRetriesExceeded => obs::TaskStatus::MaxRetriesExceeded,
        },
        attempt: result.attempt,
        output: result.output,
        error: result.error,
    }
}

pub use task_queue_core::config::QueueConfig;

#[derive(Clone, Debug)]
pub enum CallbackEvent {
    Heartbeat { id: String, info: String },
    AttemptFailed { event: AttemptFailure },
    Complete { result: TaskResult },
}

/// 为回调事件生成简短的调试摘要（不含完整 payload）。
fn callback_event_summary(event: &CallbackEvent) -> String {
    match event {
        CallbackEvent::Heartbeat { id, info } => {
            format!("Heartbeat id={id} info_len={}", info.len())
        }
        CallbackEvent::AttemptFailed { event } => {
            format!(
                "AttemptFailed id={} attempt={} source={:?} error={}",
                event.id, event.attempt, event.source, event.error
            )
        }
        CallbackEvent::Complete { result } => {
            format!(
                "Complete id={} attempt={} status={:?} error={:?} output_len={}",
                result.id,
                result.attempt,
                result.status,
                result.error,
                result.output.as_ref().map_or(0, Vec::len)
            )
        }
    }
}

pub struct ComponentData {
    /// 组件主队列（无名 producer / task-control 的默认队列）。
    queue: String,
    /// named producer import 的路由表：import 名（如 `workflowq`）→ 队列名。
    /// 由 `on_workload_item_bind` 从命名 hostInterfaces 条目构建。
    named_queues: HashMap<String, String>,
    workload: Option<ResolvedWorkload>,
    cancel_token: CancellationToken,
    /// 外部/原生 worker 模式：队列由独立的原生 worker（如 agent-manager）消费，
    /// 插件不再启动 JetStream dispatcher（避免与原始 worker 竞争同一 durable consumer），
    /// 仅订阅 `{queue}.events` 将原生 worker 发布的生命周期事件转发给 observer 的 on_xx 回调。
    /// 以外部 worker 模式运行的队列集合（主队列 + 各 named 队列）。
    /// named 条目可独立配置 `external-worker`，与主条目互不影响。
    external_queues: HashSet<String>,
}

impl ComponentData {
    /// 该组件是否绑定了指定队列（主队列或任一 named 队列）。
    fn serves_queue(&self, queue: &str) -> bool {
        self.queue == queue || self.named_queues.values().any(|q| q == queue)
    }
}

#[derive(Default)]
struct HeartbeatState {
    last_heartbeat_at: Option<SystemTime>,
}

pub struct TaskQueuePlugin {
    client: Arc<async_nats::Client>,
    tracker: Arc<RwLock<WorkloadTracker<(), ComponentData>>>,
    queues: Arc<RwLock<HashMap<String, QueueHandles>>>,
    heartbeats: Arc<Mutex<HashMap<String, HeartbeatState>>>,
    callback_tx: tokio::sync::mpsc::UnboundedSender<(String, CallbackEvent)>,
    callback_cancel: CancellationToken,
    #[allow(dead_code)]
    lifetime: Mutex<()>,
}

impl TaskQueuePlugin {
    pub fn new(client: Arc<async_nats::Client>) -> anyhow::Result<Self> {
        let callback_cancel = CancellationToken::new();
        let (callback_tx, callback_rx) = tokio::sync::mpsc::unbounded_channel();
        Ok(Self {
            client,
            tracker: Arc::new(RwLock::new(WorkloadTracker::default())),
            queues: Arc::new(RwLock::new(HashMap::new())),
            heartbeats: Arc::new(Mutex::new(HashMap::new())),
            callback_tx,
            callback_cancel,
            lifetime: Mutex::new(()),
        })
        .inspect(|plugin| plugin.spawn_callback_dispatcher(callback_rx))
    }

    fn spawn_callback_dispatcher(
        &self,
        mut receiver: tokio::sync::mpsc::UnboundedReceiver<(String, CallbackEvent)>,
    ) {
        let tracker = self.tracker.clone();
        let cancel = self.callback_cancel.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    event = receiver.recv() => match event {
                        Some((component_id, event)) => {
                            debug!(component_id = %component_id, event = %callback_event_summary(&event), "callback dispatcher: dequeued event");
                            let workload = {
                                let tracker = tracker.read().await;
                                tracker
                                    .get_component_data(&component_id)
                                    .and_then(|data| data.workload.clone())
                            };
                            if let Some(workload) = workload {
                                debug!(component_id = %component_id, "callback dispatcher: dispatching to observer");
                                if let Err(err) =
                                    dispatch_observer(&workload, &component_id, event).await
                                {
                                    warn!(component_id = %component_id, err = %err, "failed to dispatch observer callback");
                                } else {
                                    debug!(component_id = %component_id, "callback dispatcher: observer callback dispatched ok");
                                }
                            } else {
                                warn!(component_id = %component_id, "observer component not ready");
                            }
                        }
                        None => break,
                    }
                }
            }
        });
    }

    async fn ensure_queue(&self, config: QueueConfig) -> anyhow::Result<QueueHandles> {
        {
            let queues = self.queues.read().await;
            if let Some(handles) = queues.get(&config.name) {
                return Ok(handles.clone());
            }
        }

        let jetstream = async_nats::jetstream::new((*self.client).clone());
        let handles = task_queue_core::nats::QueueHandles::create(jetstream, config).await?;
        self.queues
            .write()
            .await
            .insert(handles.config.name.clone(), handles.clone());
        Ok(handles)
    }

    async fn metadata(&self, handles: &QueueHandles, task_id: &str) -> anyhow::Result<TaskMeta> {
        handles.get_metadata(task_id).await
    }

    async fn put_metadata(&self, handles: &QueueHandles, meta: &TaskMeta) -> anyhow::Result<u64> {
        handles.put_metadata(meta).await
    }

    async fn observe(&self, component_id: &str, event: CallbackEvent) -> anyhow::Result<()> {
        debug!(component_id = %component_id, event = %callback_event_summary(&event), "task-queue: queueing observer callback");
        self.callback_tx
            .send((component_id.to_string(), event))
            .context("callback channel closed")
    }

    // ── producer 队列操作（无名与 named Host 实现共用）──────────────

    async fn submit_on(
        &self,
        queue: &str,
        component_id: &str,
        payload: Vec<u8>,
    ) -> wasmtime::Result<Result<String, String>> {
        debug!(component_id = %component_id, queue = %queue, payload_len = payload.len(), "task-queue: submit called");
        let handles = {
            let queues = self.queues.read().await;
            queues.get(queue).cloned()
        };
        let Some(handles) = handles else {
            return Ok(Err("queue not ready".into()));
        };
        if payload.len() > PAYLOAD_MAX_BYTES {
            return Ok(Err(format!("payload exceeds {PAYLOAD_MAX_BYTES} bytes")));
        }

        let producer = TaskProducer::from_handles(Arc::new(handles));
        match producer.submit(CoreTask { payload }).await {
            Ok(task_id) => {
                debug!(component_id = %component_id, queue = %queue, task_id = %task_id, "task-queue: task submitted");
                Ok(Ok(task_id))
            }
            Err(err) => {
                warn!(component_id = %component_id, queue = %queue, err = %err, "task-queue: submit failed");
                Ok(Err(err.to_string()))
            }
        }
    }

    async fn query_status_on(
        &self,
        queue: &str,
        task_id: &str,
    ) -> wasmtime::Result<Result<Option<TaskInfo>, String>> {
        let handles = {
            let queues = self.queues.read().await;
            queues.get(queue).cloned()
        };
        let Some(handles) = handles else {
            return Ok(Ok(None));
        };
        match self.metadata(&handles, task_id).await {
            Ok(meta) => Ok(Ok(Some(guest_task_info(&meta)))),
            Err(_) => Ok(Ok(None)),
        }
    }

    async fn cancel_task_on(
        &self,
        queue: &str,
        task_id: &str,
    ) -> wasmtime::Result<Result<(), String>> {
        let handles = {
            let queues = self.queues.read().await;
            queues.get(queue).cloned()
        };
        let Some(handles) = handles else {
            return Ok(Err("queue not ready".into()));
        };
        let mut meta = match self.metadata(&handles, task_id).await {
            Ok(meta) => meta,
            Err(err) => return Ok(Err(format!("task not found: {err}"))),
        };
        if matches!(
            meta.state,
            TaskState::Succeeded
                | TaskState::Failed
                | TaskState::DispatchTimeout
                | TaskState::ExecutionTimeout
                | TaskState::Cancelled
                | TaskState::MaxRetriesExceeded
        ) {
            return Ok(Err("task already completed".into()));
        }
        meta.cancel_requested = true;
        meta.state = if meta.state == TaskState::Queued {
            TaskState::Cancelled
        } else {
            meta.state
        };
        if let Err(err) = self.put_metadata(&handles, &meta).await {
            return Ok(Err(format!("failed to update task metadata: {err}")));
        }
        if meta.state == TaskState::Cancelled {
            let subject = format!("{}.tasks.{task_id}", handles.config.name);
            let _ = handles.task_stream.purge().filter(subject).await;
        }
        Ok(Ok(()))
    }

    /// 解析组件调用 producer 时的目标队列：无名调用用主队列；
    /// named 调用由 [`QueueId`] 直接携带，不经本方法。
    async fn default_queue_for(&self, component_id: &str) -> Option<String> {
        let tracker = self.tracker.read().await;
        tracker
            .get_component_data(component_id)
            .map(|data| data.queue.clone())
    }

    async fn worker_component(
        &self,
        component_id: &str,
    ) -> Option<(ResolvedWorkload, CancellationToken)> {
        let tracker = self.tracker.read().await;
        let data = tracker.get_component_data(component_id)?;
        Some((data.workload.clone()?, data.cancel_token.clone()))
    }

    async fn execute_task(
        &self,
        handles: QueueHandles,
        message: &mut async_nats::jetstream::Message,
    ) {
        let subject = message.subject.clone();
        let info = message.info().ok();
        let task_id = subject
            .as_ref()
            .rsplit('.')
            .next()
            .map(str::to_string)
            .unwrap_or_default();
        let attempt = info.as_ref().map(|info| info.delivered).unwrap_or(1).max(1) as u32;
        let cancel = CancellationToken::new();
        let (_task, acker) = message.clone().split();
        let acker = Arc::new(acker);
        let renew_acker = acker.clone();
        let renew_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(
                task_queue_core::config::LEASE_RENEW_INTERVAL_MS,
            ));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            interval.tick().await;
            loop {
                interval.tick().await;
                if AckAction::Progress.apply(&renew_acker).await.is_err() {
                    break;
                }
            }
        });

        if let Err(err) = self
            .put_metadata_with(&handles, &task_id, |meta| {
                meta.state = TaskState::Running;
                meta.attempt = attempt;
                meta.dispatched_at_ms = Some(now_ms());
                meta.deadline_ms = now_ms() + handles.config.execution_timeout.as_millis() as u64;
            })
            .await
        {
            warn!(task_id = %task_id, err = %err, "failed to mark task running");
            return;
        }

        let component_id = self.find_worker_component(&handles.config.name).await;
        let started = now_ms();
        let (result, output) = match (
            self.worker_component(&component_id).await,
            component_id.is_empty(),
        ) {
            (Some((workload, _)), false) => {
                call_worker(
                    &workload,
                    &component_id,
                    GuestTask {
                        payload: message.payload.to_vec(),
                    },
                )
                .await
            }
            _ => {
                let duration = now_ms().saturating_sub(started);
                let error = "no worker component bound to queue".to_string();
                self.record_attempt_failure(
                    &handles, &task_id, attempt, "system", &error, started, duration,
                )
                .await;
                (Err(error), None)
            }
        };

        match result {
            Ok(()) => {
                let state = if cancel.is_cancelled() {
                    TaskState::Cancelled
                } else {
                    TaskState::Succeeded
                };
                self.complete_task(&handles, &task_id, attempt, state, output, None, started)
                    .await;
                let _ = AckAction::Ack.apply(&acker).await;
            }
            Err(error) => {
                let duration = now_ms().saturating_sub(started);
                self.record_attempt_failure(
                    &handles, &task_id, attempt, "guest", &error, started, duration,
                )
                .await;
                if attempt >= handles.config.max_deliver.max(1) as u32 {
                    self.complete_task(
                        &handles,
                        &task_id,
                        attempt,
                        TaskState::MaxRetriesExceeded,
                        None,
                        Some("max retries exceeded".to_string()),
                        started,
                    )
                    .await;
                    let _ = AckAction::Term.apply(&acker).await;
                } else {
                    let _ = AckAction::Nak(Some(handles.config.backoff_for_attempt(attempt)))
                        .apply(&acker)
                        .await;
                }
            }
        }
        renew_task.abort();
    }

    async fn put_metadata_with<F>(
        &self,
        handles: &QueueHandles,
        task_id: &str,
        mutate: F,
    ) -> anyhow::Result<()>
    where
        F: FnOnce(&mut TaskMeta),
    {
        let mut meta = self.metadata(handles, task_id).await?;
        mutate(&mut meta);
        self.put_metadata(handles, &meta).await?;
        Ok(())
    }

    async fn find_worker_component(&self, queue: &str) -> String {
        let tracker = self.tracker.read().await;
        tracker
            .components
            .keys()
            .find_map(|component_id| {
                tracker
                    .get_component_data(component_id)
                    .filter(|data| data.serves_queue(queue))
                    .map(|_| component_id.clone())
            })
            .unwrap_or_default()
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_attempt_failure(
        &self,
        handles: &QueueHandles,
        task_id: &str,
        attempt: u32,
        source: &str,
        error: &str,
        started_at_ms: u64,
        duration_ms: u64,
    ) {
        let failure = AttemptFailureRecord {
            attempt,
            source: source.to_string(),
            error: error.to_string(),
            started_at_ms: Some(started_at_ms),
            failed_at_ms: Some(now_ms()),
            duration_ms: Some(duration_ms),
        };
        let mut meta = match self.metadata(handles, task_id).await {
            Ok(meta) => meta,
            Err(err) => {
                warn!(task_id = %task_id, err = %err, "failed to read task before failure record");
                return;
            }
        };
        meta.attempts.push(failure.clone());
        if let Err(err) = self.put_metadata(handles, &meta).await {
            warn!(task_id = %task_id, err = %err, "failed to record attempt failure");
        }

        let source = if source == "guest" {
            AttemptErrorSource::Guest
        } else {
            AttemptErrorSource::System
        };
        let event = AttemptFailure {
            id: task_id.to_string(),
            attempt,
            source,
            error: error.to_string(),
            started_at_ms: failure.started_at_ms,
            failed_at_ms: failure.failed_at_ms,
            duration_ms: failure.duration_ms,
        };
        if let Some(component_id) = self.find_producer_component(&handles.config.name).await
            && let Err(err) = self
                .observe(&component_id, CallbackEvent::AttemptFailed { event })
                .await
        {
            warn!(err = %err, "failed to queue attempt failure callback");
        }
        debug!(task_id = %task_id, "attempt failed callback queued");
    }

    async fn find_producer_component(&self, queue: &str) -> Option<String> {
        let tracker = self.tracker.read().await;
        let mut fallback = None;
        for component_id in tracker.components.keys() {
            let matches = tracker
                .get_component_data(component_id)
                .is_some_and(|data| data.serves_queue(queue));
            if !matches {
                continue;
            }
            // 优先返回已 resolve（workload 就绪）的 observer 组件；滚动更新期间
            // 旧 worklod 尚未解绑时，其组件会先出现在 tracker 中，避免把回调
            // 固定到即将解绑的陈旧组件上。
            let ready = tracker
                .get_component_data(component_id)
                .is_some_and(|data| data.workload.is_some());
            if ready {
                return Some(component_id.clone());
            }
            fallback.get_or_insert_with(|| component_id.clone());
        }
        fallback
    }

    #[allow(clippy::too_many_arguments)]
    async fn complete_task(
        &self,
        handles: &QueueHandles,
        task_id: &str,
        attempt: u32,
        state: TaskState,
        output: Option<Vec<u8>>,
        error: Option<String>,
        started_at_ms: u64,
    ) {
        let mut meta = match self.metadata(handles, task_id).await {
            Ok(meta) => meta,
            Err(err) => {
                warn!(task_id = %task_id, err = %err, "failed to read task before completion");
                return;
            }
        };
        meta.state = state;
        meta.completed_at_ms = Some(now_ms());
        if let Err(err) = self.put_metadata(handles, &meta).await {
            warn!(task_id = %task_id, err = %err, "failed to update completed metadata");
        }

        let status = status_variant(&state);
        let result = CoreTaskResult {
            id: task_id.to_string(),
            status,
            attempt,
            output,
            error,
        };
        let archived = TaskResultEvent {
            schema_version: EVENT_SCHEMA_VERSION,
            id: result.id.clone(),
            status: result.status,
            attempt: result.attempt,
            output_base64: result.output.as_deref().map(base64_encode),
            output: None,
            error: result.error.clone(),
            completed_at_ms: meta.completed_at_ms,
        };
        self.archive_result(handles, &archived).await;

        if let Some(component_id) = self.find_producer_component(&handles.config.name).await
            && let Err(err) = self
                .observe(
                    &component_id,
                    CallbackEvent::Complete {
                        result: guest_task_result(result),
                    },
                )
                .await
        {
            warn!(err = %err, "failed to queue completion callback");
        }
        debug!(task_id = %task_id, started_at_ms, "task completed");
    }

    async fn archive_result(&self, handles: &QueueHandles, result: &TaskResultEvent) {
        let subject = task_queue_core::nats::result_subject(&handles.config.name, &result.id);
        let raw = match serde_json::to_vec(result) {
            Ok(raw) => raw,
            Err(err) => {
                warn!(err = %err, "failed to encode archived result");
                return;
            }
        };
        if let Err(err) = handles.jetstream.publish(subject, Bytes::from(raw)).await {
            warn!(err = %err, "failed to archive task result");
        }
    }

    async fn start_queue_loop(&self, handles: QueueHandles, cancel: CancellationToken) {
        let consumer = match handles.ensure_worker().await {
            Ok(consumer) => consumer,
            Err(err) => {
                warn!(err = %err, "failed to create task consumer");
                return;
            }
        };

        let mut messages = match consumer.messages().await {
            Ok(messages) => messages,
            Err(err) => {
                warn!(err = %err, "failed to subscribe to task consumer");
                return;
            }
        };
        let plugin_self = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    message = messages.next() => match message {
                        Some(Ok(mut message)) => {
                            let plugin = plugin_self.clone();
                            let handles = handles.clone();
                            tokio::spawn(async move {
                                plugin.execute_task(handles, &mut message).await;
                            });
                        }
                        Some(Err(err)) => warn!(err = %err, "task consumer stream error"),
                        None => break,
                    }
                }
            }
        });
    }

    /// 订阅 `{queue}.events`，将原生/外部 worker 发布的生命周期事件转发给
    /// observer 的 on_xx 回调。外部 worker 模式下这是 status 回报的唯一通道。
    async fn start_events_loop(
        &self,
        _handles: QueueHandles,
        queue: &str,
        cancel: CancellationToken,
    ) {
        let subject = format!("{queue}.events");
        let queue = queue.to_string();
        let client = (*self.client).clone();
        let plugin = self.clone();
        tokio::spawn(async move {
            let mut subscriber = match client.subscribe(subject.clone()).await {
                Ok(subscriber) => subscriber,
                Err(err) => {
                    warn!(subject = %subject, err = %err, "failed to subscribe to task events");
                    return;
                }
            };
            debug!(subject = %subject, "events loop: subscribed");
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        debug!(subject = %subject, "events loop: cancelled, stopping");
                        break;
                    }
                    message = subscriber.next() => match message {
                        Some(message) => {
                            debug!(subject = %subject, event_type = ?message.payload.first().map(|b| *b as char), payload_len = message.payload.len(), "events loop: received message");
                            if let Some(event) = parse_control_event(&message.payload) {
                                // 每条事件动态解析当前可用的 observer 组件：滚动更新
                                // 期间旧组件未解绑时，仍能路由到已 resolve 的新组件。
                                let Some(producer) = plugin.find_producer_component(&queue).await else {
                                    debug!(subject = %subject, event = %callback_event_summary(&event), "events loop: no producer component available, dropping event");
                                    continue;
                                };
                                debug!(subject = %subject, producer_component = %producer, event = %callback_event_summary(&event), "events loop: parsed event, forwarding to observer");
                                let _ = plugin.observe(&producer, event).await;
                            } else {
                                debug!(subject = %subject, payload_len = message.payload.len(), "events loop: message failed to parse as control event");
                            }
                        }
                        None => {
                            debug!(subject = %subject, "events loop: subscription ended");
                            break;
                        }
                    },
                }
            }
        });
    }
}

/// 解析原生 worker 发布的 `{queue}.events` JSON 事件为插件内部的 `CallbackEvent`。
/// 约定负载：
/// ```json
/// { "type": "complete", "id": "<queue-task-id>", "attempt": 1,
///   "status": "succeeded|failed|execution-timeout|...", "output": "<base64>", "error": "..." }
/// { "type": "attempt_failed", "id": "...", "attempt": 1, "source": "guest|system", "error": "..." }
/// { "type": "heartbeat", "id": "...", "info": "..." }
/// ```
fn parse_control_event(payload: &[u8]) -> Option<CallbackEvent> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let id = value.get("id")?.as_str()?.to_string();
    let event_type = value.get("type").and_then(serde_json::Value::as_str)?;
    match event_type {
        "complete" => {
            let status = match value
                .get("status")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("failed")
            {
                "succeeded" => TaskStatus::Succeeded,
                "execution-timeout" => TaskStatus::ExecutionTimeout,
                "dispatch-timeout" => TaskStatus::DispatchTimeout,
                "cancelled" => TaskStatus::Cancelled,
                "max-retries-exceeded" => TaskStatus::MaxRetriesExceeded,
                _ => TaskStatus::Failed,
            };
            let attempt = value
                .get("attempt")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1) as u32;
            let output = value
                .get("output")
                .and_then(serde_json::Value::as_str)
                .and_then(|b64| task_queue_core::types::base64_decode(b64).ok());
            let error = value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .map(ToString::to_string);
            Some(CallbackEvent::Complete {
                result: TaskResult {
                    id,
                    status,
                    attempt,
                    output,
                    error,
                },
            })
        }
        "attempt_failed" => {
            let attempt = value
                .get("attempt")
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(1) as u32;
            let source = match value.get("source").and_then(serde_json::Value::as_str) {
                Some("system") => AttemptErrorSource::System,
                _ => AttemptErrorSource::Guest,
            };
            let error = value
                .get("error")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            Some(CallbackEvent::AttemptFailed {
                event: AttemptFailure {
                    id,
                    attempt,
                    source,
                    error,
                    started_at_ms: None,
                    failed_at_ms: None,
                    duration_ms: None,
                },
            })
        }
        "heartbeat" => {
            let info = value
                .get("info")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .to_string();
            Some(CallbackEvent::Heartbeat { id, info })
        }
        _ => None,
    }
}

impl Clone for TaskQueuePlugin {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            tracker: self.tracker.clone(),
            queues: self.queues.clone(),
            heartbeats: Arc::clone(&self.heartbeats),
            callback_tx: self.callback_tx.clone(),
            callback_cancel: self.callback_cancel.clone(),
            lifetime: Mutex::new(()),
        }
    }
}

impl std::fmt::Debug for TaskQueuePlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueuePlugin").finish()
    }
}

pub struct CallbackDispatcher {
    #[allow(dead_code)]
    receiver: tokio::sync::mpsc::UnboundedReceiver<(String, CallbackEvent)>,
}

async fn dispatch_observer(
    workload: &ResolvedWorkload,
    component_id: &str,
    event: CallbackEvent,
) -> anyhow::Result<()> {
    debug!(component_id = %component_id, event = %callback_event_summary(&event), "dispatch_observer: binding observer store");
    let mut store = workload
        .new_store(component_id)
        .await
        .context("failed to create observer store")?;
    let instance_pre = workload
        .instantiate_pre(component_id)
        .await
        .context("failed to instantiate observer")?;
    // Bind against the observer-only world so producer-only components (which
    // export `observer` but not `worker`) can receive lifecycle callbacks.
    let pre = observer_bindings::TaskQueueObserverPre::new(instance_pre)
        .map_err(|err| anyhow::anyhow!("failed to bind observer: {err}"))?;
    let proxy = pre
        .instantiate_async(&mut store)
        .await
        .map_err(|err| anyhow::anyhow!("failed to start observer: {err}"))?;
    let observer = proxy.custom_task_queue_observer();
    let call_result = store
        .run_concurrent(async move |accessor| match event {
            CallbackEvent::Heartbeat { id, info } => {
                let res = observer.call_on_heartbeat(accessor, id, info).await;
                debug!(component_id = %component_id, "dispatch_observer: on_heartbeat called, ok={}", res.is_ok());
                res
            }
            CallbackEvent::AttemptFailed { event } => {
                let res = observer
                    .call_on_attempt_failed(accessor, to_observer_attempt_failure(event))
                    .await;
                debug!(component_id = %component_id, "dispatch_observer: on_attempt_failed called, ok={}", res.is_ok());
                res
            }
            CallbackEvent::Complete { result } => {
                let res = observer
                    .call_on_complete(accessor, to_observer_task_result(result))
                    .await;
                debug!(component_id = %component_id, "dispatch_observer: on_complete called, ok={}", res.is_ok());
                res
            }
        })
        .await
        .map_err(|err| anyhow::anyhow!("observer store call failed: {err}"))?
        .map_err(|err| anyhow::anyhow!("observer handler failed: {err}"))?;
    drop(call_result);
    Ok(())
}

async fn call_worker(
    workload: &ResolvedWorkload,
    component_id: &str,
    task: GuestTask,
) -> (Result<(), String>, Option<Vec<u8>>) {
    let mut store = match workload.new_store(component_id).await {
        Ok(store) => store,
        Err(err) => return (Err(format!("failed to create worker store: {err}")), None),
    };
    let instance_pre = match workload.instantiate_pre(component_id).await {
        Ok(pre) => pre,
        Err(err) => return (Err(format!("failed to instantiate worker: {err}")), None),
    };
    // Bind against the worker-only world so worker-only components (which export
    // `worker` but not `observer`) can be driven without also exporting
    // `observer`.
    let pre = match worker_bindings::TaskQueueWorkerPre::new(instance_pre) {
        Ok(pre) => pre,
        Err(err) => return (Err(format!("failed to bind worker export: {err}")), None),
    };
    let proxy = match pre.instantiate_async(&mut store).await {
        Ok(proxy) => proxy,
        Err(err) => return (Err(format!("failed to start worker: {err}")), None),
    };
    let worker = proxy.custom_task_queue_worker();
    let worker_task = wk::Task {
        payload: task.payload,
    };
    match store
        .run_concurrent(async move |accessor| worker.call_handle_task(accessor, worker_task).await)
        .await
    {
        Ok(Ok(Ok(output))) => (Ok(()), output),
        Ok(Ok(Err(error))) => (Err(error), None),
        Ok(Err(err)) => (Err(format!("worker call failed: {err}")), None),
        Err(err) => (Err(format!("worker store call failed: {err}")), None),
    }
}

impl<'a> bindings::custom::task_queue::types::Host for ActiveCtx<'a> {}

impl<'a> bindings::custom::task_queue::producer::Host for ActiveCtx<'a> {
    async fn submit(&mut self, task: GuestTask) -> wasmtime::Result<Result<String, String>> {
        let Ok(plugin) = self.try_get_plugin::<TaskQueuePlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let component_id = self.component_id.as_ref().to_string();
        let Some(queue) = plugin.default_queue_for(&component_id).await else {
            debug!(component_id = %component_id, "task-queue: submit from untracked component");
            return Ok(Err("component not tracked".into()));
        };
        plugin.submit_on(&queue, &component_id, task.payload).await
    }

    async fn query_status(
        &mut self,
        task_id: String,
    ) -> wasmtime::Result<Result<Option<TaskInfo>, String>> {
        let Ok(plugin) = self.try_get_plugin::<TaskQueuePlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let component_id = self.component_id.as_ref().to_string();
        let Some(queue) = plugin.default_queue_for(&component_id).await else {
            return Ok(Err("component not tracked".into()));
        };
        plugin.query_status_on(&queue, &task_id).await
    }

    async fn cancel_task(&mut self, task_id: String) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<TaskQueuePlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let component_id = self.component_id.as_ref().to_string();
        let Some(queue) = plugin.default_queue_for(&component_id).await else {
            return Ok(Err("component not tracked".into()));
        };
        plugin.cancel_task_on(&queue, &task_id).await
    }
}

/// named producer 实例（`import <name>: custom:task-queue/producer@0.1.0;`）。
/// 目标队列由 [`QueueId`] 携带（bind 阶段按 import 名从清单配置解析），
/// 因此调用方无法误投到其他队列。
impl<'a> bindings::named_imports::custom::task_queue::producer::Host for ActiveCtx<'a> {
    async fn submit(
        &mut self,
        id: QueueId,
        task: GuestTask,
    ) -> wasmtime::Result<Result<String, String>> {
        let Ok(plugin) = self.try_get_plugin::<TaskQueuePlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let component_id = self.component_id.as_ref().to_string();
        plugin.submit_on(&id.0, &component_id, task.payload).await
    }

    async fn query_status(
        &mut self,
        id: QueueId,
        task_id: String,
    ) -> wasmtime::Result<Result<Option<TaskInfo>, String>> {
        let Ok(plugin) = self.try_get_plugin::<TaskQueuePlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        plugin.query_status_on(&id.0, &task_id).await
    }

    async fn cancel_task(
        &mut self,
        id: QueueId,
        task_id: String,
    ) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<TaskQueuePlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        plugin.cancel_task_on(&id.0, &task_id).await
    }
}

impl<'a> bindings::custom::task_queue::task_control::Host for ActiveCtx<'a> {
    async fn send_heartbeat(
        &mut self,
        task_id: String,
        info: String,
    ) -> wasmtime::Result<Result<(), String>> {
        let Ok(plugin) = self.try_get_plugin::<TaskQueuePlugin>(PLUGIN_ID) else {
            return Ok(Err("plugin not available".into()));
        };
        let component_id = self.component_id.as_ref().to_string();
        let queue = {
            let tracker = plugin.tracker.read().await;
            let Some(data) = tracker.get_component_data(&component_id) else {
                return Ok(Err("component not tracked".into()));
            };
            data.queue.clone()
        };
        if info.len() > HEARTBEAT_MAX_INFO_BYTES {
            return Ok(Err(format!(
                "info exceeds {HEARTBEAT_MAX_INFO_BYTES} bytes"
            )));
        }
        if let Some(component_id) = plugin.find_producer_component(&queue).await {
            let now = SystemTime::now();
            let mut heartbeats = plugin.heartbeats.lock().await;
            let accepted = match heartbeats.get_mut(&task_id) {
                Some(state) => state.last_heartbeat_at.is_none_or(|last| {
                    now.duration_since(last).is_ok_and(|elapsed| {
                        elapsed >= Duration::from_millis(HEARTBEAT_MIN_INTERVAL_MS)
                    })
                }),
                None => true,
            };
            if !accepted {
                return Ok(Err(format!(
                    "heartbeat exceeds minimum interval of {HEARTBEAT_MIN_INTERVAL_MS} ms"
                )));
            }
            heartbeats.insert(
                task_id.clone(),
                HeartbeatState {
                    last_heartbeat_at: Some(now),
                },
            );
            drop(heartbeats);
            return plugin
                .observe(
                    &component_id,
                    CallbackEvent::Heartbeat { id: task_id, info },
                )
                .await
                .map(|_| Ok(()))
                .map_err(|err| wasmtime::Error::msg(err.to_string()));
        }
        Ok(Ok(()))
    }

    async fn is_cancelled(&mut self, task_id: String) -> wasmtime::Result<bool> {
        let Ok(plugin) = self.try_get_plugin::<TaskQueuePlugin>(PLUGIN_ID) else {
            return Ok(false);
        };
        let component_id = self.component_id.as_ref().to_string();
        let queue = {
            let tracker = plugin.tracker.read().await;
            let Some(data) = tracker.get_component_data(&component_id) else {
                return Ok(false);
            };
            data.queue.clone()
        };
        let handles = {
            let queues = plugin.queues.read().await;
            queues.get(&queue).cloned()
        };
        let Some(handles) = handles else {
            return Ok(false);
        };
        Ok(matches!(
            plugin.metadata(&handles, &task_id).await,
            Ok(meta) if meta.cancel_requested
        ))
    }
}

#[async_trait]
impl HostPlugin for TaskQueuePlugin {
    fn id(&self) -> &'static str {
        PLUGIN_ID
    }

    fn world(&self) -> WitWorld {
        WitWorld {
            imports: HashSet::from([WitInterface::from(
                "custom:task-queue/producer,task-control,types@0.1.0",
            )]),
            exports: HashSet::from([WitInterface::from(
                "custom:task-queue/observer,worker@0.1.0",
            )]),
        }
    }

    /// 支持同名接口的多命名实例（`import agentq: custom:task-queue/producer@0.1.0;`
    /// 等），按 import 名路由到各自 `queue` 配置的队列。
    fn supports_named_instances(&self) -> bool {
        true
    }

    async fn start(&self) -> anyhow::Result<()> {
        info!("task queue plugin started");
        Ok(())
    }

    async fn on_workload_item_bind<'a>(
        &self,
        item: &mut WorkloadItem<'a>,
        interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        // 注意：不能用 `interfaces.get(...)` —— 其内部是 `.find()`，同名接口存在
        // 多个条目（无名 + named）时只会返回其中一个且不按 name 过滤。
        let tq_entries: Vec<WitInterface> = interfaces
            .iter()
            .filter(|i| i.namespace == "custom" && i.package == "task-queue")
            .cloned()
            .collect();
        if tq_entries.is_empty() {
            return Ok(());
        }

        bindings::custom::task_queue::types::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        bindings::custom::task_queue::task_control::add_to_linker::<_, SharedCtx>(
            item.linker(),
            extract_active_ctx,
        )?;
        // 无名 producer：单队列语义（组件主队列），向后兼容既有组件。
        if tq_entries
            .iter()
            .any(|i| i.name.is_none() && i.interfaces.contains("producer"))
        {
            bindings::custom::task_queue::producer::add_to_linker::<_, SharedCtx>(
                item.linker(),
                extract_active_ctx,
            )?;
        }
        // named producer：每个命名实例按其 `queue` 配置路由。
        let named_queues: HashMap<String, String> = tq_entries
            .iter()
            .filter_map(|i| {
                let name = i.name.as_ref()?;
                let queue = i.config.get("queue")?;
                Some((name.clone(), queue.clone()))
            })
            .collect();
        if !named_queues.is_empty() {
            let registry = named_queues.clone();
            let component = item.component().clone();
            bindings::named_imports::custom::task_queue::producer::add_to_linker::<_, SharedCtx>(
                item.linker(),
                &component,
                move |name| {
                    registry.get(name).cloned().map(QueueId).ok_or_else(|| {
                        wasmtime::Error::msg(format!(
                            "no queue configured for named task-queue import '{name}'"
                        ))
                    })
                },
                extract_active_ctx,
            )?;
        }

        let WorkloadItem::Component(component) = item else {
            return Ok(());
        };
        let component_id = component.id().to_string();
        // 主队列取自无名条目（与既有行为一致）；named 条目各自独立成队列。
        let primary = tq_entries.iter().find(|i| i.name.is_none());
        let Some(primary) = primary else {
            anyhow::bail!("task-queue requires an unnamed hostInterfaces entry")
        };
        let mut config = component.local_resources().config.clone();
        config.extend(primary.config.clone());
        let queue_config = QueueConfig::from_config(&config)?;

        let mut external_queues = HashSet::new();
        if config
            .get("external-worker")
            .and_then(|value| value.parse::<bool>().ok())
            .unwrap_or(false)
        {
            external_queues.insert(queue_config.name.clone());
        }
        for iface in &tq_entries {
            let Some(name) = &iface.name else { continue };
            let external = iface
                .config
                .get("external-worker")
                .and_then(|value| value.parse::<bool>().ok())
                .unwrap_or(false);
            if external && let Some(q) = named_queues.get(name) {
                external_queues.insert(q.clone());
            }
        }

        debug!(
            component_id = %component_id, queue = %queue_config.name,
            named = ?named_queues, external = ?external_queues,
            "task-queue: component bound"
        );
        self.tracker.write().await.add_component(
            component,
            ComponentData {
                queue: queue_config.name.clone(),
                named_queues,
                workload: None,
                cancel_token: CancellationToken::new(),
                external_queues,
            },
        );
        Ok(())
    }

    async fn on_workload_resolved(
        &self,
        workload: &ResolvedWorkload,
        component_id: &str,
    ) -> anyhow::Result<()> {
        debug!(component_id = %component_id, "task-queue: workload resolved, setting observer workload");
        let (queue, named_queues, external_queues, cancel_token) = {
            let mut tracker = self.tracker.write().await;
            let Some(data) = tracker.get_component_data_mut(component_id) else {
                debug!(component_id = %component_id, "task-queue: workload resolved but component not tracked, skipping");
                return Ok(());
            };
            data.workload = Some(workload.clone());
            // 使用 per-component 的 cancel_token 派生子令牌：workload 解绑时
            // on_workload_unbind 会取消 data.cancel_token，从而真正终止本组件
            // 启动的队列/事件循环。此前误用 plugin 级 callback_cancel，导致解绑后
            // 循环永不停止（僵尸队列循环持续消费 agent-task 并调用 call_worker）。
            (
                data.queue.clone(),
                data.named_queues.values().cloned().collect::<Vec<_>>(),
                data.external_queues.clone(),
                data.cancel_token.child_token(),
            )
        };
        // 主队列 + 各 named 队列（去重，named 可能与主队列同名）。
        let mut targets = vec![queue.clone()];
        targets.extend(named_queues);
        targets.sort();
        targets.dedup();
        debug!(component_id = %component_id, queues = ?targets, "task-queue: workload resolved, starting loops");
        for target in targets {
            let external = external_queues.contains(&target);
            let config = QueueConfig::new(target.clone());
            let handles = self.ensure_queue(config).await?;
            // 外部/原生 worker 模式下，队列由独立原生 worker（agent-manager /
            // workflow-manager）消费，插件不启动 JetStream dispatcher，避免与
            // 原始 worker 竞争同一 durable consumer。
            if !external {
                self.start_queue_loop(handles.clone(), cancel_token.clone())
                    .await;
            }
            // 无论是否自消费，插件都订阅 `{queue}.events`，将原生 worker 发布的
            // 生命周期事件转发给 observer 的 on_xx 回调（status 经此通道回报）。
            self.start_events_loop(handles, &target, cancel_token.clone())
                .await;
        }
        Ok(())
    }

    async fn on_workload_unbind(
        &self,
        workload_id: &str,
        _interfaces: WitInterfaces<'_>,
    ) -> anyhow::Result<()> {
        debug!(workload_id = %workload_id, "task-queue: workload unbinding");
        self.tracker
            .write()
            .await
            .remove_workload_with_cleanup(
                workload_id,
                |_| async {},
                |data: ComponentData| async move {
                    data.cancel_token.cancel();
                },
            )
            .await;
        Ok(())
    }

    async fn stop(&self) -> anyhow::Result<()> {
        self.callback_cancel.cancel();
        Ok(())
    }
}

pub type WitWorld = wash_runtime::wit::WitWorld;

#[cfg(test)]
mod tests {
    use super::*;

    use task_queue_core::config::{
        DEFAULT_RETRY_BACKOFF_MS as RETRY_BACKOFF_MS, parse_backoff_ms, validate_queue_name,
    };
    use task_queue_core::nats::metadata_key;
    use task_queue_core::types::TaskStatus as CoreTaskStatus;

    #[test]
    fn queue_name_accepts_valid_characters() {
        assert!(validate_queue_name("agent-task").is_ok());
        assert!(validate_queue_name("Agent_2").is_ok());
    }

    #[test]
    fn queue_name_rejects_invalid_names() {
        assert!(validate_queue_name("").is_err());
        assert!(validate_queue_name("-agent").is_err());
        assert!(validate_queue_name("agent.*").is_err());
        assert!(validate_queue_name("agent task").is_err());
        let long = "a".repeat(65);
        assert!(validate_queue_name(&long).is_err());
    }

    #[test]
    fn missing_queue_is_required() {
        let err = QueueConfig::from_config(&HashMap::new()).unwrap_err();
        assert!(err.to_string().contains("missing required config: 'queue'"));
    }

    #[test]
    fn backoff_parses_csv_values() {
        let mut config = HashMap::new();
        config.insert("queue".to_string(), "agent-task".to_string());
        config.insert(
            "retry-backoff-ms".to_string(),
            "1000, 5000,15000 ,60000".to_string(),
        );
        let parsed =
            parse_backoff_ms(&config, "retry-backoff-ms", RETRY_BACKOFF_MS).expect("valid backoff");
        assert_eq!(parsed, vec![1_000, 5_000, 15_000, 60_000]);
    }

    #[test]
    fn backoff_uses_default_without_config() {
        assert_eq!(
            parse_backoff_ms(&HashMap::new(), "retry-backoff-ms", RETRY_BACKOFF_MS)
                .expect("valid default backoff"),
            RETRY_BACKOFF_MS.to_vec()
        );
    }

    #[test]
    fn backoff_rejects_invalid_values() {
        let mut config = HashMap::new();
        config.insert("retry-backoff-ms".to_string(), "1000,invalid".to_string());
        assert!(parse_backoff_ms(&config, "retry-backoff-ms", RETRY_BACKOFF_MS).is_err());
    }

    #[test]
    fn metadata_keys_are_namespaced_by_queue() {
        assert_eq!(
            metadata_key("agent-task", "0198e57c-0000-7000-8000-000000000000"),
            "agent-task.0198e57c-0000-7000-8000-000000000000"
        );
    }

    #[test]
    fn status_names_are_stable() {
        assert_eq!(CoreTaskStatus::Succeeded.as_str(), "succeeded");
        assert_eq!(CoreTaskStatus::DispatchTimeout.as_str(), "dispatch-timeout");
    }

    #[test]
    fn status_variants_are_stable() {
        assert!(matches!(
            status_variant(&TaskState::DispatchTimeoutPending),
            CoreTaskStatus::DispatchTimeout
        ));
        assert!(matches!(
            status_variant(&TaskState::Cancelled),
            CoreTaskStatus::Cancelled
        ));
    }
}
