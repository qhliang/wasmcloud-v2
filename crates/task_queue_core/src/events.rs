//! Control event and terminal result schemas.
//!
//! These schemas are exchanged outside JetStream task subjects: heartbeat and
//! lifecycle events use core NATS, while terminal results may also be archived.

use serde::{Deserialize, Serialize};

use crate::types::{
    AttemptFailure, TaskError, TaskId, TaskOutput, TaskStatus, base64_encode, decode_base64,
};

/// v2: observer 接口新增 `on-start`（external worker 可发布 `start` 事件类型），
/// `on-complete` 更名为 `on-terminate`。
pub const SCHEMA_VERSION: u32 = 2;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventProducer {
    /// Descriptive workload identity; it is not an authorization credential.
    pub namespace: String,
    pub workload: String,
    pub component: String,
}

/// Business progress published by a worker. It does not extend the JetStream
/// lease and must not be used as a liveness signal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HeartbeatEvent {
    pub task_id: TaskId,
    pub attempt: u32,
    pub timestamp_ms: u64,
    pub info: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<EventProducer>,
}

/// Terminal result archived for auditing and late observer recovery.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResultEvent {
    pub schema_version: u32,
    pub id: TaskId,
    pub status: TaskStatus,
    pub attempt: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<Vec<u8>>,
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at_ms: Option<u64>,
}

impl TaskResultEvent {
    pub fn output(&self) -> TaskOutput {
        if let Some(output) = self.output.clone() {
            return Some(output);
        }
        match self.output_base64.as_deref().map(decode_base64) {
            Some(output) => output,
            None => self.output.clone(),
        }
    }
}

/// One failed delivery attempt reported to the producer's observer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptFailedEvent {
    pub schema_version: u32,
    #[serde(flatten)]
    pub failure: AttemptFailure,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub producer: Option<EventProducer>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ControlEvent<'a> {
    Heartbeat(&'a HeartbeatEvent),
    AttemptFailed(&'a AttemptFailedEvent),
    Complete(&'a TaskResultEvent),
}

impl ControlEvent<'_> {
    pub fn subject(queue: &str) -> String {
        format!("{queue}.events")
    }
}

// ---------- 控制事件构造与发布的单一事实源 ----------
//
// `{queue}.events` 上的 JSON 事件契约只在下面这几个函数里定义一次：
// 参考实现 `task_queue_worker`（runner 侧）与 `TaskContext`（worker 侧）
// 都必须经由它们构造和发布，禁止各自手写 JSON。

/// `start` 事件：任务某次 attempt 开始执行。
pub fn start_event(id: &str, attempt: u32) -> serde_json::Value {
    serde_json::json!({
        "type": "start",
        "id": id,
        "attempt": attempt,
    })
}

/// `attempt_failed` 事件：某次尝试失败（`source` 为 `"guest"` 或 `"system"`）。
pub fn attempt_failed_event(
    id: &str,
    attempt: u32,
    source: &str,
    error: &str,
) -> serde_json::Value {
    serde_json::json!({
        "type": "attempt_failed",
        "id": id,
        "attempt": attempt,
        "source": source,
        "error": error,
    })
}

/// `heartbeat` 事件：业务进度上报。
pub fn heartbeat_event(id: &str, info: &str) -> serde_json::Value {
    serde_json::json!({
        "type": "heartbeat",
        "id": id,
        "info": info,
    })
}

/// `complete` 事件：任务终态。`output` 存在时以 base64 编码承载，
/// `status` 使用 [`TaskStatus::as_str`] 的 kebab-case 拼写，
/// 与宿主插件 `parse_control_event` 的解析契约对齐。
pub fn complete_event(
    id: &str,
    attempt: u32,
    status: TaskStatus,
    output: Option<&[u8]>,
    error: Option<&str>,
) -> serde_json::Value {
    serde_json::json!({
        "type": "complete",
        "id": id,
        "attempt": attempt,
        "status": status.as_str(),
        "output": output.map(base64_encode),
        "error": error,
    })
}

/// 把控制事件发布到 core NATS 主题上。
pub async fn publish_control_event(
    client: &async_nats::Client,
    subject: String,
    value: serde_json::Value,
) -> Result<(), TaskError> {
    client
        .publish(subject, value.to_string().into_bytes().into())
        .await
        .map_err(|err| TaskError::system(format!("failed to publish task event: {err}")))
}
