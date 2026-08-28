//! Control event and terminal result schemas.
//!
//! These schemas are exchanged outside JetStream task subjects: heartbeat and
//! lifecycle events use core NATS, while terminal results may also be archived.

use serde::{Deserialize, Serialize};

use crate::types::{AttemptFailure, TaskId, TaskOutput, TaskStatus, decode_base64};

pub const SCHEMA_VERSION: u32 = 1;

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
