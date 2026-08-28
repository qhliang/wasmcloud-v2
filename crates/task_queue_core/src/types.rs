//! Task records and public result types.
//!
//! These types are the wire contract for task metadata and envelopes. Fields
//! must remain backward compatible; add new fields with defaults instead of
//! renaming existing ones.

use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

pub type TaskId = String;
pub type TaskOutput = Option<Vec<u8>>;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Task {
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    Queued,
    DispatchTimeoutPending,
    Running,
    DispatchTimeout,
    ExecutionTimeout,
    Cancelled,
    MaxRetriesExceeded,
    Succeeded,
    Failed,
}

impl TaskState {
    /// Pending states may be retried or cancelled; terminal states must not
    /// be executed again.
    pub fn is_terminal(self) -> bool {
        !matches!(
            self,
            Self::Queued | Self::DispatchTimeoutPending | Self::Running
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Succeeded,
    Failed,
    DispatchTimeout,
    ExecutionTimeout,
    Cancelled,
    MaxRetriesExceeded,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::DispatchTimeout => "dispatch-timeout",
            Self::ExecutionTimeout => "execution-timeout",
            Self::Cancelled => "cancelled",
            Self::MaxRetriesExceeded => "max-retries-exceeded",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptFailureRecord {
    pub attempt: u32,
    pub source: String,
    pub error: String,
    pub started_at_ms: Option<u64>,
    pub failed_at_ms: Option<u64>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMeta {
    /// Bumped when metadata semantics change; readers reject unknown versions.
    pub schema_version: u32,
    pub id: String,
    pub queue: String,
    pub state: TaskState,
    pub attempt: u32,
    pub created_at_ms: u64,
    pub dispatched_at_ms: Option<u64>,
    pub completed_at_ms: Option<u64>,
    pub deadline_ms: u64,
    pub cancel_requested: bool,
    #[serde(default)]
    pub attempts: Vec<AttemptFailureRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskEnvelope {
    pub schema_version: u32,
    pub task_id: String,
    pub payload_encoding: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<Vec<u8>>,
    pub execution_deadline_ms: u64,
}

impl TaskEnvelope {
    pub const SCHEMA_VERSION: u32 = 1;
    pub const PAYLOAD_ENCODING_RAW: &str = "raw";
    pub const PAYLOAD_ENCODING_BINARY: &str = "binary";

    /// Creates a base64-encoded envelope that preserves arbitrary raw bytes.
    pub fn raw(task_id: impl Into<String>, payload: Vec<u8>, deadline_ms: u64) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            task_id: task_id.into(),
            payload_encoding: Self::PAYLOAD_ENCODING_RAW.to_string(),
            payload_base64: Some(base64_encode(&payload)),
            payload: None,
            execution_deadline_ms: deadline_ms,
        }
    }

    /// Creates a binary-field envelope. Prefer `raw` for new producers.
    pub fn binary(task_id: impl Into<String>, payload: Vec<u8>, deadline_ms: u64) -> Self {
        Self {
            schema_version: Self::SCHEMA_VERSION,
            task_id: task_id.into(),
            payload_encoding: Self::PAYLOAD_ENCODING_BINARY.to_string(),
            payload_base64: None,
            payload: Some(payload),
            execution_deadline_ms: deadline_ms,
        }
    }

    pub fn task_id(&self) -> &str {
        &self.task_id
    }

    pub fn execution_deadline_ms(&self) -> u64 {
        self.execution_deadline_ms
    }

    pub fn decode_payload(&self) -> Result<Vec<u8>, PayloadError> {
        // Base64 is normalized for raw payloads; binary is a compatibility
        // fallback for byte-array envelopes already in flight.
        match self.payload_encoding.as_str() {
            Self::PAYLOAD_ENCODING_RAW => {
                let encoded = self
                    .payload_base64
                    .as_deref()
                    .ok_or(PayloadError::MissingPayload)?;
                base64_decode(encoded).map_err(|_| PayloadError::InvalidBase64)
            }
            Self::PAYLOAD_ENCODING_BINARY => {
                if let Some(payload) = self.payload.as_ref() {
                    return Ok(payload.clone());
                }
                let encoded = self
                    .payload_base64
                    .as_deref()
                    .ok_or(PayloadError::MissingPayload)?;
                base64_decode(encoded).map_err(|_| PayloadError::InvalidBase64)
            }
            _ => Err(PayloadError::UnsupportedEncoding),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadError {
    UnsupportedEncoding,
    InvalidBase64,
    MissingPayload,
}

impl std::fmt::Display for PayloadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::UnsupportedEncoding => "unsupported payload encoding",
            Self::InvalidBase64 => "invalid base64 payload",
            Self::MissingPayload => "missing payload",
        };
        f.write_str(message)
    }
}

impl std::error::Error for PayloadError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskInfo {
    pub id: String,
    pub status: TaskStatus,
    pub attempt: Option<u32>,
    pub created_at_ms: u64,
    pub dispatched_at_ms: Option<u64>,
    pub completed_at_ms: Option<u64>,
    pub cancel_requested: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptErrorSource {
    Guest,
    System,
}

impl AttemptErrorSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Guest => "guest",
            Self::System => "system",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptFailure {
    pub id: String,
    pub attempt: u32,
    pub source: AttemptErrorSource,
    pub error: String,
    pub started_at_ms: Option<u64>,
    pub failed_at_ms: Option<u64>,
    pub duration_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskResult {
    pub id: String,
    pub status: TaskStatus,
    pub attempt: u32,
    pub output: TaskOutput,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskErrorSource {
    Guest,
    System,
}

impl TaskErrorSource {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Guest => "guest",
            Self::System => "system",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskError {
    pub source: TaskErrorSource,
    pub message: String,
}

impl TaskError {
    pub fn guest(message: impl Into<String>) -> Self {
        Self {
            source: TaskErrorSource::Guest,
            message: message.into(),
        }
    }

    pub fn system(message: impl Into<String>) -> Self {
        Self {
            source: TaskErrorSource::System,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.source.as_str(), self.message)
    }
}

impl std::error::Error for TaskError {}

pub fn base64_encode(value: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let alphabet_char = |index: u32| -> char {
        let offset = usize::try_from(index & 63).unwrap_or_default();
        ALPHABET.get(offset).map_or('\0', |byte| *byte as char)
    };
    let mut encoded = String::with_capacity(value.len().div_ceil(3) * 4);
    for chunk in value.chunks(3) {
        let a = chunk.first().map_or(0u32, |value| u32::from(*value));
        let b = chunk.get(1).map_or(0u32, |value| u32::from(*value));
        let c = chunk.get(2).map_or(0u32, |value| u32::from(*value));
        let bits = (a << 16) | (b << 8) | c;
        encoded.push(alphabet_char(bits >> 18));
        encoded.push(alphabet_char(bits >> 12));
        encoded.push(if chunk.len() > 1 {
            alphabet_char(bits >> 6)
        } else {
            '='
        });
        encoded.push(if chunk.len() > 2 {
            alphabet_char(bits)
        } else {
            '='
        });
    }
    encoded
}

pub fn base64_decode(value: &str) -> Result<Vec<u8>> {
    let mut output = Vec::with_capacity(value.len() / 4 * 3);
    let mut bits = 0u64;
    let mut count = 0u32;
    for byte in value.bytes() {
        let digit = match byte {
            b'A'..=b'Z' => u32::from(byte - b'A'),
            b'a'..=b'z' => u32::from(byte - b'a') + 26,
            b'0'..=b'9' => u32::from(byte - b'0') + 52,
            b'+' => 62,
            b'/' => 63,
            b'=' if count == 2 => {
                output.push(u8::try_from((bits >> 4) & 255).unwrap_or_default());
                break;
            }
            b'=' if count == 3 => {
                output.push(u8::try_from((bits >> 10) & 255).unwrap_or_default());
                output.push(u8::try_from((bits >> 2) & 255).unwrap_or_default());
                break;
            }
            b'=' => bail!("unexpected base64 padding"),
            _ => bail!("invalid base64 character"),
        };
        bits = (bits << 6) | u64::from(digit);
        count += 1;
        if count == 4 {
            output.extend_from_slice(&bits.to_be_bytes()[5..]);
            bits = 0;
            count = 0;
        }
    }
    if count == 1 || value.ends_with('=') && !value.len().is_multiple_of(4) {
        bail!("invalid base64 length");
    }
    Ok(output)
}

pub fn decode_base64(value: &str) -> Option<Vec<u8>> {
    base64_decode(value).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_envelope_round_trips_payload() {
        let payload = b"hello".to_vec();
        let envelope = TaskEnvelope::raw("task-1", payload.clone(), 123);
        assert_eq!(envelope.decode_payload().expect("valid payload"), payload);
    }

    #[test]
    fn binary_envelope_round_trips_payload() {
        let payload = vec![0u8, 1, 2, 3];
        let envelope = TaskEnvelope::binary("task-1", payload.clone(), 123);
        assert_eq!(envelope.decode_payload().expect("valid payload"), payload);
    }

    #[test]
    fn invalid_encoding_is_rejected() {
        let mut envelope = TaskEnvelope::raw("task-1", b"hello".to_vec(), 123);
        envelope.payload_encoding = "xml".to_string();
        assert_eq!(
            envelope.decode_payload(),
            Err(PayloadError::UnsupportedEncoding)
        );
    }

    #[test]
    fn status_names_are_stable() {
        assert_eq!(TaskStatus::Succeeded.as_str(), "succeeded");
        assert_eq!(TaskStatus::DispatchTimeout.as_str(), "dispatch-timeout");
    }

    #[test]
    fn pending_states_are_not_terminal() {
        assert!(!TaskState::Queued.is_terminal());
        assert!(!TaskState::Running.is_terminal());
        assert!(TaskState::Cancelled.is_terminal());
    }
}
